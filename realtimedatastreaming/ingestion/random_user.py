# Copyright (c) 2026 BeardedSheeep

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from threading import Lock
from typing import Any, TypedDict

import httpx

RANDOM_USER_COUNTRY_CODES = {
    "Australia": "AU",
    "Brazil": "BR",
    "Canada": "CA",
    "Denmark": "DK",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "India": "IN",
    "Iran": "IR",
    "Ireland": "IE",
    "Mexico": "MX",
    "Netherlands": "NL",
    "New Zealand": "NZ",
    "Norway": "NO",
    "Serbia": "RS",
    "Spain": "ES",
    "Switzerland": "CH",
    "Turkey": "TR",
    "Ukraine": "UA",
    "United Kingdom": "GB",
    "United States": "US",
}


class NormalizedUserProfile(TypedDict):
    source: str
    source_user_id: str
    gender: str | None
    title: str | None
    first_name: str
    last_name: str
    street_number: int | None
    street_name: str | None
    city: str | None
    state: str | None
    country: str
    country_code: str | None
    postcode: str | None
    latitude: str | None
    longitude: str | None
    timezone_offset: str | None
    timezone_description: str | None
    email: str
    username: str
    date_of_birth: str
    registered_at: str | None
    phone: str | None
    cell: str | None
    picture_large: str | None
    picture_medium: str | None
    picture_thumbnail: str | None
    nationality: str | None


class RandomUserError(Exception):
    """Base error raised by the Random User ingestion boundary."""

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class RandomUserHTTPError(RandomUserError):
    """Raised when the Random User API cannot be reached successfully."""


class RandomUserPayloadError(RandomUserError):
    """Raised when the Random User API returns an unexpected payload."""


@dataclass(slots=True)
class RandomUserRateLimiter:
    requests_per_second: float
    sleep: Any = time.sleep
    monotonic: Any = time.monotonic
    _last_request_started_at: float | None = field(default=None, init=False)
    _lock: Lock = field(default_factory=Lock, init=False)

    def wait(self) -> None:
        with self._lock:
            if self.requests_per_second <= 0:
                msg = "requests_per_second must be greater than 0"
                raise ValueError(msg)

            now = self.monotonic()
            if self._last_request_started_at is not None:
                minimum_interval_seconds = 1 / self.requests_per_second
                elapsed_seconds = now - self._last_request_started_at
                remaining_seconds = minimum_interval_seconds - elapsed_seconds
                if remaining_seconds > 0:
                    self.sleep(remaining_seconds)
                    now = self.monotonic()

            self._last_request_started_at = now


@dataclass(slots=True)
class RandomUserClient:
    api_url: str
    timeout_seconds: float
    http_client: httpx.Client | None = None
    max_retries: int = 2
    backoff_seconds: tuple[float, ...] = (0.5, 1.0)
    requests_per_second: float = 2.0
    sleep: Any = time.sleep
    monotonic: Any = time.monotonic
    _rate_limiter: RandomUserRateLimiter = field(init=False)

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            msg = "max_retries must be greater than or equal to 0"
            raise ValueError(msg)
        self._rate_limiter = RandomUserRateLimiter(
            requests_per_second=self.requests_per_second,
            sleep=self.sleep,
            monotonic=self.monotonic,
        )

    def fetch_users(self, *, count: int = 1) -> list[NormalizedUserProfile]:
        if count < 1:
            msg = "count must be greater than or equal to 1"
            raise ValueError(msg)

        last_error: RandomUserError | None = None
        for attempt in range(self.max_retries + 1):
            try:
                self._rate_limiter.wait()
                response = self._get(params={"results": count})
                return _normalize_random_user_http_response(response, expected_count=count)
            except httpx.TimeoutException as exc:
                last_error = RandomUserHTTPError("Random User API request timed out", reason="timeout")
                if attempt == self.max_retries:
                    raise last_error from exc
                self._sleep_before_retry(attempt)
            except httpx.HTTPError as exc:
                last_error = RandomUserHTTPError("Random User API request failed", reason="http_error")
                if attempt == self.max_retries:
                    raise last_error from exc
                self._sleep_before_retry(attempt)
            except RandomUserError as exc:
                last_error = exc
                if not _is_retryable_random_user_error(exc) or attempt == self.max_retries:
                    raise
                self._sleep_before_retry(attempt)

        if last_error is not None:
            raise last_error

        msg = "failed to fetch users from Random User API"
        raise RandomUserHTTPError(msg, reason="unexpected_response")

    def _get(self, *, params: Mapping[str, int]) -> httpx.Response:
        timeout = httpx.Timeout(self.timeout_seconds)
        if self.http_client is not None:
            return self.http_client.get(self.api_url, params=params, timeout=timeout)

        with httpx.Client(timeout=timeout) as client:
            return client.get(self.api_url, params=params)

    def _sleep_before_retry(self, attempt: int) -> None:
        if not self.backoff_seconds:
            return
        delay_seconds = self.backoff_seconds[min(attempt, len(self.backoff_seconds) - 1)]
        self.sleep(delay_seconds)


def normalize_random_user_response(payload: Mapping[str, Any]) -> list[NormalizedUserProfile]:
    results = _required_results(payload)
    return [normalize_random_user_profile(result) for result in results]


def normalize_random_user_profile(profile: Mapping[str, Any]) -> NormalizedUserProfile:
    login = _required_mapping(profile, "login")
    name = _required_mapping(profile, "name")
    location = _required_mapping(profile, "location")
    street = _optional_mapping(location, "street")
    coordinates = _optional_mapping(location, "coordinates")
    timezone = _optional_mapping(location, "timezone")
    dob = _required_mapping(profile, "dob")
    registered = _optional_mapping(profile, "registered")
    picture = _optional_mapping(profile, "picture")
    country = _required_str(location, "country")

    return {
        "source": "random_user",
        "source_user_id": _required_str(login, "uuid"),
        "gender": _optional_str(profile, "gender"),
        "title": _optional_str(name, "title"),
        "first_name": _required_str(name, "first"),
        "last_name": _required_str(name, "last"),
        "street_number": _optional_int(street, "number"),
        "street_name": _optional_str(street, "name"),
        "city": _optional_str(location, "city"),
        "state": _optional_str(location, "state"),
        "country": country,
        "country_code": RANDOM_USER_COUNTRY_CODES.get(country),
        "postcode": _optional_stringified(location, "postcode"),
        "latitude": _optional_str(coordinates, "latitude"),
        "longitude": _optional_str(coordinates, "longitude"),
        "timezone_offset": _optional_str(timezone, "offset"),
        "timezone_description": _optional_str(timezone, "description"),
        "email": _required_str(profile, "email"),
        "username": _required_str(login, "username"),
        "date_of_birth": _required_str(dob, "date"),
        "registered_at": _optional_str(registered, "date"),
        "phone": _optional_str(profile, "phone"),
        "cell": _optional_str(profile, "cell"),
        "picture_large": _optional_str(picture, "large"),
        "picture_medium": _optional_str(picture, "medium"),
        "picture_thumbnail": _optional_str(picture, "thumbnail"),
        "nationality": _optional_str(profile, "nat"),
    }


def _required_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        msg = f"missing or invalid object field: {key}"
        raise RandomUserPayloadError(msg, reason="invalid_payload")
    return value


def _optional_mapping(payload: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = payload.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        msg = f"invalid object field: {key}"
        raise RandomUserPayloadError(msg, reason="invalid_payload")
    return value


def _required_str(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        msg = f"missing or invalid string field: {key}"
        raise RandomUserPayloadError(msg, reason="invalid_payload")
    return value


def _optional_str(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        msg = f"invalid string field: {key}"
        raise RandomUserPayloadError(msg, reason="invalid_payload")
    return value


def _optional_int(payload: Mapping[str, Any], key: str) -> int | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, int):
        msg = f"invalid integer field: {key}"
        raise RandomUserPayloadError(msg, reason="invalid_payload")
    return value


def _optional_stringified(payload: Mapping[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if isinstance(value, str | int):
        return str(value)
    msg = f"invalid string-compatible field: {key}"
    raise RandomUserPayloadError(msg, reason="invalid_payload")


def _normalize_random_user_http_response(
    response: httpx.Response, *, expected_count: int
) -> list[NormalizedUserProfile]:
    if response.status_code == 429 or 500 <= response.status_code < 600:
        msg = f"Random User API returned retryable HTTP status {response.status_code}"
        raise RandomUserHTTPError(msg, reason=f"http_{response.status_code}")

    if not 200 <= response.status_code < 300:
        msg = f"Random User API returned HTTP status {response.status_code}"
        raise RandomUserHTTPError(msg, reason=f"http_{response.status_code}")

    try:
        payload = response.json()
    except ValueError as exc:
        msg = "Random User API returned invalid JSON"
        raise RandomUserPayloadError(msg, reason="invalid_json") from exc

    if not isinstance(payload, Mapping):
        msg = "Random User API returned a non-object JSON payload"
        raise RandomUserPayloadError(msg, reason="unexpected_response")

    users = normalize_random_user_response(payload)
    if len(users) != expected_count:
        msg = f"Random User API returned {len(users)} users, expected {expected_count}"
        raise RandomUserPayloadError(msg, reason="unexpected_response")
    return users


def _required_results(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    value = payload.get("results")
    if not isinstance(value, list):
        msg = "missing or invalid list field: results"
        raise RandomUserPayloadError(msg, reason="unexpected_response")
    if not value:
        msg = "Random User API returned empty results"
        raise RandomUserPayloadError(msg, reason="empty_results")
    if not all(isinstance(item, Mapping) for item in value):
        msg = "results must contain only objects"
        raise RandomUserPayloadError(msg, reason="invalid_payload")
    return value


def _is_retryable_random_user_error(error: RandomUserError) -> bool:
    return error.reason in {"timeout", "http_429", "empty_results"} or error.reason.startswith("http_5")
