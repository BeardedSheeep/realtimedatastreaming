# Copyright (c) 2026 BeardedSheeep

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from realtimedatastreaming.ingestion.random_user import RandomUserClient, RandomUserHTTPError, RandomUserPayloadError


def test_random_user_client_does_not_retry_non_retryable_http_errors() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(400, json={"error": "bad request"})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        sleep=lambda _seconds: None,
    )

    with pytest.raises(RandomUserHTTPError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "http_400"
    assert calls == 1


def test_random_user_client_retries_rate_limit_then_returns_valid_payload(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    fake_clock: Any,
) -> None:
    responses = [
        httpx.Response(429, json={"error": "too many requests"}),
        httpx.Response(200, json=random_user_payload_factory()),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    users = client.fetch_users()

    assert users[0]["username"] == "adal-1"
    assert fake_clock.sleep_calls == [0.5]


def test_random_user_client_retries_any_5xx_then_returns_valid_payload(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    fake_clock: Any,
) -> None:
    responses = [
        httpx.Response(501, json={"error": "not implemented"}),
        httpx.Response(200, json=random_user_payload_factory()),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    users = client.fetch_users()

    assert users[0]["source_user_id"] == "2f4c4f6e-743b-4c8e-82df-35b2c789f35f-1"
    assert fake_clock.sleep_calls == [0.5]


def test_random_user_client_retries_empty_results_then_returns_valid_payload(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    fake_clock: Any,
) -> None:
    responses = [
        httpx.Response(200, json={"results": [], "info": {"results": 1}}),
        httpx.Response(200, json=random_user_payload_factory()),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        backoff_seconds=(0.5, 1.0),
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    users = client.fetch_users()

    assert users[0]["source_user_id"] == "2f4c4f6e-743b-4c8e-82df-35b2c789f35f-1"
    assert fake_clock.sleep_calls == [0.5]


def test_random_user_client_classifies_invalid_json() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{")

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=0,
    )

    with pytest.raises(RandomUserPayloadError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "invalid_json"


def test_random_user_client_classifies_invalid_response_shape() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"info": {"results": 1}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=0,
    )

    with pytest.raises(RandomUserPayloadError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "unexpected_response"


def test_random_user_client_rejects_unexpected_user_count(
    random_user_payload_factory: Callable[..., dict[str, Any]],
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=random_user_payload_factory(results=1))

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=0,
    )

    with pytest.raises(RandomUserPayloadError) as exc_info:
        client.fetch_users(count=2)

    assert exc_info.value.reason == "unexpected_response"


def test_random_user_client_fails_cleanly_after_empty_results_retries(fake_clock: Any) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"results": [], "info": {"results": 1}})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        backoff_seconds=(0.5, 1.0),
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    with pytest.raises(RandomUserPayloadError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "empty_results"
    assert calls == 3
    assert fake_clock.sleep_calls == [0.5, 1.0]


def test_random_user_client_fails_cleanly_after_5xx_retries(fake_clock: Any) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "unavailable"})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        backoff_seconds=(0.5, 1.0),
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    with pytest.raises(RandomUserHTTPError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "http_503"
    assert calls == 3
    assert fake_clock.sleep_calls == [0.5, 1.0]


def test_random_user_client_does_not_retry_invalid_payload(
    random_user_payload_factory: Callable[..., dict[str, Any]],
) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        payload = random_user_payload_factory()
        del payload["results"][0]["email"]
        return httpx.Response(200, json=payload)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        sleep=lambda _seconds: None,
    )

    with pytest.raises(RandomUserPayloadError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "invalid_payload"
    assert calls == 1


def test_random_user_client_retries_timeouts(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    fake_clock: Any,
) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("request timed out")
        return httpx.Response(200, json=random_user_payload_factory())

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    users = client.fetch_users()

    assert users[0]["username"] == "adal-1"
    assert calls == 2
    assert fake_clock.sleep_calls == [0.5]


def test_random_user_client_fails_cleanly_after_timeout_retries(fake_clock: Any) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("request timed out")

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        backoff_seconds=(0.5, 1.0),
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    with pytest.raises(RandomUserHTTPError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "timeout"
    assert calls == 3
    assert fake_clock.sleep_calls == [0.5, 1.0]


def test_random_user_client_reuses_last_backoff_when_retry_count_exceeds_backoff_schedule(fake_clock: Any) -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"error": "unavailable"})

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        requests_per_second=100.0,
        backoff_seconds=(0.25,),
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    with pytest.raises(RandomUserHTTPError) as exc_info:
        client.fetch_users()

    assert exc_info.value.reason == "http_503"
    assert calls == 3
    assert fake_clock.sleep_calls == [0.25, 0.25]


def test_random_user_client_applies_rate_limit_between_retries(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    fake_clock: Any,
) -> None:
    responses = [
        httpx.Response(200, json={"results": [], "info": {"results": 1}}),
        httpx.Response(200, json=random_user_payload_factory()),
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return responses.pop(0)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=2,
        requests_per_second=1.0,
        backoff_seconds=(0.25, 1.0),
        sleep=fake_clock.sleep,
        monotonic=fake_clock.monotonic,
    )

    users = client.fetch_users()

    assert users[0]["username"] == "adal-1"
    assert fake_clock.sleep_calls == [0.25, 0.75]


def test_random_user_client_rejects_invalid_retry_configuration() -> None:
    with pytest.raises(ValueError, match="max_retries"):
        RandomUserClient(api_url="https://randomuser.example/api/", timeout_seconds=2.5, max_retries=-1)
