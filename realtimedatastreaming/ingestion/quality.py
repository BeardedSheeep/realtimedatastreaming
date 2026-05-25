from __future__ import annotations

import hashlib
import hmac
import re
import unicodedata
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import urlparse

from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid

MIN_PLAUSIBLE_AGE = 13
MAX_PLAUSIBLE_AGE = 100
MAX_TEXT_FIELD_LENGTH = 512
MAX_URL_LENGTH = 2_048

RANDOM_USER_SUPPORTED_COUNTRY_NAMES = frozenset({
    "Australia",
    "Brazil",
    "Canada",
    "Denmark",
    "Finland",
    "France",
    "Germany",
    "India",
    "Iran",
    "Ireland",
    "Mexico",
    "Netherlands",
    "New Zealand",
    "Norway",
    "Serbia",
    "Spain",
    "Switzerland",
    "Turkey",
    "Ukraine",
    "United Kingdom",
    "United States",
})
ISO_ALPHA_2_COUNTRY_CODES = frozenset({
    "AD",
    "AE",
    "AF",
    "AG",
    "AI",
    "AL",
    "AM",
    "AO",
    "AQ",
    "AR",
    "AS",
    "AT",
    "AU",
    "AW",
    "AX",
    "AZ",
    "BA",
    "BB",
    "BD",
    "BE",
    "BF",
    "BG",
    "BH",
    "BI",
    "BJ",
    "BL",
    "BM",
    "BN",
    "BO",
    "BQ",
    "BR",
    "BS",
    "BT",
    "BV",
    "BW",
    "BY",
    "BZ",
    "CA",
    "CC",
    "CD",
    "CF",
    "CG",
    "CH",
    "CI",
    "CK",
    "CL",
    "CM",
    "CN",
    "CO",
    "CR",
    "CU",
    "CV",
    "CW",
    "CX",
    "CY",
    "CZ",
    "DE",
    "DJ",
    "DK",
    "DM",
    "DO",
    "DZ",
    "EC",
    "EE",
    "EG",
    "EH",
    "ER",
    "ES",
    "ET",
    "FI",
    "FJ",
    "FK",
    "FM",
    "FO",
    "FR",
    "GA",
    "GB",
    "GD",
    "GE",
    "GF",
    "GG",
    "GH",
    "GI",
    "GL",
    "GM",
    "GN",
    "GP",
    "GQ",
    "GR",
    "GS",
    "GT",
    "GU",
    "GW",
    "GY",
    "HK",
    "HM",
    "HN",
    "HR",
    "HT",
    "HU",
    "ID",
    "IE",
    "IL",
    "IM",
    "IN",
    "IO",
    "IQ",
    "IR",
    "IS",
    "IT",
    "JE",
    "JM",
    "JO",
    "JP",
    "KE",
    "KG",
    "KH",
    "KI",
    "KM",
    "KN",
    "KP",
    "KR",
    "KW",
    "KY",
    "KZ",
    "LA",
    "LB",
    "LC",
    "LI",
    "LK",
    "LR",
    "LS",
    "LT",
    "LU",
    "LV",
    "LY",
    "MA",
    "MC",
    "MD",
    "ME",
    "MF",
    "MG",
    "MH",
    "MK",
    "ML",
    "MM",
    "MN",
    "MO",
    "MP",
    "MQ",
    "MR",
    "MS",
    "MT",
    "MU",
    "MV",
    "MW",
    "MX",
    "MY",
    "MZ",
    "NA",
    "NC",
    "NE",
    "NF",
    "NG",
    "NI",
    "NL",
    "NO",
    "NP",
    "NR",
    "NU",
    "NZ",
    "OM",
    "PA",
    "PE",
    "PF",
    "PG",
    "PH",
    "PK",
    "PL",
    "PM",
    "PN",
    "PR",
    "PS",
    "PT",
    "PW",
    "PY",
    "QA",
    "RE",
    "RO",
    "RS",
    "RU",
    "RW",
    "SA",
    "SB",
    "SC",
    "SD",
    "SE",
    "SG",
    "SH",
    "SI",
    "SJ",
    "SK",
    "SL",
    "SM",
    "SN",
    "SO",
    "SR",
    "SS",
    "ST",
    "SV",
    "SX",
    "SY",
    "SZ",
    "TC",
    "TD",
    "TF",
    "TG",
    "TH",
    "TJ",
    "TK",
    "TL",
    "TM",
    "TN",
    "TO",
    "TR",
    "TT",
    "TV",
    "TW",
    "TZ",
    "UA",
    "UG",
    "UM",
    "US",
    "UY",
    "UZ",
    "VA",
    "VC",
    "VE",
    "VG",
    "VI",
    "VN",
    "VU",
    "WF",
    "WS",
    "YE",
    "YT",
    "ZA",
    "ZM",
    "ZW",
})
INVALID_EVENT_PAYLOAD_ALLOWED_FIELDS = frozenset({
    "schema_version",
    "event_type",
    "source",
    "country_code",
})
PSEUDONYMIZED_SOURCE_USER_ID_PREFIX = "sha256:"

REJECTION_MISSING_SOURCE = "missing_source"
REJECTION_MISSING_SOURCE_USER_ID = "missing_source_user_id"
REJECTION_MISSING_EMAIL = "missing_email"
REJECTION_INVALID_EMAIL = "invalid_email"
REJECTION_MISSING_USERNAME = "missing_username"
REJECTION_MISSING_COUNTRY = "missing_country"
REJECTION_UNSUPPORTED_COUNTRY = "unsupported_country"
REJECTION_INVALID_COUNTRY_CODE = "invalid_country_code"
REJECTION_MISSING_DATE_OF_BIRTH = "missing_date_of_birth"
REJECTION_INVALID_DATE_OF_BIRTH = "invalid_date_of_birth"
REJECTION_IMPLAUSIBLE_AGE = "implausible_age"
REJECTION_INVALID_REGISTERED_AT = "invalid_registered_at"
REJECTION_REGISTERED_BEFORE_DATE_OF_BIRTH = "registered_before_date_of_birth"
REJECTION_REGISTERED_IN_FUTURE = "registered_in_future"
REJECTION_TEXT_FIELD_TOO_LONG = "text_field_too_long"
REJECTION_TEXT_FIELD_HAS_CONTROL_CHARACTERS = "text_field_has_control_characters"
REJECTION_INVALID_LATITUDE = "invalid_latitude"
REJECTION_INVALID_LONGITUDE = "invalid_longitude"
REJECTION_INVALID_NATIONALITY = "invalid_nationality"
REJECTION_INVALID_TIMEZONE_OFFSET = "invalid_timezone_offset"
REJECTION_INVALID_PICTURE_URL = "invalid_picture_url"

NATIONALITY_PATTERN = re.compile(r"^[A-Z]{2}$")
TIMEZONE_OFFSET_PATTERN = re.compile(r"^([+-]?)(\d{1,2}):(\d{2})$")


def validate_user_profile_quality(profile: UserCreated, *, today: date | None = None) -> tuple[str, ...]:
    """Return explicit quality rejection reasons for a normalized user profile."""

    reasons: list[str] = []
    reference_date = today or datetime.now(tz=UTC).date()

    if _is_blank(profile.source):
        reasons.append(REJECTION_MISSING_SOURCE)
    if _is_blank(profile.source_user_id):
        reasons.append(REJECTION_MISSING_SOURCE_USER_ID)
    if _is_blank(str(profile.email)):
        reasons.append(REJECTION_MISSING_EMAIL)
    if _is_blank(profile.username):
        reasons.append(REJECTION_MISSING_USERNAME)
    _validate_country(profile, reasons)

    birth_datetime = _parse_datetime(profile.date_of_birth)
    if _is_blank(str(profile.date_of_birth)):
        reasons.append(REJECTION_MISSING_DATE_OF_BIRTH)
    elif birth_datetime is None:
        reasons.append(REJECTION_INVALID_DATE_OF_BIRTH)
    elif not _has_plausible_age(birth_datetime.date(), today=reference_date):
        reasons.append(REJECTION_IMPLAUSIBLE_AGE)

    _validate_registered_at(profile, birth_datetime, reference_date, reasons)
    _validate_coordinates(profile, reasons)
    _validate_nationality(profile, reasons)
    _validate_timezone_offset(profile, reasons)
    _validate_picture_urls(profile, reasons)
    _validate_text_fields(profile, reasons)

    return tuple(reasons)


def is_valid_user_profile(profile: UserCreated, *, today: date | None = None) -> bool:
    return not validate_user_profile_quality(profile, today=today)


def build_invalid_user_profile_event(
    profile: UserCreated,
    rejection_reasons: tuple[str, ...],
    *,
    pseudonymization_salt: str | None = None,
) -> UserProfileInvalid:
    return UserProfileInvalid(
        source=profile.source,
        source_user_id=pseudonymize_source_user_id(
            source=profile.source,
            source_user_id=profile.source_user_id,
            salt=pseudonymization_salt,
        ),
        rejection_reasons=rejection_reasons,
        payload=_redact_invalid_payload(profile),
    )


def pseudonymize_source_user_id(*, source: str, source_user_id: str, salt: str | None = None) -> str:
    if salt is None:
        msg = "pseudonymization salt is required"
        raise ValueError(msg)
    message = f"{source}:{source_user_id}".encode()
    digest = hmac.new(salt.encode(), message, hashlib.sha256).hexdigest()
    return f"{PSEUDONYMIZED_SOURCE_USER_ID_PREFIX}{digest}"


def _validate_registered_at(
    profile: UserCreated,
    birth_datetime: datetime | None,
    reference_date: date,
    reasons: list[str],
) -> datetime | None:
    if profile.registered_at is None:
        return None

    registered_datetime = _parse_datetime(profile.registered_at)
    if registered_datetime is None:
        reasons.append(REJECTION_INVALID_REGISTERED_AT)
        return None

    if birth_datetime is not None and registered_datetime < birth_datetime:
        reasons.append(REJECTION_REGISTERED_BEFORE_DATE_OF_BIRTH)
    if registered_datetime.date() > reference_date:
        reasons.append(REJECTION_REGISTERED_IN_FUTURE)
    return registered_datetime


def _validate_country(profile: UserCreated, reasons: list[str]) -> None:
    if _is_blank(profile.country):
        reasons.append(REJECTION_MISSING_COUNTRY)
    elif profile.source == "random_user" and profile.country not in RANDOM_USER_SUPPORTED_COUNTRY_NAMES:
        reasons.append(REJECTION_UNSUPPORTED_COUNTRY)

    if profile.country_code is not None and profile.country_code not in ISO_ALPHA_2_COUNTRY_CODES:
        reasons.append(REJECTION_INVALID_COUNTRY_CODE)


def _validate_coordinates(profile: UserCreated, reasons: list[str]) -> None:
    if profile.latitude is not None and not _is_float_in_range(profile.latitude, minimum=-90.0, maximum=90.0):
        reasons.append(REJECTION_INVALID_LATITUDE)
    if profile.longitude is not None and not _is_float_in_range(profile.longitude, minimum=-180.0, maximum=180.0):
        reasons.append(REJECTION_INVALID_LONGITUDE)


def _validate_nationality(profile: UserCreated, reasons: list[str]) -> None:
    if profile.nationality is not None and (
        NATIONALITY_PATTERN.fullmatch(profile.nationality) is None
        or profile.nationality not in ISO_ALPHA_2_COUNTRY_CODES
    ):
        reasons.append(REJECTION_INVALID_NATIONALITY)


def _validate_timezone_offset(profile: UserCreated, reasons: list[str]) -> None:
    if profile.timezone_offset is None:
        return

    match = TIMEZONE_OFFSET_PATTERN.fullmatch(profile.timezone_offset)
    if match is None:
        reasons.append(REJECTION_INVALID_TIMEZONE_OFFSET)
        return

    sign, hours_value, minutes_value = match.groups()
    hours = int(hours_value)
    minutes = int(minutes_value)
    total_minutes = hours * 60 + minutes
    if sign == "-":
        total_minutes *= -1
    if minutes > 59 or total_minutes < -12 * 60 or total_minutes > 14 * 60:
        reasons.append(REJECTION_INVALID_TIMEZONE_OFFSET)


def _validate_picture_urls(profile: UserCreated, reasons: list[str]) -> None:
    picture_urls = {
        "picture_large": profile.picture_large,
        "picture_medium": profile.picture_medium,
        "picture_thumbnail": profile.picture_thumbnail,
    }
    for field_name, picture_url in picture_urls.items():
        if picture_url is not None and not _is_valid_https_url(picture_url):
            reasons.append(f"{REJECTION_INVALID_PICTURE_URL}:{field_name}")


def _validate_text_fields(profile: UserCreated, reasons: list[str]) -> None:
    for field_name, value in _text_fields(profile).items():
        if value is None:
            continue
        text_value = str(value)
        max_length = MAX_URL_LENGTH if field_name.startswith("picture_") else MAX_TEXT_FIELD_LENGTH
        if len(text_value) > max_length:
            reasons.append(f"{REJECTION_TEXT_FIELD_TOO_LONG}:{field_name}")
        if _has_control_characters(text_value):
            reasons.append(f"{REJECTION_TEXT_FIELD_HAS_CONTROL_CHARACTERS}:{field_name}")


def _text_fields(profile: UserCreated) -> dict[str, object | None]:
    return {
        "source": profile.source,
        "source_user_id": profile.source_user_id,
        "gender": profile.gender,
        "title": profile.title,
        "first_name": profile.first_name,
        "last_name": profile.last_name,
        "street_name": profile.street_name,
        "city": profile.city,
        "state": profile.state,
        "country": profile.country,
        "country_code": profile.country_code,
        "postcode": profile.postcode,
        "latitude": profile.latitude,
        "longitude": profile.longitude,
        "timezone_offset": profile.timezone_offset,
        "timezone_description": profile.timezone_description,
        "email": profile.email,
        "username": profile.username,
        "date_of_birth": profile.date_of_birth,
        "registered_at": profile.registered_at,
        "phone": profile.phone,
        "cell": profile.cell,
        "picture_large": profile.picture_large,
        "picture_medium": profile.picture_medium,
        "picture_thumbnail": profile.picture_thumbnail,
        "nationality": profile.nationality,
    }


def _parse_datetime(value: datetime | str) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    try:
        parsed_datetime = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed_datetime.tzinfo is None:
        return parsed_datetime.replace(tzinfo=UTC)
    return parsed_datetime.astimezone(UTC)


def _has_plausible_age(birth_date: date, *, today: date | None = None) -> bool:
    reference_date = today or datetime.now(tz=UTC).date()
    age = reference_date.year - birth_date.year
    if (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day):
        age -= 1
    return MIN_PLAUSIBLE_AGE <= age <= MAX_PLAUSIBLE_AGE


def _redact_invalid_payload(profile: UserCreated) -> dict[str, object]:
    return {
        key: value
        for key, value in profile.model_dump(mode="json").items()
        if key in INVALID_EVENT_PAYLOAD_ALLOWED_FIELDS
    }


def _is_blank(value: str) -> bool:
    return not value.strip()


def _is_float_in_range(value: float | str, *, minimum: float, maximum: float) -> bool:
    try:
        number = float(value)
    except ValueError:
        return False
    return minimum <= number <= maximum


def _is_valid_https_url(value: Any) -> bool:
    url_value = str(value)
    parsed_url = urlparse(url_value)
    return (
        parsed_url.scheme == "https"
        and bool(parsed_url.netloc)
        and len(url_value) <= MAX_URL_LENGTH
        and not _has_control_characters(url_value)
    )


def _has_control_characters(value: str) -> bool:
    return any(unicodedata.category(character) == "Cc" for character in value)
