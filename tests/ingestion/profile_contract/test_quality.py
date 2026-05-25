from datetime import date

import pytest

from realtimedatastreaming.ingestion.quality import (
    REJECTION_IMPLAUSIBLE_AGE,
    REJECTION_INVALID_COUNTRY_CODE,
    REJECTION_INVALID_NATIONALITY,
    REJECTION_INVALID_PICTURE_URL,
    REJECTION_INVALID_TIMEZONE_OFFSET,
    REJECTION_REGISTERED_BEFORE_DATE_OF_BIRTH,
    REJECTION_REGISTERED_IN_FUTURE,
    REJECTION_TEXT_FIELD_HAS_CONTROL_CHARACTERS,
    REJECTION_TEXT_FIELD_TOO_LONG,
    REJECTION_UNSUPPORTED_COUNTRY,
    build_invalid_user_profile_event,
    pseudonymize_source_user_id,
    validate_user_profile_quality,
)
from realtimedatastreaming.ingestion.schemas import UserCreated


def test_validate_user_profile_quality_accepts_valid_profile() -> None:
    assert validate_user_profile_quality(_valid_user(), today=date(2025, 1, 1)) == ()


@pytest.mark.parametrize(
    "updates, expected_reason",
    [
        ({"country": "Atlantis"}, REJECTION_UNSUPPORTED_COUNTRY),
        ({"country_code": "ZZ"}, REJECTION_INVALID_COUNTRY_CODE),
        ({"country_code": "gb"}, REJECTION_INVALID_COUNTRY_CODE),
        ({"date_of_birth": "1890-01-01T00:00:00Z"}, REJECTION_IMPLAUSIBLE_AGE),
        ({"date_of_birth": "2026-01-01T00:00:00Z"}, REJECTION_IMPLAUSIBLE_AGE),
        ({"registered_at": "1999-01-01T00:00:00Z"}, REJECTION_REGISTERED_BEFORE_DATE_OF_BIRTH),
        ({"registered_at": "2026-01-01T00:00:00Z"}, REJECTION_REGISTERED_IN_FUTURE),
        ({"nationality": "GBR"}, REJECTION_INVALID_NATIONALITY),
        ({"nationality": "gb"}, REJECTION_INVALID_NATIONALITY),
        ({"nationality": "ZZ"}, REJECTION_INVALID_NATIONALITY),
        ({"timezone_offset": "+15:00"}, REJECTION_INVALID_TIMEZONE_OFFSET),
        ({"timezone_offset": "+01:99"}, REJECTION_INVALID_TIMEZONE_OFFSET),
        ({"picture_large": "http://example.com/large.jpg"}, f"{REJECTION_INVALID_PICTURE_URL}:picture_large"),
        ({"picture_thumbnail": "http://example.com/thumb.jpg"}, f"{REJECTION_INVALID_PICTURE_URL}:picture_thumbnail"),
        ({"first_name": "a" * 513}, f"{REJECTION_TEXT_FIELD_TOO_LONG}:first_name"),
        ({"first_name": "A\nda"}, f"{REJECTION_TEXT_FIELD_HAS_CONTROL_CHARACTERS}:first_name"),
    ],
)
def test_validate_user_profile_quality_returns_explicit_rejection_reason(
    updates: dict[str, object],
    expected_reason: str,
) -> None:
    payload = _valid_user_payload()
    payload.update(updates)
    profile = UserCreated.model_validate(payload)

    reasons = validate_user_profile_quality(profile, today=date(2025, 1, 1))

    assert expected_reason in reasons


def test_validate_user_profile_quality_accepts_example_email_for_demo_source() -> None:
    payload = _valid_user_payload()
    payload.update({"email": "ada@example.com"})
    profile = UserCreated.model_validate(payload)

    assert validate_user_profile_quality(profile, today=date(2025, 1, 1)) == ()


def test_validate_user_profile_quality_accepts_non_random_user_country_names_with_iso_code() -> None:
    profile = _valid_user().model_copy(
        update={
            "source": "crm",
            "country": "Cote d Ivoire",
            "country_code": "CI",
        }
    )

    assert validate_user_profile_quality(profile, today=date(2025, 1, 1)) == ()


@pytest.mark.parametrize(
    "payload_updates, expected_reason",
    [
        ({"country": "Atlantis"}, REJECTION_UNSUPPORTED_COUNTRY),
        ({"date_of_birth": "1890-01-01T00:00:00Z"}, REJECTION_IMPLAUSIBLE_AGE),
    ],
)
def test_user_created_payload_validates_through_schema_then_quality(
    payload_updates: dict[str, str],
    expected_reason: str,
) -> None:
    payload = _valid_user_payload()
    payload.update(payload_updates)

    profile = UserCreated.model_validate(payload)
    reasons = validate_user_profile_quality(profile, today=date(2025, 1, 1))

    assert expected_reason in reasons


def test_user_created_payload_passes_schema_and_quality_when_valid() -> None:
    profile = UserCreated.model_validate(_valid_user_payload())

    assert validate_user_profile_quality(profile, today=date(2025, 1, 1)) == ()


def test_build_invalid_user_profile_event_keeps_only_safe_payload_fields() -> None:
    profile = _valid_user()

    invalid_event = build_invalid_user_profile_event(
        profile,
        ("invalid_email",),
        pseudonymization_salt="local-test-salt",
    )

    assert invalid_event.source_user_id != profile.source_user_id
    assert invalid_event.source_user_id == pseudonymize_source_user_id(
        source=profile.source,
        source_user_id=profile.source_user_id,
        salt="local-test-salt",
    )
    assert invalid_event.payload == {
        "schema_version": "1.0",
        "event_type": "UserCreated",
        "source": "random_user",
        "country_code": "GB",
    }


def test_build_invalid_user_profile_event_requires_salted_source_user_id_pseudonymization() -> None:
    profile = _valid_user()

    try:
        build_invalid_user_profile_event(profile, ("invalid_email",))
    except ValueError as exc:
        assert "salt" in str(exc)
    else:
        raise AssertionError("build_invalid_user_profile_event should require a pseudonymization salt")

    invalid_event = build_invalid_user_profile_event(
        profile,
        ("invalid_email",),
        pseudonymization_salt="local-test-salt",
    )

    assert invalid_event.source_user_id == pseudonymize_source_user_id(
        source=profile.source,
        source_user_id=profile.source_user_id,
        salt="local-test-salt",
    )


def _valid_user() -> UserCreated:
    return UserCreated.model_validate(_valid_user_payload())


def _valid_user_payload() -> dict[str, object]:
    return {
        "source": "random_user",
        "source_user_id": "2f4c4f6e-743b-4c8e-82df-35b2c789f35f",
        "gender": "female",
        "title": "Ms",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "street_number": 42,
        "street_name": "Analytical Engine Road",
        "city": "London",
        "state": "Greater London",
        "country": "United Kingdom",
        "country_code": "GB",
        "postcode": "SW1A 1AA",
        "latitude": "51.5072",
        "longitude": "-0.1276",
        "timezone_offset": "+0:00",
        "timezone_description": "London",
        "email": "ada.lovelace@randomuser.me",
        "username": "adal",
        "date_of_birth": "2000-01-01T00:00:00Z",
        "registered_at": "2024-01-01T12:00:00Z",
        "phone": "020 7946 0958",
        "cell": "07123 456789",
        "picture_large": "https://example.com/large.jpg",
        "picture_medium": "https://example.com/medium.jpg",
        "picture_thumbnail": "https://example.com/thumb.jpg",
        "nationality": "GB",
    }
