# Copyright (c) 2026 BeardedSheeep

from typing import Any

import pytest
from pydantic import ValidationError

from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid


def test_user_created_sets_version_and_type_defaults() -> None:
    user = UserCreated.model_validate(_valid_user_created_payload())

    assert user.schema_version == "1.0"
    assert user.event_type == "UserCreated"


def test_user_created_rejects_extra_fields() -> None:
    payload = _valid_user_created_payload()
    payload["unexpected"] = "not-in-contract"

    with pytest.raises(ValidationError):
        UserCreated.model_validate(payload)


@pytest.mark.parametrize(
    "payload_updates",
    [
        {"source": ""},
        {"source_user_id": ""},
        {"username": ""},
        {"email": ""},
        {"email": "ada.example.com"},
        {"country": ""},
        {"date_of_birth": ""},
        {"date_of_birth": "not-a-date"},
        {"registered_at": "not-a-date"},
        {"latitude": "91.0"},
        {"latitude": "north"},
        {"longitude": "181.0"},
        {"longitude": "east"},
        {"picture_large": "not-a-url"},
    ],
)
def test_user_created_rejects_structurally_invalid_payloads(payload_updates: dict[str, str]) -> None:
    payload = _valid_user_created_payload()
    payload.update(payload_updates)

    with pytest.raises(ValidationError):
        UserCreated.model_validate(payload)


def test_user_created_strips_whitespace_from_string_fields() -> None:
    payload = _valid_user_created_payload()
    payload["source"] = " random_user "
    payload["email"] = " ada@example.com "

    user = UserCreated.model_validate(payload)

    assert user.source == "random_user"
    assert user.email == "ada@example.com"


def test_user_created_normalizes_structured_fields() -> None:
    user = UserCreated.model_validate(_valid_user_created_payload())

    assert user.latitude == 51.5072
    assert user.longitude == -0.1276
    assert user.date_of_birth.year == 2000
    assert user.registered_at is not None
    assert user.registered_at.year == 2024
    assert str(user.picture_large) == "https://example.com/large.jpg"


def test_user_created_is_frozen() -> None:
    user = UserCreated.model_validate(_valid_user_created_payload())

    with pytest.raises(ValidationError):
        user.email = "new@example.com"


def test_user_profile_invalid_requires_rejection_reasons() -> None:
    with pytest.raises(ValidationError):
        UserProfileInvalid(rejection_reasons=())


def test_user_profile_invalid_sets_version_and_type_defaults() -> None:
    event = UserProfileInvalid(
        source="random_user",
        source_user_id="api-user-1",
        rejection_reasons=("invalid_email",),
        payload={"email": "<redacted>"},
    )

    assert event.schema_version == "1.0"
    assert event.event_type == "UserProfileInvalid"


def _valid_user_created_payload() -> dict[str, Any]:
    return {
        "source": "random_user",
        "source_user_id": "api-user-1",
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
        "email": "ada@example.com",
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
