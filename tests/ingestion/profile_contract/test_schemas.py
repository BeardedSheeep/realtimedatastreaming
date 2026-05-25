from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from realtimedatastreaming.ingestion.random_user import RandomUserClient, RandomUserPayloadError
from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid


def test_user_created_counts_valid_and_corrupted_api_profiles() -> None:
    corrupted_request_numbers = {3, 7, 11, 14}
    request_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal request_count

        request_count += 1
        payload = _random_user_payload()
        profile = payload["results"][0]
        profile["login"]["uuid"] = f"api-user-{request_count}"
        profile["login"]["username"] = f"api-user-{request_count}"

        if request_count in corrupted_request_numbers:
            del profile["email"]

        return httpx.Response(200, json=payload)

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
        max_retries=0,
    )

    valid_count = 0
    invalid_count = 0

    for _ in range(15):
        try:
            users = client.fetch_users()
            UserCreated.model_validate(users[0])
            valid_count += 1
        except (RandomUserPayloadError, ValidationError):
            invalid_count += 1

    assert request_count == 15
    assert len(corrupted_request_numbers) == 4
    assert valid_count == 11
    assert invalid_count == 4


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


def _random_user_payload() -> dict[str, Any]:
    return {
        "results": [
            {
                "gender": "female",
                "name": {"title": "Ms", "first": "Ada", "last": "Lovelace"},
                "location": {
                    "street": {"number": 42, "name": "Analytical Engine Road"},
                    "city": "London",
                    "state": "Greater London",
                    "country": "United Kingdom",
                    "postcode": "SW1A 1AA",
                    "coordinates": {"latitude": "51.5072", "longitude": "-0.1276"},
                    "timezone": {"offset": "+0:00", "description": "London"},
                },
                "email": "ada.lovelace@example.com",
                "login": {
                    "uuid": "2f4c4f6e-743b-4c8e-82df-35b2c789f35f",
                    "username": "adal",
                },
                "dob": {"date": "1815-12-10T00:00:00.000Z", "age": 208},
                "registered": {"date": "2024-01-01T12:00:00.000Z", "age": 1},
                "phone": "020 7946 0958",
                "cell": "07123 456789",
                "picture": {
                    "large": "https://example.com/large.jpg",
                    "medium": "https://example.com/medium.jpg",
                    "thumbnail": "https://example.com/thumb.jpg",
                },
                "nat": "GB",
            }
        ],
        "info": {"results": 1},
    }
