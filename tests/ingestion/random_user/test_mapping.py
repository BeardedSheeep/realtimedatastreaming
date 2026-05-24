from collections.abc import Callable
from typing import Any, cast

import pytest

from realtimedatastreaming.ingestion.random_user import RandomUserPayloadError, normalize_random_user_response


def test_normalize_random_user_response_maps_source_payload(
    random_user_payload_factory: Callable[..., dict[str, Any]],
) -> None:
    users = normalize_random_user_response(random_user_payload_factory())

    assert users == [
        {
            "source": "random_user",
            "source_user_id": "2f4c4f6e-743b-4c8e-82df-35b2c789f35f-1",
            "gender": "female",
            "title": "Ms",
            "first_name": "Ada",
            "last_name": "Lovelace",
            "street_number": 42,
            "street_name": "Analytical Engine Road",
            "city": "London",
            "state": "Greater London",
            "country": "United Kingdom",
            "postcode": "SW1A 1AA",
            "latitude": "51.5072",
            "longitude": "-0.1276",
            "timezone_offset": "+0:00",
            "timezone_description": "London",
            "email": "ada.lovelace@example.com",
            "username": "adal-1",
            "date_of_birth": "1815-12-10T00:00:00.000Z",
            "registered_at": "2024-01-01T12:00:00.000Z",
            "phone": "020 7946 0958",
            "cell": "07123 456789",
            "picture_large": "https://example.com/large.jpg",
            "picture_medium": "https://example.com/medium.jpg",
            "picture_thumbnail": "https://example.com/thumb.jpg",
            "nationality": "GB",
        }
    ]


def test_normalize_random_user_response_rejects_missing_required_field(
    random_user_payload_factory: Callable[..., dict[str, Any]],
) -> None:
    payload = random_user_payload_factory()
    del payload["results"][0]["email"]

    with pytest.raises(RandomUserPayloadError, match="email") as exc_info:
        normalize_random_user_response(payload)

    assert exc_info.value.reason == "invalid_payload"


@pytest.mark.parametrize(
    "field_path",
    [
        ("results", 0, "login", "uuid"),
        ("results", 0, "login", "username"),
        ("results", 0, "name", "first"),
        ("results", 0, "name", "last"),
        ("results", 0, "location", "country"),
        ("results", 0, "dob", "date"),
    ],
)
def test_normalize_random_user_response_rejects_missing_critical_fields(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    field_path: tuple[str | int, ...],
) -> None:
    payload = random_user_payload_factory()
    _delete_nested_value(payload, field_path)

    with pytest.raises(RandomUserPayloadError) as exc_info:
        normalize_random_user_response(payload)

    assert exc_info.value.reason == "invalid_payload"


def test_normalize_random_user_response_rejects_empty_results() -> None:
    with pytest.raises(RandomUserPayloadError) as exc_info:
        normalize_random_user_response({"results": [], "info": {"results": 1}})

    assert exc_info.value.reason == "empty_results"


@pytest.mark.parametrize(
    "field_path, expected_fields",
    [
        (
            ("results", 0, "location", "street"),
            {"street_number": None, "street_name": None},
        ),
        (
            ("results", 0, "location", "coordinates"),
            {"latitude": None, "longitude": None},
        ),
        (
            ("results", 0, "location", "timezone"),
            {"timezone_offset": None, "timezone_description": None},
        ),
        (
            ("results", 0, "registered"),
            {"registered_at": None},
        ),
        (
            ("results", 0, "picture"),
            {"picture_large": None, "picture_medium": None, "picture_thumbnail": None},
        ),
    ],
)
def test_normalize_random_user_response_allows_missing_optional_nested_objects(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    field_path: tuple[str | int, ...],
    expected_fields: dict[str, None],
) -> None:
    payload = random_user_payload_factory()
    _delete_nested_value(payload, field_path)

    users = normalize_random_user_response(payload)

    user = cast(dict[str, Any], users[0])
    for field_name, expected_value in expected_fields.items():
        assert user[field_name] is expected_value


@pytest.mark.parametrize(
    "field_path",
    [
        ("results", 0, "location", "street"),
        ("results", 0, "location", "coordinates"),
        ("results", 0, "location", "timezone"),
        ("results", 0, "registered"),
        ("results", 0, "picture"),
    ],
)
def test_normalize_random_user_response_rejects_invalid_optional_nested_objects(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    field_path: tuple[str | int, ...],
) -> None:
    payload = random_user_payload_factory()
    _set_nested_value(payload, field_path, "not-an-object")

    with pytest.raises(RandomUserPayloadError) as exc_info:
        normalize_random_user_response(payload)

    assert exc_info.value.reason == "invalid_payload"


def test_normalize_random_user_response_rejects_non_object_result() -> None:
    with pytest.raises(RandomUserPayloadError) as exc_info:
        normalize_random_user_response({"results": ["not-an-object"], "info": {"results": 1}})

    assert exc_info.value.reason == "invalid_payload"


@pytest.mark.parametrize("postcode", [48.8566, {"code": "SW1A 1AA"}])
def test_normalize_random_user_response_rejects_invalid_postcode_type(
    random_user_payload_factory: Callable[..., dict[str, Any]],
    postcode: object,
) -> None:
    payload = random_user_payload_factory()
    payload["results"][0]["location"]["postcode"] = postcode

    with pytest.raises(RandomUserPayloadError) as exc_info:
        normalize_random_user_response(payload)

    assert exc_info.value.reason == "invalid_payload"


def _delete_nested_value(payload: dict[str, Any], field_path: tuple[str | int, ...]) -> None:
    current: Any = payload
    for key in field_path[:-1]:
        current = current[key]
    del current[field_path[-1]]


def _set_nested_value(payload: dict[str, Any], field_path: tuple[str | int, ...], value: object) -> None:
    current: Any = payload
    for key in field_path[:-1]:
        current = current[key]
    current[field_path[-1]] = value
