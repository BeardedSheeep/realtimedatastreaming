from collections.abc import Callable
from typing import Any

import pytest


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleep_calls: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.now += seconds


RandomUserPayloadFactory = Callable[..., dict[str, Any]]


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def random_user_payload_factory() -> RandomUserPayloadFactory:
    return random_user_payload


def random_user_payload(*, results: int = 1) -> dict[str, Any]:
    user_profiles = []
    for index in range(1, results + 1):
        user_profiles.append(random_user_profile(index))

    return {
        "results": user_profiles,
        "info": {"results": results},
    }


def random_user_profile(index: int) -> dict[str, Any]:
    return {
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
            "uuid": f"2f4c4f6e-743b-4c8e-82df-35b2c789f35f-{index}",
            "username": f"adal-{index}",
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
