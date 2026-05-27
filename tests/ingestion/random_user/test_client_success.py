# Copyright (c) 2026 BeardedSheeep

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from realtimedatastreaming.ingestion.random_user import RandomUserClient


def test_random_user_client_fetches_multiple_users(
    random_user_payload_factory: Callable[..., dict[str, Any]],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["results"] == "2"
        return httpx.Response(200, json=random_user_payload_factory(results=2))

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
    )

    users = client.fetch_users(count=2)

    assert [user["username"] for user in users] == ["adal-1", "adal-2"]


def test_random_user_client_applies_timeout_to_injected_http_client(
    random_user_payload_factory: Callable[..., dict[str, Any]],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.extensions["timeout"] == {
            "connect": 2.5,
            "read": 2.5,
            "write": 2.5,
            "pool": 2.5,
        }
        return httpx.Response(200, json=random_user_payload_factory())

    http_client = httpx.Client(transport=httpx.MockTransport(handler))
    client = RandomUserClient(
        api_url="https://randomuser.example/api/",
        timeout_seconds=2.5,
        http_client=http_client,
    )

    users = client.fetch_users()

    assert users[0]["username"] == "adal-1"


def test_random_user_client_rejects_invalid_count() -> None:
    client = RandomUserClient(api_url="https://randomuser.example/api/", timeout_seconds=2.5)

    with pytest.raises(ValueError, match="count"):
        client.fetch_users(count=0)
