from typing import Any

import pytest
from pydantic import ValidationError

from realtimedatastreaming.settings import Settings


@pytest.mark.parametrize(
    ("username", "password"),
    [
        ("profile-user", None),
        (None, "profile-password"),
    ],
)
def test_cassandra_settings_reject_partial_credentials(
    monkeypatch: Any, username: str | None, password: str | None
) -> None:
    if username is not None:
        monkeypatch.setenv("CASSANDRA_USERNAME", username)
    if password is not None:
        monkeypatch.setenv("CASSANDRA_PASSWORD", password)

    with pytest.raises(ValidationError, match="CASSANDRA_USERNAME and CASSANDRA_PASSWORD"):
        Settings()


@pytest.mark.parametrize("keyspace", ["", "1profiles", "profile-keyspace", "profiles.schema", "a" * 49])
def test_settings_reject_invalid_cassandra_keyspace(monkeypatch: Any, keyspace: str) -> None:
    monkeypatch.setenv("CASSANDRA_KEYSPACE", keyspace)

    with pytest.raises(ValidationError, match="CASSANDRA_KEYSPACE"):
        Settings()
