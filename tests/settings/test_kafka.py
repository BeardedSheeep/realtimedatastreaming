from typing import Any

import pytest
from pydantic import ValidationError

from realtimedatastreaming.settings import Settings


@pytest.mark.parametrize(
    ("username", "password"),
    [
        ("stream-user", None),
        (None, "stream-password"),
    ],
)
def test_kafka_settings_reject_partial_sasl_credentials(
    monkeypatch: Any, username: str | None, password: str | None
) -> None:
    if username is not None:
        monkeypatch.setenv("KAFKA_SASL_USERNAME", username)
    if password is not None:
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", password)

    with pytest.raises(ValidationError, match="KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD"):
        Settings()


@pytest.mark.parametrize(
    "bootstrap_servers",
    [
        "",
        " , ",
        "broker-a:9092,,broker-b:9092",
        ",broker-a:9092",
        "broker-a:9092,",
    ],
)
def test_settings_reject_empty_kafka_bootstrap_servers(monkeypatch: Any, bootstrap_servers: str) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers)

    with pytest.raises(ValidationError, match="KAFKA_BOOTSTRAP_SERVERS"):
        Settings()


@pytest.mark.parametrize("topic", ["", "events users", "events/users", ".", "..", "a" * 250])
@pytest.mark.parametrize("env_name", ["KAFKA_USERS_CREATED_TOPIC", "KAFKA_USERS_CREATED_INVALID_TOPIC"])
def test_settings_reject_invalid_kafka_topic_names(monkeypatch: Any, env_name: str, topic: str) -> None:
    monkeypatch.setenv(env_name, topic)

    with pytest.raises(ValidationError):
        Settings()
