from typing import Any

from realtimedatastreaming.settings import Settings


def test_sentry_dsn_value_exposes_raw_secret_only_when_configured(monkeypatch: Any) -> None:
    monkeypatch.setenv("SENTRY_DSN", "https://public@example.invalid/1")

    settings = Settings()

    assert settings.sentry_dsn is not None
    assert str(settings.sentry_dsn) == "**********"
    assert settings.sentry_dsn_value == "https://public@example.invalid/1"


def test_secret_settings_treat_empty_environment_values_as_missing(monkeypatch: Any) -> None:
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "")
    monkeypatch.setenv("CASSANDRA_USERNAME", "")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "")
    monkeypatch.setenv("SENTRY_DSN", "")

    settings = Settings()

    assert settings.kafka_sasl_username is None
    assert settings.kafka_sasl_password is None
    assert settings.cassandra_username is None
    assert settings.cassandra_password is None
    assert settings.sentry_dsn is None
    assert settings.sentry_dsn_value is None
