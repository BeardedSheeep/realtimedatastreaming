from typing import Any

import pytest
from pydantic import ValidationError

from template_doc.settings import Settings, get_settings


def test_settings_defaults() -> None:
    settings = Settings()

    assert settings.app_name == "template-doc"
    assert settings.environment == "development"
    assert settings.debug is False
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"
    assert settings.service_name == "template-doc"
    assert settings.service_version == "0.1.0"
    assert settings.otel_enabled is False
    assert settings.otel_exporter_otlp_endpoint is None
    assert settings.otel_service_name == "template-doc"
    assert settings.sentry_dsn is None
    assert settings.sentry_environment == "development"
    assert settings.sentry_traces_sample_rate == 0.0


def test_settings_read_environment_overrides(monkeypatch: Any) -> None:
    monkeypatch.setenv("APP_NAME", "custom-app")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("APP_DEBUG", "true")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "text")
    monkeypatch.setenv("SERVICE_NAME", "custom-service")
    monkeypatch.setenv("SERVICE_VERSION", "1.2.3")
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "otel-service")
    monkeypatch.setenv("SENTRY_DSN", "https://example.invalid/1")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", "prod")
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.25")

    settings = Settings()

    assert settings.app_name == "custom-app"
    assert settings.environment == "production"
    assert settings.debug is True
    assert settings.log_level == "DEBUG"
    assert settings.log_format == "text"
    assert settings.service_name == "custom-service"
    assert settings.service_version == "1.2.3"
    assert settings.otel_enabled is True
    assert settings.otel_exporter_otlp_endpoint == "http://collector:4318"
    assert settings.otel_service_name == "otel-service"
    assert settings.sentry_dsn == "https://example.invalid/1"
    assert settings.sentry_environment == "prod"
    assert settings.sentry_traces_sample_rate == 0.25


def test_get_settings_cache_can_be_cleared(monkeypatch: Any) -> None:
    get_settings.cache_clear()
    monkeypatch.setenv("APP_NAME", "first")
    assert get_settings().app_name == "first"

    monkeypatch.setenv("APP_NAME", "second")
    assert get_settings().app_name == "first"

    get_settings.cache_clear()
    assert get_settings().app_name == "second"
    get_settings.cache_clear()


def test_settings_reject_invalid_float(monkeypatch: Any) -> None:
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "not-a-float")

    with pytest.raises(ValidationError):
        Settings()


def test_settings_reject_invalid_log_level(monkeypatch: Any) -> None:
    monkeypatch.setenv("LOG_LEVEL", "DEBG")

    with pytest.raises(ValidationError):
        Settings()


def test_settings_reject_invalid_log_format(monkeypatch: Any) -> None:
    monkeypatch.setenv("LOG_FORMAT", "xml")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("sample_rate", ["-0.1", "1.1"])
def test_settings_reject_out_of_range_sentry_sample_rate(monkeypatch: Any, sample_rate: str) -> None:
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", sample_rate)

    with pytest.raises(ValidationError):
        Settings()
