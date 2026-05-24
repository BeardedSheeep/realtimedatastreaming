from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"})
LogFormat = Literal["json", "text"]


class Settings(BaseSettings):
    app_name: str = Field(default="template-doc", alias="APP_NAME")
    environment: str = Field(default="development", alias="APP_ENV")
    debug: bool = Field(default=False, alias="APP_DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: LogFormat = Field(default="json", alias="LOG_FORMAT")
    service_name: str = Field(default="template-doc", alias="SERVICE_NAME")
    service_version: str = Field(default="0.1.0", alias="SERVICE_VERSION")
    otel_enabled: bool = Field(default=False, alias="OTEL_ENABLED")
    otel_exporter_otlp_endpoint: str | None = Field(default=None, alias="OTEL_EXPORTER_OTLP_ENDPOINT")
    otel_service_name: str = Field(default="template-doc", alias="OTEL_SERVICE_NAME")
    sentry_dsn: str | None = Field(default=None, alias="SENTRY_DSN")
    sentry_environment: str = Field(default="development", alias="SENTRY_ENVIRONMENT")
    sentry_traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0, alias="SENTRY_TRACES_SAMPLE_RATE")

    model_config = SettingsConfigDict(extra="ignore")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized_value = value.upper()
        if normalized_value not in LOG_LEVELS:
            allowed_values = ", ".join(sorted(LOG_LEVELS))
            msg = f"LOG_LEVEL must be one of: {allowed_values}"
            raise ValueError(msg)
        return normalized_value


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings.

    Tests that mutate environment variables should call
    ``get_settings.cache_clear()`` before reading settings again.
    """
    return Settings()
