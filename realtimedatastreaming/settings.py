from functools import lru_cache
from typing import Annotated, Any, Literal

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"})
LogFormat = Literal["json", "text"]


class Settings(BaseSettings):
    app_name: str = Field(default="realtimedatastreaming", alias="APP_NAME")
    environment: str = Field(default="development", alias="APP_ENV")
    debug: bool = Field(default=False, alias="APP_DEBUG")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: LogFormat = Field(default="json", alias="LOG_FORMAT")
    service_name: str = Field(default="realtimedatastreaming", alias="SERVICE_NAME")
    service_version: str = Field(default="0.1.0", alias="SERVICE_VERSION")
    random_user_api_url: AnyHttpUrl = Field(
        default=AnyHttpUrl("https://randomuser.me/api/"), alias="RANDOM_USER_API_URL"
    )
    random_user_http_timeout_seconds: float = Field(
        default=10.0,
        gt=0.0,
        alias="RANDOM_USER_HTTP_TIMEOUT_SECONDS",
    )
    kafka_bootstrap_servers: Annotated[tuple[str, ...], NoDecode] = Field(
        default=("localhost:9092",),
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_users_created_topic: str = Field(
        default="users_created",
        min_length=1,
        alias="KAFKA_USERS_CREATED_TOPIC",
    )
    kafka_users_created_invalid_topic: str = Field(
        default="users_created_invalid",
        min_length=1,
        alias="KAFKA_USERS_CREATED_INVALID_TOPIC",
    )
    kafka_sasl_username: str | None = Field(default=None, alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: SecretStr | None = Field(default=None, alias="KAFKA_SASL_PASSWORD")
    spark_app_name: str = Field(
        default="realtimedatastreaming",
        min_length=1,
        alias="SPARK_APP_NAME",
    )
    spark_master_url: str = Field(
        default="local[*]",
        min_length=1,
        alias="SPARK_MASTER_URL",
    )
    spark_checkpoint_location: str = Field(
        default="/tmp/realtimedatastreaming/checkpoints",
        min_length=1,
        alias="SPARK_CHECKPOINT_LOCATION",
    )
    cassandra_host: str = Field(
        default="localhost",
        min_length=1,
        alias="CASSANDRA_HOST",
    )
    cassandra_port: int = Field(
        default=9042,
        ge=1,
        le=65535,
        alias="CASSANDRA_PORT",
    )
    cassandra_keyspace: str = Field(
        default="realtimedatastreaming",
        min_length=1,
        alias="CASSANDRA_KEYSPACE",
    )
    cassandra_username: str | None = Field(default=None, alias="CASSANDRA_USERNAME")
    cassandra_password: SecretStr | None = Field(default=None, alias="CASSANDRA_PASSWORD")
    otel_enabled: bool = Field(default=False, alias="OTEL_ENABLED")
    otel_exporter_otlp_endpoint: str | None = Field(default=None, alias="OTEL_EXPORTER_OTLP_ENDPOINT")
    otel_service_name: str = Field(default="realtimedatastreaming", alias="OTEL_SERVICE_NAME")
    sentry_dsn: SecretStr | None = Field(default=None, alias="SENTRY_DSN")
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

    @field_validator(
        "kafka_sasl_username",
        "kafka_sasl_password",
        "cassandra_username",
        "cassandra_password",
        "sentry_dsn",
        mode="before",
    )
    @classmethod
    def empty_optional_secret_values_as_none(cls, value: Any) -> Any:
        if value == "":
            return None
        return value

    @field_validator("kafka_bootstrap_servers", mode="before")
    @classmethod
    def split_kafka_bootstrap_servers(cls, value: Any) -> Any:
        if isinstance(value, str):
            return tuple(server.strip() for server in value.split(",") if server.strip())
        return value

    @field_validator("kafka_bootstrap_servers")
    @classmethod
    def validate_kafka_bootstrap_servers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            msg = "KAFKA_BOOTSTRAP_SERVERS must contain at least one server"
            raise ValueError(msg)
        if any(not server.strip() for server in value):
            msg = "KAFKA_BOOTSTRAP_SERVERS cannot contain blank servers"
            raise ValueError(msg)
        return value


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings.

    Tests that mutate environment variables should call
    ``get_settings.cache_clear()`` before reading settings again.
    """
    return Settings()
