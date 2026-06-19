# Copyright (c) 2026 BeardedSheeep

import re
from collections.abc import Mapping
from functools import lru_cache
from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

LOG_LEVELS = frozenset({"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"})
KAFKA_TOPIC_PATTERN = re.compile(r"[A-Za-z0-9._-]+")
CASSANDRA_KEYSPACE_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,47}")
LogFormat = Literal["json", "text"]


class ApplicationSettings(BaseSettings):
    app_name: str = Field(default="realtimedatastreaming", alias="APP_NAME")
    environment: str = Field(default="development", alias="APP_ENV")
    debug: bool = Field(default=False, alias="APP_DEBUG")

    model_config = SettingsConfigDict(extra="ignore")


class LoggingSettings(BaseSettings):
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: LogFormat = Field(default="json", alias="LOG_FORMAT")

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


class ObservabilitySettings(BaseSettings):
    service_name: str = Field(default="realtimedatastreaming", alias="SERVICE_NAME")
    service_version: str = Field(default="0.1.0", alias="SERVICE_VERSION")
    otel_enabled: bool = Field(default=False, alias="OTEL_ENABLED")
    otel_exporter_otlp_endpoint: str | None = Field(default=None, alias="OTEL_EXPORTER_OTLP_ENDPOINT")
    otel_service_name: str = Field(default="realtimedatastreaming", alias="OTEL_SERVICE_NAME")
    sentry_dsn: SecretStr | None = Field(default=None, alias="SENTRY_DSN")
    sentry_environment: str = Field(default="development", alias="SENTRY_ENVIRONMENT")
    sentry_traces_sample_rate: float = Field(default=0.0, ge=0.0, le=1.0, alias="SENTRY_TRACES_SAMPLE_RATE")

    model_config = SettingsConfigDict(extra="ignore")

    @property
    def sentry_dsn_value(self) -> str | None:
        if self.sentry_dsn is None:
            return None
        return self.sentry_dsn.get_secret_value()

    @field_validator("sentry_dsn", mode="before")
    @classmethod
    def empty_sentry_dsn_as_none(cls, value: Any) -> Any:
        if value == "":
            return None
        return value


class PrivacySettings(BaseSettings):
    pii_pseudonymization_salt: SecretStr | None = Field(default=None, alias="PII_PSEUDONYMIZATION_SALT")

    model_config = SettingsConfigDict(extra="ignore")

    @property
    def pii_pseudonymization_salt_value(self) -> str | None:
        if self.pii_pseudonymization_salt is None:
            return None
        return self.pii_pseudonymization_salt.get_secret_value()

    @field_validator("pii_pseudonymization_salt", mode="before")
    @classmethod
    def empty_salt_as_none(cls, value: Any) -> Any:
        if value == "":
            return None
        return value


class RandomUserSettings(BaseSettings):
    random_user_api_url: AnyHttpUrl = Field(
        default=AnyHttpUrl("https://randomuser.me/api/"), alias="RANDOM_USER_API_URL"
    )
    random_user_http_timeout_seconds: float = Field(
        default=10.0,
        gt=0.0,
        alias="RANDOM_USER_HTTP_TIMEOUT_SECONDS",
    )

    model_config = SettingsConfigDict(extra="ignore")


class KafkaSettings(BaseSettings):
    kafka_bootstrap_servers: Annotated[tuple[str, ...], NoDecode] = Field(
        default=("localhost:9092",),
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    kafka_users_created_topic: str = Field(
        default="users_created",
        min_length=1,
        alias="KAFKA_USERS_CREATED_TOPIC",
    )
    kafka_users_created_valid_topic: str = Field(
        default="users_created_valid",
        min_length=1,
        alias="KAFKA_USERS_CREATED_VALID_TOPIC",
    )
    kafka_users_created_invalid_topic: str = Field(
        default="users_created_invalid",
        min_length=1,
        alias="KAFKA_USERS_CREATED_INVALID_TOPIC",
    )
    schema_registry_url: AnyHttpUrl = Field(
        default=AnyHttpUrl("http://localhost:8081"),
        alias="SCHEMA_REGISTRY_URL",
    )
    schema_registry_basic_auth_user_info: SecretStr | None = Field(
        default=None,
        alias="SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
    )
    schema_registry_ssl_ca_location: str | None = Field(default=None, alias="SCHEMA_REGISTRY_SSL_CA_LOCATION")
    kafka_sasl_username: str | None = Field(default=None, alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: SecretStr | None = Field(default=None, alias="KAFKA_SASL_PASSWORD")

    model_config = SettingsConfigDict(extra="ignore")

    @field_validator(
        "kafka_sasl_username",
        "kafka_sasl_password",
        "schema_registry_basic_auth_user_info",
        "schema_registry_ssl_ca_location",
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
            servers = tuple(server.strip() for server in value.split(","))
            if any(not server for server in servers):
                msg = "KAFKA_BOOTSTRAP_SERVERS cannot contain blank servers"
                raise ValueError(msg)
            return servers
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

    @field_validator(
        "kafka_users_created_topic",
        "kafka_users_created_valid_topic",
        "kafka_users_created_invalid_topic",
    )
    @classmethod
    def validate_kafka_topic_name(cls, value: str) -> str:
        if value in {".", ".."} or not KAFKA_TOPIC_PATTERN.fullmatch(value) or len(value) > 249:
            msg = (
                "Kafka topic names must be 1-249 characters and contain only letters, digits, dots, "
                "underscores, or hyphens"
            )
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def validate_sasl_credentials_are_complete(self) -> Self:
        if (self.kafka_sasl_username is None) != (self.kafka_sasl_password is None):
            msg = "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be configured together"
            raise ValueError(msg)
        return self


class SparkSettings(BaseSettings):
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
    spark_kafka_package: str = Field(
        default="org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
        min_length=1,
        alias="SPARK_KAFKA_PACKAGE",
    )

    model_config = SettingsConfigDict(extra="ignore")


class CassandraSettings(BaseSettings):
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

    model_config = SettingsConfigDict(extra="ignore")

    @field_validator("cassandra_username", "cassandra_password", mode="before")
    @classmethod
    def empty_optional_secret_values_as_none(cls, value: Any) -> Any:
        if value == "":
            return None
        return value

    @field_validator("cassandra_keyspace")
    @classmethod
    def validate_cassandra_keyspace(cls, value: str) -> str:
        if not CASSANDRA_KEYSPACE_PATTERN.fullmatch(value):
            msg = (
                "CASSANDRA_KEYSPACE must start with a letter and contain only letters, digits, "
                "or underscores, up to 48 characters"
            )
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def validate_credentials_are_complete(self) -> Self:
        if (self.cassandra_username is None) != (self.cassandra_password is None):
            msg = "CASSANDRA_USERNAME and CASSANDRA_PASSWORD must be configured together"
            raise ValueError(msg)
        return self


class Settings(BaseSettings):
    settings_groups: ClassVar[Mapping[str, type[BaseSettings]]] = {
        "application": ApplicationSettings,
        "logging": LoggingSettings,
        "observability": ObservabilitySettings,
        "privacy": PrivacySettings,
        "random_user": RandomUserSettings,
        "kafka": KafkaSettings,
        "spark": SparkSettings,
        "cassandra": CassandraSettings,
    }

    application: ApplicationSettings = Field(default_factory=ApplicationSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    privacy: PrivacySettings = Field(default_factory=PrivacySettings)
    random_user: RandomUserSettings = Field(default_factory=RandomUserSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    spark: SparkSettings = Field(default_factory=SparkSettings)
    cassandra: CassandraSettings = Field(default_factory=CassandraSettings)

    model_config = SettingsConfigDict(extra="ignore")

    def __init__(self, **data: Any) -> None:
        normalized_data = dict(data)
        for group_name, group_type in self.settings_groups.items():
            group_value = normalized_data.pop(group_name, None)
            group_data = group_value if isinstance(group_value, dict) else {}
            for field_name, field_info in group_type.model_fields.items():
                alias = field_info.alias
                if alias is not None and alias in normalized_data:
                    group_data[alias] = normalized_data.pop(alias)
                if field_name in normalized_data:
                    group_data[field_name] = normalized_data.pop(field_name)
            if group_data:
                normalized_data[group_name] = group_type(**group_data)
            elif group_value is not None:
                normalized_data[group_name] = group_value

        super().__init__(**normalized_data)

        if self.environment != "development" and self.privacy.pii_pseudonymization_salt is None:
            msg = "PII_PSEUDONYMIZATION_SALT must be configured outside development"
            raise ValueError(msg)

    @property
    def app_name(self) -> str:
        return self.application.app_name

    @property
    def environment(self) -> str:
        return self.application.environment

    @property
    def debug(self) -> bool:
        return self.application.debug

    @property
    def log_level(self) -> str:
        return self.logging.log_level

    @property
    def log_format(self) -> LogFormat:
        return self.logging.log_format

    @property
    def service_name(self) -> str:
        return self.observability.service_name

    @property
    def service_version(self) -> str:
        return self.observability.service_version

    @property
    def random_user_api_url(self) -> AnyHttpUrl:
        return self.random_user.random_user_api_url

    @property
    def random_user_http_timeout_seconds(self) -> float:
        return self.random_user.random_user_http_timeout_seconds

    @property
    def kafka_bootstrap_servers(self) -> tuple[str, ...]:
        return self.kafka.kafka_bootstrap_servers

    @property
    def kafka_users_created_topic(self) -> str:
        return self.kafka.kafka_users_created_topic

    @property
    def kafka_users_created_valid_topic(self) -> str:
        return self.kafka.kafka_users_created_valid_topic

    @property
    def kafka_users_created_invalid_topic(self) -> str:
        return self.kafka.kafka_users_created_invalid_topic

    @property
    def schema_registry_url(self) -> AnyHttpUrl:
        return self.kafka.schema_registry_url

    @property
    def schema_registry_basic_auth_user_info(self) -> SecretStr | None:
        return self.kafka.schema_registry_basic_auth_user_info

    @property
    def schema_registry_ssl_ca_location(self) -> str | None:
        return self.kafka.schema_registry_ssl_ca_location

    @property
    def kafka_sasl_username(self) -> str | None:
        return self.kafka.kafka_sasl_username

    @property
    def kafka_sasl_password(self) -> SecretStr | None:
        return self.kafka.kafka_sasl_password

    @property
    def spark_app_name(self) -> str:
        return self.spark.spark_app_name

    @property
    def spark_master_url(self) -> str:
        return self.spark.spark_master_url

    @property
    def spark_checkpoint_location(self) -> str:
        return self.spark.spark_checkpoint_location

    @property
    def spark_kafka_package(self) -> str:
        return self.spark.spark_kafka_package

    @property
    def cassandra_host(self) -> str:
        return self.cassandra.cassandra_host

    @property
    def cassandra_port(self) -> int:
        return self.cassandra.cassandra_port

    @property
    def cassandra_keyspace(self) -> str:
        return self.cassandra.cassandra_keyspace

    @property
    def cassandra_username(self) -> str | None:
        return self.cassandra.cassandra_username

    @property
    def cassandra_password(self) -> SecretStr | None:
        return self.cassandra.cassandra_password

    @property
    def otel_enabled(self) -> bool:
        return self.observability.otel_enabled

    @property
    def otel_exporter_otlp_endpoint(self) -> str | None:
        return self.observability.otel_exporter_otlp_endpoint

    @property
    def otel_service_name(self) -> str:
        return self.observability.otel_service_name

    @property
    def sentry_dsn(self) -> SecretStr | None:
        return self.observability.sentry_dsn

    @property
    def sentry_dsn_value(self) -> str | None:
        return self.observability.sentry_dsn_value

    @property
    def pii_pseudonymization_salt(self) -> SecretStr | None:
        return self.privacy.pii_pseudonymization_salt

    @property
    def pii_pseudonymization_salt_value(self) -> str | None:
        return self.privacy.pii_pseudonymization_salt_value

    @property
    def sentry_environment(self) -> str:
        return self.observability.sentry_environment

    @property
    def sentry_traces_sample_rate(self) -> float:
        return self.observability.sentry_traces_sample_rate


@lru_cache
def get_settings() -> Settings:
    """Return cached application settings.

    Tests that mutate environment variables should call
    ``get_settings.cache_clear()`` before reading settings again.
    """
    return Settings()
