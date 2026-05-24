from typing import Any

import pytest
from pydantic import ValidationError

from realtimedatastreaming.settings import Settings, get_settings


def test_settings_defaults() -> None:
    settings = Settings()

    assert settings.app_name == "realtimedatastreaming"
    assert settings.environment == "development"
    assert settings.debug is False
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"
    assert settings.service_name == "realtimedatastreaming"
    assert settings.service_version == "0.1.0"
    assert str(settings.random_user_api_url) == "https://randomuser.me/api/"
    assert settings.random_user_http_timeout_seconds == 10.0
    assert settings.kafka_bootstrap_servers == ("localhost:9092",)
    assert settings.kafka_users_created_topic == "users_created"
    assert settings.kafka_users_created_invalid_topic == "users_created_invalid"
    assert settings.kafka_sasl_username is None
    assert settings.kafka_sasl_password is None
    assert settings.spark_app_name == "realtimedatastreaming"
    assert settings.spark_master_url == "local[*]"
    assert settings.spark_checkpoint_location == "/tmp/realtimedatastreaming/checkpoints"
    assert settings.cassandra_host == "localhost"
    assert settings.cassandra_port == 9042
    assert settings.cassandra_keyspace == "realtimedatastreaming"
    assert settings.cassandra_username is None
    assert settings.cassandra_password is None
    assert settings.otel_enabled is False
    assert settings.otel_exporter_otlp_endpoint is None
    assert settings.otel_service_name == "realtimedatastreaming"
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
    monkeypatch.setenv("RANDOM_USER_API_URL", "https://randomuser.example/api/")
    monkeypatch.setenv("RANDOM_USER_HTTP_TIMEOUT_SECONDS", "2.5")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-a:9092,kafka-b:9092")
    monkeypatch.setenv("KAFKA_USERS_CREATED_TOPIC", "custom_users_created")
    monkeypatch.setenv("KAFKA_USERS_CREATED_INVALID_TOPIC", "custom_users_created_invalid")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "kafka-user")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "kafka-password")
    monkeypatch.setenv("SPARK_APP_NAME", "custom-spark-app")
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    monkeypatch.setenv("SPARK_CHECKPOINT_LOCATION", "file:///tmp/custom-checkpoints")
    monkeypatch.setenv("CASSANDRA_HOST", "cassandra")
    monkeypatch.setenv("CASSANDRA_PORT", "9142")
    monkeypatch.setenv("CASSANDRA_KEYSPACE", "custom_keyspace")
    monkeypatch.setenv("CASSANDRA_USERNAME", "cassandra-user")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "cassandra-password")
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
    assert str(settings.random_user_api_url) == "https://randomuser.example/api/"
    assert settings.random_user_http_timeout_seconds == 2.5
    assert settings.kafka_bootstrap_servers == ("kafka-a:9092", "kafka-b:9092")
    assert settings.kafka_users_created_topic == "custom_users_created"
    assert settings.kafka_users_created_invalid_topic == "custom_users_created_invalid"
    assert settings.kafka_sasl_username == "kafka-user"
    assert settings.kafka_sasl_password is not None
    assert settings.kafka_sasl_password.get_secret_value() == "kafka-password"
    assert settings.spark_app_name == "custom-spark-app"
    assert settings.spark_master_url == "spark://spark-master:7077"
    assert settings.spark_checkpoint_location == "file:///tmp/custom-checkpoints"
    assert settings.cassandra_host == "cassandra"
    assert settings.cassandra_port == 9142
    assert settings.cassandra_keyspace == "custom_keyspace"
    assert settings.cassandra_username == "cassandra-user"
    assert settings.cassandra_password is not None
    assert settings.cassandra_password.get_secret_value() == "cassandra-password"
    assert settings.otel_enabled is True
    assert settings.otel_exporter_otlp_endpoint == "http://collector:4318"
    assert settings.otel_service_name == "otel-service"
    assert settings.sentry_dsn is not None
    assert settings.sentry_dsn.get_secret_value() == "https://example.invalid/1"
    assert settings.sentry_environment == "prod"
    assert settings.sentry_traces_sample_rate == 0.25


def test_random_user_settings_defaults() -> None:
    settings = Settings()

    assert str(settings.random_user_api_url) == "https://randomuser.me/api/"
    assert settings.random_user_http_timeout_seconds == 10.0


def test_random_user_settings_read_environment_overrides(monkeypatch: Any) -> None:
    monkeypatch.setenv("RANDOM_USER_API_URL", "https://randomuser.example/api/")
    monkeypatch.setenv("RANDOM_USER_HTTP_TIMEOUT_SECONDS", "1.75")

    settings = Settings()

    assert str(settings.random_user_api_url) == "https://randomuser.example/api/"
    assert settings.random_user_http_timeout_seconds == 1.75


def test_kafka_settings_defaults() -> None:
    settings = Settings()

    assert settings.kafka_bootstrap_servers == ("localhost:9092",)
    assert settings.kafka_users_created_topic == "users_created"
    assert settings.kafka_users_created_invalid_topic == "users_created_invalid"
    assert settings.kafka_sasl_username is None
    assert settings.kafka_sasl_password is None


def test_kafka_settings_read_environment_overrides(monkeypatch: Any) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-a:9092, kafka-b:9092")
    monkeypatch.setenv("KAFKA_USERS_CREATED_TOPIC", "events.users.created")
    monkeypatch.setenv("KAFKA_USERS_CREATED_INVALID_TOPIC", "events.users.created.invalid")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "stream-user")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "stream-password")

    settings = Settings()

    assert settings.kafka_bootstrap_servers == ("kafka-a:9092", "kafka-b:9092")
    assert settings.kafka_users_created_topic == "events.users.created"
    assert settings.kafka_users_created_invalid_topic == "events.users.created.invalid"
    assert settings.kafka_sasl_username == "stream-user"
    assert settings.kafka_sasl_password is not None
    assert settings.kafka_sasl_password.get_secret_value() == "stream-password"


def test_spark_settings_defaults() -> None:
    settings = Settings()

    assert settings.spark_app_name == "realtimedatastreaming"
    assert settings.spark_master_url == "local[*]"
    assert settings.spark_checkpoint_location == "/tmp/realtimedatastreaming/checkpoints"


def test_spark_settings_read_environment_overrides(monkeypatch: Any) -> None:
    monkeypatch.setenv("SPARK_APP_NAME", "streaming-quality-monitor")
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    monkeypatch.setenv("SPARK_CHECKPOINT_LOCATION", "s3a://quality-monitor/checkpoints")

    settings = Settings()

    assert settings.spark_app_name == "streaming-quality-monitor"
    assert settings.spark_master_url == "spark://spark-master:7077"
    assert settings.spark_checkpoint_location == "s3a://quality-monitor/checkpoints"


def test_cassandra_settings_defaults() -> None:
    settings = Settings()

    assert settings.cassandra_host == "localhost"
    assert settings.cassandra_port == 9042
    assert settings.cassandra_keyspace == "realtimedatastreaming"
    assert settings.cassandra_username is None
    assert settings.cassandra_password is None


def test_cassandra_settings_read_environment_overrides(monkeypatch: Any) -> None:
    monkeypatch.setenv("CASSANDRA_HOST", "cassandra")
    monkeypatch.setenv("CASSANDRA_PORT", "9142")
    monkeypatch.setenv("CASSANDRA_KEYSPACE", "profiles")
    monkeypatch.setenv("CASSANDRA_USERNAME", "profile-user")
    monkeypatch.setenv("CASSANDRA_PASSWORD", "profile-password")

    settings = Settings()

    assert settings.cassandra_host == "cassandra"
    assert settings.cassandra_port == 9142
    assert settings.cassandra_keyspace == "profiles"
    assert settings.cassandra_username == "profile-user"
    assert settings.cassandra_password is not None
    assert settings.cassandra_password.get_secret_value() == "profile-password"


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


def test_settings_reject_invalid_random_user_api_url(monkeypatch: Any) -> None:
    monkeypatch.setenv("RANDOM_USER_API_URL", "ftp://randomuser.example/api/")

    with pytest.raises(ValidationError):
        Settings()


def test_settings_reject_malformed_random_user_api_url(monkeypatch: Any) -> None:
    monkeypatch.setenv("RANDOM_USER_API_URL", "not-a-url")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("timeout", ["0", "-1"])
def test_settings_reject_non_positive_random_user_http_timeout(monkeypatch: Any, timeout: str) -> None:
    monkeypatch.setenv("RANDOM_USER_HTTP_TIMEOUT_SECONDS", timeout)

    with pytest.raises(ValidationError):
        Settings()


def test_settings_reject_invalid_random_user_http_timeout(monkeypatch: Any) -> None:
    monkeypatch.setenv("RANDOM_USER_HTTP_TIMEOUT_SECONDS", "not-a-float")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("bootstrap_servers", ["", " , "])
def test_settings_reject_empty_kafka_bootstrap_servers(monkeypatch: Any, bootstrap_servers: str) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers)

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("env_name", ["KAFKA_USERS_CREATED_TOPIC", "KAFKA_USERS_CREATED_INVALID_TOPIC"])
def test_settings_reject_empty_kafka_topic_names(monkeypatch: Any, env_name: str) -> None:
    monkeypatch.setenv(env_name, "")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("env_name", ["SPARK_APP_NAME", "SPARK_MASTER_URL", "SPARK_CHECKPOINT_LOCATION"])
def test_settings_reject_empty_spark_settings(monkeypatch: Any, env_name: str) -> None:
    monkeypatch.setenv(env_name, "")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("env_name", ["CASSANDRA_HOST", "CASSANDRA_KEYSPACE"])
def test_settings_reject_empty_cassandra_string_settings(monkeypatch: Any, env_name: str) -> None:
    monkeypatch.setenv(env_name, "")

    with pytest.raises(ValidationError):
        Settings()


def test_settings_reject_invalid_cassandra_port(monkeypatch: Any) -> None:
    monkeypatch.setenv("CASSANDRA_PORT", "not-a-port")

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("port", ["0", "65536"])
def test_settings_reject_out_of_range_cassandra_port(monkeypatch: Any, port: str) -> None:
    monkeypatch.setenv("CASSANDRA_PORT", port)

    with pytest.raises(ValidationError):
        Settings()


@pytest.mark.parametrize("sample_rate", ["-0.1", "1.1"])
def test_settings_reject_out_of_range_sentry_sample_rate(monkeypatch: Any, sample_rate: str) -> None:
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", sample_rate)

    with pytest.raises(ValidationError):
        Settings()
