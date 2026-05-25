from typing import Any

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
    assert str(settings.schema_registry_url) == "http://localhost:8081/"
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
    assert settings.pii_pseudonymization_salt is None
    assert settings.pii_pseudonymization_salt_value is None
    assert settings.sentry_environment == "development"
    assert settings.sentry_traces_sample_rate == 0.0


def test_settings_exposes_domain_specific_config_groups() -> None:
    settings = Settings()

    assert settings.application.app_name == settings.app_name
    assert settings.logging.log_level == settings.log_level
    assert settings.observability.service_name == settings.service_name
    assert settings.random_user.random_user_api_url == settings.random_user_api_url
    assert settings.kafka.kafka_bootstrap_servers == settings.kafka_bootstrap_servers
    assert settings.spark.spark_master_url == settings.spark_master_url
    assert settings.cassandra.cassandra_host == settings.cassandra_host


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
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-a:9092, kafka-b:9092")
    monkeypatch.setenv("KAFKA_USERS_CREATED_TOPIC", "events.users.created")
    monkeypatch.setenv("KAFKA_USERS_CREATED_INVALID_TOPIC", "events.users.created.invalid")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
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
    monkeypatch.setenv("PII_PSEUDONYMIZATION_SALT", "production-salt")
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
    assert settings.kafka_users_created_topic == "events.users.created"
    assert settings.kafka_users_created_invalid_topic == "events.users.created.invalid"
    assert str(settings.schema_registry_url) == "http://schema-registry:8081/"
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
    assert settings.pii_pseudonymization_salt is not None
    assert settings.pii_pseudonymization_salt_value == "production-salt"
    assert settings.sentry_dsn.get_secret_value() == "https://example.invalid/1"
    assert settings.sentry_dsn_value == "https://example.invalid/1"
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


def test_settings_require_pii_pseudonymization_salt_outside_development(monkeypatch: Any) -> None:
    monkeypatch.setenv("APP_ENV", "production")

    try:
        Settings()
    except ValueError as exc:
        assert "PII_PSEUDONYMIZATION_SALT" in str(exc)
    else:
        raise AssertionError("Settings should reject production without PII_PSEUDONYMIZATION_SALT")
