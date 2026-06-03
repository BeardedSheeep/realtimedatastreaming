# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json
import logging
from typing import Any, cast

from pyspark.sql import DataFrame

from realtimedatastreaming.ingestion.schema_registry import USERS_CREATED_VALUE_CONTRACT
from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming.user_profiles import (
    IS_VALID_CONTRACT_COLUMN,
    SPARK_CHECKPOINT_LOCATION_SETTING,
    USER_PROFILE_COLUMNS,
    USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS,
    USER_PROFILES_BY_SOURCE_ID_TABLE,
    USER_PROFILES_REPLAY_POLICY,
    DummyUserProfileSink,
    UserProfilesBatchMonitoring,
    build_user_profiles_stream_config,
    kafka_source_options_for_monitoring,
    user_created_spark_schema,
    validate_event_payload_against_contract,
)


class CountingRecords:
    def __init__(self, count: int) -> None:
        self.count_value = count

    def count(self) -> int:
        return self.count_value


def test_user_created_spark_schema_matches_profile_contract_columns() -> None:
    schema = user_created_spark_schema()

    assert schema.fieldNames() == [
        "schema_version",
        "event_type",
        "source",
        "source_user_id",
        "gender",
        "title",
        "first_name",
        "last_name",
        "street_number",
        "street_name",
        "city",
        "state",
        "country",
        "country_code",
        "postcode",
        "latitude",
        "longitude",
        "timezone_offset",
        "timezone_description",
        "email",
        "username",
        "date_of_birth",
        "registered_at",
        "phone",
        "cell",
        "picture_large",
        "picture_medium",
        "picture_thumbnail",
        "nationality",
    ]
    assert {"source_user_id", "email", "processed_at", "kafka_offset"}.issubset(USER_PROFILE_COLUMNS)


def test_kafka_source_options_use_configured_topic_and_bootstrap_servers() -> None:
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS="kafka-a:9092,kafka-b:9092",
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
    )

    options = kafka_source_options_for_monitoring(settings)

    assert options["kafka.bootstrap.servers"] == "kafka-a:9092,kafka-b:9092"
    assert options["subscribe"] == "events.users.created"
    assert options["startingOffsets"] == "latest"
    assert options["failOnDataLoss"] == "false"


def test_replay_policy_uses_checkpoint_resume_and_manual_replay_defaults() -> None:
    assert USER_PROFILES_REPLAY_POLICY.checkpoint_setting == SPARK_CHECKPOINT_LOCATION_SETTING
    assert USER_PROFILES_REPLAY_POLICY.default_starting_offsets == "latest"
    assert USER_PROFILES_REPLAY_POLICY.replay_mode == "manual_checkpoint_replay"
    assert USER_PROFILES_REPLAY_POLICY.production_scheduling_gate == (
        "disabled_until_real_cassandra_sink_is_idempotent"
    )


def test_cassandra_idempotence_key_is_stable_for_replays() -> None:
    assert USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS == ("source", "source_user_id", "event_type")
    assert set(USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS).issubset(USER_PROFILE_COLUMNS)
    assert "processed_at" not in USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS
    assert "kafka_offset" not in USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS


def test_stream_config_uses_explicit_schema_registry_contract_for_users_created_topic() -> None:
    settings = Settings(
        SCHEMA_REGISTRY_URL="https://schema-registry.example",
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
        KAFKA_USERS_CREATED_INVALID_TOPIC="events.users.created.invalid",
    )

    stream_config = build_user_profiles_stream_config(settings)

    assert stream_config.kafka_source_options["subscribe"] == "events.users.created"
    assert stream_config.schema_registry_config == {"url": "https://schema-registry.example/"}
    assert stream_config.value_contract.subject == "events.users.created-value"
    assert stream_config.value_contract.schema_path == "users_created-value/v1.json"
    assert stream_config.value_contract.schema_type == "JSON"


def test_stream_config_includes_schema_registry_auth_and_tls() -> None:
    settings = Settings(
        SCHEMA_REGISTRY_URL="https://schema-registry.example",
        SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="api-key:api-secret",
        SCHEMA_REGISTRY_SSL_CA_LOCATION="/etc/ssl/schema-registry-ca.pem",
    )

    stream_config = build_user_profiles_stream_config(settings)

    assert stream_config.schema_registry_config == {
        "url": "https://schema-registry.example/",
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "api-key:api-secret",
        "ssl.ca.location": "/etc/ssl/schema-registry-ca.pem",
    }


def test_kafka_source_options_include_sasl_when_credentials_are_configured() -> None:
    settings = Settings(KAFKA_SASL_USERNAME="profile-user", KAFKA_SASL_PASSWORD="profile-password")

    options = kafka_source_options_for_monitoring(settings)

    assert options["kafka.security.protocol"] == "SASL_SSL"
    assert options["kafka.sasl.mechanism"] == "PLAIN"
    assert 'username="profile-user"' in options["kafka.sasl.jaas.config"]
    assert 'password="profile-password"' in options["kafka.sasl.jaas.config"]


def test_profile_columns_are_ready_for_queryable_cassandra_table() -> None:
    assert USER_PROFILES_BY_SOURCE_ID_TABLE == "user_profiles_by_source_id"
    assert IS_VALID_CONTRACT_COLUMN not in USER_PROFILE_COLUMNS
    assert USER_PROFILE_COLUMNS[:4] == ("source", "source_user_id", "schema_version", "event_type")
    assert USER_PROFILE_COLUMNS[-5:] == (
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "processed_at",
    )


def test_dummy_user_profile_sink_counts_prepared_records_without_cassandra_connection(caplog: Any) -> None:
    caplog.set_level(logging.INFO)
    sink = DummyUserProfileSink()
    settings = Settings(CASSANDRA_KEYSPACE="profiles")

    prepared_records = sink.write(cast("DataFrame", CountingRecords(3)), settings=settings)

    assert prepared_records == 3
    record = caplog.records[0]
    assert record.msg == "spark_user_profiles_ready_for_cassandra"
    assert record.prepared_cassandra_writes == 3
    assert record.cassandra_keyspace == "profiles"
    assert record.cassandra_table == USER_PROFILES_BY_SOURCE_ID_TABLE
    assert record.sink_type == "dummy"


def test_user_profiles_batch_monitoring_exposes_required_local_signals() -> None:
    monitoring = UserProfilesBatchMonitoring(
        batch_id=7,
        input_records=10,
        invalid_records=2,
        prepared_cassandra_writes=8,
        batch_duration_ms=123,
        sink_type="dummy",
    )

    assert monitoring.as_log_extra() == {
        "batch_id": 7,
        "input_records": 10,
        "invalid_records": 2,
        "prepared_cassandra_writes": 8,
        "batch_duration_ms": 123,
        "sink_type": "dummy",
    }


def test_event_payload_contract_validation_accepts_valid_users_created_event() -> None:
    assert validate_event_payload_against_contract(
        json.dumps(_valid_users_created_payload()),
        USERS_CREATED_VALUE_CONTRACT.schema_text(),
    )


def test_event_payload_contract_validation_rejects_extra_fields_before_storage() -> None:
    payload = _valid_users_created_payload()
    payload["unexpected"] = "not-in-contract"

    assert (
        validate_event_payload_against_contract(
            json.dumps(payload),
            USERS_CREATED_VALUE_CONTRACT.schema_text(),
        )
        is False
    )


def test_event_payload_contract_validation_rejects_invalid_formats_before_storage() -> None:
    payload = _valid_users_created_payload()
    payload["email"] = "ada.example.com"

    assert (
        validate_event_payload_against_contract(
            json.dumps(payload),
            USERS_CREATED_VALUE_CONTRACT.schema_text(),
        )
        is False
    )


def test_event_payload_contract_validation_rejects_malformed_json_before_storage() -> None:
    assert validate_event_payload_against_contract("{not-json", USERS_CREATED_VALUE_CONTRACT.schema_text()) is False


def _valid_users_created_payload() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "event_type": "UserCreated",
        "source": "random_user",
        "source_user_id": "api-user-1",
        "gender": "female",
        "title": "Ms",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "street_number": 42,
        "street_name": "Analytical Engine Road",
        "city": "London",
        "state": "Greater London",
        "country": "United Kingdom",
        "country_code": "GB",
        "postcode": "SW1A 1AA",
        "latitude": 51.5072,
        "longitude": -0.1276,
        "timezone_offset": "+0:00",
        "timezone_description": "London",
        "email": "ada@example.com",
        "username": "adal",
        "date_of_birth": "2000-01-01T00:00:00Z",
        "registered_at": "2024-01-01T12:00:00Z",
        "phone": "020 7946 0958",
        "cell": "07123 456789",
        "picture_large": "https://example.com/large.jpg",
        "picture_medium": "https://example.com/medium.jpg",
        "picture_thumbnail": "https://example.com/thumb.jpg",
        "nationality": "GB",
    }
