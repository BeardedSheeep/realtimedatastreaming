# Copyright (c) 2026 BeardedSheeep

import json
from typing import Any, cast

from confluent_kafka.schema_registry import Schema

from realtimedatastreaming.ingestion.schema_registry import (
    KAFKA_VALUE_CONTRACTS,
    SCHEMA_REGISTRY_COMPATIBILITY,
    USERS_CREATED_INVALID_VALUE_CONTRACT,
    USERS_CREATED_INVALID_VALUE_SUBJECT,
    USERS_CREATED_VALUE_CONTRACT,
    USERS_CREATED_VALUE_SUBJECT,
    kafka_value_contracts_for_topics,
    register_kafka_value_contracts,
    schema_registry_config_from_settings,
    value_subject_for_topic,
)
from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid
from realtimedatastreaming.settings import Settings


def test_schema_registry_uses_topic_value_subject_naming() -> None:
    assert value_subject_for_topic("users_created") == USERS_CREATED_VALUE_SUBJECT
    assert value_subject_for_topic("users_created_invalid") == USERS_CREATED_INVALID_VALUE_SUBJECT


def test_schema_registry_contracts_can_be_derived_from_configured_topics() -> None:
    contracts = kafka_value_contracts_for_topics(
        users_created_topic="events.users.created",
        users_created_invalid_topic="events.users.created.invalid",
    )

    assert [contract.subject for contract in contracts] == [
        "events.users.created-value",
        "events.users.created.invalid-value",
    ]
    assert [contract.schema_path for contract in contracts] == [
        USERS_CREATED_VALUE_CONTRACT.schema_path,
        USERS_CREATED_INVALID_VALUE_CONTRACT.schema_path,
    ]


def test_schema_registry_contracts_are_packaged_and_versioned_json_schema() -> None:
    for contract in KAFKA_VALUE_CONTRACTS:
        schema = _load_contract_schema(contract.schema_text())

        assert contract.schema_type == "JSON"
        assert contract.version == 1
        assert contract.compatibility == SCHEMA_REGISTRY_COMPATIBILITY
        assert contract.schema_path.endswith("/v1.json")
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False


def test_users_created_contract_matches_current_event_fields() -> None:
    schema = _load_contract_schema(USERS_CREATED_VALUE_CONTRACT.schema_text())

    assert set(schema["properties"]) == set(UserCreated.model_fields)
    assert schema["title"] == "UserCreated"
    assert schema["properties"]["schema_version"] == {"const": "1.0"}
    assert schema["properties"]["event_type"] == {"const": "UserCreated"}
    assert set(schema["required"]) == {
        "schema_version",
        "event_type",
        "source",
        "source_user_id",
        "first_name",
        "last_name",
        "country",
        "email",
        "username",
        "date_of_birth",
    }


def test_users_created_invalid_contract_matches_current_event_fields() -> None:
    schema = _load_contract_schema(USERS_CREATED_INVALID_VALUE_CONTRACT.schema_text())

    assert set(schema["properties"]) == set(UserProfileInvalid.model_fields)
    assert schema["title"] == "UserProfileInvalid"
    assert schema["properties"]["schema_version"] == {"const": "1.0"}
    assert schema["properties"]["event_type"] == {"const": "UserProfileInvalid"}
    assert schema["properties"]["source_user_id"]["pattern"] == "^sha256:[0-9a-f]{64}$"
    assert set(schema["required"]) == {
        "schema_version",
        "event_type",
        "rejection_reasons",
    }


def test_register_kafka_value_contracts_uses_confluent_schema_registry_client() -> None:
    schema_registry_client = _FakeSchemaRegistryClient()

    registered_schema_ids = register_kafka_value_contracts(
        "http://schema-registry:8081",
        schema_registry_client=schema_registry_client,
    )

    assert set(registered_schema_ids) == {
        USERS_CREATED_VALUE_SUBJECT,
        USERS_CREATED_INVALID_VALUE_SUBJECT,
    }
    assert schema_registry_client.compatibility_updates == [
        (USERS_CREATED_VALUE_SUBJECT, SCHEMA_REGISTRY_COMPATIBILITY),
        (USERS_CREATED_INVALID_VALUE_SUBJECT, SCHEMA_REGISTRY_COMPATIBILITY),
    ]
    assert [subject for subject, _ in schema_registry_client.registered_schemas] == [
        USERS_CREATED_VALUE_SUBJECT,
        USERS_CREATED_INVALID_VALUE_SUBJECT,
    ]
    first_registered_schema = schema_registry_client.registered_schemas[0][1]
    assert isinstance(first_registered_schema, Schema)
    assert first_registered_schema.schema_type == "JSON"
    assert first_registered_schema.schema_str is not None
    assert _load_contract_schema(first_registered_schema.schema_str)["title"] == "UserCreated"


def test_register_kafka_value_contracts_derives_subjects_from_configured_topics() -> None:
    schema_registry_client = _FakeSchemaRegistryClient()

    registered_schema_ids = register_kafka_value_contracts(
        "http://schema-registry:8081",
        schema_registry_client=schema_registry_client,
        users_created_topic="events.users.created",
        users_created_invalid_topic="events.users.created.invalid",
    )

    assert set(registered_schema_ids) == {
        "events.users.created-value",
        "events.users.created.invalid-value",
    }
    assert [subject for subject, _ in schema_registry_client.registered_schemas] == [
        "events.users.created-value",
        "events.users.created.invalid-value",
    ]


def test_schema_registry_config_from_settings_adds_optional_auth_and_tls() -> None:
    settings = Settings(
        SCHEMA_REGISTRY_URL="https://schema-registry.example",
        SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="api-key:api-secret",
        SCHEMA_REGISTRY_SSL_CA_LOCATION="/etc/ssl/schema-registry-ca.pem",
    )

    config = schema_registry_config_from_settings(settings)

    assert config == {
        "url": "https://schema-registry.example/",
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "api-key:api-secret",
        "ssl.ca.location": "/etc/ssl/schema-registry-ca.pem",
    }


def _load_contract_schema(schema_text: str) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(schema_text))


class _FakeSchemaRegistryClient:
    def __init__(self) -> None:
        self.compatibility_updates: list[tuple[str | None, str | None]] = []
        self.registered_schemas: list[tuple[str, Schema]] = []

    def set_compatibility(self, subject_name: str | None = None, level: str | None = None) -> str:
        self.compatibility_updates.append((subject_name, level))
        return level or ""

    def register_schema(self, subject_name: str, schema: Schema, normalize_schemas: bool = False) -> int:
        self.registered_schemas.append((subject_name, schema))
        return len(self.registered_schemas)
