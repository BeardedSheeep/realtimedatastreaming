# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json

import pytest
from confluent_kafka.schema_registry import RegisteredSchema, Schema

from realtimedatastreaming.ingestion.schema_registry import KafkaJsonSchemaContract, kafka_value_contracts_for_topics
from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming.user_created_deserializer import (
    deserialize_user_created_frame,
    resolve_supported_users_created_schema_id,
)


def test_resolve_supported_schema_id_matches_configured_topic_contract() -> None:
    settings = Settings(KAFKA_USERS_CREATED_TOPIC="events.users.created")
    client = FakeSchemaRegistryClient(schema_id=42, version=1)

    schema_id = resolve_supported_users_created_schema_id(settings, schema_registry_client=client)

    assert schema_id == 42
    assert client.subject == "events.users.created-value"
    assert client.schema is not None
    assert client.schema.schema_type == "JSON"


def test_resolve_supported_schema_id_rejects_unexpected_contract_version() -> None:
    client = FakeSchemaRegistryClient(schema_id=42, version=2)

    with pytest.raises(RuntimeError, match="supported contract version 1"):
        resolve_supported_users_created_schema_id(Settings(), schema_registry_client=client)


@pytest.mark.parametrize(
    ("framed_value", "reason", "schema_id"),
    [
        (None, "invalid_confluent_frame", None),
        (b"\x00\x00", "invalid_confluent_frame", None),
        (b"\x01\x00\x00\x00\x2a{}", "invalid_confluent_magic_byte", 42),
        (b"\x00\x00\x00\x00\x29{}", "unsupported_schema_id", 41),
        (b"\x00\x00\x00\x00\x2a\xff", "invalid_json_payload", 42),
        (b"\x00\x00\x00\x00\x2a{}", "schema_validation_failed", 42),
    ],
)
def test_deserialize_user_created_frame_rejects_invalid_values(
    framed_value: bytes | None,
    reason: str,
    schema_id: int | None,
) -> None:
    contract = _users_created_contract()

    result = deserialize_user_created_frame(framed_value, supported_schema_id=42, contract=contract)

    assert result.payload_json is None
    assert result.rejection_reason == reason
    assert result.schema_id == schema_id


def test_deserialize_user_created_frame_returns_validated_json(user_created_payload: dict[str, object]) -> None:
    contract = _users_created_contract()
    frame = b"\x00" + (42).to_bytes(4, byteorder="big") + json.dumps(user_created_payload).encode()

    result = deserialize_user_created_frame(frame, supported_schema_id=42, contract=contract)

    assert result.rejection_reason is None
    assert result.schema_id == 42
    assert result.payload_json is not None
    assert json.loads(result.payload_json)["event_type"] == "UserCreated"


@pytest.fixture
def user_created_payload() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "event_type": "UserCreated",
        "source": "random_user",
        "source_user_id": "source-42",
        "gender": None,
        "title": None,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "street_number": None,
        "street_name": None,
        "city": None,
        "state": None,
        "country": "United Kingdom",
        "country_code": "GB",
        "postcode": None,
        "latitude": None,
        "longitude": None,
        "timezone_offset": None,
        "timezone_description": None,
        "email": "ada@example.com",
        "username": "ada",
        "date_of_birth": "2000-01-01T00:00:00Z",
        "registered_at": None,
        "phone": None,
        "cell": None,
        "picture_large": None,
        "picture_medium": None,
        "picture_thumbnail": None,
        "nationality": None,
    }


class FakeSchemaRegistryClient:
    def __init__(self, *, schema_id: int | None, version: int | None) -> None:
        self.schema_id = schema_id
        self.version = version
        self.subject: str | None = None
        self.schema: Schema | None = None

    def lookup_schema(
        self,
        subject_name: str,
        schema: Schema,
        normalize_schemas: bool = False,
        fmt: str | None = None,
        deleted: bool = False,
    ) -> RegisteredSchema:
        self.subject = subject_name
        self.schema = schema
        return RegisteredSchema(subject_name, self.version, self.schema_id, None, schema)


def _users_created_contract() -> KafkaJsonSchemaContract:
    contract, _ = kafka_value_contracts_for_topics(
        users_created_topic="users_created",
        users_created_invalid_topic="users_created_invalid",
    )
    return contract
