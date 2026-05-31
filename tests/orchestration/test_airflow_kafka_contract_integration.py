# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json
from typing import Any, cast

import httpx
from confluent_kafka.schema_registry import RegisteredSchema, Schema

from realtimedatastreaming.ingestion.random_user import RandomUserClient
from realtimedatastreaming.ingestion.schemas import UserCreated
from realtimedatastreaming.messaging import kafka_producer
from realtimedatastreaming.messaging.kafka_producer import SchemaRegistryValueSerializer
from realtimedatastreaming.orchestration import schema_registry_contracts
from realtimedatastreaming.settings import Settings


def test_airflow_registered_schema_contract_serializes_mocked_random_user_event(monkeypatch: Any) -> None:
    settings = Settings(
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
        KAFKA_USERS_CREATED_INVALID_TOPIC="events.users.created.invalid",
        SCHEMA_REGISTRY_URL="http://schema-registry:8081",
    )
    schema_registry_client = _SharedFakeSchemaRegistryClient()
    monkeypatch.setattr(schema_registry_contracts, "get_settings", lambda: settings)
    monkeypatch.setattr(
        "realtimedatastreaming.ingestion.schema_registry.SchemaRegistryClient",
        lambda config: schema_registry_client,
    )
    monkeypatch.setattr(kafka_producer, "SchemaRegistryClient", lambda config: schema_registry_client)

    registered_versions = schema_registry_contracts.register_configured_kafka_value_contracts()
    event = UserCreated.model_validate(_fetch_one_mocked_random_user_profile())
    serialized_value = SchemaRegistryValueSerializer.from_settings(settings).serialize(
        topic=settings.kafka_users_created_topic,
        value=event,
    )

    assert registered_versions == {
        "events.users.created-value": 1,
        "events.users.created.invalid-value": 2,
    }
    assert schema_registry_client.lookup_subjects == ["events.users.created-value"]
    assert serialized_value.startswith(b"\x00")
    assert b'"event_type":"UserCreated"' in serialized_value
    assert b'"source_user_id":"api-user-1"' in serialized_value


def _fetch_one_mocked_random_user_profile() -> dict[str, object]:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["results"] == "1"
        return httpx.Response(200, json=_random_user_api_payload())

    with httpx.Client(transport=httpx.MockTransport(handler)) as http_client:
        client = RandomUserClient(
            api_url="https://randomuser.example/api/",
            timeout_seconds=2.5,
            http_client=http_client,
            sleep=lambda seconds: None,
        )
        return cast(dict[str, object], client.fetch_users(count=1)[0])


class _SharedFakeSchemaRegistryClient:
    def __init__(self) -> None:
        self._schemas_by_subject: dict[str, tuple[int, Schema]] = {}
        self.lookup_subjects: list[str] = []

    def set_compatibility(self, subject_name: str | None = None, level: str | None = None) -> str:
        return level or ""

    def register_schema(self, subject_name: str, schema: Schema, normalize_schemas: bool = False) -> int:
        schema_id = len(self._schemas_by_subject) + 1
        self._schemas_by_subject[subject_name] = (schema_id, schema)
        return schema_id

    def lookup_schema(
        self,
        subject_name: str,
        schema: Schema,
        normalize_schemas: bool = False,
        fmt: str | None = None,
        deleted: bool = False,
    ) -> RegisteredSchema:
        self.lookup_subjects.append(subject_name)
        schema_id, registered_schema = self._schemas_by_subject[subject_name]
        assert registered_schema.schema_type == schema.schema_type
        assert json.loads(registered_schema.schema_str or "{}") == json.loads(schema.schema_str or "{}")
        return RegisteredSchema(subject_name, 1, schema_id, None, registered_schema)

    def get_latest_version(self, subject_name: str, fmt: str | None = None) -> RegisteredSchema:
        schema_id, schema = self._schemas_by_subject[subject_name]
        return RegisteredSchema(subject_name, 1, schema_id, None, schema)


def _random_user_api_payload() -> dict[str, object]:
    return {
        "results": [
            {
                "gender": "female",
                "name": {"title": "Ms", "first": "Ada", "last": "Lovelace"},
                "location": {
                    "street": {"number": 42, "name": "Analytical Engine Road"},
                    "city": "London",
                    "state": "Greater London",
                    "country": "United Kingdom",
                    "postcode": "SW1A 1AA",
                    "coordinates": {"latitude": "51.5072", "longitude": "-0.1276"},
                    "timezone": {"offset": "+0:00", "description": "London"},
                },
                "email": "ada@example.com",
                "login": {"uuid": "api-user-1", "username": "adal"},
                "dob": {"date": "2000-01-01T00:00:00.000Z", "age": 26},
                "registered": {"date": "2024-01-01T12:00:00.000Z", "age": 2},
                "phone": "020 7946 0958",
                "cell": "07123 456789",
                "picture": {
                    "large": "https://example.com/large.jpg",
                    "medium": "https://example.com/medium.jpg",
                    "thumbnail": "https://example.com/thumb.jpg",
                },
                "nat": "GB",
            }
        ],
        "info": {"results": 1},
    }
