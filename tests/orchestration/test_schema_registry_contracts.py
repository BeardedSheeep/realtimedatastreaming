# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from typing import Any

from realtimedatastreaming.orchestration import schema_registry_contracts
from realtimedatastreaming.settings import Settings


def test_register_configured_kafka_value_contracts_uses_settings_topics(monkeypatch: Any) -> None:
    settings = Settings(
        SCHEMA_REGISTRY_URL="http://schema-registry:8081",
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
        KAFKA_USERS_CREATED_INVALID_TOPIC="events.users.created.invalid",
    )
    captured_call: dict[str, object] = {}

    def fake_register_kafka_value_contracts(
        schema_registry_url: str,
        *,
        schema_registry_config: dict[str, str],
        users_created_topic: str,
        users_created_invalid_topic: str,
    ) -> dict[str, int]:
        captured_call.update({
            "schema_registry_url": schema_registry_url,
            "schema_registry_config": schema_registry_config,
            "users_created_topic": users_created_topic,
            "users_created_invalid_topic": users_created_invalid_topic,
        })
        return {"events.users.created-value": 1}

    monkeypatch.setattr(schema_registry_contracts, "get_settings", lambda: settings)
    monkeypatch.setattr(
        schema_registry_contracts,
        "register_kafka_value_contracts",
        fake_register_kafka_value_contracts,
    )

    registered_versions = schema_registry_contracts.register_configured_kafka_value_contracts()

    assert registered_versions == {"events.users.created-value": 1}
    assert captured_call == {
        "schema_registry_url": "http://schema-registry:8081/",
        "schema_registry_config": {"url": "http://schema-registry:8081/"},
        "users_created_topic": "events.users.created",
        "users_created_invalid_topic": "events.users.created.invalid",
    }


def test_register_configured_kafka_value_contracts_uses_schema_registry_auth_and_tls(monkeypatch: Any) -> None:
    settings = Settings(
        SCHEMA_REGISTRY_URL="https://schema-registry.example",
        SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="api-key:api-secret",
        SCHEMA_REGISTRY_SSL_CA_LOCATION="/etc/ssl/schema-registry-ca.pem",
    )
    captured_call: dict[str, object] = {}

    def fake_register_kafka_value_contracts(
        schema_registry_url: str,
        *,
        schema_registry_config: dict[str, str],
        users_created_topic: str,
        users_created_invalid_topic: str,
    ) -> dict[str, int]:
        captured_call.update({
            "schema_registry_url": schema_registry_url,
            "schema_registry_config": schema_registry_config,
            "users_created_topic": users_created_topic,
            "users_created_invalid_topic": users_created_invalid_topic,
        })
        return {"users_created-value": 1}

    monkeypatch.setattr(schema_registry_contracts, "get_settings", lambda: settings)
    monkeypatch.setattr(
        schema_registry_contracts,
        "register_kafka_value_contracts",
        fake_register_kafka_value_contracts,
    )

    schema_registry_contracts.register_configured_kafka_value_contracts()

    assert captured_call == {
        "schema_registry_url": "https://schema-registry.example/",
        "schema_registry_config": {
            "url": "https://schema-registry.example/",
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": "api-key:api-secret",
            "ssl.ca.location": "/etc/ssl/schema-registry-ca.pem",
        },
        "users_created_topic": "users_created",
        "users_created_invalid_topic": "users_created_invalid",
    }
