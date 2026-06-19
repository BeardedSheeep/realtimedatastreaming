# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from typing import Protocol

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from realtimedatastreaming.settings import Settings

SCHEMA_REGISTRY_COMPATIBILITY = "BACKWARD"
SCHEMA_REGISTRY_SCHEMA_TYPE = "JSON"
SCHEMA_REGISTRY_SCHEMA_VERSION = 1

USERS_CREATED_VALUE_SUBJECT = "users_created-value"
USERS_CREATED_INVALID_VALUE_SUBJECT = "users_created_invalid-value"
USERS_CREATED_VALUE_SCHEMA_PATH = "users_created-value/v1.json"
USERS_CREATED_INVALID_VALUE_SCHEMA_PATH = "users_created_invalid-value/v1.json"


@dataclass(frozen=True, slots=True)
class KafkaJsonSchemaContract:
    subject: str
    schema_path: str
    schema_type: str = SCHEMA_REGISTRY_SCHEMA_TYPE
    version: int = SCHEMA_REGISTRY_SCHEMA_VERSION
    compatibility: str = SCHEMA_REGISTRY_COMPATIBILITY

    def schema_text(self) -> str:
        return (
            files("realtimedatastreaming.ingestion.schema_registry_schemas")
            .joinpath(self.schema_path)
            .read_text(encoding="utf-8")
        )


def value_subject_for_topic(topic: str) -> str:
    return f"{topic}-value"


USERS_CREATED_VALUE_CONTRACT = KafkaJsonSchemaContract(
    subject=USERS_CREATED_VALUE_SUBJECT,
    schema_path=USERS_CREATED_VALUE_SCHEMA_PATH,
)
USERS_CREATED_INVALID_VALUE_CONTRACT = KafkaJsonSchemaContract(
    subject=USERS_CREATED_INVALID_VALUE_SUBJECT,
    schema_path=USERS_CREATED_INVALID_VALUE_SCHEMA_PATH,
)
KAFKA_VALUE_CONTRACTS = (
    USERS_CREATED_VALUE_CONTRACT,
    USERS_CREATED_INVALID_VALUE_CONTRACT,
)


def kafka_value_contracts_for_topics(
    *,
    users_created_topic: str,
    users_created_invalid_topic: str,
) -> tuple[KafkaJsonSchemaContract, KafkaJsonSchemaContract]:
    return (
        KafkaJsonSchemaContract(
            subject=value_subject_for_topic(users_created_topic),
            schema_path=USERS_CREATED_VALUE_SCHEMA_PATH,
        ),
        KafkaJsonSchemaContract(
            subject=value_subject_for_topic(users_created_invalid_topic),
            schema_path=USERS_CREATED_INVALID_VALUE_SCHEMA_PATH,
        ),
    )


def kafka_streaming_value_contracts_for_topics(
    *,
    users_created_valid_topic: str,
    users_created_invalid_topic: str,
) -> tuple[KafkaJsonSchemaContract, KafkaJsonSchemaContract]:
    return (
        KafkaJsonSchemaContract(
            subject=value_subject_for_topic(users_created_valid_topic),
            schema_path=USERS_CREATED_VALUE_SCHEMA_PATH,
        ),
        KafkaJsonSchemaContract(
            subject=value_subject_for_topic(users_created_invalid_topic),
            schema_path=USERS_CREATED_INVALID_VALUE_SCHEMA_PATH,
        ),
    )


class SchemaRegistryClientProtocol(Protocol):
    def set_compatibility(self, subject_name: str | None = None, level: str | None = None) -> str: ...

    def register_schema(self, subject_name: str, schema: Schema, normalize_schemas: bool = False) -> int: ...


def schema_registry_config_from_settings(settings: Settings) -> dict[str, str]:
    config = {"url": str(settings.schema_registry_url)}

    if settings.schema_registry_basic_auth_user_info is not None:
        config.update({
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": settings.schema_registry_basic_auth_user_info.get_secret_value(),
        })
    if settings.schema_registry_ssl_ca_location is not None:
        config["ssl.ca.location"] = settings.schema_registry_ssl_ca_location

    return config


def register_kafka_value_contracts(
    schema_registry_url: str,
    *,
    schema_registry_config: dict[str, str] | None = None,
    schema_registry_client: SchemaRegistryClientProtocol | None = None,
    users_created_topic: str = "users_created",
    users_created_valid_topic: str | None = None,
    users_created_invalid_topic: str = "users_created_invalid",
    contracts: tuple[KafkaJsonSchemaContract, ...] | None = None,
) -> dict[str, int]:
    client = schema_registry_client or SchemaRegistryClient(schema_registry_config or {"url": schema_registry_url})
    if contracts is not None:
        value_contracts = contracts
    else:
        value_contracts = kafka_value_contracts_for_topics(
            users_created_topic=users_created_topic,
            users_created_invalid_topic=users_created_invalid_topic,
        )
        if users_created_valid_topic is not None:
            valid_contract, _ = kafka_streaming_value_contracts_for_topics(
                users_created_valid_topic=users_created_valid_topic,
                users_created_invalid_topic=users_created_invalid_topic,
            )
            value_contracts = (*value_contracts, valid_contract)
    return {
        contract.subject: _register_contract(contract=contract, schema_registry_client=client)
        for contract in value_contracts
    }


def _register_contract(
    *,
    contract: KafkaJsonSchemaContract,
    schema_registry_client: SchemaRegistryClientProtocol,
) -> int:
    schema_registry_client.set_compatibility(subject_name=contract.subject, level=contract.compatibility)
    return schema_registry_client.register_schema(
        subject_name=contract.subject,
        schema=Schema(contract.schema_text(), schema_type=contract.schema_type),
    )
