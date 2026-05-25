from __future__ import annotations

from dataclasses import dataclass
from importlib.resources import files
from typing import Protocol

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

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


class SchemaRegistryClientProtocol(Protocol):
    def set_compatibility(self, subject_name: str | None = None, level: str | None = None) -> str: ...

    def register_schema(self, subject_name: str, schema: Schema, normalize_schemas: bool = False) -> int: ...


def register_kafka_value_contracts(
    schema_registry_url: str,
    *,
    schema_registry_client: SchemaRegistryClientProtocol | None = None,
    users_created_topic: str = "users_created",
    users_created_invalid_topic: str = "users_created_invalid",
    contracts: tuple[KafkaJsonSchemaContract, ...] | None = None,
) -> dict[str, int]:
    client = schema_registry_client or SchemaRegistryClient({"url": schema_registry_url})
    value_contracts = contracts or kafka_value_contracts_for_topics(
        users_created_topic=users_created_topic,
        users_created_invalid_topic=users_created_invalid_topic,
    )
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
