# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol

from confluent_kafka.schema_registry import RegisteredSchema, Schema, SchemaRegistryClient
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
from pydantic import ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import LongType, StringType, StructField, StructType

from realtimedatastreaming.ingestion.schema_registry import (
    KafkaJsonSchemaContract,
    kafka_value_contracts_for_topics,
    schema_registry_config_from_settings,
)
from realtimedatastreaming.ingestion.schemas import UserCreated
from realtimedatastreaming.settings import Settings

CONFLUENT_MAGIC_BYTE = 0
CONFLUENT_HEADER_SIZE = 5


class SchemaRegistryLookupClient(Protocol):
    def lookup_schema(
        self,
        subject_name: str,
        schema: Schema,
        normalize_schemas: bool = False,
        fmt: str | None = None,
        deleted: bool = False,
    ) -> RegisteredSchema: ...


@dataclass(frozen=True, slots=True)
class UserCreatedDeserializationResult:
    payload_json: str | None
    rejection_reason: str | None
    schema_id: int | None


DESERIALIZATION_RESULT_SCHEMA = StructType([
    StructField("payload_json", StringType(), nullable=True),
    StructField("rejection_reason", StringType(), nullable=True),
    StructField("schema_id", LongType(), nullable=True),
])


def resolve_supported_users_created_schema_id(
    settings: Settings,
    *,
    schema_registry_client: SchemaRegistryLookupClient | None = None,
) -> int:
    contract = _users_created_contract(settings)
    client = schema_registry_client or SchemaRegistryClient(schema_registry_config_from_settings(settings))
    registered_schema = client.lookup_schema(
        contract.subject,
        Schema(contract.schema_text(), schema_type=contract.schema_type),
    )
    if registered_schema.schema_id is None or registered_schema.version != contract.version:
        msg = f"Schema Registry subject {contract.subject} must contain supported contract version {contract.version}"
        raise RuntimeError(msg)
    return registered_schema.schema_id


def deserialize_user_created_frame(
    framed_value: bytes | bytearray | None,
    *,
    supported_schema_id: int,
    contract: KafkaJsonSchemaContract,
) -> UserCreatedDeserializationResult:
    if framed_value is None or len(framed_value) < CONFLUENT_HEADER_SIZE:
        return UserCreatedDeserializationResult(None, "invalid_confluent_frame", None)

    raw_value = bytes(framed_value)
    schema_id = int.from_bytes(raw_value[1:CONFLUENT_HEADER_SIZE], byteorder="big")
    if raw_value[0] != CONFLUENT_MAGIC_BYTE:
        return UserCreatedDeserializationResult(None, "invalid_confluent_magic_byte", schema_id)
    if schema_id != supported_schema_id:
        return UserCreatedDeserializationResult(None, "unsupported_schema_id", schema_id)

    try:
        payload = json.loads(raw_value[CONFLUENT_HEADER_SIZE:].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return UserCreatedDeserializationResult(None, "invalid_json_payload", schema_id)

    validator = _validator_for_schema(contract.schema_text())
    if not validator.is_valid(payload):
        return UserCreatedDeserializationResult(None, "schema_validation_failed", schema_id)

    try:
        user_created = UserCreated.model_validate(payload)
    except ValidationError:
        return UserCreatedDeserializationResult(None, "schema_validation_failed", schema_id)

    return UserCreatedDeserializationResult(user_created.model_dump_json(), None, schema_id)


def deserialize_users_created_stream(
    dataframe: DataFrame,
    settings: Settings,
    supported_schema_id: int,
) -> DataFrame:
    contract = _users_created_contract(settings)

    @udf(returnType=DESERIALIZATION_RESULT_SCHEMA)
    def deserialize_frame(framed_value: bytearray | None) -> tuple[str | None, str | None, int | None]:
        result = deserialize_user_created_frame(
            framed_value,
            supported_schema_id=supported_schema_id,
            contract=contract,
        )
        return result.payload_json, result.rejection_reason, result.schema_id

    return dataframe.withColumn("deserialization", deserialize_frame(col("framed_value"))).select(
        "*",
        "deserialization.payload_json",
        "deserialization.rejection_reason",
        "deserialization.schema_id",
    )


def _users_created_contract(settings: Settings) -> KafkaJsonSchemaContract:
    contract, _ = kafka_value_contracts_for_topics(
        users_created_topic=settings.kafka_users_created_topic,
        users_created_invalid_topic=settings.kafka_users_created_invalid_topic,
    )
    return contract


@lru_cache(maxsize=4)
def _validator_for_schema(schema_text: str) -> Draft202012Validator:
    return Draft202012Validator(json.loads(schema_text))
