# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from dataclasses import dataclass

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from pydantic import ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import BinaryType

from realtimedatastreaming.ingestion.quality import (
    build_invalid_user_profile_event,
)
from realtimedatastreaming.ingestion.schema_registry import (
    KafkaJsonSchemaContract,
    kafka_streaming_value_contracts_for_topics,
    schema_registry_config_from_settings,
)
from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid
from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming.kafka_source import kafka_connection_options
from realtimedatastreaming.streaming.profile_quality import QUALITY_INVALID, QUALITY_VALID
from realtimedatastreaming.streaming.user_created_deserializer import SchemaRegistryLookupClient

CONFLUENT_MAGIC_BYTE = b"\x00"


@dataclass(frozen=True, slots=True)
class OutputSchemaIds:
    valid: int
    invalid: int


def resolve_output_schema_ids(
    settings: Settings,
    *,
    schema_registry_client: SchemaRegistryLookupClient | None = None,
) -> OutputSchemaIds:
    client = schema_registry_client or SchemaRegistryClient(schema_registry_config_from_settings(settings))
    valid_contract, invalid_contract = _output_contracts(settings)
    return OutputSchemaIds(
        valid=_lookup_schema_id(client, valid_contract),
        invalid=_lookup_schema_id(client, invalid_contract),
    )


def build_invalid_event_json(
    payload_json: str | None,
    *,
    deserialization_rejection_reason: str | None,
    quality_rejection_reasons: list[str] | tuple[str, ...] | None,
    pseudonymization_salt: str | None,
) -> str:
    if payload_json is not None and quality_rejection_reasons:
        try:
            profile = UserCreated.model_validate_json(payload_json)
        except ValidationError:
            profile = None
        if profile is not None:
            reasons = tuple(quality_rejection_reasons)
            if pseudonymization_salt is not None:
                return build_invalid_user_profile_event(
                    profile,
                    reasons,
                    pseudonymization_salt=pseudonymization_salt,
                ).model_dump_json()
            return UserProfileInvalid(
                source=profile.source,
                rejection_reasons=reasons,
                payload={
                    "schema_version": profile.schema_version,
                    "event_type": profile.event_type,
                    "source": profile.source,
                    "country_code": profile.country_code,
                },
            ).model_dump_json()

    reason = deserialization_rejection_reason or "invalid_deserialized_profile"
    return UserProfileInvalid(rejection_reasons=(reason,)).model_dump_json()


def frame_json_payload(payload_json: str, schema_id: int) -> bytes:
    return CONFLUENT_MAGIC_BYTE + schema_id.to_bytes(4, byteorder="big") + payload_json.encode("utf-8")


def start_kafka_routes(
    dataframe: DataFrame,
    settings: Settings,
    output_schema_ids: OutputSchemaIds,
) -> tuple[StreamingQuery, StreamingQuery]:
    @udf(returnType=BinaryType())
    def frame_valid(payload_json: str) -> bytes:
        return frame_json_payload(payload_json, output_schema_ids.valid)

    @udf(returnType=BinaryType())
    def frame_invalid(
        payload_json: str | None,
        deserialization_rejection_reason: str | None,
        quality_rejection_reasons: list[str] | None,
    ) -> bytes:
        invalid_json = build_invalid_event_json(
            payload_json,
            deserialization_rejection_reason=deserialization_rejection_reason,
            quality_rejection_reasons=quality_rejection_reasons,
            pseudonymization_salt=settings.pii_pseudonymization_salt_value,
        )
        return frame_json_payload(invalid_json, output_schema_ids.invalid)

    valid_records = dataframe.filter(col("quality_status") == QUALITY_VALID).select(
        col("key"), frame_valid(col("payload_json")).alias("value")
    )
    rejected_records = dataframe.filter(
        (col("quality_status") == QUALITY_INVALID) | col("rejection_reason").isNotNull()
    ).select(
        lit(None).cast("binary").alias("key"),
        frame_invalid(
            col("payload_json"),
            col("rejection_reason"),
            col("quality_rejection_reasons"),
        ).alias("value"),
    )

    valid_query = _start_kafka_query(
        valid_records,
        settings,
        topic=settings.kafka_users_created_valid_topic,
        checkpoint_suffix="valid",
        query_name="users-created-valid",
    )
    invalid_query = _start_kafka_query(
        rejected_records,
        settings,
        topic=settings.kafka_users_created_invalid_topic,
        checkpoint_suffix="invalid",
        query_name="users-created-invalid",
    )
    return valid_query, invalid_query


def _start_kafka_query(
    dataframe: DataFrame,
    settings: Settings,
    *,
    topic: str,
    checkpoint_suffix: str,
    query_name: str,
) -> StreamingQuery:
    writer = dataframe.writeStream.format("kafka").outputMode("append").queryName(query_name)
    for key, value in kafka_connection_options(settings).items():
        writer = writer.option(key, value)
    return (
        writer
        .option("topic", topic)
        .option(
            "checkpointLocation",
            f"{settings.spark_checkpoint_location.rstrip('/')}/{checkpoint_suffix}",
        )
        .start()
    )


def _lookup_schema_id(
    client: SchemaRegistryLookupClient,
    contract: KafkaJsonSchemaContract,
) -> int:
    registered = client.lookup_schema(
        contract.subject,
        Schema(contract.schema_text(), schema_type=contract.schema_type),
    )
    if registered.schema_id is None or registered.version != contract.version:
        msg = f"Schema Registry subject {contract.subject} must contain supported contract version {contract.version}"
        raise RuntimeError(msg)
    return registered.schema_id


def _output_contracts(settings: Settings) -> tuple[KafkaJsonSchemaContract, KafkaJsonSchemaContract]:
    return kafka_streaming_value_contracts_for_topics(
        users_created_valid_topic=settings.kafka_users_created_valid_topic,
        users_created_invalid_topic=settings.kafka_users_created_invalid_topic,
    )
