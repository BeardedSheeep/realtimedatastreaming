# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from typing import Protocol

import orjson
from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, decode, expr, from_json, to_timestamp, udf
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from realtimedatastreaming.ingestion.schema_registry import (
    KafkaJsonSchemaContract,
    kafka_value_contracts_for_topics,
    schema_registry_config_from_settings,
)
from realtimedatastreaming.observability import configure_observability, set_correlation_context
from realtimedatastreaming.settings import Settings, get_settings

logger = logging.getLogger(__name__)

USER_PROFILES_BY_SOURCE_ID_TABLE = "user_profiles_by_source_id"
IS_VALID_CONTRACT_COLUMN = "is_valid_contract"
SPARK_CHECKPOINT_LOCATION_SETTING = "SPARK_CHECKPOINT_LOCATION"
USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS: tuple[str, ...] = ("source", "source_user_id", "event_type")
USER_PROFILE_COLUMNS: tuple[str, ...] = (
    "source",
    "source_user_id",
    "schema_version",
    "event_type",
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
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "processed_at",
)


@dataclass(frozen=True, slots=True)
class UserProfilesStreamConfig:
    kafka_source_options: Mapping[str, str]
    schema_registry_config: Mapping[str, str]
    value_contract: KafkaJsonSchemaContract


@dataclass(frozen=True, slots=True)
class UserProfilesBatchMonitoring:
    batch_id: int
    input_records: int
    invalid_records: int
    prepared_cassandra_writes: int
    batch_duration_ms: int
    sink_type: str

    def as_log_extra(self) -> dict[str, int | str]:
        return {
            "batch_id": self.batch_id,
            "input_records": self.input_records,
            "invalid_records": self.invalid_records,
            "prepared_cassandra_writes": self.prepared_cassandra_writes,
            "batch_duration_ms": self.batch_duration_ms,
            "sink_type": self.sink_type,
        }


@dataclass(frozen=True, slots=True)
class UserProfilesReplayPolicy:
    checkpoint_setting: str
    default_starting_offsets: str
    idempotence_key_columns: tuple[str, ...]
    replay_mode: str
    production_scheduling_gate: str

    def as_log_extra(self) -> dict[str, str]:
        return {
            "checkpoint_setting": self.checkpoint_setting,
            "default_starting_offsets": self.default_starting_offsets,
            "idempotence_key_columns": ",".join(self.idempotence_key_columns),
            "replay_mode": self.replay_mode,
            "production_scheduling_gate": self.production_scheduling_gate,
        }


USER_PROFILES_REPLAY_POLICY = UserProfilesReplayPolicy(
    checkpoint_setting=SPARK_CHECKPOINT_LOCATION_SETTING,
    default_starting_offsets="latest",
    idempotence_key_columns=USER_PROFILE_IDEMPOTENCE_KEY_COLUMNS,
    replay_mode="manual_checkpoint_replay",
    production_scheduling_gate="disabled_until_real_cassandra_sink_is_idempotent",
)


class UserProfileSink(Protocol):
    sink_type: str

    def write(self, records: DataFrame, *, settings: Settings) -> int: ...


class DummyUserProfileSink:
    sink_type = "dummy"

    def write(self, records: DataFrame, *, settings: Settings) -> int:
        prepared_cassandra_writes = records.count()
        logger.info(
            "spark_user_profiles_ready_for_cassandra",
            extra={
                "prepared_cassandra_writes": prepared_cassandra_writes,
                "cassandra_keyspace": settings.cassandra_keyspace,
                "cassandra_table": USER_PROFILES_BY_SOURCE_ID_TABLE,
                "sink_type": self.sink_type,
            },
        )
        return prepared_cassandra_writes


def main() -> None:
    settings = get_settings()
    configure_observability(settings)
    set_correlation_context(request_id_value=str(uuid.uuid4()))
    stream_config = build_user_profiles_stream_config(settings)

    logger.info(
        "spark_streaming_job_starting",
        extra={
            "kafka_topic": settings.kafka_users_created_topic,
            "schema_registry_url": stream_config.schema_registry_config["url"],
            "schema_registry_subject": stream_config.value_contract.subject,
            "cassandra_keyspace": settings.cassandra_keyspace,
            "cassandra_table": USER_PROFILES_BY_SOURCE_ID_TABLE,
            **USER_PROFILES_REPLAY_POLICY.as_log_extra(),
        },
    )

    spark = build_spark_session(settings)
    query = start_user_profiles_stream(spark, settings)
    query.awaitTermination()


def build_spark_session(settings: Settings) -> SparkSession:
    builder = SparkSession.builder.appName(settings.spark_app_name).master(settings.spark_master_url)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(settings.log_level)
    return spark


def start_user_profiles_stream(spark: SparkSession, settings: Settings) -> StreamingQuery:
    stream_config = build_user_profiles_stream_config(settings)
    stream = _read_kafka_user_created_stream(spark, stream_config)
    profiles = _select_user_profile_view(stream, stream_config.value_contract)

    return (
        profiles.writeStream
        .foreachBatch(
            lambda batch, batch_id: write_user_profiles_batch(batch, batch_id, settings, DummyUserProfileSink())
        )
        .option("checkpointLocation", settings.spark_checkpoint_location)
        .queryName("user_profiles_prepare_for_cassandra")
        .start()
    )


def write_user_profiles_batch(
    batch: DataFrame,
    batch_id: int,
    settings: Settings,
    sink: UserProfileSink | None = None,
) -> None:
    started_at = time.perf_counter()
    profile_sink = sink or DummyUserProfileSink()
    input_records = batch.count()
    invalid_records = batch.where(_invalid_contract_record()).count()
    logger.info(
        "spark_user_profiles_batch_started",
        extra={
            "batch_id": batch_id,
            "input_records": input_records,
            "invalid_records": invalid_records,
            "cassandra_keyspace": settings.cassandra_keyspace,
            "cassandra_table": USER_PROFILES_BY_SOURCE_ID_TABLE,
            "sink_type": profile_sink.sink_type,
        },
    )

    if input_records == 0:
        logger.info("spark_user_profiles_batch_skipped", extra={"batch_id": batch_id, "reason": "empty_batch"})
        return

    valid_records = prepare_user_profiles_for_cassandra(batch)

    try:
        prepared_cassandra_writes = profile_sink.write(valid_records, settings=settings)
    except Exception:
        logger.exception(
            "spark_user_profiles_sink_failed",
            extra={
                "batch_id": batch_id,
                "input_records": input_records,
                "invalid_records": invalid_records,
                "cassandra_table": USER_PROFILES_BY_SOURCE_ID_TABLE,
                "sink_type": profile_sink.sink_type,
            },
        )
        raise

    monitoring = UserProfilesBatchMonitoring(
        batch_id=batch_id,
        input_records=input_records,
        invalid_records=invalid_records,
        prepared_cassandra_writes=prepared_cassandra_writes,
        batch_duration_ms=int((time.perf_counter() - started_at) * 1000),
        sink_type=profile_sink.sink_type,
    )
    logger.info(
        "spark_user_profiles_batch_finished",
        extra=monitoring.as_log_extra(),
    )


def build_user_profiles_stream_config(settings: Settings) -> UserProfilesStreamConfig:
    users_created_contract, _ = kafka_value_contracts_for_topics(
        users_created_topic=settings.kafka_users_created_topic,
        users_created_invalid_topic=settings.kafka_users_created_invalid_topic,
    )
    return UserProfilesStreamConfig(
        kafka_source_options=_kafka_source_options(settings),
        schema_registry_config=schema_registry_config_from_settings(settings),
        value_contract=users_created_contract,
    )


def _read_kafka_user_created_stream(spark: SparkSession, stream_config: UserProfilesStreamConfig) -> DataFrame:
    return spark.readStream.format("kafka").options(**stream_config.kafka_source_options).load()


def _kafka_source_options(settings: Settings) -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": ",".join(settings.kafka_bootstrap_servers),
        "subscribe": settings.kafka_users_created_topic,
        "startingOffsets": USER_PROFILES_REPLAY_POLICY.default_starting_offsets,
        "failOnDataLoss": "false",
    }

    if settings.kafka_sasl_username is not None and settings.kafka_sasl_password is not None:
        options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{settings.kafka_sasl_username}" '
                f'password="{settings.kafka_sasl_password.get_secret_value()}";'
            ),
        })

    return options


def _select_user_profile_view(stream: DataFrame, value_contract: KafkaJsonSchemaContract) -> DataFrame:
    decoded = stream.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        _decode_kafka_json_value().alias("value_json"),
    )
    contract_checked = decoded.withColumn(
        IS_VALID_CONTRACT_COLUMN,
        _kafka_value_matches_contract(col("value_json"), value_contract),
    )
    parsed = contract_checked.select(
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col(IS_VALID_CONTRACT_COLUMN),
        from_json(col("value_json"), user_created_spark_schema()).alias("profile"),
    )

    return parsed.select(
        col("profile.source").alias("source"),
        col("profile.source_user_id").alias("source_user_id"),
        col("profile.schema_version").alias("schema_version"),
        col("profile.event_type").alias("event_type"),
        col("profile.gender").alias("gender"),
        col("profile.title").alias("title"),
        col("profile.first_name").alias("first_name"),
        col("profile.last_name").alias("last_name"),
        col("profile.street_number").alias("street_number"),
        col("profile.street_name").alias("street_name"),
        col("profile.city").alias("city"),
        col("profile.state").alias("state"),
        col("profile.country").alias("country"),
        col("profile.country_code").alias("country_code"),
        col("profile.postcode").alias("postcode"),
        col("profile.latitude").alias("latitude"),
        col("profile.longitude").alias("longitude"),
        col("profile.timezone_offset").alias("timezone_offset"),
        col("profile.timezone_description").alias("timezone_description"),
        col("profile.email").alias("email"),
        col("profile.username").alias("username"),
        to_timestamp(col("profile.date_of_birth")).alias("date_of_birth"),
        to_timestamp(col("profile.registered_at")).alias("registered_at"),
        col("profile.phone").alias("phone"),
        col("profile.cell").alias("cell"),
        col("profile.picture_large").alias("picture_large"),
        col("profile.picture_medium").alias("picture_medium"),
        col("profile.picture_thumbnail").alias("picture_thumbnail"),
        col("profile.nationality").alias("nationality"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col(IS_VALID_CONTRACT_COLUMN),
        current_timestamp().alias("processed_at"),
    )


def prepare_user_profiles_for_cassandra(batch: DataFrame) -> DataFrame:
    return batch.where(col(IS_VALID_CONTRACT_COLUMN) & col("source_user_id").isNotNull()).select(*USER_PROFILE_COLUMNS)


def _invalid_contract_record() -> Column:
    return col(IS_VALID_CONTRACT_COLUMN).isNull() | ~col(IS_VALID_CONTRACT_COLUMN)


def _kafka_value_matches_contract(value_json: Column, value_contract: KafkaJsonSchemaContract) -> Column:
    schema_text = value_contract.schema_text()
    validate_payload = udf(
        lambda raw_value: validate_event_payload_against_contract(raw_value, schema_text),
        BooleanType(),
    )
    return validate_payload(value_json)


def validate_event_payload_against_contract(value_json: str | None, schema_text: str) -> bool:
    if value_json is None:
        return False

    try:
        payload = orjson.loads(value_json)
    except orjson.JSONDecodeError:
        return False

    return bool(_contract_validator(schema_text).is_valid(payload))


@lru_cache(maxsize=16)
def _contract_validator(schema_text: str) -> Draft202012Validator:
    schema = orjson.loads(schema_text)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def user_created_spark_schema() -> StructType:
    return StructType([
        StructField("schema_version", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_user_id", StringType(), nullable=False),
        StructField("gender", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("street_number", IntegerType(), nullable=True),
        StructField("street_name", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("state", StringType(), nullable=True),
        StructField("country", StringType(), nullable=False),
        StructField("country_code", StringType(), nullable=True),
        StructField("postcode", StringType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("timezone_offset", StringType(), nullable=True),
        StructField("timezone_description", StringType(), nullable=True),
        StructField("email", StringType(), nullable=False),
        StructField("username", StringType(), nullable=False),
        StructField("date_of_birth", StringType(), nullable=False),
        StructField("registered_at", StringType(), nullable=True),
        StructField("phone", StringType(), nullable=True),
        StructField("cell", StringType(), nullable=True),
        StructField("picture_large", StringType(), nullable=True),
        StructField("picture_medium", StringType(), nullable=True),
        StructField("picture_thumbnail", StringType(), nullable=True),
        StructField("nationality", StringType(), nullable=True),
    ])


def _decode_kafka_json_value() -> Column:
    # Confluent JSON Schema payloads are framed as magic byte + 4-byte schema id + JSON.
    value_bytes = expr(
        "CASE WHEN substring(value, 1, 1) = X'00' THEN substring(value, 6, length(value) - 5) ELSE value END"
    )
    return decode(value_bytes, "UTF-8")


def kafka_source_options_for_monitoring(settings: Settings) -> Mapping[str, str]:
    return _kafka_source_options(settings)
