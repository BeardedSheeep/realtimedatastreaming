# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any, cast
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import BinaryType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

from realtimedatastreaming.ingestion.schema_registry import register_kafka_value_contracts
from realtimedatastreaming.ingestion.schemas import UserCreated
from realtimedatastreaming.messaging.kafka_producer import UserProfileEventProducer
from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming.user_profiles import (
    IS_VALID_CONTRACT_COLUMN,
    USER_PROFILE_COLUMNS,
    _select_user_profile_view,
    build_user_profiles_stream_config,
    write_user_profiles_batch,
)

pytestmark = pytest.mark.integration

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
SERVICE_TIMEOUT_SECONDS = 90.0


class RecordingUserProfileSink:
    sink_type = "recording"

    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def write(self, records: DataFrame, *, settings: Settings) -> int:
        self.rows = [row.asDict() for row in records.collect()]
        return len(self.rows)


def test_spark_slice_prepares_cassandra_shaped_profiles_and_emits_local_monitoring(
    caplog: pytest.LogCaptureFixture,
) -> None:
    _wait_for_kafka()
    _wait_for_schema_registry()
    caplog.set_level(logging.INFO)

    topic_suffix = uuid4().hex
    users_created_topic = f"it.spark.users.created.{topic_suffix}"
    users_created_invalid_topic = f"it.spark.users.created.invalid.{topic_suffix}"
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS=KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_USERS_CREATED_TOPIC=users_created_topic,
        KAFKA_USERS_CREATED_INVALID_TOPIC=users_created_invalid_topic,
        SCHEMA_REGISTRY_URL=SCHEMA_REGISTRY_URL,
        SPARK_APP_NAME=f"it-user-profiles-{topic_suffix}",
    )
    register_kafka_value_contracts(
        SCHEMA_REGISTRY_URL,
        users_created_topic=users_created_topic,
        users_created_invalid_topic=users_created_invalid_topic,
    )

    event = UserCreated.model_validate(_valid_user_created_payload(source_user_id="api-user-1"))
    producer = UserProfileEventProducer.from_settings(settings)
    delivery_report = producer.publish_sync(
        topic=users_created_topic,
        key=event.source_user_id,
        value=event,
        timeout=30.0,
    )
    producer.close(timeout=30.0)
    consumed_message = _consume_single_message(
        topic=users_created_topic,
        partition=delivery_report.partition,
        group_id=f"it-spark-users-created-{topic_suffix}",
        expected_key=b"api-user-1",
    )

    spark = (
        SparkSession.builder
        .appName(f"it-user-profiles-{topic_suffix}")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    sink = RecordingUserProfileSink()
    try:
        kafka_frame = _kafka_message_dataframe(
            spark,
            topic=users_created_topic,
            partition=delivery_report.partition,
            offset=delivery_report.offset,
            value=consumed_message.value(),
        )
        profiles = _select_user_profile_view(kafka_frame, build_user_profiles_stream_config(settings).value_contract)

        write_user_profiles_batch(profiles, batch_id=0, settings=settings, sink=sink)
    finally:
        spark.stop()

    assert len(sink.rows) == 1
    assert set(sink.rows[0]) == set(USER_PROFILE_COLUMNS)
    assert sink.rows[0]["source_user_id"] == "api-user-1"
    assert sink.rows[0]["event_type"] == "UserCreated"
    assert sink.rows[0]["kafka_topic"] == users_created_topic
    assert sink.rows[0]["kafka_offset"] == delivery_report.offset
    assert IS_VALID_CONTRACT_COLUMN not in sink.rows[0]

    finished_log = next(record for record in caplog.records if record.msg == "spark_user_profiles_batch_finished")
    finished_log_dynamic = cast(Any, finished_log)
    assert finished_log_dynamic.input_records == 1
    assert finished_log_dynamic.invalid_records == 0
    assert finished_log_dynamic.prepared_cassandra_writes == 1
    assert finished_log_dynamic.sink_type == "recording"
    assert isinstance(finished_log_dynamic.batch_duration_ms, int)


def _kafka_message_dataframe(
    spark: SparkSession,
    *,
    topic: str,
    partition: int,
    offset: int,
    value: bytes,
) -> DataFrame:
    schema = StructType([
        StructField("topic", StringType(), nullable=False),
        StructField("partition", IntegerType(), nullable=False),
        StructField("offset", LongType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("value", BinaryType(), nullable=False),
    ])
    return spark.createDataFrame(
        [
            Row(
                topic=topic,
                partition=partition,
                offset=offset,
                timestamp=datetime(2026, 1, 1, tzinfo=UTC),
                value=value,
            )
        ],
        schema=schema,
    )


def _wait_for_kafka() -> None:
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    deadline = time.monotonic() + SERVICE_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            admin_client.list_topics(timeout=5.0)
            return
        except KafkaException as exc:
            last_error = exc
            time.sleep(1.0)
    pytest.fail(f"Kafka did not become ready within {SERVICE_TIMEOUT_SECONDS:.0f}s: {last_error}")


def _wait_for_schema_registry() -> None:
    deadline = time.monotonic() + SERVICE_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urlopen(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5.0) as response:
                if response.status == 200:
                    return
        except (OSError, URLError) as exc:
            last_error = exc
            time.sleep(1.0)
    pytest.fail(f"Schema Registry did not become ready within {SERVICE_TIMEOUT_SECONDS:.0f}s: {last_error}")


def _consume_single_message(*, topic: str, partition: int, group_id: str, expected_key: bytes) -> Any:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "enable.auto.commit": "false",
        "auto.offset.reset": "earliest",
    })
    try:
        consumer.assign([TopicPartition(topic, partition, 0)])
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())
            if message.key() == expected_key:
                return message
    finally:
        consumer.close()
    pytest.fail(f"Message with key {expected_key!r} was not consumed from topic {topic} within 30s")


def _valid_user_created_payload(*, source_user_id: str) -> dict[str, Any]:
    return {
        "source": "random_user",
        "source_user_id": source_user_id,
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
