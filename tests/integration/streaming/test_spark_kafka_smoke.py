# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json
import pprint
import time
from typing import Any, cast
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException
from pyspark.sql.streaming.query import StreamingQuery

from realtimedatastreaming.ingestion.schema_registry import register_kafka_value_contracts
from realtimedatastreaming.ingestion.schemas import UserCreated
from realtimedatastreaming.messaging.kafka_producer import KafkaProducerRecord, UserProfileEventProducer
from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming.kafka_routes import resolve_output_schema_ids, start_kafka_routes
from realtimedatastreaming.streaming.kafka_source import build_users_created_stream
from realtimedatastreaming.streaming.profile_quality import apply_profile_quality_rules
from realtimedatastreaming.streaming.spark_job import build_spark_session
from realtimedatastreaming.streaming.user_created_deserializer import (
    deserialize_users_created_stream,
    resolve_supported_users_created_schema_id,
)

pytestmark = pytest.mark.integration

OUTPUT_TIMEOUT_SECONDS = 90.0
STREAMING_INPUT_TIMEOUT_SECONDS = 90.0


def test_spark_routes_valid_and_rejected_user_created_events() -> None:
    suffix = uuid4().hex
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS="kafka:29092",
        KAFKA_USERS_CREATED_TOPIC=f"it.spark.users.created.{suffix}",
        KAFKA_USERS_CREATED_VALID_TOPIC=f"it.spark.users.created.valid.{suffix}",
        KAFKA_USERS_CREATED_INVALID_TOPIC=f"it.spark.users.created.invalid.{suffix}",
        SCHEMA_REGISTRY_URL="http://schema-registry:8081",
        SPARK_APP_NAME=f"spark-routing-smoke-{suffix}",
        SPARK_MASTER_URL="local[2]",
        SPARK_CHECKPOINT_LOCATION=f"/tmp/realtimedatastreaming/checkpoints/{suffix}",
    )
    register_kafka_value_contracts(
        str(settings.schema_registry_url),
        users_created_topic=settings.kafka_users_created_topic,
        users_created_valid_topic=settings.kafka_users_created_valid_topic,
        users_created_invalid_topic=settings.kafka_users_created_invalid_topic,
    )

    spark = build_spark_session(settings)
    queries: tuple[StreamingQuery, ...] = ()
    producer = UserProfileEventProducer.from_settings(settings)
    try:
        producer.publish_batch_sync(
            topic=settings.kafka_users_created_topic,
            records=(
                KafkaProducerRecord(
                    key="valid-user",
                    value=UserCreated.model_validate(_profile_payload(source_user_id="valid-user")),
                ),
                KafkaProducerRecord(
                    key="rejected-user",
                    value=UserCreated.model_validate(
                        _profile_payload(source_user_id="rejected-user", country="Atlantis")
                    ),
                ),
            ),
            timeout=30.0,
        )

        source = build_users_created_stream(spark, settings)
        input_schema_id = resolve_supported_users_created_schema_id(settings)
        deserialized = deserialize_users_created_stream(source, settings, input_schema_id)
        quality_checked = apply_profile_quality_rules(deserialized)
        queries = start_kafka_routes(quality_checked, settings, resolve_output_schema_ids(settings))
        _wait_for_streaming_input(queries, timeout_seconds=STREAMING_INPUT_TIMEOUT_SECONDS)

        valid_event = _consume_event(settings.kafka_users_created_valid_topic)
        invalid_event = _consume_event(settings.kafka_users_created_invalid_topic)

        assert valid_event["event_type"] == "UserCreated"
        assert valid_event["source_user_id"] == "valid-user"
        assert invalid_event["event_type"] == "UserProfileInvalid"
        assert invalid_event["source_user_id"] is None
        assert "unsupported_country" in invalid_event["rejection_reasons"]
        assert "email" not in invalid_event["payload"]
    finally:
        producer.close(timeout=30.0)
        for query in queries:
            query.stop()
        spark.stop()


def _wait_for_streaming_input(
    queries: tuple[StreamingQuery, ...],
    *,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        _assert_queries_have_no_exceptions(queries)
        if all(_query_has_processed_input(query) for query in queries):
            return
        time.sleep(1.0)

    _assert_queries_have_no_exceptions(queries)
    pytest.fail(_format_query_debug(queries, timeout_seconds=timeout_seconds))


def _assert_queries_have_no_exceptions(queries: tuple[StreamingQuery, ...]) -> None:
    failures = [
        f"{_query_label(query)} failed: {query.exception()}" for query in queries if query.exception() is not None
    ]
    if failures:
        pytest.fail("Spark streaming query failed before processing input:\n" + "\n".join(failures))


def _query_has_processed_input(query: StreamingQuery) -> bool:
    return any(progress.get("numInputRows", 0) > 0 for progress in query.recentProgress)


def _format_query_debug(
    queries: tuple[StreamingQuery, ...],
    *,
    timeout_seconds: float,
) -> str:
    reports = [
        {
            "name": query.name,
            "id": str(query.id),
            "runId": str(query.runId),
            "isActive": query.isActive,
            "status": query.status,
            "exception": str(query.exception()) if query.exception() is not None else None,
            "lastProgress": query.lastProgress,
            "recentProgressTail": query.recentProgress[-3:],
        }
        for query in queries
    ]
    return (
        f"Spark streaming queries did not process input rows within {timeout_seconds:.0f}s.\n"
        f"{pprint.pformat(reports, sort_dicts=False)}"
    )


def _query_label(query: StreamingQuery) -> str:
    return f"{query.name} ({query.id}/{query.runId})"


def _consume_event(topic: str) -> dict[str, Any]:
    consumer = Consumer({
        "bootstrap.servers": "kafka:29092",
        "group.id": f"spark-smoke-{uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    })
    consumer.subscribe([topic])
    deadline = time.monotonic() + OUTPUT_TIMEOUT_SECONDS
    try:
        while time.monotonic() < deadline:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            error = message.error()
            if error is not None:
                if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                raise KafkaException(error)
            value = message.value()
            if value is not None and len(value) >= 5:
                return cast(dict[str, Any], json.loads(value[5:].decode("utf-8")))
    finally:
        consumer.close()
    pytest.fail(f"No event consumed from {topic} within {OUTPUT_TIMEOUT_SECONDS:.0f}s")


def _profile_payload(
    *,
    source_user_id: str,
    country: str = "United Kingdom",
) -> dict[str, Any]:
    return {
        "source": "random_user",
        "source_user_id": source_user_id,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "country": country,
        "country_code": "GB",
        "email": "ada@example.com",
        "username": source_user_id,
        "date_of_birth": "2000-01-01T00:00:00Z",
    }
