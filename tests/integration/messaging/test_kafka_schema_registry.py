# Copyright (c) 2026 BeardedSheeep

import json
import time
from typing import Any, cast
from urllib.error import URLError
from urllib.request import urlopen
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from realtimedatastreaming.ingestion.schema_registry import register_kafka_value_contracts, value_subject_for_topic
from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid
from realtimedatastreaming.messaging.kafka_producer import KafkaProducerRecord, UserProfileEventProducer
from realtimedatastreaming.settings import Settings

pytestmark = pytest.mark.integration

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
SCHEMA_REGISTRY_URL = "http://localhost:18081"
SERVICE_TIMEOUT_SECONDS = 90.0


def test_kafka_schema_registry_stack_registers_contracts_and_routes_profile_events() -> None:
    _wait_for_kafka()
    _wait_for_schema_registry()

    topic_suffix = uuid4().hex
    users_created_topic = f"it.users.created.{topic_suffix}"
    users_created_invalid_topic = f"it.users.created.invalid.{topic_suffix}"
    users_created_subject = value_subject_for_topic(users_created_topic)
    users_created_invalid_subject = value_subject_for_topic(users_created_invalid_topic)
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS=KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_USERS_CREATED_TOPIC=users_created_topic,
        KAFKA_USERS_CREATED_INVALID_TOPIC=users_created_invalid_topic,
        SCHEMA_REGISTRY_URL=SCHEMA_REGISTRY_URL,
    )

    registered_schema_ids = register_kafka_value_contracts(
        SCHEMA_REGISTRY_URL,
        users_created_topic=users_created_topic,
        users_created_invalid_topic=users_created_invalid_topic,
    )
    producer = UserProfileEventProducer.from_settings(settings)
    valid_event = UserCreated.model_validate(_valid_user_created_payload(source_user_id="api-user-1"))
    invalid_event = UserProfileInvalid.model_validate({
        "source": "random_user",
        "source_user_id": "sha256:" + "a" * 64,
        "rejection_reasons": ("invalid_email",),
        "payload": {"source": "random_user", "country_code": "GB"},
    })

    valid_delivery_report = producer.publish_sync(
        topic=users_created_topic,
        key=valid_event.source_user_id,
        value=valid_event,
        timeout=30.0,
    )
    invalid_delivery_report = producer.publish_sync(
        topic=users_created_invalid_topic,
        key=invalid_event.source_user_id,
        value=invalid_event,
        timeout=30.0,
    )
    batch_delivery_reports = producer.publish_batch_sync(
        topic=users_created_topic,
        records=(
            KafkaProducerRecord(
                key="api-user-2",
                value=UserCreated.model_validate(_valid_user_created_payload(source_user_id="api-user-2")),
            ),
            KafkaProducerRecord(
                key="api-user-3",
                value=UserCreated.model_validate(_valid_user_created_payload(source_user_id="api-user-3")),
            ),
        ),
        timeout=30.0,
    )
    producer.close(timeout=30.0)

    consumed_valid_message = _consume_single_message(
        topic=users_created_topic,
        partition=valid_delivery_report.partition,
        group_id=f"it-users-created-{topic_suffix}",
        expected_key=b"api-user-1",
    )
    consumed_invalid_message = _consume_single_message(
        topic=users_created_invalid_topic,
        partition=invalid_delivery_report.partition,
        group_id=f"it-users-created-invalid-{topic_suffix}",
        expected_key=("sha256:" + "a" * 64).encode(),
    )
    created_schema = _latest_schema_for_subject(users_created_subject)
    invalid_schema = _latest_schema_for_subject(users_created_invalid_subject)

    assert set(registered_schema_ids) == {users_created_subject, users_created_invalid_subject}
    assert _subject_versions(users_created_subject) == [1]
    assert _subject_versions(users_created_invalid_subject) == [1]
    assert _topic_exists(users_created_topic)
    assert _topic_exists(users_created_invalid_topic)
    assert valid_delivery_report.topic == users_created_topic
    assert invalid_delivery_report.topic == users_created_invalid_topic
    assert {report.topic for report in batch_delivery_reports} == {users_created_topic}
    assert len(batch_delivery_reports) == 2
    assert consumed_valid_message.value().startswith(b"\x00")
    assert b'"event_type":"UserCreated"' in consumed_valid_message.value()
    assert b'"source_user_id":"api-user-1"' in consumed_valid_message.value()
    assert consumed_invalid_message.value().startswith(b"\x00")
    assert b'"event_type":"UserProfileInvalid"' in consumed_invalid_message.value()
    assert b'"rejection_reasons":["invalid_email"]' in consumed_invalid_message.value()
    assert created_schema["title"] == "UserCreated"
    assert invalid_schema["title"] == "UserProfileInvalid"


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


def _latest_schema_for_subject(subject: str) -> dict[str, Any]:
    payload = _schema_registry_json(f"/subjects/{subject}/versions/latest")
    return cast("dict[str, Any]", json.loads(payload["schema"]))


def _subject_versions(subject: str) -> list[int]:
    return cast("list[int]", _schema_registry_json(f"/subjects/{subject}/versions"))


def _schema_registry_json(path: str) -> Any:
    with urlopen(f"{SCHEMA_REGISTRY_URL}{path}", timeout=5.0) as response:
        return json.loads(response.read().decode("utf-8"))


def _topic_exists(topic: str) -> bool:
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin_client.list_topics(topic=topic, timeout=10.0)
    return topic in metadata.topics and metadata.topics[topic].error is None


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
