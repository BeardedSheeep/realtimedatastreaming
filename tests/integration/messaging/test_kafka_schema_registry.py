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

from realtimedatastreaming.ingestion.schema_registry import value_subject_for_topic
from realtimedatastreaming.ingestion.schemas import UserCreated
from realtimedatastreaming.messaging.kafka_producer import UserProfileEventProducer
from realtimedatastreaming.settings import Settings

pytestmark = pytest.mark.integration

KAFKA_BOOTSTRAP_SERVERS = "localhost:19092"
SCHEMA_REGISTRY_URL = "http://localhost:18081"
SERVICE_TIMEOUT_SECONDS = 90.0


def test_user_profile_event_producer_publishes_schema_registry_framed_event_to_kafka() -> None:
    _wait_for_kafka()
    _wait_for_schema_registry()

    topic_suffix = uuid4().hex
    users_created_topic = f"it.users.created.{topic_suffix}"
    users_created_invalid_topic = f"it.users.created.invalid.{topic_suffix}"
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS=KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_USERS_CREATED_TOPIC=users_created_topic,
        KAFKA_USERS_CREATED_INVALID_TOPIC=users_created_invalid_topic,
        SCHEMA_REGISTRY_URL=SCHEMA_REGISTRY_URL,
    )
    producer = UserProfileEventProducer.from_settings(settings, auto_register_schemas=True)
    event = UserCreated.model_validate(_valid_user_created_payload())

    delivery_report = producer.publish_sync(
        topic=users_created_topic,
        key=event.source_user_id,
        value=event,
        timeout=30.0,
    )

    consumed_message = _consume_single_message(
        topic=users_created_topic,
        partition=delivery_report.partition,
        group_id=f"it-users-created-{topic_suffix}",
    )
    schema = _latest_schema_for_subject(value_subject_for_topic(users_created_topic))

    assert delivery_report.topic == users_created_topic
    assert consumed_message.key() == b"api-user-1"
    assert consumed_message.value().startswith(b"\x00")
    assert b'"event_type":"UserCreated"' in consumed_message.value()
    assert b'"source_user_id":"api-user-1"' in consumed_message.value()
    assert schema["title"] == "UserCreated"


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


def _consume_single_message(*, topic: str, partition: int, group_id: str) -> Any:
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
            return message
    finally:
        consumer.close()
    pytest.fail(f"Message was not consumed from topic {topic} within 30s")


def _latest_schema_for_subject(subject: str) -> dict[str, Any]:
    with urlopen(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=5.0) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return cast("dict[str, Any]", json.loads(payload["schema"]))


def _valid_user_created_payload() -> dict[str, Any]:
    return {
        "source": "random_user",
        "source_user_id": "api-user-1",
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
