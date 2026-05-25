import logging
from typing import Any, cast

import pytest
from confluent_kafka.schema_registry import RegisteredSchema, Schema

from realtimedatastreaming.ingestion.schema_registry import USERS_CREATED_VALUE_CONTRACT, KafkaJsonSchemaContract
from realtimedatastreaming.ingestion.schemas import UserCreated
from realtimedatastreaming.messaging.kafka_producer import (
    KafkaDeliveryCallback,
    KafkaMessageValue,
    KafkaPublicationError,
    SchemaRegistryValueSerializer,
    UserProfileEventProducer,
    _producer_config_from_settings,
    _schema_registry_config_from_settings,
)
from realtimedatastreaming.settings import Settings


def test_publish_serializes_with_configured_schema_registry_contract_and_polls_producer() -> None:
    producer = _FakeKafkaProducer()
    serializer = _FakeValueSerializer()
    user_profile_producer = UserProfileEventProducer(producer, serializer)

    user_profile_producer.publish(
        topic="users_created",
        key="source-user-1",
        value={"source_user_id": "source-user-1", "event_type": "UserCreated"},
    )

    assert producer.produced_messages == [
        {
            "topic": "users_created",
            "key": b"source-user-1",
            "value": b"schema-registry-framed-value",
            "on_delivery": producer.produced_messages[0]["on_delivery"],
        }
    ]
    assert serializer.serialized_values == [
        ("users_created", {"source_user_id": "source-user-1", "event_type": "UserCreated"})
    ]
    assert producer.poll_calls == [0.0]


def test_publish_preserves_binary_keys_and_custom_delivery_callback() -> None:
    producer = _FakeKafkaProducer()
    serializer = _FakeValueSerializer()
    user_profile_producer = UserProfileEventProducer(producer, serializer)

    def on_delivery(_error: Any, _message: Any) -> None:
        return None

    user_profile_producer.publish(
        topic="users_created",
        key=b"source-user-1",
        value={"event_type": "UserCreated"},
        on_delivery=on_delivery,
    )

    assert producer.produced_messages[0]["key"] == b"source-user-1"
    assert producer.produced_messages[0]["on_delivery"] is on_delivery


def test_publish_retries_when_local_producer_queue_is_full() -> None:
    producer = _FakeKafkaProducer(buffer_errors_before_success=2)
    user_profile_producer = UserProfileEventProducer(
        producer,
        _FakeValueSerializer(),
        queue_full_retries=2,
        queue_full_poll_timeout_seconds=0.25,
    )

    user_profile_producer.publish(topic="users_created", value={"event_type": "UserCreated"})

    assert len(producer.produced_messages) == 1
    assert producer.poll_calls == [0.25, 0.25, 0.0]


def test_publish_raises_domain_error_when_local_producer_queue_stays_full() -> None:
    producer = _FakeKafkaProducer(buffer_errors_before_success=3)
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer(), queue_full_retries=1)

    with pytest.raises(KafkaPublicationError, match="Kafka producer queue is full") as exc_info:
        user_profile_producer.publish(topic="users_created", value={"event_type": "UserCreated"})

    assert exc_info.value.topic == "users_created"
    assert exc_info.value.reason == "producer_queue_full"
    assert producer.produced_messages == []


def test_publish_sync_returns_delivery_report_when_message_is_acknowledged() -> None:
    producer = _FakeKafkaProducer(delivery_error=None)
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())

    delivery_report = user_profile_producer.publish_sync(
        topic="users_created",
        value={"event_type": "UserCreated"},
        timeout=5.0,
    )

    assert delivery_report.topic == "users_created"
    assert delivery_report.partition == 2
    assert delivery_report.offset == 42
    assert producer.flush_calls == [5.0]


def test_publish_sync_raises_when_delivery_callback_reports_failure() -> None:
    producer = _FakeKafkaProducer(delivery_error=_FakeKafkaError("broker unavailable"))
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())

    with pytest.raises(KafkaPublicationError, match="broker unavailable") as exc_info:
        user_profile_producer.publish_sync(topic="users_created", value={"event_type": "UserCreated"})

    assert exc_info.value.topic == "users_created"
    assert exc_info.value.reason == "delivery_failed"


def test_publish_sync_raises_when_delivery_is_not_acknowledged() -> None:
    producer = _FakeKafkaProducer(run_delivery_callbacks_on_flush=False)
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())

    with pytest.raises(KafkaPublicationError, match="delivery was not acknowledged") as exc_info:
        user_profile_producer.publish_sync(topic="users_created", value={"event_type": "UserCreated"})

    assert exc_info.value.topic == "users_created"
    assert exc_info.value.reason == "delivery_not_acknowledged"


def test_close_flushes_producer() -> None:
    producer = _FakeKafkaProducer()
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())

    remaining_messages = user_profile_producer.close(timeout=5.0)

    assert remaining_messages == 0
    assert producer.flush_calls == [5.0]


def test_flush_without_timeout_uses_producer_default() -> None:
    producer = _FakeKafkaProducer()
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())

    remaining_messages = user_profile_producer.flush()

    assert remaining_messages == 0
    assert producer.flush_calls == [-1.0]


def test_schema_registry_value_serializer_rejects_topics_without_contract() -> None:
    serializer = SchemaRegistryValueSerializer({})

    try:
        serializer.serialize(topic="unknown_topic", value={"event_type": "UserCreated"})
    except ValueError as error:
        assert str(error) == "No Schema Registry value contract configured for Kafka topic: unknown_topic"
    else:
        raise AssertionError("Expected missing topic contract to be rejected")


def test_schema_registry_value_serializer_serializes_real_user_created_contract() -> None:
    serializer = SchemaRegistryValueSerializer({
        "users_created": _build_test_json_serializer(USERS_CREATED_VALUE_CONTRACT.schema_text())
    })
    user_created = UserCreated.model_validate(_valid_user_created_payload())

    serialized_value = serializer.serialize(topic="users_created", value=user_created)

    assert serialized_value.startswith(b"\x00")
    assert b'"event_type":"UserCreated"' in serialized_value
    assert b'"date_of_birth":"2000-01-01T00:00:00Z"' in serialized_value


def test_schema_registry_value_serializer_rejects_payloads_that_do_not_match_the_contract() -> None:
    serializer = SchemaRegistryValueSerializer({
        "users_created": _build_test_json_serializer(USERS_CREATED_VALUE_CONTRACT.schema_text())
    })

    with pytest.raises(Exception, match="unexpected"):
        serializer.serialize(
            topic="users_created",
            value={**_valid_user_created_payload(), "unexpected": "not-in-contract"},
        )


def test_schema_registry_value_serializer_rejects_serializers_returning_none() -> None:
    serializer = SchemaRegistryValueSerializer({"users_created": _NullValueSerializer()})

    with pytest.raises(ValueError, match="returned None"):
        serializer.serialize(topic="users_created", value={"event_type": "UserCreated"})


def test_producer_config_from_settings_enables_reliable_delivery_defaults() -> None:
    settings = Settings(APP_NAME="quality-stream", KAFKA_BOOTSTRAP_SERVERS="broker-a:9092,broker-b:9092")

    config = _producer_config_from_settings(settings)

    assert config == {
        "bootstrap.servers": "broker-a:9092,broker-b:9092",
        "client.id": "quality-stream",
        "enable.idempotence": "true",
        "acks": "all",
        "retries": "10",
        "delivery.timeout.ms": "120000",
        "request.timeout.ms": "30000",
        "compression.type": "snappy",
    }


def test_producer_config_from_settings_adds_sasl_when_credentials_are_configured() -> None:
    settings = Settings(KAFKA_SASL_USERNAME="stream-user", KAFKA_SASL_PASSWORD="stream-password")

    config = _producer_config_from_settings(settings)

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanisms"] == "PLAIN"
    assert config["sasl.username"] == "stream-user"
    assert config["sasl.password"] == "stream-password"


def test_schema_registry_config_from_settings_adds_optional_auth_and_tls() -> None:
    settings = Settings(
        SCHEMA_REGISTRY_URL="https://schema-registry.example",
        SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO="api-key:api-secret",
        SCHEMA_REGISTRY_SSL_CA_LOCATION="/etc/ssl/schema-registry-ca.pem",
    )

    config = _schema_registry_config_from_settings(settings)

    assert config == {
        "url": "https://schema-registry.example/",
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "api-key:api-secret",
        "ssl.ca.location": "/etc/ssl/schema-registry-ca.pem",
    }


def test_default_delivery_callback_logs_success(caplog: Any) -> None:
    producer = _FakeKafkaProducer()
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())
    user_profile_producer.publish(topic="users_created", value={"event_type": "UserCreated"})
    delivery_callback = producer.produced_messages[0]["on_delivery"]

    with caplog.at_level(logging.INFO, logger="realtimedatastreaming.messaging.kafka_producer"):
        delivery_callback(None, _FakeKafkaMessage(topic="users_created", partition=2, offset=42))

    assert caplog.messages == ["Kafka publication succeeded"]
    assert caplog.records[0].topic == "users_created"
    assert caplog.records[0].partition == 2
    assert caplog.records[0].offset == 42


def test_default_delivery_callback_logs_failure(caplog: Any) -> None:
    producer = _FakeKafkaProducer()
    user_profile_producer = UserProfileEventProducer(producer, _FakeValueSerializer())
    user_profile_producer.publish(topic="users_created_invalid", value={"event_type": "UserProfileInvalid"})
    delivery_callback = producer.produced_messages[0]["on_delivery"]

    with caplog.at_level(logging.ERROR, logger="realtimedatastreaming.messaging.kafka_producer"):
        delivery_callback(_FakeKafkaError("broker unavailable"), _FakeKafkaMessage(topic="users_created_invalid"))

    assert caplog.messages == ["Kafka publication failed"]
    assert caplog.records[0].topic == "users_created_invalid"
    assert caplog.records[0].error == "broker unavailable"


class _FakeValueSerializer:
    def __init__(self) -> None:
        self.serialized_values: list[tuple[str, KafkaMessageValue]] = []

    def serialize(self, *, topic: str, value: KafkaMessageValue) -> bytes:
        self.serialized_values.append((topic, value))
        return b"schema-registry-framed-value"


class _FakeKafkaProducer:
    def __init__(
        self,
        *,
        buffer_errors_before_success: int = 0,
        delivery_error: Any = None,
        run_delivery_callbacks_on_flush: bool = True,
    ) -> None:
        self.produced_messages: list[dict[str, Any]] = []
        self.flush_calls: list[float] = []
        self.poll_calls: list[float] = []
        self._buffer_errors_before_success = buffer_errors_before_success
        self._delivery_error = delivery_error
        self._run_delivery_callbacks_on_flush = run_delivery_callbacks_on_flush

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes | None = None,
        on_delivery: KafkaDeliveryCallback | None = None,
    ) -> None:
        if self._buffer_errors_before_success > 0:
            self._buffer_errors_before_success -= 1
            raise BufferError("queue full")
        self.produced_messages.append({
            "topic": topic,
            "key": key,
            "value": value,
            "on_delivery": on_delivery,
        })

    def flush(self, timeout: float = -1.0) -> int:
        self.flush_calls.append(timeout)
        if self._run_delivery_callbacks_on_flush:
            for message in self.produced_messages:
                callback = message["on_delivery"]
                if callback is not None:
                    callback(
                        self._delivery_error,
                        _FakeKafkaMessage(topic=message["topic"], partition=2, offset=42),
                    )
        return 0

    def poll(self, timeout: float = 0.0) -> int:
        self.poll_calls.append(timeout)
        return 0


class _FakeKafkaMessage:
    def __init__(self, *, topic: str, partition: int = 0, offset: int = 0) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


class _FakeKafkaError:
    def __init__(self, message: str) -> None:
        self._message = message

    def __str__(self) -> str:
        return self._message


class _NullValueSerializer:
    def __call__(self, obj: object, ctx: Any = None) -> bytes | None:
        return None


class _FakeSchemaRegistryClient:
    def register_schema(self, subject_name: str, schema: Schema, normalize_schemas: bool = False) -> int:
        return 1

    def lookup_schema(
        self,
        subject_name: str,
        schema: Schema,
        normalize_schemas: bool = False,
        fmt: str | None = None,
        deleted: bool = False,
    ) -> RegisteredSchema:
        return RegisteredSchema(subject_name, 1, 1, None, schema)

    def get_latest_version(self, subject_name: str, fmt: str | None = None) -> RegisteredSchema:
        msg = "get_latest_version should not be called in these tests"
        raise AssertionError(msg)


def _build_test_json_serializer(schema_text: str) -> Any:
    from realtimedatastreaming.messaging.kafka_producer import _build_json_serializer

    return _build_json_serializer(
        contract=cast("KafkaJsonSchemaContract", _TestKafkaJsonSchemaContract(schema_text)),
        schema_registry_client=cast(Any, _FakeSchemaRegistryClient()),
        auto_register_schemas=False,
    )


class _TestKafkaJsonSchemaContract:
    schema_type: str = "JSON"

    def __init__(self, schema_text: str) -> None:
        self._schema_text = schema_text

    def schema_text(self) -> str:
        return self._schema_text


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
