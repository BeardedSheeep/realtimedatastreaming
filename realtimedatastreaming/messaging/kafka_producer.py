from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pydantic import BaseModel

from realtimedatastreaming.ingestion.schema_registry import (
    KafkaJsonSchemaContract,
    kafka_value_contracts_for_topics,
)
from realtimedatastreaming.settings import Settings

logger = logging.getLogger(__name__)

KafkaDeliveryCallback = Callable[[KafkaError | None, Message], None]
KafkaMessageValue = BaseModel | Mapping[str, Any]
DEFAULT_QUEUE_FULL_RETRIES = 3
QUEUE_FULL_POLL_TIMEOUT_SECONDS = 0.1


class KafkaPublicationError(Exception):
    """Raised when a Kafka publication cannot be accepted or delivered."""

    def __init__(self, message: str, *, topic: str, reason: str) -> None:
        super().__init__(message)
        self.topic = topic
        self.reason = reason


@dataclass(frozen=True, slots=True)
class KafkaDeliveryReport:
    topic: str
    partition: int
    offset: int


class KafkaProducerProtocol(Protocol):
    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes | None = None,
        on_delivery: KafkaDeliveryCallback | None = None,
    ) -> None: ...

    def flush(self, timeout: float = -1.0) -> int: ...

    def poll(self, timeout: float = 0.0) -> int: ...


class KafkaValueSerializerProtocol(Protocol):
    def serialize(self, *, topic: str, value: KafkaMessageValue) -> bytes: ...


class ConfluentJsonSerializerProtocol(Protocol):
    def __call__(self, obj: object, ctx: SerializationContext | None = None) -> bytes | None: ...


class SchemaRegistryValueSerializer:
    def __init__(self, serializers_by_topic: Mapping[str, ConfluentJsonSerializerProtocol]) -> None:
        self._serializers_by_topic = dict(serializers_by_topic)

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        auto_register_schemas: bool = False,
    ) -> SchemaRegistryValueSerializer:
        schema_registry_client = SchemaRegistryClient(_schema_registry_config_from_settings(settings))
        users_created_contract, users_created_invalid_contract = kafka_value_contracts_for_topics(
            users_created_topic=settings.kafka_users_created_topic,
            users_created_invalid_topic=settings.kafka_users_created_invalid_topic,
        )
        contracts_by_topic = {
            settings.kafka_users_created_topic: users_created_contract,
            settings.kafka_users_created_invalid_topic: users_created_invalid_contract,
        }
        return cls({
            topic: _build_json_serializer(
                contract=contract,
                schema_registry_client=schema_registry_client,
                auto_register_schemas=auto_register_schemas,
            )
            for topic, contract in contracts_by_topic.items()
        })

    def serialize(self, *, topic: str, value: KafkaMessageValue) -> bytes:
        serializer = self._serializers_by_topic.get(topic)
        if serializer is None:
            msg = f"No Schema Registry value contract configured for Kafka topic: {topic}"
            raise ValueError(msg)

        serialized_value = serializer(
            _to_json_serializable_mapping(value),
            SerializationContext(topic, MessageField.VALUE),
        )
        if serialized_value is None:
            msg = "Kafka value serializer returned None for a non-null event"
            raise ValueError(msg)
        return serialized_value


class UserProfileEventProducer:
    """Small application wrapper around the Confluent Kafka producer."""

    def __init__(
        self,
        producer: KafkaProducerProtocol,
        value_serializer: KafkaValueSerializerProtocol,
        *,
        queue_full_retries: int = DEFAULT_QUEUE_FULL_RETRIES,
        queue_full_poll_timeout_seconds: float = QUEUE_FULL_POLL_TIMEOUT_SECONDS,
    ) -> None:
        if queue_full_retries < 0:
            msg = "queue_full_retries must be greater than or equal to zero"
            raise ValueError(msg)
        if queue_full_poll_timeout_seconds <= 0:
            msg = "queue_full_poll_timeout_seconds must be greater than zero"
            raise ValueError(msg)
        self._producer = producer
        self._value_serializer = value_serializer
        self._queue_full_retries = queue_full_retries
        self._queue_full_poll_timeout_seconds = queue_full_poll_timeout_seconds

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        auto_register_schemas: bool = False,
    ) -> UserProfileEventProducer:
        return cls(
            Producer(_producer_config_from_settings(settings)),
            SchemaRegistryValueSerializer.from_settings(settings, auto_register_schemas=auto_register_schemas),
        )

    def publish(
        self,
        *,
        topic: str,
        value: KafkaMessageValue,
        key: str | bytes | None = None,
        on_delivery: KafkaDeliveryCallback | None = None,
    ) -> None:
        encoded_key = _encode_key(key)
        encoded_value = self._value_serializer.serialize(topic=topic, value=value)
        self._produce_with_backpressure_retry(
            topic=topic,
            key=encoded_key,
            value=encoded_value,
            on_delivery=on_delivery or _log_delivery_result,
        )
        self._producer.poll(0.0)

    def publish_sync(
        self,
        *,
        topic: str,
        value: KafkaMessageValue,
        key: str | bytes | None = None,
        timeout: float | None = None,
    ) -> KafkaDeliveryReport:
        delivery_error: KafkaError | None = None
        delivery_report: KafkaDeliveryReport | None = None

        def capture_delivery_result(error: KafkaError | None, message: Message) -> None:
            nonlocal delivery_error, delivery_report

            if error is not None:
                delivery_error = error
                return
            topic = message.topic()
            partition = message.partition()
            offset = message.offset()
            if topic is None or partition is None or offset is None:
                return
            delivery_report = KafkaDeliveryReport(
                topic=topic,
                partition=partition,
                offset=offset,
            )

        self.publish(topic=topic, value=value, key=key, on_delivery=capture_delivery_result)
        self.flush(timeout)

        if delivery_error is not None:
            msg = f"Kafka publication failed for topic {topic}: {delivery_error}"
            raise KafkaPublicationError(msg, topic=topic, reason="delivery_failed")
        if delivery_report is None:
            msg = f"Kafka publication delivery was not acknowledged for topic {topic}"
            raise KafkaPublicationError(msg, topic=topic, reason="delivery_not_acknowledged")
        return delivery_report

    def flush(self, timeout: float | None = None) -> int:
        if timeout is None:
            return self._producer.flush()
        return self._producer.flush(timeout)

    def close(self, timeout: float | None = None) -> int:
        return self.flush(timeout)

    def _produce_with_backpressure_retry(
        self,
        *,
        topic: str,
        value: bytes,
        key: bytes | None,
        on_delivery: KafkaDeliveryCallback,
    ) -> None:
        attempts = self._queue_full_retries + 1
        for attempt in range(attempts):
            try:
                self._producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
                return
            except BufferError as exc:
                if attempt == self._queue_full_retries:
                    msg = f"Kafka producer queue is full for topic {topic}"
                    raise KafkaPublicationError(msg, topic=topic, reason="producer_queue_full") from exc
                self._producer.poll(self._queue_full_poll_timeout_seconds)


def _producer_config_from_settings(settings: Settings) -> dict[str, str]:
    config = {
        "bootstrap.servers": ",".join(settings.kafka_bootstrap_servers),
        "client.id": settings.app_name,
        "enable.idempotence": "true",
        "acks": "all",
        "retries": "10",
        "delivery.timeout.ms": "120000",
        "request.timeout.ms": "30000",
        "compression.type": "snappy",
    }

    if settings.kafka_sasl_username is not None and settings.kafka_sasl_password is not None:
        config.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": settings.kafka_sasl_username,
            "sasl.password": settings.kafka_sasl_password.get_secret_value(),
        })

    return config


def _schema_registry_config_from_settings(settings: Settings) -> dict[str, str]:
    config = {"url": str(settings.schema_registry_url)}

    if settings.schema_registry_basic_auth_user_info is not None:
        config.update({
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": settings.schema_registry_basic_auth_user_info.get_secret_value(),
        })
    if settings.schema_registry_ssl_ca_location is not None:
        config["ssl.ca.location"] = settings.schema_registry_ssl_ca_location

    return config


def _build_json_serializer(
    *,
    contract: KafkaJsonSchemaContract,
    schema_registry_client: SchemaRegistryClient,
    auto_register_schemas: bool,
) -> JSONSerializer:
    return cast(
        "JSONSerializer",
        JSONSerializer(
            Schema(contract.schema_text(), schema_type=contract.schema_type),
            schema_registry_client,
            conf={
                "auto.register.schemas": auto_register_schemas,
                "subject.name.strategy": topic_subject_name_strategy,
                "validate": True,
            },
        ),
    )


def _to_json_serializable_mapping(value: KafkaMessageValue) -> Mapping[str, Any]:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return value


def _encode_key(key: str | bytes | None) -> bytes | None:
    if key is None or isinstance(key, bytes):
        return key
    return key.encode("utf-8")


def _log_delivery_result(error: KafkaError | None, message: Message) -> None:
    if error is not None:
        logger.error("Kafka publication failed", extra={"topic": message.topic(), "error": str(error)})
        return

    logger.info(
        "Kafka publication succeeded",
        extra={
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
        },
    )
