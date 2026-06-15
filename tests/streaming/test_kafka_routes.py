# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import cast

from confluent_kafka.schema_registry import RegisteredSchema, Schema
from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming import kafka_routes
from realtimedatastreaming.streaming.kafka_routes import (
    build_invalid_event_json,
    frame_json_payload,
    resolve_output_schema_ids,
)


def test_resolve_output_schema_ids_uses_dedicated_topic_subjects() -> None:
    settings = Settings(
        KAFKA_USERS_CREATED_VALID_TOPIC="events.users.created.valid",
        KAFKA_USERS_CREATED_INVALID_TOPIC="events.users.created.invalid",
    )
    client = FakeSchemaRegistryClient()

    schema_ids = resolve_output_schema_ids(settings, schema_registry_client=client)

    assert schema_ids.valid == 101
    assert schema_ids.invalid == 102
    assert client.subjects == [
        "events.users.created.valid-value",
        "events.users.created.invalid-value",
    ]


def test_frame_json_payload_uses_confluent_wire_format() -> None:
    framed = frame_json_payload('{"event_type":"UserCreated"}', 42)

    assert framed[:1] == b"\x00"
    assert int.from_bytes(framed[1:5], byteorder="big") == 42
    assert framed[5:] == b'{"event_type":"UserCreated"}'


def test_build_invalid_event_json_is_privacy_safe_without_salt() -> None:
    invalid_json = build_invalid_event_json(
        json.dumps(_valid_payload()),
        deserialization_rejection_reason=None,
        quality_rejection_reasons=["unsupported_country"],
        pseudonymization_salt=None,
    )
    event = json.loads(invalid_json)

    assert event["source_user_id"] is None
    assert event["rejection_reasons"] == ["unsupported_country"]
    assert event["payload"] == {
        "schema_version": "1.0",
        "event_type": "UserCreated",
        "source": "random_user",
        "country_code": "GB",
    }
    assert "email" not in event["payload"]


def test_build_invalid_event_json_routes_technical_rejection_without_raw_payload() -> None:
    invalid_json = build_invalid_event_json(
        None,
        deserialization_rejection_reason="invalid_json_payload",
        quality_rejection_reasons=None,
        pseudonymization_salt=None,
    )
    event = json.loads(invalid_json)

    assert event["rejection_reasons"] == ["invalid_json_payload"]
    assert event["payload"] == {}
    assert event["source_user_id"] is None


def test_start_kafka_query_configures_topic_checkpoint_and_connection() -> None:
    writer = FakeStreamingWriter()
    dataframe = cast(DataFrame, FakeDataFrame(writer))
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS="broker-a:9092,broker-b:9092",
        SPARK_CHECKPOINT_LOCATION="s3://streaming-checkpoints/users",
    )

    query = kafka_routes._start_kafka_query(
        dataframe,
        settings,
        topic="events.users.created.valid",
        checkpoint_suffix="valid",
        query_name="users-created-valid",
    )

    assert query is writer.query
    assert writer.source_format == "kafka"
    assert writer.output_mode == "append"
    assert writer.query_name == "users-created-valid"
    assert writer.options == {
        "kafka.bootstrap.servers": "broker-a:9092,broker-b:9092",
        "topic": "events.users.created.valid",
        "checkpointLocation": "s3://streaming-checkpoints/users/valid",
    }
    assert writer.start_calls == 1


class FakeSchemaRegistryClient:
    def __init__(self) -> None:
        self.subjects: list[str] = []

    def lookup_schema(
        self,
        subject_name: str,
        schema: Schema,
        normalize_schemas: bool = False,
        fmt: str | None = None,
        deleted: bool = False,
    ) -> RegisteredSchema:
        self.subjects.append(subject_name)
        schema_id = 100 + len(self.subjects)
        return RegisteredSchema(subject_name, 1, schema_id, None, schema)


@dataclass
class FakeDataFrame:
    writeStream: FakeStreamingWriter


@dataclass
class FakeStreamingWriter:
    source_format: str | None = None
    output_mode: str | None = None
    query_name: str | None = None
    options: dict[str, str] = field(default_factory=dict)
    start_calls: int = 0
    query: StreamingQuery = field(default=cast(StreamingQuery, object()))

    def format(self, source_format: str) -> FakeStreamingWriter:
        self.source_format = source_format
        return self

    def outputMode(self, output_mode: str) -> FakeStreamingWriter:
        self.output_mode = output_mode
        return self

    def queryName(self, query_name: str) -> FakeStreamingWriter:
        self.query_name = query_name
        return self

    def option(self, key: str, value: str) -> FakeStreamingWriter:
        self.options[key] = value
        return self

    def start(self) -> StreamingQuery:
        self.start_calls += 1
        return self.query


def _valid_payload() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "event_type": "UserCreated",
        "source": "random_user",
        "source_user_id": "private-source-id",
        "gender": None,
        "title": None,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "street_number": None,
        "street_name": None,
        "city": None,
        "state": None,
        "country": "Atlantis",
        "country_code": "GB",
        "postcode": None,
        "latitude": None,
        "longitude": None,
        "timezone_offset": None,
        "timezone_description": None,
        "email": "ada@example.com",
        "username": "ada",
        "date_of_birth": "2000-01-01T00:00:00Z",
        "registered_at": None,
        "phone": None,
        "cell": None,
        "picture_large": None,
        "picture_medium": None,
        "picture_thumbnail": None,
        "nationality": None,
    }
