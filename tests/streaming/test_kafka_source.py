# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from pyspark.sql import DataFrame, SparkSession

from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming.kafka_source import KAFKA_SOURCE_COLUMNS, build_users_created_stream


class FakeKafkaReader:
    def __init__(self) -> None:
        self.source_format: str | None = None
        self.options: dict[str, str] = {}
        self.selected_expressions: tuple[str, ...] = ()
        self.dataframe = cast(DataFrame, object())

    def format(self, source_format: str) -> FakeKafkaReader:
        self.source_format = source_format
        return self

    def option(self, key: str, value: str) -> FakeKafkaReader:
        self.options[key] = value
        return self

    def load(self) -> FakeKafkaReader:
        return self

    def selectExpr(self, *expressions: str) -> DataFrame:
        self.selected_expressions = expressions
        return self.dataframe


@dataclass
class FakeSparkSession:
    readStream: FakeKafkaReader


def test_build_users_created_stream_reads_configured_kafka_topic() -> None:
    settings = Settings(
        KAFKA_BOOTSTRAP_SERVERS="broker-a:9092,broker-b:9092",
        KAFKA_USERS_CREATED_TOPIC="events.users.created",
    )
    reader = FakeKafkaReader()
    spark = cast(SparkSession, FakeSparkSession(reader))

    dataframe = build_users_created_stream(spark, settings)

    assert dataframe is reader.dataframe
    assert reader.source_format == "kafka"
    assert reader.options == {
        "kafka.bootstrap.servers": "broker-a:9092,broker-b:9092",
        "subscribe": "events.users.created",
        "startingOffsets": "earliest",
    }


def test_build_users_created_stream_preserves_framed_value_and_kafka_metadata() -> None:
    reader = FakeKafkaReader()
    spark = cast(SparkSession, FakeSparkSession(reader))

    build_users_created_stream(spark, Settings())

    assert reader.selected_expressions == tuple(
        f"{source} AS {target}"
        for source, target in (
            ("key", "key"),
            ("value", "framed_value"),
            ("topic", "topic"),
            ("partition", "partition"),
            ("offset", "offset"),
            ("timestamp", "kafka_timestamp"),
            ("timestampType", "timestamp_type"),
        )
    )
    assert KAFKA_SOURCE_COLUMNS == (
        "key",
        "framed_value",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "timestamp_type",
    )


def test_build_users_created_stream_configures_kafka_sasl_credentials() -> None:
    settings = Settings(
        KAFKA_SASL_USERNAME='stream"user',
        KAFKA_SASL_PASSWORD="stream\\password",
    )
    reader = FakeKafkaReader()
    spark = cast(SparkSession, FakeSparkSession(reader))

    build_users_created_stream(spark, settings)

    assert reader.options["kafka.security.protocol"] == "SASL_SSL"
    assert reader.options["kafka.sasl.mechanism"] == "PLAIN"
    assert reader.options["kafka.sasl.jaas.config"] == (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        'username="stream\\"user" password="stream\\\\password";'
    )
