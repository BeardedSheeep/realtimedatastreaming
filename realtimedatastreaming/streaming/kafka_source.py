# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from realtimedatastreaming.settings import Settings

KAFKA_SOURCE_COLUMNS = (
    "key",
    "framed_value",
    "topic",
    "partition",
    "offset",
    "kafka_timestamp",
    "timestamp_type",
)


def build_users_created_stream(spark: SparkSession, settings: Settings) -> DataFrame:
    """Read Schema Registry-framed values from the configured Kafka topic.

    The value remains binary so the Confluent magic byte and schema ID can be
    validated before JSON deserialization.
    """
    reader = spark.readStream.format("kafka")
    for key, value in _kafka_source_options(settings).items():
        reader = reader.option(key, value)

    return reader.load().selectExpr(
        "key AS key",
        "value AS framed_value",
        "topic AS topic",
        "partition AS partition",
        "offset AS offset",
        "timestamp AS kafka_timestamp",
        "timestampType AS timestamp_type",
    )


def _kafka_source_options(settings: Settings) -> dict[str, str]:
    return {
        **kafka_connection_options(settings),
        "kafka.bootstrap.servers": ",".join(settings.kafka_bootstrap_servers),
        "subscribe": settings.kafka_users_created_topic,
        "startingOffsets": "earliest",
    }


def kafka_connection_options(settings: Settings) -> dict[str, str]:
    options: dict[str, str] = {
        "kafka.bootstrap.servers": ",".join(settings.kafka_bootstrap_servers),
    }
    if settings.kafka_sasl_username is not None and settings.kafka_sasl_password is not None:
        username = _escape_jaas_value(settings.kafka_sasl_username)
        password = _escape_jaas_value(settings.kafka_sasl_password.get_secret_value())
        options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{username}" password="{password}";'
            ),
        })
    return options


def _escape_jaas_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')
