# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession

from realtimedatastreaming.observability import configure_observability
from realtimedatastreaming.settings import Settings, get_settings
from realtimedatastreaming.streaming.kafka_routes import (
    OutputSchemaIds,
    resolve_output_schema_ids,
    start_kafka_routes,
)
from realtimedatastreaming.streaming.kafka_source import build_users_created_stream
from realtimedatastreaming.streaming.profile_quality import apply_profile_quality_rules
from realtimedatastreaming.streaming.user_created_deserializer import (
    deserialize_users_created_stream,
    resolve_supported_users_created_schema_id,
)

logger = logging.getLogger(__name__)
SparkSessionFactory = Callable[[Settings], SparkSession]
KafkaSourceFactory = Callable[[SparkSession, Settings], DataFrame]
SchemaIdResolver = Callable[[Settings], int]
StreamDeserializer = Callable[[DataFrame, Settings, int], DataFrame]
QualityTransformer = Callable[[DataFrame], DataFrame]
OutputSchemaIdResolver = Callable[[Settings], OutputSchemaIds]
KafkaRouter = Callable[[DataFrame, Settings, OutputSchemaIds], tuple[object, ...]]


def build_spark_session(settings: Settings) -> SparkSession:
    """Create the Spark session owned by the streaming job."""
    return (
        SparkSession.builder
        .appName(settings.spark_app_name)
        .master(settings.spark_master_url)
        .config("spark.jars.packages", settings.spark_kafka_package)
        .config("spark.sql.streaming.checkpointLocation", settings.spark_checkpoint_location)
        .getOrCreate()
    )


def run_streaming_job(
    settings: Settings,
    *,
    spark_session_factory: SparkSessionFactory = build_spark_session,
    kafka_source_factory: KafkaSourceFactory = build_users_created_stream,
    schema_id_resolver: SchemaIdResolver = resolve_supported_users_created_schema_id,
    stream_deserializer: StreamDeserializer = deserialize_users_created_stream,
    quality_transformer: QualityTransformer = apply_profile_quality_rules,
    output_schema_id_resolver: OutputSchemaIdResolver = resolve_output_schema_ids,
    kafka_router: KafkaRouter = start_kafka_routes,
) -> None:
    """Run the UserCreated validation and Kafka routing pipeline."""
    spark = spark_session_factory(settings)
    try:
        users_created_stream = kafka_source_factory(spark, settings)
        supported_schema_id = schema_id_resolver(settings)
        deserialized_stream = stream_deserializer(users_created_stream, settings, supported_schema_id)
        quality_stream = quality_transformer(deserialized_stream)
        output_schema_ids = output_schema_id_resolver(settings)
        queries = kafka_router(quality_stream, settings, output_schema_ids)
        logger.info(
            "spark_streaming_routes_started",
            extra={
                "spark_app_name": settings.spark_app_name,
                "spark_master_url": settings.spark_master_url,
                "spark_checkpoint_location": settings.spark_checkpoint_location,
                "kafka_topic": settings.kafka_users_created_topic,
                "streaming_source": quality_stream.isStreaming,
                "supported_schema_id": supported_schema_id,
                "streaming_query_count": len(queries),
            },
        )
        spark.streams.awaitAnyTermination()
    finally:
        spark.stop()
        logger.info("spark_streaming_job_stopped")


def main() -> None:
    settings = get_settings()
    configure_observability(settings)

    try:
        run_streaming_job(settings)
    except Exception:
        logger.exception("spark_streaming_job_failed")
        raise


if __name__ == "__main__":
    main()
