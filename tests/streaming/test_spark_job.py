# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import logging
from typing import Any, cast

import pytest
from pyspark.sql import DataFrame, SparkSession

from realtimedatastreaming.settings import Settings
from realtimedatastreaming.streaming import spark_job
from realtimedatastreaming.streaming.kafka_routes import OutputSchemaIds


class FakeSparkSession:
    def __init__(self) -> None:
        self.stop_calls = 0
        self.streams = FakeStreamingQueryManager()

    def stop(self) -> None:
        self.stop_calls += 1


class FakeStreamingDataFrame:
    isStreaming = True


class FakeStreamingQueryManager:
    def __init__(self) -> None:
        self.await_calls = 0

    def awaitAnyTermination(self) -> bool:
        self.await_calls += 1
        return True


def test_run_streaming_job_uses_settings_and_stops_session(caplog: Any) -> None:
    settings = Settings(
        SPARK_APP_NAME="profile-stream",
        SPARK_MASTER_URL="local[2]",
        SPARK_CHECKPOINT_LOCATION="file:///tmp/profile-stream",
    )
    spark = FakeSparkSession()
    received_settings: list[Settings] = []
    received_source_arguments: list[tuple[SparkSession, Settings]] = []
    received_deserializer_arguments: list[tuple[DataFrame, Settings, int]] = []
    received_quality_streams: list[DataFrame] = []
    received_routes: list[tuple[DataFrame, Settings, OutputSchemaIds]] = []
    source_stream = cast(DataFrame, FakeStreamingDataFrame())
    deserialized_stream = cast(DataFrame, FakeStreamingDataFrame())
    quality_stream = cast(DataFrame, FakeStreamingDataFrame())

    def create_spark(candidate: Settings) -> SparkSession:
        received_settings.append(candidate)
        return cast(SparkSession, spark)

    def create_source(candidate_spark: SparkSession, candidate_settings: Settings) -> DataFrame:
        received_source_arguments.append((candidate_spark, candidate_settings))
        return source_stream

    def deserialize_source(dataframe: DataFrame, candidate_settings: Settings, schema_id: int) -> DataFrame:
        received_deserializer_arguments.append((dataframe, candidate_settings, schema_id))
        return deserialized_stream

    def apply_quality(dataframe: DataFrame) -> DataFrame:
        received_quality_streams.append(dataframe)
        return quality_stream

    def route(
        dataframe: DataFrame,
        candidate_settings: Settings,
        output_schema_ids: OutputSchemaIds,
    ) -> tuple[object, ...]:
        received_routes.append((dataframe, candidate_settings, output_schema_ids))
        return object(), object()

    with caplog.at_level(logging.INFO, logger="realtimedatastreaming.streaming.spark_job"):
        spark_job.run_streaming_job(
            settings,
            spark_session_factory=create_spark,
            kafka_source_factory=create_source,
            schema_id_resolver=lambda _settings: 42,
            stream_deserializer=deserialize_source,
            quality_transformer=apply_quality,
            output_schema_id_resolver=lambda _settings: OutputSchemaIds(valid=43, invalid=44),
            kafka_router=route,
        )

    assert received_settings == [settings]
    assert received_source_arguments == [(cast(SparkSession, spark), settings)]
    assert received_deserializer_arguments == [(source_stream, settings, 42)]
    assert received_quality_streams == [deserialized_stream]
    assert received_routes == [(quality_stream, settings, OutputSchemaIds(valid=43, invalid=44))]
    assert spark.streams.await_calls == 1
    assert spark.stop_calls == 1
    assert [record.message for record in caplog.records] == [
        "spark_streaming_routes_started",
        "spark_streaming_job_stopped",
    ]


def test_run_streaming_job_stops_session_when_start_logging_fails(monkeypatch: Any) -> None:
    settings = Settings()
    spark = FakeSparkSession()

    def fail_on_info(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("logging failed")

    monkeypatch.setattr(spark_job.logger, "info", fail_on_info)

    with pytest.raises(RuntimeError, match="logging failed"):
        spark_job.run_streaming_job(
            settings,
            spark_session_factory=lambda _settings: cast(SparkSession, spark),
            kafka_source_factory=lambda _spark, _settings: cast(DataFrame, FakeStreamingDataFrame()),
            schema_id_resolver=lambda _settings: 42,
            stream_deserializer=lambda dataframe, _settings, _schema_id: dataframe,
            quality_transformer=lambda dataframe: dataframe,
            output_schema_id_resolver=lambda _settings: OutputSchemaIds(valid=43, invalid=44),
            kafka_router=lambda _dataframe, _settings, _schema_ids: (),
        )

    assert spark.stop_calls == 1


def test_main_configures_observability_and_runs_job(monkeypatch: Any) -> None:
    settings = Settings(SPARK_APP_NAME="entrypoint-test")
    calls: list[tuple[str, Settings]] = []

    monkeypatch.setattr(spark_job, "get_settings", lambda: settings)
    monkeypatch.setattr(
        spark_job,
        "configure_observability",
        lambda candidate: calls.append(("observability", candidate)),
    )
    monkeypatch.setattr(
        spark_job,
        "run_streaming_job",
        lambda candidate: calls.append(("job", candidate)),
    )

    spark_job.main()

    assert calls == [
        ("observability", settings),
        ("job", settings),
    ]


def test_main_logs_failure_before_reraising(monkeypatch: Any, caplog: Any) -> None:
    settings = Settings()

    monkeypatch.setattr(spark_job, "get_settings", lambda: settings)
    monkeypatch.setattr(spark_job, "configure_observability", lambda _settings: None)

    def fail_job(_settings: Settings) -> None:
        raise RuntimeError("spark failed")

    monkeypatch.setattr(spark_job, "run_streaming_job", fail_job)

    with (
        caplog.at_level(logging.ERROR, logger="realtimedatastreaming.streaming.spark_job"),
        pytest.raises(RuntimeError, match="spark failed"),
    ):
        spark_job.main()

    assert [record.message for record in caplog.records] == ["spark_streaming_job_failed"]
