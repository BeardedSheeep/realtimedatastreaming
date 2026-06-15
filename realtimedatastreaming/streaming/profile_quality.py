# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal

from pydantic import ValidationError
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from realtimedatastreaming.ingestion.quality import validate_user_profile_quality
from realtimedatastreaming.ingestion.schemas import UserCreated

QUALITY_VALID: Literal["valid"] = "valid"
QUALITY_INVALID: Literal["invalid"] = "invalid"
QUALITY_NOT_EVALUATED: Literal["not_evaluated"] = "not_evaluated"
QualityStatus = Literal["valid", "invalid", "not_evaluated"]


@dataclass(frozen=True, slots=True)
class ProfileQualityResult:
    status: QualityStatus
    rejection_reasons: tuple[str, ...]


PROFILE_QUALITY_RESULT_SCHEMA = StructType([
    StructField("status", StringType(), nullable=False),
    StructField("rejection_reasons", ArrayType(StringType(), containsNull=False), nullable=False),
])


def evaluate_profile_quality(
    payload_json: str | None,
    *,
    deserialization_rejection_reason: str | None = None,
    today: date | None = None,
) -> ProfileQualityResult:
    """Apply domain quality rules only to successfully deserialized profiles."""
    if deserialization_rejection_reason is not None or payload_json is None:
        return ProfileQualityResult(QUALITY_NOT_EVALUATED, ())

    try:
        profile = UserCreated.model_validate_json(payload_json)
    except ValidationError:
        return ProfileQualityResult(QUALITY_NOT_EVALUATED, ())

    rejection_reasons = validate_user_profile_quality(profile, today=today)
    if rejection_reasons:
        return ProfileQualityResult(QUALITY_INVALID, rejection_reasons)
    return ProfileQualityResult(QUALITY_VALID, ())


def apply_profile_quality_rules(dataframe: DataFrame, today: date | None = None) -> DataFrame:
    """Add deterministic quality status and rejection reasons to a stream."""
    reference_date = today or datetime.now(tz=UTC).date()

    @udf(returnType=PROFILE_QUALITY_RESULT_SCHEMA)
    def evaluate(
        payload_json: str | None,
        deserialization_rejection_reason: str | None,
    ) -> tuple[str, tuple[str, ...]]:
        result = evaluate_profile_quality(
            payload_json,
            deserialization_rejection_reason=deserialization_rejection_reason,
            today=reference_date,
        )
        return result.status, result.rejection_reasons

    evaluated = dataframe.withColumn(
        "profile_quality",
        evaluate(col("payload_json"), col("rejection_reason")),
    )
    return (
        evaluated
        .withColumn("quality_status", col("profile_quality.status"))
        .withColumn("quality_rejection_reasons", col("profile_quality.rejection_reasons"))
        .drop("profile_quality")
    )
