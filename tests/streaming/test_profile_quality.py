# Copyright (c) 2026 BeardedSheeep

from __future__ import annotations

import json
from datetime import date

from realtimedatastreaming.ingestion.quality import REJECTION_IMPLAUSIBLE_AGE, REJECTION_UNSUPPORTED_COUNTRY
from realtimedatastreaming.streaming.profile_quality import (
    QUALITY_INVALID,
    QUALITY_NOT_EVALUATED,
    QUALITY_VALID,
    evaluate_profile_quality,
)


def test_evaluate_profile_quality_accepts_valid_deserialized_profile() -> None:
    result = evaluate_profile_quality(
        json.dumps(_valid_payload()),
        today=date(2025, 1, 1),
    )

    assert result.status == QUALITY_VALID
    assert result.rejection_reasons == ()


def test_evaluate_profile_quality_reuses_all_domain_rejection_reasons() -> None:
    payload = _valid_payload()
    payload.update({
        "country": "Atlantis",
        "date_of_birth": "1890-01-01T00:00:00Z",
    })

    result = evaluate_profile_quality(
        json.dumps(payload),
        today=date(2025, 1, 1),
    )

    assert result.status == QUALITY_INVALID
    assert REJECTION_UNSUPPORTED_COUNTRY in result.rejection_reasons
    assert REJECTION_IMPLAUSIBLE_AGE in result.rejection_reasons


def test_evaluate_profile_quality_skips_deserialization_failures() -> None:
    result = evaluate_profile_quality(
        None,
        deserialization_rejection_reason="unsupported_schema_id",
        today=date(2025, 1, 1),
    )

    assert result.status == QUALITY_NOT_EVALUATED
    assert result.rejection_reasons == ()


def test_evaluate_profile_quality_defensively_skips_invalid_payload_json() -> None:
    result = evaluate_profile_quality("not-json", today=date(2025, 1, 1))

    assert result.status == QUALITY_NOT_EVALUATED
    assert result.rejection_reasons == ()


def _valid_payload() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "event_type": "UserCreated",
        "source": "random_user",
        "source_user_id": "source-42",
        "gender": None,
        "title": None,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "street_number": None,
        "street_name": None,
        "city": None,
        "state": None,
        "country": "United Kingdom",
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
