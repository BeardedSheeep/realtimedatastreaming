# Copyright (c) 2026 BeardedSheeep

from typing import Any

import pytest

from realtimedatastreaming.ingestion.random_user import RandomUserRateLimiter


def test_random_user_rate_limiter_enforces_two_requests_per_second(fake_clock: Any) -> None:
    sleep_calls: list[float] = []
    rate_limiter = RandomUserRateLimiter(
        requests_per_second=2.0,
        sleep=sleep_calls.append,
        monotonic=fake_clock.monotonic,
    )

    rate_limiter.wait()
    rate_limiter.wait()

    assert sleep_calls == [0.5]


def test_random_user_rate_limiter_rejects_invalid_rate() -> None:
    rate_limiter = RandomUserRateLimiter(requests_per_second=0)

    with pytest.raises(ValueError, match="requests_per_second"):
        rate_limiter.wait()
