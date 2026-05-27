# Copyright (c) 2026 BeardedSheeep

from typing import Any

import pytest
from pydantic import ValidationError

from realtimedatastreaming.settings import Settings


def test_settings_reject_invalid_log_level(monkeypatch: Any) -> None:
    monkeypatch.setenv("LOG_LEVEL", "DEBG")

    with pytest.raises(ValidationError):
        Settings()
