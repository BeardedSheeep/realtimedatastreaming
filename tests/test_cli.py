import json
from typing import Any

import pytest

from template_doc.cli import main
from template_doc.settings import get_settings


def test_cli_emits_user_output_and_structured_logs(capsys: Any, monkeypatch: Any) -> None:
    monkeypatch.setenv("LOG_FORMAT", "json")
    get_settings.cache_clear()

    main()

    captured = capsys.readouterr()
    logs = [json.loads(line) for line in captured.err.splitlines()]

    assert captured.out == "template-doc [development]\n"
    assert [log["message"] for log in logs] == ["application_started", "application_finished"]
    assert logs[0]["request_id"] == logs[1]["request_id"]
    assert logs[0]["trace_id"] is None

    get_settings.cache_clear()


def test_cli_logs_failure_before_reraising(capsys: Any, monkeypatch: Any) -> None:
    def broken_print(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("print failed")

    monkeypatch.setenv("LOG_FORMAT", "json")
    monkeypatch.setattr("template_doc.cli._print_user_output", broken_print)
    get_settings.cache_clear()

    with pytest.raises(RuntimeError, match="print failed"):
        main()

    captured = capsys.readouterr()
    logs = [json.loads(line) for line in captured.err.splitlines()]

    assert [log["message"] for log in logs] == ["application_started", "application_failed"]
    assert "RuntimeError: print failed" in logs[1]["exception"]

    get_settings.cache_clear()
