import json
import logging
from typing import Any

from template_doc.observability import configure_observability, set_correlation_context
from template_doc.settings import Settings


def test_configure_observability_emits_structured_json(capsys: Any) -> None:
    settings = Settings()
    configure_observability(settings)
    set_correlation_context(request_id_value="request-123", trace_id_value="trace-456")

    logging.getLogger("template_doc.test").info("application_started")

    captured = capsys.readouterr()
    payload = json.loads(captured.err)

    assert payload["level"] == "info"
    assert payload["logger"] == "template_doc.test"
    assert payload["message"] == "application_started"
    assert payload["service"] == "template-doc"
    assert payload["environment"] == "development"
    assert payload["version"] == "0.1.0"
    assert payload["request_id"] == "request-123"
    assert payload["trace_id"] == "trace-456"
    assert "timestamp" in payload


def test_configure_observability_supports_text_logs(capsys: Any) -> None:
    settings = Settings(LOG_FORMAT="text")
    configure_observability(settings)

    logging.getLogger("template_doc.test").warning("plain_message")

    captured = capsys.readouterr()
    assert "WARNING [template_doc.test] plain_message" in captured.err


def test_configure_observability_preserves_external_handlers() -> None:
    root_logger = logging.getLogger()
    external_handler = logging.NullHandler()
    root_logger.addHandler(external_handler)

    try:
        configure_observability(Settings())
        configure_observability(Settings())

        assert external_handler in root_logger.handlers
        managed_handlers = [
            handler for handler in root_logger.handlers if getattr(handler, "_template_doc_managed_handler", False)
        ]
        assert len(managed_handlers) == 1
    finally:
        root_logger.removeHandler(external_handler)


def test_structured_exception_log_includes_exception(capsys: Any) -> None:
    settings = Settings()
    configure_observability(settings)

    logger = logging.getLogger("template_doc.test")
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        logger.exception("application_failed")

    captured = capsys.readouterr()
    payload = json.loads(captured.err)

    assert payload["message"] == "application_failed"
    assert "RuntimeError: boom" in payload["exception"]
