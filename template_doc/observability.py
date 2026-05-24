from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from template_doc.settings import LOG_LEVELS, Settings

request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)
_MANAGED_HANDLER_ATTR = "_template_doc_managed_handler"


class JsonFormatter(logging.Formatter):
    """Format log records as structured JSON for log backends."""

    def __init__(self, *, service: str, environment: str, version: str) -> None:
        super().__init__()
        self.service = service
        self.environment = environment
        self.version = version

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service,
            "environment": self.environment,
            "version": self.version,
            "request_id": request_id.get(),
            "trace_id": trace_id.get(),
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_observability(settings: Settings) -> None:
    """Configure process-wide observability defaults.

    Backend integrations are intentionally left optional. Future services can
    initialize OpenTelemetry or Sentry here without changing application
    entrypoints.
    """
    logging.captureWarnings(capture=True)

    level = _log_level(settings.log_level)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    setattr(handler, _MANAGED_HANDLER_ATTR, True)

    if settings.log_format.lower() == "json":
        handler.setFormatter(
            JsonFormatter(
                service=settings.service_name,
                environment=settings.environment,
                version=settings.service_version,
            )
        )
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [
        existing_handler
        for existing_handler in root_logger.handlers
        if not getattr(existing_handler, _MANAGED_HANDLER_ATTR, False)
    ]
    root_logger.addHandler(handler)


def set_correlation_context(*, request_id_value: str | None = None, trace_id_value: str | None = None) -> None:
    """Set correlation identifiers for the current execution context."""
    request_id.set(request_id_value)
    trace_id.set(trace_id_value)


def _log_level(value: str) -> int:
    normalized_value = value.upper()
    if normalized_value not in LOG_LEVELS:
        msg = f"Unsupported log level: {value}"
        raise ValueError(msg)

    level = logging.getLevelName(normalized_value)
    if isinstance(level, int):
        return level

    msg = f"Unsupported log level: {value}"
    raise ValueError(msg)
