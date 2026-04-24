from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from onehaven_platform.backend.src.middleware.request_id import get_request_id


_STANDARD_LOG_RECORD_FIELDS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
    "asctime",
}


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        json.dumps(value)
        return value
    except Exception:
        return str(value)


class JsonFormatter(logging.Formatter):
    """
    JSON formatter with support for:
    - request_id
    - standard log record fields
    - arbitrary logging extras passed via extra={.}
    - exception and stack details
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        rid = get_request_id()
        if rid:
            payload["request_id"] = rid

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        # Common explicit keys that are especially useful for filtering
        for key in (
            "event",
            "org_id",
            "user_id",
            "property_id",
            "run_id",
            "agent_key",
            "route",
            "path",
            "method",
            "status_code",
            "pane",
            "step",
            "duration_ms",
            "query_ms",
            "build_ms",
            "total_ms",
            "limit",
            "count",
            "page_number",
            "source_id",
            "provider",
            "trigger_type",
            "idempotency_key",
        ):
            if hasattr(record, key):
                payload[key] = _json_safe(getattr(record, key))

        # Pull in all other extras automatically
        for key, value in record.__dict__.items():
            if key in _STANDARD_LOG_RECORD_FIELDS:
                continue
            if key in payload:
                continue
            if key.startswith("_"):
                continue
            payload[key] = _json_safe(value)

        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    level = (os.getenv("LOG_LEVEL") or "INFO").upper()

    root = logging.getLogger()
    root.setLevel(level)

    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter())

    root.addHandler(handler)

    logging.getLogger("uvicorn.access").setLevel(level)
    logging.getLogger("sqlalchemy.engine").setLevel((os.getenv("SQL_LOG_LEVEL") or "WARNING").upper())


def log_event(logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logger.log(level, event, extra=payload)


class LogTimer:
    """
    Context manager for timing a code block and logging it automatically.

    Example:
        with LogTimer(log, "properties_list_query", org_id=1, route="/properties"):
            rows = db.scalars(stmt).all()
    """

    def __init__(self, logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any) -> None:
        self.logger = logger
        self.event = event
        self.level = level
        self.fields = dict(fields)
        self.started = 0.0

    def __enter__(self) -> "LogTimer":
        self.started = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        duration_ms = round((time.perf_counter() - self.started) * 1000, 2)
        payload = dict(self.fields)
        payload["event"] = self.event
        payload["duration_ms"] = duration_ms

        if exc is not None:
            payload["error_type"] = exc_type.__name__ if exc_type else "Exception"
            payload["error"] = str(exc)
            self.logger.log(self.level, self.event, extra=payload, exc_info=True)
            return

        self.logger.log(self.level, self.event, extra=payload)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
