# backend/app/logging_config.py
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from .middleware.request_id import get_request_id


class JsonFormatter(logging.Formatter):
    """
    Minimal JSON formatter.
    Includes request_id (if present), level, message, logger, timestamp, exception.
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

        # Optional structured extras (only if set on record)
        for k in ("org_id", "user_id", "property_id", "run_id", "agent_key"):
            if hasattr(record, k):
                payload[k] = getattr(record, k)

        return json.dumps(payload, ensure_ascii=False)


def configure_logging() -> None:
    level = (os.getenv("LOG_LEVEL") or "INFO").upper()

    root = logging.getLogger()
    root.setLevel(level)

    # Clear existing handlers (important for uvicorn reload)
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter())

    root.addHandler(handler)

    # Keep access logs consistent; allow SQL log tuning separately
    logging.getLogger("uvicorn.access").setLevel(level)
    logging.getLogger("sqlalchemy.engine").setLevel((os.getenv("SQL_LOG_LEVEL") or "WARNING").upper())
    