# backend/app/middleware/structured_logging.py
from __future__ import annotations

import json
import logging
import time
from typing import Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

log = logging.getLogger("onehaven.request")


def _json_log(payload: dict) -> None:
    # Keep it simple: one JSON line per request.
    # Works in Docker, works in CloudWatch, works in ELK, works everywhere.
    try:
        log.info(json.dumps(payload, default=str))
    except Exception:
        log.info(str(payload))


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """
    Emits one structured log line per request with:
      request_id, org_id, user_id, method, path, status_code, latency_ms

    Assumes RequestIdMiddleware sets request.state.request_id.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        t0 = time.time()

        # Best-effort principal extraction:
        # - in dev mode you use headers
        # - in jwt mode your auth dependency sets principal inside handlers
        # For "request log line", headers are good enough (and stable).
        org_slug = request.headers.get("X-Org-Slug")
        user_email = request.headers.get("X-User-Email")

        request_id: Optional[str] = getattr(request.state, "request_id", None)

        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            latency_ms = int((time.time() - t0) * 1000)

            _json_log(
                {
                    "event": "http_request",
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "query": str(request.url.query) if request.url.query else "",
                    "status_code": status_code,
                    "latency_ms": latency_ms,
                    "org_slug": org_slug,
                    "user_email": user_email,
                }
            )
            