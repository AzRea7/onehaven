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
    try:
        log.info(json.dumps(payload, default=str))
    except Exception:
        log.info(str(payload))


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """
    Emits one JSON log line per request with:
      request_id, method, path, status_code, latency_ms, org_slug, user_email

    Assumes RequestIDMiddleware sets request.state.request_id.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        t0 = time.time()

        # Prefer headers, fall back to querystring (important for EventSource)
        org_slug = (request.headers.get("X-Org-Slug") or request.query_params.get("org_slug") or "").strip() or None
        user_email = (request.headers.get("X-User-Email") or request.query_params.get("user_email") or "").strip() or None

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
            