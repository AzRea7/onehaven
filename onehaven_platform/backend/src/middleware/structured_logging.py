# backend/app/middleware/structured_logging.py
from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Any, Callable, Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

log = logging.getLogger("onehaven.request")


def _iso_utc_now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _safe_json_default(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def emit_structured_log(logger_name: str, payload: dict[str, Any], level: int = logging.INFO) -> None:
    logger = logging.getLogger(logger_name)
    body = dict(payload)
    body.setdefault("ts", _iso_utc_now())

    try:
        logger.log(level, json.dumps(body, default=_safe_json_default))
    except Exception:
        logger.log(level, str(body))


def _request_org_slug(request: Request) -> str | None:
    return (request.headers.get("X-Org-Slug") or request.query_params.get("org_slug") or "").strip() or None


def _request_user_email(request: Request) -> str | None:
    return (request.headers.get("X-User-Email") or request.query_params.get("user_email") or "").strip() or None


def _get_request_state_value(request: Request, attr: str) -> Any:
    try:
        return getattr(request.state, attr, None)
    except Exception:
        return None


def _principal_fields(request: Request) -> dict[str, Any]:
    principal = _get_request_state_value(request, "principal")
    if principal is None:
        return {
            "org_id": _get_request_state_value(request, "org_id"),
            "principal_id": _get_request_state_value(request, "principal_id"),
            "principal_type": _get_request_state_value(request, "principal_type"),
            "principal_role": _get_request_state_value(request, "principal_role"),
            "api_key_id": _get_request_state_value(request, "api_key_id"),
        }

    return {
        "org_id": getattr(principal, "org_id", None),
        "principal_id": getattr(principal, "user_id", None),
        "principal_type": getattr(principal, "principal_type", None),
        "principal_role": getattr(principal, "role", None),
        "api_key_id": getattr(principal, "api_key_id", None),
    }


class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    """
    Emits one structured JSON line per HTTP request.

    Fields:
      ts, event, request_id, method, route, path, query, status_code, latency_ms,
      org_slug, org_id, principal_id, principal_type, principal_role, api_key_id,
      user_email, outcome, error_class
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        t0 = time.time()

        request_id: Optional[str] = _get_request_state_value(request, "request_id")
        org_slug = _request_org_slug(request)
        user_email = _request_user_email(request)

        status_code = 500
        outcome = "error"
        error_class: str | None = None
        route_path: str | None = None

        try:
            response = await call_next(request)
            status_code = response.status_code
            outcome = "success" if status_code < 400 else "error"

            try:
                route = request.scope.get("route")
                route_path = getattr(route, "path", None)
            except Exception:
                route_path = None

            return response
        except Exception as exc:
            error_class = type(exc).__name__
            raise
        finally:
            latency_ms = int((time.time() - t0) * 1000)
            payload = {
                "event": "http_request",
                "request_id": request_id,
                "method": request.method,
                "route": route_path,
                "path": request.url.path,
                "query": str(request.url.query) if request.url.query else "",
                "status_code": status_code,
                "latency_ms": latency_ms,
                "org_slug": org_slug,
                "user_email": user_email,
                "outcome": outcome,
                "error_class": error_class,
            }
            payload.update(_principal_fields(request))
            emit_structured_log("onehaven.request", payload)
            