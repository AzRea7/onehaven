# backend/app/middleware/request_id.py
from __future__ import annotations

import uuid
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


class RequestIdMiddleware(BaseHTTPMiddleware):
    """
    Adds:
      - request.state.request_id
      - X-Request-Id response header

    Accepts incoming X-Request-Id if provided (useful behind proxies).
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        rid = request.headers.get("X-Request-Id") or str(uuid.uuid4())
        request.state.request_id = rid

        resp = await call_next(request)
        resp.headers["X-Request-Id"] = rid
        return resp
    