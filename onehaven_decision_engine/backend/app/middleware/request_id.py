# backend/app/middleware/request_id.py
from __future__ import annotations

import uuid
from contextvars import ContextVar
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

request_id_ctx: ContextVar[str | None] = ContextVar("request_id", default=None)


def get_request_id() -> str | None:
    return request_id_ctx.get()


class RequestIDMiddleware(BaseHTTPMiddleware):
    """
    Sets a per-request id and returns it in response headers.

    - Accepts incoming X-Request-ID (preferred) or X-Request-Id (common variant)
    - Otherwise generates UUID4
    - Stores in ContextVar so logging can retrieve it anywhere
    """

    header_out = "X-Request-ID"

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        rid = request.headers.get("X-Request-ID") or request.headers.get("X-Request-Id")
        if not rid:
            rid = str(uuid.uuid4())

        token = request_id_ctx.set(rid)
        try:
            resp = await call_next(request)
            resp.headers[self.header_out] = rid
            return resp
        finally:
            request_id_ctx.reset(token)
            