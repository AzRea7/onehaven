# backend/app/routers/metrics.py
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from app.services.runtime_metrics import METRICS

router = APIRouter(prefix="/metrics", tags=["ops"])


@router.get("", response_class=PlainTextResponse)
def metrics():
    # Prometheus text-ish format
    lines = []
    for k, v in METRICS.snapshot().items():
        lines.append(f"{k} {v}")
    return "\n".join(lines) + "\n"