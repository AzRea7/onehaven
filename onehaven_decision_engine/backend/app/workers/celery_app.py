# backend/app/workers/celery_app.py
from __future__ import annotations

from celery import Celery
from ..config import settings

broker = settings.celery_broker_url or "redis://redis:6379/0"
backend = settings.celery_result_backend or "redis://redis:6379/1"

celery_app = Celery("onehaven_agents", broker=broker, backend=backend)
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 120)),
)