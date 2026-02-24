from __future__ import annotations

from celery import Celery

from ..config import settings

broker = getattr(settings, "celery_broker_url", None) or "redis://redis:6379/0"
backend = getattr(settings, "celery_result_backend", None) or "redis://redis:6379/1"
queue = getattr(settings, "celery_queue", None) or "celery"

celery_app = Celery(
    "onehaven_agents",
    broker=broker,
    backend=backend,
)

celery_app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",

    # Reliability
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,

    # Timeouts
    task_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 120)),

    # Time
    timezone="UTC",
    enable_utc=True,

    # Queue wiring
    task_default_queue=queue,
    task_default_routing_key=queue,

    # Task discovery
    imports=(
        "app.workers.agent_tasks",
    ),
)
