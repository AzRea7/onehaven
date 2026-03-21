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
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 120) or 120),
    timezone="UTC",
    enable_utc=True,
    task_default_queue=queue,
    task_default_routing_key=queue,
    imports=(
        "app.workers.agent_tasks",
        "app.tasks.ingestion_tasks",
    ),
    beat_schedule={
        "ingestion-sync-due-sources-every-15-minutes": {
            "task": "ingestion.sync_due_sources",
            "schedule": 15 * 60.0,
        },
        "ingestion-daily-market-refresh": {
            "task": "ingestion.daily_market_refresh",
            "schedule": 24 * 60 * 60.0,
        },
        "location-refresh-stale-properties": {
            "task": "location.refresh_stale_properties",
            "schedule": float(max(300, int(settings.location_refresh_schedule_minutes) * 60)),
        },
        "jurisdiction-refresh-stale-profiles": {
            "task": "jurisdiction.refresh_stale_profiles",
            "schedule": 12 * 60 * 60.0,
        },
        "jurisdiction-notify-stale-profiles": {
            "task": "jurisdiction.notify_stale_profiles",
            "schedule": 12 * 60 * 60.0,
        },
    },
)
