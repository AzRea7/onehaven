# backend/app/workers/agent_worker.py
from __future__ import annotations

from celery import Celery
from sqlalchemy import select

from app.db import SessionLocal
from app.models import AgentRun
from onehaven_platform.backend.src.services.agent_engine import execute_run_now
from app.config import settings

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
    },
)


def main(limit: int = 50) -> None:
    """
    Manual worker (CLI):
    - Useful in dev if you don't want celery running
    - Still respects idempotency + contract enforcement via execute_run_now
    """
    db = SessionLocal()
    try:
        runs = db.scalars(
            select(AgentRun)
            .where(AgentRun.status == "queued")
            .order_by(AgentRun.id.asc())
            .limit(limit)
        ).all()

        for r in runs:
            out = execute_run_now(
                db,
                org_id=int(r.org_id),
                run_id=int(r.id),
                attempt_number=int((r.attempts or 0) + 1),
            )
            print(f"[agent_worker] run_id={r.id} status={out.get('status')} ok={out.get('ok')}")
    finally:
        db.close()


__all__ = ["celery_app", "main"]


if __name__ == "__main__":
    main()