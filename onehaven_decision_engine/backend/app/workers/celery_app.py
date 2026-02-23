# onehaven_decision_engine/backend/app/workers/celery_app.py
from __future__ import annotations

import os
from celery import Celery

BROKER = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

celery_app = Celery(
    "onehaven",
    broker=BROKER,
    backend=BACKEND,
    include=["app.workers.agent_tasks"],
)

# Best-practice defaults:
celery_app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,   # avoid a single worker hoarding jobs
    task_reject_on_worker_lost=True,
    task_track_started=True,
    timezone="UTC",
)

# One dedicated queue for agents (keeps future queues clean: "imports", "emails", etc.)
celery_app.conf.task_routes = {
    "app.workers.agent_tasks.*": {"queue": "agents"},
}