# backend/app/workers/celery_app.py
from __future__ import annotations

import json
import logging
import os
import socket
from datetime import datetime
from typing import Any

from celery import Celery
from celery.signals import (
    beat_init,
    task_failure,
    task_postrun,
    task_prerun,
    task_retry,
    worker_process_init,
    worker_ready,
    worker_shutdown,
)

from ..config import settings

broker = getattr(settings, "celery_broker_url", None) or "redis://redis:6379/0"
backend = getattr(settings, "celery_result_backend", None) or "redis://redis:6379/1"
queue = getattr(settings, "celery_queue", None) or "celery"

worker_log = logging.getLogger("onehaven.worker")
task_log = logging.getLogger("onehaven.task")


def _iso_utc_now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _emit(logger: logging.Logger, payload: dict[str, Any], level: int = logging.INFO) -> None:
    body = dict(payload)
    body.setdefault("ts", _iso_utc_now())
    body.setdefault("host", socket.gethostname())
    body.setdefault("pid", os.getpid())

    try:
        logger.log(level, json.dumps(body, default=str))
    except Exception:
        logger.log(level, str(body))


def _beat_schedule() -> dict[str, dict]:
    return {
        "ingestion-sync-due-sources-every-15-minutes": {
            "task": "ingestion.sync_due_sources",
            "schedule": 15 * 60.0,
            "options": {"queue": queue, "routing_key": queue},
        },
        "ingestion-daily-market-refresh": {
            "task": "ingestion.daily_market_refresh",
            "schedule": 24 * 60 * 60.0,
            "options": {"queue": queue, "routing_key": queue},
        },
        "location-refresh-stale-properties": {
            "task": "location.refresh_stale_properties",
            "schedule": float(max(300, int(settings.location_refresh_schedule_minutes) * 60)),
            "options": {"queue": queue, "routing_key": queue},
        },
        "jurisdiction-refresh-stale-profiles": {
            "task": "jurisdiction.refresh_stale_profiles",
            "schedule": 12 * 60 * 60.0,
            "options": {"queue": queue, "routing_key": queue},
        },
        "jurisdiction-notify-stale-profiles": {
            "task": "jurisdiction.notify_stale_profiles",
            "schedule": 12 * 60 * 60.0,
            "options": {"queue": queue, "routing_key": queue},
        },
    }


celery_app = Celery(
    "onehaven_agents",
    broker=broker,
    backend=backend,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # safer worker behavior for background jobs
    task_acks_late=True,
    task_acks_on_failure_or_timeout=False,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,

    # runtime hygiene
    task_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 120) or 120),
    task_soft_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 120) or 120),
    worker_max_tasks_per_child=int(
        getattr(settings, "celery_worker_max_tasks_per_child", 200) or 200
    ),
    worker_send_task_events=True,
    task_send_sent_event=True,

    # routing stability
    task_default_queue=queue,
    task_default_routing_key=queue,
    task_routes={
        "ingestion.*": {"queue": queue, "routing_key": queue},
        "location.*": {"queue": queue, "routing_key": queue},
        "jurisdiction.*": {"queue": queue, "routing_key": queue},
        "risk.*": {"queue": queue, "routing_key": queue},
        "agent.*": {"queue": queue, "routing_key": queue},
    },

    # stable eager mode for tests if enabled
    task_always_eager=bool(getattr(settings, "celery_task_always_eager", False)),
    task_eager_propagates=bool(getattr(settings, "celery_task_eager_propagates", True)),

    # imports
    imports=(
        "app.workers.agent_tasks",
        "app.tasks.ingestion_tasks",
    ),

    # beat safety
    beat_scheduler="celery.beat:PersistentScheduler",
    beat_schedule_filename=str(
        getattr(settings, "celery_beat_schedule_filename", "celerybeat-schedule")
    ),
    beat_schedule=_beat_schedule(),
)


@worker_process_init.connect
def _worker_process_init(**kwargs):
    _emit(
        worker_log,
        {
            "event": "worker_process_init",
            "queue": queue,
            "broker": broker,
            "backend": backend,
        },
    )


@worker_ready.connect
def _worker_ready(sender=None, **kwargs):
    _emit(
        worker_log,
        {
            "event": "worker_ready",
            "queue": queue,
            "sender": str(sender) if sender else None,
        },
    )


@worker_shutdown.connect
def _worker_shutdown(sender=None, **kwargs):
    _emit(
        worker_log,
        {
            "event": "worker_shutdown",
            "queue": queue,
            "sender": str(sender) if sender else None,
        },
    )


@beat_init.connect
def _beat_init(sender=None, **kwargs):
    _emit(
        worker_log,
        {
            "event": "beat_init",
            "beat_schedule_keys": sorted(list(_beat_schedule().keys())),
            "sender": str(sender) if sender else None,
        },
    )


@task_prerun.connect
def _task_prerun(task_id=None, task=None, args=None, kwargs=None, **rest):
    _emit(
        task_log,
        {
            "event": "task_start",
            "task_id": task_id,
            "task_name": getattr(task, "name", None),
            "job_type": getattr(task, "name", None),
            "args_len": len(args or ()),
            "kwargs_keys": sorted(list((kwargs or {}).keys())),
            "outcome": "running",
        },
    )


@task_postrun.connect
def _task_postrun(task_id=None, task=None, args=None, kwargs=None, retval=None, state=None, **rest):
    _emit(
        task_log,
        {
            "event": "task_end",
            "task_id": task_id,
            "task_name": getattr(task, "name", None),
            "job_type": getattr(task, "name", None),
            "state": state,
            "outcome": "success" if str(state).upper() == "SUCCESS" else str(state).lower(),
        },
    )


@task_retry.connect
def _task_retry(request=None, reason=None, einfo=None, **rest):
    _emit(
        task_log,
        {
            "event": "task_retry",
            "task_id": getattr(request, "id", None),
            "task_name": getattr(request, "task", None),
            "job_type": getattr(request, "task", None),
            "outcome": "retry",
            "error_class": type(reason).__name__ if reason else None,
            "error": str(reason) if reason else None,
        },
        level=logging.WARNING,
    )


@task_failure.connect
def _task_failure(task_id=None, exception=None, args=None, kwargs=None, traceback=None, einfo=None, sender=None, **rest):
    _emit(
        task_log,
        {
            "event": "task_failure",
            "task_id": task_id,
            "task_name": getattr(sender, "name", None) if sender else None,
            "job_type": getattr(sender, "name", None) if sender else None,
            "outcome": "failed",
            "error_class": type(exception).__name__ if exception else None,
            "error": str(exception) if exception else None,
        },
        level=logging.ERROR,
    )
    