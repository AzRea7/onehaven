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


def _jurisdiction_runtime_flags() -> dict[str, Any]:
    return {
        "jurisdiction_automation_enabled": bool(
            getattr(settings, "jurisdiction_automation_enabled", True)
        ),
        "jurisdiction_notification_enabled": bool(
            getattr(settings, "jurisdiction_notification_enabled", True)
        ),
        "jurisdiction_critical_stale_lockout_enabled": bool(
            getattr(settings, "jurisdiction_critical_stale_lockout_enabled", True)
        ),
    }


def _base_schedule_entry(task_name: str, every_seconds: float) -> dict[str, Any]:
    return {
        "task": task_name,
        "schedule": float(every_seconds),
        "options": {"queue": queue, "routing_key": queue},
    }


def _beat_schedule() -> dict[str, dict[str, Any]]:
    schedule: dict[str, dict[str, Any]] = {
        "ingestion-sync-due-sources-every-15-minutes": _base_schedule_entry(
            "ingestion.sync_due_sources",
            15 * 60.0,
        ),
        "rent-refresh-budgeted-batch": _base_schedule_entry(
            "rent.refresh_budgeted_batch",
            24 * 60 * 60.0,
        ),
        "market-sync-daily-supported-markets": _base_schedule_entry(
            "market_sync.daily_supported_markets",
            24 * 60 * 60.0,
        ),
        "ingestion-daily-market-refresh": _base_schedule_entry(
            "ingestion.daily_market_refresh",
            24 * 60 * 60.0,
        ),
        "location-refresh-stale-properties": _base_schedule_entry(
            "location.refresh_stale_properties",
            float(max(300, int(settings.location_refresh_schedule_minutes) * 60)),
        ),
    }

    flags = _jurisdiction_runtime_flags()
    if flags["jurisdiction_automation_enabled"]:
        schedule.update(
            {
                "jurisdiction-refresh-stale-profiles": _base_schedule_entry(
                    "jurisdiction.refresh_stale_profiles",
                    float(max(300, int(settings.jurisdiction_refresh_schedule_minutes) * 60)),
                ),
                "jurisdiction-retry-discovery": _base_schedule_entry(
                    "jurisdiction.retry_discovery",
                    float(
                        max(300, int(settings.jurisdiction_discovery_retry_schedule_minutes) * 60)
                    ),
                ),
                "jurisdiction-retry-validation": _base_schedule_entry(
                    "jurisdiction.retry_validation",
                    float(
                        max(300, int(settings.jurisdiction_validation_retry_schedule_minutes) * 60)
                    ),
                ),
                "jurisdiction-recompute-due-profiles": _base_schedule_entry(
                    "jurisdiction.recompute_due_profiles",
                    float(max(300, int(settings.jurisdiction_recompute_schedule_minutes) * 60)),
                ),
                "jurisdiction-health-snapshot": _base_schedule_entry(
                    "jurisdiction.health_snapshot",
                    float(max(300, int(settings.jurisdiction_health_schedule_minutes) * 60)),
                ),
            }
        )

    if flags["jurisdiction_automation_enabled"] and flags["jurisdiction_notification_enabled"]:
        schedule["jurisdiction-notify-stale-profiles"] = _base_schedule_entry(
            "jurisdiction.notify_stale_profiles",
            float(max(300, int(settings.jurisdiction_health_schedule_minutes) * 60)),
        )

    return schedule


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
    task_acks_late=True,
    task_acks_on_failure_or_timeout=False,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    task_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 180) or 180),
    task_soft_time_limit=int(getattr(settings, "agents_run_timeout_seconds", 180) or 180),
    worker_max_tasks_per_child=int(
        getattr(settings, "celery_worker_max_tasks_per_child", 200) or 200
    ),
    worker_send_task_events=True,
    task_send_sent_event=True,
    task_default_queue=queue,
    task_default_routing_key=queue,
    task_routes={
        "ingestion.*": {"queue": queue, "routing_key": queue},
        "location.*": {"queue": queue, "routing_key": queue},
        "jurisdiction.*": {"queue": queue, "routing_key": queue},
        "risk.*": {"queue": queue, "routing_key": queue},
        "agent.*": {"queue": queue, "routing_key": queue},
        "market_sync.*": {"queue": queue, "routing_key": queue},
        "rent.*": {"queue": queue, "routing_key": queue},
    },
    task_always_eager=bool(getattr(settings, "celery_task_always_eager", False)),
    task_eager_propagates=bool(getattr(settings, "celery_task_eager_propagates", True)),
    imports=(
        "app.workers.agent_tasks",
        "app.tasks.ingestion_tasks",
        "app.tasks.market_sync_tasks",
        "app.tasks.jurisdiction_tasks",
    ),
    beat_scheduler="celery.beat:PersistentScheduler",
    beat_schedule_filename=str(
        getattr(settings, "celery_beat_schedule_filename", "/tmp/celerybeat-schedule")
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
            **_jurisdiction_runtime_flags(),
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
            **_jurisdiction_runtime_flags(),
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
            "sender": str(sender) if sender else None,
            "beat_schedule_filename": str(
                getattr(settings, "celery_beat_schedule_filename", "/tmp/celerybeat-schedule")
            ),
            "beat_schedule_keys": sorted(list((_beat_schedule() or {}).keys())),
            **_jurisdiction_runtime_flags(),
        },
    )


@task_prerun.connect
def _task_prerun(task_id=None, task=None, args=None, kwargs=None, **extra):
    _emit(
        task_log,
        {
            "event": "task_prerun",
            "task_id": task_id,
            "task_name": getattr(task, "name", None),
        },
    )


@task_postrun.connect
def _task_postrun(task_id=None, task=None, retval=None, state=None, **extra):
    _emit(
        task_log,
        {
            "event": "task_postrun",
            "task_id": task_id,
            "task_name": getattr(task, "name", None),
            "state": state,
        },
    )


@task_retry.connect
def _task_retry(request=None, reason=None, einfo=None, **extra):
    _emit(
        task_log,
        {
            "event": "task_retry",
            "task_id": getattr(request, "id", None),
            "task_name": getattr(request, "task", None),
            "reason": str(reason) if reason else None,
        },
        level=logging.WARNING,
    )


@task_failure.connect
def _task_failure(
    task_id=None,
    exception=None,
    args=None,
    kwargs=None,
    traceback=None,
    einfo=None,
    sender=None,
    **extra,
):
    _emit(
        task_log,
        {
            "event": "task_failure",
            "task_id": task_id,
            "task_name": getattr(sender, "name", None),
            "exception": str(exception) if exception else None,
        },
        level=logging.ERROR,
    )
