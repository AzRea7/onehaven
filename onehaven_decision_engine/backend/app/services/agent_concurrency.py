# backend/app/services/agent_concurrency.py
from __future__ import annotations

import zlib

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..config import settings


def _is_postgres() -> bool:
    url = str(getattr(settings, "database_url", "") or "").lower()
    return url.startswith("postgresql://") or url.startswith("postgresql+psycopg://") or url.startswith("postgres://")


def _lock_key_agent(org_id: int, agent_key: str) -> int:
    blob = f"{int(org_id)}:{str(agent_key)}".encode("utf-8")
    return int(zlib.crc32(blob))


def try_acquire_agent_lock(db: Session, *, org_id: int, agent_key: str) -> bool:
    """
    Postgres: use advisory locks.
    SQLite/local/dev: return True and rely on org concurrency guard + idempotency keys.
    """
    if not bool(getattr(settings, "agents_enable_pg_advisory_locks", True)):
        return True

    if not _is_postgres():
        return True

    key = _lock_key_agent(int(org_id), str(agent_key))
    try:
        got = db.execute(text("select pg_try_advisory_lock(:k) as ok"), {"k": key}).scalar()
        return bool(got)
    except Exception:
        return True


def release_agent_lock(db: Session, *, org_id: int, agent_key: str) -> None:
    if not bool(getattr(settings, "agents_enable_pg_advisory_locks", True)):
        return

    if not _is_postgres():
        return

    key = _lock_key_agent(int(org_id), str(agent_key))
    try:
        db.execute(text("select pg_advisory_unlock(:k)"), {"k": key})
    except Exception:
        pass


def enforce_org_concurrency(db: Session, *, org_id: int) -> None:
    """
    Hard cap on concurrent running runs per org.
    Enforced at worker execution time.
    """
    if not bool(getattr(settings, "agents_enable_org_concurrency_guard", True)):
        return

    max_running = int(getattr(settings, "agents_max_running_per_org", 3) or 3)
    if max_running <= 0:
        return

    try:
        n = db.execute(
            text("select count(*) from agent_runs where org_id=:org_id and status='running'"),
            {"org_id": int(org_id)},
        ).scalar()
        n = int(n or 0)
    except Exception:
        # Fail open in dev/local instead of bricking workers because a migration is out of sync.
        return

    if n >= max_running:
        raise HTTPException(status_code=429, detail=f"org_concurrency_limit:{n}/{max_running}")
    