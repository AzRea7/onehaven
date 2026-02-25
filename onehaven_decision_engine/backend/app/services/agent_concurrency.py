# backend/app/services/agent_concurrency.py
from __future__ import annotations

import os
import zlib
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session


def _lock_key_org(org_id: int) -> int:
    # advisory locks are BIGINT; keep it deterministic
    return int(org_id)


def _lock_key_agent(org_id: int, agent_key: str) -> int:
    s = f"{org_id}:{agent_key}".encode("utf-8")
    return int(zlib.crc32(s))


def try_acquire_agent_lock(db: Session, *, org_id: int, agent_key: str) -> bool:
    k = _lock_key_agent(org_id, agent_key)
    got = db.execute(text("select pg_try_advisory_lock(:k) as ok"), {"k": k}).scalar()
    return bool(got)


def release_agent_lock(db: Session, *, org_id: int, agent_key: str) -> None:
    k = _lock_key_agent(org_id, agent_key)
    db.execute(text("select pg_advisory_unlock(:k)"), {"k": k})


def enforce_org_concurrency(db: Session, *, org_id: int) -> None:
    """
    Hard cap on concurrent 'running' runs per org.
    Enforced at execution time (worker).
    """
    max_running = int(os.getenv("AGENTS_MAX_RUNNING_PER_ORG", "3"))

    # cheap count
    n = db.execute(
        text("select count(*) from agent_runs where org_id=:org_id and status='running'"),
        {"org_id": int(org_id)},
    ).scalar()
    n = int(n or 0)
    if n >= max_running:
        raise HTTPException(status_code=429, detail=f"org_concurrency_limit:{n}/{max_running}")