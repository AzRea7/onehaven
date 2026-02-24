# backend/app/workers/agent_worker.py 
from __future__ import annotations

from sqlalchemy import select
from app.db import SessionLocal
from app.models import AgentRun
from app.services.agent_engine import execute_run_now


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


if __name__ == "__main__":
    main()
