# onehaven_decision_engine/backend/app/workers/agent_worker.py
from __future__ import annotations

import time
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import SessionLocal
from ..models import AgentRun
from ..services.agent_engine import execute_run_now


POLL_SECONDS = 2.0


def fetch_next_queued(db: Session) -> AgentRun | None:
    return db.scalar(
        select(AgentRun)
        .where(AgentRun.status == "queued")
        .order_by(AgentRun.id.asc())
        .limit(1)
    )


def main():
    while True:
        db = SessionLocal()
        try:
            run = fetch_next_queued(db)
            if run is None:
                time.sleep(POLL_SECONDS)
                continue

            execute_run_now(db, org_id=int(run.org_id), run_id=int(run.id))
        finally:
            db.close()


if __name__ == "__main__":
    main()