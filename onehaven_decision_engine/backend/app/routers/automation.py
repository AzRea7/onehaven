# backend/app/routers/automation.py
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.auth import get_principal, require_operator
from app.db import get_db
from app.services.inspection_scheduling_service import (
    list_due_inspection_reminders,
    send_inspection_reminder,
)

router = APIRouter(prefix="/automation", tags=["automation"])


@router.post("/ingest/run", response_model=dict, deprecated=True)
def ingest_run(_op=Depends(require_operator)):
    raise HTTPException(
        status_code=410,
        detail={
            "code": "legacy_ingest_route_removed",
            "message": "Use /ingestion/* property-first sync routes instead of /automation/ingest/run.",
        },
    )


@router.get("/inspection-reminders/preview", response_model=dict)
def preview_due_inspection_reminders(
    before: datetime | None = Query(default=None),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    return {
        "ok": True,
        "rows": list_due_inspection_reminders(
            db,
            org_id=int(p.org_id),
            before=before,
        ),
    }


@router.post("/inspection-reminders/run", response_model=dict)
def run_due_inspection_reminders(
    before: datetime | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _op=Depends(require_operator),
):
    due = list_due_inspection_reminders(db, org_id=int(p.org_id), before=before)[:limit]
    sent = []
    for row in due:
        sent.append(
            send_inspection_reminder(
                db,
                org_id=int(p.org_id),
                actor_user_id=int(p.user_id),
                inspection_id=int(row["inspection_id"]),
            )
        )
    return {"ok": True, "count": len(sent), "rows": sent}
