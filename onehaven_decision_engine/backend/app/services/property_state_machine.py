# backend/app/services/property_state_machine.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import PropertyState


STAGE_ORDER = ["deal", "rehab", "compliance", "tenant", "cash", "equity"]


def _stage_rank(stage: str) -> int:
    try:
        return STAGE_ORDER.index(stage)
    except ValueError:
        return 0


def ensure_state_row(db: Session, *, org_id: int, property_id: int) -> PropertyState:
    row = db.scalar(
        select(PropertyState).where(
            PropertyState.org_id == org_id,
            PropertyState.property_id == property_id,
        )
    )
    if row:
        return row

    now = datetime.utcnow()
    row = PropertyState(
        org_id=org_id,
        property_id=property_id,
        current_stage="deal",
        constraints_json=json.dumps({}),
        outstanding_tasks_json=json.dumps({}),
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def advance_stage_if_needed(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    suggested_stage: str,
    constraints: Optional[dict[str, Any]] = None,
    outstanding_tasks: Optional[dict[str, Any]] = None,
) -> PropertyState:
    row = ensure_state_row(db, org_id=org_id, property_id=property_id)

    cur = str(row.current_stage or "deal")
    if _stage_rank(suggested_stage) > _stage_rank(cur):
        row.current_stage = suggested_stage

    if constraints is not None:
        row.constraints_json = json.dumps(constraints)

    if outstanding_tasks is not None:
        row.outstanding_tasks_json = json.dumps(outstanding_tasks)

    row.updated_at = datetime.utcnow()
    db.add(row)
    db.flush()
    return row
