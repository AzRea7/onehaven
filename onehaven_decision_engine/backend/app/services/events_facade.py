# backend/app/services/events_facade.py
from __future__ import annotations

from typing import Optional, Any
from sqlalchemy.orm import Session

from ..domain.events import emit_workflow_event


def wf(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    event_type: str,
    property_id: Optional[int] = None,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    emit_workflow_event(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        event_type=event_type,
        property_id=property_id,
        payload=payload or {},
    )
