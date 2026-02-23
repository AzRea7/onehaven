from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from app.models import WorkflowEvent


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v)
    except Exception:
        return "{}"


def _loads(s: Optional[str], default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


@dataclass(frozen=True)
class WorkflowEventOut:
    id: int
    org_id: int
    property_id: Optional[int]
    actor_user_id: Optional[int]
    event_type: str
    payload: dict[str, Any]
    created_at: Optional[datetime]


class WorkflowFacade:
    """
    Small facade used by routers that want to emit/query workflow events
    without duplicating JSON plumbing.

    Routers import:
        from ..services.events_facade import wf
    """

    def emit(
        self,
        db: Session,
        *,
        org_id: int,
        property_id: Optional[int],
        actor_user_id: Optional[int],
        event_type: str,
        payload: dict[str, Any] | None = None,
        created_at: Optional[datetime] = None,
    ) -> WorkflowEvent:
        if not event_type:
            raise ValueError("event_type required")

        row = WorkflowEvent(
            org_id=org_id,
            property_id=property_id,
            actor_user_id=actor_user_id,
            event_type=str(event_type),
            payload_json=_dumps(payload or {}),
            created_at=created_at or datetime.utcnow(),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    def list(
        self,
        db: Session,
        *,
        org_id: int,
        property_id: Optional[int] = None,
        limit: int = 200,
    ) -> list[WorkflowEventOut]:
        q = select(WorkflowEvent).where(WorkflowEvent.org_id == org_id).order_by(
            WorkflowEvent.id.desc()
        )
        if property_id is not None:
            q = q.where(WorkflowEvent.property_id == int(property_id))

        rows = db.scalars(q.limit(int(limit))).all()
        out: list[WorkflowEventOut] = []
        for r in rows:
            out.append(
                WorkflowEventOut(
                    id=int(r.id),
                    org_id=int(r.org_id),
                    property_id=getattr(r, "property_id", None),
                    actor_user_id=getattr(r, "actor_user_id", None),
                    event_type=str(getattr(r, "event_type", "")),
                    payload=_loads(getattr(r, "payload_json", None), {}),
                    created_at=getattr(r, "created_at", None),
                )
            )
        return out

    def emit_inspection_event(
        self,
        db: Session,
        *,
        org_id: int,
        property_id: int,
        actor_user_id: Optional[int],
        inspection_id: int,
        event_type: str,
        payload: dict[str, Any] | None = None,
    ) -> WorkflowEvent:
        """
        Convenience helper for inspection flows.
        Keeps your event taxonomy consistent:
            inspection_created / inspection_updated / inspection_failed / inspection_passed ...
        """
        base = {"inspection_id": int(inspection_id)}
        if payload:
            base.update(payload)
        return self.emit(
            db,
            org_id=org_id,
            property_id=int(property_id),
            actor_user_id=actor_user_id,
            event_type=str(event_type),
            payload=base,
        )


# The symbol your router expects:
wf = WorkflowFacade()