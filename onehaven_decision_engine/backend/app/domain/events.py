# backend/app/domain/events.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.auth import Principal
from app.models import WorkflowEvent, AuditEvent


def emit_workflow_event(
    db: Session,
    *,
    principal: Principal,
    event_type: str,
    property_id: Optional[int] = None,
    payload: Optional[dict[str, Any]] = None,
) -> WorkflowEvent:
    ev = WorkflowEvent(
        org_id=principal.org_id,
        property_id=property_id,
        actor_user_id=principal.user_id,
        event_type=event_type,
        payload_json=json.dumps(payload or {}, ensure_ascii=False),
        created_at=datetime.utcnow(),
    )
    db.add(ev)
    db.commit()
    db.refresh(ev)
    return ev


def emit_audit_event(
    db: Session,
    *,
    principal: Principal,
    action: str,
    entity_type: str,
    entity_id: str,
    before: Optional[dict[str, Any]] = None,
    after: Optional[dict[str, Any]] = None,
) -> AuditEvent:
    ae = AuditEvent(
        org_id=principal.org_id,
        actor_user_id=principal.user_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        before_json=json.dumps(before, ensure_ascii=False) if before is not None else None,
        after_json=json.dumps(after, ensure_ascii=False) if after is not None else None,
        created_at=datetime.utcnow(),
    )
    db.add(ae)
    db.commit()
    db.refresh(ae)
    return ae
