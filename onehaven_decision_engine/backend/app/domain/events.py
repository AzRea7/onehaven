# events.py - centralized event emission for workflow and audit events, with backwards compatibility for both principal-based and explicit org_id/actor_user_id styles.
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
    # Preferred style
    principal: Optional[Principal] = None,
    # Back-compat style
    org_id: Optional[int] = None,
    actor_user_id: Optional[int] = None,
    # Common
    event_type: str,
    property_id: Optional[int] = None,
    payload: Optional[dict[str, Any]] = None,
) -> WorkflowEvent:
    """
    Backwards-compatible workflow event emitter.

    ✅ Preferred:
        emit_workflow_event(db, principal=p, event_type="...", property_id=..., payload={...})

    ✅ Legacy:
        emit_workflow_event(db, org_id=p.org_id, actor_user_id=p.user_id, event_type="...", payload={...})

    NOTE:
    - Does NOT commit. Adds + flushes only.
    - Callers decide when to commit.
    """
    if principal is not None:
        eff_org_id = int(principal.org_id)
        eff_actor_user_id = int(principal.user_id)
    else:
        if org_id is None:
            raise TypeError("emit_workflow_event requires principal=... OR org_id=...")
        eff_org_id = int(org_id)
        eff_actor_user_id = int(actor_user_id) if actor_user_id is not None else None

    ev = WorkflowEvent(
        org_id=eff_org_id,
        property_id=int(property_id) if property_id is not None else None,
        actor_user_id=eff_actor_user_id,
        event_type=str(event_type),
        payload_json=json.dumps(payload or {}, ensure_ascii=False),
        created_at=datetime.utcnow(),
    )
    db.add(ev)
    db.flush()
    return ev


def emit_audit_event(
    db: Session,
    *,
    principal: Optional[Principal] = None,
    org_id: Optional[int] = None,
    actor_user_id: Optional[int] = None,
    action: str,
    entity_type: str,
    entity_id: str,
    before: Optional[dict[str, Any]] = None,
    after: Optional[dict[str, Any]] = None,
) -> AuditEvent:
    """
    Backwards-compatible audit event emitter.

    NOTE: flush-only, no commit.
    """
    if principal is not None:
        eff_org_id = int(principal.org_id)
        eff_actor_user_id = int(principal.user_id)
    else:
        if org_id is None:
            raise TypeError("emit_audit_event requires principal=... OR org_id=...")
        eff_org_id = int(org_id)
        eff_actor_user_id = int(actor_user_id) if actor_user_id is not None else None

    ae = AuditEvent(
        org_id=eff_org_id,
        actor_user_id=eff_actor_user_id,
        action=str(action),
        entity_type=str(entity_type),
        entity_id=str(entity_id),
        before_json=json.dumps(before, ensure_ascii=False) if before is not None else None,
        after_json=json.dumps(after, ensure_ascii=False) if after is not None else None,
        created_at=datetime.utcnow(),
    )
    db.add(ae)
    db.flush()
    return ae
