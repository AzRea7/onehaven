# onehaven_decision_engine/backend/app/domain/audit.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from ..models import AuditEvent


def _dumps(v: Optional[dict[str, Any]]) -> Optional[str]:
    if v is None:
        return None
    return json.dumps(v, sort_keys=True, default=str)


def audit_write(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    action: str,
    entity_type: str,
    entity_id: str,
    before: Optional[dict[str, Any]] = None,
    after: Optional[dict[str, Any]] = None,
    commit: bool = False,
) -> AuditEvent:
    """
    Preferred audit writer.

    - Does NOT commit by default (so routers/services can bundle writes in one txn).
    - Returns the AuditEvent row for tests / introspection.
    """
    row = AuditEvent(
        org_id=org_id,
        actor_user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=str(entity_id),
        before_json=_dumps(before),
        after_json=_dumps(after),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    if commit:
        db.commit()
        db.refresh(row)
    return row


def emit_audit(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    action: str,
    entity_type: str,
    entity_id: str,
    before: Optional[dict[str, Any]] = None,
    after: Optional[dict[str, Any]] = None,
) -> None:
    """
    Back-compat: older call sites expect this to commit immediately.
    New code should call audit_write(..., commit=False) and commit once at end.
    """
    audit_write(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        before=before,
        after=after,
        commit=True,
    )