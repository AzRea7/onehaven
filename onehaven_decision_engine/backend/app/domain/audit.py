# backend/app/domain/audit.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from ..models import AuditEvent


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
    row = AuditEvent(
        org_id=org_id,
        actor_user_id=actor_user_id,
        action=action,
        entity_type=entity_type,
        entity_id=str(entity_id),
        before_json=json.dumps(before, sort_keys=True, default=str) if before is not None else None,
        after_json=json.dumps(after, sort_keys=True, default=str) if after is not None else None,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
