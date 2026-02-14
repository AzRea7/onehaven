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
    entity_id: Optional[int],
    meta: dict[str, Any] | None = None,
) -> None:
    """
    Lightweight append-only audit log.

    Rules:
      - never raise (audit should not break main path)
      - keep meta JSON small + structured
    """
    try:
        row = AuditEvent(
            org_id=org_id,
            actor_user_id=actor_user_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            meta_json=json.dumps(meta or {}),
            created_at=datetime.utcnow(),
        )
        db.add(row)
        db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
