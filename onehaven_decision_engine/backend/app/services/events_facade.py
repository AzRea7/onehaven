# onehaven_decision_engine/backend/app/services/events_facade.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from ..models import WorkflowEvent


def emit(
    db: Session,
    *,
    org_id: int,
    property_id: Optional[int],
    actor_user_id: Optional[int],
    event_type: str,
    payload: Optional[dict[str, Any]] = None,
) -> WorkflowEvent:
    row = WorkflowEvent(
        org_id=org_id,
        property_id=property_id,
        actor_user_id=actor_user_id,
        event_type=str(event_type),
        payload_json=json.dumps(payload or {}),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    return row