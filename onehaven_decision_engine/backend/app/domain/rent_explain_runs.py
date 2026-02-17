# backend/app/domain/rent_explain_runs.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from ..models import RentExplainRun


def create_rent_explain_run(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    strategy: str,
    cap_reason: Optional[str],
    explain: dict[str, Any],
    decision_version: str,
    payment_standard_pct_used: Optional[float],
) -> RentExplainRun:
    """
    Always persist an auditable rent explanation artifact.
    This is the "paper trail" that makes underwriting defensible.
    """
    run = RentExplainRun(
        org_id=org_id,
        property_id=property_id,
        strategy=strategy,
        cap_reason=cap_reason,
        explain_json=json.dumps(explain or {}, sort_keys=True),
        decision_version=decision_version or "unknown",
        payment_standard_pct_used=payment_standard_pct_used,
        created_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    return run
