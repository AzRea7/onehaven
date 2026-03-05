from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.auth import require_owner
from app.db import get_db
from app.services.policy_seed import ensure_policy_seeded

router = APIRouter(prefix="/policy", tags=["policy"])


@router.post("/seed", response_model=dict)
def seed_policy(db: Session = Depends(get_db), p=Depends(require_owner)):
    # Seed global rows + HQS library, and optionally an org default row
    ensure_policy_seeded(db, org_id=p.org_id)
    return {"ok": True}