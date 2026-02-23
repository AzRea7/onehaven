# onehaven_decision_engine/backend/app/services/hud_fmr_service.py
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.policy_models import HudFmrRecord


def get_or_fetch_fmr(
    db: Session,
    *,
    org_id: int,
    area_name: str,
    state: str,
    bedrooms: int,
) -> HudFmrRecord:
    """
    v1 behavior:
      - if record exists in DB: return it
      - else create a placeholder record with fmr=0.0 (still “truthful”)
      - later: wire HUD API fetch here (requests.get) and fill fmr/effective_date/source_urls_json
    """
    area = (area_name or "").strip()
    st = (state or "").strip().upper()
    br = int(bedrooms or 0)

    existing = db.scalar(
        select(HudFmrRecord)
        .where(HudFmrRecord.org_id == org_id)
        .where(HudFmrRecord.area_name == area)
        .where(HudFmrRecord.state == st)
        .where(HudFmrRecord.bedrooms == br)
        .order_by(HudFmrRecord.id.desc())
    )
    if existing:
        return existing

    # Create a truthful placeholder (no silent lying).
    r = HudFmrRecord(
        org_id=org_id,
        area_name=area,
        state=st,
        bedrooms=br,
        fmr=0.0,
        effective_date=date.today().replace(month=1, day=1),
        source_urls_json=json.dumps([]),
        created_at=datetime.utcnow(),
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return r