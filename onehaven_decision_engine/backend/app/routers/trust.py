# backend/app/routers/trust.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner
from ..db import get_db
from ..services.trust_service import get_trust_score, recompute_and_persist, record_signal

router = APIRouter(prefix="/trust", tags=["trust"])


class TrustOut(BaseModel):
    org_id: int
    entity_type: str
    entity_id: str
    score: float
    confidence: float
    components: dict[str, Any] = Field(default_factory=dict)
    updated_at: Optional[str] = None


class TrustSignalIn(BaseModel):
    signal_key: str
    value: float
    weight: float = 1.0
    meta: Optional[dict[str, Any]] = None


@router.get("/{entity_type}/{entity_id}", response_model=TrustOut)
def get_trust(
    entity_type: str,
    entity_id: str,
    recompute: int = Query(default=0, description="1 = force recompute"),
    db: Session = Depends(get_db),
    p=Depends(get_principal),
):
    row = get_trust_score(
        db,
        org_id=p.org_id,
        entity_type=entity_type,
        entity_id=entity_id,
        recompute=bool(recompute),
    )

    try:
        components = json.loads(row.components_json) if row.components_json else {}
        if not isinstance(components, dict):
            components = {}
    except Exception:
        components = {}

    return TrustOut(
        org_id=int(row.org_id),
        entity_type=str(row.entity_type),
        entity_id=str(row.entity_id),
        score=float(row.score or 0.0),
        confidence=float(row.confidence or 0.0),
        components=components,
        updated_at=row.updated_at.isoformat() if getattr(row, "updated_at", None) else None,
    )


@router.post("/{entity_type}/{entity_id}/signal", response_model=TrustOut)
def inject_signal_and_recompute(
    entity_type: str,
    entity_id: str,
    payload: TrustSignalIn,
    db: Session = Depends(get_db),
    p=Depends(get_principal),
    _owner=Depends(require_owner),
):
    record_signal(
        db,
        org_id=p.org_id,
        entity_type=entity_type,
        entity_id=entity_id,
        signal_key=payload.signal_key,
        value=float(payload.value),
        weight=float(payload.weight),
        meta=payload.meta,
        created_at=datetime.utcnow(),
    )
    recompute_and_persist(db, org_id=p.org_id, entity_type=entity_type, entity_id=entity_id)
    db.commit()

    return get_trust(entity_type, entity_id, recompute=0, db=db, p=p)
