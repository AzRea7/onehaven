# onehaven_decision_engine/backend/app/services/jurisdiction_profile_service.py
from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Property
from app.policy_models import JurisdictionProfile


def _loads(s: str | None, default):
    if not s:
        return default
    try:
        v = json.loads(s)
        return v
    except Exception:
        return default


def resolve_jurisdiction_profile(db: Session, *, org_id: int, prop: Property) -> dict[str, Any]:
    """
    Deterministic matching (v1):
      - tries exact city match (case-insensitive) and state match
      - fallback to county if present (if your Property has county field)
      - fallback to default org profile (name contains 'DEFAULT')
    Returns:
      {
        "profile": {...} | None,
        "effective_steps": [ ... ],   # workflow steps/checklists
      }
    """
    city = (getattr(prop, "city", None) or "").strip().lower()
    state = (getattr(prop, "state", None) or "").strip().upper()
    county = (getattr(prop, "county", None) or "").strip().lower()

    # City+State match
    rows = db.scalars(select(JurisdictionProfile).where(JurisdictionProfile.org_id == org_id)).all()

    chosen = None
    for r in rows:
        rc = (getattr(r, "city", None) or "").strip().lower()
        rs = (getattr(r, "state", None) or "").strip().upper()
        if rc and rs and rc == city and rs == state:
            chosen = r
            break

    # County fallback
    if chosen is None and county:
        for r in rows:
            rco = (getattr(r, "county", None) or "").strip().lower()
            rs = (getattr(r, "state", None) or "").strip().upper()
            if rco and rs and rco == county and rs == state:
                chosen = r
                break

    # Default fallback
    if chosen is None:
        for r in rows:
            name = (getattr(r, "name", None) or "").upper()
            if "DEFAULT" in name:
                chosen = r
                break

    if chosen is None:
        return {"profile": None, "effective_steps": []}

    steps = _loads(getattr(chosen, "workflow_steps_json", None), [])
    if not isinstance(steps, list):
        steps = []

    profile = {
        "id": int(chosen.id),
        "name": getattr(chosen, "name", None),
        "city": getattr(chosen, "city", None),
        "county": getattr(chosen, "county", None),
        "state": getattr(chosen, "state", None),
        "effective_date": getattr(chosen, "effective_date", None).isoformat() if getattr(chosen, "effective_date", None) else None,
        "last_verified_at": getattr(chosen, "last_verified_at", None).isoformat() if getattr(chosen, "last_verified_at", None) else None,
        "source_urls": _loads(getattr(chosen, "source_urls_json", None), []),
    }

    return {"profile": profile, "effective_steps": steps}