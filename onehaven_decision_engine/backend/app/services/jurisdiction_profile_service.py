# backend/app/services/jurisdiction_profile_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.policy_models import JurisdictionProfile


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v if v else None


def _norm_city(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_county(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_state(s: Optional[str]) -> str:
    v = (s or "MI").strip().upper()
    return v or "MI"


def _loads(s: Optional[str], default: Any = None) -> Any:
    if default is None:
        default = {}
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def list_profiles(
    db: Session,
    *,
    org_id: int,
    include_global: bool = True,
    state: str = "MI",
) -> list[JurisdictionProfile]:
    st = _norm_state(state)

    q_org = select(JurisdictionProfile).where(
        and_(JurisdictionProfile.state == st, JurisdictionProfile.org_id == org_id)
    )

    if not include_global:
        return list(db.scalars(q_org).all())

    q_global = select(JurisdictionProfile).where(
        and_(JurisdictionProfile.state == st, JurisdictionProfile.org_id.is_(None))
    )

    rows = list(db.scalars(q_global).all()) + list(db.scalars(q_org).all())
    return rows


def _specificity(r: JurisdictionProfile) -> int:
    """
    Specificity rank:
      2 = city row (city present)
      1 = county row (county present, city absent)
      0 = state row (county/city absent)
    """
    if (r.city or "").strip():
        return 2
    if (r.county or "").strip():
        return 1
    return 0


def resolve_profile(
    db: Session,
    *,
    org_id: int,
    city: Optional[str],
    county: Optional[str],
    state: str = "MI",
) -> dict[str, Any]:
    """
    Deterministic resolution (CORRECT):
      1) Specificity FIRST: city > county > state
      2) Within the same specificity: org overrides beat global defaults
      3) Deterministic tie-breaker: lowest id wins

    IMPORTANT:
      This prevents an org-level state override from masking a more specific
      global county/city row.

    Returns:
      {
        "matched": bool,
        "scope": "org"|"global"|None,
        "match_level": "city"|"county"|"state"|None,
        "friction_multiplier": float,
        "pha_name": str|None,
        "policy": dict,
        "notes": str|None,
        "profile_id": int|None,
      }
    """
    st = _norm_state(state)
    req_city = _norm_city(city)
    req_county = _norm_county(county)

    rows = list_profiles(db, org_id=org_id, include_global=True, state=st)

    def match_level(r: JurisdictionProfile) -> Optional[str]:
        """
        Decide if row can satisfy the request; return the match level it provides.

        Rules:
          - If request includes city:
              accept city row if city matches (case-normalized),
              else accept county row if county matches (as fallback),
              else accept state row (as fallback).
          - If request includes county but not city:
              accept county row if county matches,
              else accept state row.
          - If request includes only state:
              accept state row.

        Note:
          A city row must match the requested city to be a city match.
          A county row must have no city (city NULL/empty) to be a county match.
          A state row must have neither county nor city.
        """
        r_city = _norm_city(r.city)
        r_county = _norm_county(r.county)

        # City request
        if req_city:
            if r_city and r_city == req_city:
                return "city"
            if (not r_city) and r_county and req_county and r_county == req_county:
                return "county"
            if (not r_city) and (not r_county):
                return "state"
            return None

        # County request (no city)
        if req_county:
            if (not r_city) and r_county and r_county == req_county:
                return "county"
            if (not r_city) and (not r_county):
                return "state"
            return None

        # State only request
        if (not r_city) and (not r_county):
            return "state"
        return None

    candidates: list[tuple[int, int, int, JurisdictionProfile, str]] = []
    for r in rows:
        lvl = match_level(r)
        if not lvl:
            continue

        spec = _specificity(r)  # 0/1/2
        scope_pri = 1 if (r.org_id == org_id) else 0  # org beats global on ties
        rid = int(r.id)

        # Sort key components we will max()/sort on:
        # (spec, scope_pri, -id?) We want deterministic by *lowest* id, so use -rid in max,
        # or use sort with rid ascending. We'll just sort below.
        candidates.append((spec, scope_pri, rid, r, lvl))

    if not candidates:
        return {
            "matched": False,
            "scope": None,
            "match_level": None,
            "friction_multiplier": 1.0,
            "pha_name": None,
            "policy": {},
            "notes": None,
            "profile_id": None,
        }

    # Highest specificity first, then org scope, then deterministic smallest id.
    candidates.sort(key=lambda t: (-t[0], -t[1], t[2]))
    best_spec, best_scope_pri, _rid, chosen, lvl = candidates[0]

    scope = "org" if best_scope_pri == 1 else "global"

    return {
        "matched": True,
        "scope": scope,
        "match_level": lvl,
        "friction_multiplier": float(chosen.friction_multiplier or 1.0),
        "pha_name": chosen.pha_name,
        "policy": _loads(getattr(chosen, "policy_json", None), {}),
        "notes": chosen.notes,
        "profile_id": int(chosen.id),
    }


def upsert_profile(
    db: Session,
    *,
    org_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
    friction_multiplier: float,
    pha_name: Optional[str],
    policy: Any,
    notes: Optional[str],
) -> JurisdictionProfile:
    """
    Upsert ORG override row for a given (state, county?, city?).
    Global rows are not written here (seed them instead).
    """
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)

    q = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.org_id == org_id)
        .where(JurisdictionProfile.state == st)
        .where(
            and_(
                (JurisdictionProfile.county.is_(None) if cnty is None else JurisdictionProfile.county == cnty),
                (JurisdictionProfile.city.is_(None) if cty is None else JurisdictionProfile.city == cty),
            )
        )
    )
    row = db.scalar(q)

    now = datetime.utcnow()

    if row is None:
        row = JurisdictionProfile(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            friction_multiplier=float(friction_multiplier or 1.0),
            pha_name=_norm(pha_name),
            policy_json=_dumps(policy or {}),
            notes=_norm(notes),
            updated_at=now,
        )
        db.add(row)
    else:
        row.friction_multiplier = float(friction_multiplier or 1.0)
        row.pha_name = _norm(pha_name)
        row.policy_json = _dumps(policy or {})
        row.notes = _norm(notes)
        row.updated_at = now

    db.commit()
    db.refresh(row)
    return row

def resolve_operational_policy(
    db: Session,
    *,
    org_id: int,
    city: Optional[str],
    county: Optional[str],
    state: str = "MI",
) -> dict[str, Any]:
    from app.services.policy_projection_service import build_property_compliance_brief

    base = resolve_profile(
        db,
        org_id=org_id,
        city=city,
        county=county,
        state=state,
    )

    brief = build_property_compliance_brief(
        db,
        org_id=None,
        state=state,
        county=county,
        city=city,
        pha_name=base.get("pha_name"),
    )

    return {
        **base,
        "coverage": brief.get("coverage", {}),
        "brief": brief.get("compliance", {}),
        "blocking_items": brief.get("blocking_items", []),
        "required_actions": brief.get("required_actions", []),
        "evidence_links": brief.get("evidence_links", []),
    }

def delete_profile(
    db: Session,
    *,
    org_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
) -> int:
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)

    q = (
        select(JurisdictionProfile)
        .where(JurisdictionProfile.org_id == org_id)
        .where(JurisdictionProfile.state == st)
        .where(
            and_(
                (JurisdictionProfile.county.is_(None) if cnty is None else JurisdictionProfile.county == cnty),
                (JurisdictionProfile.city.is_(None) if cty is None else JurisdictionProfile.city == cty),
            )
        )
    )
    row = db.scalar(q)
    if row is None:
        return 0

    db.delete(row)
    db.commit()
    return 1