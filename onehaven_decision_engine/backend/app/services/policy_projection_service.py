# backend/app/services/policy_projection_service.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion
from app.policy_models import JurisdictionProfile


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _loads(s: Optional[str], default: Any = None) -> Any:
    if default is None:
        default = {}
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_county(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _norm_city(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _score_friction_from_verified(assertions: list[PolicyAssertion]) -> float:
    """
    Very conservative friction heuristic for now.
    Start at 1.0 and add a little weight for verified operational burdens.
    This is a temporary model, not the final truth.
    """
    friction = 1.0
    seen = {a.rule_key for a in assertions}

    if "rental_registration_required" in seen:
        friction += 0.10
    if "certificate_required_before_occupancy" in seen:
        friction += 0.10
    if "inspection_program_exists" in seen:
        friction += 0.10
    if "lead_compliance_pre_1978" in seen:
        friction += 0.05
    if "pha_landlord_packet_required" in seen:
        friction += 0.05

    return round(min(friction, 2.0), 2)


def build_policy_summary(assertions: list[PolicyAssertion]) -> dict[str, Any]:
    """
    Collapse verified assertions into a structured policy_json payload.
    """
    out: dict[str, Any] = {
        "summary": "Built from verified policy assertions.",
        "verified_rules": [],
        "sources": [],
    }

    source_ids: set[int] = set()

    for a in assertions:
        value = _loads(a.value_json, {})
        out["verified_rules"].append(
            {
                "rule_key": a.rule_key,
                "value": value,
                "confidence": a.confidence,
            }
        )
        if a.source_id is not None:
            source_ids.add(a.source_id)

    out["source_ids"] = sorted(source_ids)
    return out


def project_verified_assertions_to_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    notes: Optional[str] = None,
) -> JurisdictionProfile:
    """
    Build or update a JurisdictionProfile from verified assertions.
    """
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)

    q = (
        select(PolicyAssertion)
        .where(PolicyAssertion.review_status == "verified")
        .where(PolicyAssertion.state == st)
        .where(
            PolicyAssertion.org_id.is_(None)
            if org_id is None
            else (PolicyAssertion.org_id == org_id)
        )
        .where(
            PolicyAssertion.county.is_(None)
            if cnty is None
            else (PolicyAssertion.county == cnty)
        )
        .where(
            PolicyAssertion.city.is_(None)
            if cty is None
            else (PolicyAssertion.city == cty)
        )
    )

    assertions = list(db.scalars(q).all())
    policy_json = build_policy_summary(assertions)
    friction = _score_friction_from_verified(assertions)

    existing = db.scalar(
        select(JurisdictionProfile)
        .where(JurisdictionProfile.org_id.is_(None) if org_id is None else JurisdictionProfile.org_id == org_id)
        .where(JurisdictionProfile.state == st)
        .where(JurisdictionProfile.county.is_(None) if cnty is None else JurisdictionProfile.county == cnty)
        .where(JurisdictionProfile.city.is_(None) if cty is None else JurisdictionProfile.city == cty)
    )

    now = datetime.utcnow()

    if existing is None:
        row = JurisdictionProfile(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            friction_multiplier=friction,
            pha_name=pha_name,
            policy_json=_dumps(policy_json),
            notes=notes or "Projected from verified policy assertions.",
            updated_at=now,
        )
        db.add(row)
    else:
        row = existing
        row.friction_multiplier = friction
        if pha_name:
            row.pha_name = pha_name
        row.policy_json = _dumps(policy_json)
        row.notes = notes or row.notes or "Projected from verified policy assertions."
        row.updated_at = now

    db.commit()
    db.refresh(row)
    return row