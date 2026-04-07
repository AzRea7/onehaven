from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from ..domain.audit import audit_write
from ..domain.jurisdiction_defaults import defaults_for_michigan
from ..models import JurisdictionRule
from ..policy_models import PolicyAssertion


def _norm_city(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v.title() if v else None


def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_rule_scope_value(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip().lower()
    return s or None


def _loads_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def ensure_seeded_for_org(db: Session, *, org_id: int) -> dict[str, Any]:
    existing = db.scalar(
        select(JurisdictionRule.id).where(JurisdictionRule.org_id == org_id).limit(1)
    )
    if existing is not None:
        return {"seeded": False, "reason": "org already has jurisdiction rules"}

    globals_ = (
        db.execute(select(JurisdictionRule).where(JurisdictionRule.org_id.is_(None)))
        .scalars()
        .all()
    )

    created = 0
    now = datetime.utcnow()

    if globals_:
        for g in globals_:
            row = JurisdictionRule(
                org_id=org_id,
                city=_norm_city(g.city),
                state=_norm_state(g.state),
                rental_license_required=bool(getattr(g, "rental_license_required", False)),
                inspection_authority=getattr(g, "inspection_authority", None),
                inspection_frequency=getattr(g, "inspection_frequency", None),
                typical_fail_points_json=getattr(g, "typical_fail_points_json", None),
                processing_days=getattr(g, "processing_days", None),
                tenant_waitlist_depth=getattr(g, "tenant_waitlist_depth", None),
                notes=getattr(g, "notes", None),
                created_at=now,
                updated_at=now,
            )
            db.add(row)
            created += 1
    else:
        for d in defaults_for_michigan():
            row = JurisdictionRule(
                org_id=org_id,
                city=_norm_city(d.city),
                state=_norm_state(getattr(d, "state", "MI")),
                rental_license_required=bool(getattr(d, "rental_license_required", False)),
                inspection_authority=getattr(d, "inspection_authority", None),
                inspection_frequency=getattr(d, "inspection_frequency", None),
                typical_fail_points_json=json.dumps(list(getattr(d, "typical_fail_points", []) or [])),
                processing_days=getattr(d, "processing_days", None),
                tenant_waitlist_depth=getattr(d, "tenant_waitlist_depth", None),
                notes=getattr(d, "notes", None),
                created_at=now,
                updated_at=now,
            )
            db.add(row)
            created += 1

    db.commit()
    return {"seeded": True, "created": int(created)}


def _jr_to_dict(jr: JurisdictionRule) -> dict[str, Any]:
    return {
        "id": int(jr.id),
        "org_id": int(jr.org_id) if jr.org_id is not None else None,
        "city": jr.city,
        "state": jr.state,
        "rental_license_required": bool(getattr(jr, "rental_license_required", False)),
        "inspection_authority": getattr(jr, "inspection_authority", None),
        "inspection_frequency": getattr(jr, "inspection_frequency", None),
        "typical_fail_points_json": getattr(jr, "typical_fail_points_json", None),
        "processing_days": getattr(jr, "processing_days", None),
        "tenant_waitlist_depth": getattr(jr, "tenant_waitlist_depth", None),
        "notes": getattr(jr, "notes", None),
    }


def create_rule(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    payload: dict[str, Any],
) -> JurisdictionRule:
    now = datetime.utcnow()
    row = JurisdictionRule(
        org_id=org_id,
        city=_norm_city(payload.get("city")),
        state=_norm_state(payload.get("state", "MI")),
        rental_license_required=bool(payload.get("rental_license_required", False)),
        inspection_authority=payload.get("inspection_authority"),
        inspection_frequency=payload.get("inspection_frequency"),
        typical_fail_points_json=payload.get("typical_fail_points_json"),
        processing_days=payload.get("processing_days"),
        tenant_waitlist_depth=payload.get("tenant_waitlist_depth"),
        notes=payload.get("notes"),
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    audit_write(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        action="jurisdiction_rule_created",
        entity_type="jurisdiction_rule",
        entity_id=str(row.id),
        before=None,
        after=_jr_to_dict(row),
        commit=True,
    )
    return row


def update_rule(
    db: Session,
    *,
    org_id: int,
    actor_user_id: Optional[int],
    rule_id: int,
    payload: dict[str, Any],
) -> JurisdictionRule:
    row = db.get(JurisdictionRule, rule_id)
    if row is None or row.org_id != org_id:
        raise ValueError("jurisdiction rule not found")

    before = _jr_to_dict(row)

    if "city" in payload:
        row.city = _norm_city(payload.get("city"))
    if "state" in payload:
        row.state = _norm_state(payload.get("state"))
    if "rental_license_required" in payload:
        row.rental_license_required = bool(payload.get("rental_license_required"))
    if "inspection_authority" in payload:
        row.inspection_authority = payload.get("inspection_authority")
    if "inspection_frequency" in payload:
        row.inspection_frequency = payload.get("inspection_frequency")
    if "typical_fail_points_json" in payload:
        row.typical_fail_points_json = payload.get("typical_fail_points_json")
    if "processing_days" in payload:
        row.processing_days = payload.get("processing_days")
    if "tenant_waitlist_depth" in payload:
        row.tenant_waitlist_depth = payload.get("tenant_waitlist_depth")
    if "notes" in payload:
        row.notes = payload.get("notes")

    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()
    db.refresh(row)

    audit_write(
        db,
        org_id=org_id,
        actor_user_id=actor_user_id,
        action="jurisdiction_rule_updated",
        entity_type="jurisdiction_rule",
        entity_id=str(row.id),
        before=before,
        after=_jr_to_dict(row),
        commit=True,
    )
    return row


def list_rules_for_scope(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: Optional[str] = None,
    city: Optional[str] = None,
) -> list[JurisdictionRule]:
    st = _norm_state(state)
    cnty = _norm_rule_scope_value(county)
    cty = _norm_rule_scope_value(city)

    q = db.query(JurisdictionRule).filter(JurisdictionRule.state == st)
    if org_id is None:
        q = q.filter(JurisdictionRule.org_id.is_(None))
    else:
        q = q.filter(or_(JurisdictionRule.org_id == int(org_id), JurisdictionRule.org_id.is_(None)))

    rows = q.order_by(JurisdictionRule.id.asc()).all()

    out: list[JurisdictionRule] = []
    for row in rows:
        row_city = _norm_rule_scope_value(getattr(row, "city", None))
        if row_city is not None and row_city != cty:
            continue
        out.append(row)
    return out


def resolve_layered_rules(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_rule_scope_value(county)
    cty = _norm_rule_scope_value(city)
    pha = (pha_name or "").strip() or None

    rules = list_rules_for_scope(db, org_id=org_id, state=st, county=cnty, city=cty)

    assertions_q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
    if org_id is None:
        assertions_q = assertions_q.filter(PolicyAssertion.org_id.is_(None))
    else:
        assertions_q = assertions_q.filter(or_(PolicyAssertion.org_id == int(org_id), PolicyAssertion.org_id.is_(None)))

    assertions: list[PolicyAssertion] = []
    for a in assertions_q.all():
        if getattr(a, "county", None) is not None and _norm_rule_scope_value(a.county) != cnty:
            continue
        if getattr(a, "city", None) is not None and _norm_rule_scope_value(a.city) != cty:
            continue
        if getattr(a, "pha_name", None) is not None and (a.pha_name or "").strip() != (pha or ""):
            continue
        assertions.append(a)

    layers: list[dict[str, Any]] = []
    if st == "MI":
        layers.append({"layer": "michigan_statewide_baseline", "scope": {"state": st}, "strength": "baseline"})
    if cnty:
        layers.append({"layer": "county_rules", "scope": {"state": st, "county": cnty}, "strength": "local"})
    if cty:
        layers.append({"layer": "city_rules", "scope": {"state": st, "county": cnty, "city": cty}, "strength": "local"})
    if pha:
        layers.append({"layer": "housing_authority_overlays", "scope": {"state": st, "county": cnty, "city": cty, "pha_name": pha}, "strength": "program"})
    if org_id is not None:
        layers.append({"layer": "org_overrides", "scope": {"org_id": int(org_id), "state": st, "county": cnty, "city": cty, "pha_name": pha}, "strength": "override"})

    source_evidence: list[dict[str, Any]] = []
    for rule in rules:
        source_evidence.append(
            {
                "kind": "jurisdiction_rule",
                "id": int(rule.id),
                "scope": "org" if getattr(rule, "org_id", None) is not None else "global",
                "city": getattr(rule, "city", None),
                "state": getattr(rule, "state", None),
                "updated_at": getattr(rule, "updated_at", None).isoformat() if getattr(rule, "updated_at", None) else None,
                "inspection_authority": getattr(rule, "inspection_authority", None),
                "inspection_frequency": getattr(rule, "inspection_frequency", None),
                "typical_fail_points": _loads_json_list(getattr(rule, "typical_fail_points_json", None)),
            }
        )

    for a in assertions:
        source_evidence.append(
            {
                "kind": "policy_assertion",
                "id": int(a.id),
                "rule_key": getattr(a, "rule_key", None),
                "review_status": getattr(a, "review_status", None),
                "normalized_category": getattr(a, "normalized_category", None),
                "source_id": getattr(a, "source_id", None),
                "confidence": float(getattr(a, "confidence", 0.0) or 0.0),
            }
        )

    return {
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "layers": layers,
        "source_evidence": source_evidence,
        "rule_count": len(rules),
        "assertion_count": len(assertions),
        "resolved_rule_version": f"jr:{st}:{cnty or '-'}:{cty or '-'}:{pha or '-'}:{len(rules)}:{len(assertions)}",
    }