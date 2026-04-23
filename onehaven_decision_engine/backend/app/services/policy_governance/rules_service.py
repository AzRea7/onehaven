from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.audit import audit_write
from app.domain.policy.defaults import defaults_for_michigan
from app.models import JurisdictionRule
from app.policy_models import PolicyAssertion

SAFE_GOVERNANCE_STATES = {"active"}
NON_PROJECTABLE_GOVERNANCE_STATES = {"draft", "replaced"}
NON_PROJECTABLE_RULE_STATUSES = {"candidate", "draft", "replaced", "superseded", "conflicting", "stale"}
NON_PROJECTABLE_COVERAGE_STATUSES = {"candidate", "partial", "inferred", "conflicting", "stale", "superseded"}
MANUAL_REVIEW_REVIEW_STATUS = "needs_manual_review"
HIGH_RISK_VALIDATION_STATES = {"conflicting"}
NON_PROJECTABLE_VALIDATION_STATES = {"weak_support", "ambiguous", "unsupported", "conflicting"}


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


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _conflict_hints_for_assertion(assertion: PolicyAssertion) -> list[str]:
    citation = _loads_json_dict(getattr(assertion, "citation_json", None))
    provenance = _loads_json_dict(getattr(assertion, "rule_provenance_json", None))
    out: list[str] = []

    for value in [citation.get("conflict_hints"), provenance.get("conflict_hints")]:
        if isinstance(value, list):
            out.extend(str(item).strip() for item in value if str(item).strip())

    coverage_status = (getattr(assertion, "coverage_status", None) or "").strip().lower()
    rule_status = (getattr(assertion, "rule_status", None) or "").strip().lower()
    confidence_basis = (getattr(assertion, "confidence_basis", None) or "").strip().lower()

    if coverage_status == "conflicting":
        out.append("coverage_status_conflicting")
    if rule_status == "conflicting":
        out.append("rule_status_conflicting")
    if "conflicting" in confidence_basis:
        out.append("confidence_basis_conflicting")

    return sorted(set(out))


def _requires_manual_review(assertion: PolicyAssertion) -> bool:
    validation_state = (getattr(assertion, "validation_state", None) or "pending").strip().lower()
    review_status = (getattr(assertion, "review_status", None) or "").strip().lower()
    trust_state = (getattr(assertion, "trust_state", None) or "").strip().lower()
    conflict_hints = _conflict_hints_for_assertion(assertion)
    return bool(
        validation_state in HIGH_RISK_VALIDATION_STATES
        or review_status == MANUAL_REVIEW_REVIEW_STATUS
        or (trust_state == "needs_review" and bool(conflict_hints))
    )


def _artifact_backed_assertion(assertion: PolicyAssertion) -> bool:
    """
    Hardened artifact/PDF detector.

    The prior bottom-of-file patch assumed these JSON-ish fields were already dicts.
    In practice they may be dicts, JSON strings, empty strings, or None.
    """
    payloads = [
        _loads_json_dict(getattr(assertion, "citation_json", None)),
        _loads_json_dict(getattr(assertion, "rule_provenance_json", None)),
        _loads_json_dict(getattr(assertion, "value_json", None)),
        _loads_json_dict(getattr(assertion, "change_summary", None)),
    ]

    for payload in payloads:
        if not payload:
            continue

        fam = str(payload.get("evidence_family", "") or "").strip().lower()
        raw = str(payload.get("raw_path", "") or "").strip().lower()
        url = str(payload.get("url", "") or "").strip().lower()
        publication_type = str(payload.get("publication_type", "") or "").strip().lower()
        source_kind = str(payload.get("source_kind", "") or "").strip().lower()
        matched_pdf_name = str(payload.get("matched_pdf_name", "") or "").strip().lower()

        if fam in {"artifact", "pdf", "document", "official_document"}:
            return True
        if publication_type in {"pdf", "official_document"}:
            return True
        if source_kind == "pdf":
            return True
        if raw.endswith(".pdf") or url.endswith(".pdf"):
            return True
        if matched_pdf_name.endswith(".pdf"):
            return True

        citation_payload = _loads_json_dict(payload.get("citation"))
        if citation_payload:
            c_url = str(citation_payload.get("url", "") or "").strip().lower()
            c_publication_type = str(citation_payload.get("publication_type", "") or "").strip().lower()
            c_source_kind = str(citation_payload.get("source_kind", "") or "").strip().lower()
            c_matched_pdf_name = str(citation_payload.get("matched_pdf_name", "") or "").strip().lower()

            if c_publication_type in {"pdf", "official_document"}:
                return True
            if c_source_kind == "pdf":
                return True
            if c_url.endswith(".pdf") or c_matched_pdf_name.endswith(".pdf"):
                return True

    return False


def assertion_governance_summary(assertion: PolicyAssertion) -> dict[str, Any]:
    governance_state = (getattr(assertion, "governance_state", None) or "").strip().lower()
    rule_status = (getattr(assertion, "rule_status", None) or "").strip().lower()
    review_status = (getattr(assertion, "review_status", None) or "").strip().lower()
    coverage_status = (getattr(assertion, "coverage_status", None) or "").strip().lower()
    validation_state = (getattr(assertion, "validation_state", None) or "pending").strip().lower()
    trust_state = (getattr(assertion, "trust_state", None) or "extracted").strip().lower()
    superseded = getattr(assertion, "superseded_by_assertion_id", None) is not None
    replaced = getattr(assertion, "replaced_by_assertion_id", None) is not None
    is_current = bool(getattr(assertion, "is_current", False))
    conflict_hints = _conflict_hints_for_assertion(assertion)
    manual_review_required = _requires_manual_review(assertion)
    artifact_backed = _artifact_backed_assertion(assertion)

    approved_like = governance_state in {"active", "approved"} and review_status == "verified"
    safe_for_projection = (
        approved_like
        and rule_status in {"active", "approved"}
        and validation_state == "validated"
        and trust_state in {"validated", "trusted"}
        and coverage_status not in NON_PROJECTABLE_COVERAGE_STATUSES
        and not superseded and not replaced and not conflict_hints and not manual_review_required and is_current
    )

    partial_projectable = (
        not safe_for_projection and approved_like and validation_state == 'validated' and trust_state in {'validated','trusted'}
        and not superseded and not replaced and not manual_review_required and coverage_status not in {'candidate','conflicting','stale','superseded'}
    )
    if artifact_backed and not safe_for_projection and approved_like and validation_state == 'validated' and trust_state in {'validated','trusted'}:
        partial_projectable = True

    lifecycle_blockers=[]
    if not approved_like: lifecycle_blockers.append(f'governance_state={governance_state or "unknown"}')
    if rule_status not in {'active','approved'}: lifecycle_blockers.append(f'rule_status={rule_status or "unknown"}')
    if review_status != 'verified': lifecycle_blockers.append(f'review_status={review_status or "unknown"}')
    if validation_state != 'validated': lifecycle_blockers.append(f'validation_state={validation_state or "unknown"}')
    if trust_state not in {'validated','trusted'}: lifecycle_blockers.append(f'trust_state={trust_state or "unknown"}')
    if coverage_status in NON_PROJECTABLE_COVERAGE_STATUSES: lifecycle_blockers.append(f'coverage_status={coverage_status}')
    if superseded: lifecycle_blockers.append('superseded')
    if replaced: lifecycle_blockers.append('replaced')
    if not is_current: lifecycle_blockers.append('not_current')
    if manual_review_required: lifecycle_blockers.append('manual_review_required')
    lifecycle_blockers.extend(conflict_hints)
    return {
        'assertion_id': int(getattr(assertion,'id',0) or 0),
        'rule_key': getattr(assertion,'rule_key',None),
        'normalized_category': getattr(assertion,'normalized_category',None) or getattr(assertion,'rule_category',None),
        'governance_state': governance_state,
        'rule_status': rule_status,
        'review_status': review_status,
        'coverage_status': coverage_status,
        'validation_state': validation_state,
        'trust_state': trust_state,
        'is_current': is_current,
        'safe_for_projection': safe_for_projection,
        'partial_projectable': partial_projectable,
        'manual_review_required': manual_review_required,
        'artifact_backed': artifact_backed,
        'lifecycle_blockers': sorted(set(lifecycle_blockers)),
    }


def is_assertion_governed_active(assertion: PolicyAssertion) -> bool:
    return bool(assertion_governance_summary(assertion)["safe_for_projection"])


def ensure_seeded_for_org(db: Session, *, org_id: int) -> dict[str, Any]:
    existing = db.scalar(select(JurisdictionRule.id).where(JurisdictionRule.org_id == org_id).limit(1))
    if existing is not None:
        return {"seeded": False, "reason": "org already has jurisdiction rules"}

    globals_ = db.execute(select(JurisdictionRule).where(JurisdictionRule.org_id.is_(None))).scalars().all()
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


def create_rule(db: Session, *, org_id: int, actor_user_id: Optional[int], payload: dict[str, Any]) -> JurisdictionRule:
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
    audit_write(db, org_id=org_id, actor_user_id=actor_user_id, action="jurisdiction_rule_created", entity_type="jurisdiction_rule", entity_id=str(row.id), before=None, after=_jr_to_dict(row), commit=True)
    return row


def update_rule(db: Session, *, org_id: int, actor_user_id: Optional[int], rule_id: int, payload: dict[str, Any]) -> JurisdictionRule:
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
    audit_write(db, org_id=org_id, actor_user_id=actor_user_id, action="jurisdiction_rule_updated", entity_type="jurisdiction_rule", entity_id=str(row.id), before=before, after=_jr_to_dict(row), commit=True)
    return row


def list_rules_for_scope(db: Session, *, org_id: int | None, state: str, county: Optional[str] = None, city: Optional[str] = None) -> list[JurisdictionRule]:
    st = _norm_state(state)
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


def governed_assertions_for_scope(
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

    assertions_q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
    if org_id is None:
        assertions_q = assertions_q.filter(PolicyAssertion.org_id.is_(None))
    else:
        assertions_q = assertions_q.filter(or_(PolicyAssertion.org_id == int(org_id), PolicyAssertion.org_id.is_(None)))

    scoped: list[PolicyAssertion] = []
    for a in assertions_q.all():
        if getattr(a, "county", None) is not None and _norm_rule_scope_value(a.county) != cnty:
            continue
        if getattr(a, "city", None) is not None and _norm_rule_scope_value(a.city) != cty:
            continue
        if getattr(a, "pha_name", None) is not None and (a.pha_name or "").strip() != (pha or ""):
            continue
        scoped.append(a)

    safe: list[PolicyAssertion] = []
    partial: list[PolicyAssertion] = []
    excluded: list[PolicyAssertion] = []
    manual_review: list[PolicyAssertion] = []
    summaries: list[dict[str, Any]] = []

    for assertion in scoped:
        summary = assertion_governance_summary(assertion)
        summaries.append(summary)
        if summary["safe_for_projection"]:
            safe.append(assertion)
        elif summary["manual_review_required"]:
            manual_review.append(assertion)
        elif summary["partial_projectable"]:
            partial.append(assertion)
        else:
            excluded.append(assertion)

    category_counts: dict[str, dict[str, int]] = {}
    for row, bucket in (
        [(a, "safe") for a in safe]
        + [(a, "partial") for a in partial]
        + [(a, "excluded") for a in excluded]
        + [(a, "manual_review") for a in manual_review]
    ):
        category = getattr(row, "normalized_category", None) or getattr(row, "rule_category", None) or "uncategorized"
        bucket_counts = category_counts.setdefault(category, {"safe": 0, "partial": 0, "excluded": 0, "manual_review": 0})
        bucket_counts[bucket] += 1

    return {
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "safe_assertion_ids": [int(a.id) for a in safe if getattr(a, "id", None) is not None],
        "partial_assertion_ids": [int(a.id) for a in partial if getattr(a, "id", None) is not None],
        "excluded_assertion_ids": [int(a.id) for a in excluded if getattr(a, "id", None) is not None],
        "manual_review_ids": [int(a.id) for a in manual_review if getattr(a, "id", None) is not None],
        "safe_count": len(safe),
        "partial_count": len(partial),
        "excluded_count": len(excluded),
        "manual_review_count": len(manual_review),
        "category_counts": category_counts,
        "assertion_summaries": summaries,
    }


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
    governed = governed_assertions_for_scope(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)

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
        source_evidence.append({
            "kind": "jurisdiction_rule",
            "id": int(rule.id),
            "scope": "org" if getattr(rule, "org_id", None) is not None else "global",
            "city": getattr(rule, "city", None),
            "state": getattr(rule, "state", None),
            "updated_at": getattr(rule, "updated_at", None).isoformat() if getattr(rule, "updated_at", None) else None,
            "inspection_authority": getattr(rule, "inspection_authority", None),
            "inspection_frequency": getattr(rule, "inspection_frequency", None),
            "typical_fail_points": _loads_json_list(getattr(rule, "typical_fail_points_json", None)),
        })

    for a in assertions:
        governed_summary = assertion_governance_summary(a)
        source_evidence.append({
            "kind": "policy_assertion",
            "id": int(a.id),
            "rule_key": getattr(a, "rule_key", None),
            "review_status": getattr(a, "review_status", None),
            "normalized_category": getattr(a, "normalized_category", None),
            "source_id": getattr(a, "source_id", None),
            "confidence": float(getattr(a, "confidence", 0.0) or 0.0),
            "governance_state": governed_summary["governance_state"],
            "rule_status": governed_summary["rule_status"],
            "coverage_status": governed_summary["coverage_status"],
            "validation_state": governed_summary["validation_state"],
            "trust_state": governed_summary["trust_state"],
            "safe_for_projection": governed_summary["safe_for_projection"],
            "manual_review_required": governed_summary["manual_review_required"],
            "artifact_backed": governed_summary.get("artifact_backed", False),
            "lifecycle_blockers": governed_summary["lifecycle_blockers"],
        })

    return {
        "state": st,
        "county": cnty,
        "city": cty,
        "pha_name": pha,
        "layers": layers,
        "source_evidence": source_evidence,
        "rule_count": len(rules),
        "assertion_count": len(assertions),
        "governed_assertion_count": int(governed["safe_count"]),
        "partial_assertion_count": int(governed["partial_count"]),
        "excluded_assertion_count": int(governed["excluded_count"]),
        "manual_review_count": int(governed.get("manual_review_count", 0)),
        "resolved_rule_version": f"jr:{st}:{cnty or '-'}:{cty or '-'}:{pha or '-'}:{len(rules)}:{governed['safe_count']}:{governed['partial_count']}",
        "governance_dependency": {
            "safe_states": sorted(SAFE_GOVERNANCE_STATES),
            "non_projectable_states": sorted(NON_PROJECTABLE_GOVERNANCE_STATES),
            "non_projectable_rule_statuses": sorted(NON_PROJECTABLE_RULE_STATUSES),
        },
        "governed_assertions": governed,
    }




# === approved-only governance overlay (surgical) ===

try:
    _APPROVED_ONLY_ASSERTION_GOVERNANCE_SUMMARY = assertion_governance_summary
except NameError:
    _APPROVED_ONLY_ASSERTION_GOVERNANCE_SUMMARY = None

try:
    _APPROVED_ONLY_GOVERNED_ASSERTIONS_FOR_SCOPE = governed_assertions_for_scope
except NameError:
    _APPROVED_ONLY_GOVERNED_ASSERTIONS_FOR_SCOPE = None


def _assertion_lifecycle_value(assertion: PolicyAssertion) -> str:
    for value in [
        getattr(assertion, "lifecycle", None),
        getattr(assertion, "lifecycle_state", None),
        (_loads_json_dict(getattr(assertion, "change_summary", None)) or {}).get("lifecycle"),
        (_loads_json_dict(getattr(assertion, "change_summary", None)) or {}).get("lifecycle_state"),
    ]:
        raw = str(value or "").strip().lower()
        if raw:
            return raw
    governance_state = (getattr(assertion, "governance_state", None) or "").strip().lower()
    if governance_state in {"approved", "active", "draft", "replaced"}:
        return governance_state
    return "draft"


def assertion_governance_summary(assertion: PolicyAssertion) -> dict[str, Any]:
    summary = dict(_APPROVED_ONLY_ASSERTION_GOVERNANCE_SUMMARY(assertion) or {}) if _APPROVED_ONLY_ASSERTION_GOVERNANCE_SUMMARY else {}
    lifecycle = _assertion_lifecycle_value(assertion)
    governance_state = (getattr(assertion, "governance_state", None) or "").strip().lower()
    review_status = (getattr(assertion, "review_status", None) or "").strip().lower()
    validation_state = (getattr(assertion, "validation_state", None) or "").strip().lower()
    trust_state = (getattr(assertion, "trust_state", None) or "").strip().lower()
    coverage_status = (getattr(assertion, "coverage_status", None) or "").strip().lower()
    conflict_hints = _conflict_hints_for_assertion(assertion)
    approved_only = lifecycle in {"approved", "active"} and governance_state in {"approved", "active"}
    safe_for_projection = bool(
        approved_only
        and review_status == "verified"
        and validation_state == "validated"
        and trust_state in {"validated", "trusted"}
        and coverage_status not in NON_PROJECTABLE_COVERAGE_STATUSES
        and not conflict_hints
        and bool(getattr(assertion, "is_current", False))
        and getattr(assertion, "superseded_by_assertion_id", None) is None
        and getattr(assertion, "replaced_by_assertion_id", None) is None
    )
    partial_projectable = bool(
        not safe_for_projection
        and approved_only
        and review_status == "verified"
        and validation_state == "validated"
        and trust_state in {"validated", "trusted"}
        and coverage_status not in {"candidate", "conflicting", "stale", "superseded", "inferred", "partial"}
        and not conflict_hints
        and getattr(assertion, "superseded_by_assertion_id", None) is None
        and getattr(assertion, "replaced_by_assertion_id", None) is None
    )
    blockers = list(summary.get("lifecycle_blockers") or [])
    if lifecycle not in {"approved", "active"}:
        blockers.append(f"lifecycle={lifecycle}")
    if governance_state not in {"approved", "active"}:
        blockers.append(f"governance_state={governance_state or 'unknown'}")
    if review_status != "verified":
        blockers.append(f"review_status={review_status or 'unknown'}")
    if validation_state != "validated":
        blockers.append(f"validation_state={validation_state or 'unknown'}")
    if trust_state not in {"validated", "trusted"}:
        blockers.append(f"trust_state={trust_state or 'unknown'}")
    if coverage_status in NON_PROJECTABLE_COVERAGE_STATUSES:
        blockers.append(f"coverage_status={coverage_status}")
    summary.update({
        "lifecycle": lifecycle,
        "safe_for_projection": safe_for_projection,
        "partial_projectable": partial_projectable,
        "lifecycle_blockers": sorted(set(str(x) for x in blockers if str(x).strip())),
        "approved_only_mode": True,
    })
    return summary


def governed_assertions_for_scope(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    payload = dict(_APPROVED_ONLY_GOVERNED_ASSERTIONS_FOR_SCOPE(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )) if _APPROVED_ONLY_GOVERNED_ASSERTIONS_FOR_SCOPE else {}

    summaries = list(payload.get("summaries") or [])
    if not summaries:
        # rebuild from currently scoped assertions to ensure approved-only buckets are authoritative
        st = _norm_state(state)
        cnty = _norm_rule_scope_value(county)
        cty = _norm_rule_scope_value(city)
        pha = (pha_name or "").strip() or None
        assertions_q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)
        if org_id is None:
            assertions_q = assertions_q.filter(PolicyAssertion.org_id.is_(None))
        else:
            assertions_q = assertions_q.filter(or_(PolicyAssertion.org_id == int(org_id), PolicyAssertion.org_id.is_(None)))
        scoped = []
        for a in assertions_q.all():
            if getattr(a, "county", None) is not None and _norm_rule_scope_value(a.county) != cnty:
                continue
            if getattr(a, "city", None) is not None and _norm_rule_scope_value(a.city) != cty:
                continue
            if getattr(a, "pha_name", None) is not None and (a.pha_name or "").strip() != (pha or ""):
                continue
            scoped.append(a)
        summaries = [assertion_governance_summary(a) for a in scoped]

    safe_ids = [int(s.get("assertion_id") or 0) for s in summaries if s.get("safe_for_projection")]
    partial_ids = [int(s.get("assertion_id") or 0) for s in summaries if s.get("partial_projectable") and not s.get("safe_for_projection")]
    manual_ids = [int(s.get("assertion_id") or 0) for s in summaries if s.get("manual_review_required")]
    excluded_ids = [int(s.get("assertion_id") or 0) for s in summaries if int(s.get("assertion_id") or 0) not in set(safe_ids + partial_ids + manual_ids)]

    category_counts: dict[str, dict[str, int]] = {}
    for summary in summaries:
        category = str(summary.get("normalized_category") or "uncategorized")
        bucket = "excluded"
        if int(summary.get("assertion_id") or 0) in set(safe_ids):
            bucket = "safe"
        elif int(summary.get("assertion_id") or 0) in set(partial_ids):
            bucket = "partial"
        elif int(summary.get("assertion_id") or 0) in set(manual_ids):
            bucket = "manual_review"
        category_counts.setdefault(category, {"safe": 0, "partial": 0, "excluded": 0, "manual_review": 0})[bucket] += 1

    payload.update({
        "safe_assertion_ids": safe_ids,
        "partial_assertion_ids": partial_ids,
        "excluded_assertion_ids": excluded_ids,
        "manual_review_assertion_ids": manual_ids,
        "safe_count": len(safe_ids),
        "partial_count": len(partial_ids),
        "excluded_count": len(excluded_ids),
        "manual_review_count": len(manual_ids),
        "category_counts": category_counts,
        "approved_only_mode": True,
        "summaries": summaries,
    })
    return payload
