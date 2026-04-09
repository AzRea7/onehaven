from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy import or_, select, text
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_categories
from app.domain.jurisdiction_defaults import required_categories_for_city
from app.policy_models import (
    JurisdictionCoverageStatus,
    JurisdictionProfile,
    PolicyAssertion,
    PropertyComplianceEvidence,
    PropertyComplianceProjection,
    PropertyComplianceProjectionItem,
)

RULE_KEY_TO_CATEGORY = {
    "rental_registration_required": "registration",
    "inspection_required": "inspection",
    "inspection_program_exists": "inspection",
    "certificate_required_before_occupancy": "occupancy",
    "certificate_of_occupancy_required": "occupancy",
    "local_registration_certificate_required": "registration",
    "lead_based_paint_paperwork_required": "safety",
    "smoke_detector_required": "safety",
    "smoke_detectors_required": "safety",
    "utility_service_required_before_inspection": "utilities",
    "utility_confirmation_required": "utilities",
    "local_jurisdiction_document_required": "permits",
    "pass_inspection_required": "inspection",
}

DOCUMENT_CATEGORY_RULE_MAP = {
    "inspection_report": ["pass_inspection_required", "inspection_required"],
    "pass_certificate": ["certificate_required_before_occupancy", "pass_inspection_required"],
    "reinspection_notice": ["inspection_required"],
    "repair_invoice": ["inspection_required"],
    "utility_confirmation": ["utility_confirmation_required"],
    "smoke_detector_proof": ["smoke_detector_required"],
    "lead_based_paint_paperwork": ["lead_based_paint_paperwork_required"],
    "local_jurisdiction_document": ["local_jurisdiction_document_required", "rental_registration_required"],
    "approval_letter": ["certificate_required_before_occupancy"],
    "denial_letter": ["inspection_required"],
    "photo_evidence": ["inspection_required"],
    "other_evidence": [],
}

INSPECTION_CODE_RULE_HINTS = {
    "SMOKE": "smoke_detector_required",
    "GFCI": "inspection_required",
    "HANDRAIL": "inspection_required",
    "CO_DETECTOR": "inspection_required",
    "COOKING": "inspection_required",
    "EGRESS": "inspection_required",
    "LEAD": "lead_based_paint_paperwork_required",
    "CERTIFICATE": "certificate_required_before_occupancy",
}

ACTIVE_GOVERNANCE = {"active", "approved"}
ACTIVE_REVIEW = {"verified", "accepted", "approved", "projected"}
FAILING_EVIDENCE = {"fail", "failed", "denied", "expired", "missing", "blocked"}
PASSING_EVIDENCE = {"pass", "passed", "verified", "satisfied", "confirmed", "complete"}

SOURCE_LEVEL_PRECEDENCE = {
    "property": 500,
    "program": 400,
    "local": 300,
    "county": 250,
    "city": 240,
    "state": 200,
    "federal": 100,
    "other": 50,
    "unknown": 0,
}

RULE_CATEGORY_COST_DEFAULTS: dict[str, float] = {
    "registration": 250.0,
    "inspection": 400.0,
    "occupancy": 350.0,
    "safety": 175.0,
    "utilities": 125.0,
    "permits": 300.0,
    "jurisdiction": 200.0,
    "jurisdiction_blocker": 250.0,
    "governance": 0.0,
    "other": 150.0,
}

RULE_CATEGORY_DAYS_DEFAULTS: dict[str, int] = {
    "registration": 7,
    "inspection": 10,
    "occupancy": 7,
    "safety": 3,
    "utilities": 2,
    "permits": 8,
    "jurisdiction": 5,
    "jurisdiction_blocker": 7,
    "governance": 0,
    "other": 4,
}


def _utcnow() -> datetime:
    return datetime.utcnow()


def _loads(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (list, dict)):
        return value
    try:
        parsed = json.loads(value)
        return parsed if parsed is not None else default
    except Exception:
        return default


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return json.dumps({})


def _norm_state(value: Optional[str]) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    return raw or None


def _norm_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None


@dataclass(frozen=True)
class PropertyScope:
    property_id: int | None
    state: str
    county: str | None
    city: str | None
    pha_name: str | None
    property_type: str | None
    jurisdiction_slug: str | None
    address: str | None


def _source_level_rank(value: Optional[str]) -> int:
    raw = str(value or "").strip().lower()
    return SOURCE_LEVEL_PRECEDENCE.get(raw, SOURCE_LEVEL_PRECEDENCE["unknown"])


def _specificity_score(assertion: PolicyAssertion, *, county: str | None, city: str | None, pha_name: str | None) -> int:
    score = 0
    if getattr(assertion, "state", None):
        score += 1
    if county and _norm_lower(getattr(assertion, "county", None)) == county:
        score += 2
    if city and _norm_lower(getattr(assertion, "city", None)) == city:
        score += 4
    if pha_name and _norm_text(getattr(assertion, "pha_name", None)) == pha_name:
        score += 3
    if getattr(assertion, "source_level", None) == "property":
        score += 5
    return score


def _assertion_matches_scope(
    assertion: PolicyAssertion,
    *,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> bool:
    a_county = _norm_lower(getattr(assertion, "county", None))
    a_city = _norm_lower(getattr(assertion, "city", None))
    a_pha = _norm_text(getattr(assertion, "pha_name", None))

    if a_county and a_county != county:
        return False
    if a_city and a_city != city:
        return False
    if a_pha and a_pha != pha_name:
        return False
    return True


def _query_inherited_assertions(
    db: Session,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    statuses: Iterable[str] | None = None,
) -> list[PolicyAssertion]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)
    if org_id is None:
        stmt = stmt.where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))

    rows = list(db.scalars(stmt).all())
    out: list[PolicyAssertion] = []
    allowed = {str(s).lower() for s in statuses or []}
    for row in rows:
        if not _assertion_matches_scope(row, county=cnty, city=cty, pha_name=pha):
            continue
        if row.superseded_by_assertion_id is not None:
            continue
        if allowed:
            gov = str(getattr(row, "governance_state", "") or "").lower()
            rev = str(getattr(row, "review_status", "") or "").lower()
            rule = str(getattr(row, "rule_status", "") or "").lower()
            if gov not in allowed and rev not in allowed and rule not in allowed:
                continue
        out.append(row)

    out.sort(
        key=lambda r: (
            -_source_level_rank(getattr(r, "source_level", None)),
            -_specificity_score(r, county=cnty, city=cty, pha_name=pha),
            -float(getattr(r, "confidence", 0.0) or 0.0),
            int(getattr(r, "priority", 100) or 100),
            int(getattr(r, "source_rank", 100) or 100),
            -int(getattr(r, "version_number", 1) or 1),
            int(getattr(r, "id", 0) or 0),
        )
    )
    return out


def _is_effective_assertion(row: PolicyAssertion) -> bool:
    governance_state = str(getattr(row, "governance_state", "") or "").lower()
    review_status = str(getattr(row, "review_status", "") or "").lower()
    rule_status = str(getattr(row, "rule_status", "") or "").lower()

    if governance_state in ACTIVE_GOVERNANCE:
        return True
    if governance_state == "draft" and review_status in ACTIVE_REVIEW and rule_status in {"candidate", "active", ""}:
        return True
    if governance_state == "" and review_status in ACTIVE_REVIEW:
        return True
    return False


def _rule_label(assertion: PolicyAssertion) -> str:
    value = _loads(getattr(assertion, "value_json", None), {})
    if isinstance(value, dict):
        for key in ("label", "title", "description", "rule_label"):
            raw = value.get(key)
            if raw:
                return str(raw)
    excerpt = str(getattr(assertion, "raw_excerpt", None) or "").strip()
    if excerpt:
        return excerpt[:120]
    return str(getattr(assertion, "rule_key", "rule")).replace("_", " ").title()


def _rule_value_state(assertion: PolicyAssertion) -> str:
    value = _loads(getattr(assertion, "value_json", None), {})
    if isinstance(value, dict):
        for key in ("status", "state", "answer", "value"):
            raw = value.get(key)
            if raw is None:
                continue
            text_value = str(raw).strip().lower()
            if text_value in {"yes", "required", "true", "must", "pass"}:
                return "yes"
            if text_value in {"conditional", "maybe", "depends"}:
                return "conditional"
            if text_value in {"no", "false", "not_required"}:
                return "no"
    if bool(getattr(assertion, "required", True)):
        return "yes"
    return "conditional"


def _category_for_assertion(assertion: PolicyAssertion | None) -> str:
    if assertion is None:
        return "other"
    normalized = str(getattr(assertion, "normalized_category", None) or "").strip().lower()
    if normalized:
        return normalized
    category = str(getattr(assertion, "rule_category", None) or "").strip().lower()
    if category:
        return category
    rule_key = str(getattr(assertion, "rule_key", None) or "").strip().lower()
    return RULE_KEY_TO_CATEGORY.get(rule_key, "other")


def _merge_effective_assertions(assertions: list[PolicyAssertion]) -> dict[str, PolicyAssertion]:
    out: dict[str, PolicyAssertion] = {}
    for row in assertions:
        if not _is_effective_assertion(row):
            continue
        rule_key = str(getattr(row, "rule_key", "") or "").strip()
        if not rule_key:
            continue
        if rule_key not in out:
            out[rule_key] = row
            continue

        current = out[rule_key]
        challenger_score = (
            _source_level_rank(getattr(row, "source_level", None)),
            _specificity_score(
                row,
                county=_norm_lower(getattr(row, "county", None)),
                city=_norm_lower(getattr(row, "city", None)),
                pha_name=_norm_text(getattr(row, "pha_name", None)),
            ),
            float(getattr(row, "confidence", 0.0) or 0.0),
            -(int(getattr(row, "priority", 100) or 100)),
            -(int(getattr(row, "source_rank", 100) or 100)),
            int(getattr(row, "version_number", 1) or 1),
            int(getattr(row, "id", 0) or 0),
        )
        incumbent_score = (
            _source_level_rank(getattr(current, "source_level", None)),
            _specificity_score(
                current,
                county=_norm_lower(getattr(current, "county", None)),
                city=_norm_lower(getattr(current, "city", None)),
                pha_name=_norm_text(getattr(current, "pha_name", None)),
            ),
            float(getattr(current, "confidence", 0.0) or 0.0),
            -(int(getattr(current, "priority", 100) or 100)),
            -(int(getattr(current, "source_rank", 100) or 100)),
            int(getattr(current, "version_number", 1) or 1),
            int(getattr(current, "id", 0) or 0),
        )
        if challenger_score > incumbent_score:
            out[rule_key] = row
    return out


def _group_assertions_by_rule(assertions: list[PolicyAssertion]) -> dict[str, list[PolicyAssertion]]:
    grouped: dict[str, list[PolicyAssertion]] = {}
    for row in assertions:
        if not _is_effective_assertion(row):
            continue
        rule_key = str(getattr(row, "rule_key", "") or "").strip()
        if not rule_key:
            continue
        grouped.setdefault(rule_key, []).append(row)

    for key, rows in grouped.items():
        rows.sort(
            key=lambda r: (
                -_source_level_rank(getattr(r, "source_level", None)),
                -float(getattr(r, "confidence", 0.0) or 0.0),
                int(getattr(r, "priority", 100) or 100),
                int(getattr(r, "source_rank", 100) or 100),
                -int(getattr(r, "version_number", 1) or 1),
                int(getattr(r, "id", 0) or 0),
            )
        )
        grouped[key] = rows
    return grouped


def _coverage_row(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> JurisdictionCoverageStatus | None:
    stmt = select(JurisdictionCoverageStatus).where(JurisdictionCoverageStatus.state == _norm_state(state))
    if org_id is None:
        stmt = stmt.where(JurisdictionCoverageStatus.org_id.is_(None))
    else:
        stmt = stmt.where(or_(JurisdictionCoverageStatus.org_id == org_id, JurisdictionCoverageStatus.org_id.is_(None)))
    rows = list(db.scalars(stmt).all())
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    for row in rows:
        if _norm_lower(getattr(row, "county", None)) not in {None, cnty}:
            continue
        if _norm_lower(getattr(row, "city", None)) not in {None, cty}:
            continue
        if _norm_text(getattr(row, "pha_name", None)) not in {None, pha}:
            continue
        return row
    return None


def _profile_row(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
) -> JurisdictionProfile | None:
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    try:
        stmt = select(JurisdictionProfile).where(JurisdictionProfile.state == _norm_state(state))
        if org_id is None:
            stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
        else:
            stmt = stmt.where(or_(JurisdictionProfile.org_id == org_id, JurisdictionProfile.org_id.is_(None)))
        rows = list(db.scalars(stmt).all())
        for row in rows:
            if _norm_lower(getattr(row, "county", None)) not in {None, cnty}:
                continue
            if _norm_lower(getattr(row, "city", None)) not in {None, cty}:
                continue
            return row
        return None
    except Exception:
        if not hasattr(db, "query"):
            raise
        query = db.query(JurisdictionProfile)
        try:
            query = query.filter(JurisdictionProfile.state == _norm_state(state))
            if org_id is None:
                query = query.filter(JurisdictionProfile.org_id.is_(None))
            else:
                query = query.filter(or_(JurisdictionProfile.org_id == org_id, JurisdictionProfile.org_id.is_(None)))
        except Exception:
            pass
        row = query.first()
        if row is None:
            return None
        if _norm_lower(getattr(row, "county", None)) not in {None, cnty}:
            return None
        if _norm_lower(getattr(row, "city", None)) not in {None, cty}:
            return None
        return row


def _build_property_scope(
    db: Session,
    *,
    org_id: Optional[int],
    property_id: int | None = None,
    property: Any | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
) -> PropertyScope:
    prop = property
    if prop is None and property_id is not None:
        row = db.execute(
            text(
                """
                SELECT id, address, state, county, city, property_type, program_type, jurisdiction_slug
                FROM properties
                WHERE id = :property_id AND (:org_id IS NULL OR org_id = :org_id)
                """
            ),
            {"property_id": int(property_id), "org_id": org_id},
        ).mappings().first()
        prop = row

    prop_state = _norm_state(getattr(prop, "state", None) if prop is not None else state)
    prop_county = _norm_lower(getattr(prop, "county", None) if prop is not None else county)
    prop_city = _norm_lower(getattr(prop, "city", None) if prop is not None else city)
    prop_pha = _norm_text(pha_name or (getattr(prop, "program_type", None) if prop is not None else None))
    prop_type = _norm_text(getattr(prop, "property_type", None) if prop is not None else None)
    jurisdiction_slug = _norm_text(getattr(prop, "jurisdiction_slug", None) if prop is not None else None)
    if jurisdiction_slug is None:
        parts = [p for p in [prop_city, prop_county] if p]
        jurisdiction_slug = "-".join(parts) if parts else None
    return PropertyScope(
        property_id=int(getattr(prop, "id", property_id)) if (prop is not None or property_id is not None) else None,
        state=prop_state,
        county=prop_county,
        city=prop_city,
        pha_name=prop_pha,
        property_type=prop_type,
        jurisdiction_slug=jurisdiction_slug,
        address=_norm_text(getattr(prop, "address", None) if prop is not None else None),
    )


def build_policy_summary(
    db: Session,
    assertions: list[PolicyAssertion],
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    effective = _merge_effective_assertions(assertions)
    effective_rows = list(effective.values())

    verified_rules: list[dict[str, Any]] = []
    blocking_items: list[dict[str, Any]] = []
    required_actions: list[dict[str, Any]] = []
    evidence_links: list[dict[str, Any]] = []
    local_rule_statuses: dict[str, str] = {}

    covered_categories = set()
    for row in effective_rows:
        rule_key = str(getattr(row, "rule_key", "") or "").strip()
        if not rule_key:
            continue
        status = _rule_value_state(row)
        category = _category_for_assertion(row)
        covered_categories.add(category)
        local_rule_statuses[rule_key] = status
        payload = {
            "id": int(getattr(row, "id", 0) or 0),
            "rule_key": rule_key,
            "label": _rule_label(row),
            "category": category,
            "status": status,
            "blocking": bool(getattr(row, "blocking", False)),
            "required": bool(getattr(row, "required", True)),
            "source_level": getattr(row, "source_level", None),
            "confidence": float(getattr(row, "confidence", 0.0) or 0.0),
            "source_citation": getattr(row, "source_citation", None),
            "raw_excerpt": getattr(row, "raw_excerpt", None),
        }
        verified_rules.append(payload)
        if payload["required"] and status in {"yes", "conditional"}:
            required_actions.append({"code": rule_key.upper(), "title": payload["label"], "category": category})
        if payload["blocking"]:
            blocking_items.append({"code": rule_key.upper(), "title": payload["label"], "category": category})
        if payload["source_citation"]:
            evidence_links.append({"rule_key": rule_key, "citation": payload["source_citation"]})

    required_categories = normalize_categories(
        required_categories_for_city(city, state=state, include_section8=bool(pha_name))
    )
    if not required_categories:
        required_categories = normalize_categories(["registration", "inspection", "safety"])
    category_coverage = {cat: ("verified" if cat in covered_categories else "missing") for cat in required_categories}
    for rule_key, status in local_rule_statuses.items():
        cat = RULE_KEY_TO_CATEGORY.get(rule_key)
        if cat and status == "conditional":
            category_coverage[cat] = "conditional"

    coverage_row = _coverage_row(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)
    coverage = {
        "coverage_status": getattr(coverage_row, "coverage_status", None) or ("verified_extended" if verified_rules else "not_started"),
        "production_readiness": getattr(coverage_row, "production_readiness", None) or ("ready" if verified_rules else "partial"),
        "confidence_label": (
            "high"
            if len(verified_rules) >= 6
            else "medium"
            if len(verified_rules) >= 3
            else "low"
        ),
        "completeness_score": float(getattr(coverage_row, "completeness_score", 0.0) or 0.0),
        "completeness_status": getattr(coverage_row, "completeness_status", None),
        "is_stale": bool(getattr(coverage_row, "is_stale", False)) if coverage_row is not None else False,
        "stale_reason": getattr(coverage_row, "stale_reason", None) if coverage_row is not None else None,
        "required_categories": _loads(getattr(coverage_row, "required_categories_json", None), required_categories),
        "covered_categories": _loads(getattr(coverage_row, "covered_categories_json", None), sorted(covered_categories)),
        "missing_categories": _loads(
            getattr(coverage_row, "missing_categories_json", None),
            [cat for cat, status in category_coverage.items() if status == "missing"],
        ),
    }
    if coverage.get("required_categories"):
        required_categories = normalize_categories(coverage["required_categories"])
    return {
        "coverage": coverage,
        "verified_rules": verified_rules,
        "required_actions": required_actions,
        "blocking_items": blocking_items,
        "evidence_links": evidence_links,
        "local_rule_statuses": local_rule_statuses,
        "verified_rule_count_local": len(verified_rules),
        "verified_rule_count_effective": len(verified_rules),
        "required_categories": required_categories,
        "category_coverage": category_coverage,
        "completeness_status": coverage.get("completeness_status") or ("partial" if verified_rules else "missing"),
        "completeness_score": float(coverage.get("completeness_score") or 0.0),
        "stale_status": "stale" if coverage.get("is_stale") else "fresh",
    }


def _document_rule_keys(category: str, metadata: dict[str, Any] | None = None, extracted_text: str | None = None) -> list[str]:
    out = list(DOCUMENT_CATEGORY_RULE_MAP.get(str(category or "other_evidence"), []))
    joined = " ".join(
        [
            str(category or ""),
            str((metadata or {}).get("label") or ""),
            str(extracted_text or ""),
        ]
    ).lower()
    if "registration" in joined:
        out.append("rental_registration_required")
    if "certificate" in joined or "occupancy" in joined:
        out.append("certificate_required_before_occupancy")
    if "utility" in joined:
        out.append("utility_confirmation_required")
    if "lead" in joined:
        out.append("lead_based_paint_paperwork_required")
    if "smoke" in joined:
        out.append("smoke_detector_required")
    if "inspect" in joined:
        out.append("inspection_required")
    return sorted({key for key in out if key})


def _inspection_rule_keys(code: str, category: str | None = None, fail_reason: str | None = None) -> list[str]:
    haystack = " ".join([str(code or ""), str(category or ""), str(fail_reason or "")]).upper()
    out: list[str] = []
    for token, rule_key in INSPECTION_CODE_RULE_HINTS.items():
        if token in haystack:
            out.append(rule_key)
    if not out:
        out.append("inspection_required")
    return sorted(set(out))


def _upsert_evidence(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    evidence_source_type: str,
    evidence_key: str,
    evidence_name: str | None,
    evidence_status: str,
    proof_state: str,
    satisfies_rule: bool | None,
    notes: str | None = None,
    projection_item_id: int | None = None,
    policy_assertion_id: int | None = None,
    compliance_document_id: int | None = None,
    inspection_id: int | None = None,
    checklist_item_id: int | None = None,
    observed_at: datetime | None = None,
    expires_at: datetime | None = None,
    source_details: dict[str, Any] | None = None,
) -> PropertyComplianceEvidence:
    row = db.scalar(
        select(PropertyComplianceEvidence).where(
            PropertyComplianceEvidence.org_id == org_id,
            PropertyComplianceEvidence.property_id == property_id,
            PropertyComplianceEvidence.evidence_source_type == evidence_source_type,
            PropertyComplianceEvidence.evidence_key == evidence_key,
        )
    )
    if row is None:
        row = PropertyComplianceEvidence(
            org_id=org_id,
            property_id=property_id,
            evidence_source_type=evidence_source_type,
            evidence_key=evidence_key,
            evidence_name=evidence_name,
            observed_at=observed_at,
        )
        db.add(row)

    row.projection_item_id = projection_item_id
    row.policy_assertion_id = policy_assertion_id
    row.compliance_document_id = compliance_document_id
    row.inspection_id = inspection_id
    row.checklist_item_id = checklist_item_id
    row.evidence_name = evidence_name
    row.evidence_status = evidence_status
    row.proof_state = proof_state
    row.satisfies_rule = satisfies_rule
    row.observed_at = observed_at or row.observed_at
    row.expires_at = expires_at
    row.notes = notes
    row.source_details_json = _dumps(source_details or {})
    row.updated_at = _utcnow()
    db.flush()
    return row


def sync_document_evidence_for_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    document_id: int | None = None,
) -> dict[str, Any]:
    where = ["org_id = :org_id", "property_id = :property_id", "deleted_at IS NULL"]
    params: dict[str, Any] = {"org_id": int(org_id), "property_id": int(property_id)}
    if document_id is not None:
        where.append("id = :document_id")
        params["document_id"] = int(document_id)

    rows = db.execute(
        text(
            f"""
            SELECT *
            FROM compliance_documents
            WHERE {' AND '.join(where)}
            ORDER BY id ASC
            """
        ),
        params,
    ).mappings().all()

    created_or_updated = 0
    linked_rule_keys: set[str] = set()
    for row in rows:
        metadata = _loads(row.get("metadata_json"), {})
        parser_meta = _loads(row.get("parser_meta_json"), {})
        rule_keys = _document_rule_keys(
            str(row.get("category") or "other_evidence"),
            metadata={**metadata, "label": row.get("label")},
            extracted_text=row.get("extracted_text_preview"),
        )
        scan_status = str(row.get("scan_status") or "unknown").lower()
        parse_status = str(row.get("parse_status") or "unknown").lower()
        status = "verified" if scan_status in {"clean", "ok", "unknown"} else "blocked"
        proof_state = "confirmed" if parse_status in {"parsed", "queued", "skipped"} else "inferred"
        satisfies_rule = False if status == "blocked" else True
        if not rule_keys:
            rule_keys = [f"document_category::{str(row.get('category') or 'other_evidence')}"]

        for rule_key in rule_keys:
            evidence_key = f"document:{int(row['id'])}:{rule_key}"
            _upsert_evidence(
                db,
                org_id=org_id,
                property_id=property_id,
                evidence_source_type="document",
                evidence_key=evidence_key,
                evidence_name=row.get("label") or row.get("original_filename") or rule_key,
                evidence_status=status,
                proof_state=proof_state,
                satisfies_rule=satisfies_rule,
                compliance_document_id=int(row["id"]),
                inspection_id=int(row["inspection_id"]) if row.get("inspection_id") is not None else None,
                checklist_item_id=int(row["checklist_item_id"]) if row.get("checklist_item_id") is not None else None,
                observed_at=row.get("created_at"),
                source_details={
                    "rule_key": rule_key,
                    "category": row.get("category"),
                    "metadata": metadata,
                    "parser_meta": parser_meta,
                    "parse_status": parse_status,
                    "scan_status": scan_status,
                },
            )
            created_or_updated += 1
            linked_rule_keys.add(rule_key)

    return {
        "ok": True,
        "property_id": int(property_id),
        "document_count": len(rows),
        "linked_rule_keys": sorted(linked_rule_keys),
        "evidence_rows": created_or_updated,
    }


def sync_inspection_evidence_for_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    where = ["i.org_id = :org_id", "i.property_id = :property_id"]
    params: dict[str, Any] = {"org_id": int(org_id), "property_id": int(property_id)}
    if inspection_id is not None:
        where.append("i.id = :inspection_id")
        params["inspection_id"] = int(inspection_id)

    rows = db.execute(
        text(
            f"""
            SELECT
                i.id AS inspection_id,
                i.inspection_date,
                i.passed,
                i.result_status AS inspection_result_status,
                ii.id AS inspection_item_id,
                ii.code,
                ii.category,
                ii.result_status,
                ii.fail_reason,
                ii.details,
                ii.severity,
                ii.requires_reinspection
            FROM inspections i
            LEFT JOIN inspection_items ii ON ii.inspection_id = i.id
            WHERE {' AND '.join(where)}
            ORDER BY i.id DESC, ii.id ASC
            """
        ),
        params,
    ).mappings().all()

    created_or_updated = 0
    linked_rule_keys: set[str] = set()
    for row in rows:
        if row.get("inspection_item_id") is None:
            continue

        rule_keys = _inspection_rule_keys(
            str(row.get("code") or ""),
            category=row.get("category"),
            fail_reason=row.get("fail_reason"),
        )
        item_status = str(row.get("result_status") or "").lower().strip()
        if not item_status:
            item_status = "fail" if bool(row.get("details")) and bool(row.get("severity", 0) or 0) >= 3 else "pass"

        evidence_status = "failed" if item_status in {"fail", "blocked", "inconclusive"} else "verified"
        satisfies_rule = evidence_status == "verified"
        proof_state = "confirmed"

        for rule_key in rule_keys:
            evidence_key = f"inspection_item:{int(row['inspection_item_id'])}:{rule_key}"
            _upsert_evidence(
                db,
                org_id=org_id,
                property_id=property_id,
                evidence_source_type="inspection_item",
                evidence_key=evidence_key,
                evidence_name=f"Inspection {row.get('code') or row.get('inspection_item_id')}",
                evidence_status=evidence_status,
                proof_state=proof_state,
                satisfies_rule=satisfies_rule,
                inspection_id=int(row["inspection_id"]),
                observed_at=row.get("inspection_date"),
                source_details={
                    "inspection_item_id": row.get("inspection_item_id"),
                    "rule_key": rule_key,
                    "code": row.get("code"),
                    "category": row.get("category"),
                    "fail_reason": row.get("fail_reason"),
                    "details": row.get("details"),
                    "severity": row.get("severity"),
                    "requires_reinspection": row.get("requires_reinspection"),
                },
            )
            created_or_updated += 1
            linked_rule_keys.add(rule_key)

    return {
        "ok": True,
        "property_id": int(property_id),
        "inspection_id": int(inspection_id) if inspection_id is not None else None,
        "linked_rule_keys": sorted(linked_rule_keys),
        "evidence_rows": created_or_updated,
    }


def _current_projection(db: Session, *, org_id: int, property_id: int) -> PropertyComplianceProjection | None:
    return db.scalar(
        select(PropertyComplianceProjection).where(
            PropertyComplianceProjection.org_id == org_id,
            PropertyComplianceProjection.property_id == property_id,
            PropertyComplianceProjection.is_current.is_(True),
        )
    )


def _current_projection_items(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    projection_id: int,
) -> list[PropertyComplianceProjectionItem]:
    return list(
        db.scalars(
            select(PropertyComplianceProjectionItem).where(
                PropertyComplianceProjectionItem.org_id == org_id,
                PropertyComplianceProjectionItem.property_id == property_id,
                PropertyComplianceProjectionItem.projection_id == projection_id,
            )
        ).all()
    )


def _evidence_rows(db: Session, *, org_id: int, property_id: int) -> list[PropertyComplianceEvidence]:
    return list(
        db.scalars(
            select(PropertyComplianceEvidence).where(
                PropertyComplianceEvidence.org_id == org_id,
                PropertyComplianceEvidence.property_id == property_id,
            )
        ).all()
    )


def _build_evidence_index(rows: list[PropertyComplianceEvidence]) -> dict[str, list[PropertyComplianceEvidence]]:
    out: dict[str, list[PropertyComplianceEvidence]] = {}
    for row in rows:
        details = _loads(getattr(row, "source_details_json", None), {})
        rule_key = str(details.get("rule_key") or "").strip()
        if not rule_key:
            continue
        out.setdefault(rule_key, []).append(row)
    return out


def _latest_datetime(values: list[datetime | None]) -> datetime | None:
    filtered = [v for v in values if v is not None]
    return max(filtered) if filtered else None


def _estimate_cost(rule_category: str, assertion: PolicyAssertion | None, evidence_rows: list[PropertyComplianceEvidence]) -> float | None:
    if assertion is not None:
        payload = _loads(getattr(assertion, "value_json", None), {})
        if isinstance(payload, dict):
            for key in ("estimated_cost", "cost_estimate", "projected_cost"):
                raw = payload.get(key)
                if raw is not None:
                    try:
                        return float(raw)
                    except Exception:
                        pass

    for row in evidence_rows:
        details = _loads(getattr(row, "source_details_json", None), {})
        raw = details.get("estimated_cost")
        if raw is not None:
            try:
                return float(raw)
            except Exception:
                continue

    return RULE_CATEGORY_COST_DEFAULTS.get(rule_category, RULE_CATEGORY_COST_DEFAULTS["other"])


def _estimate_days(rule_category: str, assertion: PolicyAssertion | None, evidence_rows: list[PropertyComplianceEvidence]) -> int | None:
    if assertion is not None:
        payload = _loads(getattr(assertion, "value_json", None), {})
        if isinstance(payload, dict):
            for key in ("estimated_days", "days_to_complete", "projected_days"):
                raw = payload.get(key)
                if raw is not None:
                    try:
                        return int(raw)
                    except Exception:
                        pass

    for row in evidence_rows:
        details = _loads(getattr(row, "source_details_json", None), {})
        raw = details.get("estimated_days")
        if raw is not None:
            try:
                return int(raw)
            except Exception:
                continue

    return RULE_CATEGORY_DAYS_DEFAULTS.get(rule_category, RULE_CATEGORY_DAYS_DEFAULTS["other"])


def _compute_proof_state(evidence_rows: list[PropertyComplianceEvidence]) -> str:
    if not evidence_rows:
        return "unknown"

    states = {str(getattr(r, "proof_state", "") or "").lower() for r in evidence_rows}
    if "conflicting" in states:
        return "conflicting"
    if "confirmed" in states and ("inferred" in states or "unknown" in states):
        return "confirmed"
    if "confirmed" in states:
        return "confirmed"
    if "stale" in states:
        return "stale"
    if "inferred" in states:
        return "inferred"
    return "unknown"


def _projection_status_from_counts(*, blocking: int, stale: int, unknown: int, conflicting: int) -> str:
    if conflicting > 0:
        return "conflicting"
    if blocking > 0:
        return "blocked"
    if stale > 0:
        return "stale"
    if unknown > 0:
        return "partial"
    return "computed"


def _layer_summary_for_rule(assertions: list[PolicyAssertion]) -> dict[str, Any]:
    return {
        "layers": [
            {
                "assertion_id": int(getattr(row, "id", 0) or 0),
                "source_level": getattr(row, "source_level", None),
                "jurisdiction_slug": getattr(row, "jurisdiction_slug", None),
                "rule_status": getattr(row, "rule_status", None),
                "governance_state": getattr(row, "governance_state", None),
                "confidence": float(getattr(row, "confidence", 0.0) or 0.0),
                "priority": int(getattr(row, "priority", 100) or 100),
                "source_rank": int(getattr(row, "source_rank", 100) or 100),
                "version_number": int(getattr(row, "version_number", 1) or 1),
                "required": bool(getattr(row, "required", True)),
                "blocking": bool(getattr(row, "blocking", False)),
                "source_citation": getattr(row, "source_citation", None),
            }
            for row in assertions
        ]
    }


def _evaluate_rule(
    *,
    rule_key: str,
    assertion: PolicyAssertion | None,
    layer_assertions: list[PolicyAssertion],
    evidence_rows: list[PropertyComplianceEvidence],
) -> dict[str, Any]:
    required = bool(getattr(assertion, "required", True)) if assertion is not None else True
    blocking = bool(getattr(assertion, "blocking", False)) if assertion is not None else False
    confidence = float(getattr(assertion, "confidence", 0.0) or 0.0) if assertion is not None else 0.5
    rule_category = _category_for_assertion(assertion) if assertion is not None else RULE_KEY_TO_CATEGORY.get(rule_key, "inspection")

    evidence_summary_parts: list[str] = []
    evidence_gap: str | None = None
    status_reason: str | None = None
    estimated_cost: float | None = None
    estimated_days: int | None = None

    if not evidence_rows:
        evaluation_status = "unknown"
        evidence_status = "missing"
        proof_state = "unknown"
        evidence_gap = "No evidence linked to this rule yet."
        status_reason = "Required rule has no supporting or resolving property evidence."
    else:
        failing = [row for row in evidence_rows if str(getattr(row, "evidence_status", "") or "").lower() in FAILING_EVIDENCE]
        passing = [row for row in evidence_rows if str(getattr(row, "evidence_status", "") or "").lower() in PASSING_EVIDENCE]
        expired = [row for row in evidence_rows if str(getattr(row, "evidence_status", "") or "").lower() == "expired"]
        proof_state = _compute_proof_state(evidence_rows)

        if failing and passing:
            evaluation_status = "conflicting"
            evidence_status = "conflicting"
            evidence_gap = "Both passing and failing evidence exist."
            status_reason = "At least one evidence record satisfies the rule while another shows a failure or unresolved issue."
        elif expired and not passing and not failing:
            evaluation_status = "stale"
            evidence_status = "expired"
            evidence_gap = "Evidence exists but is expired or stale."
            status_reason = "The best available evidence is no longer current."
        elif failing:
            evaluation_status = "fail"
            evidence_status = str(getattr(failing[0], "evidence_status", "failed") or "failed").lower()
            evidence_gap = str(
                getattr(failing[0], "notes", None)
                or "Inspection or document evidence still shows an unresolved issue."
            )
            status_reason = "Linked property evidence currently indicates the rule is not satisfied."
            estimated_cost = _estimate_cost(rule_category, assertion, evidence_rows)
            estimated_days = _estimate_days(rule_category, assertion, evidence_rows)
        elif passing:
            evaluation_status = "pass"
            evidence_status = "satisfied"
            evidence_gap = None
            status_reason = "Linked property evidence shows the rule is satisfied."
        else:
            evaluation_status = "unknown"
            evidence_status = "unknown"
            proof_state = _compute_proof_state(evidence_rows)
            evidence_gap = "Evidence exists but could not be confidently evaluated."
            status_reason = "Evidence is present but does not clearly prove satisfaction or failure."

        for row in evidence_rows:
            details = _loads(getattr(row, "source_details_json", None), {})
            label = getattr(row, "evidence_name", None) or details.get("code") or details.get("category") or rule_key
            evidence_summary_parts.append(str(label))

    if evaluation_status == "pass":
        blocking = False

    latest_evidence_updated_at = _latest_datetime(
        [getattr(r, "updated_at", None) for r in evidence_rows] + [getattr(r, "observed_at", None) for r in evidence_rows]
    )
    proof_confidence = max(
        [float(getattr(r, "confidence", 0.0) or 0.0) for r in evidence_rows] + [confidence],
        default=confidence,
    )

    return {
        "rule_key": rule_key,
        "rule_category": rule_category,
        "required": required,
        "blocking": blocking,
        "evaluation_status": evaluation_status,
        "evidence_status": evidence_status,
        "proof_state": proof_state,
        "confidence": max(0.1, proof_confidence),
        "estimated_cost": estimated_cost,
        "estimated_days": estimated_days,
        "evidence_summary": ", ".join(sorted(set(evidence_summary_parts))) or None,
        "evidence_gap": evidence_gap,
        "status_reason": status_reason,
        "source_citation": getattr(assertion, "source_citation", None) if assertion is not None else None,
        "raw_excerpt": getattr(assertion, "raw_excerpt", None) if assertion is not None else None,
        "rule_value_json": getattr(assertion, "value_json", None) if assertion is not None else None,
        "conflicting_evidence_count": len(evidence_rows) if evaluation_status == "conflicting" else 0,
        "required_document_kind": None,
        "evidence_updated_at": latest_evidence_updated_at,
        "resolution_detail": {
            "label": _rule_label(assertion) if assertion is not None else rule_key.replace("_", " ").title(),
            "selected_layer": getattr(assertion, "source_level", None) if assertion is not None else None,
            "selected_assertion_id": int(getattr(assertion, "id", 0) or 0) if assertion is not None else None,
            "merge_basis": _layer_summary_for_rule(layer_assertions),
            "status_reason": status_reason,
        },
    }


def rebuild_property_projection(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    property: Any | None = None,
) -> dict[str, Any]:
    scope = _build_property_scope(db, org_id=org_id, property_id=property_id, property=property)
    assertions = _query_inherited_assertions(
        db,
        org_id=org_id,
        state=scope.state,
        county=scope.county,
        city=scope.city,
        pha_name=scope.pha_name,
        statuses=None,
    )
    grouped_assertions = _group_assertions_by_rule(assertions)
    effective_assertions = _merge_effective_assertions(assertions)
    summary = build_policy_summary(
        db,
        assertions,
        org_id,
        scope.state,
        scope.county,
        scope.city,
        scope.pha_name,
    )

    sync_document_evidence_for_property(db, org_id=org_id, property_id=property_id)
    sync_inspection_evidence_for_property(db, org_id=org_id, property_id=property_id)

    evidence_rows = _evidence_rows(db, org_id=org_id, property_id=property_id)
    evidence_index = _build_evidence_index(evidence_rows)

    rule_keys = set(effective_assertions.keys()) | set(evidence_index.keys())

    current = _current_projection(db, org_id=org_id, property_id=property_id)
    if current is not None:
        current.is_current = False
        current.superseded_at = _utcnow()
        current.updated_at = _utcnow()
        db.flush()

    active_assertions = list(effective_assertions.values())
    rules_effective_at = _latest_datetime([getattr(a, "effective_date", None) for a in active_assertions])
    last_rule_change_at = _latest_datetime(
        [getattr(a, "activated_at", None) for a in active_assertions]
        + [getattr(a, "approved_at", None) for a in active_assertions]
        + [getattr(a, "updated_at", None) for a in active_assertions]
        + [getattr(a, "created_at", None) for a in active_assertions]
    )

    projection = PropertyComplianceProjection(
        org_id=org_id,
        property_id=property_id,
        jurisdiction_slug=scope.jurisdiction_slug,
        program_type=scope.pha_name,
        rules_version="v1",
        projection_status="computed",
        projection_basis_json=_dumps(
            {
                "state": scope.state,
                "county": scope.county,
                "city": scope.city,
                "pha_name": scope.pha_name,
                "address": scope.address,
                "property_type": scope.property_type,
                "verified_rule_count": len(summary.get("verified_rules") or []),
                "required_categories": summary.get("required_categories") or [],
                "category_coverage": summary.get("category_coverage") or {},
            }
        ),
        rules_effective_at=rules_effective_at,
        last_rule_change_at=last_rule_change_at,
        last_projected_at=_utcnow(),
        is_current=True,
    )
    db.add(projection)
    db.flush()

    item_rows: list[PropertyComplianceProjectionItem] = []
    impacted_rules: list[dict[str, Any]] = []
    unresolved_gaps: list[dict[str, Any]] = []

    blocking_count = 0
    unknown_count = 0
    stale_count = 0
    conflicting_count = 0
    evidence_gap_count = 0
    confirmed_count = 0
    inferred_count = 0
    failing_count = 0
    cost_total = 0.0
    days_total = 0
    confidence_values: list[float] = []

    for rule_key in sorted(rule_keys):
        assertion = effective_assertions.get(rule_key)
        layer_assertions = grouped_assertions.get(rule_key, [])
        evaluation = _evaluate_rule(
            rule_key=rule_key,
            assertion=assertion,
            layer_assertions=layer_assertions,
            evidence_rows=evidence_index.get(rule_key, []),
        )

        item = PropertyComplianceProjectionItem(
            org_id=org_id,
            projection_id=int(projection.id),
            property_id=property_id,
            policy_assertion_id=int(assertion.id) if assertion is not None else None,
            jurisdiction_slug=scope.jurisdiction_slug,
            program_type=scope.pha_name,
            property_type=scope.property_type,
            source_level=getattr(assertion, "source_level", None) if assertion is not None else "property",
            rule_key=rule_key,
            rule_category=evaluation["rule_category"],
            required=bool(evaluation["required"]),
            blocking=bool(evaluation["blocking"]),
            evaluation_status=evaluation["evaluation_status"],
            evidence_status=evaluation["evidence_status"],
            proof_state=evaluation["proof_state"],
            confidence=float(evaluation["confidence"]),
            estimated_cost=evaluation["estimated_cost"],
            estimated_days=evaluation["estimated_days"],
            evidence_summary=evaluation["evidence_summary"],
            evidence_gap=evaluation["evidence_gap"],
            status_reason=evaluation["status_reason"],
            source_citation=evaluation["source_citation"],
            raw_excerpt=evaluation["raw_excerpt"],
            rule_value_json=evaluation["rule_value_json"],
            resolution_detail_json=_dumps(evaluation["resolution_detail"]),
            conflicting_evidence_count=int(evaluation["conflicting_evidence_count"]),
            required_document_kind=evaluation["required_document_kind"],
            last_evaluated_at=_utcnow(),
            evidence_updated_at=evaluation["evidence_updated_at"],
        )
        db.add(item)
        db.flush()
        item_rows.append(item)
        confidence_values.append(float(item.confidence or 0.0))

        for evidence in evidence_index.get(rule_key, []):
            evidence.projection_item_id = int(item.id)
            evidence.policy_assertion_id = int(assertion.id) if assertion is not None else evidence.policy_assertion_id
            evidence.updated_at = _utcnow()

        if item.proof_state == "confirmed":
            confirmed_count += 1
        elif item.proof_state == "inferred":
            inferred_count += 1

        if item.evidence_gap:
            evidence_gap_count += 1

        if item.evaluation_status in {"fail", "blocked"}:
            failing_count += 1
            if item.blocking:
                blocking_count += 1
        elif item.evaluation_status == "unknown":
            unknown_count += 1
        elif item.evaluation_status == "stale":
            stale_count += 1
        elif item.evaluation_status == "conflicting":
            conflicting_count += 1
            if item.blocking:
                blocking_count += 1

        if item.evaluation_status in {"fail", "blocked", "unknown", "stale", "conflicting"}:
            impacted_rules.append(
                {
                    "rule_key": item.rule_key,
                    "evaluation_status": item.evaluation_status,
                    "evidence_status": item.evidence_status,
                    "blocking": bool(item.blocking),
                    "source_level": item.source_level,
                }
            )

        if item.evidence_gap:
            unresolved_gaps.append(
                {
                    "rule_key": item.rule_key,
                    "gap": item.evidence_gap,
                    "category": item.rule_category,
                }
            )

        if item.estimated_cost:
            cost_total += float(item.estimated_cost)
        if item.estimated_days:
            days_total += int(item.estimated_days)

    total_items = max(1, len(item_rows))
    passing_items = sum(1 for row in item_rows if row.evaluation_status == "pass")
    base_readiness = (passing_items / total_items) * 100.0

    readiness_penalty = (
        (blocking_count * 20.0)
        + (unknown_count * 6.0)
        + (stale_count * 8.0)
        + (conflicting_count * 12.0)
        + (evidence_gap_count * 4.0)
    )
    readiness_score = round(max(0.0, min(100.0, base_readiness - readiness_penalty)), 2)

    confidence_score = round(sum(confidence_values) / max(1, len(confidence_values)), 3)

    layer_confidence = {}
    for row in active_assertions:
        level = str(getattr(row, "source_level", "unknown") or "unknown").lower()
        layer_confidence.setdefault(level, [])
        layer_confidence[level].append(float(getattr(row, "confidence", 0.0) or 0.0))
    source_confidence = {
        level: round(sum(values) / max(1, len(values)), 3)
        for level, values in layer_confidence.items()
    }

    projection_reason = {
        "merge_strategy": "highest_precedence_effective_rule_per_rule_key",
        "source_level_precedence": SOURCE_LEVEL_PRECEDENCE,
        "coverage_status": summary.get("coverage", {}).get("coverage_status"),
        "production_readiness": summary.get("coverage", {}).get("production_readiness"),
        "completeness_status": summary.get("completeness_status"),
        "stale_status": summary.get("stale_status"),
        "rule_count": len(rule_keys),
        "effective_rule_count": len(effective_assertions),
    }

    projection.blocking_count = int(blocking_count)
    projection.unknown_count = int(unknown_count)
    projection.stale_count = int(stale_count)
    projection.conflicting_count = int(conflicting_count)
    projection.evidence_gap_count = int(evidence_gap_count)
    projection.confirmed_count = int(confirmed_count)
    projection.inferred_count = int(inferred_count)
    projection.failing_count = int(failing_count)
    projection.readiness_score = float(readiness_score)
    projection.projected_compliance_cost = float(round(cost_total, 2)) if cost_total else None
    projection.projected_days_to_rent = int(days_total) if days_total else None
    projection.confidence_score = float(confidence_score)
    projection.projection_status = _projection_status_from_counts(
        blocking=blocking_count,
        stale=stale_count,
        unknown=unknown_count,
        conflicting=conflicting_count,
    )
    projection.impacted_rules_json = _dumps(impacted_rules)
    projection.unresolved_evidence_gaps_json = _dumps(unresolved_gaps)
    projection.source_confidence_json = _dumps(source_confidence)
    projection.projection_reason_json = _dumps(projection_reason)
    projection.updated_at = _utcnow()
    db.flush()

    return build_property_projection_snapshot(db, org_id=org_id, property_id=property_id, projection=projection)


def build_property_projection_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    projection: PropertyComplianceProjection | None = None,
) -> dict[str, Any]:
    row = projection or _current_projection(db, org_id=org_id, property_id=property_id)
    if row is None:
        return {
            "ok": True,
            "property_id": int(property_id),
            "projection": None,
            "items": [],
            "counts": {"blocking": 0, "unknown": 0, "stale": 0, "conflicting": 0},
            "evidence_summary": {"count": 0, "linked_documents": 0, "inspection_links": 0},
            "blockers": [],
        }

    items = _current_projection_items(db, org_id=org_id, property_id=property_id, projection_id=int(row.id))
    evidence = _evidence_rows(db, org_id=org_id, property_id=property_id)

    blockers = [
        {
            "rule_key": item.rule_key,
            "evaluation_status": item.evaluation_status,
            "evidence_gap": item.evidence_gap,
            "source_level": item.source_level,
        }
        for item in items
        if item.evaluation_status in {"fail", "blocked", "conflicting", "stale"} and bool(item.blocking)
    ]

    return {
        "ok": True,
        "property_id": int(property_id),
        "projection": {
            "id": int(row.id),
            "jurisdiction_slug": row.jurisdiction_slug,
            "rules_version": row.rules_version,
            "projection_status": row.projection_status,
            "blocking_count": int(row.blocking_count or 0),
            "unknown_count": int(row.unknown_count or 0),
            "stale_count": int(row.stale_count or 0),
            "conflicting_count": int(row.conflicting_count or 0),
            "evidence_gap_count": int(row.evidence_gap_count or 0),
            "confirmed_count": int(row.confirmed_count or 0),
            "inferred_count": int(row.inferred_count or 0),
            "failing_count": int(row.failing_count or 0),
            "readiness_score": float(row.readiness_score or 0.0),
            "projected_compliance_cost": row.projected_compliance_cost,
            "projected_days_to_rent": row.projected_days_to_rent,
            "confidence_score": float(row.confidence_score or 0.0),
            "impacted_rules": _loads(row.impacted_rules_json, []),
            "unresolved_evidence_gaps": _loads(row.unresolved_evidence_gaps_json, []),
            "source_confidence": _loads(row.source_confidence_json, {}),
            "projection_reason": _loads(row.projection_reason_json, {}),
            "rules_effective_at": row.rules_effective_at,
            "last_rule_change_at": row.last_rule_change_at,
            "last_projected_at": row.last_projected_at,
        },
        "items": [
            {
                "id": int(item.id),
                "rule_key": item.rule_key,
                "rule_category": item.rule_category,
                "required": bool(item.required),
                "blocking": bool(item.blocking),
                "source_level": item.source_level,
                "evaluation_status": item.evaluation_status,
                "evidence_status": item.evidence_status,
                "proof_state": item.proof_state,
                "confidence": float(item.confidence or 0.0),
                "estimated_cost": item.estimated_cost,
                "estimated_days": item.estimated_days,
                "evidence_summary": item.evidence_summary,
                "evidence_gap": item.evidence_gap,
                "status_reason": item.status_reason,
                "source_citation": item.source_citation,
                "raw_excerpt": item.raw_excerpt,
                "rule_value": _loads(item.rule_value_json, {}),
                "resolution_detail": _loads(item.resolution_detail_json, {}),
                "conflicting_evidence_count": int(item.conflicting_evidence_count or 0),
                "required_document_kind": item.required_document_kind,
                "evidence_updated_at": item.evidence_updated_at,
                "last_evaluated_at": item.last_evaluated_at,
            }
            for item in items
        ],
        "counts": {
            "blocking": int(row.blocking_count or 0),
            "unknown": int(row.unknown_count or 0),
            "stale": int(row.stale_count or 0),
            "conflicting": int(row.conflicting_count or 0),
        },
        "evidence_summary": {
            "count": len(evidence),
            "linked_documents": sum(1 for e in evidence if getattr(e, "compliance_document_id", None) is not None),
            "inspection_links": sum(1 for e in evidence if getattr(e, "inspection_id", None) is not None),
            "confirmed": sum(1 for e in evidence if str(getattr(e, "proof_state", "") or "").lower() == "confirmed"),
            "failing": sum(1 for e in evidence if str(getattr(e, "evidence_status", "") or "").lower() in FAILING_EVIDENCE),
        },
        "blockers": blockers,
    }


def build_property_compliance_brief(
    db: Session,
    org_id: Optional[int],
    state: str | None = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    property_id: int | None = None,
    property: Any | None = None,
) -> dict[str, Any]:
    scope = _build_property_scope(
        db,
        org_id=org_id,
        property_id=property_id,
        property=property,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    assertions = _query_inherited_assertions(
        db,
        org_id=org_id,
        state=scope.state,
        county=scope.county,
        city=scope.city,
        pha_name=scope.pha_name,
    )
    summary = build_policy_summary(db, assertions, org_id, scope.state, scope.county, scope.city, scope.pha_name)

    projection_snapshot = None
    if scope.property_id is not None and org_id is not None:
        try:
            projection_snapshot = rebuild_property_projection(
                db,
                org_id=int(org_id),
                property_id=int(scope.property_id),
                property=property,
            )
        except Exception:
            projection_snapshot = build_property_projection_snapshot(
                db,
                org_id=int(org_id),
                property_id=int(scope.property_id),
            )

    blockers = list(summary.get("blocking_items") or [])
    if projection_snapshot and projection_snapshot.get("blockers"):
        blockers.extend(projection_snapshot["blockers"])

    deduped_blockers: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in blockers:
        key = (
            str(item.get("rule_key") or item.get("code") or ""),
            str(item.get("evaluation_status") or item.get("title") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped_blockers.append(item)

    return {
        "ok": True,
        "scope": {
            "state": scope.state,
            "county": scope.county,
            "city": scope.city,
            "pha_name": scope.pha_name,
            "property_type": scope.property_type,
            "jurisdiction_slug": scope.jurisdiction_slug,
            "property_id": scope.property_id,
        },
        "coverage": summary.get("coverage") or {},
        "required_categories": summary.get("required_categories") or [],
        "category_coverage": summary.get("category_coverage") or {},
        "verified_rules": summary.get("verified_rules") or [],
        "required_actions": summary.get("required_actions") or [],
        "blocking_items": deduped_blockers,
        "evidence_links": summary.get("evidence_links") or [],
        "projection": projection_snapshot.get("projection") if projection_snapshot else None,
        "projection_counts": projection_snapshot.get("counts") if projection_snapshot else None,
    }