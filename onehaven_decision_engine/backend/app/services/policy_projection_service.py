from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy import and_, or_, select, text
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
            -_specificity_score(r, county=cnty, city=cty, pha_name=pha),
            -float(getattr(r, "confidence", 0.0) or 0.0),
            int(getattr(r, "priority", 100) or 100),
            int(getattr(r, "source_rank", 100) or 100),
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


def _category_for_assertion(assertion: PolicyAssertion) -> str:
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
    return out


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
    prop_pha = _norm_text(pha_name)
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
    joined = " ".join([
        str(category or ""),
        str((metadata or {}).get("label") or ""),
        str(extracted_text or ""),
    ]).lower()
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


def _current_projection_items(db: Session, *, org_id: int, property_id: int, projection_id: int) -> list[PropertyComplianceProjectionItem]:
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


def _evaluate_rule(
    *,
    rule_key: str,
    assertion: PolicyAssertion | None,
    evidence_rows: list[PropertyComplianceEvidence],
) -> dict[str, Any]:
    required = bool(getattr(assertion, "required", True)) if assertion is not None else True
    blocking = bool(getattr(assertion, "blocking", False)) if assertion is not None else False
    confidence = float(getattr(assertion, "confidence", 0.0) or 0.0) if assertion is not None else 0.5
    rule_category = _category_for_assertion(assertion) if assertion is not None else RULE_KEY_TO_CATEGORY.get(rule_key, "inspection")
    evidence_summary_parts: list[str] = []
    evidence_gap: str | None = None
    estimated_cost: float | None = None
    estimated_days: int | None = None

    if not evidence_rows:
        evaluation_status = "unknown"
        evidence_status = "missing"
        evidence_gap = "No evidence linked to this rule yet."
    else:
        failing = [row for row in evidence_rows if str(getattr(row, "evidence_status", "") or "").lower() in FAILING_EVIDENCE]
        passing = [row for row in evidence_rows if str(getattr(row, "evidence_status", "") or "").lower() in PASSING_EVIDENCE]
        expired = [row for row in evidence_rows if str(getattr(row, "evidence_status", "") or "").lower() == "expired"]
        if failing and passing:
            evaluation_status = "conflicting"
            evidence_status = "conflicting"
            evidence_gap = "Both passing and failing evidence exist."
        elif failing:
            evaluation_status = "fail"
            evidence_status = str(getattr(failing[0], "evidence_status", "failed") or "failed").lower()
            evidence_gap = str(getattr(failing[0], "notes", None) or "Inspection or document evidence still shows an unresolved issue.")
            estimated_cost = 250.0 if blocking or required else 100.0
            estimated_days = 7 if blocking else 3
        elif expired:
            evaluation_status = "stale"
            evidence_status = "expired"
            evidence_gap = "Evidence exists but is expired or stale."
            estimated_days = 2
        elif passing:
            evaluation_status = "pass"
            evidence_status = "satisfied"
            evidence_gap = None
        else:
            evaluation_status = "unknown"
            evidence_status = "unknown"
            evidence_gap = "Evidence exists but could not be confidently evaluated."

        for row in evidence_rows:
            details = _loads(getattr(row, "source_details_json", None), {})
            label = getattr(row, "evidence_name", None) or details.get("code") or details.get("category") or rule_key
            evidence_summary_parts.append(str(label))

    if evaluation_status == "pass":
        blocking = False if blocking else False
    return {
        "rule_key": rule_key,
        "rule_category": rule_category,
        "required": required,
        "blocking": blocking,
        "evaluation_status": evaluation_status,
        "evidence_status": evidence_status,
        "confidence": max(0.1, confidence),
        "estimated_cost": estimated_cost,
        "estimated_days": estimated_days,
        "evidence_summary": ", ".join(sorted(set(evidence_summary_parts))) or None,
        "evidence_gap": evidence_gap,
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
    effective_assertions = _merge_effective_assertions(assertions)
    rule_keys = set(effective_assertions.keys()) | set(evidence_index.keys())

    current = _current_projection(db, org_id=org_id, property_id=property_id)
    if current is not None:
        current.is_current = False
        current.superseded_at = _utcnow()
        current.updated_at = _utcnow()
        db.flush()

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
                "verified_rule_count": len(summary.get("verified_rules") or []),
            }
        ),
        last_projected_at=_utcnow(),
        is_current=True,
    )
    db.add(projection)
    db.flush()

    item_rows: list[PropertyComplianceProjectionItem] = []
    impacted_rules: list[dict[str, Any]] = []
    unresolved_gaps: list[dict[str, Any]] = []
    blocking_count = unknown_count = stale_count = conflicting_count = 0
    cost_total = 0.0
    days_total = 0
    confidence_values: list[float] = []

    for rule_key in sorted(rule_keys):
        assertion = effective_assertions.get(rule_key)
        evaluation = _evaluate_rule(rule_key=rule_key, assertion=assertion, evidence_rows=evidence_index.get(rule_key, []))
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
            confidence=float(evaluation["confidence"]),
            estimated_cost=evaluation["estimated_cost"],
            estimated_days=evaluation["estimated_days"],
            evidence_summary=evaluation["evidence_summary"],
            evidence_gap=evaluation["evidence_gap"],
            resolution_detail_json=_dumps(
                {
                    "label": _rule_label(assertion) if assertion is not None else rule_key.replace("_", " ").title(),
                    "source_citation": getattr(assertion, "source_citation", None) if assertion is not None else None,
                    "raw_excerpt": getattr(assertion, "raw_excerpt", None) if assertion is not None else None,
                }
            ),
        )
        db.add(item)
        db.flush()
        item_rows.append(item)
        confidence_values.append(float(item.confidence or 0.0))

        for evidence in evidence_index.get(rule_key, []):
            evidence.projection_item_id = int(item.id)
            evidence.policy_assertion_id = int(assertion.id) if assertion is not None else evidence.policy_assertion_id
            evidence.updated_at = _utcnow()

        if item.evaluation_status in {"fail", "blocked"} and item.blocking:
            blocking_count += 1
        elif item.evaluation_status == "unknown":
            unknown_count += 1
        elif item.evaluation_status == "stale":
            stale_count += 1
        elif item.evaluation_status == "conflicting":
            conflicting_count += 1

        if item.evaluation_status in {"fail", "blocked", "unknown", "stale", "conflicting"}:
            impacted_rules.append(
                {
                    "rule_key": item.rule_key,
                    "evaluation_status": item.evaluation_status,
                    "evidence_status": item.evidence_status,
                    "blocking": bool(item.blocking),
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
    good_items = sum(1 for row in item_rows if row.evaluation_status == "pass")
    readiness_score = round((good_items / total_items) * 100.0, 2)
    if blocking_count:
        readiness_score = max(0.0, readiness_score - (blocking_count * 20.0))
    if unknown_count:
        readiness_score = max(0.0, readiness_score - (unknown_count * 5.0))
    if conflicting_count:
        readiness_score = max(0.0, readiness_score - (conflicting_count * 10.0))
    confidence_score = round(sum(confidence_values) / max(1, len(confidence_values)), 3)

    projection.blocking_count = int(blocking_count)
    projection.unknown_count = int(unknown_count)
    projection.stale_count = int(stale_count)
    projection.conflicting_count = int(conflicting_count)
    projection.readiness_score = float(readiness_score)
    projection.projected_compliance_cost = float(round(cost_total, 2)) if cost_total else None
    projection.projected_days_to_rent = int(days_total) if days_total else None
    projection.confidence_score = float(confidence_score)
    projection.impacted_rules_json = _dumps(impacted_rules)
    projection.unresolved_evidence_gaps_json = _dumps(unresolved_gaps)
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
            "readiness_score": float(row.readiness_score or 0.0),
            "projected_compliance_cost": row.projected_compliance_cost,
            "projected_days_to_rent": row.projected_days_to_rent,
            "confidence_score": float(row.confidence_score or 0.0),
            "impacted_rules": _loads(row.impacted_rules_json, []),
            "unresolved_evidence_gaps": _loads(row.unresolved_evidence_gaps_json, []),
            "last_projected_at": row.last_projected_at,
        },
        "items": [
            {
                "id": int(item.id),
                "rule_key": item.rule_key,
                "rule_category": item.rule_category,
                "required": bool(item.required),
                "blocking": bool(item.blocking),
                "evaluation_status": item.evaluation_status,
                "evidence_status": item.evidence_status,
                "confidence": float(item.confidence or 0.0),
                "estimated_cost": item.estimated_cost,
                "estimated_days": item.estimated_days,
                "evidence_summary": item.evidence_summary,
                "evidence_gap": item.evidence_gap,
                "resolution_detail": _loads(item.resolution_detail_json, {}),
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
        projection_snapshot = build_property_projection_snapshot(db, org_id=org_id, property_id=int(scope.property_id))

    blockers = list(summary.get("blocking_items") or [])
    if projection_snapshot and projection_snapshot.get("blockers"):
        blockers.extend(projection_snapshot["blockers"])

    return {
        "ok": True,
        "state": scope.state,
        "county": scope.county,
        "city": scope.city,
        "pha_name": scope.pha_name,
        "jurisdiction_slug": scope.jurisdiction_slug,
        "coverage": summary["coverage"],
        "verified_rules": summary["verified_rules"],
        "required_actions": summary["required_actions"],
        "blocking_items": summary["blocking_items"],
        "blockers": blockers,
        "evidence_links": summary["evidence_links"],
        "local_rule_statuses": summary["local_rule_statuses"],
        "verified_rule_count_local": summary["verified_rule_count_local"],
        "verified_rule_count_effective": summary["verified_rule_count_effective"],
        "required_categories": summary["required_categories"],
        "category_coverage": summary["category_coverage"],
        "projection": projection_snapshot,
    }


def project_verified_assertions_to_profile(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
    notes: str | None = None,
) -> JurisdictionProfile:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    profile = _profile_row(db, org_id=org_id, state=st, county=cnty, city=cty)
    if profile is None:
        profile = JurisdictionProfile(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=_norm_text(pha_name),
            friction_multiplier=1.0,
        )
        db.add(profile)
        db.flush()

    brief = build_property_compliance_brief(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha_name,
    )
    coverage = brief.get("coverage") or {}
    summary = build_policy_summary(
        db,
        _query_inherited_assertions(db, org_id, st, cnty, cty, pha_name),
        org_id,
        st,
        cnty,
        cty,
        pha_name,
    )
    policy_json = {
        "coverage": coverage,
        "required_categories": brief.get("required_categories") or [],
        "category_coverage": brief.get("category_coverage") or {},
        "local_rule_statuses": brief.get("local_rule_statuses") or {},
        "verified_rules": brief.get("verified_rules") or [],
        "completeness_status": coverage.get("completeness_status") or summary.get("completeness_status"),
        "stale_status": "stale" if coverage.get("is_stale") else "fresh",
    }
    profile.policy_json = _dumps(policy_json)
    profile.notes = notes or profile.notes
    profile.pha_name = _norm_text(pha_name)
    profile.completeness_status = str(policy_json.get("completeness_status") or coverage.get("completeness_status") or "missing")
    profile.completeness_score = float(coverage.get("completeness_score") or 0.0)
    profile.required_categories_json = _dumps(brief.get("required_categories") or [])
    profile.covered_categories_json = _dumps(
        [cat for cat, status in (brief.get("category_coverage") or {}).items() if status in {"verified", "conditional"}]
    )
    profile.missing_categories_json = _dumps(
        [cat for cat, status in (brief.get("category_coverage") or {}).items() if status == "missing"]
    )
    profile.is_stale = bool(coverage.get("is_stale", False))
    profile.stale_reason = coverage.get("stale_reason")
    profile.last_verified_at = _utcnow()
    profile.updated_at = _utcnow()
    db.flush()
    return profile
