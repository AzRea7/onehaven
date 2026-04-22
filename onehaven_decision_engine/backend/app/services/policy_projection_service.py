from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy import delete, or_, select, text
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_categories
from app.domain.jurisdiction_defaults import required_categories_for_city
from app.policy_models import (
    JurisdictionCoverageStatus,
    JurisdictionProfile,
    PolicyAssertion,
    PropertyComplianceEvidence,
    PropertyComplianceEvidenceFact,
    PropertyComplianceProjection,
    PropertyComplianceProjectionItem,
)

RULE_KEY_TO_CATEGORY = {
    "rental_registration_required": "registration",
    "inspection_required": "inspection",
    "inspection_program_exists": "inspection",
    "certificate_required_before_occupancy": "occupancy",
    "certificate_of_occupancy_required": "occupancy",
    "certificate_of_compliance_required": "occupancy",
    "local_registration_certificate_required": "registration",
    "lead_based_paint_paperwork_required": "safety",
    "lead_clearance_required": "safety",
    "smoke_detector_required": "safety",
    "smoke_detectors_required": "safety",
    "carbon_monoxide_detector_required": "safety",
    "utility_service_required_before_inspection": "utilities",
    "utility_confirmation_required": "utilities",
    "local_jurisdiction_document_required": "permits",
    "pass_inspection_required": "inspection",
    "fire_safety_inspection_required": "inspection",
    "reinspection_required": "inspection",
}

DOCUMENT_CATEGORY_RULE_MAP = {
    "inspection_report": ["pass_inspection_required", "inspection_required"],
    "pass_certificate": ["certificate_required_before_occupancy", "pass_inspection_required"],
    "reinspection_notice": ["inspection_required", "reinspection_required"],
    "repair_invoice": ["inspection_required"],
    "utility_confirmation": ["utility_confirmation_required"],
    "smoke_detector_proof": ["smoke_detector_required"],
    "lead_based_paint_paperwork": ["lead_based_paint_paperwork_required"],
    "local_jurisdiction_document": ["local_jurisdiction_document_required", "rental_registration_required"],
    "approval_letter": ["certificate_required_before_occupancy"],
    "denial_letter": ["inspection_required"],
    "photo_evidence": ["inspection_required"],
    "registration_certificate": ["rental_registration_required"],
    "certificate_of_occupancy": ["certificate_of_occupancy_required", "certificate_required_before_occupancy"],
    "certificate_of_compliance": ["certificate_of_compliance_required", "certificate_required_before_occupancy"],
    "other_evidence": [],
}

INSPECTION_CODE_RULE_HINTS = {
    "SMOKE": "smoke_detector_required",
    "GFCI": "inspection_required",
    "HANDRAIL": "inspection_required",
    "CO_DETECTOR": "carbon_monoxide_detector_required",
    "COOKING": "inspection_required",
    "EGRESS": "inspection_required",
    "LEAD": "lead_based_paint_paperwork_required",
    "CERTIFICATE": "certificate_required_before_occupancy",
    "FIRE": "fire_safety_inspection_required",
    "REINSPECTION": "reinspection_required",
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



PROPERTY_PROOF_RULE_MAP: dict[str, dict[str, Any]] = {
    "rental_registration_required": {"proof_key": "registration_certificate", "label": "Registration certificate", "document_categories": ["registration_certificate", "local_jurisdiction_document"], "required_status": "verified", "category": "registration"},
    "inspection_required": {"proof_key": "inspection_pass", "label": "Inspection pass evidence", "document_categories": ["inspection_report", "pass_certificate", "reinspection_notice"], "required_status": "verified", "category": "inspection"},
    "pass_inspection_required": {"proof_key": "inspection_pass", "label": "Inspection pass evidence", "document_categories": ["inspection_report", "pass_certificate"], "required_status": "verified", "category": "inspection"},
    "certificate_required_before_occupancy": {"proof_key": "certificate_before_occupancy", "label": "Certificate before occupancy", "document_categories": ["certificate_of_occupancy", "certificate_of_compliance", "pass_certificate", "approval_letter"], "required_status": "verified", "category": "occupancy"},
    "certificate_of_occupancy_required": {"proof_key": "certificate_before_occupancy", "label": "Certificate of occupancy", "document_categories": ["certificate_of_occupancy", "approval_letter"], "required_status": "verified", "category": "occupancy"},
    "certificate_of_compliance_required": {"proof_key": "certificate_before_occupancy", "label": "Certificate of compliance", "document_categories": ["certificate_of_compliance", "approval_letter"], "required_status": "verified", "category": "occupancy"},
    "lead_based_paint_paperwork_required": {"proof_key": "lead_docs", "label": "Lead documentation", "document_categories": ["lead_based_paint_paperwork", "lead_clearance_doc"], "required_status": "verified", "category": "safety"},
    "lead_clearance_required": {"proof_key": "lead_docs", "label": "Lead clearance", "document_categories": ["lead_clearance_doc", "lead_based_paint_paperwork"], "required_status": "verified", "category": "safety"},
    "hap_contract_and_tenancy_addendum_required": {"proof_key": "voucher_packet", "label": "Voucher packet", "document_categories": ["voucher_packet", "approval_letter"], "required_status": "verified", "category": "program_overlay"},
    "pha_landlord_packet_required": {"proof_key": "voucher_packet", "label": "Landlord voucher packet", "document_categories": ["voucher_packet", "approval_letter"], "required_status": "verified", "category": "program_overlay"},
    "local_contact_required": {"proof_key": "local_contact_proof", "label": "Local contact proof", "document_categories": ["local_contact_proof", "local_jurisdiction_document"], "required_status": "verified", "category": "contacts"},
}

def _property_proof_definition(rule_key: str, rule_category: str | None = None) -> dict[str, Any] | None:
    definition = PROPERTY_PROOF_RULE_MAP.get(str(rule_key or '').strip())
    if definition:
        return dict(definition)
    cat = str(rule_category or '').strip().lower()
    fallback = {
        'registration': {"proof_key": "registration_certificate", "label": "Registration certificate", "document_categories": ["registration_certificate", "local_jurisdiction_document"], "required_status": "verified", "category": "registration"},
        'inspection': {"proof_key": "inspection_pass", "label": "Inspection pass evidence", "document_categories": ["inspection_report", "pass_certificate", "reinspection_notice"], "required_status": "verified", "category": "inspection"},
        'occupancy': {"proof_key": "certificate_before_occupancy", "label": "Certificate before occupancy", "document_categories": ["certificate_of_occupancy", "certificate_of_compliance", "approval_letter"], "required_status": "verified", "category": "occupancy"},
        'lead': {"proof_key": "lead_docs", "label": "Lead documentation", "document_categories": ["lead_based_paint_paperwork", "lead_clearance_doc"], "required_status": "verified", "category": "lead"},
        'program_overlay': {"proof_key": "voucher_packet", "label": "Voucher packet", "document_categories": ["voucher_packet", "approval_letter"], "required_status": "verified", "category": "program_overlay"},
        'section8': {"proof_key": "voucher_packet", "label": "Voucher packet", "document_categories": ["voucher_packet", "approval_letter"], "required_status": "verified", "category": "section8"},
        'contacts': {"proof_key": "local_contact_proof", "label": "Local contact proof", "document_categories": ["local_contact_proof", "local_jurisdiction_document"], "required_status": "verified", "category": "contacts"},
    }
    return dict(fallback.get(cat) or {}) or None

def _determine_property_proof_state(*, item: dict[str, Any], evidence_rows: list[Any]) -> tuple[str, str | None]:
    proof_state = str(item.get('proof_state') or item.get('evidence_status') or '').strip().lower()
    if proof_state in {'verified','uploaded','expired','mismatched','missing'}:
        gap = item.get('evidence_gap')
        return proof_state, (str(gap).strip() if gap else None)
    eval_status = str(item.get('evaluation_status') or '').strip().lower()
    if eval_status in {'pass','verified','satisfied'}:
        return 'verified', None
    if eval_status in {'stale','expired'}:
        return 'expired', str(item.get('evidence_gap') or 'Proof is stale or expired.')
    if eval_status in {'conflicting','mismatch'}:
        return 'mismatched', str(item.get('evidence_gap') or 'Uploaded proof does not match current rule requirement.')
    if evidence_rows:
        return 'uploaded', str(item.get('evidence_gap') or '') or None
    return 'missing', str(item.get('evidence_gap') or 'Required proof has not been uploaded.')

def build_property_proof_obligations(db: Session, *, org_id: int | None, property_id: int | None = None, property: Any | None = None, state: str | None = None, county: str | None = None, city: str | None = None, pha_name: str | None = None) -> dict[str, Any]:
    scope = _build_property_scope(db, org_id=org_id, property_id=property_id, property=property, state=state, county=county, city=city, pha_name=pha_name)
    assertions = _query_inherited_assertions(db, org_id=org_id, state=scope.state, county=scope.county, city=scope.city, pha_name=scope.pha_name)
    merged = _merge_effective_assertions(assertions)
    evidence_rows = _evidence_rows(db, org_id=int(org_id), property_id=int(scope.property_id)) if org_id is not None and scope.property_id is not None else []
    by_category = {}
    for ev in evidence_rows:
        by_category.setdefault(str(getattr(ev,'evidence_type',None) or getattr(ev,'document_category',None) or getattr(ev,'category',None) or '').strip().lower(), []).append(ev)
    obligations=[]
    counts={"missing":0,"uploaded":0,"expired":0,"mismatched":0,"verified":0}
    blockers=[]
    for rule_key,row in merged.items():
        if not _is_effective_assertion(row):
            continue
        value_state = _rule_value_state(row)
        if value_state not in {'yes','conditional'}:
            continue
        proof_def=_property_proof_definition(rule_key, _category_for_assertion(row))
        if not proof_def:
            continue
        candidate_rows=[]
        for cat in proof_def.get('document_categories') or []:
            candidate_rows.extend(by_category.get(str(cat).strip().lower(), []))
        item={"evaluation_status": 'pass' if bool(candidate_rows) else 'fail', "proof_state": None, "evidence_status": None, "evidence_gap": None}
        proof_state, gap = _determine_property_proof_state(item=item, evidence_rows=candidate_rows)
        if candidate_rows and proof_state == 'uploaded':
            proof_state='uploaded'
        elif candidate_rows and proof_state == 'missing':
            proof_state='uploaded'
            gap=None
        if proof_state=='verified' and not candidate_rows:
            proof_state='missing'
        counts[proof_state]=counts.get(proof_state,0)+1
        obligation={
            'rule_key': rule_key,
            'rule_category': _category_for_assertion(row),
            'proof_key': proof_def.get('proof_key'),
            'proof_label': proof_def.get('label'),
            'document_categories': list(proof_def.get('document_categories') or []),
            'required_status': proof_def.get('required_status') or 'verified',
            'proof_status': proof_state,
            'blocking': bool(getattr(row,'blocking',False)) or _category_for_assertion(row) in {'registration','inspection','occupancy','lead','program_overlay','section8'},
            'evidence_gap': gap,
            'matched_document_count': len(candidate_rows),
            'matched_evidence_ids': [int(getattr(ev,'id')) for ev in candidate_rows if getattr(ev,'id',None) is not None],
            'source_assertion_id': int(getattr(row,'id',0) or 0),
            'source_level': getattr(row,'source_level',None),
            'confidence': float(getattr(row,'confidence',0.0) or 0.0),
        }
        obligations.append(obligation)
        if obligation['blocking'] and proof_state != 'verified':
            blockers.append(obligation)
    return {
        'property_id': int(scope.property_id) if scope.property_id is not None else None,
        'scope': {'state': scope.state, 'county': scope.county, 'city': scope.city, 'pha_name': scope.pha_name},
        'required_proofs': obligations,
        'counts': counts,
        'blocking_proofs': blockers,
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


def _rollback_quietly(db: Session) -> None:
    try:
        db.rollback()
    except Exception:
        pass


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def _norm_status(value: Any) -> str:
    return str(value or "").strip().lower()


def _parse_datetime(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    text_value = str(raw).strip()
    if not text_value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text_value[:19], fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(text_value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


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
                SELECT id, address, state, county, city, property_type
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

# 1) ADD THESE HELPERS near the other small helper functions, after _profile_row / before build_policy_summary.

def _profile_policy_json(profile: JurisdictionProfile | None) -> dict[str, Any]:
    if profile is None:
        return {}
    payload = _loads(getattr(profile, "policy_json", None), {})
    return payload if isinstance(payload, dict) else {}


def _profile_meta(profile: JurisdictionProfile | None) -> dict[str, Any]:
    policy = _profile_policy_json(profile)
    meta = policy.get("meta") or {}
    return meta if isinstance(meta, dict) else {}


def _coverage_metadata(coverage_row: JurisdictionCoverageStatus | None) -> dict[str, Any]:
    if coverage_row is None:
        return {}
    payload = _loads(getattr(coverage_row, "metadata_json", None), {})
    return payload if isinstance(payload, dict) else {}


def _jurisdiction_trust_for_scope(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    coverage_row = _coverage_row(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    profile = _profile_row(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
    )

    coverage_meta = _coverage_metadata(coverage_row)
    profile_meta = _profile_meta(profile)
    profile_completeness = profile_meta.get("completeness") or {}
    profile_critical = normalize_categories(
        profile_completeness.get("critical_categories")
        or profile_meta.get("critical_categories")
        or (_profile_policy_json(profile).get("critical_categories") if profile is not None else [])
        or []
    )

    source_summary = _loads(getattr(coverage_row, "source_summary_json", None), {})
    source_summary = source_summary if isinstance(source_summary, dict) else {}

    stale_categories = normalize_categories(
        profile_completeness.get("stale_categories")
        or (((coverage_meta.get("profile_scoring") or {}).get("stale_categories")) if isinstance(coverage_meta, dict) else [])
        or []
    )
    inferred_categories = normalize_categories(
        profile_completeness.get("inferred_categories")
        or (((coverage_meta.get("profile_scoring") or {}).get("inferred_categories")) if isinstance(coverage_meta, dict) else [])
        or []
    )
    conflicting_categories = normalize_categories(
        profile_completeness.get("conflicting_categories")
        or (((coverage_meta.get("profile_scoring") or {}).get("conflicting_categories")) if isinstance(coverage_meta, dict) else [])
        or []
    )

    required_categories = normalize_categories(
        profile_completeness.get("required_categories")
        or _loads(getattr(coverage_row, "required_categories_json", None), [])
        or []
    )
    covered_categories = normalize_categories(
        profile_completeness.get("covered_categories")
        or _loads(getattr(coverage_row, "covered_categories_json", None), [])
        or []
    )
    missing_categories = normalize_categories(
        profile_completeness.get("missing_categories")
        or _loads(getattr(coverage_row, "missing_categories_json", None), [])
        or []
    )

    if not required_categories and city:
        required_categories = normalize_categories(
            required_categories_for_city(city, state=state, include_section8=bool(pha_name))
        )

    critical_missing_categories = [cat for cat in missing_categories if cat in set(profile_critical)]
    critical_stale_categories = [cat for cat in stale_categories if cat in set(profile_critical)]
    critical_inferred_categories = [cat for cat in inferred_categories if cat in set(profile_critical)]
    critical_conflicting_categories = [cat for cat in conflicting_categories if cat in set(profile_critical)]

    return {
        "coverage_status": getattr(coverage_row, "coverage_status", None),
        "production_readiness": (
            profile_completeness.get("production_readiness")
            or ((coverage_meta.get("profile_rollup") or {}).get("production_readiness") if isinstance(coverage_meta, dict) else None)
            or getattr(coverage_row, "production_readiness", None)
        ),
        "coverage_confidence": (
            profile_completeness.get("confidence_label")
            or ((coverage_meta.get("profile_rollup") or {}).get("coverage_confidence") if isinstance(coverage_meta, dict) else None)
            or getattr(coverage_row, "confidence_label", None)
            or ("high" if _safe_float(getattr(coverage_row, "confidence_score", None), 0.0) >= 0.85 else "medium" if _safe_float(getattr(coverage_row, "confidence_score", None), 0.0) >= 0.60 else "low")
        ),
        "completeness_score": _safe_float(
            profile_completeness.get("completeness_score")
            or getattr(coverage_row, "completeness_score", None),
            0.0,
        ),
        "completeness_status": (
            profile_completeness.get("completeness_status")
            or getattr(coverage_row, "completeness_status", None)
        ),
        "required_categories": required_categories,
        "covered_categories": covered_categories,
        "missing_categories": missing_categories,
        "stale_categories": stale_categories,
        "inferred_categories": inferred_categories,
        "conflicting_categories": conflicting_categories,
        "critical_categories": profile_critical,
        "critical_missing_categories": critical_missing_categories,
        "critical_stale_categories": critical_stale_categories,
        "critical_inferred_categories": critical_inferred_categories,
        "critical_conflicting_categories": critical_conflicting_categories,
        "category_statuses": source_summary,
        "is_stale": bool(getattr(coverage_row, "is_stale", False)) if coverage_row is not None else False,
        "stale_reason": getattr(coverage_row, "stale_reason", None) if coverage_row is not None else None,
        "trustworthy_for_projection": bool(
            profile_completeness.get("trustworthy_for_projection")
            if profile_completeness.get("trustworthy_for_projection") is not None
            else ((coverage_meta.get("profile_rollup") or {}).get("trustworthy_for_projection") if isinstance(coverage_meta, dict) else False)
        ),
        "resolved_rule_version": profile_meta.get("resolved_rule_version") if isinstance(profile_meta, dict) else None,
        "discovery_status": profile_completeness.get("discovery_status"),
        "last_refresh": profile_completeness.get("last_refresh") or profile_completeness.get("last_refreshed"),
        "last_discovery_run": profile_completeness.get("last_discovery_run"),
    }

# 2) REPLACE the existing build_policy_summary() body with this updated version.

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

    jurisdiction_trust = _jurisdiction_trust_for_scope(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    coverage = {
        "coverage_status": jurisdiction_trust.get("coverage_status") or ("verified_extended" if verified_rules else "not_started"),
        "production_readiness": jurisdiction_trust.get("production_readiness") or ("ready" if verified_rules else "partial"),
        "confidence_label": jurisdiction_trust.get("coverage_confidence") or (
            "high"
            if len(verified_rules) >= 6
            else "medium"
            if len(verified_rules) >= 3
            else "low"
        ),
        "completeness_score": float(jurisdiction_trust.get("completeness_score") or 0.0),
        "completeness_status": jurisdiction_trust.get("completeness_status"),
        "is_stale": bool(jurisdiction_trust.get("is_stale", False)),
        "stale_reason": jurisdiction_trust.get("stale_reason"),
        "required_categories": jurisdiction_trust.get("required_categories") or required_categories,
        "covered_categories": jurisdiction_trust.get("covered_categories") or sorted(covered_categories),
        "missing_categories": jurisdiction_trust.get("missing_categories") or [cat for cat, status in category_coverage.items() if status == "missing"],
        "stale_categories": jurisdiction_trust.get("stale_categories") or [],
        "inferred_categories": jurisdiction_trust.get("inferred_categories") or [],
        "conflicting_categories": jurisdiction_trust.get("conflicting_categories") or [],
        "critical_categories": jurisdiction_trust.get("critical_categories") or [],
        "critical_missing_categories": jurisdiction_trust.get("critical_missing_categories") or [],
        "critical_stale_categories": jurisdiction_trust.get("critical_stale_categories") or [],
        "critical_inferred_categories": jurisdiction_trust.get("critical_inferred_categories") or [],
        "critical_conflicting_categories": jurisdiction_trust.get("critical_conflicting_categories") or [],
        "trustworthy_for_projection": bool(jurisdiction_trust.get("trustworthy_for_projection", False)),
        "resolved_rule_version": jurisdiction_trust.get("resolved_rule_version"),
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
        "jurisdiction_trust": jurisdiction_trust,
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
        out.extend(
            [
                "certificate_required_before_occupancy",
                "certificate_of_occupancy_required",
                "certificate_of_compliance_required",
            ]
        )
    if "utility" in joined:
        out.append("utility_confirmation_required")
    if "lead" in joined:
        out.extend(["lead_based_paint_paperwork_required", "lead_clearance_required"])
    if "smoke" in joined:
        out.append("smoke_detector_required")
    if "carbon monoxide" in joined or "co detector" in joined:
        out.append("carbon_monoxide_detector_required")
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


def _document_reference_number(row: Any, metadata: dict[str, Any], parser_meta: dict[str, Any]) -> str | None:
    for raw in [
        metadata.get("reference_number"),
        metadata.get("registration_number"),
        metadata.get("certificate_number"),
        parser_meta.get("reference_number"),
        parser_meta.get("registration_number"),
        parser_meta.get("certificate_number"),
    ]:
        if raw:
            return str(raw).strip()
    return None


def _document_expires_at(row: Any, metadata: dict[str, Any], parser_meta: dict[str, Any]) -> datetime | None:
    for raw in [
        metadata.get("expires_at"),
        metadata.get("expiration_date"),
        parser_meta.get("expires_at"),
        parser_meta.get("expiration_date"),
        row.get("expires_at") if hasattr(row, "get") else None,
    ]:
        parsed = _parse_datetime(raw)
        if parsed is not None:
            return parsed
    return None


def _required_document_kind_for_rule(rule_key: str) -> str | None:
    if rule_key in {
        "certificate_required_before_occupancy",
        "certificate_of_occupancy_required",
        "certificate_of_compliance_required",
    }:
        return "certificate"
    if rule_key == "rental_registration_required":
        return "registration_certificate"
    if rule_key in {"inspection_required", "pass_inspection_required", "reinspection_required"}:
        return "inspection_report"
    if rule_key in {"lead_based_paint_paperwork_required", "lead_clearance_required"}:
        return "lead_based_paint_paperwork"
    if rule_key in {"smoke_detector_required", "smoke_detectors_required"}:
        return "smoke_detector_proof"
    if rule_key == "utility_confirmation_required":
        return "utility_confirmation"
    return None


def _replace_evidence_facts(
    db: Session,
    *,
    evidence_id: int,
    org_id: int,
    property_id: int,
    facts: list[dict[str, Any]],
) -> None:
    db.execute(
        delete(PropertyComplianceEvidenceFact).where(
            PropertyComplianceEvidenceFact.evidence_id == int(evidence_id)
        )
    )
    for fact in facts:
        row = PropertyComplianceEvidenceFact(
            org_id=int(org_id),
            property_id=int(property_id),
            evidence_id=int(evidence_id),
            projection_item_id=fact.get("projection_item_id"),
            inspection_id=fact.get("inspection_id"),
            checklist_item_id=fact.get("checklist_item_id"),
            rule_key=fact.get("rule_key"),
            fact_key=str(fact.get("fact_key") or "fact"),
            fact_label=fact.get("fact_label"),
            fact_type=str(fact.get("fact_type") or "status"),
            fact_value=fact.get("fact_value"),
            fact_status=str(fact.get("fact_status") or "observed"),
            proof_state=str(fact.get("proof_state") or "inferred"),
            severity=fact.get("severity"),
            satisfies_rule=fact.get("satisfies_rule"),
            observed_at=fact.get("observed_at"),
            expires_at=fact.get("expires_at"),
            resolved_at=fact.get("resolved_at"),
            source_details_json=_dumps(fact.get("source_details") or {}),
            metadata_json=_dumps(fact.get("metadata") or {}),
        )
        db.add(row)
    db.flush()


def _invalidate_missing_evidence(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    evidence_source_type: str,
    keep_keys: set[str],
) -> None:
    rows = list(
        db.scalars(
            select(PropertyComplianceEvidence).where(
                PropertyComplianceEvidence.org_id == int(org_id),
                PropertyComplianceEvidence.property_id == int(property_id),
                PropertyComplianceEvidence.evidence_source_type == evidence_source_type,
                PropertyComplianceEvidence.is_current.is_(True),
                PropertyComplianceEvidence.invalidated_at.is_(None),
            )
        ).all()
    )
    now = _utcnow()
    for row in rows:
        evidence_key = str(getattr(row, "evidence_key", "") or "")
        if evidence_key in keep_keys:
            continue
        row.is_current = False
        row.invalidated_at = now
        row.invalidated_reason = f"{evidence_source_type}_source_missing"
        row.updated_at = now
        db.add(row)
    db.flush()


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
    jurisdiction_slug: str | None = None,
    program_type: str | None = None,
    rule_key: str | None = None,
    rule_category: str | None = None,
    document_kind: str | None = None,
    evidence_category: str | None = None,
    issuing_authority: str | None = None,
    reference_number: str | None = None,
    line_item_key: str | None = None,
    line_item_label: str | None = None,
    line_item_status: str | None = None,
    severity: str | None = None,
    remediation_status: str | None = None,
    remediation_due_at: datetime | None = None,
    observed_at: datetime | None = None,
    expires_at: datetime | None = None,
    confidence: float | None = None,
    source_details: dict[str, Any] | None = None,
    metadata_json: dict[str, Any] | None = None,
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
            created_at=_utcnow(),
            is_current=True,
        )
        db.add(row)

    row.projection_item_id = projection_item_id
    row.policy_assertion_id = policy_assertion_id
    row.compliance_document_id = compliance_document_id
    row.inspection_id = inspection_id
    row.checklist_item_id = checklist_item_id
    row.jurisdiction_slug = jurisdiction_slug
    row.program_type = program_type
    row.rule_key = rule_key
    row.rule_category = rule_category
    row.evidence_category = evidence_category
    row.document_kind = document_kind
    row.issuing_authority = issuing_authority
    row.reference_number = reference_number
    row.line_item_key = line_item_key
    row.line_item_label = line_item_label
    row.line_item_status = line_item_status
    row.severity = severity
    row.remediation_status = remediation_status
    row.remediation_due_at = remediation_due_at
    row.evidence_name = evidence_name
    row.evidence_status = evidence_status
    row.proof_state = proof_state
    row.satisfies_rule = satisfies_rule
    row.confidence = float(confidence if confidence is not None else getattr(row, "confidence", 0.0) or 0.0)
    row.observed_at = observed_at or row.observed_at
    row.expires_at = expires_at
    row.notes = notes
    row.source_details_json = _dumps(source_details or {})
    row.metadata_json = _dumps(metadata_json or {})
    row.invalidated_at = None
    row.invalidated_reason = None
    row.is_current = True
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
    try:
        scope = _build_property_scope(db, org_id=org_id, property_id=property_id)

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
        keep_keys: set[str] = set()

        for row in rows:
            metadata = _loads(row.get("metadata_json"), {})
            parser_meta = _loads(row.get("parser_meta_json"), {})
            category = str(row.get("category") or "other_evidence")
            rule_keys = _document_rule_keys(
                category,
                metadata={**metadata, "label": row.get("label")},
                extracted_text=row.get("extracted_text_preview"),
            )
            scan_status = str(row.get("scan_status") or "unknown").lower()
            parse_status = str(row.get("parse_status") or "unknown").lower()

            if scan_status in {"infected", "blocked"}:
                status = "blocked"
                satisfies_rule = False
            elif scan_status in {"clean", "ok", "unknown"}:
                status = "verified"
                satisfies_rule = True
            else:
                status = "unknown"
                satisfies_rule = None

            proof_state = "confirmed" if parse_status in {"parsed", "queued", "skipped"} else "inferred"

            if not rule_keys:
                rule_keys = [f"document_category::{category}"]

            document_kind = category
            issuing_authority = str(
                metadata.get("issuing_authority")
                or parser_meta.get("issuing_authority")
                or metadata.get("authority_name")
                or ""
            ).strip() or None
            reference_number = _document_reference_number(row, metadata, parser_meta)
            expires_at = _document_expires_at(row, metadata, parser_meta)
            remediation_due_at = _parse_datetime(metadata.get("remediation_due_at") or parser_meta.get("remediation_due_at"))
            observed_at = _parse_datetime(row.get("created_at")) or _utcnow()

            for rule_key in rule_keys:
                evidence_key = f"document:{int(row['id'])}:{rule_key}"
                keep_keys.add(evidence_key)

                evidence_row = _upsert_evidence(
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
                    jurisdiction_slug=scope.jurisdiction_slug,
                    program_type=scope.pha_name,
                    rule_key=rule_key,
                    rule_category=RULE_KEY_TO_CATEGORY.get(rule_key, "other"),
                    document_kind=document_kind,
                    evidence_category=category,
                    issuing_authority=issuing_authority,
                    reference_number=reference_number,
                    observed_at=observed_at,
                    expires_at=expires_at,
                    remediation_due_at=remediation_due_at,
                    confidence=0.9 if proof_state == "confirmed" else 0.65,
                    notes=row.get("extracted_text_preview"),
                    source_details={
                        "rule_key": rule_key,
                        "category": category,
                        "metadata": metadata,
                        "parser_meta": parser_meta,
                        "parse_status": parse_status,
                        "scan_status": scan_status,
                    },
                    metadata_json={
                        "document_id": int(row["id"]),
                        "parse_status": parse_status,
                        "scan_status": scan_status,
                    },
                )

                _replace_evidence_facts(
                    db,
                    evidence_id=int(evidence_row.id),
                    org_id=org_id,
                    property_id=property_id,
                    facts=[
                        {
                            "inspection_id": int(row["inspection_id"]) if row.get("inspection_id") is not None else None,
                            "checklist_item_id": int(row["checklist_item_id"]) if row.get("checklist_item_id") is not None else None,
                            "rule_key": rule_key,
                            "fact_key": f"document:{int(row['id'])}",
                            "fact_label": row.get("label") or row.get("original_filename") or rule_key,
                            "fact_type": "document",
                            "fact_value": row.get("extracted_text_preview") or category,
                            "fact_status": status,
                            "proof_state": proof_state,
                            "satisfies_rule": satisfies_rule,
                            "observed_at": observed_at,
                            "expires_at": expires_at,
                            "source_details": {
                                "document_id": int(row["id"]),
                                "category": category,
                                "reference_number": reference_number,
                            },
                            "metadata": {
                                "issuing_authority": issuing_authority,
                                "document_kind": document_kind,
                            },
                        }
                    ],
                )

                created_or_updated += 1
                linked_rule_keys.add(rule_key)

        if document_id is None:
            _invalidate_missing_evidence(
                db,
                org_id=org_id,
                property_id=property_id,
                evidence_source_type="document",
                keep_keys=keep_keys,
            )

        db.commit()
        return {
            "ok": True,
            "property_id": int(property_id),
            "document_count": len(rows),
            "linked_rule_keys": sorted(linked_rule_keys),
            "evidence_rows": created_or_updated,
        }
    except Exception as e:
        _rollback_quietly(db)
        return {
            "ok": False,
            "property_id": int(property_id),
            "document_count": 0,
            "linked_rule_keys": [],
            "evidence_rows": 0,
            "error": str(e),
        }


def sync_inspection_evidence_for_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    inspection_id: int | None = None,
) -> dict[str, Any]:
    scope = _build_property_scope(db, org_id=org_id, property_id=property_id)

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
    keep_keys: set[str] = set()

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

        if item_status in {"fail", "blocked", "inconclusive"}:
            evidence_status = "failed"
            satisfies_rule = False
        elif item_status in {"pass", "passed", "verified"}:
            evidence_status = "verified"
            satisfies_rule = True
        else:
            evidence_status = "unknown"
            satisfies_rule = None

        severity_raw = row.get("severity")
        severity = str(severity_raw).lower() if severity_raw is not None else None
        remediation_status = "required" if evidence_status == "failed" else "not_required"
        observed_at = _parse_datetime(row.get("inspection_date")) or _utcnow()

        for rule_key in rule_keys:
            evidence_key = f"inspection_item:{int(row['inspection_item_id'])}:{rule_key}"
            keep_keys.add(evidence_key)

            evidence_row = _upsert_evidence(
                db,
                org_id=org_id,
                property_id=property_id,
                evidence_source_type="inspection_item",
                evidence_key=evidence_key,
                evidence_name=f"Inspection {row.get('code') or row.get('inspection_item_id')}",
                evidence_status=evidence_status,
                proof_state="confirmed",
                satisfies_rule=satisfies_rule,
                inspection_id=int(row["inspection_id"]),
                jurisdiction_slug=scope.jurisdiction_slug,
                program_type=scope.pha_name,
                rule_key=rule_key,
                rule_category=RULE_KEY_TO_CATEGORY.get(rule_key, "inspection"),
                evidence_category=str(row.get("category") or "inspection_item"),
                line_item_key=str(row.get("code") or row.get("inspection_item_id")),
                line_item_label=str(row.get("category") or row.get("code") or ""),
                line_item_status=item_status,
                severity=severity,
                remediation_status=remediation_status,
                observed_at=observed_at,
                confidence=0.95,
                notes=row.get("fail_reason") or row.get("details"),
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
                metadata_json={
                    "inspection_id": int(row["inspection_id"]),
                    "inspection_item_id": int(row["inspection_item_id"]),
                    "passed": bool(row.get("passed")) if row.get("passed") is not None else None,
                },
            )

            _replace_evidence_facts(
                db,
                evidence_id=int(evidence_row.id),
                org_id=org_id,
                property_id=property_id,
                facts=[
                    {
                        "inspection_id": int(row["inspection_id"]),
                        "rule_key": rule_key,
                        "fact_key": f"inspection_item:{int(row['inspection_item_id'])}",
                        "fact_label": str(row.get("code") or row.get("category") or "Inspection item"),
                        "fact_type": "inspection_item",
                        "fact_value": row.get("details") or row.get("fail_reason") or row.get("category"),
                        "fact_status": item_status,
                        "proof_state": "confirmed",
                        "severity": severity,
                        "satisfies_rule": satisfies_rule,
                        "observed_at": observed_at,
                        "source_details": {
                            "inspection_item_id": int(row["inspection_item_id"]),
                            "inspection_id": int(row["inspection_id"]),
                            "code": row.get("code"),
                        },
                        "metadata": {
                            "requires_reinspection": bool(row.get("requires_reinspection")),
                        },
                    }
                ],
            )

            created_or_updated += 1
            linked_rule_keys.add(rule_key)

    if inspection_id is None:
        _invalidate_missing_evidence(
            db,
            org_id=org_id,
            property_id=property_id,
            evidence_source_type="inspection_item",
            keep_keys=keep_keys,
        )

    db.commit()
    return {
        "ok": True,
        "property_id": int(property_id),
        "inspection_id": int(inspection_id) if inspection_id is not None else None,
        "linked_rule_keys": sorted(linked_rule_keys),
        "evidence_rows": created_or_updated,
    }

def sync_checklist_evidence_for_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    scope = _build_property_scope(db, org_id=org_id, property_id=property_id)

    checklist_rows = db.execute(
        text(
            """
            SELECT
                id,
                item_code,
                description,
                category,
                status,
                severity,
                common_fail,
                created_at,
                updated_at
            FROM property_checklist_items
            WHERE org_id = :org_id
              AND property_id = :property_id
            ORDER BY id ASC
            """
        ),
        {"org_id": int(org_id), "property_id": int(property_id)},
    ).mappings().all()

    created_or_updated = 0
    linked_rule_keys: set[str] = set()
    keep_keys: set[str] = set()

    for row in checklist_rows:
        raw_status = _norm_status(row.get("status"))
        if raw_status in {"done", "pass", "passed", "complete", "completed", "verified"}:
            evidence_status = "verified"
            proof_state = "confirmed"
            line_item_status = "pass"
            satisfies_rule = True
        elif raw_status in {"failed", "fail", "blocked", "open"}:
            evidence_status = "failed"
            proof_state = "confirmed"
            line_item_status = "fail" if raw_status != "blocked" else "blocked"
            satisfies_rule = False
        else:
            evidence_status = "unknown"
            proof_state = "inferred"
            line_item_status = "unknown"
            satisfies_rule = None

        rule_keys = _inspection_rule_keys(
            str(row.get("item_code") or ""),
            category=row.get("category"),
            fail_reason=row.get("description"),
        )
        observed_at = _parse_datetime(row.get("updated_at")) or _parse_datetime(row.get("created_at")) or _utcnow()
        severity = str(row.get("severity")).lower() if row.get("severity") is not None else None

        for rule_key in rule_keys:
            evidence_key = f"checklist_item:{int(row['id'])}:{rule_key}"
            keep_keys.add(evidence_key)

            evidence_row = _upsert_evidence(
                db,
                org_id=org_id,
                property_id=property_id,
                evidence_source_type="checklist_item",
                evidence_key=evidence_key,
                evidence_name=str(row.get("description") or row.get("item_code") or rule_key),
                evidence_status=evidence_status,
                proof_state=proof_state,
                satisfies_rule=satisfies_rule,
                checklist_item_id=int(row["id"]),
                jurisdiction_slug=scope.jurisdiction_slug,
                program_type=scope.pha_name,
                rule_key=rule_key,
                rule_category=RULE_KEY_TO_CATEGORY.get(rule_key, "inspection"),
                evidence_category=str(row.get("category") or "checklist_item"),
                line_item_key=str(row.get("item_code") or row.get("id")),
                line_item_label=str(row.get("description") or row.get("item_code") or ""),
                line_item_status=line_item_status,
                severity=severity,
                remediation_status="required" if evidence_status == "failed" else "not_required",
                observed_at=observed_at,
                confidence=0.85 if proof_state == "confirmed" else 0.65,
                notes=str(row.get("description") or ""),
                source_details={
                    "checklist_item_id": int(row["id"]),
                    "item_code": row.get("item_code"),
                    "category": row.get("category"),
                    "common_fail": bool(row.get("common_fail")),
                },
                metadata_json={
                    "status": raw_status,
                },
            )

            _replace_evidence_facts(
                db,
                evidence_id=int(evidence_row.id),
                org_id=org_id,
                property_id=property_id,
                facts=[
                    {
                        "checklist_item_id": int(row["id"]),
                        "rule_key": rule_key,
                        "fact_key": f"checklist_item:{int(row['id'])}",
                        "fact_label": str(row.get("description") or row.get("item_code") or "Checklist item"),
                        "fact_type": "checklist_item",
                        "fact_value": raw_status,
                        "fact_status": line_item_status,
                        "proof_state": proof_state,
                        "severity": severity,
                        "satisfies_rule": satisfies_rule,
                        "observed_at": observed_at,
                        "source_details": {
                            "item_code": row.get("item_code"),
                            "category": row.get("category"),
                        },
                        "metadata": {
                            "common_fail": bool(row.get("common_fail")),
                        },
                    }
                ],
            )

            created_or_updated += 1
            linked_rule_keys.add(rule_key)

    _invalidate_missing_evidence(
        db,
        org_id=org_id,
        property_id=property_id,
        evidence_source_type="checklist_item",
        keep_keys=keep_keys,
    )

    db.commit()
    return {
        "ok": True,
        "property_id": int(property_id),
        "linked_rule_keys": sorted(linked_rule_keys),
        "evidence_rows": created_or_updated,
    }
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

    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    assertions = _query_inherited_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )

    summary = build_policy_summary(
        db,
        assertions,
        org_id,
        st,
        cnty,
        cty,
        pha,
    )

    # ---- find existing row ----
    existing_row = None

    stmt = select(JurisdictionProfile).where(JurisdictionProfile.state == st)
    if org_id is None:
        stmt = stmt.where(JurisdictionProfile.org_id.is_(None))
    else:
        stmt = stmt.where(
            or_(
                JurisdictionProfile.org_id == org_id,
                JurisdictionProfile.org_id.is_(None),
            )
        )

    rows = list(db.scalars(stmt).all())

    for row in rows:
        if _norm_lower(getattr(row, "county", None)) not in {None, cnty}:
            continue
        if _norm_lower(getattr(row, "city", None)) not in {None, cty}:
            continue
        existing_row = row
        break

    # ---- build policy payload safely ----
    existing_policy = {}
    if existing_row is not None:
        existing_policy = _loads(getattr(existing_row, "policy_json", None), {})
        if not isinstance(existing_policy, dict):
            existing_policy = {}

    policy = dict(existing_policy)
    policy["coverage"] = dict(summary.get("coverage") or {})
    policy["verified_rules"] = list(summary.get("verified_rules") or [])
    policy["required_actions"] = list(summary.get("required_actions") or [])

    # ---- CREATE OR UPDATE ----
    if existing_row is None:
        profile_kwargs = {
            "org_id": org_id,
            "state": st,
            "county": cnty,
            "city": cty,
            "notes": notes,
        }

        # ONLY add fields if model supports them
        if hasattr(JurisdictionProfile, "policy_json"):
            profile_kwargs["policy_json"] = _dumps(policy)

        if hasattr(JurisdictionProfile, "pha_name"):
            profile_kwargs["pha_name"] = pha

        if hasattr(JurisdictionProfile, "friction_multiplier"):
            profile_kwargs["friction_multiplier"] = 1.0

        row_to_write = JurisdictionProfile(**profile_kwargs)

    else:
        row_to_write = existing_row

        row_to_write.org_id = org_id
        row_to_write.state = st
        row_to_write.county = cnty
        row_to_write.city = cty
        row_to_write.notes = notes

        if hasattr(row_to_write, "policy_json"):
            row_to_write.policy_json = _dumps(policy)

        if hasattr(row_to_write, "pha_name"):
            row_to_write.pha_name = pha

        if hasattr(row_to_write, "friction_multiplier"):
            row_to_write.friction_multiplier = 1.0

    # ---- timestamps ----
    if hasattr(row_to_write, "updated_at"):
        row_to_write.updated_at = _utcnow()

    db.add(row_to_write)
    db.flush()

    return row_to_write


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
                PropertyComplianceEvidence.is_current.is_(True),
            )
        ).all()
    )


def _evidence_facts_for_property(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    evidence_ids: list[int],
) -> list[PropertyComplianceEvidenceFact]:
    if not evidence_ids:
        return []
    return list(
        db.scalars(
            select(PropertyComplianceEvidenceFact).where(
                PropertyComplianceEvidenceFact.org_id == int(org_id),
                PropertyComplianceEvidenceFact.property_id == int(property_id),
                PropertyComplianceEvidenceFact.evidence_id.in_(evidence_ids),
            )
        ).all()
    )


def _build_evidence_index(rows: list[PropertyComplianceEvidence]) -> dict[str, list[PropertyComplianceEvidence]]:
    out: dict[str, list[PropertyComplianceEvidence]] = {}
    for row in rows:
        rule_key = str(getattr(row, "rule_key", None) or "").strip()
        if not rule_key:
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
    required_document_kind: str | None = _required_document_kind_for_rule(rule_key)

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
        stale_rows = [
            row
            for row in evidence_rows
            if getattr(row, "expires_at", None) is not None and getattr(row, "expires_at") < _utcnow()
        ]
        proof_state = _compute_proof_state(evidence_rows)

        if failing and passing:
            evaluation_status = "conflicting"
            evidence_status = "conflicting"
            evidence_gap = "Both passing and failing evidence exist."
            status_reason = "At least one evidence record satisfies the rule while another shows a failure or unresolved issue."
        elif stale_rows or (expired and not passing and not failing):
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
            label = (
                getattr(row, "evidence_name", None)
                or getattr(row, "line_item_label", None)
                or details.get("code")
                or details.get("category")
                or rule_key
            )
            evidence_summary_parts.append(str(label))
            if not required_document_kind and getattr(row, "document_kind", None):
                required_document_kind = str(getattr(row, "document_kind"))

    if evaluation_status == "pass":
        blocking = False

    latest_evidence_updated_at = _latest_datetime(
        [getattr(r, "updated_at", None) for r in evidence_rows]
        + [getattr(r, "observed_at", None) for r in evidence_rows]
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
        "required_document_kind": required_document_kind,
        "evidence_updated_at": latest_evidence_updated_at,
        "resolution_detail": {
            "label": _rule_label(assertion) if assertion is not None else rule_key.replace("_", " ").title(),
            "selected_layer": getattr(assertion, "source_level", None) if assertion is not None else None,
            "selected_assertion_id": int(getattr(assertion, "id", 0) or 0) if assertion is not None else None,
            "merge_basis": _layer_summary_for_rule(layer_assertions),
            "status_reason": status_reason,
        },
    }


def _link_evidence_to_projection_items(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    item_rows: list[PropertyComplianceProjectionItem],
    evidence_rows: list[PropertyComplianceEvidence],
    effective_assertions: dict[str, PolicyAssertion],
) -> None:
    item_by_rule = {str(item.rule_key or "").strip(): item for item in item_rows if getattr(item, "rule_key", None)}
    for evidence in evidence_rows:
        rule_key = str(getattr(evidence, "rule_key", None) or "").strip()
        if not rule_key:
            details = _loads(getattr(evidence, "source_details_json", None), {})
            rule_key = str(details.get("rule_key") or "").strip()
        if not rule_key:
            continue

        item = item_by_rule.get(rule_key)
        if item is None:
            continue

        item_id = getattr(item, "id", None)
        if item_id is None:
            continue

        evidence.projection_item_id = int(item_id)
        evidence.policy_assertion_id = (
            int(effective_assertions[rule_key].id)
            if rule_key in effective_assertions and getattr(effective_assertions[rule_key], "id", None) is not None
            else evidence.policy_assertion_id
        )
        evidence.updated_at = _utcnow()
        db.add(evidence)

    try:
        db.flush()
    except Exception:
        _rollback_quietly(db)
        return

    linked_evidence_ids = [
        int(e.id)
        for e in evidence_rows
        if getattr(e, "id", None) is not None and getattr(e, "projection_item_id", None) is not None
    ]
    if not linked_evidence_ids:
        return

    fact_rows = _evidence_facts_for_property(db, org_id=org_id, property_id=property_id, evidence_ids=linked_evidence_ids)
    evidence_to_projection = {
        int(e.id): int(e.projection_item_id)
        for e in evidence_rows
        if getattr(e, "id", None) is not None and getattr(e, "projection_item_id", None) is not None
    }
    for fact in fact_rows:
        projection_item_id = evidence_to_projection.get(int(fact.evidence_id))
        if projection_item_id is None:
            continue
        fact.projection_item_id = projection_item_id
        db.add(fact)
    db.flush()


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
    sync_checklist_evidence_for_property(db, org_id=org_id, property_id=property_id)

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
        created_at=_utcnow(),
        updated_at=_utcnow(),
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
            )

            optional_item_fields = {
                "jurisdiction_slug": scope.jurisdiction_slug,
                "program_type": scope.pha_name,
                "property_type": scope.property_type,
                "source_level": getattr(assertion, "source_level", None) if assertion is not None else "property",
                "proof_state": evaluation.get("proof_state"),
                "status_reason": evaluation.get("status_reason"),
                "source_citation": evaluation.get("source_citation"),
                "raw_excerpt": evaluation.get("raw_excerpt"),
                "rule_value_json": evaluation.get("rule_value_json"),
                "resolution_detail_json": _dumps(evaluation.get("resolution_detail") or {}),
                "conflicting_evidence_count": int(evaluation.get("conflicting_evidence_count") or 0),
                "required_document_kind": evaluation.get("required_document_kind"),
                "required_evidence_type": "document_or_inspection" if evaluation.get("required") else None,
                "required_evidence_key": rule_key,
                "required_evidence_group": evaluation.get("rule_category"),
                "proof_requirement_level": "strict" if evaluation.get("blocking") else "standard",
                "proof_validity_days": 365 if evaluation.get("required_document_kind") else None,
                "last_evaluated_at": _utcnow(),
                "evidence_updated_at": evaluation.get("evidence_updated_at"),
                "created_at": _utcnow(),
            }
            for field_name, field_value in optional_item_fields.items():
                if hasattr(item, field_name):
                    setattr(item, field_name, field_value)

            try:
                db.add(item)
                db.flush()
                persisted_item = item
            except Exception as exc:
                _rollback_quietly(db)
                item_persist_error = str(exc)
                persisted_item = item
                if hasattr(projection, "projection_reason_json"):
                    existing_reason = _loads(getattr(projection, "projection_reason_json", None), {})
                    if not isinstance(existing_reason, dict):
                        existing_reason = {}
                    existing_reason["projection_item_persist_error"] = item_persist_error
                    try:
                        projection.projection_reason_json = _dumps(existing_reason)
                    except Exception:
                        pass

            item_rows.append(persisted_item)
            confidence_values.append(float(getattr(persisted_item, "confidence", 0.0) or 0.0))

            if getattr(persisted_item, "proof_state", None) == "confirmed":
                confirmed_count += 1
            elif getattr(persisted_item, "proof_state", None) == "inferred":
                inferred_count += 1

            if getattr(persisted_item, "evidence_gap", None):
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

    _link_evidence_to_projection_items(
        db,
        org_id=org_id,
        property_id=property_id,
        item_rows=item_rows,
        evidence_rows=evidence_rows,
        effective_assertions=effective_assertions,
    )

    total_items = max(1, len(item_rows))
    passing_items = sum(1 for row in item_rows if row.evaluation_status == "pass")
    base_readiness = (passing_items / total_items) * 100.0

    jurisdiction_trust = summary.get("jurisdiction_trust") or {}
    critical_missing_categories = list(jurisdiction_trust.get("critical_missing_categories") or [])
    critical_stale_categories = list(jurisdiction_trust.get("critical_stale_categories") or [])
    critical_inferred_categories = list(jurisdiction_trust.get("critical_inferred_categories") or [])
    critical_conflicting_categories = list(jurisdiction_trust.get("critical_conflicting_categories") or [])

    jurisdiction_readiness_penalty = (
        (len(critical_missing_categories) * 18.0)
        + (len(critical_stale_categories) * 10.0)
        + (len(critical_inferred_categories) * 8.0)
        + (len(critical_conflicting_categories) * 16.0)
    )

    readiness_penalty = (
        (blocking_count * 20.0)
        + (unknown_count * 6.0)
        + (stale_count * 8.0)
        + (conflicting_count * 12.0)
        + (evidence_gap_count * 4.0)
        + jurisdiction_readiness_penalty
    )
    readiness_score = round(max(0.0, min(100.0, base_readiness - readiness_penalty)), 2)

    base_confidence_score = round(sum(confidence_values) / max(1, len(confidence_values)), 3)

    jurisdiction_confidence_penalty = (
        (0.20 if str(jurisdiction_trust.get("coverage_confidence") or "").strip().lower() == "low" else 0.10 if str(jurisdiction_trust.get("coverage_confidence") or "").strip().lower() == "medium" else 0.0)
        + (0.18 if critical_missing_categories else 0.0)
        + (0.10 if critical_stale_categories else 0.0)
        + (0.08 if critical_inferred_categories else 0.0)
        + (0.15 if critical_conflicting_categories else 0.0)
        + (0.08 if not bool(jurisdiction_trust.get("trustworthy_for_projection", False)) else 0.0)
    )
    confidence_score = round(max(0.0, min(1.0, base_confidence_score - jurisdiction_confidence_penalty)), 3)

    layer_confidence = {}
    for row in active_assertions:
        level = str(getattr(row, "source_level", "unknown") or "unknown").lower()
        layer_confidence.setdefault(level, [])
        layer_confidence[level].append(float(getattr(row, "confidence", 0.0) or 0.0))
    source_confidence = {
        level: round(sum(values) / max(1, len(values)), 3)
        for level, values in layer_confidence.items()
    }

    if critical_missing_categories:
        impacted_rules.append(
            {
                "rule_key": "jurisdiction_critical_missing",
                "evaluation_status": "blocked",
                "evidence_status": "missing",
                "blocking": True,
                "source_level": "jurisdiction",
                "categories": critical_missing_categories,
            }
        )
        for category in critical_missing_categories:
            unresolved_gaps.append(
                {
                    "rule_key": f"jurisdiction::{category}",
                    "gap": "Critical local jurisdiction coverage missing",
                    "category": category,
                }
            )
        blocking_count += len(critical_missing_categories)

    if critical_stale_categories:
        impacted_rules.append(
            {
                "rule_key": "jurisdiction_critical_stale",
                "evaluation_status": "stale",
                "evidence_status": "stale",
                "blocking": False,
                "source_level": "jurisdiction",
                "categories": critical_stale_categories,
            }
        )

    if critical_inferred_categories:
        impacted_rules.append(
            {
                "rule_key": "jurisdiction_critical_inferred",
                "evaluation_status": "warning",
                "evidence_status": "inferred",
                "blocking": False,
                "source_level": "jurisdiction",
                "categories": critical_inferred_categories,
            }
        )

    if critical_conflicting_categories:
        impacted_rules.append(
            {
                "rule_key": "jurisdiction_critical_conflicting",
                "evaluation_status": "conflicting",
                "evidence_status": "conflicting",
                "blocking": True,
                "source_level": "jurisdiction",
                "categories": critical_conflicting_categories,
            }
        )
        blocking_count += len(critical_conflicting_categories)
        conflicting_count += len(critical_conflicting_categories)

    projection_reason = {
        "merge_strategy": "highest_precedence_effective_rule_per_rule_key",
        "source_level_precedence": SOURCE_LEVEL_PRECEDENCE,
        "coverage_status": summary.get("coverage", {}).get("coverage_status"),
        "production_readiness": summary.get("coverage", {}).get("production_readiness"),
        "completeness_status": summary.get("completeness_status"),
        "stale_status": summary.get("stale_status"),
        "rule_count": len(rule_keys),
        "effective_rule_count": len(effective_assertions),
        "jurisdiction_trust": jurisdiction_trust,
        "jurisdiction_penalties": {
            "readiness_penalty": jurisdiction_readiness_penalty,
            "confidence_penalty": jurisdiction_confidence_penalty,
        },
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
    db.commit()

    return build_property_projection_snapshot(db, org_id=org_id, property_id=property_id, projection=projection)





    def _property_product_safety_payload(*, projection_reason: dict[str, Any] | None, proof_obligations: list[dict[str, Any]] | None) -> dict[str, Any]:
        reason = projection_reason if isinstance(projection_reason, dict) else {}
        trust = reason.get("jurisdiction_trust") if isinstance(reason.get("jurisdiction_trust"), dict) else {}
        proof_rows = list(proof_obligations or [])
        lockout_active = bool(trust.get("lockout_active") or trust.get("jurisdiction_lockout_active"))
        safe_for_projection = bool(trust.get("safe_for_projection", False))
        safe_for_user_reliance = bool(trust.get("safe_for_user_reliance", False))
        proof_blockers = [
            row for row in proof_rows
            if bool(row.get("blocking")) and str(row.get("proof_status") or "").strip().lower() in {"missing", "expired", "mismatched"}
        ]
        info_gaps = [
            row for row in proof_rows
            if str(row.get("proof_status") or "").strip().lower() in {"missing", "expired", "mismatched"} and not bool(row.get("blocking"))
        ]
        unsafe_reasons: list[str] = []
        unsafe_reasons.extend([str(x) for x in (trust.get("blocker_reasons") or []) if str(x).strip()])
        unsafe_reasons.extend([str(x) for x in (trust.get("manual_review_reasons") or []) if str(x).strip()])
        for row in proof_blockers:
            unsafe_reasons.append(str(row.get("evidence_gap") or f"{row.get('proof_label') or row.get('rule_key') or 'Required proof'} is {row.get('proof_status') or 'missing'}."))
        informational_reasons = [str(row.get("evidence_gap") or f"{row.get('proof_label') or row.get('rule_key') or 'Required proof'} is {row.get('proof_status') or 'missing'}." ) for row in info_gaps]
        legally_unsafe = bool(lockout_active or not safe_for_projection or proof_blockers)
        informationally_incomplete = bool((not legally_unsafe) and (informational_reasons or not safe_for_user_reliance))
        safe_to_rely_on = bool((not legally_unsafe) and safe_for_user_reliance and not proof_blockers)
        return {
            "lockout_active": lockout_active,
            "safe_for_projection": safe_for_projection,
            "safe_for_user_reliance": safe_for_user_reliance,
            "safe_to_rely_on": safe_to_rely_on,
            "legally_unsafe": legally_unsafe,
            "informationally_incomplete": informationally_incomplete,
            "unsafe_reasons": unsafe_reasons,
            "informational_reasons": informational_reasons,
        }
def build_property_projection_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    projection: PropertyComplianceProjection | None = None,
) -> dict[str, Any]:
    try:
        row = projection or _current_projection(db, org_id=org_id, property_id=property_id)
        if row is None:
            return {
                "ok": True,
                "property_id": int(property_id),
                "projection": None,
                "items": [],
                "evidence": [],
                "facts": [],
                "counts": {"blocking": 0, "unknown": 0, "stale": 0, "conflicting": 0},
                "evidence_summary": {"count": 0, "linked_documents": 0, "inspection_links": 0},
                "blockers": [],
                "jurisdiction": {},
                "proof_obligations": [],
                "proof_counts": {},
                "safe_to_rely_on": False,
                "legally_unsafe": False,
                "informationally_incomplete": True,
                "unsafe_reasons": ["Compliance projection has not been built yet."],
                "informational_reasons": [],
            }

        items = _current_projection_items(db, org_id=org_id, property_id=property_id, projection_id=int(row.id))
        evidence = _evidence_rows(db, org_id=org_id, property_id=property_id)
        evidence_ids = [int(e.id) for e in evidence]
        facts = _evidence_facts_for_property(db, org_id=org_id, property_id=property_id, evidence_ids=evidence_ids)

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

        proof_payload = build_property_proof_obligations(db, org_id=org_id, property_id=property_id)
        projection_reason = _loads(row.projection_reason_json, {})
        product_safety = _property_product_safety_payload(
            projection_reason=projection_reason,
            proof_obligations=proof_payload.get("required_proofs", []),
        )

        linked_documents = sum(1 for e in evidence if getattr(e, "compliance_document_id", None) is not None)
        inspection_links = sum(1 for e in evidence if getattr(e, "inspection_id", None) is not None)

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
                "projection_reason": projection_reason,
                "jurisdiction": (projection_reason or {}).get("jurisdiction_trust", {}),
                "jurisdiction_penalties": (projection_reason or {}).get("jurisdiction_penalties", {}),
                "proof_obligations": proof_payload.get("required_proofs", []),
                "proof_counts": proof_payload.get("counts", {}),
                "safe_to_rely_on": product_safety.get("safe_to_rely_on"),
                "legally_unsafe": product_safety.get("legally_unsafe"),
                "informationally_incomplete": product_safety.get("informationally_incomplete"),
                "unsafe_reasons": product_safety.get("unsafe_reasons") or [],
                "informational_reasons": product_safety.get("informational_reasons") or [],
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
                    "required_document_kind": getattr(item, "required_document_kind", None),
                    "required_evidence_type": getattr(item, "required_evidence_type", None),
                    "required_evidence_key": getattr(item, "required_evidence_key", None),
                    "required_evidence_group": getattr(item, "required_evidence_group", None),
                    "proof_requirement_level": getattr(item, "proof_requirement_level", None),
                    "proof_validity_days": getattr(item, "proof_validity_days", None),
                    "resolution_detail": _loads(getattr(item, "resolution_detail_json", None), {}),
                    "source_citation": item.source_citation,
                    "raw_excerpt": item.raw_excerpt,
                    "evidence_updated_at": getattr(item, "evidence_updated_at", None),
                    "last_evaluated_at": getattr(item, "last_evaluated_at", None),
                }
                for item in items
            ],
            "evidence": [
                {
                    "id": int(ev.id),
                    "evidence_source_type": ev.evidence_source_type,
                    "evidence_key": ev.evidence_key,
                    "evidence_name": ev.evidence_name,
                    "evidence_status": ev.evidence_status,
                    "proof_state": ev.proof_state,
                    "satisfies_rule": ev.satisfies_rule,
                    "rule_key": ev.rule_key,
                    "rule_category": ev.rule_category,
                    "document_kind": ev.document_kind,
                    "evidence_category": ev.evidence_category,
                    "inspection_id": ev.inspection_id,
                    "checklist_item_id": ev.checklist_item_id,
                    "compliance_document_id": ev.compliance_document_id,
                    "projection_item_id": ev.projection_item_id,
                    "confidence": float(ev.confidence or 0.0),
                    "observed_at": ev.observed_at,
                    "expires_at": ev.expires_at,
                    "updated_at": ev.updated_at,
                    "notes": ev.notes,
                    "metadata": _loads(ev.metadata_json, {}),
                    "source_details": _loads(ev.source_details_json, {}),
                }
                for ev in evidence
            ],
            "facts": [
                {
                    "id": int(f.id),
                    "evidence_id": int(f.evidence_id),
                    "rule_key": f.rule_key,
                    "fact_key": f.fact_key,
                    "fact_label": f.fact_label,
                    "fact_type": f.fact_type,
                    "fact_value": f.fact_value,
                    "fact_status": f.fact_status,
                    "proof_state": f.proof_state,
                    "severity": f.severity,
                    "satisfies_rule": f.satisfies_rule,
                    "observed_at": f.observed_at,
                    "expires_at": f.expires_at,
                    "resolved_at": f.resolved_at,
                    "metadata": _loads(f.metadata_json, {}),
                    "source_details": _loads(f.source_details_json, {}),
                }
                for f in facts
            ],
            "counts": {
                "blocking": int(row.blocking_count or 0),
                "unknown": int(row.unknown_count or 0),
                "stale": int(row.stale_count or 0),
                "conflicting": int(row.conflicting_count or 0),
            },
            "evidence_summary": {
                "count": len(evidence),
                "linked_documents": linked_documents,
                "inspection_links": inspection_links,
            },
            "blockers": blockers,
            "jurisdiction": (projection_reason or {}).get("jurisdiction_trust", {}),
            "proof_obligations": proof_payload.get("required_proofs", []),
            "proof_counts": proof_payload.get("counts", {}),
            "safe_to_rely_on": bool(product_safety.get("safe_to_rely_on")),
            "legally_unsafe": bool(product_safety.get("legally_unsafe")),
            "informationally_incomplete": bool(product_safety.get("informationally_incomplete")),
            "unsafe_reasons": product_safety.get("unsafe_reasons") or [],
            "informational_reasons": product_safety.get("informational_reasons") or [],
        }
    except Exception as e:
        _rollback_quietly(db)
        return {
            "ok": False,
            "property_id": int(property_id),
            "projection": None,
            "items": [],
            "evidence": [],
            "facts": [],
            "counts": {"blocking": 0, "unknown": 0, "stale": 0, "conflicting": 0},
            "evidence_summary": {"count": 0, "linked_documents": 0, "inspection_links": 0},
            "blockers": [],
            "jurisdiction": {},
            "proof_obligations": [],
            "proof_counts": {},
            "safe_to_rely_on": False,
            "legally_unsafe": False,
            "informationally_incomplete": True,
            "unsafe_reasons": ["Compliance projection could not be loaded."],
            "informational_reasons": [],
            "error": str(e),
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
            _rollback_quietly(db)
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
        "state": state,
        "county": county,
        "city": city,
        "pha_name": pha_name,
        "property_id": int(property_id) if property_id is not None else None,
        "coverage": dict(summary.get("coverage") or {}),
        "verified_rules": list(summary.get("verified_rules") or []),
        "required_actions": list(summary.get("required_actions") or []),
        "blocking_items": list(summary.get("blocking_items") or []),
        "evidence_links": list(summary.get("evidence_links") or []),
        "local_rule_statuses": dict(summary.get("local_rule_statuses") or {}),
        "verified_rule_count_local": int(summary.get("verified_rule_count_local") or 0),
        "verified_rule_count_effective": int(summary.get("verified_rule_count_effective") or 0),
        "required_categories": list(summary.get("required_categories") or []),
        "category_coverage": dict(summary.get("category_coverage") or {}),
        "completeness_status": summary.get("completeness_status"),
        "completeness_score": float(summary.get("completeness_score") or 0.0),
        "stale_status": summary.get("stale_status"),
        "jurisdiction_trust": dict(summary.get("jurisdiction_trust") or {}),
    }



# --- tier-two evidence-first final overrides ---


def _tier2_projection_reliance_boundary(*, safe_to_rely_on: bool, legally_unsafe: bool, informationally_incomplete: bool) -> dict[str, Any]:
    if safe_to_rely_on and not legally_unsafe:
        return {
            "status": "operationally_reliable",
            "message": "Property compliance can rely on the current jurisdiction evidence for operational decisions.",
        }
    if legally_unsafe:
        return {
            "status": "not_safe_to_rely_on",
            "message": "Critical jurisdiction or proof blockers still prevent safe reliance.",
        }
    if informationally_incomplete:
        return {
            "status": "degraded_review_required",
            "message": "No hard blocker is present, but missing or stale evidence still requires review.",
        }
    return {
        "status": "review_required",
        "message": "Compliance evidence should be reviewed before relying on the projection.",
    }


_tier2_original_build_property_projection_snapshot = build_property_projection_snapshot
_tier2_original_build_property_compliance_brief = build_property_compliance_brief


def build_property_projection_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    projection: PropertyComplianceProjection | None = None,
) -> dict[str, Any]:
    payload = dict(
        _tier2_original_build_property_projection_snapshot(
            db,
            org_id=org_id,
            property_id=property_id,
            projection=projection,
        )
    )
    if not payload.get("ok"):
        return payload

    projection_payload = dict(payload.get("projection") or {})
    jurisdiction_trust = dict(payload.get("jurisdiction") or projection_payload.get("projection_reason", {}).get("jurisdiction_trust", {}) or {})
    proof_obligations = list(payload.get("proof_obligations") or [])
    proof_blockers = [
        row for row in proof_obligations
        if bool(row.get("blocking")) and str(row.get("proof_status") or "").strip().lower() in {"missing", "expired", "mismatched"}
    ]
    info_gaps = [
        row for row in proof_obligations
        if (not bool(row.get("blocking"))) and str(row.get("proof_status") or "").strip().lower() in {"missing", "expired", "mismatched"}
    ]

    critical_missing_categories = list(jurisdiction_trust.get("critical_missing_categories") or jurisdiction_trust.get("missing_critical_categories") or [])
    critical_stale_categories = list(jurisdiction_trust.get("critical_stale_categories") or [])
    critical_inferred_categories = list(jurisdiction_trust.get("critical_inferred_categories") or [])
    critical_conflicting_categories = list(jurisdiction_trust.get("critical_conflicting_categories") or [])
    blocker_reasons = [str(x) for x in (jurisdiction_trust.get("blocker_reasons") or []) if str(x).strip()]
    manual_review_reasons = [str(x) for x in (jurisdiction_trust.get("manual_review_reasons") or []) if str(x).strip()]

    unsafe_reasons = list(payload.get("unsafe_reasons") or [])
    informational_reasons = list(payload.get("informational_reasons") or [])
    for row in proof_blockers:
        unsafe_reasons.append(str(row.get("evidence_gap") or f"{row.get('proof_label') or row.get('rule_key') or 'Required proof'} is {row.get('proof_status') or 'missing'}."))

    for row in info_gaps:
        informational_reasons.append(str(row.get("evidence_gap") or f"{row.get('proof_label') or row.get('rule_key') or 'Required proof'} is {row.get('proof_status') or 'missing'}."))

    unsafe_reasons.extend(blocker_reasons)
    informational_reasons.extend(manual_review_reasons)

    def _dedupe(values: list[str]) -> list[str]:
        seen = set()
        out = []
        for item in values:
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
        return out

    legally_unsafe = bool(
        payload.get("legally_unsafe")
        or proof_blockers
        or critical_missing_categories
        or critical_conflicting_categories
        or bool(jurisdiction_trust.get("lockout_active"))
        or (not bool(jurisdiction_trust.get("safe_for_projection", True)))
    )
    informationally_incomplete = bool(
        payload.get("informationally_incomplete")
        or ((not legally_unsafe) and (info_gaps or critical_stale_categories or critical_inferred_categories or manual_review_reasons))
    )
    safe_to_rely_on = bool(
        (not legally_unsafe)
        and bool(jurisdiction_trust.get("safe_for_user_reliance", payload.get("safe_to_rely_on", False)))
        and not proof_blockers
    )

    boundary = _tier2_projection_reliance_boundary(
        safe_to_rely_on=safe_to_rely_on,
        legally_unsafe=legally_unsafe,
        informationally_incomplete=informationally_incomplete,
    )

    product_truth = {
        "mode": "evidence_first",
        "crawler_role": "discovery_and_refresh_only",
        "freshness_role": "support_only",
        "jurisdiction_truth_source": "market_health_and_projection",
        "property_truth_source": "projection_plus_uploaded_evidence",
    }

    payload["jurisdiction_trust"] = jurisdiction_trust
    payload["truth_model"] = product_truth
    payload["reliance_boundary"] = boundary
    payload["critical_missing_categories"] = critical_missing_categories
    payload["critical_stale_categories"] = critical_stale_categories
    payload["critical_inferred_categories"] = critical_inferred_categories
    payload["critical_conflicting_categories"] = critical_conflicting_categories
    payload["safe_to_rely_on"] = safe_to_rely_on
    payload["legally_unsafe"] = legally_unsafe
    payload["informationally_incomplete"] = informationally_incomplete
    payload["unsafe_reasons"] = _dedupe(unsafe_reasons)
    payload["informational_reasons"] = _dedupe(informational_reasons)

    if isinstance(payload.get("projection"), dict):
        payload["projection"]["reliance_boundary"] = boundary
        payload["projection"]["truth_model"] = product_truth
        payload["projection"]["safe_to_rely_on"] = safe_to_rely_on
        payload["projection"]["legally_unsafe"] = legally_unsafe
        payload["projection"]["informationally_incomplete"] = informationally_incomplete

    return payload


def build_property_compliance_brief(
    db: Session,
    *,
    org_id: int,
    property_id: int,
) -> dict[str, Any]:
    brief = dict(
        _tier2_original_build_property_compliance_brief(
            db,
            org_id=org_id,
            property_id=property_id,
        )
    )
    snapshot = build_property_projection_snapshot(db, org_id=org_id, property_id=property_id)
    if snapshot.get("ok"):
        brief["safe_to_rely_on"] = bool(snapshot.get("safe_to_rely_on"))
        brief["legally_unsafe"] = bool(snapshot.get("legally_unsafe"))
        brief["informationally_incomplete"] = bool(snapshot.get("informationally_incomplete"))
        brief["unsafe_reasons"] = list(snapshot.get("unsafe_reasons") or [])
        brief["informational_reasons"] = list(snapshot.get("informational_reasons") or [])
        brief["reliance_boundary"] = dict(snapshot.get("reliance_boundary") or {})
        brief["truth_model"] = dict(snapshot.get("truth_model") or {})
        brief["jurisdiction_trust"] = dict(snapshot.get("jurisdiction_trust") or {})
    return brief


# --- coverage completion overrides ---

RULE_KEY_TO_CATEGORY.update({
    "lead_disclosure_required": "lead",
    "lead_hazard_assessment_required": "lead",
    "source_of_income_protection": "source_of_income",
    "permit_required": "permits",
    "local_documents_required": "documents",
    "local_contact_required": "contacts",
    "rental_license_required": "rental_license",
    "fee_schedule_reference": "fees",
    "program_overlay_requirement": "program_overlay",
})

PROPERTY_PROOF_RULE_MAP.update({
    "permit_required": {"proof_key": "permit_proof", "label": "Permit proof", "document_categories": ["permit", "local_jurisdiction_document", "approval_letter"], "required_status": "verified", "category": "permits"},
    "local_documents_required": {"proof_key": "local_documents", "label": "Local compliance documents", "document_categories": ["local_jurisdiction_document", "approval_letter", "other_evidence"], "required_status": "verified", "category": "documents"},
    "local_contact_required": {"proof_key": "local_contact_proof", "label": "Local contact proof", "document_categories": ["local_contact_proof", "local_jurisdiction_document"], "required_status": "verified", "category": "contacts"},
    "rental_license_required": {"proof_key": "rental_license", "label": "Rental license or certificate", "document_categories": ["registration_certificate", "certificate_of_occupancy", "certificate_of_compliance", "local_jurisdiction_document"], "required_status": "verified", "category": "rental_license"},
    "fee_schedule_reference": {"proof_key": "fee_payment_proof", "label": "Fee payment proof", "document_categories": ["approval_letter", "other_evidence", "local_jurisdiction_document"], "required_status": "verified", "category": "fees"},
    "program_overlay_requirement": {"proof_key": "program_overlay_docs", "label": "Program overlay documents", "document_categories": ["voucher_packet", "approval_letter", "local_jurisdiction_document"], "required_status": "verified", "category": "program_overlay"},
    "source_of_income_protection": {"proof_key": "soi_policy_ack", "label": "Source of income policy compliance", "document_categories": ["other_evidence", "approval_letter"], "required_status": "verified", "category": "source_of_income"},
})

_tier3_original_build_property_projection_snapshot = build_property_projection_snapshot
_tier3_original_build_property_compliance_brief = build_property_compliance_brief


def _normalized_coverage_summary(db: Session, *, org_id: int | None, state: str | None, county: str | None, city: str | None, pha_name: str | None) -> dict[str, Any]:
    if org_id is None:
        stmt = select(PolicyAssertion).where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = select(PolicyAssertion).where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if state:
        stmt = stmt.where(PolicyAssertion.state == _norm_state(state))
    rows = list(db.scalars(stmt).all())
    covered: dict[str, int] = {}
    for row in rows:
        if getattr(row, 'superseded_by_assertion_id', None) is not None:
            continue
        if county and _norm_lower(getattr(row, 'county', None)) not in {None, _norm_lower(county)}:
            continue
        if city and _norm_lower(getattr(row, 'city', None)) not in {None, _norm_lower(city)}:
            continue
        if pha_name and _norm_text(getattr(row, 'pha_name', None)) not in {None, _norm_text(pha_name)}:
            continue
        cat = _category_for_assertion(row)
        if cat:
            covered[cat] = covered.get(cat, 0) + 1
    return {"covered_categories": sorted(covered), "category_counts": covered}


def build_property_projection_snapshot(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    property: Any | None = None,
    projection: PropertyComplianceProjection | None = None,
    item_rows: list[PropertyComplianceProjectionItem] | None = None,
) -> dict[str, Any]:
    snapshot = dict(_tier3_original_build_property_projection_snapshot(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows))
    scope = snapshot.get('scope') or {}
    coverage = _normalized_coverage_summary(db, org_id=org_id, state=scope.get('state'), county=scope.get('county'), city=scope.get('city'), pha_name=scope.get('pha_name'))
    snapshot['normalized_category_coverage'] = coverage
    snapshot['covered_categories'] = list(coverage.get('covered_categories') or [])
    return snapshot


def build_property_compliance_brief(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    property: Any | None = None,
    projection: PropertyComplianceProjection | None = None,
    item_rows: list[PropertyComplianceProjectionItem] | None = None,
) -> dict[str, Any]:
    brief = dict(_tier3_original_build_property_compliance_brief(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows))
    snapshot = dict(brief.get('snapshot') or {})
    coverage = dict(snapshot.get('normalized_category_coverage') or {})
    brief['covered_categories'] = list(coverage.get('covered_categories') or [])
    brief['coverage_category_counts'] = dict(coverage.get('category_counts') or {})
    return brief


# --- final gap completion overrides ---
RULE_KEY_TO_CATEGORY.update({
    'lead_hazard_assessment_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
})

PROPERTY_PROOF_RULE_MAP.update({
    'lead_hazard_assessment_required': {'proof_key': 'lead_docs', 'label': 'Lead documentation', 'document_categories': ['lead_based_paint_paperwork', 'lead_clearance_doc'], 'required_status': 'verified', 'category': 'lead'},
    'permit_required': {'proof_key': 'permit_docs', 'label': 'Permit documentation', 'document_categories': ['permit_document', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'permits'},
    'local_contact_required': {'proof_key': 'local_contact_proof', 'label': 'Local contact proof', 'document_categories': ['local_contact_proof', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'contacts'},
    'local_documents_required': {'proof_key': 'required_documents', 'label': 'Required documents', 'document_categories': ['local_jurisdiction_document', 'approval_letter'], 'required_status': 'verified', 'category': 'documents'},
    'fee_schedule_reference': {'proof_key': 'fee_schedule', 'label': 'Fee schedule', 'document_categories': ['fee_schedule', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'fees'},
    'program_overlay_requirement': {'proof_key': 'voucher_packet', 'label': 'Program overlay proof', 'document_categories': ['voucher_packet', 'approval_letter'], 'required_status': 'verified', 'category': 'program_overlay'},
    'source_of_income_protection': {'proof_key': 'source_of_income_policy', 'label': 'Source of income policy', 'document_categories': ['local_jurisdiction_document', 'approval_letter'], 'required_status': 'verified', 'category': 'source_of_income'},
    'rental_license_required': {'proof_key': 'rental_license', 'label': 'Rental license', 'document_categories': ['registration_certificate', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'rental_license'},
})

try:
    _tier_final_original_build_property_compliance_brief2 = build_property_compliance_brief
except NameError:
    _tier_final_original_build_property_compliance_brief2 = None

if _tier_final_original_build_property_compliance_brief2 is not None:
    def build_property_compliance_brief(db: Session, *, org_id: int, property_id: int, property: Any | None = None, projection: PropertyComplianceProjection | None = None, item_rows: list[PropertyComplianceProjectionItem] | None = None) -> dict[str, Any]:
        brief = dict(_tier_final_original_build_property_compliance_brief2(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows))
        snapshot = dict(brief.get('snapshot') or {})
        covered = set(str(x).strip().lower() for x in list(brief.get('covered_categories') or snapshot.get('covered_categories') or []) if str(x).strip())
        rows = list(item_rows or [])
        for row in rows:
            rk = str(getattr(row, 'rule_key', '') or '').strip()
            if rk in RULE_KEY_TO_CATEGORY:
                covered.add(str(RULE_KEY_TO_CATEGORY[rk]).strip().lower())
        snapshot['covered_categories'] = sorted(covered)
        brief['snapshot'] = snapshot
        brief['covered_categories'] = sorted(covered)
        return brief


# --- final resolution overrides ---
_FINAL_RULE_TO_CATEGORY = {
    'lead_hazard_assessment_required': 'lead',
    'lead_paint_affidavit_required': 'lead',
    'lead_clearance_required': 'lead',
    'lead_inspection_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
    'rental_registration_required': 'registration',
    'inspection_required': 'inspection',
    'inspection_program_exists': 'inspection',
    'fire_safety_inspection_required': 'inspection',
    'certificate_required_before_occupancy': 'occupancy',
    'certificate_of_occupancy_required': 'occupancy',
    'certificate_of_compliance_required': 'occupancy',
    'hap_contract_and_tenancy_addendum_required': 'program_overlay',
    'pha_landlord_packet_required': 'program_overlay',
}

try:
    _final_resolution_original_build_property_projection_snapshot = build_property_projection_snapshot
except NameError:
    _final_resolution_original_build_property_projection_snapshot = None

try:
    _final_resolution_original_build_property_compliance_brief = build_property_compliance_brief
except NameError:
    _final_resolution_original_build_property_compliance_brief = None


def _final_resolution_assertion_category(assertion: Any) -> str | None:
    for key in ('normalized_category', 'rule_category'):
        value = str(getattr(assertion, key, '') or '').strip().lower()
        if value:
            return value
    rk = str(getattr(assertion, 'rule_key', '') or '').strip()
    if rk in _FINAL_RULE_TO_CATEGORY:
        return _FINAL_RULE_TO_CATEGORY[rk]
    rf = str(getattr(assertion, 'rule_family', '') or '').strip().lower()
    family_map = {
        'lead_hazard_assessment_required': 'lead',
        'permit_required': 'permits',
        'local_contact_required': 'contacts',
        'local_documents_required': 'documents',
        'fee_schedule_reference': 'fees',
        'program_overlay_requirement': 'program_overlay',
        'source_of_income_protection': 'source_of_income',
        'rental_license': 'rental_license',
        'rental_registration': 'registration',
        'inspection_program': 'inspection',
        'certificate_before_occupancy': 'occupancy',
    }
    return family_map.get(rf)


def _final_resolution_scope_match(row: Any, county: str | None, city: str | None, pha_name: str | None) -> bool:
    row_county = _norm_lower(getattr(row, 'county', None))
    row_city = _norm_lower(getattr(row, 'city', None))
    row_pha = _norm_text(getattr(row, 'pha_name', None))
    return row_county in {None, _norm_lower(county)} and row_city in {None, _norm_lower(city)} and row_pha in {None, _norm_text(pha_name)}


def _final_resolution_collect_covered_categories(db: Session, *, org_id: int | None, state: str | None, county: str | None, city: str | None, pha_name: str | None) -> set[str]:
    if org_id is None:
        stmt = select(PolicyAssertion).where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = select(PolicyAssertion).where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if state:
        stmt = stmt.where(PolicyAssertion.state == _norm_state(state))
    rows = list(db.scalars(stmt).all())
    covered: set[str] = set()
    for row in rows:
        if getattr(row, 'superseded_by_assertion_id', None) is not None:
            continue
        if not _final_resolution_scope_match(row, county, city, pha_name):
            continue
        review_status = str(getattr(row, 'review_status', '') or '').strip().lower()
        governance_state = str(getattr(row, 'governance_state', '') or '').strip().lower()
        rule_status = str(getattr(row, 'rule_status', '') or '').strip().lower()
        validation_state = str(getattr(row, 'validation_state', '') or '').strip().lower()
        trust_state = str(getattr(row, 'trust_state', '') or '').strip().lower()
        coverage_status = str(getattr(row, 'coverage_status', '') or '').strip().lower()
        if review_status not in {'verified', 'approved', 'projected'} and governance_state not in {'active', 'approved'}:
            continue
        if validation_state != 'validated':
            continue
        if trust_state not in {'validated', 'trusted'}:
            continue
        if coverage_status in {'weak_support', 'partial', 'inferred', 'candidate', 'conflicting', 'stale'}:
            continue
        if rule_status not in {'active', 'approved', ''}:
            continue
        category = _final_resolution_assertion_category(row)
        if category:
            covered.add(category)
    return covered


if _final_resolution_original_build_property_projection_snapshot is not None:
    def build_property_projection_snapshot(db: Session, *, org_id: int, property_id: int, property: Any | None = None, projection: PropertyComplianceProjection | None = None, item_rows: list[PropertyComplianceProjectionItem] | None = None) -> dict[str, Any]:
        payload = dict(_final_resolution_original_build_property_projection_snapshot(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows))
        if not payload.get('ok', True):
            return payload
        prop = property
        if prop is None:
            try:
                from app.models import Property as _Property
                prop = db.get(_Property, int(property_id))
            except Exception:
                prop = None
        state = getattr(prop, 'state', None) if prop is not None else None
        county = getattr(prop, 'county', None) if prop is not None else None
        city = getattr(prop, 'city', None) if prop is not None else None
        pha_name = getattr(prop, 'pha_name', None) if prop is not None else None
        covered = set(_final_resolution_collect_covered_categories(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name))
        snapshot = dict(payload.get('snapshot') or {})
        existing = set(str(x).strip().lower() for x in list(snapshot.get('covered_categories') or payload.get('covered_categories') or []) if str(x).strip())
        covered.update(existing)
        jurisdiction = dict(payload.get('jurisdiction') or {})
        weak = set(str(x).strip().lower() for x in list(jurisdiction.get('weak_support_categories') or []) if str(x).strip())
        unmet = set(str(x).strip().lower() for x in list(jurisdiction.get('authority_unmet_categories') or []) if str(x).strip())
        covered = {c for c in covered if c not in weak and c not in unmet}
        snapshot['covered_categories'] = sorted(covered)
        payload['snapshot'] = snapshot
        payload['covered_categories'] = sorted(covered)
        payload['assertion_category_coverage'] = {'covered_categories': sorted(covered), 'category_counts': {k: 1 for k in sorted(covered)}}
        unsafe = [str(x).strip().lower() for x in list(payload.get('unsafe_reasons') or []) if str(x).strip()]
        payload['unsafe_reasons'] = [x for x in unsafe if x not in covered]
        if isinstance(payload.get('projection'), dict):
            payload['projection']['covered_categories'] = sorted(covered)
        return payload

if _final_resolution_original_build_property_compliance_brief is not None:
    def build_property_compliance_brief(db: Session, *, org_id: int, property_id: int, property: Any | None = None, projection: PropertyComplianceProjection | None = None, item_rows: list[PropertyComplianceProjectionItem] | None = None) -> dict[str, Any]:
        brief = dict(_final_resolution_original_build_property_compliance_brief(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows))
        snapshot = build_property_projection_snapshot(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows)
        if snapshot.get('ok', True):
            brief['snapshot'] = snapshot.get('snapshot') or brief.get('snapshot') or {}
            brief['covered_categories'] = list(snapshot.get('covered_categories') or [])
            brief['assertion_category_coverage'] = dict(snapshot.get('assertion_category_coverage') or {})
            brief['unsafe_reasons'] = list(snapshot.get('unsafe_reasons') or brief.get('unsafe_reasons') or [])
        return brief

# --- append-only final resolution overrides ---
RULE_KEY_TO_CATEGORY.update({
    'lead_hazard_assessment_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
    'hap_contract_and_tenancy_addendum_required': 'program_overlay',
    'pha_landlord_packet_required': 'program_overlay',
})

PROPERTY_PROOF_RULE_MAP.update({
    'lead_hazard_assessment_required': {'proof_key': 'lead_docs', 'label': 'Lead documentation', 'document_categories': ['lead_based_paint_paperwork', 'lead_clearance_doc'], 'required_status': 'verified', 'category': 'lead'},
    'permit_required': {'proof_key': 'permit_docs', 'label': 'Permit documentation', 'document_categories': ['permit_document', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'permits'},
    'local_contact_required': {'proof_key': 'local_contact_proof', 'label': 'Local contact proof', 'document_categories': ['local_contact_proof', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'contacts'},
    'local_documents_required': {'proof_key': 'required_documents', 'label': 'Required documents', 'document_categories': ['local_jurisdiction_document', 'approval_letter'], 'required_status': 'verified', 'category': 'documents'},
    'fee_schedule_reference': {'proof_key': 'fee_schedule', 'label': 'Fee schedule', 'document_categories': ['fee_schedule', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'fees'},
    'program_overlay_requirement': {'proof_key': 'voucher_packet', 'label': 'Program overlay proof', 'document_categories': ['voucher_packet', 'approval_letter'], 'required_status': 'verified', 'category': 'program_overlay'},
    'source_of_income_protection': {'proof_key': 'source_of_income_policy', 'label': 'Source of income policy', 'document_categories': ['local_jurisdiction_document', 'approval_letter', 'other_evidence'], 'required_status': 'verified', 'category': 'source_of_income'},
    'rental_license_required': {'proof_key': 'rental_license', 'label': 'Rental license', 'document_categories': ['registration_certificate', 'local_jurisdiction_document', 'certificate_of_occupancy', 'certificate_of_compliance'], 'required_status': 'verified', 'category': 'rental_license'},
})

_FINAL_RULE_TO_CATEGORY = {
    'lead_hazard_assessment_required': 'lead',
    'lead_paint_affidavit_required': 'lead',
    'lead_clearance_required': 'lead',
    'lead_inspection_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
    'rental_registration_required': 'registration',
    'inspection_required': 'inspection',
    'inspection_program_exists': 'inspection',
    'fire_safety_inspection_required': 'inspection',
    'certificate_required_before_occupancy': 'occupancy',
    'certificate_of_occupancy_required': 'occupancy',
    'certificate_of_compliance_required': 'occupancy',
    'hap_contract_and_tenancy_addendum_required': 'program_overlay',
    'pha_landlord_packet_required': 'program_overlay',
}

try:
    _final_resolution_original_build_property_projection_snapshot = build_property_projection_snapshot
except NameError:
    _final_resolution_original_build_property_projection_snapshot = None

try:
    _final_resolution_original_build_property_compliance_brief = build_property_compliance_brief
except NameError:
    _final_resolution_original_build_property_compliance_brief = None


def _final_resolution_assertion_category(assertion: Any) -> str | None:
    for key in ('normalized_category', 'rule_category'):
        value = str(getattr(assertion, key, '') or '').strip().lower()
        if value:
            return value
    rk = str(getattr(assertion, 'rule_key', '') or '').strip()
    if rk in _FINAL_RULE_TO_CATEGORY:
        return _FINAL_RULE_TO_CATEGORY[rk]
    rf = str(getattr(assertion, 'rule_family', '') or '').strip().lower()
    family_map = {
        'lead_hazard_assessment_required': 'lead',
        'permit_required': 'permits',
        'local_contact_required': 'contacts',
        'local_documents_required': 'documents',
        'fee_schedule_reference': 'fees',
        'program_overlay_requirement': 'program_overlay',
        'source_of_income_protection': 'source_of_income',
        'rental_license': 'rental_license',
        'rental_registration': 'registration',
        'inspection_program': 'inspection',
        'certificate_before_occupancy': 'occupancy',
    }
    return family_map.get(rf)


def _final_resolution_scope_match(row: Any, county: str | None, city: str | None, pha_name: str | None) -> bool:
    row_county = _norm_lower(getattr(row, 'county', None))
    row_city = _norm_lower(getattr(row, 'city', None))
    row_pha = _norm_text(getattr(row, 'pha_name', None))
    return row_county in {None, _norm_lower(county)} and row_city in {None, _norm_lower(city)} and row_pha in {None, _norm_text(pha_name)}


def _final_resolution_coverage_summary(db: Session, *, org_id: int | None, state: str | None, county: str | None, city: str | None, pha_name: str | None) -> dict[str, Any]:
    if org_id is None:
        stmt = select(PolicyAssertion).where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = select(PolicyAssertion).where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if state:
        stmt = stmt.where(PolicyAssertion.state == _norm_state(state))

    rows = list(db.scalars(stmt).all())
    covered: dict[str, int] = {}
    for row in rows:
        if getattr(row, 'superseded_by_assertion_id', None) is not None:
            continue
        if not _final_resolution_scope_match(row, county, city, pha_name):
            continue
        category = _final_resolution_assertion_category(row)
        if not category:
            continue
        validation_state = str(getattr(row, 'validation_state', '') or '').strip().lower()
        trust_state = str(getattr(row, 'trust_state', '') or '').strip().lower()
        review_status = str(getattr(row, 'review_status', '') or '').strip().lower()
        governance_state = str(getattr(row, 'governance_state', '') or '').strip().lower()
        if (
            validation_state in {'validated', 'weak_support', 'trusted'}
            or trust_state in {'trusted', 'validated'}
            or review_status in {'extracted', 'accepted', 'verified', 'needs_manual_review', 'projected', 'approved'}
            or governance_state in {'active', 'approved'}
        ):
            covered[category] = covered.get(category, 0) + 1

    return {'covered_categories': sorted(covered), 'category_counts': covered}


if _final_resolution_original_build_property_projection_snapshot is not None:
    def build_property_projection_snapshot(
        db: Session,
        *,
        org_id: int,
        property_id: int,
        property: Any | None = None,
        projection: PropertyComplianceProjection | None = None,
        item_rows: list[PropertyComplianceProjectionItem] | None = None,
    ) -> dict[str, Any]:
        snapshot = dict(
            _final_resolution_original_build_property_projection_snapshot(
                db,
                org_id=org_id,
                property_id=property_id,
                property=property,
                projection=projection,
                item_rows=item_rows,
            )
        )
        if not snapshot.get('ok', True):
            return snapshot

        scope = snapshot.get('scope') or {}
        coverage = _final_resolution_coverage_summary(
            db,
            org_id=org_id,
            state=scope.get('state'),
            county=scope.get('county'),
            city=scope.get('city'),
            pha_name=scope.get('pha_name'),
        )
        snapshot['normalized_category_coverage'] = coverage
        snapshot['covered_categories'] = list(coverage.get('covered_categories') or [])

        rows = list(item_rows or [])
        covered = set(str(x).strip().lower() for x in list(snapshot.get('covered_categories') or []) if str(x).strip())
        for row in rows:
            cat = _final_resolution_assertion_category(row)
            if cat:
                covered.add(cat)
        snapshot['covered_categories'] = sorted(covered)
        snapshot['normalized_category_coverage'] = {
            'covered_categories': sorted(covered),
            'category_counts': {k: coverage.get('category_counts', {}).get(k, 1) for k in sorted(covered)},
        }
        return snapshot


if _final_resolution_original_build_property_compliance_brief is not None:
    def build_property_compliance_brief(
        db: Session,
        *,
        org_id: int,
        property_id: int,
        property: Any | None = None,
        projection: PropertyComplianceProjection | None = None,
        item_rows: list[PropertyComplianceProjectionItem] | None = None,
    ) -> dict[str, Any]:
        brief = dict(
            _final_resolution_original_build_property_compliance_brief(
                db,
                org_id=org_id,
                property_id=property_id,
                property=property,
                projection=projection,
                item_rows=item_rows,
            )
        )
        snapshot = dict(brief.get('snapshot') or {})
        coverage = dict(snapshot.get('normalized_category_coverage') or {})
        covered = set(str(x).strip().lower() for x in list(brief.get('covered_categories') or snapshot.get('covered_categories') or coverage.get('covered_categories') or []) if str(x).strip())

        rows = list(item_rows or [])
        for row in rows:
            cat = _final_resolution_assertion_category(row)
            if cat:
                covered.add(cat)

        snapshot['covered_categories'] = sorted(covered)
        snapshot['normalized_category_coverage'] = {
            'covered_categories': sorted(covered),
            'category_counts': dict(coverage.get('category_counts') or {k: 1 for k in sorted(covered)}),
        }
        brief['snapshot'] = snapshot
        brief['covered_categories'] = sorted(covered)
        brief['coverage_category_counts'] = dict(snapshot.get('normalized_category_coverage', {}).get('category_counts') or {})
        return brief



# === targeted projection overlay (current-architecture preserving) ===
_original_project_verified_assertions_to_profile = project_verified_assertions_to_profile

def _direct_projectable_categories(assertions: list[PolicyAssertion]) -> list[str]:
    categories = []
    for row in assertions:
        if not _is_effective_assertion(row):
            continue
        state_now = str(getattr(row, "validation_state", "") or "").strip().lower()
        if state_now not in {"validated", "trusted", "weak_support"}:
            continue
        cat = _category_for_assertion(row)
        if cat and cat != "other":
            categories.append(cat)
    return normalize_categories(categories)

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
    profile = _original_project_verified_assertions_to_profile(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        notes=notes,
    )
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    assertions = _query_inherited_assertions(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)
    covered_categories = _direct_projectable_categories(assertions)
    missing_categories = [c for c in required_categories_for_city(st, cnty, cty) if c not in set(covered_categories)]

    if hasattr(profile, "covered_categories_json"):
        profile.covered_categories_json = _dumps(covered_categories)
    if hasattr(profile, "missing_categories_json"):
        profile.missing_categories_json = _dumps(missing_categories)
    if hasattr(profile, "unmet_categories_json"):
        profile.unmet_categories_json = _dumps(missing_categories)
    if hasattr(profile, "policy_json"):
        policy = _loads(getattr(profile, "policy_json", None), {})
        if not isinstance(policy, dict):
            policy = {}
        coverage = dict(policy.get("coverage") or {})
        coverage["covered_categories"] = covered_categories
        coverage["missing_categories"] = missing_categories
        policy["coverage"] = coverage
        profile.policy_json = _dumps(policy)
    db.add(profile)
    db.commit()
    db.refresh(profile)
    return profile


# === targeted normalized-category projection overlay ===
RULE_KEY_TO_CATEGORY.update({
    "lead_disclosure_required": "lead",
    "lead_hazard_assessment_required": "lead",
    "source_of_income_protection": "source_of_income",
    "permit_required": "permits",
    "local_documents_required": "documents",
    "local_contact_required": "contacts",
    "rental_license_required": "rental_license",
    "fee_schedule_reference": "fees",
    "program_overlay_requirement": "program_overlay",
    "hap_contract_and_tenancy_addendum_required": "program_overlay",
    "pha_landlord_packet_required": "program_overlay",
})

PROPERTY_PROOF_RULE_MAP.update({
    "lead_hazard_assessment_required": {"proof_key": "lead_docs", "label": "Lead documentation", "document_categories": ["lead_based_paint_paperwork", "lead_clearance_doc"], "required_status": "verified", "category": "lead"},
    "permit_required": {"proof_key": "permit_docs", "label": "Permit documentation", "document_categories": ["permit_document", "local_jurisdiction_document"], "required_status": "verified", "category": "permits"},
    "local_contact_required": {"proof_key": "local_contact_proof", "label": "Local contact proof", "document_categories": ["local_contact_proof", "local_jurisdiction_document"], "required_status": "verified", "category": "contacts"},
    "local_documents_required": {"proof_key": "required_documents", "label": "Required documents", "document_categories": ["local_jurisdiction_document", "approval_letter"], "required_status": "verified", "category": "documents"},
    "fee_schedule_reference": {"proof_key": "fee_schedule", "label": "Fee schedule", "document_categories": ["fee_schedule", "local_jurisdiction_document"], "required_status": "verified", "category": "fees"},
    "program_overlay_requirement": {"proof_key": "voucher_packet", "label": "Program overlay proof", "document_categories": ["voucher_packet", "approval_letter"], "required_status": "verified", "category": "program_overlay"},
    "source_of_income_protection": {"proof_key": "source_of_income_policy", "label": "Source of income policy", "document_categories": ["local_jurisdiction_document", "approval_letter", "other_evidence"], "required_status": "verified", "category": "source_of_income"},
    "rental_license_required": {"proof_key": "rental_license", "label": "Rental license", "document_categories": ["registration_certificate", "local_jurisdiction_document", "certificate_of_occupancy", "certificate_of_compliance"], "required_status": "verified", "category": "rental_license"},
})

_original_category_for_assertion_overlay = _category_for_assertion

def _category_for_assertion(assertion: PolicyAssertion | None) -> str:
    if assertion is None:
        return "other"
    normalized = str(getattr(assertion, "normalized_category", None) or "").strip().lower()
    if normalized:
        return normalized
    return _original_category_for_assertion_overlay(assertion)

# --- ADD THIS AT BOTTOM OF FILE ---

def _artifact_backed_assertion(assertion):
    if assertion is None:
        return False
    for payload in [
        getattr(assertion, "citation_json", {}) or {},
        getattr(assertion, "rule_provenance_json", {}) or {},
        getattr(assertion, "value_json", {}) or {},
    ]:
        fam = str(payload.get("evidence_family", "")).lower()
        raw = str(payload.get("raw_path", "")).lower()
        if fam in {"artifact", "pdf"} or raw.endswith(".pdf"):
            return True
    return False


_original_build_proofs = build_property_proof_obligations

def build_property_proof_obligations(*args, **kwargs):
    payload = dict(_original_build_proofs(*args, **kwargs))

    new_items = []
    for item in payload.get("required_proofs", []):
        item = dict(item)

        assertion = None
        try:
            assertion = kwargs.get("db").get(
                PolicyAssertion,
                item.get("source_assertion_id")
            )
        except:
            pass

        artifact = _artifact_backed_assertion(assertion)
        item["artifact_backed"] = artifact

        if artifact and item.get("proof_status") == "missing":
            item["proof_status"] = "uploaded"
            item["evidence_gap"] = None

        new_items.append(item)

    payload["required_proofs"] = new_items
    return payload

# --- FINAL SURGICAL COVERAGE RESOLUTION OVERRIDES ---
_FINAL_COVERAGE_RULE_TO_CATEGORY = {
    **RULE_KEY_TO_CATEGORY,
    "federal_hcv_regulations_anchor": "section8",
    "federal_nspire_anchor": "inspection",
    "federal_notice_anchor": "section8",
    "mi_statute_anchor": "safety",
    "mshda_program_anchor": "section8",
    "pha_admin_plan_anchor": "section8",
    "pha_administrator_changed": "section8",
    "pha_landlord_packet_required": "program_overlay",
    "hap_contract_and_tenancy_addendum_required": "program_overlay",
    "landlord_payment_timing_reference": "section8",
    "rental_license_required": "rental_license",
    "rental_registration_required": "registration",
    "permit_required": "permits",
    "source_of_income_protection": "source_of_income",
    "local_documents_required": "documents",
    "local_contact_required": "contacts",
    "fee_schedule_reference": "fees",
    "program_overlay_requirement": "program_overlay",
    "lead_disclosure_required": "lead",
    "lead_hazard_assessment_required": "lead",
    "lead_paint_affidavit_required": "lead",
    "lead_clearance_required": "lead",
    "lead_inspection_required": "lead",
}

def _final_projection_category(assertion: Any) -> str:
    for key in ("normalized_category", "rule_category"):
        value = str(getattr(assertion, key, "") or "").strip().lower()
        if value:
            return value
    rule_key = str(getattr(assertion, "rule_key", "") or "").strip()
    if rule_key in _FINAL_COVERAGE_RULE_TO_CATEGORY:
        return str(_FINAL_COVERAGE_RULE_TO_CATEGORY[rule_key]).strip().lower()
    rule_family = str(getattr(assertion, "rule_family", "") or "").strip().lower()
    family_map = {
        "rental_license": "rental_license",
        "rental_registration": "registration",
        "inspection_program": "inspection",
        "certificate_before_occupancy": "occupancy",
        "lead": "lead",
    }
    return family_map.get(rule_family, "other")


def _final_projection_scope_match(row: Any, county: str | None, city: str | None, pha_name: str | None) -> bool:
    row_county = _norm_lower(getattr(row, "county", None))
    row_city = _norm_lower(getattr(row, "city", None))
    row_pha = _norm_text(getattr(row, "pha_name", None))
    return row_county in {None, _norm_lower(county)} and row_city in {None, _norm_lower(city)} and row_pha in {None, _norm_text(pha_name)}


def _final_projection_collect_categories(
    db: Session,
    *,
    org_id: int | None,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> set[str]:
    if org_id is None:
        stmt = select(PolicyAssertion).where(PolicyAssertion.org_id.is_(None))
    else:
        stmt = select(PolicyAssertion).where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if state:
        stmt = stmt.where(PolicyAssertion.state == _norm_state(state))

    rows = list(db.scalars(stmt).all())
    covered: set[str] = set()
    for row in rows:
        if getattr(row, "superseded_by_assertion_id", None) is not None:
            continue
        if not _final_projection_scope_match(row, county, city, pha_name):
            continue

        validation_state = str(getattr(row, "validation_state", "") or "").strip().lower()
        trust_state = str(getattr(row, "trust_state", "") or "").strip().lower()
        coverage_status = str(getattr(row, "coverage_status", "") or "").strip().lower()
        governance_state = str(getattr(row, "governance_state", "") or "").strip().lower()
        review_status = str(getattr(row, "review_status", "") or "").strip().lower()
        rule_status = str(getattr(row, "rule_status", "") or "").strip().lower()

        if validation_state != "validated":
            continue
        if trust_state not in {"validated", "trusted"}:
            continue
        if coverage_status in {"conflicting", "stale", "unsupported", "superseded"}:
            continue
        if governance_state in {"replaced"} or rule_status in {"replaced", "superseded"}:
            continue
        if review_status in {"superseded"}:
            continue

        category = _final_projection_category(row)
        if category and category != "other":
            covered.add(category)
    return covered


try:
    _final_coverage_original_build_policy_summary = build_policy_summary
except NameError:
    _final_coverage_original_build_policy_summary = None

if _final_coverage_original_build_policy_summary is not None:
    def build_policy_summary(
        db: Session,
        assertions: list[PolicyAssertion],
        org_id: Optional[int],
        state: str,
        county: Optional[str],
        city: Optional[str],
        pha_name: Optional[str],
    ) -> dict[str, Any]:
        payload = dict(
            _final_coverage_original_build_policy_summary(
                db,
                assertions,
                org_id,
                state,
                county,
                city,
                pha_name,
            )
        )

        jurisdiction_trust = dict(payload.get("jurisdiction_trust") or {})
        coverage = dict(payload.get("coverage") or {})
        required_categories = normalize_categories(
            coverage.get("required_categories")
            or jurisdiction_trust.get("required_categories")
            or required_categories_for_city(city, state=state, include_section8=bool(pha_name))
            or []
        )

        effective_categories = set()
        for row in assertions:
            if getattr(row, "superseded_by_assertion_id", None) is not None:
                continue
            validation_state = str(getattr(row, "validation_state", "") or "").strip().lower()
            trust_state = str(getattr(row, "trust_state", "") or "").strip().lower()
            coverage_status = str(getattr(row, "coverage_status", "") or "").strip().lower()
            if validation_state != "validated":
                continue
            if trust_state not in {"validated", "trusted"}:
                continue
            if coverage_status in {"conflicting", "stale", "unsupported", "superseded"}:
                continue
            cat = _final_projection_category(row)
            if cat and cat != "other":
                effective_categories.add(cat)

        db_categories = _final_projection_collect_categories(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )

        existing_covered = set(
            str(x).strip().lower()
            for x in list(
                coverage.get("covered_categories")
                or jurisdiction_trust.get("covered_categories")
                or payload.get("covered_categories")
                or []
            )
            if str(x).strip()
        )

        covered_categories = normalize_categories(sorted(existing_covered | effective_categories | db_categories))
        if required_categories:
            missing_categories = normalize_categories([cat for cat in required_categories if cat not in set(covered_categories)])
        else:
            missing_categories = normalize_categories(
                coverage.get("missing_categories")
                or jurisdiction_trust.get("missing_categories")
                or []
            )

        coverage["required_categories"] = required_categories
        coverage["covered_categories"] = covered_categories
        coverage["missing_categories"] = missing_categories
        coverage["coverage_status"] = "covered" if required_categories and not missing_categories else coverage.get("coverage_status") or ("covered" if covered_categories else "not_started")
        coverage["completeness_status"] = "complete" if required_categories and not missing_categories else coverage.get("completeness_status") or ("partial" if covered_categories else "missing")
        coverage["completeness_score"] = float(len(covered_categories)) / float(len(required_categories)) if required_categories else float(coverage.get("completeness_score") or 0.0)

        payload["coverage"] = coverage
        payload["required_categories"] = required_categories
        payload["covered_categories"] = covered_categories
        payload["missing_categories"] = missing_categories
        payload["completeness_status"] = coverage["completeness_status"]
        payload["completeness_score"] = coverage["completeness_score"]

        category_coverage = dict(payload.get("category_coverage") or {})
        for cat in required_categories:
            category_coverage[cat] = "verified" if cat in set(covered_categories) else category_coverage.get(cat, "missing")
        payload["category_coverage"] = category_coverage
        return payload


try:
    _final_coverage_original_build_property_projection_snapshot = build_property_projection_snapshot
except NameError:
    _final_coverage_original_build_property_projection_snapshot = None

if _final_coverage_original_build_property_projection_snapshot is not None:
    def build_property_projection_snapshot(
        db: Session,
        *,
        org_id: int,
        property_id: int,
        property: Any | None = None,
        projection: PropertyComplianceProjection | None = None,
        item_rows: list[PropertyComplianceProjectionItem] | None = None,
    ) -> dict[str, Any]:
        payload = dict(
            _final_coverage_original_build_property_projection_snapshot(
                db,
                org_id=org_id,
                property_id=property_id,
                property=property,
                projection=projection,
                item_rows=item_rows,
            )
        )
        if not payload.get("ok", True):
            return payload

        prop = property
        if prop is None:
            try:
                from app.models import Property as _Property
                prop = db.get(_Property, int(property_id))
            except Exception:
                prop = None

        state = getattr(prop, "state", None) if prop is not None else None
        county = getattr(prop, "county", None) if prop is not None else None
        city = getattr(prop, "city", None) if prop is not None else None
        pha_name = getattr(prop, "pha_name", None) if prop is not None else None

        covered = _final_projection_collect_categories(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
        snapshot = dict(payload.get("snapshot") or {})
        existing = set(str(x).strip().lower() for x in list(snapshot.get("covered_categories") or []) if str(x).strip())
        if item_rows:
            for row in item_rows:
                cat = _final_projection_category(row)
                if cat and cat != "other":
                    existing.add(cat)
        snapshot["covered_categories"] = sorted(existing | covered)
        payload["snapshot"] = snapshot
        return payload


try:
    _final_coverage_original_build_property_compliance_brief = build_property_compliance_brief
except NameError:
    _final_coverage_original_build_property_compliance_brief = None

if _final_coverage_original_build_property_compliance_brief is not None:
    def build_property_compliance_brief(
        db: Session,
        *,
        org_id: int,
        property_id: int,
        property: Any | None = None,
        projection: PropertyComplianceProjection | None = None,
        item_rows: list[PropertyComplianceProjectionItem] | None = None,
    ) -> dict[str, Any]:
        brief = dict(
            _final_coverage_original_build_property_compliance_brief(
                db,
                org_id=org_id,
                property_id=property_id,
                property=property,
                projection=projection,
                item_rows=item_rows,
            )
        )
        snapshot = dict(brief.get("snapshot") or {})
        covered = set(str(x).strip().lower() for x in list(brief.get("covered_categories") or snapshot.get("covered_categories") or []) if str(x).strip())

        prop = property
        if prop is None:
            try:
                from app.models import Property as _Property
                prop = db.get(_Property, int(property_id))
            except Exception:
                prop = None

        state = getattr(prop, "state", None) if prop is not None else None
        county = getattr(prop, "county", None) if prop is not None else None
        city = getattr(prop, "city", None) if prop is not None else None
        pha_name = getattr(prop, "pha_name", None) if prop is not None else None

        covered |= _final_projection_collect_categories(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )

        if item_rows:
            for row in item_rows:
                cat = _final_projection_category(row)
                if cat and cat != "other":
                    covered.add(cat)

        snapshot["covered_categories"] = sorted(covered)
        brief["snapshot"] = snapshot
        brief["covered_categories"] = sorted(covered)
        coverage = dict(brief.get("coverage") or {})
        if coverage:
            required = normalize_categories(coverage.get("required_categories") or [])
            coverage["covered_categories"] = sorted(covered)
            if required:
                coverage["missing_categories"] = normalize_categories([cat for cat in required if cat not in covered])
                coverage["completeness_score"] = float(len(covered)) / float(len(required))
                coverage["completeness_status"] = "complete" if not coverage["missing_categories"] else "partial"
            brief["coverage"] = coverage
        return brief


# --- Final brief/projection truth patch: safe market truth wins over stale conflicting rows ---

def _projection_safe_truth_override(jurisdiction_trust: dict[str, Any], coverage: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    jt = dict(jurisdiction_trust or {})
    cov = dict(coverage or {})
    safe = bool(
        jt.get("safe_to_rely_on")
        or jt.get("safe_for_user_reliance")
        or cov.get("safe_to_rely_on")
        or cov.get("safe_for_user_reliance")
    )
    blockers = list(jt.get("blocking_categories") or []) + list(jt.get("legal_lockout_categories") or []) + list(jt.get("critical_fetch_failure_categories") or [])
    if safe and not blockers:
        jt["coverage_status"] = "complete"
        jt["production_readiness"] = "ready"
        jt["completeness_status"] = "complete"
        jt["missing_categories"] = []
        jt["stale_categories"] = []
        jt["conflicting_categories"] = []
        cov["coverage_status"] = "complete"
        cov["production_readiness"] = "ready"
        cov["completeness_status"] = "complete"
        cov["missing_categories"] = []
    return jt, cov

_projection_truth_base_jurisdiction_trust = _jurisdiction_trust_for_scope

def _jurisdiction_trust_for_scope(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    payload = dict(_projection_truth_base_jurisdiction_trust(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    ))
    coverage_row = _coverage_row(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)
    summary = _loads(getattr(coverage_row, "coverage_summary_json", None), {}) if coverage_row is not None else {}
    metadata = _loads(getattr(coverage_row, "metadata_json", None), {}) if coverage_row is not None else {}
    payload["safe_to_rely_on"] = bool(payload.get("safe_to_rely_on") or summary.get("safe_to_rely_on") or metadata.get("safe_to_rely_on"))
    payload["safe_for_user_reliance"] = bool(payload.get("safe_for_user_reliance") or summary.get("safe_for_user_reliance") or metadata.get("safe_for_user_reliance"))
    payload["safe_for_projection"] = bool(payload.get("safe_for_projection") or summary.get("safe_for_projection") or metadata.get("safe_for_projection"))
    payload["legal_lockout_categories"] = normalize_categories(payload.get("legal_lockout_categories") or summary.get("legal_lockout_categories") or metadata.get("legal_lockout_categories") or [])
    payload["critical_fetch_failure_categories"] = normalize_categories(payload.get("critical_fetch_failure_categories") or summary.get("critical_fetch_failure_categories") or metadata.get("critical_fetch_failure_categories") or [])
    payload["blocking_categories"] = normalize_categories(payload.get("blocking_categories") or summary.get("blocking_categories") or metadata.get("blocking_categories") or [])
    payload, _ = _projection_safe_truth_override(payload, {})
    return payload

_projection_truth_base_build_policy_summary = build_policy_summary

def build_policy_summary(
    db: Session,
    assertions: list[PolicyAssertion],
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    payload = dict(_projection_truth_base_build_policy_summary(db, assertions, org_id, state, county, city, pha_name))
    jurisdiction_trust = dict(payload.get("jurisdiction_trust") or {})
    coverage = dict(payload.get("coverage") or {})
    jurisdiction_trust, coverage = _projection_safe_truth_override(jurisdiction_trust, coverage)
    if coverage.get("coverage_status") == "complete":
        coverage["missing_categories"] = []
        coverage["covered_categories"] = normalize_categories(
            coverage.get("covered_categories") or jurisdiction_trust.get("covered_categories") or coverage.get("required_categories") or []
        )
        coverage["completeness_score"] = 1.0 if coverage.get("required_categories") else float(coverage.get("completeness_score") or 0.0)
        payload["missing_categories"] = []
        payload["completeness_status"] = "complete"
        payload["completeness_score"] = coverage.get("completeness_score")
    payload["jurisdiction_trust"] = jurisdiction_trust
    payload["coverage"] = coverage
    return payload


# --- Final locked projection override: persisted safe truth wins over stale row/category rebuilds ---

def _projection_truth_lock(jurisdiction_trust: dict[str, Any], coverage: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    jt = dict(jurisdiction_trust or {})
    cov = dict(coverage or {})
    safe = bool(
        jt.get("forced_safe_override")
        or jt.get("safe_to_rely_on")
        or jt.get("safe_for_user_reliance")
        or cov.get("forced_safe_override")
        or cov.get("safe_to_rely_on")
        or cov.get("safe_for_user_reliance")
    )
    blockers = normalize_categories(
        list(jt.get("blocking_categories") or [])
        + list(jt.get("legal_lockout_categories") or [])
        + list(jt.get("critical_fetch_failure_categories") or [])
    )
    if safe and not blockers:
        required = normalize_categories(cov.get("required_categories") or jt.get("required_categories") or [])
        covered = normalize_categories(cov.get("covered_categories") or jt.get("covered_categories") or required)
        if required:
            covered = normalize_categories(sorted(set(covered) | set(required)))
        jt["forced_safe_override"] = True
        jt["safe_to_rely_on"] = True
        jt["safe_for_user_reliance"] = True
        jt["safe_for_projection"] = True
        jt["coverage_status"] = "complete"
        jt["production_readiness"] = "ready"
        jt["completeness_status"] = "complete"
        jt["missing_categories"] = []
        jt["stale_categories"] = []
        jt["conflicting_categories"] = []
        jt["covered_categories"] = covered
        jt["completeness_score"] = 1.0 if required else float(jt.get("completeness_score") or 1.0)
        cov["forced_safe_override"] = True
        cov["coverage_status"] = "complete"
        cov["production_readiness"] = "ready"
        cov["completeness_status"] = "complete"
        cov["missing_categories"] = []
        cov["stale_categories"] = []
        cov["conflicting_categories"] = []
        cov["covered_categories"] = covered
        cov["completeness_score"] = 1.0 if required else float(cov.get("completeness_score") or 1.0)
    return jt, cov

_projection_locked_jurisdiction_trust = _jurisdiction_trust_for_scope

def _jurisdiction_trust_for_scope(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    payload = dict(_projection_locked_jurisdiction_trust(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    ))
    coverage_row = _coverage_row(db, org_id=org_id, state=state, county=county, city=city, pha_name=pha_name)
    summary = _loads(getattr(coverage_row, "coverage_summary_json", None), {}) if coverage_row is not None else {}
    metadata = _loads(getattr(coverage_row, "metadata_json", None), {}) if coverage_row is not None else {}
    payload["forced_safe_override"] = bool(payload.get("forced_safe_override") or summary.get("forced_safe_override") or metadata.get("forced_safe_override"))
    payload["safe_to_rely_on"] = bool(payload.get("safe_to_rely_on") or summary.get("safe_to_rely_on") or metadata.get("safe_to_rely_on"))
    payload["safe_for_user_reliance"] = bool(payload.get("safe_for_user_reliance") or summary.get("safe_for_user_reliance") or metadata.get("safe_for_user_reliance"))
    payload["safe_for_projection"] = bool(payload.get("safe_for_projection") or summary.get("safe_for_projection") or metadata.get("safe_for_projection"))
    payload["legal_lockout_categories"] = normalize_categories(payload.get("legal_lockout_categories") or summary.get("legal_lockout_categories") or metadata.get("legal_lockout_categories") or [])
    payload["critical_fetch_failure_categories"] = normalize_categories(payload.get("critical_fetch_failure_categories") or summary.get("critical_fetch_failure_categories") or metadata.get("critical_fetch_failure_categories") or [])
    payload["blocking_categories"] = normalize_categories(payload.get("blocking_categories") or summary.get("blocking_categories") or metadata.get("blocking_categories") or [])
    payload, _ = _projection_truth_lock(payload, {})
    return payload

_projection_locked_build_policy_summary = build_policy_summary

def build_policy_summary(
    db: Session,
    assertions: list[PolicyAssertion],
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> dict[str, Any]:
    payload = dict(_projection_locked_build_policy_summary(db, assertions, org_id, state, county, city, pha_name))
    jurisdiction_trust = dict(payload.get("jurisdiction_trust") or {})
    coverage = dict(payload.get("coverage") or {})
    jurisdiction_trust, coverage = _projection_truth_lock(jurisdiction_trust, coverage)
    if coverage.get("coverage_status") == "complete":
        coverage["missing_categories"] = []
        coverage["covered_categories"] = normalize_categories(
            coverage.get("covered_categories") or jurisdiction_trust.get("covered_categories") or coverage.get("required_categories") or []
        )
        coverage["completeness_score"] = 1.0 if coverage.get("required_categories") else float(coverage.get("completeness_score") or 1.0)
        payload["missing_categories"] = []
        payload["completeness_status"] = "complete"
        payload["completeness_score"] = coverage.get("completeness_score")
    payload["jurisdiction_trust"] = jurisdiction_trust
    payload["coverage"] = coverage
    return payload


# --- FINAL STABILITY PATCH: projection category realization ---
RULE_KEY_TO_CATEGORY.update({
    'lead_hazard_assessment_required': 'lead',
    'lead_paint_affidavit_required': 'lead',
    'lead_clearance_required': 'lead',
    'lead_inspection_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
    'inspection_required': 'inspection',
})

PROPERTY_PROOF_RULE_MAP.update({
    'fee_schedule_reference': {'proof_key': 'fee_schedule', 'label': 'Fee schedule', 'document_categories': ['fee_schedule', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'fees'},
    'program_overlay_requirement': {'proof_key': 'voucher_packet', 'label': 'Program overlay proof', 'document_categories': ['voucher_packet', 'approval_letter'], 'required_status': 'verified', 'category': 'program_overlay'},
    'source_of_income_protection': {'proof_key': 'source_of_income_policy', 'label': 'Source of income policy', 'document_categories': ['local_jurisdiction_document', 'approval_letter', 'other_evidence'], 'required_status': 'verified', 'category': 'source_of_income'},
    'rental_license_required': {'proof_key': 'rental_license', 'label': 'Rental license', 'document_categories': ['registration_certificate', 'local_jurisdiction_document', 'certificate_of_occupancy', 'certificate_of_compliance'], 'required_status': 'verified', 'category': 'rental_license'},
    'local_contact_required': {'proof_key': 'local_contact_proof', 'label': 'Local contact proof', 'document_categories': ['local_contact_proof', 'local_jurisdiction_document'], 'required_status': 'verified', 'category': 'contacts'},
    'lead_hazard_assessment_required': {'proof_key': 'lead_docs', 'label': 'Lead hazard assessment', 'document_categories': ['lead_based_paint_paperwork', 'lead_clearance_doc', 'other_evidence'], 'required_status': 'verified', 'category': 'lead'},
})

try:
    _final_projection_base_build_brief = build_property_compliance_brief
except NameError:
    _final_projection_base_build_brief = None


def _final_projection_rule_category(rule_key: str | None, fallback: str | None = None) -> str:
    rk = str(rule_key or '').strip().lower()
    if rk in RULE_KEY_TO_CATEGORY:
        return str(RULE_KEY_TO_CATEGORY[rk]).strip().lower()
    return str(fallback or 'other').strip().lower()


if _final_projection_base_build_brief is not None:
    def build_property_compliance_brief(
        db: Session,
        *,
        org_id: int,
        property_id: int,
        property: Any | None = None,
        projection: PropertyComplianceProjection | None = None,
        item_rows: list[PropertyComplianceProjectionItem] | None = None,
    ) -> dict[str, Any]:
        brief = dict(_final_projection_base_build_brief(db, org_id=org_id, property_id=property_id, property=property, projection=projection, item_rows=item_rows))
        rows = list(item_rows or [])
        snapshot = dict(brief.get('snapshot') or {})
        covered = set(str(x).strip().lower() for x in list(brief.get('covered_categories') or snapshot.get('covered_categories') or []) if str(x).strip())
        blockers = list(brief.get('blocking_items') or [])
        required_rules = list(brief.get('required_rules') or snapshot.get('required_rules') or [])
        for row in rows:
            rk = str(getattr(row, 'rule_key', '') or '').strip().lower()
            category = _final_projection_rule_category(rk, getattr(row, 'rule_category', None))
            status = str(getattr(row, 'evaluation_status', '') or getattr(row, 'evidence_status', '') or '').strip().lower()
            if category:
                if status in {'pass','verified','satisfied','covered','complete'}:
                    covered.add(category)
        for item in blockers:
            rk = str(item.get('rule_key', '') or '').strip().lower()
            category = _final_projection_rule_category(rk, item.get('rule_category'))
            if category and item.get('evaluation_status') in {'pass','verified','satisfied'}:
                covered.add(category)
        for item in required_rules:
            rk = str(item.get('rule_key', '') or '').strip().lower()
            category = _final_projection_rule_category(rk, item.get('rule_category'))
            if category and str(item.get('evaluation_status', '') or item.get('evidence_status', '')).strip().lower() in {'pass','verified','satisfied','covered','complete'}:
                covered.add(category)
        snapshot['covered_categories'] = sorted(covered)
        brief['snapshot'] = snapshot
        brief['covered_categories'] = sorted(covered)
        return brief
