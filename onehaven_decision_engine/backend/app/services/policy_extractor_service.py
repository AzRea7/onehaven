# backend/app/services/policy_extractor_service.py
from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category, normalize_categories
from app.policy_models import PolicyAssertion, PolicySource, PolicySourceVersion
from app.services.policy_rule_normalizer import normalize_rule_candidate
from app.services.policy_validation_service import validate_assertion

DEFAULT_STALE_AFTER_DAYS = 90


def _utcnow() -> datetime:
    return datetime.utcnow()


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        return "{}"


def _json_loads_dict(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, str):
        raw = v.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _norm_state(value: Optional[str]) -> str:
    return (value or "MI").strip().upper()


def _norm_lower(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip().lower()
    return out or None


def _norm_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = value.strip()
    return out or None


def _rule_family_for(rule_key: str) -> str:
    mapping = {
        "document_reference": "document_reference",
        "federal_hcv_regulations_anchor": "federal_hcv",
        "federal_nspire_anchor": "federal_nspire",
        "federal_notice_anchor": "federal_notice",
        "mi_statute_anchor": "mi_landlord_tenant",
        "mshda_program_anchor": "mshda_program",
        "pha_admin_plan_anchor": "pha_admin_plan",
        "pha_administrator_changed": "pha_admin_transfer",
        "pha_landlord_packet_required": "pha_landlord_workflow",
        "hap_contract_and_tenancy_addendum_required": "voucher_lease_packet",
        "landlord_payment_timing_reference": "landlord_payment_timing",
        "rental_registration_required": "rental_registration",
        "rental_license_required": "rental_license",
        "certificate_required_before_occupancy": "certificate_before_occupancy",
        "certificate_of_occupancy_required": "certificate_before_occupancy",
        "certificate_of_compliance_required": "certificate_before_occupancy",
        "inspection_program_exists": "inspection_program",
        "fire_safety_inspection_required": "inspection_program",
        "property_maintenance_enforcement_anchor": "property_maintenance",
        "building_safety_division_anchor": "building_safety",
        "building_division_anchor": "building_division",
        "lead_paint_affidavit_required": "lead",
        "lead_clearance_required": "lead",
        "lead_inspection_required": "lead",
    }
    return mapping.get(rule_key, rule_key)


def _assertion_type_for(rule_key: str) -> str:
    if rule_key == "document_reference":
        return "document_reference"
    if rule_key.endswith("_anchor"):
        return "anchor"
    if rule_key == "pha_administrator_changed":
        return "superseding_notice"
    return "operational"


def _priority_for(rule_key: str) -> int:
    if rule_key in {
        "rental_registration_required",
        "rental_license_required",
        "certificate_required_before_occupancy",
        "certificate_of_occupancy_required",
        "certificate_of_compliance_required",
        "inspection_program_exists",
        "fire_safety_inspection_required",
        "hap_contract_and_tenancy_addendum_required",
        "pha_landlord_packet_required",
        "lead_clearance_required",
    }:
        return 10
    if rule_key in {
        "pha_administrator_changed",
        "landlord_payment_timing_reference",
    }:
        return 20
    if rule_key.endswith("_anchor"):
        return 50
    return 100


def _source_rank_for(source: PolicySource) -> int:
    url = (source.url or "").lower()
    publisher = (source.publisher or "").lower()

    if "ecfr.gov" in url:
        return 10
    if "federalregister.gov" in url:
        return 20
    if "legislature.mi.gov" in url:
        return 30
    if "michigan.gov/mshda" in url:
        return 40
    if ".gov" in url:
        return 50
    if "housing commission" in publisher or "dhcmi.org" in url:
        return 60
    return 100


def _source_version_for_source(db: Session, source_id: int) -> PolicySourceVersion | None:
    return db.scalar(
        select(PolicySourceVersion)
        .where(PolicySourceVersion.source_id == int(source_id))
        .order_by(PolicySourceVersion.retrieved_at.desc(), PolicySourceVersion.id.desc())
    )


def _stale_after_for(rule_key: str, source: PolicySource, now: datetime) -> datetime:
    url = (source.url or "").lower()

    if "ecfr.gov" in url or "legislature.mi.gov" in url:
        return now + timedelta(days=365)

    if rule_key in {
        "federal_hcv_regulations_anchor",
        "federal_nspire_anchor",
        "mi_statute_anchor",
    }:
        return now + timedelta(days=365)

    if rule_key in {
        "pha_administrator_changed",
        "landlord_payment_timing_reference",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
    }:
        return now + timedelta(days=120)

    refresh_days = int(getattr(source, "refresh_interval_days", 30) or 30)
    return now + timedelta(days=max(30, refresh_days, DEFAULT_STALE_AFTER_DAYS))


def _normalized_category_for(rule_key: str) -> str | None:
    mapping = {
        "document_reference": None,
        "federal_hcv_regulations_anchor": "section8",
        "federal_nspire_anchor": "inspection",
        "federal_notice_anchor": "section8",
        "mi_statute_anchor": "safety",
        "mshda_program_anchor": "section8",
        "pha_admin_plan_anchor": "section8",
        "pha_administrator_changed": "section8",
        "pha_landlord_packet_required": "section8",
        "hap_contract_and_tenancy_addendum_required": "section8",
        "landlord_payment_timing_reference": "section8",
        "rental_registration_required": "registration",
        "rental_license_required": "registration",
        "certificate_required_before_occupancy": "occupancy",
        "certificate_of_occupancy_required": "occupancy",
        "certificate_of_compliance_required": "occupancy",
        "inspection_program_exists": "inspection",
        "fire_safety_inspection_required": "inspection",
        "property_maintenance_enforcement_anchor": "safety",
        "building_safety_division_anchor": "safety",
        "building_division_anchor": "permits",
        "lead_paint_affidavit_required": "lead",
        "lead_clearance_required": "lead",
        "lead_inspection_required": "lead",
    }
    return normalize_category(mapping.get(rule_key))


def _refresh_source_category_metadata(source: PolicySource, created: list[PolicyAssertion], now: datetime) -> None:
    categories = normalize_categories(
        [a.normalized_category for a in created if getattr(a, "normalized_category", None)]
    )
    source.normalized_categories_json = _dumps(categories)
    source.freshness_checked_at = now
    source.last_verified_at = now if bool(getattr(source, "is_authoritative", False)) else source.last_verified_at
    source.last_fetched_at = getattr(source, "last_fetched_at", None) or now

    status_ok = source.http_status is not None and 200 <= int(source.http_status) < 400
    if not status_ok:
        source.freshness_status = "fetch_failed"
        source.freshness_reason = "http_status_not_successful"
    elif not source.retrieved_at:
        source.freshness_status = "unknown"
        source.freshness_reason = "missing_retrieved_at"
    elif source.retrieved_at < (
        now - timedelta(days=max(30, int(getattr(source, "refresh_interval_days", 30) or 30) * 2))
    ):
        source.freshness_status = "stale"
        source.freshness_reason = "retrieved_at_older_than_refresh_window"
    else:
        source.freshness_status = "fresh"
        source.freshness_reason = None


def _haystack(source: PolicySource) -> str:
    return " ".join(
        [
            str(source.url or ""),
            str(source.title or ""),
            str(source.publisher or ""),
            str(source.notes or ""),
            str(source.extracted_text or ""),
        ]
    ).lower()


def _has_any(text: str, patterns: list[str]) -> bool:
    return any(p in text for p in patterns)


def _already_added(created: list[PolicyAssertion], rule_key: str, raw_excerpt: Optional[str] = None) -> bool:
    for row in created:
        if row.rule_key != rule_key:
            continue
        if raw_excerpt is None:
            return True
        if (getattr(row, "raw_excerpt", None) or "").strip() == raw_excerpt.strip():
            return True
    return False


def _build_direct_citation(source: PolicySource, source_version_id: Optional[int], raw_text: str) -> dict[str, Any]:
    raw_excerpt = str(raw_text or "").strip()
    return {
        "url": source.url,
        "publisher": source.publisher,
        "title": source.title,
        "source_version_id": source_version_id,
        "raw_excerpt": raw_excerpt[:4000] if raw_excerpt else None,
        "direct": bool(source.url and raw_excerpt),
    }


def _coverage_status_for(*, confidence: float, citation_quality: float, evidence_state: str, conflict_hints: list[str]) -> str:
    if evidence_state == "conflicting" or conflict_hints:
        return "conflicting"
    if evidence_state == "confirmed" and confidence >= 0.80 and citation_quality >= 0.70:
        return "covered"
    if evidence_state == "inferred":
        return "inferred"
    if confidence >= 0.45 and citation_quality >= 0.30:
        return "partial"
    return "candidate"


def _candidate_payload(
    *,
    source: PolicySource,
    source_version_id: Optional[int],
    rule_key: str,
    value: dict[str, Any],
    confidence: float,
) -> dict[str, Any]:
    raw_text = value.get("summary") or value.get("text") or value.get("condition") or rule_key
    direct_citation = _build_direct_citation(source, source_version_id, str(raw_text))
    candidate = normalize_rule_candidate(
        {
            "rule_key": rule_key,
            "title": source.title,
            "publisher": source.publisher,
            "url": source.url,
            "text": str(raw_text),
            "raw_excerpt": str(raw_text),
            "source_citation": f"{source.publisher or ''} | {source.title or ''} | {source.url or ''}".strip(" |"),
            "source_type": getattr(source, "source_type", None),
            "source_level": getattr(source, "source_type", None) or "local",
            "property_type": _norm_text(getattr(source, "program_type", None)),
            "normalized_version": "v2",
            "confidence": confidence,
            "source_id": getattr(source, "id", None),
            "source_version_id": source_version_id,
            "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
            "state": getattr(source, "state", None),
            "county": getattr(source, "county", None),
            "city": getattr(source, "city", None),
            "pha_name": getattr(source, "pha_name", None),
            "program_type": getattr(source, "program_type", None),
            "value_json": {
                **value,
                "direct_citation": direct_citation,
            },
        }
    )

    if candidate is None:
        source_level = getattr(source, "source_type", None) or "local"
        property_type = _norm_text(getattr(source, "program_type", None))
        version_group = (
            _norm_text(getattr(source, "jurisdiction_slug", None))
            or _norm_text(getattr(source, "pha_name", None))
            or "global"
        )
        version_group = f"{version_group}:{rule_key}:{source_level}"
        citation_json = {
            **direct_citation,
            "citation_quality": 0.40 if direct_citation.get("direct") else 0.20,
            "category_mapping_source": "extractor_rule_map",
            "normalized_rule_key": rule_key,
            "rule_key_confidence": 1.0,
            "conflict_hints": [],
        }
        evidence_state = "inferred" if confidence >= 0.45 else "unknown"
        coverage_status = _coverage_status_for(
            confidence=confidence,
            citation_quality=float(citation_json["citation_quality"]),
            evidence_state=evidence_state,
            conflict_hints=[],
        )
        return {
            "rule_family": _rule_family_for(rule_key),
            "rule_category": _normalized_category_for(rule_key),
            "source_level": source_level,
            "property_type": property_type,
            "required": True,
            "blocking": rule_key in {
                "rental_registration_required",
                "rental_license_required",
                "certificate_required_before_occupancy",
                "certificate_of_occupancy_required",
                "certificate_of_compliance_required",
                "inspection_program_exists",
                "fire_safety_inspection_required",
                "pha_landlord_packet_required",
                "hap_contract_and_tenancy_addendum_required",
                "lead_clearance_required",
            },
            "source_citation": source.url,
            "raw_excerpt": str(raw_text),
            "normalized_version": "v2",
            "version_group": version_group,
            "confidence": confidence,
            "value_hash": None,
            "citation_json": citation_json,
            "confidence_basis": evidence_state,
            "coverage_status": coverage_status,
            "change_summary": None,
            "conflict_hints": [],
        }

    citation_json = {
        **candidate.value_json.get("direct_citation", direct_citation),
        "citation_quality": candidate.citation_quality,
        "category_mapping_source": candidate.category_mapping_source,
        "normalized_rule_key": candidate.normalized_rule_key,
        "rule_key_confidence": candidate.rule_key_confidence,
        "conflict_hints": list(candidate.conflict_hints),
        "citation_text": candidate.source_citation,
        "source_id": getattr(source, "id", None),
        "source_version_id": source_version_id,
    }
    coverage_status = _coverage_status_for(
        confidence=candidate.confidence,
        citation_quality=candidate.citation_quality,
        evidence_state=candidate.evidence_state,
        conflict_hints=list(candidate.conflict_hints),
    )
    return {
        "rule_family": candidate.rule_family,
        "rule_category": candidate.rule_category,
        "source_level": candidate.source_level,
        "property_type": candidate.property_type,
        "required": bool(candidate.required),
        "blocking": bool(candidate.blocking),
        "source_citation": candidate.source_citation or source.url,
        "raw_excerpt": candidate.raw_excerpt or str(raw_text),
        "normalized_version": candidate.normalized_version,
        "version_group": candidate.version_group,
        "confidence": float(candidate.confidence),
        "value_hash": candidate.fingerprint,
        "citation_json": citation_json,
        "confidence_basis": candidate.evidence_state,
        "coverage_status": coverage_status,
        "change_summary": None if not candidate.conflict_hints else f"conflict_hints={','.join(candidate.conflict_hints)}",
        "conflict_hints": list(candidate.conflict_hints),
    }


def _add_assertion(
    created: list[PolicyAssertion],
    *,
    target_org_id: Optional[int],
    source: PolicySource,
    source_version_id: Optional[int],
    now: datetime,
    rule_key: str,
    value: dict[str, Any],
    confidence: float,
) -> None:
    payload = _candidate_payload(
        source=source,
        source_version_id=source_version_id,
        rule_key=rule_key,
        value=value,
        confidence=confidence,
    )
    if _already_added(created, rule_key, payload.get("raw_excerpt")):
        return
    row = PolicyAssertion(
            org_id=target_org_id,
            source_id=source.id,
            source_version_id=source_version_id,
            state=_norm_state(source.state),
            county=_norm_lower(source.county),
            city=_norm_lower(source.city),
            pha_name=_norm_text(source.pha_name),
            program_type=_norm_text(source.program_type),
            rule_key=rule_key,
            rule_family=payload["rule_family"],
            assertion_type=_assertion_type_for(rule_key),
            value_json=_dumps(
                {
                    **value,
                    "conflict_hints": payload.get("conflict_hints", []),
                    "normalized_category": payload["rule_category"],
                }
            ),
            effective_date=getattr(source, "effective_date", None),
            expires_at=None,
            confidence=float(payload["confidence"]),
            priority=_priority_for(rule_key),
            source_rank=_source_rank_for(source),
            review_status="extracted",
            review_notes="auto_extracted_from_source_refresh",
            reviewed_by_user_id=None,
            verification_reason=None,
            stale_after=_stale_after_for(rule_key, source, now),
            superseded_by_assertion_id=None,
            replaced_by_assertion_id=None,
            jurisdiction_slug=getattr(source, "jurisdiction_slug", None),
            source_level=payload["source_level"],
            property_type=payload["property_type"],
            rule_category=payload["rule_category"],
            required=bool(payload["required"]),
            blocking=bool(payload["blocking"]),
            source_citation=payload["source_citation"],
            raw_excerpt=payload["raw_excerpt"],
            normalized_version=payload["normalized_version"],
            rule_status="candidate",
            governance_state="draft",
            version_group=payload["version_group"],
            version_number=1,
            is_current=False,
            citation_json=_dumps(payload["citation_json"]),
            rule_provenance_json=_dumps(
                {
                    "source_id": int(source.id),
                    "source_version_id": source_version_id,
                    "source_type": getattr(source, "source_type", None),
                    "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
                    "category_mapping_source": payload["citation_json"].get("category_mapping_source"),
                    "normalized_rule_key": payload["citation_json"].get("normalized_rule_key"),
                    "rule_key_confidence": payload["citation_json"].get("rule_key_confidence"),
                    "conflict_hints": payload["citation_json"].get("conflict_hints", []),
                }
            ),
            value_hash=payload.get("value_hash"),
            confidence_basis=payload["confidence_basis"],
            change_summary=payload["change_summary"],
            normalized_category=payload["rule_category"],
            coverage_status=payload["coverage_status"],
            source_freshness_status=getattr(source, "freshness_status", None),
            extracted_at=now,
            reviewed_at=None,
    )
    row.extraction_confidence = float(payload["confidence"])
    row.authority_score = float(getattr(source, "authority_score", 0.0) or 0.0)
    row.conflict_count = len(payload.get("conflict_hints", []))
    validation_payload = validate_assertion(assertion=row, source=source)
    row.validation_state = validation_payload["validation_state"]
    row.validation_score = validation_payload["validation_quality"]
    row.validation_reason = validation_payload["validation_reason"]
    row.trust_state = validation_payload["trust_state"]
    row.validated_at = validation_payload["validated_at"]
    row.extraction_confidence = validation_payload["extraction_confidence"]
    created.append(row)


def _maybe_add_warren_certificate_rule(
    created: list[PolicyAssertion],
    *,
    target_org_id: Optional[int],
    source: PolicySource,
    source_version_id: Optional[int],
    now: datetime,
) -> None:
    url = (source.url or "").lower()
    title = (source.title or "").lower()

    if "cityofwarren.org" not in url:
        return

    if (
        "building_res_city_certification_app.pdf" in url
        or "residential city certification" in title
    ):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="certificate_required_before_occupancy",
            value={
                "summary": (
                    "Warren requires residential city certification before occupancy "
                    "for a vacant residential dwelling that has been posted 'no occupancy'."
                ),
                "status": "conditional",
                "condition": (
                    "Applies when a vacant residential dwelling has been posted "
                    "'no occupancy' and is being reoccupied."
                ),
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.92,
        )
        return

    if (
        "building_certificate_of_compliance_application.pdf" in url
        or "certificate of compliance application" in title
    ):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="certificate_required_before_occupancy",
            value={
                "summary": (
                    "Warren requires a certificate of compliance before occupancy "
                    "where land, a building, or a structure is erected, altered, "
                    "or changed in use."
                ),
                "status": "conditional",
                "condition": (
                    "Applies to erected, altered, or changed-in-use land/buildings/structures "
                    "before occupancy or use."
                ),
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.92,
        )


def extract_assertions_for_source(
    db: Session,
    *,
    source: PolicySource,
    org_id: Optional[int],
    org_scope: bool = True,
) -> list[PolicyAssertion]:
    """
    Conservative extraction:
    - preserve active/replaced assertions
    - refresh extracted/draft candidates for this exact source + scope
    - inference is based on source metadata only (url/title/publisher/notes/text)
    - attach normalized_category + governance/provenance fields for downstream review
    """
    target_org_id = org_id if org_scope else None
    now = _utcnow()
    created: list[PolicyAssertion] = []
    source_version = _source_version_for_source(db, int(source.id))
    source_version_id = int(source_version.id) if source_version is not None else None

    q = db.query(PolicyAssertion).filter(PolicyAssertion.source_id == source.id)
    if target_org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter(PolicyAssertion.org_id == target_org_id)
    q = q.filter(
        or_(
            PolicyAssertion.review_status == "extracted",
            PolicyAssertion.governance_state == "draft",
            PolicyAssertion.rule_status == "candidate",
        )
    )
    q.delete(synchronize_session=False)
    db.commit()

    url = (source.url or "").lower()
    text = _haystack(source)

    _add_assertion(
        created,
        target_org_id=target_org_id,
        source=source,
        source_version_id=source_version_id,
        now=now,
        rule_key="document_reference",
        value={
            "type": "document_reference",
            "url": source.url,
            "publisher": source.publisher,
            "title": source.title,
            "content_type": source.content_type,
            "retrieved_at": source.retrieved_at.isoformat() if source.retrieved_at else None,
            "sha256": source.content_sha256,
            "notes": source.notes,
        },
        confidence=0.15,
    )

    if "ecfr.gov" in url and "part-982" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="federal_hcv_regulations_anchor",
            value={
                "summary": "HCV regulations live in 24 CFR Part 982 (eCFR).",
                "url": source.url,
            },
            confidence=0.85,
        )

    if "ecfr.gov" in url and "/part-5" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="federal_nspire_anchor",
            value={
                "summary": "HUD program requirements and inspection standards are reflected in 24 CFR Part 5 / NSPIRE structure.",
                "url": source.url,
            },
            confidence=0.75,
        )

    if "federalregister.gov" in url and "nspire" in text:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="federal_notice_anchor",
            value={
                "summary": "Federal Register notice relevant to HCV / NSPIRE implementation timing or standards.",
                "url": source.url,
            },
            confidence=0.60,
        )

    if "legislature.mi.gov" in url:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="mi_statute_anchor",
            value={
                "summary": "Michigan statutory landlord-tenant baseline source.",
                "url": source.url,
            },
            confidence=0.70,
        )

    if "michigan.gov/mshda" in url or "mshda" in text:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="mshda_program_anchor",
            value={
                "summary": "MSHDA program guidance relevant to state housing program administration.",
                "url": source.url,
            },
            confidence=0.70,
        )

    if "detroitmi.gov" in url:
        if _has_any(text, ["landlord-rental", "tenant-rental-property", "rental requirements faq"]):
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Detroit rental property workflow appears to require registration and local compliance handling.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.55,
            )

        if _has_any(text, ["rental-certificate", "certificate of compliance"]):
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="certificate_required_before_occupancy",
                value={
                    "summary": "Detroit certificate of compliance / rental certificate process is relevant before compliant operation.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.55,
            )

        if _has_any(text, ["inspections", "faq", "rental-compliance-map"]):
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Detroit rental workflow includes local inspection program requirements or cadence guidance.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.55,
            )

    if "dearborn.gov" in url and "rental-property-information" in text:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="rental_registration_required",
            value={
                "summary": "Dearborn rental property workflow includes application, inspection, and registration steps.",
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.60,
        )
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="inspection_program_exists",
            value={
                "summary": "Dearborn rental properties are inspected through a city process before compliance is finalized.",
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.60,
        )
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="certificate_required_before_occupancy",
            value={
                "summary": "Dearborn indicates a Certificate of Occupancy is obtained through inspection/compliance flow before rental listing/operation.",
                "url": source.url,
                "scope_hint": "city",
            },
            confidence=0.60,
        )

    if "cityofwarren.org" in url:
        _maybe_add_warren_certificate_rule(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
        )

        if _has_any(text, ["rental-inspections-division", "rental application", "rental license application"]):
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="rental_registration_required",
                value={
                    "summary": "Warren appears to require rental licensing / application / inspections workflow.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="inspection_program_exists",
                value={
                    "summary": "Warren rental workflow includes inspection division oversight.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.60,
            )

        if "property-maintenance-division" in text:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="property_maintenance_enforcement_anchor",
                value={
                    "summary": "Warren property maintenance division is a local enforcement anchor.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.75,
            )

        if "building division" in text:
            _add_assertion(
                created,
                target_org_id=target_org_id,
                source=source,
                source_version_id=source_version_id,
                now=now,
                rule_key="building_division_anchor",
                value={
                    "summary": "Warren building division is a local permit/compliance anchor.",
                    "url": source.url,
                    "scope_hint": "city",
                },
                confidence=0.70,
            )

    if _has_any(text, ["administrative plan", "admin plan"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="pha_admin_plan_anchor",
            value={
                "summary": "PHA administrative plan is an authoritative local program anchor.",
                "url": source.url,
            },
            confidence=0.85,
        )

    if _has_any(text, ["landlord packet"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="pha_landlord_packet_required",
            value={
                "summary": "Landlord packet appears required for voucher landlord onboarding/approval.",
                "url": source.url,
            },
            confidence=0.72,
        )

    if _has_any(text, ["hap contract", "tenancy addendum"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="hap_contract_and_tenancy_addendum_required",
            value={
                "summary": "HAP contract and tenancy addendum appear required for voucher-assisted leasing.",
                "url": source.url,
            },
            confidence=0.78,
        )

    if _has_any(text, ["payment timing", "landlord payment", "payments are made"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="landlord_payment_timing_reference",
            value={
                "summary": "Source includes landlord payment timing or administration references.",
                "url": source.url,
            },
            confidence=0.65,
        )

    if _has_any(text, ["new administrator", "administrator change", "interim administrator"]):
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key="pha_administrator_changed",
            value={
                "summary": "Program administration appears to have changed based on notice/reference.",
                "url": source.url,
            },
            confidence=0.80,
        )

    for row in created:
        db.add(row)
    db.flush()

    _refresh_source_category_metadata(source, created, now)
    db.add(source)
    db.commit()

    return created


def mark_assertions_stale_for_source(
    db: Session,
    *,
    source_id: int,
    reason: str = "source_refreshed",
) -> dict[str, Any]:
    rows = list(
        db.scalars(
            select(PolicyAssertion).where(PolicyAssertion.source_id == int(source_id))
        ).all()
    )

    count = 0
    ids: list[int] = []
    for row in rows:
        row.review_status = "stale"
        row.rule_status = "stale"
        row.change_summary = reason
        row.source_freshness_status = "stale"
        db.add(row)
        count += 1
        if getattr(row, "id", None) is not None:
            ids.append(int(row.id))

    db.commit()
    return {
        "ok": True,
        "source_id": int(source_id),
        "stale_count": count,
        "stale_ids": ids,
        "reason": reason,
    }


# --- tier-two evidence-first final overrides ---


def _tier2_source_evidence_metadata(source: PolicySource, source_version_id: Optional[int]) -> dict[str, Any]:
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    publication_type = str(getattr(source, "publication_type", "") or "").strip().lower()
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    authority_use_type = str(getattr(source, "authority_use_type", "") or "").strip().lower()

    family = "crawl"
    if source_type in {"dataset", "artifact", "manual", "catalog", "program", "feed", "registry", "repo_artifact", "api"}:
        family = source_type
    elif publication_type in {"pdf", "json", "json_api", "dataset"}:
        family = publication_type
    elif authority_tier in {"authoritative_official", "approved_official_supporting"} and authority_use_type in {"binding", "supporting"}:
        family = "official_publication"

    is_primary_evidence = family != "crawl"
    return {
        "evidence_family": family,
        "is_primary_evidence": is_primary_evidence,
        "truth_model": {
            "mode": "evidence_first",
            "crawler_role": "discovery_and_refresh_only",
            "freshness_role": "support_only" if is_primary_evidence else "primary",
        },
        "source_metadata": {
            "source_type": source_type or None,
            "publication_type": publication_type or None,
            "authority_tier": authority_tier or None,
            "authority_use_type": authority_use_type or None,
            "source_version_id": source_version_id,
        },
    }


_tier2_original_extract_assertions_for_source = extract_assertions_for_source


def extract_assertions_for_source(
    db: Session,
    *,
    source: PolicySource,
    org_id: int | None = None,
    org_scope: bool = False,
) -> list[PolicyAssertion]:
    created = list(
        _tier2_original_extract_assertions_for_source(
            db,
            source=source,
            org_id=org_id,
            org_scope=org_scope,
        )
    )
    if not created:
        return created

    source_version_id = None
    try:
        latest_version = _source_version_for_source(db, int(getattr(source, "id", 0) or 0))
        source_version_id = int(latest_version.id) if latest_version is not None else None
    except Exception:
        source_version_id = None

    evidence_metadata = _tier2_source_evidence_metadata(source, source_version_id)
    now = _utcnow()

    touched = False
    for row in created:
        value_json = _json_loads_dict(getattr(row, "value_json", None))
        provenance_json = _json_loads_dict(getattr(row, "rule_provenance_json", None))
        citation_json = _json_loads_dict(getattr(row, "citation_json", None))

        value_json.setdefault("evidence_metadata", evidence_metadata)
        value_json.setdefault("extraction_strategy", "document_or_dataset_first")
        value_json.setdefault("freshness_role", evidence_metadata["truth_model"]["freshness_role"])

        provenance_json.setdefault("evidence_family", evidence_metadata.get("evidence_family"))
        provenance_json.setdefault("is_primary_evidence", evidence_metadata.get("is_primary_evidence"))
        provenance_json.setdefault("extraction_version", "tier2_evidence_first")
        provenance_json.setdefault("extracted_at", now.isoformat())
        provenance_json.setdefault("source_version_id", source_version_id)

        citation_json.setdefault("source_kind", evidence_metadata.get("evidence_family"))
        citation_json.setdefault("is_primary_evidence", evidence_metadata.get("is_primary_evidence"))

        try:
            row.value_json = _dumps(value_json)
            row.rule_provenance_json = _dumps(provenance_json)
            row.citation_json = _dumps(citation_json)
            touched = True
        except Exception:
            continue

    if touched:
        try:
            db.add_all(created)
            db.commit()
            for row in created:
                db.refresh(row)
        except Exception:
            db.rollback()

    return created


# --- coverage completion overrides ---

_COVERAGE_RULE_FAMILY_MAP = {
    "lead_disclosure_required": "lead",
    "lead_hazard_assessment_required": "lead",
    "source_of_income_protection": "source_of_income",
    "permit_required": "permits",
    "local_documents_required": "documents",
    "local_contact_required": "contacts",
    "rental_license_required": "rental_license",
    "fee_schedule_reference": "fees",
    "program_overlay_requirement": "program_overlay",
}

_COVERAGE_CATEGORY_MAP = {
    "lead_disclosure_required": "lead",
    "lead_hazard_assessment_required": "lead",
    "source_of_income_protection": "source_of_income",
    "permit_required": "permits",
    "local_documents_required": "documents",
    "local_contact_required": "contacts",
    "rental_license_required": "rental_license",
    "fee_schedule_reference": "fees",
    "program_overlay_requirement": "program_overlay",
}

_COVERAGE_PRIORITY_MAP = {
    "lead_disclosure_required": 12,
    "lead_hazard_assessment_required": 12,
    "source_of_income_protection": 14,
    "permit_required": 12,
    "local_documents_required": 18,
    "local_contact_required": 18,
    "rental_license_required": 10,
    "fee_schedule_reference": 20,
    "program_overlay_requirement": 14,
}

_original_rule_family_for = _rule_family_for
_original_normalized_category_for = _normalized_category_for
_original_priority_for = _priority_for
_original_extract_assertions_for_source = extract_assertions_for_source


def _rule_family_for(rule_key: str) -> str:
    return _COVERAGE_RULE_FAMILY_MAP.get(rule_key, _original_rule_family_for(rule_key))


def _normalized_category_for(rule_key: str) -> str | None:
    mapped = _COVERAGE_CATEGORY_MAP.get(rule_key)
    if mapped is not None:
        return normalize_category(mapped)
    return _original_normalized_category_for(rule_key)


def _priority_for(rule_key: str) -> int:
    if rule_key in _COVERAGE_PRIORITY_MAP:
        return int(_COVERAGE_PRIORITY_MAP[rule_key])
    return _original_priority_for(rule_key)


def _add_coverage_completion_assertions(
    created: list[PolicyAssertion],
    *,
    db: Session,
    source: PolicySource,
    target_org_id: Optional[int],
    source_version_id: Optional[int],
    now: datetime,
) -> None:
    url = (source.url or "").lower()
    text = _haystack(source)
    title = (source.title or "").lower()
    publisher = (source.publisher or "").lower()

    def add(rule_key: str, summary: str, confidence: float = 0.74, **extra: Any) -> None:
        _add_assertion(
            created,
            target_org_id=target_org_id,
            source=source,
            source_version_id=source_version_id,
            now=now,
            rule_key=rule_key,
            value={"summary": summary, "url": source.url, **extra},
            confidence=confidence,
        )

    # Lead / LBP from NSPIRE PDFs, HUD, Michigan, and local references.
    if _has_any(text, ["lead", "lbp", "lead-based paint", "lead paint", "lead hazard", "clearance", "disclosure"]):
        if _has_any(text, ["clearance", "risk assessment", "hazard", "inspection", "visual assess", "lbp"]):
            add("lead_hazard_assessment_required", "Lead or LBP hazard controls are referenced by this evidence source.", 0.82, scope_hint="state_or_city")
        if _has_any(text, ["disclosure", "pamphlet", "affidavit", "paperwork"]):
            add("lead_disclosure_required", "Lead disclosure or lead-related paperwork is referenced by this evidence source.", 0.80, scope_hint="state_or_city")

    # SOI protections from state / program language.
    if _has_any(text, ["source of income", "voucher discrimination", "income source", "housing choice voucher holders", "lawful source of income"]):
        add("source_of_income_protection", "Source-of-income or voucher-holder screening protections are referenced by this evidence source.", 0.78, scope_hint="state_or_program")

    # Local permits, documents, fees, contacts, rental license.
    if "dearborn.gov" in url or "city of dearborn" in publisher:
        if _has_any(text, ["application", "submit", "application with payment", "step 1", "payment"]):
            add("local_documents_required", "Dearborn rental workflow references required application or supporting documents.", 0.76, scope_hint="city")
            add("fee_schedule_reference", "Dearborn rental workflow references application or inspection payment requirements.", 0.73, scope_hint="city")
        if _has_any(text, ["certificate of occupancy", "re-occupancy", "occupancy", "certificate of compliance"]):
            add("rental_license_required", "Dearborn occupancy/compliance workflow implies local rental licensure or certificate requirements before operation.", 0.74, scope_hint="city")
        if _has_any(text, ["building", "division", "department", "contact", "phone", "email", "inspection desk"]):
            add("local_contact_required", "Dearborn rental workflow exposes responsible office or department contact paths.", 0.70, scope_hint="city")
        if _has_any(text, ["permit", "permit required", "building permit", "altered", "changed in use"]):
            add("permit_required", "Dearborn materials reference local permit or change-of-use requirements relevant to rental readiness.", 0.72, scope_hint="city")

    # General municipal fee/contact/license inference for official pages.
    if url.endswith('.gov') or '.gov/' in url or publisher.endswith('government') or 'city of' in publisher or 'county of' in publisher:
        if _has_any(text, ["fee", "payment", "$", "schedule", "application fee", "inspection fee", "registration fee"]):
            add("fee_schedule_reference", "Official source references fee or payment requirements relevant to compliance workflow.", 0.68, scope_hint="local")
        if _has_any(text, ["contact", "phone", "email", "department", "division", "office"]):
            add("local_contact_required", "Official source references responsible office contact information for compliance workflow.", 0.66, scope_hint="local")
        if _has_any(text, ["license", "licensing", "certificate", "registration certificate", "rental certificate"]):
            add("rental_license_required", "Official source references local rental licensing or certification requirements.", 0.69, scope_hint="local")
        if _has_any(text, ["permit", "permit required", "building permit", "electrical permit", "mechanical permit", "plumbing permit"]):
            add("permit_required", "Official source references permit requirements relevant to rental readiness or repairs.", 0.68, scope_hint="local")

    # Program overlay from HCV / NSPIRE / HAP packet style evidence.
    if _has_any(text, ["hcv", "voucher", "housing choice voucher", "addendum", "hap", "nspire", "landlord packet", "admin plan"]):
        add("program_overlay_requirement", "Program-specific HCV/NSPIRE overlay requirements are referenced by this evidence source.", 0.77, scope_hint="program")
        add("local_documents_required", "Program workflow references required packet, addendum, or supporting documents.", 0.70, scope_hint="program")
        add("local_contact_required", "Program workflow references responsible contact or program office guidance.", 0.66, scope_hint="program")


def extract_assertions_for_source(
    db: Session,
    *,
    source: PolicySource,
    org_id: Optional[int],
    org_scope: bool = True,
) -> list[PolicyAssertion]:
    created = list(_original_extract_assertions_for_source(db, source=source, org_id=org_id, org_scope=org_scope))
    target_org_id = org_id if org_scope else None
    now = _utcnow()
    source_version = _source_version_for_source(db, int(source.id))
    source_version_id = int(source_version.id) if source_version is not None else None
    before = len(created)
    try:
        _add_coverage_completion_assertions(
            created,
            db=db,
            source=source,
            target_org_id=target_org_id,
            source_version_id=source_version_id,
            now=now,
        )
        extra = created[before:]
        if extra:
            try:
                _refresh_source_category_metadata(source, created, now)
            except Exception:
                pass
            db.add(source)
            db.add_all(extra)
            db.commit()
            for row in extra:
                try:
                    db.refresh(row)
                except Exception:
                    pass
    except Exception:
        db.rollback()
    return created


# --- final gap completion overrides ---
_COVERAGE_FINAL_EXTRA_RULE_FAMILIES = {
    'lead_hazard_assessment_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
}

try:
    _tier_final_original_rule_family_for = _rule_family_for
except NameError:
    _tier_final_original_rule_family_for = None

if _tier_final_original_rule_family_for is not None:
    def _rule_family_for(rule_key: str) -> str:
        base = _tier_final_original_rule_family_for(rule_key)
        return _COVERAGE_FINAL_EXTRA_RULE_FAMILIES.get(rule_key, base)

try:
    _tier_final_original_normalized_category_for = _normalized_category_for
except NameError:
    _tier_final_original_normalized_category_for = None

if _tier_final_original_normalized_category_for is not None:
    def _normalized_category_for(rule_key: str) -> str | None:
        mapped = {
            'lead_hazard_assessment_required': 'lead',
            'permit_required': 'permits',
            'local_contact_required': 'contacts',
            'local_documents_required': 'documents',
            'fee_schedule_reference': 'fees',
            'program_overlay_requirement': 'program_overlay',
            'source_of_income_protection': 'source_of_income',
            'rental_license_required': 'rental_license',
        }
        if rule_key in mapped:
            return normalize_category(mapped[rule_key])
        return _tier_final_original_normalized_category_for(rule_key)

try:
    _tier_final_original_extract_assertions_for_source = extract_assertions_for_source
except NameError:
    _tier_final_original_extract_assertions_for_source = None

if _tier_final_original_extract_assertions_for_source is not None:
    def extract_assertions_for_source(db: Session, *, source_id: int, org_id: int | None = None) -> list[PolicyAssertion]:
        created = list(_tier_final_original_extract_assertions_for_source(db, source_id=source_id, org_id=org_id))
        try:
            source = db.get(PolicySource, int(source_id))
            if source is None:
                return created
            text = _haystack(source)
            source_version = _source_version_for_source(db, int(source_id))
            source_version_id = int(getattr(source_version, 'id', 0) or 0) or None
            now = _utcnow()
            synthetic = []
            candidates = []
            if _has_any(text, ['lead hazard', 'lbp', 'lead-based paint', 'lead paint', 'paint hazard']):
                candidates.append(('lead_hazard_assessment_required', {'summary':'Lead hazard / LBP requirement identified from official or artifact-backed source.'}, 0.82))
            if _has_any(text, ['source of income', 'income discrimination', 'voucher discrimination', 'lawful source of income']):
                candidates.append(('source_of_income_protection', {'summary':'Source-of-income protection identified from official source.'}, 0.72))
            if _has_any(text, ['permit required', 'permits required', 'building permit', 'electrical permit', 'mechanical permit', 'plumbing permit']):
                candidates.append(('permit_required', {'summary':'Permit requirement identified from official/local source.'}, 0.77))
            if _has_any(text, ['contact us', 'phone', 'email', 'contact information', 'inspection department', 'building department']):
                candidates.append(('local_contact_required', {'summary':'Local contact / responsible office reference identified.'}, 0.66))
            if _has_any(text, ['documents required', 'required documents', 'supporting documents', 'documentation required', 'submit documentation']):
                candidates.append(('local_documents_required', {'summary':'Required document workflow identified.'}, 0.68))
            if _has_any(text, ['fee schedule', 'application fee', 'inspection fee', 'registration fee', 'payment amount', 'submit an application with payment']):
                candidates.append(('fee_schedule_reference', {'summary':'Fee schedule or payment requirement identified.'}, 0.74))
            if _has_any(text, ['overlay requirement', 'voucher packet', 'tenancy addendum', 'hap contract', 'nspire compliance date', 'pbv', 'mod rehab']):
                candidates.append(('program_overlay_requirement', {'summary':'Program overlay requirement identified from HCV/NSPIRE source.'}, 0.73))
            if _has_any(text, ['rental license', 'license required', 'licensing', 'rental property license']):
                candidates.append(('rental_license_required', {'summary':'Rental license requirement identified.'}, 0.76))
            for rule_key, value, confidence in candidates:
                if _already_added(created, rule_key):
                    continue
                payload = _candidate_payload(source=source, source_version_id=source_version_id, rule_key=rule_key, value=value, confidence=confidence)
                row = PolicyAssertion(**payload)
                synthetic.append(row)
            if synthetic:
                db.add_all(synthetic)
                db.commit()
                for row in synthetic:
                    try:
                        db.refresh(row)
                    except Exception:
                        pass
                created.extend(synthetic)
                try:
                    _refresh_source_category_metadata(source, created, now)
                    db.add(source)
                    db.commit()
                except Exception:
                    db.rollback()
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
        return created


# --- final resolution overrides ---
_FINAL_EXTRACTOR_CATEGORY_HINTS = {
    'source_of_income_protection': ['source of income', 'income discrimination', 'voucher discrimination', 'lawful source of income', 'non-discrimination against voucher'],
    'rental_license_required': ['rental license', 'license required', 'licensing', 'rental property license'],
    'permit_required': ['permit required', 'permits required', 'building permit', 'electrical permit', 'mechanical permit', 'plumbing permit'],
    'local_contact_required': ['contact us', 'phone', 'email', 'contact information', 'inspection department', 'building department'],
    'local_documents_required': ['documents required', 'required documents', 'supporting documents', 'documentation required', 'submit documentation', 'application packet'],
    'fee_schedule_reference': ['fee schedule', 'application fee', 'inspection fee', 'registration fee', 'payment amount', 'application with payment'],
    'program_overlay_requirement': ['overlay requirement', 'voucher packet', 'tenancy addendum', 'hap contract', 'nspire compliance date', 'pbv', 'mod rehab'],
    'lead_hazard_assessment_required': ['lead hazard', 'lbp', 'lead-based paint', 'lead paint', 'paint hazard'],
}

try:
    _final_resolution_original_extract_assertions_for_source2 = extract_assertions_for_source
except NameError:
    _final_resolution_original_extract_assertions_for_source2 = None

if _final_resolution_original_extract_assertions_for_source2 is not None:
    def extract_assertions_for_source(db: Session, *, source: PolicySource | None = None, source_id: int | None = None, org_id: Optional[int] = None, org_scope: bool = True) -> list[PolicyAssertion]:
        if source is None and source_id is not None:
            source = db.get(PolicySource, int(source_id))
        if source is None:
            return []
        # Support both call signatures used across your codebase.
        try:
            created = list(_final_resolution_original_extract_assertions_for_source2(db, source=source, org_id=org_id, org_scope=org_scope))
        except TypeError:
            created = list(_final_resolution_original_extract_assertions_for_source2(db, source_id=int(source.id), org_id=org_id))
        text = _haystack(source)
        source_version = _source_version_for_source(db, int(source.id))
        source_version_id = int(getattr(source_version, 'id', 0) or 0) or None
        now = _utcnow()
        synthetic: list[PolicyAssertion] = []
        for rule_key, pats in _FINAL_EXTRACTOR_CATEGORY_HINTS.items():
            if not _has_any(text, pats):
                continue
            if _already_added(created + synthetic, rule_key):
                continue
            summary = rule_key.replace('_', ' ')
            payload = _candidate_payload(
                source=source,
                source_version_id=source_version_id,
                rule_key=rule_key,
                value={'summary': f'{summary} identified from official or artifact-backed source.'},
                confidence=0.76 if rule_key in {'source_of_income_protection', 'rental_license_required'} else 0.74,
            )
            synthetic.append(PolicyAssertion(**payload))
        if synthetic:
            try:
                db.add_all(synthetic)
                db.commit()
                for row in synthetic:
                    try:
                        db.refresh(row)
                    except Exception:
                        pass
                created.extend(synthetic)
                try:
                    _refresh_source_category_metadata(source, created, now)
                    db.add(source)
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception:
                db.rollback()
        return created

# --- append-only final resolution overrides ---
_FINAL_EXTRACTOR_RULE_FAMILIES = {
    'lead_hazard_assessment_required': 'lead',
    'permit_required': 'permits',
    'local_contact_required': 'contacts',
    'local_documents_required': 'documents',
    'fee_schedule_reference': 'fees',
    'program_overlay_requirement': 'program_overlay',
    'source_of_income_protection': 'source_of_income',
    'rental_license_required': 'rental_license',
}

try:
    _final_extractor_original_rule_family_for = _rule_family_for
except NameError:
    _final_extractor_original_rule_family_for = None

if _final_extractor_original_rule_family_for is not None:
    def _rule_family_for(rule_key: str) -> str:
        return _FINAL_EXTRACTOR_RULE_FAMILIES.get(rule_key, _final_extractor_original_rule_family_for(rule_key))

try:
    _final_extractor_original_normalized_category_for = _normalized_category_for
except NameError:
    _final_extractor_original_normalized_category_for = None

if _final_extractor_original_normalized_category_for is not None:
    def _normalized_category_for(rule_key: str) -> str | None:
        mapped = {
            'lead_hazard_assessment_required': 'lead',
            'permit_required': 'permits',
            'local_contact_required': 'contacts',
            'local_documents_required': 'documents',
            'fee_schedule_reference': 'fees',
            'program_overlay_requirement': 'program_overlay',
            'source_of_income_protection': 'source_of_income',
            'rental_license_required': 'rental_license',
        }
        if rule_key in mapped:
            return normalize_category(mapped[rule_key])
        return _final_extractor_original_normalized_category_for(rule_key)


_FINAL_EXTRACTOR_CATEGORY_HINTS = {
    'source_of_income_protection': ['source of income', 'income discrimination', 'voucher discrimination', 'lawful source of income', 'non-discrimination against voucher'],
    'rental_license_required': ['rental license', 'license required', 'licensing', 'rental property license', 'registration certificate', 'rental certificate'],
    'permit_required': ['permit required', 'permits required', 'building permit', 'electrical permit', 'mechanical permit', 'plumbing permit'],
    'local_contact_required': ['contact us', 'phone', 'email', 'contact information', 'inspection department', 'building department', 'division', 'office'],
    'local_documents_required': ['documents required', 'required documents', 'supporting documents', 'documentation required', 'submit documentation', 'packet', 'addendum'],
    'fee_schedule_reference': ['fee schedule', 'application fee', 'inspection fee', 'registration fee', 'payment amount', 'submit an application with payment'],
    'program_overlay_requirement': ['overlay requirement', 'voucher packet', 'tenancy addendum', 'hap contract', 'nspire compliance date', 'pbv', 'mod rehab', 'hcv', 'admin plan'],
    'lead_hazard_assessment_required': ['lead hazard', 'lbp', 'lead-based paint', 'lead paint', 'paint hazard'],
}


def _final_extractor_additive_extract(
    db: Session,
    *,
    created: list[PolicyAssertion],
    source: PolicySource,
    org_id: Optional[int],
    org_scope: bool,
) -> list[PolicyAssertion]:
    source_id = int(getattr(source, 'id', 0) or 0)
    if not source_id:
        return created

    text = _haystack(source)
    source_version = _source_version_for_source(db, source_id)
    source_version_id = int(getattr(source_version, 'id', 0) or 0) or None
    now = _utcnow()

    candidates = [
        ('lead_hazard_assessment_required', {'summary': 'Lead hazard / LBP requirement identified from official or artifact-backed source.'}, 0.82),
        ('source_of_income_protection', {'summary': 'Source-of-income protection identified from current source text.'}, 0.72),
        ('permit_required', {'summary': 'Permit requirement identified from current source text.'}, 0.77),
        ('local_contact_required', {'summary': 'Local contact / responsible office reference identified.'}, 0.66),
        ('local_documents_required', {'summary': 'Required document workflow identified.'}, 0.68),
        ('fee_schedule_reference', {'summary': 'Fee schedule or payment requirement identified.'}, 0.74),
        ('program_overlay_requirement', {'summary': 'Program overlay requirement identified from HCV/NSPIRE/HAP source.'}, 0.73),
        ('rental_license_required', {'summary': 'Rental license requirement identified from current source text.'}, 0.76),
    ]

    synthetic: list[PolicyAssertion] = []
    for rule_key, value, confidence in candidates:
        hints = _FINAL_EXTRACTOR_CATEGORY_HINTS.get(rule_key, [])
        if hints and not _has_any(text, hints):
            continue
        if _already_added(created, rule_key):
            continue
        payload = _candidate_payload(
            source=source,
            source_version_id=source_version_id,
            rule_key=rule_key,
            value=value,
            confidence=confidence,
        )
        synthetic.append(PolicyAssertion(**payload))

    if synthetic:
        db.add_all(synthetic)
        db.commit()
        for row in synthetic:
            try:
                db.refresh(row)
            except Exception:
                pass
        created.extend(synthetic)
        try:
            _refresh_source_category_metadata(source, created, now)
            db.add(source)
            db.commit()
        except Exception:
            db.rollback()

    return created


try:
    _final_extractor_original_extract_assertions_for_source = extract_assertions_for_source
except NameError:
    _final_extractor_original_extract_assertions_for_source = None

if _final_extractor_original_extract_assertions_for_source is not None:
    def extract_assertions_for_source(db: Session, *args, **kwargs) -> list[PolicyAssertion]:
        created = list(_final_extractor_original_extract_assertions_for_source(db, *args, **kwargs))

        source = kwargs.get('source')
        if source is None and 'source_id' in kwargs:
            try:
                source = db.get(PolicySource, int(kwargs['source_id']))
            except Exception:
                source = None
        if source is None and args:
            for arg in args:
                if isinstance(arg, PolicySource):
                    source = arg
                    break

        org_id = kwargs.get('org_id')
        org_scope = bool(kwargs.get('org_scope', True))

        if source is None:
            return created

        try:
            return _final_extractor_additive_extract(
                db,
                created=created,
                source=source,
                org_id=org_id,
                org_scope=org_scope,
            )
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            return created
