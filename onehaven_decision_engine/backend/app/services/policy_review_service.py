# backend/app/services/policy_review_service.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category
from app.policy_models import PolicyAssertion, PolicySource
from app.services.policy_rule_normalizer import (
    assertion_fingerprint,
    candidate_to_update_dict,
    normalize_rule_candidate,
)

AUTO_VERIFY_RULE_KEYS = {
    "rental_registration_required",
    "inspection_program_exists",
    "certificate_required_before_occupancy",
    "property_maintenance_enforcement_anchor",
    "building_safety_division_anchor",
    "building_division_anchor",
    "pha_admin_plan_anchor",
    "pha_administrator_changed",
    "pha_landlord_packet_required",
    "hap_contract_and_tenancy_addendum_required",
    "federal_hcv_regulations_anchor",
    "federal_nspire_anchor",
    "federal_notice_anchor",
    "mi_statute_anchor",
    "mshda_program_anchor",
    "landlord_payment_timing_reference",
}


def _norm_state(s: Optional[str]) -> str:
    return (s or "MI").strip().upper()


def _norm_lower(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip().lower()
    return v or None


def _norm_text(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v or None


def _market_label(state: str, county: Optional[str], city: Optional[str], pha_name: Optional[str]) -> str:
    if city:
        if county:
            return f"{city.title()}, {county.title()} County, {state}"
        return f"{city.title()}, {state}"
    if pha_name:
        return f"{pha_name}, {state}"
    if county:
        return f"{county.title()} County, {state}"
    return state


def _source_is_authoritative(src: Optional[PolicySource]) -> bool:
    if src is None:
        return False

    if bool(getattr(src, "is_authoritative", False)):
        return True

    url = (src.url or "").lower()
    publisher = (src.publisher or "").lower()

    try:
        status_ok = src.http_status is not None and 200 <= int(src.http_status) < 400
    except Exception:
        status_ok = False

    if not status_ok:
        return False

    authoritative_domains = (
        "ecfr.gov",
        "federalregister.gov",
        "hud.gov",
        "legislature.mi.gov",
        "michigan.gov",
        ".gov",
        "dhcmi.org",
        "cityofwarren.org",
        "cityofwestland.com",
        "cityoftaylor.com",
        "ci.taylor.mi.us",
        "livonia.gov",
        "pontiac.mi.us",
        "cityofsouthfield.com",
        "dearborn.gov",
        "detroitmi.gov",
    )

    if any(domain in url for domain in authoritative_domains):
        return True

    if any(
        phrase in publisher
        for phrase in (
            "city of",
            "michigan legislature",
            "mshda",
            "hud",
            "federal register",
            "detroit housing commission",
            "housing commission",
            "michigan courts",
        )
    ):
        return True

    return False


def _category_for_verified(a: PolicyAssertion) -> str | None:
    current = normalize_category(getattr(a, "rule_category", None) or getattr(a, "normalized_category", None))
    if current:
        return current

    by_rule = {
        "rental_registration_required": "registration",
        "inspection_program_exists": "inspection",
        "certificate_required_before_occupancy": "occupancy",
        "property_maintenance_enforcement_anchor": "safety",
        "building_safety_division_anchor": "safety",
        "building_division_anchor": "permits",
        "pha_admin_plan_anchor": "section8",
        "pha_administrator_changed": "section8",
        "pha_landlord_packet_required": "section8",
        "hap_contract_and_tenancy_addendum_required": "section8",
        "federal_hcv_regulations_anchor": "section8",
        "federal_nspire_anchor": "inspection",
        "federal_notice_anchor": "section8",
        "mi_statute_anchor": "safety",
        "mshda_program_anchor": "section8",
        "landlord_payment_timing_reference": "section8",
    }
    return normalize_category(by_rule.get(a.rule_key))


def _confidence_for(rule_key: str, src: Optional[PolicySource]) -> float:
    url = (src.url or "").lower() if src else ""

    if rule_key in {"federal_hcv_regulations_anchor", "federal_nspire_anchor"}:
        return 0.97
    if rule_key == "mi_statute_anchor":
        return 0.96
    if rule_key in {
        "rental_registration_required",
        "inspection_program_exists",
        "certificate_required_before_occupancy",
    }:
        if ".pdf" in url:
            return 0.94
        return 0.91
    if rule_key in {
        "property_maintenance_enforcement_anchor",
        "building_safety_division_anchor",
        "building_division_anchor",
        "mshda_program_anchor",
        "pha_admin_plan_anchor",
        "pha_administrator_changed",
        "pha_landlord_packet_required",
        "hap_contract_and_tenancy_addendum_required",
        "landlord_payment_timing_reference",
        "federal_notice_anchor",
    }:
        return 0.90

    return 0.88


def _coverage_status_for_verified(a: PolicyAssertion) -> str:
    if (a.rule_key or "").endswith("_anchor"):
        return "verified"
    if bool(getattr(a, "blocking", False)):
        return "covered"
    return "covered"


def _market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    q = db.query(PolicyAssertion).filter(PolicyAssertion.state == st)

    if org_id is None:
        q = q.filter(PolicyAssertion.org_id.is_(None))
    else:
        q = q.filter((PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None)))

    rows = q.all()
    out: list[PolicyAssertion] = []
    for a in rows:
        if a.county is not None and a.county != cnty:
            continue
        if a.city is not None and a.city != cty:
            continue
        if a.pha_name is not None and a.pha_name != pha:
            continue
        out.append(a)
    return out


def _source_map_for_assertions(
    db: Session,
    assertions: list[PolicyAssertion],
) -> dict[int, PolicySource]:
    source_ids = sorted({a.source_id for a in assertions if a.source_id is not None})
    if not source_ids:
        return {}
    rows = db.query(PolicySource).filter(PolicySource.id.in_(source_ids)).all()
    return {r.id: r for r in rows}


def _active_assertions_for_version_group(
    rows: list[PolicyAssertion],
    *,
    version_group: str,
) -> list[PolicyAssertion]:
    out: list[PolicyAssertion] = []
    for row in rows:
        if (row.version_group or "") != version_group:
            continue
        if (row.governance_state or "").lower() != "active":
            continue
        if (row.rule_status or "").lower() not in {"active", "candidate", "approved"}:
            continue
        if row.superseded_by_assertion_id is not None:
            continue
        out.append(row)
    return out


def _set_phase2_fields_from_candidate(assertion: PolicyAssertion, source: Optional[PolicySource]) -> bool:
    hint = f"{getattr(source, 'title', '')} {getattr(source, 'publisher', '')}"
    raw_text = getattr(assertion, "raw_excerpt", None) or getattr(assertion, "review_notes", None) or getattr(assertion, "value_json", None) or getattr(source, "title", None) or assertion.rule_key
    candidate = normalize_rule_candidate(
        str(raw_text or assertion.rule_key),
        hint=hint,
        source_url=getattr(source, "url", None),
        property_type=getattr(assertion, "property_type", None),
        normalized_version=str(getattr(assertion, "normalized_version", None) or "v2"),
    )

    changed = False

    if candidate is not None:
        payload = candidate_to_update_dict(candidate)
        for key, value in payload.items():
            current = getattr(assertion, key, None)
            if current != value and value is not None:
                setattr(assertion, key, value)
                changed = True

    if not getattr(assertion, "source_citation", None) and source is not None:
        assertion.source_citation = getattr(source, "url", None)
        changed = True

    if not getattr(assertion, "raw_excerpt", None):
        source_title = getattr(source, "title", None) if source is not None else None
        assertion.raw_excerpt = source_title or assertion.rule_key
        changed = True

    if not getattr(assertion, "version_group", None):
        source_level = getattr(assertion, "source_level", None) or "local"
        property_type = getattr(assertion, "property_type", None) or "all"
        assertion.version_group = f"{source_level}:{assertion.rule_key}:{property_type}"
        changed = True

    if not getattr(assertion, "normalized_version", None):
        assertion.normalized_version = "v2"
        changed = True

    if not getattr(assertion, "rule_family", None):
        assertion.rule_family = assertion.rule_key
        changed = True

    if not getattr(assertion, "rule_category", None):
        assertion.rule_category = _category_for_verified(assertion)
        changed = True

    if not getattr(assertion, "source_level", None):
        assertion.source_level = "local"
        changed = True

    if getattr(assertion, "required", None) is None:
        assertion.required = True
        changed = True

    if getattr(assertion, "blocking", None) is None:
        assertion.blocking = False
        changed = True

    return changed


def normalize_market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
) -> dict[str, Any]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    src_map = _source_map_for_assertions(db, rows)

    normalized_ids: list[int] = []
    for row in rows:
        src = src_map.get(row.source_id) if row.source_id is not None else None
        if _set_phase2_fields_from_candidate(row, src):
            normalized_ids.append(int(row.id))

    db.commit()
    return {"normalized_count": len(normalized_ids), "normalized_ids": normalized_ids}


def detect_assertion_diff(
    active_assertions: list[PolicyAssertion],
    candidate_assertion: PolicyAssertion,
) -> dict[str, Any]:
    candidate_fp = assertion_fingerprint(candidate_assertion)
    if not active_assertions:
        return {
            "status": "new_rule",
            "changed": True,
            "candidate_fingerprint": candidate_fp,
            "active_fingerprints": [],
            "matched_active_assertion_id": None,
        }

    active_fps = {assertion_fingerprint(row): row for row in active_assertions}
    if candidate_fp in active_fps:
        return {
            "status": "unchanged",
            "changed": False,
            "candidate_fingerprint": candidate_fp,
            "active_fingerprints": list(active_fps.keys()),
            "matched_active_assertion_id": int(active_fps[candidate_fp].id),
        }

    return {
        "status": "changed",
        "changed": True,
        "candidate_fingerprint": candidate_fp,
        "active_fingerprints": list(active_fps.keys()),
        "matched_active_assertion_id": None,
    }


def apply_governance_lifecycle(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: Optional[int] = None,
) -> dict[str, Any]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    grouped: dict[str, list[PolicyAssertion]] = {}
    for row in rows:
        key = row.version_group or f"{getattr(row, 'source_level', 'local')}:{row.rule_key}:{getattr(row, 'property_type', None) or 'all'}"
        grouped.setdefault(key, []).append(row)

    activated_ids: list[int] = []
    approved_ids: list[int] = []
    replaced_ids: list[int] = []
    unchanged_ids: list[int] = []
    now = datetime.utcnow()

    for _, group in grouped.items():
        actives = _active_assertions_for_version_group(group, version_group=group[0].version_group or "")
        candidates = [
            row for row in group
            if (row.governance_state or "").lower() in {"draft", "approved"}
            and (row.review_status or "").lower() in {"verified", "extracted", "needs_recheck", "approved"}
        ]
        if not candidates:
            continue

        ordered_candidates = sorted(
            candidates,
            key=lambda row: (
                float(getattr(row, "confidence", 0.0) or 0.0),
                int(getattr(row, "version_number", 1) or 1),
                int(getattr(row, "id", 0) or 0),
            ),
            reverse=True,
        )
        winner = ordered_candidates[0]
        diff = detect_assertion_diff(actives, winner)

        winner.reviewed_by_user_id = reviewer_user_id
        winner.reviewed_at = now

        if diff["status"] == "unchanged":
            winner.governance_state = "approved"
            winner.rule_status = "active"
            winner.review_status = "verified"
            approved_ids.append(int(winner.id))
            unchanged_ids.append(int(winner.id))

            for active in actives:
                if active.id != diff["matched_active_assertion_id"]:
                    active.governance_state = "replaced"
                    active.rule_status = "superseded"
                    active.review_status = "superseded"
                    active.superseded_by_assertion_id = diff["matched_active_assertion_id"]
                    active.reviewed_by_user_id = reviewer_user_id
                    active.reviewed_at = now
                    replaced_ids.append(int(active.id))
            continue

        winner.governance_state = "active"
        winner.rule_status = "active"
        winner.review_status = "verified"
        winner.version_number = max(
            [int(getattr(row, "version_number", 1) or 1) for row in group] + [0]
        )
        activated_ids.append(int(winner.id))

        for active in actives:
            if active.id == winner.id:
                continue
            active.governance_state = "replaced"
            active.rule_status = "superseded"
            active.review_status = "superseded"
            active.superseded_by_assertion_id = winner.id
            active.reviewed_by_user_id = reviewer_user_id
            active.reviewed_at = now
            replaced_ids.append(int(active.id))

        for draft in ordered_candidates[1:]:
            if draft.id == winner.id:
                continue
            if (draft.governance_state or "").lower() == "active":
                continue
            draft.governance_state = "replaced"
            draft.rule_status = "superseded"
            draft.review_status = "superseded"
            draft.superseded_by_assertion_id = winner.id
            draft.reviewed_by_user_id = reviewer_user_id
            draft.reviewed_at = now
            replaced_ids.append(int(draft.id))

    db.commit()
    return {
        "activated_count": len(activated_ids),
        "activated_ids": activated_ids,
        "approved_count": len(approved_ids),
        "approved_ids": approved_ids,
        "replaced_count": len(replaced_ids),
        "replaced_ids": replaced_ids,
        "unchanged_count": len(unchanged_ids),
        "unchanged_ids": unchanged_ids,
    }


def auto_verify_market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: Optional[int] = None,
) -> dict:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    market_label = _market_label(st, cnty, cty, pha)

    rows = _market_assertions(
        db,
        org_id=org_id,
        state=st,
        county=cnty,
        city=cty,
        pha_name=pha,
    )
    src_map = _source_map_for_assertions(db, rows)

    updated_ids: list[int] = []
    now = datetime.utcnow()

    for a in rows:
        src = src_map.get(a.source_id) if a.source_id is not None else None
        _set_phase2_fields_from_candidate(a, src)

        if a.rule_key not in AUTO_VERIFY_RULE_KEYS:
            continue
        if (a.review_status or "").lower() not in {"extracted", "needs_recheck", "approved", "verified"}:
            continue
        if not _source_is_authoritative(src):
            continue

        a.review_status = "verified"
        a.governance_state = "approved"
        a.rule_status = "candidate"
        a.confidence = max(float(a.confidence or 0.0), _confidence_for(a.rule_key, src))
        a.verification_reason = "official_source_review"
        a.reviewed_by_user_id = reviewer_user_id
        a.reviewed_at = now
        a.stale_after = None
        a.normalized_category = _category_for_verified(a)
        a.rule_category = a.rule_category or a.normalized_category
        a.coverage_status = _coverage_status_for_verified(a)
        a.source_freshness_status = getattr(src, "freshness_status", None) if src else None

        auto_note = (
            f"Auto-verified for {market_label} from authoritative source: {src.title or src.url}"
            if src
            else f"Auto-verified for {market_label} from authoritative source"
        )
        existing_note = (a.review_notes or "").strip()
        a.review_notes = auto_note if not existing_note else existing_note
        if not a.source_citation and src is not None:
            a.source_citation = src.url

        updated_ids.append(a.id)

    db.commit()

    return {
        "updated_count": len(updated_ids),
        "updated_ids": updated_ids,
    }


def supersede_replaced_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: Optional[int] = None,
) -> dict:
    lifecycle = apply_governance_lifecycle(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
    )
    return {
        "superseded_count": int(lifecycle.get("replaced_count", 0)),
        "superseded_ids": lifecycle.get("replaced_ids", []),
        "activated_count": int(lifecycle.get("activated_count", 0)),
        "activated_ids": lifecycle.get("activated_ids", []),
    }


def cleanup_market_stale_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: Optional[int] = None,
    archive_extracted_duplicates: bool = True,
) -> dict:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    now = datetime.utcnow()
    cleaned_ids: list[int] = []
    stale_resolved_ids: list[int] = []
    archived_duplicate_ids: list[int] = []

    by_group: dict[tuple[str, int | None, str | None, str | None, str | None], list[PolicyAssertion]] = {}
    for row in rows:
        key = (
            row.rule_key,
            row.source_id,
            row.county,
            row.city,
            row.pha_name,
        )
        by_group.setdefault(key, []).append(row)

    for _, group in by_group.items():
        group = sorted(group, key=lambda x: (float(x.confidence or 0.0), int(x.id or 0)), reverse=True)
        keeper = group[0]
        for row in group[1:]:
            if (row.governance_state or "").lower() == "active":
                continue
            if row.review_status not in {"stale", "needs_recheck", "extracted", "approved", "verified"}:
                continue
            row.review_status = "superseded"
            row.governance_state = "replaced"
            row.rule_status = "superseded"
            row.superseded_by_assertion_id = keeper.id
            row.reviewed_by_user_id = reviewer_user_id
            row.reviewed_at = now
            row.stale_after = None
            row.coverage_status = "superseded"
            cleaned_ids.append(int(row.id))
            if row.review_status in {"stale", "needs_recheck"}:
                stale_resolved_ids.append(int(row.id))
            else:
                archived_duplicate_ids.append(int(row.id))

    db.commit()

    remaining_rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    stale_remaining = [
        a.id
        for a in remaining_rows
        if (a.review_status or "").lower() in {"stale", "needs_recheck"}
        and a.superseded_by_assertion_id is None
    ]

    return {
        "cleaned_count": len(cleaned_ids),
        "cleaned_ids": cleaned_ids,
        "stale_resolved_count": len(stale_resolved_ids),
        "stale_resolved_ids": stale_resolved_ids,
        "archived_duplicate_count": len(archived_duplicate_ids),
        "archived_duplicate_ids": archived_duplicate_ids,
        "stale_items_remaining": len(stale_remaining),
        "stale_item_ids_remaining": stale_remaining,
    }