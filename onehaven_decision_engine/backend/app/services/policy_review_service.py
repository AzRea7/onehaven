from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category
from app.policy_models import PolicyAssertion, PolicySource

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


def _source_is_authoritative(src: Optional[PolicySource]) -> bool:
    if src is None:
        return False

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


def _category_for_verified(a: PolicyAssertion) -> str | None:
    current = normalize_category(getattr(a, "normalized_category", None))
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


def _coverage_status_for_verified(a: PolicyAssertion) -> str:
    if (a.assertion_type or "").strip().lower() == "document_reference":
        return "candidate"
    if (a.rule_key or "").endswith("_anchor"):
        return "verified"
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
        q = q.filter(
            (PolicyAssertion.org_id == org_id) | (PolicyAssertion.org_id.is_(None))
        )

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
        if a.rule_key not in AUTO_VERIFY_RULE_KEYS:
            continue
        if a.review_status not in {"extracted", "needs_recheck"}:
            continue

        src = src_map.get(a.source_id) if a.source_id is not None else None
        if not _source_is_authoritative(src):
            continue

        a.review_status = "verified"
        a.confidence = max(float(a.confidence or 0.0), _confidence_for(a.rule_key, src))
        a.verification_reason = "official_source_review"
        a.reviewed_by_user_id = reviewer_user_id
        a.reviewed_at = now
        a.stale_after = None
        a.normalized_category = _category_for_verified(a)
        a.coverage_status = _coverage_status_for_verified(a)
        a.source_freshness_status = getattr(src, "freshness_status", None) if src else None

        auto_note = (
            f"Auto-verified for {market_label} from authoritative source: "
            f"{src.title or src.url}"
            if src
            else f"Auto-verified for {market_label} from authoritative source"
        )
        existing_note = (a.review_notes or "").strip()
        if "warren" in existing_note.lower() or "source_changed=" in existing_note.lower():
            a.review_notes = auto_note
        else:
            a.review_notes = auto_note if not existing_note else existing_note

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
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    by_rule: dict[str, list[PolicyAssertion]] = {}
    for a in rows:
        by_rule.setdefault(a.rule_key, []).append(a)

    superseded_ids: list[int] = []
    now = datetime.utcnow()

    for _, group in by_rule.items():
        verified = [a for a in group if a.review_status == "verified"]
        if not verified:
            continue

        winner = sorted(
            verified,
            key=lambda x: (
                -(float(x.confidence or 0.0)),
                -(x.id or 0),
            ),
        )[0]

        for a in group:
            if a.id == winner.id:
                continue
            if a.review_status not in {"needs_recheck", "stale", "verified", "extracted"}:
                continue

            a.review_status = "superseded"
            a.superseded_by_assertion_id = winner.id
            a.reviewed_by_user_id = reviewer_user_id
            a.reviewed_at = now
            a.stale_after = None
            a.coverage_status = "superseded"

            existing_note = (a.review_notes or "").strip()
            extra = f"Superseded by verified assertion {winner.id}"
            a.review_notes = extra if not existing_note else f"{existing_note} | {extra}"

            superseded_ids.append(a.id)

    db.commit()

    return {
        "superseded_count": len(superseded_ids),
        "superseded_ids": superseded_ids,
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
    """
    Product-safe cleanup:
    - keep authoritative verified winners
    - supersede stale / needs_recheck rows when a verified winner exists
    - supersede extracted rows too when a verified winner exists
    - optionally supersede older duplicate extracted rows
    """

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

    by_rule: dict[str, list[PolicyAssertion]] = {}
    for row in rows:
        by_rule.setdefault(row.rule_key, []).append(row)

    for _, group in by_rule.items():
        verified = [a for a in group if a.review_status == "verified"]
        winner = None
        if verified:
            winner = sorted(
                verified,
                key=lambda x: (
                    -(float(x.confidence or 0.0)),
                    -(x.id or 0),
                ),
            )[0]

        if winner is not None:
            for a in group:
                if a.id == winner.id:
                    continue
                if a.review_status not in {"stale", "needs_recheck", "extracted"}:
                    continue

                a.review_status = "superseded"
                a.superseded_by_assertion_id = winner.id
                a.reviewed_by_user_id = reviewer_user_id
                a.reviewed_at = now
                a.stale_after = None
                a.coverage_status = "superseded"

                extra = (
                    f"Resolved stale/recheck assertion during cleanup; "
                    f"superseded by verified assertion {winner.id}"
                )
                existing = (a.review_notes or "").strip()
                a.review_notes = extra if not existing else f"{existing} | {extra}"

                cleaned_ids.append(a.id)
                stale_resolved_ids.append(a.id)

    if archive_extracted_duplicates:
        duplicate_groups: dict[
            tuple[str, int | None, str | None, str | None, str | None],
            list[PolicyAssertion],
        ] = {}
        for a in rows:
            if a.review_status != "extracted":
                continue
            key = (
                a.rule_key,
                a.source_id,
                a.county,
                a.city,
                a.pha_name,
            )
            duplicate_groups.setdefault(key, []).append(a)

        for _, group in duplicate_groups.items():
            if len(group) <= 1:
                continue

            group_sorted = sorted(
                group,
                key=lambda x: (
                    x.extracted_at.isoformat() if x.extracted_at else "",
                    x.id or 0,
                ),
                reverse=True,
            )
            keeper = group_sorted[0]
            for a in group_sorted[1:]:
                a.review_status = "superseded"
                a.superseded_by_assertion_id = keeper.id
                a.reviewed_by_user_id = reviewer_user_id
                a.reviewed_at = now
                a.stale_after = None
                a.coverage_status = "superseded"

                extra = (
                    f"Archived duplicate extracted assertion during cleanup; "
                    f"kept newer assertion {keeper.id}"
                )
                existing = (a.review_notes or "").strip()
                a.review_notes = extra if not existing else f"{existing} | {extra}"

                cleaned_ids.append(a.id)
                archived_duplicate_ids.append(a.id)

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
        if a.review_status in {"stale", "needs_recheck"}
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
