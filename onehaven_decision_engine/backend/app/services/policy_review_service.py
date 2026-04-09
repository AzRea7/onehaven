# backend/app/services/policy_review_service.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_category
from app.policy_models import PolicyAssertion, PolicySource
from app.services.policy_rule_normalizer import (
    NormalizedRuleCandidate,
    assertion_fingerprint,
    candidate_matches_assertion,
    candidate_to_update_dict,
    diff_reason,
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


def _utcnow() -> datetime:
    return datetime.utcnow()


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


def _query_market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
):
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    stmt = select(PolicyAssertion).where(PolicyAssertion.state == st)
    if hasattr(PolicyAssertion, "org_id"):
        if org_id is None:
            stmt = stmt.where(PolicyAssertion.org_id.is_(None))
        else:
            stmt = stmt.where(or_(PolicyAssertion.org_id == org_id, PolicyAssertion.org_id.is_(None)))
    if cnty is None:
        stmt = stmt.where(PolicyAssertion.county.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.county == cnty)
    if cty is None:
        stmt = stmt.where(PolicyAssertion.city.is_(None))
    else:
        stmt = stmt.where(PolicyAssertion.city == cty)
    if hasattr(PolicyAssertion, "pha_name"):
        if pha is None:
            stmt = stmt.where(or_(PolicyAssertion.pha_name.is_(None), PolicyAssertion.pha_name == ""))
        else:
            stmt = stmt.where(PolicyAssertion.pha_name == pha)
    return stmt.order_by(PolicyAssertion.id.asc())


def _market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
    return list(
        db.scalars(
            _query_market_assertions(
                db,
                org_id=org_id,
                state=state,
                county=county,
                city=city,
                pha_name=pha_name,
            )
        ).all()
    )


def _active_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str],
) -> list[PolicyAssertion]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    out: list[PolicyAssertion] = []
    for row in rows:
        gov = (row.governance_state or "").lower()
        status = (row.rule_status or "").lower()
        if gov == "active" or status == "active" or bool(getattr(row, "is_current", False)):
            out.append(row)
    return out


def _source_context(source: PolicySource | None) -> dict[str, Any]:
    if source is None:
        return {}
    return {
        "source_id": int(source.id),
        "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
        "source_type": getattr(source, "source_type", None),
        "source_version_id": None,
        "publisher": getattr(source, "publisher", None),
        "title": getattr(source, "title", None),
        "url": getattr(source, "url", None),
        "jurisdiction_slug": getattr(source, "jurisdiction_slug", None),
        "state": getattr(source, "state", None),
        "county": getattr(source, "county", None),
        "city": getattr(source, "city", None),
        "pha_name": getattr(source, "pha_name", None),
        "program_type": getattr(source, "program_type", None),
        "source_level": getattr(source, "source_type", None) or getattr(source, "source_level", None),
    }


def _coerce_candidate(
    raw: Any,
    *,
    source: PolicySource | None = None,
    fallback_state: Optional[str] = None,
    fallback_county: Optional[str] = None,
    fallback_city: Optional[str] = None,
    fallback_pha_name: Optional[str] = None,
) -> dict[str, Any]:
    data = dict(raw or {})
    ctx = _source_context(source)

    for key, value in ctx.items():
        if data.get(key) in {None, ""} and value not in {None, ""}:
            data[key] = value

    if data.get("state") in {None, ""} and fallback_state:
        data["state"] = fallback_state
    if data.get("county") in {None, ""} and fallback_county:
        data["county"] = fallback_county
    if data.get("city") in {None, ""} and fallback_city:
        data["city"] = fallback_city
    if data.get("pha_name") in {None, ""} and fallback_pha_name:
        data["pha_name"] = fallback_pha_name

    if data.get("rule_category"):
        data["rule_category"] = normalize_category(data.get("rule_category"))
    elif data.get("category"):
        data["rule_category"] = normalize_category(data.get("category"))

    return data


def _assertion_scope_matches(
    row: PolicyAssertion,
    *,
    rule_key: str,
    source_id: int | None,
    source_level: str | None,
    property_type: str | None,
    version_group: str | None,
) -> bool:
    if (row.rule_key or "") != (rule_key or ""):
        return False
    if int(getattr(row, "source_id", 0) or 0) != int(source_id or 0):
        return False
    if (getattr(row, "source_level", None) or "") != (source_level or ""):
        return False
    if (getattr(row, "property_type", None) or None) != (property_type or None):
        return False
    if (getattr(row, "version_group", None) or "") != (version_group or ""):
        return False
    return True


def _next_version_number(group_rows: list[PolicyAssertion], version_group: str) -> int:
    max_version = 0
    for row in group_rows:
        if (getattr(row, "version_group", None) or "") != (version_group or ""):
            continue
        try:
            max_version = max(max_version, int(getattr(row, "version_number", 0) or 0))
        except Exception:
            continue
    return max_version + 1


def _set_lifecycle_state(
    row: PolicyAssertion,
    *,
    governance_state: str,
    reviewer_user_id: int | None,
    reviewed_at: datetime | None = None,
) -> None:
    now = reviewed_at or _utcnow()
    row.governance_state = governance_state

    if governance_state == "draft":
        row.review_status = "extracted"
        row.rule_status = "candidate"
        row.coverage_status = "candidate"
        row.is_current = False
    elif governance_state == "approved":
        row.review_status = "approved"
        row.rule_status = "approved"
        row.coverage_status = "approved"
        row.is_current = False
        row.approved_at = row.approved_at or now
        row.approved_by_user_id = row.approved_by_user_id or reviewer_user_id
    elif governance_state == "active":
        row.review_status = "verified"
        row.rule_status = "active"
        row.coverage_status = "verified"
        row.is_current = True
        row.approved_at = row.approved_at or now
        row.approved_by_user_id = row.approved_by_user_id or reviewer_user_id
        row.activated_at = row.activated_at or now
        row.activated_by_user_id = row.activated_by_user_id or reviewer_user_id
    elif governance_state == "replaced":
        row.review_status = "superseded"
        row.rule_status = "superseded"
        row.coverage_status = "superseded"
        row.is_current = False
        row.replaced_at = row.replaced_at or now

    row.reviewed_by_user_id = reviewer_user_id
    row.reviewed_at = now


def _replace_previous_current(
    rows: list[PolicyAssertion],
    *,
    keeper: PolicyAssertion,
    reviewer_user_id: int | None,
) -> list[int]:
    now = _utcnow()
    replaced_ids: list[int] = []
    for row in rows:
        if row.id == keeper.id:
            continue
        if (row.version_group or "") != (keeper.version_group or ""):
            continue
        if (row.rule_key or "") != (keeper.rule_key or ""):
            continue
        if not (bool(getattr(row, "is_current", False)) or (row.governance_state or "").lower() == "active"):
            continue
        row.replaced_by_assertion_id = keeper.id
        row.superseded_by_assertion_id = keeper.id
        _set_lifecycle_state(row, governance_state="replaced", reviewer_user_id=reviewer_user_id, reviewed_at=now)
        replaced_ids.append(int(row.id))
    return replaced_ids


def normalize_market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: int | None = None,
    source_id: int | None = None,
    raw_candidates: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    rows = _market_assertions(db, org_id=org_id, state=st, county=cnty, city=cty, pha_name=pha)
    source: PolicySource | None = None
    if source_id is not None:
        source = db.get(PolicySource, int(source_id))

    if raw_candidates is None:
        out = []
        updated = 0
        for row in rows:
            raw = {
                "rule_key": row.rule_key,
                "rule_category": row.rule_category or row.normalized_category,
                "source_level": getattr(row, "source_level", None),
                "property_type": getattr(row, "property_type", None),
                "required": getattr(row, "required", True),
                "blocking": getattr(row, "blocking", False),
                "confidence": getattr(row, "confidence", 0.0),
                "governance_state": getattr(row, "governance_state", "draft"),
                "rule_status": getattr(row, "rule_status", "candidate"),
                "normalized_version": getattr(row, "normalized_version", "v1"),
                "version_group": getattr(row, "version_group", None),
                "value_json": getattr(row, "value_json", None),
                "source_citation": getattr(row, "source_citation", None),
                "raw_excerpt": getattr(row, "raw_excerpt", None),
                "source_id": getattr(row, "source_id", None),
                "state": row.state,
                "county": row.county,
                "city": row.city,
                "pha_name": getattr(row, "pha_name", None),
            }
            candidate = normalize_rule_candidate(raw)
            if candidate is None:
                continue
            updates = candidate_to_update_dict(candidate, raw)
            old_fp = assertion_fingerprint(row)
            for key, value in updates.items():
                setattr(row, key, value)
            row.change_summary = None if old_fp == candidate.fingerprint else "normalized_fields_updated"
            updated += 1
            out.append(
                {
                    "assertion_id": int(row.id),
                    "rule_key": row.rule_key,
                    "fingerprint": candidate.fingerprint,
                    "changed": old_fp != candidate.fingerprint,
                }
            )
        db.commit()
        return {"normalized_count": updated, "items": out}

    existing_rows = rows
    created_ids: list[int] = []
    updated_ids: list[int] = []
    unchanged_ids: list[int] = []
    skipped: list[dict[str, Any]] = []

    for raw in raw_candidates:
        payload = _coerce_candidate(
            raw,
            source=source,
            fallback_state=st,
            fallback_county=cnty,
            fallback_city=cty,
            fallback_pha_name=pha,
        )
        candidate = normalize_rule_candidate(payload)
        if candidate is None:
            skipped.append({"reason": "unrecognized_rule", "raw": raw})
            continue

        scoped_rows = [
            row for row in existing_rows
            if _assertion_scope_matches(
                row,
                rule_key=candidate.rule_key,
                source_id=payload.get("source_id"),
                source_level=candidate.source_level,
                property_type=candidate.property_type,
                version_group=candidate.version_group,
            )
        ]

        matched = None
        for row in scoped_rows:
            if candidate_matches_assertion(candidate, row):
                matched = row
                break

        if matched is not None:
            updates = candidate_to_update_dict(candidate, payload)
            for key, value in updates.items():
                setattr(matched, key, value)
            matched.change_summary = None
            updated_ids.append(int(matched.id))
            unchanged_ids.append(int(matched.id))
            continue

        next_version = _next_version_number(scoped_rows, candidate.version_group)
        updates = candidate_to_update_dict(candidate, payload)
        row = PolicyAssertion(
            org_id=org_id,
            source_id=payload.get("source_id"),
            source_version_id=payload.get("source_version_id"),
            state=st,
            county=cnty,
            city=cty,
            pha_name=pha,
            program_type=_norm_text(payload.get("program_type")),
            priority=int(payload.get("priority") or 100),
            source_rank=int(payload.get("source_rank") or 100),
            assertion_type=_norm_text(payload.get("assertion_type")) or "document_reference",
            effective_date=payload.get("effective_date") or payload.get("effective_at"),
            expires_at=payload.get("expires_at"),
            stale_after=payload.get("stale_after"),
            source_freshness_status=_norm_text(payload.get("source_freshness_status")),
            version_number=next_version,
            extracted_at=_utcnow(),
            **updates,
        )
        row.review_status = "extracted"
        row.rule_status = "candidate"
        row.coverage_status = "candidate"
        row.is_current = False
        row.change_summary = "new_candidate_from_normalization"
        db.add(row)
        db.flush()
        existing_rows.append(row)
        created_ids.append(int(row.id))

    db.commit()
    return {
        "normalized_count": len(created_ids) + len(updated_ids),
        "created_count": len(created_ids),
        "created_ids": created_ids,
        "updated_count": len(updated_ids),
        "updated_ids": updated_ids,
        "unchanged_count": len(unchanged_ids),
        "unchanged_ids": unchanged_ids,
        "skipped": skipped,
    }


def auto_verify_market_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: int | None = None,
) -> dict[str, Any]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    now = _utcnow()
    approved_ids: list[int] = []
    active_ids: list[int] = []
    for row in rows:
        if (row.rule_key or "") not in AUTO_VERIFY_RULE_KEYS:
            continue
        if float(row.confidence or 0.0) < 0.75:
            continue
        if (row.governance_state or "").lower() == "active":
            row.is_current = True
            active_ids.append(int(row.id))
            continue
        if float(row.confidence or 0.0) >= 0.9:
            _set_lifecycle_state(row, governance_state="active", reviewer_user_id=reviewer_user_id, reviewed_at=now)
            active_ids.append(int(row.id))
        else:
            _set_lifecycle_state(row, governance_state="approved", reviewer_user_id=reviewer_user_id, reviewed_at=now)
            approved_ids.append(int(row.id))
    db.commit()
    return {
        "approved_count": len(approved_ids),
        "approved_ids": approved_ids,
        "active_count": len(active_ids),
        "active_ids": active_ids,
    }


def supersede_replaced_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: int | None = None,
) -> dict[str, Any]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    by_group: dict[tuple[str, str], list[PolicyAssertion]] = {}
    for row in rows:
        key = ((row.version_group or ""), (row.rule_key or ""))
        by_group.setdefault(key, []).append(row)

    replaced_ids: list[int] = []
    kept_ids: list[int] = []
    for _, group in by_group.items():
        ordered = sorted(
            group,
            key=lambda x: (
                1 if bool(getattr(x, "is_current", False)) else 0,
                1 if (x.governance_state or "").lower() == "active" else 0,
                1 if (x.governance_state or "").lower() == "approved" else 0,
                float(x.confidence or 0.0),
                int(x.version_number or 0),
                int(x.id or 0),
            ),
            reverse=True,
        )
        keeper = ordered[0]
        kept_ids.append(int(keeper.id))
        for row in ordered[1:]:
            if (row.governance_state or "").lower() == "replaced":
                replaced_ids.append(int(row.id))
                continue
            row.replaced_by_assertion_id = keeper.id
            row.superseded_by_assertion_id = keeper.id
            _set_lifecycle_state(row, governance_state="replaced", reviewer_user_id=reviewer_user_id)
            replaced_ids.append(int(row.id))
    db.commit()
    return {
        "kept_count": len(kept_ids),
        "kept_ids": kept_ids,
        "replaced_count": len(replaced_ids),
        "replaced_ids": replaced_ids,
    }


def apply_governance_lifecycle(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: int | None = None,
    auto_activate: bool = True,
) -> dict[str, Any]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )
    now = _utcnow()
    approved_ids: list[int] = []
    active_ids: list[int] = []
    replaced_ids: list[int] = []

    by_group: dict[tuple[str, str], list[PolicyAssertion]] = {}
    for row in rows:
        key = ((row.version_group or ""), (row.rule_key or ""))
        by_group.setdefault(key, []).append(row)

    for _, group in by_group.items():
        ordered = sorted(
            group,
            key=lambda x: (
                float(x.confidence or 0.0),
                int(x.version_number or 0),
                int(x.id or 0),
            ),
            reverse=True,
        )
        keeper = ordered[0]

        target_state = "approved"
        if auto_activate and float(keeper.confidence or 0.0) >= 0.85:
            target_state = "active"

        _set_lifecycle_state(keeper, governance_state=target_state, reviewer_user_id=reviewer_user_id, reviewed_at=now)
        if target_state == "active":
            active_ids.append(int(keeper.id))
            replaced_ids.extend(_replace_previous_current(group, keeper=keeper, reviewer_user_id=reviewer_user_id))
        else:
            approved_ids.append(int(keeper.id))

        for row in ordered[1:]:
            if row.id == keeper.id:
                continue
            if (row.governance_state or "").lower() not in {"active", "replaced"}:
                _set_lifecycle_state(row, governance_state="replaced", reviewer_user_id=reviewer_user_id, reviewed_at=now)
                row.replaced_by_assertion_id = keeper.id
                row.superseded_by_assertion_id = keeper.id
                replaced_ids.append(int(row.id))

    db.commit()
    return {
        "approved_count": len(sorted(set(approved_ids))),
        "approved_ids": sorted(set(approved_ids)),
        "active_count": len(sorted(set(active_ids))),
        "active_ids": sorted(set(active_ids)),
        "replaced_count": len(sorted(set(replaced_ids))),
        "replaced_ids": sorted(set(replaced_ids)),
    }


def cleanup_market_stale_assertions(
    db: Session,
    *,
    org_id: Optional[int],
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    reviewer_user_id: int | None = None,
) -> dict[str, Any]:
    rows = _market_assertions(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
    )

    now = _utcnow()
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
            getattr(row, "pha_name", None),
        )
        by_group.setdefault(key, []).append(row)

    for _, group in by_group.items():
        group = sorted(group, key=lambda x: (float(x.confidence or 0.0), int(x.id or 0)), reverse=True)
        keeper = group[0]
        for row in group[1:]:
            original_status = row.review_status
            if (row.governance_state or "").lower() == "active":
                continue
            if row.review_status not in {"stale", "needs_recheck", "extracted", "approved", "verified"}:
                continue
            row.review_status = "superseded"
            row.governance_state = "replaced"
            row.rule_status = "superseded"
            row.superseded_by_assertion_id = keeper.id
            row.replaced_by_assertion_id = keeper.id
            row.reviewed_at = now
            row.reviewed_by_user_id = reviewer_user_id
            row.is_current = False
            archived_duplicate_ids.append(int(row.id))
            if original_status == "stale":
                stale_resolved_ids.append(int(row.id))
            cleaned_ids.append(int(row.id))

    stale_remaining = [
        int(row.id)
        for row in rows
        if (row.review_status or "").lower() == "stale"
    ]

    db.commit()
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


def diff_active_rules_for_source(
    db: Session,
    *,
    org_id: Optional[int],
    source_id: int,
    state: str,
    county: Optional[str],
    city: Optional[str],
    pha_name: Optional[str] = None,
    raw_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    source = db.get(PolicySource, int(source_id))
    active_rows = [
        row for row in _active_assertions(
            db,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
        )
        if int(getattr(row, "source_id", 0) or 0) == int(source_id)
    ]

    candidate_fingerprints: dict[str, dict[str, Any]] = {}
    changed: list[dict[str, Any]] = []
    unchanged: list[dict[str, Any]] = []
    new_candidates: list[dict[str, Any]] = []

    for raw in raw_candidates:
        payload = _coerce_candidate(
            raw,
            source=source,
            fallback_state=state,
            fallback_county=county,
            fallback_city=city,
            fallback_pha_name=pha_name,
        )
        candidate = normalize_rule_candidate(payload)
        if candidate is None:
            continue
        key = f"{candidate.version_group}|{candidate.rule_key}"
        candidate_fingerprints[key] = {"candidate": candidate, "raw": payload}

    active_map: dict[str, PolicyAssertion] = {}
    for row in active_rows:
        key = f"{row.version_group}|{row.rule_key}"
        active_map[key] = row

    for key, wrapped in candidate_fingerprints.items():
        candidate = wrapped["candidate"]
        row = active_map.get(key)
        if row is None:
            new_candidates.append(
                {
                    "rule_key": candidate.rule_key,
                    "version_group": candidate.version_group,
                    "fingerprint": candidate.fingerprint,
                    "reason": "no_active_rule",
                }
            )
            continue
        if candidate_matches_assertion(candidate, row):
            unchanged.append(
                {
                    "assertion_id": int(row.id),
                    "rule_key": row.rule_key,
                    "version_group": row.version_group,
                    "fingerprint": candidate.fingerprint,
                }
            )
        else:
            changed.append(
                {
                    "assertion_id": int(row.id),
                    "rule_key": row.rule_key,
                    "version_group": row.version_group,
                    "old_fingerprint": assertion_fingerprint(row),
                    "new_fingerprint": candidate.fingerprint,
                    "reason": diff_reason(candidate, row),
                }
            )

    missing_from_new_snapshot: list[dict[str, Any]] = []
    for key, row in active_map.items():
        if key not in candidate_fingerprints:
            missing_from_new_snapshot.append(
                {
                    "assertion_id": int(row.id),
                    "rule_key": row.rule_key,
                    "version_group": row.version_group,
                    "reason": "missing_from_latest_source_snapshot",
                }
            )

    return {
        "source_id": int(source_id),
        "changed_count": len(changed),
        "changed": changed,
        "unchanged_count": len(unchanged),
        "unchanged": unchanged,
        "new_count": len(new_candidates),
        "new": new_candidates,
        "missing_count": len(missing_from_new_snapshot),
        "missing": missing_from_new_snapshot,
    }