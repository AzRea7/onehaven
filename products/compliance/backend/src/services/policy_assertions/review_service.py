from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.policy.categories import normalize_category
from app.policy_models import PolicyAssertion, PolicyOverrideLedger, PolicySource
from app.services.policy_rule_normalizer import (
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

SUPPORTING_ONLY_CATEGORIES = {"contacts", "documents", "fees", "program_overlay"}
MANUAL_REVIEW_REVIEW_STATUS = "needs_manual_review"
HIGH_RISK_VALIDATION_STATES = {"conflicting"}
NON_PROJECTABLE_VALIDATION_STATES = {"weak_support", "ambiguous", "unsupported", "conflicting"}


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


def _loads_json(value: Any, default: Any) -> Any:
    import json
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        parsed = json.loads(value)
        return parsed if parsed is not None else default
    except Exception:
        return default


def _dumps_json(value: Any) -> str:
    import json
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return json.dumps([] if isinstance(value, list) else {})


def _parse_datetime_value(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _is_verified_like(row: PolicyAssertion) -> bool:
    review_status = (getattr(row, "review_status", None) or "").strip().lower()
    verification_reason = (getattr(row, "verification_reason", None) or "").strip().lower()
    return review_status in {"verified", "approved"} or bool(verification_reason)


def _normalized_category(row: PolicyAssertion) -> str | None:
    return getattr(row, "normalized_category", None) or getattr(row, "rule_category", None)


def _has_projectable_category(row: PolicyAssertion) -> bool:
    category = _normalized_category(row)
    if not category:
        return False
    return str(category).strip().lower() not in SUPPORTING_ONLY_CATEGORIES


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


def _governance_conflict_hints(row: PolicyAssertion) -> list[str]:
    hints: list[str] = []
    for attr in ("citation_json", "rule_provenance_json"):
        raw = getattr(row, attr, None)
        try:
            parsed = {} if raw in {None, ""} else (raw if isinstance(raw, dict) else __import__("json").loads(raw))
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            maybe = parsed.get("conflict_hints")
            if isinstance(maybe, list):
                hints.extend(str(x).strip() for x in maybe if str(x).strip())
    if (getattr(row, "coverage_status", None) or "").strip().lower() == "conflicting":
        hints.append("coverage_status_conflicting")
    if (getattr(row, "rule_status", None) or "").strip().lower() == "conflicting":
        hints.append("rule_status_conflicting")
    return sorted(set(hints))


def _requires_manual_review(row: PolicyAssertion) -> bool:
    review_status = (getattr(row, "review_status", None) or "").strip().lower()
    verification_reason = (getattr(row, "verification_reason", None) or "").strip().lower()
    normalized_category = _normalized_category(row)
    validation_state = (getattr(row, "validation_state", None) or "pending").strip().lower()
    trust_state = (getattr(row, "trust_state", None) or "").strip().lower()

    if normalized_category and (review_status in {"verified", "approved"} or verification_reason):
        if validation_state not in HIGH_RISK_VALIDATION_STATES and trust_state != MANUAL_REVIEW_REVIEW_STATUS:
            return False

    return bool(
        validation_state in HIGH_RISK_VALIDATION_STATES
        or review_status == MANUAL_REVIEW_REVIEW_STATUS
        or (trust_state == "needs_review" and bool(_governance_conflict_hints(row)))
    )


def _set_lifecycle_state(
    row: PolicyAssertion,
    *,
    governance_state: str,
    reviewer_user_id: int | None,
    reviewed_at: datetime | None = None,
) -> None:
    now = reviewed_at or _utcnow()
    normalized_category = _normalized_category(row)
    review_status = (getattr(row, "review_status", None) or "").strip().lower()
    verification_reason = (getattr(row, "verification_reason", None) or "").strip().lower()

    if normalized_category and (review_status in {"verified", "approved"} or verification_reason):
        if governance_state == "active":
            if hasattr(row, "validation_state"):
                row.validation_state = "validated"
            if hasattr(row, "trust_state"):
                row.trust_state = "trusted"
            row.review_status = "verified"
            row.rule_status = "active"
            row.coverage_status = "verified"
            row.governance_state = "active"
            row.is_current = bool(_has_projectable_category(row))
            row.reviewed_by_user_id = reviewer_user_id
            row.reviewed_at = now
            row.approved_at = getattr(row, "approved_at", None) or now
            row.approved_by_user_id = getattr(row, "approved_by_user_id", None) or reviewer_user_id
            row.activated_at = getattr(row, "activated_at", None) or now
            row.activated_by_user_id = getattr(row, "activated_by_user_id", None) or reviewer_user_id
            return
        if governance_state == "approved":
            if hasattr(row, "validation_state"):
                row.validation_state = "validated"
            if hasattr(row, "trust_state"):
                row.trust_state = "validated"
            row.review_status = "verified"
            row.rule_status = "approved"
            row.coverage_status = "verified"
            row.governance_state = "approved"
            row.is_current = False
            row.reviewed_by_user_id = reviewer_user_id
            row.reviewed_at = now
            row.approved_at = getattr(row, "approved_at", None) or now
            row.approved_by_user_id = getattr(row, "approved_by_user_id", None) or reviewer_user_id
            return

    row.governance_state = governance_state

    if governance_state == "draft":
        if not _is_verified_like(row):
            row.review_status = "extracted"
        row.rule_status = "candidate"
        row.coverage_status = getattr(row, "coverage_status", None) or "candidate"
        row.is_current = False
        if hasattr(row, "trust_state") and not _is_verified_like(row) and (getattr(row, "trust_state", None) in {None, "trusted"}):
            row.trust_state = "extracted"
    elif governance_state == "approved":
        validation_state = (getattr(row, "validation_state", None) or "pending").lower()
        row.review_status = "approved" if validation_state == "validated" else "needs_validation"
        row.rule_status = "approved" if validation_state == "validated" else "candidate"
        row.coverage_status = "approved" if validation_state == "validated" else "partial"
        row.is_current = False
        if hasattr(row, "trust_state"):
            row.trust_state = "validated" if validation_state == "validated" else "needs_review"
        row.approved_at = row.approved_at or now
        row.approved_by_user_id = row.approved_by_user_id or reviewer_user_id
    elif governance_state == "active":
        validation_state = (getattr(row, "validation_state", None) or "pending").lower()
        if validation_state != "validated" and not _is_verified_like(row):
            row.review_status = "needs_validation"
            row.rule_status = "candidate"
            row.coverage_status = "partial"
            row.is_current = False
            if hasattr(row, "trust_state"):
                row.trust_state = "needs_review"
            row.reviewed_by_user_id = reviewer_user_id
            row.reviewed_at = now
            return
        row.review_status = "verified"
        row.rule_status = "active"
        row.coverage_status = "verified"
        row.is_current = bool(_has_projectable_category(row))
        if hasattr(row, "trust_state"):
            row.trust_state = "trusted"
        if hasattr(row, "validation_state") and validation_state != "validated":
            row.validation_state = "validated"
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

    if governance_state == "active" and _requires_manual_review(row):
        row.governance_state = "draft"
        row.review_status = MANUAL_REVIEW_REVIEW_STATUS
        row.coverage_status = "conflicting" if _governance_conflict_hints(row) else "partial"
        if hasattr(row, "trust_state"):
            row.trust_state = "needs_review"
        row.is_current = False

    if _requires_manual_review(row) and governance_state != "replaced":
        row.review_status = MANUAL_REVIEW_REVIEW_STATUS
        row.is_current = False


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
    manual_review_ids: list[int] = []
    forced_validated_ids: list[int] = []

    for row in rows:
        if (row.rule_key or "") not in AUTO_VERIFY_RULE_KEYS:
            continue
        if float(row.confidence or 0.0) < 0.75:
            continue
        if not _normalized_category(row):
            continue

        validation_state = (getattr(row, "validation_state", None) or "pending").lower()
        if validation_state != "validated" and _is_verified_like(row):
            row.validation_state = "validated"
            forced_validated_ids.append(int(row.id))
            if hasattr(row, "trust_state") and (getattr(row, "trust_state", None) or "").lower() in {"", "extracted", "needs_review"}:
                row.trust_state = "validated"
            validation_state = "validated"
        if validation_state != "validated":
            continue

        if _requires_manual_review(row):
            row.review_status = MANUAL_REVIEW_REVIEW_STATUS
            row.is_current = False
            if hasattr(row, "trust_state"):
                row.trust_state = "needs_review"
            manual_review_ids.append(int(row.id))
            continue

        if _has_projectable_category(row) and (_is_verified_like(row) or float(row.confidence or 0.0) >= 0.9):
            _set_lifecycle_state(row, governance_state="active", reviewer_user_id=reviewer_user_id, reviewed_at=now)
            active_ids.append(int(row.id))
        else:
            _set_lifecycle_state(row, governance_state="approved", reviewer_user_id=reviewer_user_id, reviewed_at=now)
            approved_ids.append(int(row.id))

    db.commit()
    return {
        "approved_count": len(sorted(set(approved_ids))),
        "approved_ids": sorted(set(approved_ids)),
        "active_count": len(sorted(set(active_ids))),
        "active_ids": sorted(set(active_ids)),
        "manual_review_count": len(sorted(set(manual_review_ids))),
        "manual_review_ids": sorted(set(manual_review_ids)),
        "forced_validated_count": len(sorted(set(forced_validated_ids))),
        "forced_validated_ids": sorted(set(forced_validated_ids)),
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


def _lifecycle_target_for_row(row: PolicyAssertion, *, auto_activate: bool) -> str:
    validation_state = (getattr(row, "validation_state", None) or "pending").strip().lower()
    trust_state = (getattr(row, "trust_state", None) or "extracted").strip().lower()
    confidence = float(getattr(row, "confidence", 0.0) or 0.0)
    if _requires_manual_review(row):
        return "draft"
    if validation_state == "validated":
        if _has_projectable_category(row) and auto_activate and trust_state in {"validated", "trusted"} and confidence >= 0.85:
            return "active"
        return "approved"
    if validation_state in NON_PROJECTABLE_VALIDATION_STATES:
        return "draft"
    return "draft"


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
    rejected_ids: list[int] = []
    replaced_ids: list[int] = []
    manual_review_ids: list[int] = []
    by_group: dict[tuple[str, str], list[PolicyAssertion]] = {}
    for row in rows:
        key = ((getattr(row, "version_group", None) or ""), (getattr(row, "rule_key", None) or ""))
        by_group.setdefault(key, []).append(row)

    for _, group in by_group.items():
        ordered = sorted(
            group,
            key=lambda x: (
                float(getattr(x, "confidence", 0.0) or 0.0),
                int(getattr(x, "version_number", 0) or 0),
                int(getattr(x, "id", 0) or 0),
            ),
            reverse=True,
        )
        keeper = ordered[0]
        target_state = _lifecycle_target_for_row(keeper, auto_activate=auto_activate)
        _set_lifecycle_state(keeper, governance_state=target_state, reviewer_user_id=reviewer_user_id, reviewed_at=now)

        if _requires_manual_review(keeper):
            manual_review_ids.append(int(keeper.id))
        if target_state == "active":
            active_ids.append(int(keeper.id))
            replaced_ids.extend(_replace_previous_current(group, keeper=keeper, reviewer_user_id=reviewer_user_id))
        elif target_state == "approved":
            approved_ids.append(int(keeper.id))
        else:
            rejected_ids.append(int(keeper.id))

        for row in ordered[1:]:
            if row.id == keeper.id:
                continue
            if (getattr(row, "governance_state", None) or "").lower() != "replaced":
                row.replaced_by_assertion_id = keeper.id
                row.superseded_by_assertion_id = keeper.id
                _set_lifecycle_state(row, governance_state="replaced", reviewer_user_id=reviewer_user_id, reviewed_at=now)
                replaced_ids.append(int(row.id))

    db.commit()
    return {
        "approved_count": len(sorted(set(approved_ids))),
        "approved_ids": sorted(set(approved_ids)),
        "active_count": len(sorted(set(active_ids))),
        "active_ids": sorted(set(active_ids)),
        "rejected_count": len(sorted(set(rejected_ids))),
        "rejected_ids": sorted(set(rejected_ids)),
        "replaced_count": len(sorted(set(replaced_ids))),
        "replaced_ids": sorted(set(replaced_ids)),
        "manual_review_count": len(sorted(set(manual_review_ids))),
        "manual_review_ids": sorted(set(manual_review_ids)),
        "validated_ids": sorted({int(row.id) for row in rows if (getattr(row, "validation_state", None) or "").lower() == "validated"}),
        "needs_review_ids": sorted({int(row.id) for row in rows if _requires_manual_review(row) or (getattr(row, "trust_state", None) or "").lower() == "needs_review"}),
        "downgraded_ids": sorted({int(row.id) for row in rows if (getattr(row, "trust_state", None) or "").lower() == "downgraded"}),
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
    manual_review_ids: list[int] = []

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

    for row in rows:
        if _requires_manual_review(row):
            row.review_status = MANUAL_REVIEW_REVIEW_STATUS
            row.coverage_status = "conflicting" if _governance_conflict_hints(row) else (getattr(row, "coverage_status", None) or "partial")
            row.is_current = False
            manual_review_ids.append(int(row.id))
            db.add(row)

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
        "manual_review_count": len(sorted(set(manual_review_ids))),
        "manual_review_ids": sorted(set(manual_review_ids)),
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


def _override_matches_scope(
    override: PolicyOverrideLedger,
    *,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None,
) -> bool:
    st = _norm_state(state) if state is not None else None
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)
    if st is not None and _norm_state(getattr(override, "state", None)) != st:
        return False
    if getattr(override, "county", None) is not None and _norm_lower(getattr(override, "county", None)) != cnty:
        return False
    if getattr(override, "city", None) is not None and _norm_lower(getattr(override, "city", None)) != cty:
        return False
    if getattr(override, "pha_name", None) is not None and _norm_text(getattr(override, "pha_name", None)) != pha:
        return False
    return True


def _override_to_dict(row: PolicyOverrideLedger) -> dict[str, Any]:
    return {
        "id": int(getattr(row, "id", 0) or 0),
        "org_id": getattr(row, "org_id", None),
        "jurisdiction_profile_id": getattr(row, "jurisdiction_profile_id", None),
        "assertion_id": getattr(row, "assertion_id", None),
        "state": getattr(row, "state", None),
        "county": getattr(row, "county", None),
        "city": getattr(row, "city", None),
        "pha_name": getattr(row, "pha_name", None),
        "program_type": getattr(row, "program_type", None),
        "override_scope": getattr(row, "override_scope", None),
        "override_type": getattr(row, "override_type", None),
        "rule_key": getattr(row, "rule_key", None),
        "rule_category": getattr(row, "rule_category", None),
        "severity": getattr(row, "severity", None),
        "is_active": bool(getattr(row, "is_active", False)),
        "carrying_critical_rule": bool(getattr(row, "carrying_critical_rule", False)),
        "trust_impact": getattr(row, "trust_impact", None),
        "reason": getattr(row, "reason", None),
        "linked_evidence": _loads_json(getattr(row, "linked_evidence_json", None), []),
        "metadata": _loads_json(getattr(row, "metadata_json", None), {}),
        "created_by_user_id": getattr(row, "created_by_user_id", None),
        "approved_by_user_id": getattr(row, "approved_by_user_id", None),
        "expires_at": getattr(row, "expires_at", None).isoformat() if getattr(row, "expires_at", None) else None,
        "revoked_at": getattr(row, "revoked_at", None).isoformat() if getattr(row, "revoked_at", None) else None,
        "revoked_reason": getattr(row, "revoked_reason", None),
        "created_at": getattr(row, "created_at", None).isoformat() if getattr(row, "created_at", None) else None,
        "updated_at": getattr(row, "updated_at", None).isoformat() if getattr(row, "updated_at", None) else None,
        "is_currently_effective": bool(getattr(row, "is_currently_effective", False)),
    }


def list_policy_overrides(
    db: Session,
    *,
    org_id: Optional[int],
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    jurisdiction_profile_id: int | None = None,
    include_inactive: bool = False,
) -> dict[str, Any]:
    stmt = select(PolicyOverrideLedger)
    if org_id is None:
        stmt = stmt.where(PolicyOverrideLedger.org_id.is_(None))
    else:
        stmt = stmt.where(or_(PolicyOverrideLedger.org_id == org_id, PolicyOverrideLedger.org_id.is_(None)))
    if jurisdiction_profile_id is not None:
        stmt = stmt.where(PolicyOverrideLedger.jurisdiction_profile_id == int(jurisdiction_profile_id))
    rows = list(db.scalars(stmt.order_by(PolicyOverrideLedger.created_at.desc(), PolicyOverrideLedger.id.desc())).all())
    out = []
    for row in rows:
        if not include_inactive and not bool(getattr(row, "is_currently_effective", False)):
            continue
        if not _override_matches_scope(row, state=state, county=county, city=city, pha_name=pha_name):
            continue
        out.append(_override_to_dict(row))
    return {"ok": True, "count": len(out), "items": out}


def summarize_policy_overrides(
    db: Session,
    *,
    org_id: Optional[int],
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    jurisdiction_profile_id: int | None = None,
) -> dict[str, Any]:
    listing = list_policy_overrides(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        jurisdiction_profile_id=jurisdiction_profile_id,
        include_inactive=False,
    )
    items = list(listing.get("items") or [])
    critical = [row for row in items if bool(row.get("carrying_critical_rule"))]
    review_required = [row for row in items if str(row.get("trust_impact") or "").strip().lower() in {"review_required", "reduced_confidence", "blocked"}]
    return {
        "count": len(items),
        "items": items,
        "critical_count": len(critical),
        "critical_items": critical,
        "review_required": bool(review_required),
        "reduces_legal_confidence": bool(items),
        "carrying_critical_override": bool(critical),
        "reasons": [str(row.get("reason") or "").strip() for row in items if str(row.get("reason") or "").strip()],
    }


def create_policy_override(
    db: Session,
    *,
    org_id: Optional[int],
    created_by_user_id: int | None,
    jurisdiction_profile_id: int | None = None,
    assertion_id: int | None = None,
    state: str | None = None,
    county: str | None = None,
    city: str | None = None,
    pha_name: str | None = None,
    program_type: str | None = None,
    override_scope: str = "jurisdiction",
    override_type: str = "interim_operational_override",
    rule_key: str | None = None,
    rule_category: str | None = None,
    severity: str = "medium",
    carrying_critical_rule: bool = False,
    trust_impact: str = "review_required",
    reason: str = "",
    linked_evidence: list[dict[str, Any]] | list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    expires_at: Any = None,
) -> dict[str, Any]:
    row = PolicyOverrideLedger(
        org_id=org_id,
        jurisdiction_profile_id=jurisdiction_profile_id,
        assertion_id=assertion_id,
        state=_norm_state(state) if state else None,
        county=_norm_lower(county),
        city=_norm_lower(city),
        pha_name=_norm_text(pha_name),
        program_type=_norm_text(program_type),
        override_scope=_norm_text(override_scope) or "jurisdiction",
        override_type=_norm_text(override_type) or "interim_operational_override",
        rule_key=_norm_text(rule_key),
        rule_category=normalize_category(rule_category) if rule_category else None,
        severity=_norm_text(severity) or "medium",
        is_active=True,
        carrying_critical_rule=bool(carrying_critical_rule),
        trust_impact=_norm_text(trust_impact) or "review_required",
        reason=_norm_text(reason) or "Override reason required",
        linked_evidence_json=_dumps_json(list(linked_evidence or [])),
        metadata_json=_dumps_json(dict(metadata or {})),
        created_by_user_id=created_by_user_id,
        expires_at=_parse_datetime_value(expires_at),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"ok": True, "item": _override_to_dict(row)}


def revoke_policy_override(
    db: Session,
    *,
    override_id: int,
    revoked_reason: str | None = None,
    approved_by_user_id: int | None = None,
) -> dict[str, Any]:
    row = db.get(PolicyOverrideLedger, int(override_id))
    if row is None:
        return {"ok": False, "error": "policy_override_not_found"}
    row.is_active = False
    row.revoked_at = _utcnow()
    row.revoked_reason = _norm_text(revoked_reason)
    row.approved_by_user_id = approved_by_user_id or getattr(row, "approved_by_user_id", None)
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"ok": True, "item": _override_to_dict(row)}


# === Final governance throughput overrides ===
_BASE_LIFECYCLE_TARGET = _lifecycle_target_for_row
_BASE_AUTO_VERIFY = auto_verify_market_assertions
_BASE_APPLY_GOVERNANCE = apply_governance_lifecycle


def _category_is_supporting_only(row: PolicyAssertion) -> bool:
    category = (_normalized_category(row) or "").strip().lower()
    return category in SUPPORTING_ONLY_CATEGORIES


def _row_can_auto_activate(row: PolicyAssertion) -> bool:
    validation_state = (getattr(row, "validation_state", None) or "pending").strip().lower()
    trust_state = (getattr(row, "trust_state", None) or "extracted").strip().lower()
    confidence = float(getattr(row, "confidence", 0.0) or 0.0)
    coverage_status = (getattr(row, "coverage_status", None) or "").strip().lower()
    if _requires_manual_review(row):
        return False
    if _category_is_supporting_only(row):
        return False
    return (
        validation_state == "validated"
        and trust_state in {"validated", "trusted"}
        and confidence >= 0.82
        and coverage_status not in {"unsupported", "conflicting"}
        and _has_projectable_category(row)
    )


def _lifecycle_target_for_row(row: PolicyAssertion, *, auto_activate: bool) -> str:
    if _requires_manual_review(row):
        return "draft"
    validation_state = (getattr(row, "validation_state", None) or "pending").strip().lower()
    trust_state = (getattr(row, "trust_state", None) or "extracted").strip().lower()
    if validation_state in NON_PROJECTABLE_VALIDATION_STATES:
        return "draft"
    if validation_state == "validated":
        if auto_activate and _row_can_auto_activate(row):
            return "active"
        if trust_state in {"validated", "trusted"}:
            return "approved"
    return "draft"


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
    manual_review_ids: list[int] = []
    forced_validated_ids: list[int] = []

    for row in rows:
        validation_state = (getattr(row, "validation_state", None) or "pending").lower()
        confidence = float(getattr(row, "confidence", 0.0) or 0.0)
        if not _normalized_category(row):
            continue

        # widen auto-verify beyond a small rule-key allowlist for strong validated official assertions
        if validation_state != "validated" and _is_verified_like(row) and confidence >= 0.80:
            row.validation_state = "validated"
            forced_validated_ids.append(int(row.id))
            if hasattr(row, "trust_state") and (getattr(row, "trust_state", None) or "").lower() in {"", "extracted", "needs_review"}:
                row.trust_state = "validated"
            validation_state = "validated"

        if validation_state != "validated":
            continue

        if _requires_manual_review(row):
            row.review_status = MANUAL_REVIEW_REVIEW_STATUS
            row.is_current = False
            if hasattr(row, "trust_state"):
                row.trust_state = "needs_review"
            manual_review_ids.append(int(row.id))
            continue

        if _row_can_auto_activate(row):
            _set_lifecycle_state(row, governance_state="active", reviewer_user_id=reviewer_user_id, reviewed_at=now)
            active_ids.append(int(row.id))
        else:
            _set_lifecycle_state(row, governance_state="approved", reviewer_user_id=reviewer_user_id, reviewed_at=now)
            approved_ids.append(int(row.id))

    db.commit()
    return {
        "approved_count": len(sorted(set(approved_ids))),
        "approved_ids": sorted(set(approved_ids)),
        "active_count": len(sorted(set(active_ids))),
        "active_ids": sorted(set(active_ids)),
        "manual_review_count": len(sorted(set(manual_review_ids))),
        "manual_review_ids": sorted(set(manual_review_ids)),
        "forced_validated_count": len(sorted(set(forced_validated_ids))),
        "forced_validated_ids": sorted(set(forced_validated_ids)),
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
    payload = dict(_BASE_APPLY_GOVERNANCE(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        reviewer_user_id=reviewer_user_id,
        auto_activate=auto_activate,
    ))
    payload["governance_mode"] = "throughput_hardened"
    return payload
