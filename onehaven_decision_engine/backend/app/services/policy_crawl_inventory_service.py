from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.domain.jurisdiction_categories import normalize_categories
from app.policy_models import PolicySource, PolicySourceInventory
from app.services.policy_discovery_service import (
    sync_policy_source_into_inventory,
    update_inventory_after_fetch,
)
from app.services.policy_source_service import _is_rejected_discovered_source


AUTHORITY_TIER_RANKS: dict[str, int] = {
    "derived_or_inferred": 25,
    "semi_authoritative_operational": 60,
    "approved_official_supporting": 85,
    "authoritative_official": 100,
}

CATEGORY_ALIASES: dict[str, str] = {
    "registration": "registration",
    "rental_registration": "registration",
    "rental_license": "rental_license",
    "inspection": "inspection",
    "rental_inspection": "inspection",
    "occupancy": "occupancy",
    "certificate_of_occupancy": "occupancy",
    "certificate_of_compliance": "occupancy",
    "lead": "lead",
    "safety": "safety",
    "permits": "permits",
    "permits_building": "permits",
    "fees": "fees",
    "fees_forms": "fees",
    "documents": "documents",
    "program_overlay": "program_overlay",
    "section8": "section8",
    "contacts": "contacts",
    "contact": "contacts",
}


def _normalize_category(value: Any) -> str | None:
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    raw = raw.replace("-", "_").replace(" ", "_").replace("/", "_")
    while "__" in raw:
        raw = raw.replace("__", "_")
    return CATEGORY_ALIASES.get(raw, raw)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        norm = _normalize_category(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def _authority_policy_payload(*, authority_tier: str, authority_rank: int | None = None) -> dict[str, Any]:
    tier = str(authority_tier or "derived_or_inferred").strip() or "derived_or_inferred"
    rank = int(authority_rank if authority_rank is not None else AUTHORITY_TIER_RANKS.get(tier, 25))

    if tier == "authoritative_official":
        use_type = "binding"
        binding_sufficient = True
        supporting_only = False
        usable = True
    elif tier in {"approved_official_supporting", "semi_authoritative_operational"}:
        use_type = "supporting"
        binding_sufficient = False
        supporting_only = True
        usable = True
    else:
        use_type = "weak"
        binding_sufficient = False
        supporting_only = False
        usable = False

    return {
        "authority_tier": tier,
        "authority_rank": rank,
        "use_type": use_type,
        "binding_sufficient": binding_sufficient,
        "supporting_only": supporting_only,
        "usable": usable,
    }


def _change_summary(fetch_result: dict[str, Any]) -> dict[str, Any]:
    raw = fetch_result.get("change_summary")
    return dict(raw) if isinstance(raw, dict) else {}


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _apply_inventory_authority_from_source(inventory: Any, source: PolicySource) -> None:
    tier = str(getattr(source, "authority_tier", None) or "").strip() or "derived_or_inferred"
    rank = int(getattr(source, "authority_rank", 0) or 0)
    explicit_use_type = str(getattr(source, "authority_use_type", None) or "").strip().lower()

    policy = _authority_policy_payload(authority_tier=tier, authority_rank=rank)
    if explicit_use_type in {"binding", "supporting", "weak"}:
        policy["use_type"] = explicit_use_type
        policy["binding_sufficient"] = explicit_use_type == "binding" and tier == "authoritative_official"
        policy["supporting_only"] = explicit_use_type == "supporting"
        policy["usable"] = explicit_use_type in {"binding", "supporting"}

    if hasattr(inventory, "authority_use_type"):
        inventory.authority_use_type = str(policy.get("use_type") or "weak")
    if hasattr(inventory, "authority_tier"):
        inventory.authority_tier = tier
    if hasattr(inventory, "authority_rank"):
        inventory.authority_rank = int(rank or 0)
    if hasattr(inventory, "authority_policy_json"):
        import json
        try:
            inventory.authority_policy_json = json.dumps(policy, sort_keys=True, default=str)
        except Exception:
            inventory.authority_policy_json = "{}"


def _source_category_hints(source: PolicySource, inventory: Any | None = None) -> list[str]:
    values: list[Any] = []
    for attr in ("normalized_categories_json", "category_hints_json", "expected_categories_json"):
        if hasattr(source, attr):
            values.append(getattr(source, attr))
        if inventory is not None and hasattr(inventory, attr):
            values.append(getattr(inventory, attr))
    out: list[str] = []
    for value in values:
        if isinstance(value, list):
            out.extend(value)
        elif isinstance(value, tuple):
            out.extend(list(value))
        elif isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                import json
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, list):
                        out.extend(parsed)
                except Exception:
                    pass
            elif stripped:
                out.append(stripped)
    return normalize_categories(out)


def _inventory_row_scope_match(
    row: PolicySourceInventory,
    *,
    org_id: int | None,
    state: str | None,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    program_type: str | None = None,
) -> bool:
    if org_id is None:
        if getattr(row, "org_id", None) is not None:
            return False
    elif getattr(row, "org_id", None) not in {None, org_id}:
        return False
    if state and str(getattr(row, "state", "") or "").strip().upper() != str(state).strip().upper():
        return False
    if county and str(getattr(row, "county", "") or "").strip().lower() not in {"", str(county).strip().lower()}:
        return False
    if city and str(getattr(row, "city", "") or "").strip().lower() not in {"", str(city).strip().lower()}:
        return False
    if pha_name and str(getattr(row, "pha_name", "") or "").strip() not in {"", str(pha_name).strip()}:
        return False
    if program_type and str(getattr(row, "program_type", "") or "").strip() not in {"", str(program_type).strip()}:
        return False
    return True


def _inventory_category_entry(row: PolicySourceInventory) -> dict[str, Any]:
    authority_tier = str(getattr(row, "authority_tier", None) or "").strip() or "derived_or_inferred"
    authority_rank = int(getattr(row, "authority_rank", 0) or AUTHORITY_TIER_RANKS.get(authority_tier, 25))
    authority_use_type = str(getattr(row, "authority_use_type", None) or "").strip().lower() or _authority_policy_payload(authority_tier=authority_tier, authority_rank=authority_rank)["use_type"]
    lifecycle_state = str(getattr(row, "lifecycle_state", None) or "").strip().lower()
    crawl_status = str(getattr(row, "crawl_status", None) or "").strip().lower()
    refresh_state = str(getattr(row, "refresh_state", None) or "").strip().lower()
    is_curated = bool(getattr(row, "is_curated", False))
    is_official_candidate = bool(getattr(row, "is_official_candidate", False))

    usable_for_coverage = (
        lifecycle_state in {"accepted", "discovered", "pending_crawl", "active"}
        and crawl_status not in {"failed", "not_found"}
        and refresh_state not in {"failed", "blocked"}
        and authority_use_type in {"binding", "supporting"}
        and is_official_candidate
    )
    binding_sufficient = authority_use_type == "binding" and authority_tier == "authoritative_official"

    return {
        "inventory_id": int(getattr(row, "id", 0) or 0),
        "canonical_url": getattr(row, "canonical_url", None),
        "title": getattr(row, "title", None),
        "publisher": getattr(row, "publisher", None),
        "source_type": getattr(row, "source_type", None),
        "publication_type": getattr(row, "publication_type", None),
        "inventory_origin": getattr(row, "inventory_origin", None),
        "lifecycle_state": lifecycle_state,
        "crawl_status": crawl_status,
        "refresh_state": refresh_state,
        "next_refresh_step": getattr(row, "next_refresh_step", None),
        "authority_tier": authority_tier,
        "authority_rank": authority_rank,
        "authority_use_type": authority_use_type,
        "is_curated": is_curated,
        "is_official_candidate": is_official_candidate,
        "usable_for_coverage": usable_for_coverage,
        "binding_sufficient": binding_sufficient,
        "validation_due_at": _iso_or_none(getattr(row, "validation_due_at", None)),
        "updated_at": _iso_or_none(getattr(row, "updated_at", None)),
    }


def summarize_inventory_category_coverage(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    program_type: str | None = None,
    required_categories: list[str] | None = None,
) -> dict[str, Any]:
    stmt = select(PolicySourceInventory)
    rows = list(db.scalars(stmt).all())

    category_map: dict[str, list[dict[str, Any]]] = {}
    covered: list[str] = []
    binding: list[str] = []
    supporting_only: list[str] = []

    for row in rows:
        if not _inventory_row_scope_match(
            row,
            org_id=org_id,
            state=state,
            county=county,
            city=city,
            pha_name=pha_name,
            program_type=program_type,
        ):
            continue
        hints = _source_category_hints(source=row, inventory=row)
        if not hints:
            continue
        entry = _inventory_category_entry(row)
        for category in hints:
            category_map.setdefault(category, []).append(entry)
            if entry["usable_for_coverage"]:
                covered.append(category)
            if entry["binding_sufficient"]:
                binding.append(category)
            elif entry["usable_for_coverage"]:
                supporting_only.append(category)

    for items in category_map.values():
        items.sort(
            key=lambda x: (
                -int(x["usable_for_coverage"]),
                -int(x["binding_sufficient"]),
                -int(x["is_curated"]),
                -int(x["authority_rank"]),
                str(x.get("canonical_url") or ""),
            )
        )

    required = _dedupe(list(required_categories or []))
    covered_norm = _dedupe(covered)
    binding_norm = _dedupe(binding)
    supporting_only_norm = [c for c in _dedupe(supporting_only) if c not in set(binding_norm)]
    missing = [c for c in required if c not in set(covered_norm)]

    return {
        "scope": {
            "org_id": org_id,
            "state": state,
            "county": county,
            "city": city,
            "pha_name": pha_name,
            "program_type": program_type,
        },
        "required_categories": required,
        "covered_categories": covered_norm,
        "binding_categories": binding_norm,
        "supporting_only_categories": supporting_only_norm,
        "missing_categories": _dedupe(missing),
        "category_map": category_map,
        "inventory_count": sum(len(v) for v in category_map.values()),
    }


def _inventory_result_shape(inventory: Any, change_summary: dict[str, Any], category_coverage: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "ok": inventory is not None,
        "inventory_id": int(inventory.id) if inventory is not None else None,
        "lifecycle_state": getattr(inventory, "lifecycle_state", None) if inventory is not None else None,
        "crawl_status": getattr(inventory, "crawl_status", None) if inventory is not None else None,
        "refresh_state": getattr(inventory, "refresh_state", None) if inventory is not None else None,
        "refresh_status_reason": getattr(inventory, "refresh_status_reason", None) if inventory is not None else None,
        "next_refresh_step": getattr(inventory, "next_refresh_step", None) if inventory is not None else None,
        "revalidation_required": bool(getattr(inventory, "revalidation_required", False)) if inventory is not None else False,
        "validation_due_at": (
            _iso_or_none(getattr(inventory, "validation_due_at", None))
            if inventory is not None
            else None
        ),
        "current_source_version_id": getattr(inventory, "current_source_version_id", None) if inventory is not None else None,
        "last_change_summary": change_summary,
        "authority_use_type": getattr(inventory, "authority_use_type", None) if inventory is not None else None,
        "category_coverage": dict(category_coverage or {}),
    }


def sync_crawl_result_to_inventory(
    db: Session,
    *,
    source: PolicySource,
    fetch_result: dict[str, Any],
) -> dict[str, Any]:
    change_summary = _change_summary(fetch_result)
    normalized_fetch = dict(fetch_result or {})

    normalized_fetch.setdefault("source_id", int(getattr(source, "id", 0) or 0))
    normalized_fetch.setdefault("source_version_id", fetch_result.get("source_version_id"))
    normalized_fetch.setdefault(
        "current_fingerprint",
        fetch_result.get("current_fingerprint")
        or change_summary.get("current_fingerprint")
        or getattr(source, "current_fingerprint", None)
        or getattr(source, "content_sha256", None),
    )
    normalized_fetch.setdefault(
        "previous_fingerprint",
        fetch_result.get("previous_fingerprint")
        or change_summary.get("previous_fingerprint"),
    )
    normalized_fetch.setdefault(
        "comparison_state",
        fetch_result.get("comparison_state")
        or change_summary.get("comparison_state"),
    )
    normalized_fetch.setdefault(
        "change_kind",
        fetch_result.get("change_kind")
        or change_summary.get("change_kind"),
    )
    normalized_fetch.setdefault(
        "actionable_outcome",
        fetch_result.get("actionable_outcome")
        or change_summary.get("actionable_outcome"),
    )
    normalized_fetch.setdefault(
        "changed",
        bool(fetch_result.get("changed") or change_summary.get("changed")),
    )
    normalized_fetch.setdefault(
        "change_detected",
        bool(
            fetch_result.get("change_detected")
            or change_summary.get("change_detected")
            or normalized_fetch.get("changed")
        ),
    )
    normalized_fetch.setdefault(
        "revalidation_required",
        bool(
            fetch_result.get("revalidation_required")
            or change_summary.get("requires_revalidation")
        ),
    )
    normalized_fetch.setdefault(
        "raw_path",
        fetch_result.get("raw_path")
        or change_summary.get("raw_path")
        or getattr(source, "raw_path", None),
    )
    normalized_fetch.setdefault(
        "content_sha256",
        fetch_result.get("content_sha256")
        or change_summary.get("current_fingerprint")
        or getattr(source, "content_sha256", None),
    )
    normalized_fetch.setdefault(
        "retry_due_at",
        fetch_result.get("retry_due_at")
        or change_summary.get("retry_due_at")
        or getattr(source, "next_refresh_due_at", None),
    )

    inventory = update_inventory_after_fetch(
        db,
        source=source,
        fetch_result=normalized_fetch,
        source_version_id=normalized_fetch.get("source_version_id"),
    )

    if inventory is None:
        inventory = sync_policy_source_into_inventory(
            db,
            source=source,
            org_id=getattr(source, "org_id", None),
        )

    if inventory is not None:
        _apply_inventory_authority_from_source(inventory, source)

        if _is_rejected_discovered_source(source):
            inventory.lifecycle_state = "failed"
            inventory.crawl_status = "failed"
            inventory.refresh_state = "failed"
            inventory.refresh_status_reason = "rejected_discovery_candidate"
            inventory.next_refresh_step = "ignore"

            if hasattr(inventory, "candidate_status_reason"):
                inventory.candidate_status_reason = "rejected_discovery_candidate"

            if hasattr(inventory, "is_official_candidate"):
                inventory.is_official_candidate = False

        elif not bool(normalized_fetch.get("ok", False)) and getattr(inventory, "refresh_state", None) in {None, "", "healthy"}:
            inventory.refresh_state = "failed"
            inventory.refresh_status_reason = str(
                normalized_fetch.get("fetch_error")
                or normalized_fetch.get("status_reason")
                or "fetch_failed"
            )

        elif bool(normalized_fetch.get("revalidation_required", False)) and getattr(inventory, "refresh_state", None) == "healthy":
            inventory.refresh_state = "validating"
            inventory.refresh_status_reason = str(
                normalized_fetch.get("revalidation_reason")
                or "revalidation_required"
            )

        db.add(inventory)
        db.flush()

    category_coverage = {}
    if inventory is not None:
        category_coverage = summarize_inventory_category_coverage(
            db,
            org_id=getattr(inventory, "org_id", None),
            state=getattr(inventory, "state", None) or getattr(source, "state", None) or "MI",
            county=getattr(inventory, "county", None) or getattr(source, "county", None),
            city=getattr(inventory, "city", None) or getattr(source, "city", None),
            pha_name=getattr(inventory, "pha_name", None) or getattr(source, "pha_name", None),
            program_type=getattr(inventory, "program_type", None),
            required_categories=_source_category_hints(source, inventory),
        )

    return _inventory_result_shape(inventory, change_summary, category_coverage=category_coverage)


def _inventory_nonblocking_failed_with_artifacts(source: PolicySource) -> bool:
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    authority_use_type = str(getattr(source, "authority_use_type", "") or "").strip().lower()
    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
    notes = str(getattr(source, "notes", "") or "").lower()
    publication_type = str(getattr(source, "publication_type", "") or "").strip().lower()
    try:
        http_status = int(getattr(source, "http_status", 0) or 0)
    except Exception:
        http_status = 0
    try:
        retry_count = int(getattr(source, "refresh_retry_count", 0) or 0)
    except Exception:
        retry_count = 0

    weakish = authority_tier in {"derived_or_inferred", ""} or authority_use_type in {"weak", ""}
    failed = freshness_status in {"fetch_failed", "error", "blocked"} or refresh_state == "failed" or http_status in {403, 404, 410}
    repeated = retry_count >= 1
    alt_backed = publication_type in {"official_document", "official_form", "legal_code"} or "[curated]" in notes or "artifact" in notes or "catalog" in notes
    return bool(weakish and failed and repeated and alt_backed)


_final_inventory_sync_original = sync_crawl_result_to_inventory

def sync_crawl_result_to_inventory(
    db: Session,
    *,
    source: PolicySource,
    fetch_result: dict[str, Any],
) -> dict[str, Any]:
    result = dict(_final_inventory_sync_original(db, source=source, fetch_result=fetch_result) or {})
    inventory = None
    try:
        inventory = sync_policy_source_into_inventory(
            db,
            source=source,
            org_id=getattr(source, "org_id", None),
        )
    except Exception:
        inventory = None

    if inventory is not None and _inventory_nonblocking_failed_with_artifacts(source):
        inventory.next_refresh_step = "ignore"
        inventory.revalidation_required = False
        inventory.refresh_state = "ignored"
        inventory.refresh_status_reason = "artifact_backed_nonblocking_failed_source_skipped"
        db.add(inventory)
        db.flush()
        result.update({
            "ok": True,
            "inventory_id": int(getattr(inventory, "id", 0) or 0),
            "lifecycle_state": getattr(inventory, "lifecycle_state", None),
            "crawl_status": getattr(inventory, "crawl_status", None),
            "refresh_state": "ignored",
            "refresh_status_reason": "artifact_backed_nonblocking_failed_source_skipped",
            "next_refresh_step": "ignore",
            "revalidation_required": False,
            "authority_use_type": getattr(inventory, "authority_use_type", None),
        })

    inv = inventory
    if inv is None:
        try:
            stmt = select(PolicySourceInventory).where(PolicySourceInventory.policy_source_id == int(getattr(source, "id", 0) or 0))
            inv = db.scalars(stmt.order_by(PolicySourceInventory.id.desc())).first()
        except Exception:
            inv = None

    if inv is not None:
        result["category_coverage"] = summarize_inventory_category_coverage(
            db,
            org_id=getattr(inv, "org_id", None),
            state=getattr(inv, "state", None) or getattr(source, "state", None) or "MI",
            county=getattr(inv, "county", None) or getattr(source, "county", None),
            city=getattr(inv, "city", None) or getattr(source, "city", None),
            pha_name=getattr(inv, "pha_name", None) or getattr(source, "pha_name", None),
            program_type=getattr(inv, "program_type", None),
            required_categories=_source_category_hints(source, inv),
        )
    return result
