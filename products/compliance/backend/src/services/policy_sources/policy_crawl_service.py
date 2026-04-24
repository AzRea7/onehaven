from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from onehaven_platform.backend.src.policy_models import PolicySource
from products.compliance.backend.src.services.policy_sources.discovery_service import sync_policy_source_into_inventory, update_inventory_after_fetch


def _change_summary(fetch_result: dict[str, Any]) -> dict[str, Any]:
    raw = fetch_result.get("change_summary")
    return dict(raw) if isinstance(raw, dict) else {}


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


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
        fetch_result.get("previous_fingerprint") or change_summary.get("previous_fingerprint"),
    )
    normalized_fetch.setdefault(
        "comparison_state",
        fetch_result.get("comparison_state") or change_summary.get("comparison_state"),
    )
    normalized_fetch.setdefault(
        "change_kind",
        fetch_result.get("change_kind") or change_summary.get("change_kind"),
    )
    normalized_fetch.setdefault(
        "actionable_outcome",
        fetch_result.get("actionable_outcome") or change_summary.get("actionable_outcome"),
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
        bool(fetch_result.get("revalidation_required") or change_summary.get("requires_revalidation")),
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
    normalized_fetch.setdefault(
        "refresh_state",
        fetch_result.get("refresh_state") or getattr(source, "refresh_state", None),
    )
    normalized_fetch.setdefault(
        "status_reason",
        fetch_result.get("status_reason") or getattr(source, "refresh_status_reason", None),
    )
    normalized_fetch.setdefault(
        "next_step",
        fetch_result.get("next_step") or getattr(source, "next_refresh_step", None),
    )
    normalized_fetch.setdefault(
        "validation_due_at",
        fetch_result.get("validation_due_at") or getattr(source, "validation_due_at", None),
    )
    normalized_fetch.setdefault(
        "http_status",
        fetch_result.get("http_status") or getattr(source, "http_status", None),
    )
    normalized_fetch.setdefault(
        "fetch_error",
        fetch_result.get("fetch_error") or getattr(source, "refresh_status_reason", None),
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
    return {
        "ok": inventory is not None,
        "inventory_id": int(inventory.id) if inventory is not None else None,
        "lifecycle_state": getattr(inventory, "lifecycle_state", None) if inventory is not None else None,
        "crawl_status": getattr(inventory, "crawl_status", None) if inventory is not None else None,
        "refresh_state": getattr(inventory, "refresh_state", None) if inventory is not None else None,
        "refresh_status_reason": getattr(inventory, "refresh_status_reason", None) if inventory is not None else None,
        "next_refresh_step": getattr(inventory, "next_refresh_step", None) if inventory is not None else None,
        "revalidation_required": bool(getattr(inventory, "revalidation_required", False)) if inventory is not None else False,
        "validation_due_at": _iso_or_none(getattr(inventory, "validation_due_at", None)) if inventory is not None else None,
        "current_source_version_id": getattr(inventory, "current_source_version_id", None) if inventory is not None else None,
        "last_change_summary": change_summary,
    }


from products.compliance.backend.src.services.policy_sources.fetch_service import fetch_policy_source_candidate
from products.compliance.backend.src.services.policy_sources.discovery_service import discover_source_family_candidates


def _resolution_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    out = {"fetched": 0, "manual_required": 0, "blocked": 0, "unresolved": 0, "failed": 0}
    for row in rows:
        key = str(row.get("resolution") or "failed").strip().lower()
        if key in out:
            out[key] += 1
    return out


def crawl_source_family_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    result = fetch_policy_source_candidate(candidate)
    result["category"] = candidate.get("category")
    result["source_family_id"] = candidate.get("source_family_id")
    result["jurisdiction_id"] = candidate.get("jurisdiction_id")
    result["authority_tier"] = candidate.get("authority_tier")
    result["official_domain"] = candidate.get("official_domain")
    result["host"] = candidate.get("host")
    result["pdf_lookup_names"] = candidate.get("pdf_lookup_names") or result.get("pdf_lookup_names") or []
    result["has_local_pdf"] = bool(candidate.get("has_local_pdf"))
    result["discovery_origin"] = candidate.get("discovery_origin")
    result["source_label"] = candidate.get("source_label")
    return result


def crawl_discovered_candidates(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None = None,
    program_type: str | None = None,
    jurisdiction_id: int | None = None,
    expected_categories: list[str] | None = None,
    expected_tiers: list[str] | None = None,
    commit: bool = False,
) -> dict[str, Any]:
    discovery = discover_source_family_candidates(
        db,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        pha_name=pha_name,
        program_type=program_type,
        jurisdiction_id=jurisdiction_id,
        expected_categories=expected_categories,
        expected_tiers=expected_tiers,
        commit=False,
    )
    results: list[dict[str, Any]] = []
    for candidate in discovery.get("candidates", []):
        results.append(crawl_source_family_candidate(candidate))
    if commit:
        db.commit()
    else:
        db.flush()
    counts = _resolution_counts(results)
    return {
        "ok": True,
        "candidate_count": int(discovery.get("candidate_count") or 0),
        "fetched_count": counts["fetched"],
        "manual_required_count": counts["manual_required"],
        "blocked_count": counts["blocked"],
        "unresolved_count": counts["unresolved"],
        "failed_count": counts["failed"],
        "results": results,
        "discovery": discovery,
        "resolution_counts": counts,
        "pdf_result_count": sum(1 for r in results if str(r.get("fetch_mode") or "").strip().lower() == "pdf"),
        "local_pdf_result_count": sum(1 for r in results if bool(r.get("local_pdf_path"))),
    }
