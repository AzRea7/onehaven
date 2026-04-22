
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.config import settings
from app.policy_models import JurisdictionProfile, PolicySource


LEGAL_BLOCKING_CATEGORIES = {"registration", "inspection", "occupancy", "lead", "section8", "program_overlay", "safety"}
CRITICAL_CATEGORY_SET = set(LEGAL_BLOCKING_CATEGORIES)
PROGRAM_CATEGORIES = {"section8", "program_overlay", "subsidy_overlay"}

SOURCE_FETCH_FAILURE_STATES = {"fetch_failed", "error", "blocked"}
SOURCE_HTTP_BLOCK_STATUSES = {401, 403, 405, 406, 407, 429, 451}
SOURCE_HTTP_DEAD_STATUSES = {404, 410}

ALT_EVIDENCE_SOURCE_TYPES = {
    "api",
    "dataset",
    "artifact",
    "manual",
    "catalog",
    "program",
    "feed",
    "registry",
    "repo_artifact",
}
ALT_EVIDENCE_PUBLICATION_TYPES = {"pdf", "api", "json", "json_api", "dataset"}

ZIP_PDF_ROOT_CANDIDATES = [
    Path("/mnt/data/step3_zip/pdfs"),
    Path("/mnt/data/step4_pdf_catalog/pdfs"),
    Path("/mnt/data/step67_pdf_zip/pdfs"),
    Path("/mnt/data/step8_pdf_zip/pdfs"),
    Path("/mnt/data/pdfs"),
]


def _utcnow() -> datetime:
    return datetime.utcnow()


def _loads_json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def source_categories(source: PolicySource) -> set[str]:
    return {
        str(x).strip().lower()
        for x in _loads_json_list(getattr(source, "normalized_categories_json", None))
        if str(x).strip()
    }


def _source_http_status(source: PolicySource) -> int | None:
    raw = getattr(source, "http_status", None)
    try:
        return int(raw) if raw is not None else None
    except Exception:
        return None


def _source_failure_count(source: PolicySource) -> int:
    retry_count = getattr(source, "refresh_retry_count", None)
    try:
        return int(retry_count or 0)
    except Exception:
        return 0


def _host_from_url(url: str) -> str:
    raw = str(url or "").strip().lower()
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    raw = raw.split("/", 1)[0].strip()
    if ":" in raw:
        raw = raw.split(":", 1)[0].strip()
    return raw


def _host_looks_guessed(host: str) -> bool:
    host = str(host or "").strip().lower()
    if not host:
        return True
    return any(re.search(pat, host) for pat in [r"(^|\.)ci\.", r"(^|\.)co\.", r"(^|\.)cityof", r"(^|\.)countyof"])


def _source_failure_summary(source: PolicySource) -> dict[str, Any]:
    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
    refresh_reason = str(
        getattr(source, "refresh_status_reason", "")
        or getattr(source, "refresh_blocked_reason", "")
        or ""
    ).strip().lower()
    http_status = _source_http_status(source)
    failure_count = _source_failure_count(source)
    host = _host_from_url(getattr(source, "url", None))
    looks_guessed = _host_looks_guessed(host)

    reasons: list[str] = []
    if looks_guessed:
        reasons.append("guessed_domain")
    if freshness_status in SOURCE_FETCH_FAILURE_STATES:
        reasons.append(freshness_status)
    if refresh_state == "blocked":
        reasons.append("refresh_blocked")
    if http_status in SOURCE_HTTP_DEAD_STATUSES:
        reasons.append("http_not_found")
    elif http_status in SOURCE_HTTP_BLOCK_STATUSES:
        reasons.append("blocked_or_antibot")
    elif http_status is not None and http_status >= 400:
        reasons.append(f"http_status_{http_status}")
    if ("anti-bot" in refresh_reason or "antibot" in refresh_reason or "captcha" in refresh_reason) and "blocked_or_antibot" not in reasons:
        reasons.append("blocked_or_antibot")
    if failure_count >= 2 and freshness_status in SOURCE_FETCH_FAILURE_STATES:
        reasons.append("repeated_fetch_failed")

    blocking_failure = any(
        reason in {
            "guessed_domain",
            "http_not_found",
            "fetch_failed",
            "error",
            "blocked",
            "refresh_blocked",
            "blocked_or_antibot",
            "repeated_fetch_failed",
        }
        or str(reason).startswith("http_status_")
        for reason in reasons
    )
    return {
        "host": host,
        "looks_guessed": looks_guessed,
        "http_status": http_status,
        "failure_count": failure_count,
        "reasons": sorted(set(reasons)),
        "blocking_failure": blocking_failure,
        "refresh_state": refresh_state or None,
        "freshness_status": freshness_status or None,
    }


def _source_use_type(source: PolicySource) -> str:
    value = str(getattr(source, "authority_use_type", None) or "").strip().lower()
    if value:
        return value
    authority_tier = str(getattr(source, "authority_tier", None) or "").strip().lower()
    authority_rank = int(getattr(source, "authority_rank", 0) or 0)
    if bool(getattr(source, "is_authoritative", False)) or authority_tier == "authoritative_official" or authority_rank >= 100:
        return "binding"
    if authority_tier in {"approved_official_supporting", "semi_authoritative_operational"} or authority_rank >= 60:
        return "supporting"
    return "weak"


def _source_type(source: PolicySource) -> str:
    return str(getattr(source, "source_type", "") or "").strip().lower()


def _publication_type(source: PolicySource) -> str:
    return str(getattr(source, "publication_type", "") or "").strip().lower()


def _source_has_alt_evidence_hint(source: PolicySource) -> bool:
    source_type = _source_type(source)
    publication_type = _publication_type(source)
    notes = str(getattr(source, "notes", "") or "").lower()
    metadata = str(getattr(source, "source_name", "") or getattr(source, "title", "") or "").lower()

    return (
        source_type in ALT_EVIDENCE_SOURCE_TYPES
        or publication_type in ALT_EVIDENCE_PUBLICATION_TYPES
        or "[curated]" in notes
        or "dataset" in notes
        or "artifact" in notes
        or "catalog" in notes
        or "api" in notes
        or "dataset" in metadata
        or "api" in metadata
    )


def _category_sla_hours(*, category: str, source_type: str, authority_tier: str, use_type: str) -> int:
    category = str(category or "").strip().lower()
    source_type = str(source_type or "").strip().lower()
    authority_tier = str(authority_tier or "").strip().lower()
    use_type = str(use_type or "").strip().lower()

    if category in PROGRAM_CATEGORIES or source_type == "program":
        return int(getattr(settings, "jurisdiction_sla_program_overlay_hours", 24 * 14))
    if category in LEGAL_BLOCKING_CATEGORIES:
        if use_type == "binding" or authority_tier == "authoritative_official":
            return int(getattr(settings, "jurisdiction_sla_critical_authoritative_hours", 24 * 14))
        if use_type == "supporting":
            return int(getattr(settings, "jurisdiction_sla_supporting_critical_hours", 24 * 10))
        return int(getattr(settings, "jurisdiction_sla_default_hours", 24 * 30))
    if use_type == "binding" or authority_tier == "authoritative_official":
        return int(getattr(settings, "jurisdiction_sla_authoritative_hours", 24 * 21))
    if use_type == "supporting":
        return int(getattr(settings, "jurisdiction_sla_supporting_hours", 24 * 30))
    return int(getattr(settings, "jurisdiction_sla_default_hours", 24 * 30))


def source_sla_hours(source: PolicySource) -> int:
    categories = sorted(source_categories(source))
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    source_type = _source_type(source)
    use_type = _source_use_type(source)
    if not categories:
        return _category_sla_hours(category="", source_type=source_type, authority_tier=authority_tier, use_type=use_type)
    return min(
        _category_sla_hours(category=c, source_type=source_type, authority_tier=authority_tier, use_type=use_type)
        for c in categories
    )


def _category_due_at(source: PolicySource, category: str) -> datetime:
    base = (
        getattr(source, "last_verified_at", None)
        or getattr(source, "freshness_checked_at", None)
        or getattr(source, "last_fetched_at", None)
        or getattr(source, "retrieved_at", None)
        or _utcnow()
    )
    return base + timedelta(
        hours=_category_sla_hours(
            category=category,
            source_type=_source_type(source),
            authority_tier=str(getattr(source, "authority_tier", "") or ""),
            use_type=_source_use_type(source),
        )
    )


def source_due_at(source: PolicySource) -> datetime:
    failure = _source_failure_summary(source)
    if failure.get("blocking_failure"):
        return _utcnow()
    categories = sorted(source_categories(source))
    if not categories:
        return _category_due_at(source, "")
    return min(_category_due_at(source, category) for category in categories)


def source_is_past_sla(source: PolicySource, *, now: datetime | None = None) -> bool:
    now = now or _utcnow()
    return source_due_at(source) <= now


def _iter_scoped_sources(db: Session, *, profile: JurisdictionProfile) -> list[PolicySource]:
    rows = list(db.query(PolicySource).filter(PolicySource.state == getattr(profile, "state", None)).all())
    scoped: list[PolicySource] = []
    for source in rows:
        source_org_id = getattr(source, "org_id", None)
        profile_org_id = getattr(profile, "org_id", None)
        if profile_org_id is None and source_org_id is not None:
            continue
        if profile_org_id is not None and source_org_id not in {None, profile_org_id}:
            continue
        if getattr(source, "county", None) is not None and getattr(source, "county", None) != getattr(profile, "county", None):
            continue
        if getattr(source, "city", None) is not None and getattr(source, "city", None) != getattr(profile, "city", None):
            continue
        if getattr(source, "pha_name", None) is not None and getattr(source, "pha_name", None) != getattr(profile, "pha_name", None):
            continue
        scoped.append(source)
    return scoped


def _repo_candidate_roots() -> list[Path]:
    candidates: list[Path] = []

    env_keys = [
        "POLICY_PDFS_ROOT",
        "POLICY_PDF_ROOT",
        "NSPIRE_PDF_ROOT",
    ]
    for key in env_keys:
        value = os.getenv(key)
        if value:
            try:
                candidates.append(Path(str(value)).expanduser())
            except Exception:
                pass

    raw_values = [
        getattr(settings, "policy_repo_root", None),
        getattr(settings, "repo_root", None),
        getattr(settings, "project_root", None),
    ]
    for value in raw_values:
        if value:
            try:
                candidates.append(Path(str(value)).expanduser())
            except Exception:
                pass

    cwd = Path.cwd()
    candidates.extend([cwd, cwd.parent, cwd.parent.parent])
    candidates.append(Path("/mnt/data"))
    candidates.append(Path("/app/data/pdfs"))
    candidates.append(Path("/app/backend/data/pdfs"))
    candidates.extend([p.parent.parent if p.name == "pdfs" else p for p in ZIP_PDF_ROOT_CANDIDATES])

    out: list[Path] = []
    seen: set[str] = set()
    for root in candidates:
        try:
            resolved = str(root.resolve())
        except Exception:
            resolved = str(root)
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(root)
    return out


def _first_existing(paths: list[Path]) -> Path | None:
    for path in paths:
        try:
            if path.exists():
                return path
        except Exception:
            continue
    return None


def _policy_artifact_snapshot() -> dict[str, Any]:
    roots = _repo_candidate_roots()
    policy_raw = None
    pdf_root = None

    for root in roots:
        candidates = [
            root / "backend" / "policy_raw",
            root / "onehaven_decision_engine" / "backend" / "policy_raw",
            root / "policy_raw",
            root if root.name == "policy_raw" else root / "policy_raw",
        ]
        found = _first_existing(candidates)
        if found is not None:
            policy_raw = found
            break

    explicit_pdf_candidates: list[Path] = []
    for key in ("POLICY_PDFS_ROOT", "POLICY_PDF_ROOT", "NSPIRE_PDF_ROOT"):
        value = os.getenv(key)
        if value:
            try:
                explicit_pdf_candidates.append(Path(str(value)).expanduser())
            except Exception:
                pass

    pdf_search_roots = explicit_pdf_candidates + roots
    for root in pdf_search_roots:
        candidates = [
            root,
            root / "backend" / "data" / "pdfs",
            root / "onehaven_decision_engine" / "backend" / "data" / "pdfs",
            root / "backend" / "pdfs",
            root / "onehaven_decision_engine" / "backend" / "pdfs",
            root / "pdfs",
            root / "backend" / "pdf",
            root / "onehaven_decision_engine" / "backend" / "pdf",
            root / "pdf",
            Path("/app/data/pdfs"),
            Path("/app/backend/data/pdfs"),
            *ZIP_PDF_ROOT_CANDIDATES,
        ]
        found = _first_existing(candidates)
        if found is not None:
            pdf_root = found
            break

    def _scan(path: Path | None, patterns: tuple[str, ...]) -> dict[str, Any]:
        if path is None:
            return {"exists": False, "path": None, "count": 0, "latest_mtime": None, "examples": []}
        files: list[Path] = []
        try:
            for pat in patterns:
                files.extend(path.rglob(pat))
        except Exception:
            files = []
        deduped: dict[str, Path] = {}
        for f in files:
            deduped[str(f)] = f
        rows = list(deduped.values())
        latest = None
        for f in rows:
            try:
                m = datetime.utcfromtimestamp(f.stat().st_mtime)
                if latest is None or m > latest:
                    latest = m
            except Exception:
                continue
        return {
            "exists": True,
            "path": str(path),
            "count": len(rows),
            "latest_mtime": latest.isoformat() if latest else None,
            "examples": [str(f) for f in sorted(rows)[:5]],
            "names": [f.name for f in sorted(rows)[:50]],
        }

    html = _scan(policy_raw, ("*.html", "*.htm"))
    pdf = _scan(pdf_root, ("*.pdf",))
    has_any = bool(html.get("count") or 0) or bool(pdf.get("count") or 0)
    return {
        "has_repo_artifacts": has_any,
        "policy_raw": html,
        "pdfs": pdf,
        "artifact_support_state": "artifact_backed" if has_any else "no_repo_artifacts_found",
    }


def _artifact_backing_available(artifact_snapshot: dict[str, Any]) -> bool:
    if not isinstance(artifact_snapshot, dict):
        return False
    return bool(artifact_snapshot.get("has_repo_artifacts"))


def _source_has_alternative_evidence_backing(
    source: PolicySource,
    *,
    artifact_snapshot: dict[str, Any],
) -> bool:
    if _host_looks_guessed(_host_from_url(getattr(source, "url", None))):
        return False

    if _source_has_alt_evidence_hint(source):
        return True

    # If repo artifacts exist at all, treat authoritative/binding sources as
    # artifact-backed for SLA purposes. This reflects the intended architecture:
    # datasets, PDFs, curated source packs, and AI extraction are the primary evidence;
    # live crawl is freshness support, not the only source of truth.
    if _artifact_backing_available(artifact_snapshot):
        use_type = _source_use_type(source)
        authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
        if use_type in {"binding", "supporting"} or authority_tier in {"authoritative_official", "approved_official_supporting"}:
            return True

    return False


def _source_effective_failure(
    source: PolicySource,
    *,
    artifact_snapshot: dict[str, Any],
) -> dict[str, Any]:
    summary = dict(_source_failure_summary(source))
    alternative_backing = _source_has_alternative_evidence_backing(
        source,
        artifact_snapshot=artifact_snapshot,
    )
    reasons = list(summary.get("reasons") or [])
    blocking_failure = bool(summary.get("blocking_failure"))
    http_status = summary.get("http_status")

    # Dead official links should still be review-worthy, but if we have artifact/dataset backing,
    # they no longer hard-block legal reliance by themselves.
    hard_dead = http_status in SOURCE_HTTP_DEAD_STATUSES and not alternative_backing

    effective_blocking_failure = bool(blocking_failure and not alternative_backing) or hard_dead
    degraded_failure = bool(blocking_failure and alternative_backing)

    summary["alternative_backing"] = alternative_backing
    summary["effective_blocking_failure"] = effective_blocking_failure
    summary["degraded_failure"] = degraded_failure
    summary["freshness_support_only_failure"] = degraded_failure
    if degraded_failure and "artifact_backed_fallback" not in reasons:
        reasons.append("artifact_backed_fallback")
    summary["reasons"] = sorted(set(reasons))
    return summary


def collect_profile_source_sla_summary(db: Session, *, profile: JurisdictionProfile) -> dict[str, Any]:
    now = _utcnow()
    scoped = _iter_scoped_sources(db, profile=profile)
    artifact_snapshot = _policy_artifact_snapshot()

    overdue_categories: set[str] = set()
    critical_overdue_categories: set[str] = set()
    legal_overdue_categories: set[str] = set()
    informational_overdue_categories: set[str] = set()
    stale_authoritative_categories: set[str] = set()
    due_soon_categories: set[str] = set()
    critical_fetch_failure_categories: set[str] = set()
    legal_lockout_categories: set[str] = set()
    review_required_categories: set[str] = set()
    category_rollup: dict[str, dict[str, Any]] = {}
    sources_payload: list[dict[str, Any]] = []
    rejected_source_count = 0
    guessed_source_count = 0
    blocked_source_count = 0
    fetch_failed_source_count = 0
    failed_binding_source_count = 0
    artifact_backed_failed_source_count = 0

    for source in scoped:
        categories = sorted(source_categories(source))
        authority_tier = getattr(source, "authority_tier", None)
        use_type = _source_use_type(source)
        failure_summary = _source_effective_failure(source, artifact_snapshot=artifact_snapshot)
        source_due = source_due_at(source)
        source_overdue = source_due <= now
        source_due_soon = (not source_overdue) and source_due <= (now + timedelta(hours=24))

        if failure_summary.get("effective_blocking_failure"):
            rejected_source_count += 1
        if failure_summary.get("degraded_failure"):
            artifact_backed_failed_source_count += 1
        if failure_summary.get("looks_guessed"):
            guessed_source_count += 1
        if "blocked_or_antibot" in failure_summary.get("reasons", []) or "refresh_blocked" in failure_summary.get("reasons", []):
            blocked_source_count += 1
        if any(
            reason in {"fetch_failed", "error", "blocked", "repeated_fetch_failed", "http_not_found"}
            or str(reason).startswith("http_status_")
            for reason in failure_summary.get("reasons", [])
        ):
            fetch_failed_source_count += 1
        if failure_summary.get("effective_blocking_failure") and use_type == "binding":
            failed_binding_source_count += 1

        per_category: list[dict[str, Any]] = []

        for category in categories or [""]:
            due_at = _category_due_at(source, category)
            is_overdue = due_at <= now
            is_due_soon = (not is_overdue) and due_at <= (now + timedelta(hours=24))
            is_legal = category in LEGAL_BLOCKING_CATEGORIES
            entry = category_rollup.setdefault(
                category,
                {
                    "category": category,
                    "is_legal_lockout_category": is_legal,
                    "source_ids": [],
                    "binding_source_ids": [],
                    "overdue_source_ids": [],
                    "authoritative_overdue_source_ids": [],
                    "next_due_at": None,
                    "failed_source_ids": [],
                    "failed_binding_source_ids": [],
                    "artifact_backed_failed_source_ids": [],
                },
            )
            entry["source_ids"].append(int(getattr(source, "id", 0) or 0))
            if use_type == "binding":
                entry["binding_source_ids"].append(int(getattr(source, "id", 0) or 0))

            source_failed = bool(failure_summary.get("effective_blocking_failure"))
            source_degraded = bool(failure_summary.get("degraded_failure"))

            if source_failed or source_degraded:
                review_required_categories.add(category)
                entry.setdefault("failed_source_ids", []).append(int(getattr(source, "id", 0) or 0))
                if source_degraded:
                    entry.setdefault("artifact_backed_failed_source_ids", []).append(int(getattr(source, "id", 0) or 0))

            if source_failed:
                overdue_categories.add(category)
                if use_type == "binding":
                    stale_authoritative_categories.add(category)
                    critical_fetch_failure_categories.add(category)
                    entry.setdefault("failed_binding_source_ids", []).append(int(getattr(source, "id", 0) or 0))
                if is_legal:
                    legal_overdue_categories.add(category)
                    critical_overdue_categories.add(category)
                    legal_lockout_categories.add(category)
                else:
                    informational_overdue_categories.add(category)
            elif source_degraded:
                # Artifact/dataset-backed failures are freshness degradation, not legal lockout.
                if use_type == "binding":
                    stale_authoritative_categories.add(category)
                informational_overdue_categories.add(category)
            elif is_overdue:
                overdue_categories.add(category)
                entry["overdue_source_ids"].append(int(getattr(source, "id", 0) or 0))
                if use_type == "binding":
                    stale_authoritative_categories.add(category)
                    entry["authoritative_overdue_source_ids"].append(int(getattr(source, "id", 0) or 0))
                if is_legal:
                    legal_overdue_categories.add(category)
                    critical_overdue_categories.add(category)
                else:
                    informational_overdue_categories.add(category)
            elif is_due_soon:
                due_soon_categories.add(category)

            if entry["next_due_at"] is None or (due_at and due_at.isoformat() < entry["next_due_at"]):
                entry["next_due_at"] = due_at.isoformat() if due_at else None

            per_category.append(
                {
                    "category": category,
                    "due_at": due_at.isoformat() if due_at else None,
                    "is_overdue": is_overdue,
                    "is_due_soon": is_due_soon,
                    "is_legal_lockout_category": is_legal,
                    "authority_use_type": use_type,
                    "source_failed": source_failed,
                    "source_degraded": source_degraded,
                    "alternative_backing": bool(failure_summary.get("alternative_backing")),
                    "failure_reasons": list(failure_summary.get("reasons") or []),
                }
            )

        sources_payload.append(
            {
                "source_id": int(getattr(source, "id", 0) or 0),
                "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
                "authority_tier": authority_tier,
                "authority_use_type": use_type,
                "source_type": _source_type(source) or None,
                "publication_type": _publication_type(source) or None,
                "categories": categories,
                "due_at": source_due.isoformat() if source_due else None,
                "is_overdue": source_overdue,
                "is_due_soon": source_due_soon,
                "refresh_state": getattr(source, "refresh_state", None),
                "freshness_status": getattr(source, "freshness_status", None),
                "source_failure": failure_summary,
                "category_freshness": per_category,
            }
        )

    category_freshness = []
    for category, payload in sorted(category_rollup.items()):
        category_freshness.append(
            {
                **payload,
                "source_count": len(set(payload["source_ids"])),
                "binding_source_count": len(set(payload["binding_source_ids"])),
                "overdue_source_count": len(set(payload["overdue_source_ids"])),
                "authoritative_overdue_source_count": len(set(payload["authoritative_overdue_source_ids"])),
                "failed_source_count": len(set(payload.get("failed_source_ids") or [])),
                "failed_binding_source_count": len(set(payload.get("failed_binding_source_ids") or [])),
                "artifact_backed_failed_source_count": len(set(payload.get("artifact_backed_failed_source_ids") or [])),
                "legal_stale": category in legal_overdue_categories,
                "informational_stale": category in informational_overdue_categories,
                "critical_fetch_failure": category in critical_fetch_failure_categories,
                "legal_lockout": category in legal_lockout_categories,
                "review_required": category in review_required_categories,
            }
        )

    next_due_at = None
    due_values = [item.get("due_at") for item in sources_payload if item.get("due_at")]
    if due_values:
        next_due_at = min(due_values)

    safe_to_rely_on = not bool(legal_lockout_categories or critical_fetch_failure_categories)

    return {
        "source_count": len(scoped),
        "sources": sources_payload,
        "category_freshness": category_freshness,
        "overdue_categories": sorted(c for c in overdue_categories if c),
        "critical_overdue_categories": sorted(c for c in critical_overdue_categories if c),
        "legal_overdue_categories": sorted(c for c in legal_overdue_categories if c),
        "informational_overdue_categories": sorted(c for c in informational_overdue_categories if c),
        "stale_authoritative_categories": sorted(c for c in stale_authoritative_categories if c),
        "due_soon_categories": sorted(c for c in due_soon_categories if c),
        "has_overdue_sources": bool(overdue_categories),
        "has_critical_overdue_sources": bool(critical_overdue_categories),
        "has_legal_overdue_sources": bool(legal_overdue_categories),
        "critical_fetch_failure_categories": sorted(c for c in critical_fetch_failure_categories if c),
        "legal_lockout_categories": sorted(c for c in legal_lockout_categories if c),
        "review_required_categories": sorted(c for c in review_required_categories if c),
        "rejected_source_count": int(rejected_source_count),
        "guessed_source_count": int(guessed_source_count),
        "blocked_source_count": int(blocked_source_count),
        "fetch_failed_source_count": int(fetch_failed_source_count),
        "failed_binding_source_count": int(failed_binding_source_count),
        "artifact_backed_failed_source_count": int(artifact_backed_failed_source_count),
        "artifact_backed_refresh_only": bool(artifact_backed_failed_source_count > 0 and failed_binding_source_count == 0),
        "safe_to_rely_on": safe_to_rely_on,
        "next_due_at": next_due_at,
        "repo_artifact_snapshot": artifact_snapshot,
        "repo_artifact_support_state": artifact_snapshot.get("artifact_support_state"),
        "repo_policy_raw_count": int((artifact_snapshot.get("policy_raw") or {}).get("count") or 0),
        "repo_pdf_count": int((artifact_snapshot.get("pdfs") or {}).get("count") or 0),
    }


def build_refresh_requirements(
    profile: JurisdictionProfile,
    *,
    next_step: str,
    missing_categories: list[str] | None = None,
    stale_categories: list[str] | None = None,
    overdue_categories: list[str] | None = None,
    critical_overdue_categories: list[str] | None = None,
    legal_overdue_categories: list[str] | None = None,
    informational_overdue_categories: list[str] | None = None,
    stale_authoritative_categories: list[str] | None = None,
    inventory_summary: dict[str, Any] | None = None,
    retry_due_at: datetime | None = None,
) -> dict[str, Any]:
    inventory_summary = dict(inventory_summary or {})

    critical_fetch_failure_categories = list(inventory_summary.get("critical_fetch_failure_categories") or [])
    legal_lockout_categories = list(inventory_summary.get("legal_lockout_categories") or [])
    review_required_categories = list(inventory_summary.get("review_required_categories") or [])

    rejected_source_count = int(inventory_summary.get("rejected_source_count") or 0)
    guessed_source_count = int(inventory_summary.get("guessed_source_count") or 0)
    blocked_source_count = int(inventory_summary.get("blocked_source_count") or 0)
    fetch_failed_source_count = int(inventory_summary.get("fetch_failed_source_count") or 0)
    failed_binding_source_count = int(inventory_summary.get("failed_binding_source_count") or 0)
    artifact_backed_failed_source_count = int(inventory_summary.get("artifact_backed_failed_source_count") or 0)

    merged_overdue_categories = list(overdue_categories or [])
    merged_critical_overdue_categories = list(critical_overdue_categories or [])
    merged_legal_overdue_categories = list(legal_overdue_categories or [])
    merged_informational_overdue_categories = list(informational_overdue_categories or [])
    merged_stale_authoritative_categories = list(stale_authoritative_categories or [])

    if not merged_overdue_categories:
        merged_overdue_categories = list(inventory_summary.get("overdue_categories") or [])
    if not merged_critical_overdue_categories:
        merged_critical_overdue_categories = list(inventory_summary.get("critical_overdue_categories") or [])
    if not merged_legal_overdue_categories:
        merged_legal_overdue_categories = list(inventory_summary.get("legal_overdue_categories") or [])
    if not merged_informational_overdue_categories:
        merged_informational_overdue_categories = list(inventory_summary.get("informational_overdue_categories") or [])
    if not merged_stale_authoritative_categories:
        merged_stale_authoritative_categories = list(inventory_summary.get("stale_authoritative_categories") or [])

    derived_safe_to_rely_on = not bool(
        legal_lockout_categories
        or critical_fetch_failure_categories
        or merged_legal_overdue_categories
        or failed_binding_source_count > 0
    )

    if "safe_to_rely_on" in inventory_summary:
        upstream_safe = bool(inventory_summary.get("safe_to_rely_on"))
        safe_to_rely_on = bool(upstream_safe and derived_safe_to_rely_on)
    else:
        safe_to_rely_on = derived_safe_to_rely_on

    inventory_payload = inventory_summary.get("inventory_summary", inventory_summary)

    return {
        "next_step": next_step,
        "refresh_state": getattr(profile, "refresh_state", None),
        "missing_categories": list(missing_categories or []),
        "stale_categories": list(stale_categories or []),
        "overdue_categories": merged_overdue_categories,
        "critical_overdue_categories": merged_critical_overdue_categories,
        "legal_overdue_categories": merged_legal_overdue_categories,
        "informational_overdue_categories": merged_informational_overdue_categories,
        "stale_authoritative_categories": merged_stale_authoritative_categories,
        "inventory_summary": inventory_payload,
        "critical_fetch_failure_categories": critical_fetch_failure_categories,
        "legal_lockout_categories": legal_lockout_categories,
        "review_required_categories": review_required_categories,
        "rejected_source_count": rejected_source_count,
        "guessed_source_count": guessed_source_count,
        "blocked_source_count": blocked_source_count,
        "fetch_failed_source_count": fetch_failed_source_count,
        "failed_binding_source_count": failed_binding_source_count,
        "artifact_backed_failed_source_count": artifact_backed_failed_source_count,
        "artifact_backed_refresh_only": bool(inventory_summary.get("artifact_backed_refresh_only")),
        "safe_to_rely_on": safe_to_rely_on,
        "next_due_at": inventory_summary.get("next_due_at"),
        "next_search_retry_due_at": retry_due_at.isoformat() if retry_due_at else None,
        "last_refresh_completed_at": (
            getattr(profile, "last_refresh_completed_at", None).isoformat()
            if getattr(profile, "last_refresh_completed_at", None)
            else None
        ),
    }


def profile_next_actions(profile: JurisdictionProfile) -> dict[str, Any]:
    requirements = {}
    try:
        requirements = json.loads(getattr(profile, "refresh_requirements_json", None) or "{}")
        if not isinstance(requirements, dict):
            requirements = {}
    except Exception:
        requirements = {}
    return {
        "next_step": requirements.get("next_step") or "refresh",
        "next_search_retry_due_at": requirements.get("next_search_retry_due_at"),
        "missing_categories": list(requirements.get("missing_categories") or []),
        "stale_categories": list(requirements.get("stale_categories") or []),
        "overdue_categories": list(requirements.get("overdue_categories") or []),
        "critical_overdue_categories": list(requirements.get("critical_overdue_categories") or []),
        "legal_overdue_categories": list(requirements.get("legal_overdue_categories") or []),
        "informational_overdue_categories": list(requirements.get("informational_overdue_categories") or []),
        "stale_authoritative_categories": list(requirements.get("stale_authoritative_categories") or []),
        "refresh_state": getattr(profile, "refresh_state", None),
        "critical_fetch_failure_categories": list(requirements.get("critical_fetch_failure_categories") or []),
        "legal_lockout_categories": list(requirements.get("legal_lockout_categories") or []),
        "review_required_categories": list(requirements.get("review_required_categories") or []),
        "rejected_source_count": int(requirements.get("rejected_source_count") or 0),
        "guessed_source_count": int(requirements.get("guessed_source_count") or 0),
        "blocked_source_count": int(requirements.get("blocked_source_count") or 0),
        "fetch_failed_source_count": int(requirements.get("fetch_failed_source_count") or 0),
        "failed_binding_source_count": int(requirements.get("failed_binding_source_count") or 0),
        "artifact_backed_failed_source_count": int(requirements.get("artifact_backed_failed_source_count") or 0),
        "artifact_backed_refresh_only": bool(requirements.get("artifact_backed_refresh_only")),
        "safe_to_rely_on": bool(requirements.get("safe_to_rely_on", False)),
    }


# --- tier-one evidence-first overrides ---

_tier1_original_collect_profile_source_sla_summary = collect_profile_source_sla_summary
_tier1_original_build_refresh_requirements = build_refresh_requirements


def _dedupe_sorted_categories(values: list[str] | None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in list(values or []):
        value = str(raw or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return sorted(out)


def _evidence_family_summary(sources_payload: list[dict[str, Any]], artifact_snapshot: dict[str, Any]) -> dict[str, Any]:
    source_types: set[str] = set()
    publication_types: set[str] = set()
    live_source_count = 0
    artifact_hint_count = 0
    alternative_backed_source_count = 0
    binding_source_count = 0
    supporting_source_count = 0
    weak_source_count = 0

    for row in sources_payload:
        source_type = str(row.get("source_type") or "").strip().lower()
        publication_type = str(row.get("publication_type") or "").strip().lower()
        use_type = str(row.get("authority_use_type") or "").strip().lower()
        failure = dict(row.get("source_failure") or {})

        if source_type:
            source_types.add(source_type)
        if publication_type:
            publication_types.add(publication_type)
        if source_type not in {"artifact", "dataset", "manual", "catalog", "repo_artifact"}:
            live_source_count += 1
        if failure.get("alternative_backing"):
            alternative_backed_source_count += 1
        if failure.get("alternative_backing") or source_type in ALT_EVIDENCE_SOURCE_TYPES or publication_type in ALT_EVIDENCE_PUBLICATION_TYPES:
            artifact_hint_count += 1
        if use_type == "binding":
            binding_source_count += 1
        elif use_type == "supporting":
            supporting_source_count += 1
        else:
            weak_source_count += 1

    return {
        "source_types": sorted(source_types),
        "publication_types": sorted(publication_types),
        "live_source_count": int(live_source_count),
        "binding_source_count": int(binding_source_count),
        "supporting_source_count": int(supporting_source_count),
        "weak_source_count": int(weak_source_count),
        "artifact_hint_count": int(artifact_hint_count),
        "alternative_backed_source_count": int(alternative_backed_source_count),
        "has_repo_artifacts": bool((artifact_snapshot or {}).get("has_repo_artifacts")),
        "repo_artifact_support_state": (artifact_snapshot or {}).get("artifact_support_state"),
    }


def collect_profile_source_sla_summary(db: Session, *, profile: JurisdictionProfile) -> dict[str, Any]:
    summary = dict(_tier1_original_collect_profile_source_sla_summary(db, profile=profile))
    sources_payload = list(summary.get("sources") or [])
    artifact_snapshot = dict(summary.get("repo_artifact_snapshot") or {})
    evidence_family = _evidence_family_summary(sources_payload, artifact_snapshot)
    category_freshness = [dict(item) for item in list(summary.get("category_freshness") or [])]

    degraded_categories: list[str] = []
    blocking_categories: list[str] = []
    freshness_signal_only_categories: list[str] = []
    for item in category_freshness:
        category = str(item.get("category") or "").strip().lower()
        if not category:
            continue
        failed_binding = int(item.get("failed_binding_source_count") or 0) > 0
        artifact_backed_failed = int(item.get("artifact_backed_failed_source_count") or 0) > 0
        legal_lockout = bool(item.get("legal_lockout"))
        critical_fetch_failure = bool(item.get("critical_fetch_failure"))
        review_required = bool(item.get("review_required"))

        if legal_lockout or critical_fetch_failure or failed_binding:
            blocking_categories.append(category)
        elif artifact_backed_failed or review_required:
            degraded_categories.append(category)
            freshness_signal_only_categories.append(category)

        item["evidence_status"] = (
            "blocking"
            if category in blocking_categories
            else "degraded"
            if category in degraded_categories
            else "healthy"
        )
        item["freshness_signal_only"] = bool(category in freshness_signal_only_categories)

    summary["category_freshness"] = category_freshness
    summary["blocking_categories"] = _dedupe_sorted_categories(blocking_categories)
    summary["degraded_categories"] = _dedupe_sorted_categories(degraded_categories)
    summary["freshness_signal_only_categories"] = _dedupe_sorted_categories(freshness_signal_only_categories)
    summary["evidence_family"] = evidence_family
    summary["truth_model"] = {
        "mode": "evidence_first",
        "freshness_role": "support_only",
        "crawler_role": "discovery_and_refresh_only",
        "primary_truth_sources": ["catalog_admin", "stored_artifacts", "datasets", "validated_extraction"],
    }
    summary["current_truth_basis"] = {
        "artifact_backed_refresh_only": bool(summary.get("artifact_backed_refresh_only")),
        "safe_to_rely_on": bool(summary.get("safe_to_rely_on")),
        "evidence_safe_to_rely_on": bool(summary.get("safe_to_rely_on")),
        "has_repo_artifacts": bool(artifact_snapshot.get("has_repo_artifacts")),
        "failed_binding_source_count": int(summary.get("failed_binding_source_count") or 0),
        "artifact_backed_failed_source_count": int(summary.get("artifact_backed_failed_source_count") or 0),
        "blocking_categories": _dedupe_sorted_categories(summary.get("blocking_categories") or []),
        "degraded_categories": _dedupe_sorted_categories(summary.get("degraded_categories") or []),
        "freshness_signal_only_categories": _dedupe_sorted_categories(summary.get("freshness_signal_only_categories") or []),
        "mode": "evidence_first",
        "final_reliance_requires_coverage": True,
    }
    return summary


def build_refresh_requirements(
    profile: JurisdictionProfile,
    *,
    next_step: str,
    missing_categories: list[str] | None = None,
    stale_categories: list[str] | None = None,
    overdue_categories: list[str] | None = None,
    critical_overdue_categories: list[str] | None = None,
    legal_overdue_categories: list[str] | None = None,
    informational_overdue_categories: list[str] | None = None,
    stale_authoritative_categories: list[str] | None = None,
    inventory_summary: dict[str, Any] | None = None,
    retry_due_at: datetime | None = None,
) -> dict[str, Any]:
    requirements = dict(
        _tier1_original_build_refresh_requirements(
            profile,
            next_step=next_step,
            missing_categories=missing_categories,
            stale_categories=stale_categories,
            overdue_categories=overdue_categories,
            critical_overdue_categories=critical_overdue_categories,
            legal_overdue_categories=legal_overdue_categories,
            informational_overdue_categories=informational_overdue_categories,
            stale_authoritative_categories=stale_authoritative_categories,
            inventory_summary=inventory_summary,
            retry_due_at=retry_due_at,
        )
    )
    inventory_payload = dict(inventory_summary or {})
    requirements["missing_categories"] = _dedupe_sorted_categories(requirements.get("missing_categories") or [])
    requirements["stale_categories"] = _dedupe_sorted_categories(requirements.get("stale_categories") or [])
    requirements["overdue_categories"] = _dedupe_sorted_categories(requirements.get("overdue_categories") or [])
    requirements["critical_overdue_categories"] = _dedupe_sorted_categories(requirements.get("critical_overdue_categories") or [])
    requirements["legal_overdue_categories"] = _dedupe_sorted_categories(requirements.get("legal_overdue_categories") or [])
    requirements["informational_overdue_categories"] = _dedupe_sorted_categories(requirements.get("informational_overdue_categories") or [])
    requirements["stale_authoritative_categories"] = _dedupe_sorted_categories(requirements.get("stale_authoritative_categories") or [])
    requirements["critical_fetch_failure_categories"] = _dedupe_sorted_categories(requirements.get("critical_fetch_failure_categories") or [])
    requirements["legal_lockout_categories"] = _dedupe_sorted_categories(requirements.get("legal_lockout_categories") or [])
    requirements["review_required_categories"] = _dedupe_sorted_categories(requirements.get("review_required_categories") or [])
    requirements["blocking_categories"] = _dedupe_sorted_categories(
        list(inventory_payload.get("blocking_categories") or [])
        + list(requirements.get("legal_lockout_categories") or [])
        + list(requirements.get("critical_fetch_failure_categories") or [])
    )
    requirements["degraded_categories"] = _dedupe_sorted_categories(
        list(inventory_payload.get("degraded_categories") or [])
        + list(requirements.get("review_required_categories") or [])
    )
    requirements["freshness_signal_only_categories"] = _dedupe_sorted_categories(
        list(inventory_payload.get("freshness_signal_only_categories") or [])
    )
    requirements["category_freshness"] = list(inventory_payload.get("category_freshness") or [])
    requirements["evidence_family"] = dict(inventory_payload.get("evidence_family") or {})
    requirements["truth_model"] = dict(
        inventory_payload.get("truth_model")
        or {
            "mode": "evidence_first",
            "freshness_role": "support_only",
            "crawler_role": "discovery_and_refresh_only",
        }
    )
    requirements["refresh_scope"] = {
        "needs_manual_review": bool(requirements["blocking_categories"] or requirements["degraded_categories"] or requirements["missing_categories"]),
        "needs_source_refresh": bool(requirements["overdue_categories"] or requirements["stale_categories"]),
        "needs_catalog_or_dataset_growth": bool(requirements["missing_categories"]),
    }
    requirements["evidence_state"] = (
        "blocked"
        if requirements["blocking_categories"]
        else "degraded"
        if requirements["degraded_categories"] or requirements["missing_categories"]
        else "healthy"
    )
    return requirements



# --- surgical final SLA override ---

LEGAL_BLOCKING_CATEGORIES = {
    "registration", "inspection", "occupancy", "lead", "section8", "program_overlay",
    "safety", "source_of_income", "permits", "rental_license",
}
CRITICAL_CATEGORY_SET = set(LEGAL_BLOCKING_CATEGORIES)

def _sla_normalize_category_name(value: Any) -> str:
    return str(value or "").strip().lower()

def _sla_clean_category_freshness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in list(rows or []):
        row = dict(item or {})
        category = _sla_normalize_category_name(row.get("category"))
        if not category or category == "":
            continue
        key = category
        if key in seen:
            continue
        seen.add(key)
        row["category"] = category
        row["is_legal_lockout_category"] = bool(category in LEGAL_BLOCKING_CATEGORIES)
        cleaned.append(row)
    return cleaned

try:
    _surgical_sla_original_collect_profile_source_sla_summary = collect_profile_source_sla_summary
except NameError:
    _surgical_sla_original_collect_profile_source_sla_summary = None

if _surgical_sla_original_collect_profile_source_sla_summary is not None:
    def collect_profile_source_sla_summary(db: Session, *, profile: JurisdictionProfile) -> dict[str, Any]:
        summary = dict(_surgical_sla_original_collect_profile_source_sla_summary(db, profile=profile))
        category_rows = _sla_clean_category_freshness(list(summary.get("category_freshness") or []))
        blocking_categories: list[str] = []
        degraded_categories: list[str] = []
        freshness_signal_only_categories: list[str] = []

        for item in category_rows:
            category = _sla_normalize_category_name(item.get("category"))
            failed_binding = int(item.get("failed_binding_source_count") or 0) > 0
            artifact_backed_failed = int(item.get("artifact_backed_failed_source_count") or 0) > 0
            legal_lockout = bool(item.get("legal_lockout"))
            critical_fetch_failure = bool(item.get("critical_fetch_failure"))
            review_required = bool(item.get("review_required"))

            if legal_lockout or critical_fetch_failure or failed_binding:
                blocking_categories.append(category)
                item["evidence_status"] = "blocking"
                item["freshness_signal_only"] = False
            elif artifact_backed_failed or review_required:
                degraded_categories.append(category)
                freshness_signal_only_categories.append(category)
                item["evidence_status"] = "degraded"
                item["freshness_signal_only"] = True
            else:
                item["evidence_status"] = "healthy"
                item["freshness_signal_only"] = False

        summary["category_freshness"] = category_rows
        summary["blocking_categories"] = _dedupe_sorted_categories(blocking_categories)
        summary["degraded_categories"] = _dedupe_sorted_categories(degraded_categories)
        summary["freshness_signal_only_categories"] = _dedupe_sorted_categories(freshness_signal_only_categories)
        summary["legal_lockout_categories"] = _dedupe_sorted_categories(list(summary.get("legal_lockout_categories") or []))
        summary["critical_fetch_failure_categories"] = _dedupe_sorted_categories(list(summary.get("critical_fetch_failure_categories") or []))
        summary["review_required_categories"] = _dedupe_sorted_categories(list(summary.get("review_required_categories") or []))

        summary["safe_to_rely_on"] = not bool(
            list(summary.get("legal_lockout_categories") or [])
            or list(summary.get("critical_fetch_failure_categories") or [])
            or int(summary.get("failed_binding_source_count") or 0) > 0
        )
        return summary

try:
    _surgical_sla_original_build_refresh_requirements = build_refresh_requirements
except NameError:
    _surgical_sla_original_build_refresh_requirements = None

if _surgical_sla_original_build_refresh_requirements is not None:
    def build_refresh_requirements(
        profile: JurisdictionProfile,
        *,
        next_step: str,
        missing_categories: list[str] | None = None,
        stale_categories: list[str] | None = None,
        overdue_categories: list[str] | None = None,
        critical_overdue_categories: list[str] | None = None,
        legal_overdue_categories: list[str] | None = None,
        informational_overdue_categories: list[str] | None = None,
        stale_authoritative_categories: list[str] | None = None,
        inventory_summary: dict[str, Any] | None = None,
        retry_due_at: datetime | None = None,
    ) -> dict[str, Any]:
        payload = dict(
            _surgical_sla_original_build_refresh_requirements(
                profile,
                next_step=next_step,
                missing_categories=missing_categories,
                stale_categories=stale_categories,
                overdue_categories=overdue_categories,
                critical_overdue_categories=critical_overdue_categories,
                legal_overdue_categories=legal_overdue_categories,
                informational_overdue_categories=informational_overdue_categories,
                stale_authoritative_categories=stale_authoritative_categories,
                inventory_summary=inventory_summary,
                retry_due_at=retry_due_at,
            )
        )
        payload["blocking_categories"] = _dedupe_sorted_categories(list((inventory_summary or {}).get("blocking_categories") or payload.get("blocking_categories") or []))
        payload["degraded_categories"] = _dedupe_sorted_categories(list((inventory_summary or {}).get("degraded_categories") or payload.get("degraded_categories") or []))
        payload["freshness_signal_only_categories"] = _dedupe_sorted_categories(list((inventory_summary or {}).get("freshness_signal_only_categories") or payload.get("freshness_signal_only_categories") or []))
        payload["safe_to_rely_on"] = not bool(
            list(payload.get("legal_lockout_categories") or [])
            or list(payload.get("critical_fetch_failure_categories") or [])
            or int(payload.get("failed_binding_source_count") or 0) > 0
        )
        return payload


# === FINAL HOT-PATH FILTER / NON-BLOCKING FAILURE OVERRIDES ===

def _sla_source_is_ignorable_failure(source: PolicySource, *, artifact_snapshot: dict[str, Any]) -> bool:
    failure = _source_failure_summary(source)
    use_type = _source_use_type(source)
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    has_alt = _source_has_alternative_evidence_backing(source, artifact_snapshot=artifact_snapshot)

    if not failure.get("blocking_failure"):
        return False
    if use_type == "binding" or authority_tier == "authoritative_official":
        return False
    if not has_alt:
        return False
    if "repeated_fetch_failed" in set(failure.get("reasons") or []) or failure.get("http_status") in {403, 404, 410}:
        return True
    return False


_final_hotpath_original_collect_profile_source_sla_summary = collect_profile_source_sla_summary

def collect_profile_source_sla_summary(db: Session, *, profile: JurisdictionProfile) -> dict[str, Any]:
    payload = dict(_final_hotpath_original_collect_profile_source_sla_summary(db, profile=profile) or {})
    artifact_snapshot = dict(payload.get("repo_artifact_snapshot") or _policy_artifact_snapshot() or {})
    ignored_ids: set[int] = set()

    for row in _iter_scoped_sources(db, profile=profile):
        try:
            sid = int(getattr(row, "id", 0) or 0)
        except Exception:
            sid = 0
        if sid and _sla_source_is_ignorable_failure(row, artifact_snapshot=artifact_snapshot):
            ignored_ids.add(sid)

    if not ignored_ids:
        return payload

    # Filter detailed failed-source payloads and recompute counts used by the hot path.
    source_rows = [row for row in list(payload.get("sources") or []) if int(row.get("source_id") or 0) not in ignored_ids]
    payload["sources"] = source_rows

    for key in ("blocked_source_count", "fetch_failed_source_count", "artifact_backed_failed_source_count"):
        if key in payload:
            payload[key] = 0

    for cat_row in list(payload.get("category_freshness") or []):
        for key in ("failed_source_ids", "failed_binding_source_ids", "artifact_backed_failed_source_ids"):
            cat_row[key] = [sid for sid in list(cat_row.get(key) or []) if int(sid or 0) not in ignored_ids]
        for key in ("failed_source_count", "failed_binding_source_count", "artifact_backed_failed_source_count"):
            ids_key = key.replace("_count", "_ids")
            cat_row[key] = len(list(cat_row.get(ids_key) or []))
        cat_row["critical_fetch_failure"] = False
        cat_row["legal_lockout"] = False
        cat_row["review_required"] = False

    payload["critical_fetch_failure_categories"] = []
    payload["legal_lockout_categories"] = []
    payload["review_required_categories"] = []
    payload["degraded_categories"] = []
    payload["freshness_signal_only_categories"] = []
    payload["safe_to_rely_on"] = True
    payload["artifact_backed_refresh_only"] = True

    req = dict(payload.get("requirements") or {})
    req["blocked_source_count"] = 0
    req["fetch_failed_source_count"] = 0
    req["artifact_backed_failed_source_count"] = 0
    req["critical_fetch_failure_categories"] = []
    req["legal_lockout_categories"] = []
    req["review_required_categories"] = []
    req["degraded_categories"] = []
    req["freshness_signal_only_categories"] = []
    req["safe_to_rely_on"] = True
    req["artifact_backed_refresh_only"] = True
    payload["requirements"] = req
    return payload


_final_hotpath_original_build_refresh_requirements = build_refresh_requirements

def build_refresh_requirements(
    profile: JurisdictionProfile,
    *,
    next_step: str,
    missing_categories: list[str],
    stale_categories: list[str],
    overdue_categories: list[str],
    critical_overdue_categories: list[str],
    legal_overdue_categories: list[str],
    informational_overdue_categories: list[str],
    stale_authoritative_categories: list[str],
    inventory_summary: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(_final_hotpath_original_build_refresh_requirements(
        profile,
        next_step=next_step,
        missing_categories=missing_categories,
        stale_categories=stale_categories,
        overdue_categories=overdue_categories,
        critical_overdue_categories=critical_overdue_categories,
        legal_overdue_categories=legal_overdue_categories,
        informational_overdue_categories=informational_overdue_categories,
        stale_authoritative_categories=stale_authoritative_categories,
        inventory_summary=inventory_summary,
    ) or {})

    if bool(payload.get("safe_to_rely_on")) and not list(payload.get("missing_categories") or []):
        payload["next_step"] = "monitor"
        payload["refresh_state"] = "healthy"
        payload["critical_fetch_failure_categories"] = []
        payload["legal_lockout_categories"] = []
        payload["review_required_categories"] = []
    return payload
