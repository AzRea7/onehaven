from __future__ import annotations

import json
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
    refresh_reason = str(getattr(source, "refresh_status_reason", "") or getattr(source, "refresh_blocked_reason", "") or "").strip().lower()
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
        reason in {"guessed_domain", "http_not_found", "fetch_failed", "error", "blocked", "refresh_blocked", "blocked_or_antibot", "repeated_fetch_failed"}
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
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    use_type = _source_use_type(source)
    if not categories:
        return _category_sla_hours(category="", source_type=source_type, authority_tier=authority_tier, use_type=use_type)
    return min(_category_sla_hours(category=c, source_type=source_type, authority_tier=authority_tier, use_type=use_type) for c in categories)


def _category_due_at(source: PolicySource, category: str) -> datetime:
    base = (
        getattr(source, "last_verified_at", None)
        or getattr(source, "freshness_checked_at", None)
        or getattr(source, "last_fetched_at", None)
        or getattr(source, "retrieved_at", None)
        or _utcnow()
    )
    return base + timedelta(hours=_category_sla_hours(
        category=category,
        source_type=str(getattr(source, "source_type", "") or ""),
        authority_tier=str(getattr(source, "authority_tier", "") or ""),
        use_type=_source_use_type(source),
    ))


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


def collect_profile_source_sla_summary(db: Session, *, profile: JurisdictionProfile) -> dict[str, Any]:
    now = _utcnow()
    scoped = _iter_scoped_sources(db, profile=profile)

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

    for source in scoped:
        categories = sorted(source_categories(source))
        authority_tier = getattr(source, "authority_tier", None)
        use_type = _source_use_type(source)
        failure_summary = _source_failure_summary(source)
        source_due = source_due_at(source)
        source_overdue = source_due <= now
        source_due_soon = (not source_overdue) and source_due <= (now + timedelta(hours=24))
        if failure_summary.get("blocking_failure"):
            rejected_source_count += 1
        if failure_summary.get("looks_guessed"):
            guessed_source_count += 1
        if "blocked_or_antibot" in failure_summary.get("reasons", []) or "refresh_blocked" in failure_summary.get("reasons", []):
            blocked_source_count += 1
        if any(reason in {"fetch_failed", "error", "blocked", "repeated_fetch_failed", "http_not_found"} or str(reason).startswith("http_status_") for reason in failure_summary.get("reasons", [])):
            fetch_failed_source_count += 1
        if failure_summary.get("blocking_failure") and use_type == "binding":
            failed_binding_source_count += 1
        per_category: list[dict[str, Any]] = []

        for category in categories or [""]:
            due_at = _category_due_at(source, category)
            is_overdue = due_at <= now
            is_due_soon = (not is_overdue) and due_at <= (now + timedelta(hours=24))
            is_legal = category in LEGAL_BLOCKING_CATEGORIES
            entry = category_rollup.setdefault(category, {
                "category": category,
                "is_legal_lockout_category": is_legal,
                "source_ids": [],
                "binding_source_ids": [],
                "overdue_source_ids": [],
                "authoritative_overdue_source_ids": [],
                "next_due_at": None,
                "failed_source_ids": [],
                "failed_binding_source_ids": [],
            })
            entry["source_ids"].append(int(getattr(source, "id", 0) or 0))
            if use_type == "binding":
                entry["binding_source_ids"].append(int(getattr(source, "id", 0) or 0))
            source_failed = bool(failure_summary.get("blocking_failure"))
            if source_failed:
                overdue_categories.add(category)
                review_required_categories.add(category)
                entry.setdefault("failed_source_ids", []).append(int(getattr(source, "id", 0) or 0))
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
            per_category.append({
                "category": category,
                "due_at": due_at.isoformat() if due_at else None,
                "is_overdue": is_overdue,
                "is_due_soon": is_due_soon,
                "is_legal_lockout_category": is_legal,
                "authority_use_type": use_type,
                "source_failed": source_failed,
                "failure_reasons": list(failure_summary.get("reasons") or []),
            })

        sources_payload.append({
            "source_id": int(getattr(source, "id", 0) or 0),
            "source_name": getattr(source, "source_name", None) or getattr(source, "title", None),
            "authority_tier": authority_tier,
            "authority_use_type": use_type,
            "categories": categories,
            "due_at": source_due.isoformat() if source_due else None,
            "is_overdue": source_overdue,
            "is_due_soon": source_due_soon,
            "refresh_state": getattr(source, "refresh_state", None),
            "freshness_status": getattr(source, "freshness_status", None),
            "source_failure": failure_summary,
            "category_freshness": per_category,
        })

    category_freshness = []
    for category, payload in sorted(category_rollup.items()):
        category_freshness.append({
            **payload,
            "source_count": len(set(payload["source_ids"])),
            "binding_source_count": len(set(payload["binding_source_ids"])),
            "overdue_source_count": len(set(payload["overdue_source_ids"])),
            "authoritative_overdue_source_count": len(set(payload["authoritative_overdue_source_ids"])),
            "failed_source_count": len(set(payload.get("failed_source_ids") or [])),
            "failed_binding_source_count": len(set(payload.get("failed_binding_source_ids") or [])),
            "legal_stale": category in legal_overdue_categories,
            "informational_stale": category in informational_overdue_categories,
            "critical_fetch_failure": category in critical_fetch_failure_categories,
            "legal_lockout": category in legal_lockout_categories,
            "review_required": category in review_required_categories,
        })

    next_due_at = None
    due_values = [item.get("due_at") for item in sources_payload if item.get("due_at")]
    if due_values:
        next_due_at = min(due_values)

    artifact_snapshot = _policy_artifact_snapshot()

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
        "safe_to_rely_on": not bool(legal_lockout_categories or critical_fetch_failure_categories),
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

    return {
        "next_step": next_step,
        "refresh_state": getattr(profile, "refresh_state", None),
        "missing_categories": list(missing_categories or []),
        "stale_categories": list(stale_categories or []),
        "overdue_categories": list(overdue_categories or []),
        "critical_overdue_categories": list(critical_overdue_categories or []),
        "legal_overdue_categories": list(legal_overdue_categories or []),
        "informational_overdue_categories": list(informational_overdue_categories or []),
        "stale_authoritative_categories": list(stale_authoritative_categories or []),
        "inventory_summary": inventory_summary,
        "critical_fetch_failure_categories": list(inventory_summary.get("critical_fetch_failure_categories") or []),
        "legal_lockout_categories": list(inventory_summary.get("legal_lockout_categories") or []),
        "review_required_categories": list(inventory_summary.get("review_required_categories") or []),
        "rejected_source_count": int(inventory_summary.get("rejected_source_count") or 0),
        "guessed_source_count": int(inventory_summary.get("guessed_source_count") or 0),
        "blocked_source_count": int(inventory_summary.get("blocked_source_count") or 0),
        "fetch_failed_source_count": int(inventory_summary.get("fetch_failed_source_count") or 0),
        "failed_binding_source_count": int(inventory_summary.get("failed_binding_source_count") or 0),
        "safe_to_rely_on": bool(inventory_summary.get("safe_to_rely_on", True)),
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
    }


def _repo_candidate_roots() -> list[Path]:
    candidates: list[Path] = []
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
    candidates.append(Path('/mnt/data'))

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
            root / 'backend' / 'policy_raw',
            root / 'onehaven_decision_engine' / 'backend' / 'policy_raw',
            root / 'policy_raw',
        ]
        found = _first_existing(candidates)
        if found is not None:
            policy_raw = found
            break
    for root in roots:
        candidates = [
            root / 'backend' / 'pdfs',
            root / 'onehaven_decision_engine' / 'backend' / 'pdfs',
            root / 'pdfs',
            root / 'backend' / 'pdf',
            root / 'onehaven_decision_engine' / 'backend' / 'pdf',
            root / 'pdf',
        ]
        found = _first_existing(candidates)
        if found is not None:
            pdf_root = found
            break

    def _scan(path: Path | None, patterns: tuple[str, ...]) -> dict[str, Any]:
        if path is None:
            return {'exists': False, 'path': None, 'count': 0, 'latest_mtime': None, 'examples': []}
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
            'exists': True,
            'path': str(path),
            'count': len(rows),
            'latest_mtime': latest.isoformat() if latest else None,
            'examples': [str(f) for f in sorted(rows)[:5]],
        }

    html = _scan(policy_raw, ('*.html', '*.htm'))
    pdf = _scan(pdf_root, ('*.pdf',))
    has_any = bool(html.get('count') or 0) or bool(pdf.get('count') or 0)
    return {
        'has_repo_artifacts': has_any,
        'policy_raw': html,
        'pdfs': pdf,
        'artifact_support_state': 'artifact_backed' if has_any else 'no_repo_artifacts_found',
    }
