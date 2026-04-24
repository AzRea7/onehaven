from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.policy_models import PolicySource, PolicySourceVersion


def _norm_state(v: Optional[str]) -> str:
    return (v or "MI").strip().upper()


def _norm_lower(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip().lower()
    return out or None


def _norm_text(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    out = str(v).strip()
    return out or None


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


def _source_truth_bucket(source: PolicySource | None) -> str:
    if source is None:
        return "unknown"
    authority_use_type = str(getattr(source, "authority_use_type", "") or "").strip().lower()
    authority_tier = str(getattr(source, "authority_tier", "") or "").strip().lower()
    refresh_state = str(getattr(source, "refresh_state", "") or "").strip().lower()
    freshness_status = str(getattr(source, "freshness_status", "") or "").strip().lower()
    http_status = getattr(source, "http_status", None)
    try:
        http_code = int(http_status) if http_status is not None else None
    except Exception:
        http_code = None

    if refresh_state in {"failed", "blocked"} or freshness_status in {"fetch_failed", "error", "blocked"}:
        return "unusable"
    if http_code is not None and http_code >= 400:
        return "unusable"
    if authority_use_type == "binding" and authority_tier == "authoritative_official":
        return "binding"
    if authority_use_type in {"binding", "supporting"} or authority_tier in {"authoritative_official", "approved_official_supporting"}:
        return "supporting"
    return "weak"


def source_version_snapshot(version: PolicySourceVersion, source: PolicySource | None = None) -> dict[str, Any]:
    return {
        "version_id": int(getattr(version, "id", 0) or 0),
        "source_id": getattr(version, "source_id", None),
        "retrieved_at": getattr(version, "retrieved_at", None).isoformat() if getattr(version, "retrieved_at", None) else None,
        "content_sha256": getattr(version, "content_sha256", None),
        "raw_path": getattr(version, "raw_path", None),
        "content_type": getattr(version, "content_type", None),
        "http_status": getattr(version, "http_status", None),
        "version_meta_json": _loads(getattr(version, "version_meta_json", None), {}),
        "truth_bucket": _source_truth_bucket(source),
        "source_url": getattr(source, "url", None) if source is not None else None,
        "authority_use_type": getattr(source, "authority_use_type", None) if source is not None else None,
        "authority_tier": getattr(source, "authority_tier", None) if source is not None else None,
    }


def evidence_version_diff(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    content_changed = (left.get("content_sha256") != right.get("content_sha256")) or (left.get("raw_path") != right.get("raw_path"))
    status_changed = left.get("http_status") != right.get("http_status")
    truth_changed = left.get("truth_bucket") != right.get("truth_bucket")
    return {
        "changed": bool(content_changed or status_changed or truth_changed),
        "content_changed": bool(content_changed),
        "status_changed": bool(status_changed),
        "truth_changed": bool(truth_changed),
        "from_version_id": left.get("version_id"),
        "to_version_id": right.get("version_id"),
        "from_sha256": left.get("content_sha256"),
        "to_sha256": right.get("content_sha256"),
        "from_retrieved_at": left.get("retrieved_at"),
        "to_retrieved_at": right.get("retrieved_at"),
        "from_truth_bucket": left.get("truth_bucket"),
        "to_truth_bucket": right.get("truth_bucket"),
    }


def evidence_versions_for_market(
    db: Session,
    *,
    org_id: int | None,
    state: str,
    county: str | None,
    city: str | None,
    pha_name: str | None,
    include_global: bool = True,
    limit: int = 100,
) -> dict[str, Any]:
    st = _norm_state(state)
    cnty = _norm_lower(county)
    cty = _norm_lower(city)
    pha = _norm_text(pha_name)

    src_stmt = select(PolicySource).where(PolicySource.state == st)
    if include_global:
        if org_id is None:
            src_stmt = src_stmt.where(PolicySource.org_id.is_(None))
        else:
            src_stmt = src_stmt.where(or_(PolicySource.org_id == org_id, PolicySource.org_id.is_(None)))
    else:
        src_stmt = src_stmt.where(PolicySource.org_id == org_id)

    sources = []
    for row in db.scalars(src_stmt).all():
        if getattr(row, "county", None) is not None and getattr(row, "county", None) != cnty:
            continue
        if getattr(row, "city", None) is not None and getattr(row, "city", None) != cty:
            continue
        if getattr(row, "pha_name", None) is not None and getattr(row, "pha_name", None) != pha:
            continue
        sources.append(row)

    source_ids = [int(getattr(s, "id", 0) or 0) for s in sources if getattr(s, "id", None) is not None]
    source_map = {int(getattr(s, "id", 0) or 0): s for s in sources if getattr(s, "id", None) is not None}
    if not source_ids:
        return {
            "ok": True,
            "market": {"state": st, "county": cnty, "city": cty, "pha_name": pha},
            "rows": [],
            "summary": {"version_count": 0, "service_role": "evidence_version_registry", "truth_model": "evidence_first"},
        }

    version_stmt = select(PolicySourceVersion).where(PolicySourceVersion.source_id.in_(source_ids))
    rows = list(db.scalars(version_stmt).all())[:limit]
    payload_rows = [source_version_snapshot(r, source_map.get(int(getattr(r, "source_id", 0) or 0))) for r in rows]
    payload_rows.sort(key=lambda r: ((r.get("source_id") or 0), r.get("retrieved_at") or "", r.get("version_id") or 0), reverse=True)

    diffs = []
    by_source: dict[int, list[dict[str, Any]]] = {}
    for row in payload_rows:
        by_source.setdefault(int(row.get("source_id") or 0), []).append(row)
    for source_id, versions in by_source.items():
        if len(versions) >= 2:
            diffs.append(evidence_version_diff(versions[1], versions[0]))

    truth_bucket_counts: dict[str, int] = {}
    for row in payload_rows:
        bucket = str(row.get("truth_bucket") or "unknown")
        truth_bucket_counts[bucket] = truth_bucket_counts.get(bucket, 0) + 1

    return {
        "ok": True,
        "market": {"state": st, "county": cnty, "city": cty, "pha_name": pha},
        "rows": payload_rows,
        "diffs": diffs,
        "summary": {
            "version_count": len(payload_rows),
            "source_count": len(by_source),
            "changed_source_count": sum(1 for d in diffs if d.get("changed")),
            "truth_bucket_counts": truth_bucket_counts,
            "service_role": "evidence_version_registry",
            "truth_model": "evidence_first",
        },
    }


# --- surgical pdf/artifact version overlay ---
def _artifact_backed_source(source: PolicySource | None) -> bool:
    if source is None:
        return False
    source_type = str(getattr(source, "source_type", "") or "").strip().lower()
    publication_type = str(getattr(source, "publication_type", "") or "").strip().lower()
    notes = str(getattr(source, "notes", "") or "").strip().lower()
    raw_path = str(getattr(source, "raw_path", "") or "").strip().lower()
    url = str(getattr(source, "url", "") or "").strip().lower()
    return bool(
        source_type in {"artifact", "dataset", "catalog", "manual"}
        or publication_type in {"pdf", "official_document"}
        or "artifact" in notes
        or "pdf" in notes
        or raw_path.endswith(".pdf")
        or url.endswith(".pdf")
    )


_evver_orig_source_version_snapshot = source_version_snapshot
_evver_orig_evidence_version_diff = evidence_version_diff

def source_version_snapshot(version: PolicySourceVersion, source: PolicySource | None = None) -> dict[str, Any]:
    payload = dict(_evver_orig_source_version_snapshot(version, source))
    payload["artifact_backed"] = bool(_artifact_backed_source(source))
    return payload

def evidence_version_diff(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    payload = dict(_evver_orig_evidence_version_diff(left, right))
    artifact_changed = bool(left.get("artifact_backed")) != bool(right.get("artifact_backed"))
    payload["artifact_changed"] = bool(artifact_changed)
    payload["changed"] = bool(payload.get("changed") or artifact_changed)
    return payload
