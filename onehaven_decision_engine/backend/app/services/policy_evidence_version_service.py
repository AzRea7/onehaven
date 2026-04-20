
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


def source_version_snapshot(version: PolicySourceVersion) -> dict[str, Any]:
    return {
        "version_id": int(getattr(version, "id", 0) or 0),
        "source_id": getattr(version, "source_id", None),
        "retrieved_at": getattr(version, "retrieved_at", None).isoformat() if getattr(version, "retrieved_at", None) else None,
        "content_sha256": getattr(version, "content_sha256", None),
        "raw_path": getattr(version, "raw_path", None),
        "content_type": getattr(version, "content_type", None),
        "http_status": getattr(version, "http_status", None),
        "version_meta_json": _loads(getattr(version, "version_meta_json", None), {}),
    }


def evidence_version_diff(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    return {
        "changed": (left.get("content_sha256") != right.get("content_sha256")) or (left.get("raw_path") != right.get("raw_path")),
        "from_version_id": left.get("version_id"),
        "to_version_id": right.get("version_id"),
        "from_sha256": left.get("content_sha256"),
        "to_sha256": right.get("content_sha256"),
        "from_retrieved_at": left.get("retrieved_at"),
        "to_retrieved_at": right.get("retrieved_at"),
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
    if not source_ids:
        return {
            "ok": True,
            "market": {"state": st, "county": cnty, "city": cty, "pha_name": pha},
            "rows": [],
            "summary": {"version_count": 0, "service_role": "evidence_version_registry", "truth_model": "evidence_first"},
        }

    version_stmt = select(PolicySourceVersion).where(PolicySourceVersion.source_id.in_(source_ids))
    rows = list(db.scalars(version_stmt).all())[:limit]
    payload_rows = [source_version_snapshot(r) for r in rows]
    payload_rows.sort(key=lambda r: ((r.get("source_id") or 0), r.get("retrieved_at") or "", r.get("version_id") or 0), reverse=True)

    diffs = []
    by_source: dict[int, list[dict[str, Any]]] = {}
    for row in payload_rows:
        by_source.setdefault(int(row.get("source_id") or 0), []).append(row)
    for source_id, versions in by_source.items():
        if len(versions) >= 2:
            diffs.append(evidence_version_diff(versions[1], versions[0]))

    return {
        "ok": True,
        "market": {"state": st, "county": cnty, "city": cty, "pha_name": pha},
        "rows": payload_rows,
        "diffs": diffs,
        "summary": {
            "version_count": len(payload_rows),
            "source_count": len(by_source),
            "changed_source_count": sum(1 for d in diffs if d.get("changed")),
            "service_role": "evidence_version_registry",
            "truth_model": "evidence_first",
        },
    }
