from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import MetaData, Table, and_, select, update
from sqlalchemy.orm import Session


RESOLUTION_STATUS_PENDING = "pending"
RESOLUTION_STATUS_READY = "ready"
RESOLUTION_STATUS_BLOCKED = "blocked"
RESOLUTION_STATUS_PARTIAL = "partial"

TRUST_STATUS_UNKNOWN = "unknown"
TRUST_STATUS_TRUSTED = "trusted"
TRUST_STATUS_PARTIAL = "partial"
TRUST_STATUS_BLOCKED = "blocked"


def _norm_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _norm_lower(value: Any) -> str | None:
    text = _norm_text(value)
    return text.lower() if text else None


def _table(db: Session, name: str) -> Table:
    metadata = MetaData()
    return Table(name, metadata, autoload_with=db.bind)


def _dump_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except Exception:
        return json.dumps({}, sort_keys=True)


def _property_exists(db: Session, *, property_id: int) -> bool:
    props = _table(db, "properties")
    row = db.execute(
        select(props.c.id).where(props.c.id == int(property_id)).limit(1)
    ).first()
    return row is not None


def _find_latest_snapshot_row(
    db: Session,
    *,
    property_id: int,
    org_id: int | None = None,
) -> Any:
    table = _table(db, "property_compliance_resolution_snapshots")
    conditions = [table.c.property_id == int(property_id)]
    if org_id is not None and "org_id" in table.c:
        conditions.append(table.c.org_id == int(org_id))
    return db.execute(
        select(table)
        .where(and_(*conditions))
        .order_by(table.c.version.desc(), table.c.id.desc())
        .limit(1)
    ).first()


def compute_resolution_status(
    *,
    missing_categories: list[str] | None,
    stale_categories: list[str] | None,
    blocked_categories: list[str] | None,
    unresolved_items: list[dict[str, Any]] | None,
) -> str:
    missing_categories = list(missing_categories or [])
    stale_categories = list(stale_categories or [])
    blocked_categories = list(blocked_categories or [])
    unresolved_items = list(unresolved_items or [])

    if blocked_categories:
        return RESOLUTION_STATUS_BLOCKED
    if missing_categories or stale_categories or unresolved_items:
        return RESOLUTION_STATUS_PARTIAL
    return RESOLUTION_STATUS_READY


def compute_trust_status(
    *,
    safe_to_rely_on: bool | None,
    blocked_categories: list[str] | None,
    missing_categories: list[str] | None,
    stale_categories: list[str] | None,
) -> str:
    if blocked_categories:
        return TRUST_STATUS_BLOCKED
    if safe_to_rely_on is True and not missing_categories and not stale_categories:
        return TRUST_STATUS_TRUSTED
    if safe_to_rely_on is False:
        return TRUST_STATUS_PARTIAL
    return TRUST_STATUS_UNKNOWN


def upsert_property_compliance_resolution_snapshot(
    db: Session,
    *,
    property_id: int,
    org_id: int | None,
    jurisdiction_id: int | None = None,
    jurisdiction_profile_id: int | None = None,
    trust_status: str | None = None,
    resolution_status: str | None = None,
    safe_to_rely_on: bool | None = None,
    missing_categories: list[str] | None = None,
    stale_categories: list[str] | None = None,
    blocked_categories: list[str] | None = None,
    source_family_summary: dict[str, Any] | None = None,
    applied_rule_refs: list[dict[str, Any]] | None = None,
    unresolved_items: list[dict[str, Any]] | None = None,
    evidence_summary: dict[str, Any] | None = None,
    notes: str | None = None,
    recompute_reason: str | None = None,
) -> dict[str, Any]:
    if not _property_exists(db, property_id=property_id):
        raise ValueError(f"property {property_id} not found")

    table = _table(db, "property_compliance_resolution_snapshots")
    latest = _find_latest_snapshot_row(db, property_id=property_id, org_id=org_id)
    next_version = 1 if latest is None else int(latest._mapping.get("version") or 0) + 1

    missing_categories = list(missing_categories or [])
    stale_categories = list(stale_categories or [])
    blocked_categories = list(blocked_categories or [])
    applied_rule_refs = list(applied_rule_refs or [])
    unresolved_items = list(unresolved_items or [])
    source_family_summary = dict(source_family_summary or {})
    evidence_summary = dict(evidence_summary or {})

    if resolution_status is None:
        resolution_status = compute_resolution_status(
            missing_categories=missing_categories,
            stale_categories=stale_categories,
            blocked_categories=blocked_categories,
            unresolved_items=unresolved_items,
        )

    if trust_status is None:
        trust_status = compute_trust_status(
            safe_to_rely_on=safe_to_rely_on,
            blocked_categories=blocked_categories,
            missing_categories=missing_categories,
            stale_categories=stale_categories,
        )

    payload = {
        "org_id": org_id,
        "property_id": int(property_id),
        "jurisdiction_id": jurisdiction_id,
        "jurisdiction_profile_id": jurisdiction_profile_id,
        "version": next_version,
        "trust_status": _norm_lower(trust_status) or TRUST_STATUS_UNKNOWN,
        "resolution_status": _norm_lower(resolution_status) or RESOLUTION_STATUS_PENDING,
        "safe_to_rely_on": bool(safe_to_rely_on) if safe_to_rely_on is not None else False,
        "missing_categories_json": _dump_json(missing_categories),
        "stale_categories_json": _dump_json(stale_categories),
        "blocked_categories_json": _dump_json(blocked_categories),
        "source_family_summary_json": _dump_json(source_family_summary),
        "applied_rule_refs_json": _dump_json(applied_rule_refs),
        "unresolved_items_json": _dump_json(unresolved_items),
        "evidence_summary_json": _dump_json(evidence_summary),
        "notes": _norm_text(notes),
        "recompute_reason": _norm_text(recompute_reason),
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    inserted = db.execute(table.insert().values(**payload))
    db.flush()

    row = db.execute(
        select(table)
        .where(table.c.id == int(inserted.inserted_primary_key[0]))
        .limit(1)
    ).first()

    return dict(row._mapping) if row else payload


def get_latest_property_compliance_resolution_snapshot(
    db: Session,
    *,
    property_id: int,
    org_id: int | None = None,
) -> dict[str, Any] | None:
    row = _find_latest_snapshot_row(db, property_id=property_id, org_id=org_id)
    return dict(row._mapping) if row else None


def mark_property_resolution_superseded(
    db: Session,
    *,
    property_id: int,
    org_id: int | None,
    note: str | None = None,
) -> int:
    table = _table(db, "property_compliance_resolution_snapshots")
    conditions = [table.c.property_id == int(property_id)]
    if org_id is not None and "org_id" in table.c:
        conditions.append(table.c.org_id == int(org_id))

    result = db.execute(
        update(table)
        .where(and_(*conditions))
        .values(
            notes=_norm_text(note),
            updated_at=datetime.utcnow(),
        )
    )
    db.flush()
    return int(result.rowcount or 0)