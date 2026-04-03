from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

VALID_ACQUISITION_TAGS = {"saved", "shortlisted", "review_later", "rejected", "offer_candidate"}
DEFAULT_INVESTOR_PRESERVE_TAGS = {"saved", "shortlisted"}
ACQUISITION_POSTURE_TAGS = {"offer_candidate"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def normalize_tag(tag: str) -> str:
    value = str(tag or '').strip().lower()
    if value not in VALID_ACQUISITION_TAGS:
        raise ValueError(f'invalid acquisition tag: {tag}')
    return value


def normalize_preserve_tags(tags: list[str] | None, *, default_to_investor_tags: bool = True) -> list[str]:
    incoming = tags if tags is not None else (sorted(DEFAULT_INVESTOR_PRESERVE_TAGS) if default_to_investor_tags else [])
    normalized = sorted({normalize_tag(x) for x in incoming if str(x or '').strip()})
    return [tag for tag in normalized if tag not in ACQUISITION_POSTURE_TAGS]


def list_property_tags(db: Session, *, org_id: int, property_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, org_id, property_id, tag, source, created_by_user_id, created_at, updated_at
            FROM acquisition_property_tags
            WHERE org_id = :org_id AND property_id = :property_id
            ORDER BY tag ASC
            """
        ),
        {'org_id': int(org_id), 'property_id': int(property_id)},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def list_tags_for_properties(db: Session, *, org_id: int, property_ids: list[int]) -> dict[int, list[str]]:
    if not property_ids:
        return {}
    rows = db.execute(
        text(
            """
            SELECT property_id, tag
            FROM acquisition_property_tags
            WHERE org_id = :org_id AND property_id = ANY(:property_ids)
            ORDER BY property_id ASC, tag ASC
            """
        ),
        {'org_id': int(org_id), 'property_ids': list(property_ids)},
    ).fetchall()
    out: dict[int, list[str]] = {int(pid): [] for pid in property_ids}
    for row in rows:
        out.setdefault(int(row.property_id), []).append(str(row.tag))
    return out


def replace_property_tags(
    db: Session,
    *,
    org_id: int,
    property_id: int,
    tags: list[str],
    actor_user_id: int | None,
    source: str = 'operator',
) -> list[dict[str, Any]]:
    normalized = sorted({normalize_tag(x) for x in tags})
    db.execute(
        text('DELETE FROM acquisition_property_tags WHERE org_id = :org_id AND property_id = :property_id'),
        {'org_id': int(org_id), 'property_id': int(property_id)},
    )
    now = _utcnow()
    for tag in normalized:
        db.execute(
            text(
                """
                INSERT INTO acquisition_property_tags (
                    org_id, property_id, tag, source, created_by_user_id, created_at, updated_at
                ) VALUES (
                    :org_id, :property_id, :tag, :source, :actor_user_id, :created_at, :updated_at
                )
                """
            ),
            {
                'org_id': int(org_id), 'property_id': int(property_id), 'tag': tag, 'source': source,
                'actor_user_id': actor_user_id, 'created_at': now, 'updated_at': now,
            },
        )
    db.commit()
    return list_property_tags(db, org_id=org_id, property_id=property_id)


def count_tags_for_scope(db: Session, *, org_id: int) -> dict[str, int]:
    rows = db.execute(
        text(
            """
            SELECT tag, COUNT(*) AS count
            FROM acquisition_property_tags
            WHERE org_id = :org_id
            GROUP BY tag
            """
        ),
        {'org_id': int(org_id)},
    ).fetchall()
    out = {tag: 0 for tag in sorted(VALID_ACQUISITION_TAGS)}
    for row in rows:
        out[str(row.tag)] = int(row.count or 0)
    return out
