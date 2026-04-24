from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if hasattr(row, '_mapping'):
        return dict(row._mapping)
    return dict(row)


def list_watchlists(db: Session, *, org_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, org_id, name, description, filters_json, sort_json, is_default,
                   created_by_user_id, updated_by_user_id, created_at, updated_at
            FROM portfolio_watchlists
            WHERE org_id = :org_id
            ORDER BY is_default DESC, updated_at DESC, id DESC
            """
        ),
        {'org_id': int(org_id)},
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        item = _row_to_dict(row)
        item['filters_json'] = item.get('filters_json') or {}
        item['sort_json'] = item.get('sort_json') or {}
        out.append(item)
    return out


def get_watchlist(db: Session, *, org_id: int, watchlist_id: int) -> dict[str, Any] | None:
    row = db.execute(
        text(
            """
            SELECT id, org_id, name, description, filters_json, sort_json, is_default,
                   created_by_user_id, updated_by_user_id, created_at, updated_at
            FROM portfolio_watchlists
            WHERE org_id = :org_id AND id = :watchlist_id
            """
        ),
        {'org_id': int(org_id), 'watchlist_id': int(watchlist_id)},
    ).fetchone()
    if row is None:
        return None
    item = _row_to_dict(row)
    item['filters_json'] = item.get('filters_json') or {}
    item['sort_json'] = item.get('sort_json') or {}
    return item


def upsert_watchlist(
    db: Session,
    *,
    org_id: int,
    watchlist_id: int | None,
    name: str,
    description: str | None,
    filters_json: dict[str, Any] | None,
    sort_json: dict[str, Any] | None,
    is_default: bool,
    actor_user_id: int | None,
) -> dict[str, Any]:
    now = _utcnow()
    if watchlist_id is None:
        row = db.execute(
            text(
                """
                INSERT INTO portfolio_watchlists (
                    org_id, name, description, filters_json, sort_json, is_default,
                    created_by_user_id, updated_by_user_id, created_at, updated_at
                ) VALUES (
                    :org_id, :name, :description, CAST(:filters_json AS JSONB), CAST(:sort_json AS JSONB), :is_default,
                    :actor_user_id, :actor_user_id, :created_at, :updated_at
                )
                RETURNING id
                """
            ),
            {
                'org_id': int(org_id),
                'name': name.strip(),
                'description': (description or '').strip() or None,
                'filters_json': json.dumps(filters_json or {}, default=str),
                'sort_json': json.dumps(sort_json or {}, default=str),
                'is_default': bool(is_default),
                'actor_user_id': actor_user_id,
                'created_at': now,
                'updated_at': now,
            },
        ).fetchone()
        if is_default:
            db.execute(text('UPDATE portfolio_watchlists SET is_default = false WHERE org_id = :org_id AND id <> :id'), {'org_id': int(org_id), 'id': int(row[0])})
        db.commit()
        return get_watchlist(db, org_id=org_id, watchlist_id=int(row[0])) or {}

    db.execute(
        text(
            """
            UPDATE portfolio_watchlists
            SET name = :name,
                description = :description,
                filters_json = CAST(:filters_json AS JSONB),
                sort_json = CAST(:sort_json AS JSONB),
                is_default = :is_default,
                updated_by_user_id = :actor_user_id,
                updated_at = :updated_at
            WHERE org_id = :org_id AND id = :watchlist_id
            """
        ),
        {
            'org_id': int(org_id),
            'watchlist_id': int(watchlist_id),
            'name': name.strip(),
            'description': (description or '').strip() or None,
            'filters_json': json.dumps(filters_json or {}, default=str),
            'sort_json': json.dumps(sort_json or {}, default=str),
            'is_default': bool(is_default),
            'actor_user_id': actor_user_id,
            'updated_at': now,
        },
    )
    if is_default:
        db.execute(text('UPDATE portfolio_watchlists SET is_default = false WHERE org_id = :org_id AND id <> :watchlist_id'), {'org_id': int(org_id), 'watchlist_id': int(watchlist_id)})
    db.commit()
    return get_watchlist(db, org_id=org_id, watchlist_id=int(watchlist_id)) or {}


def delete_watchlist(db: Session, *, org_id: int, watchlist_id: int) -> bool:
    result = db.execute(
        text('DELETE FROM portfolio_watchlists WHERE org_id = :org_id AND id = :watchlist_id'),
        {'org_id': int(org_id), 'watchlist_id': int(watchlist_id)},
    )
    db.commit()
    return bool(result.rowcount)


def list_search_presets(db: Session, *, org_id: int) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            """
            SELECT id, org_id, name, filters_json, sort_json, created_by_user_id, updated_by_user_id,
                   created_at, updated_at
            FROM acquisition_search_presets
            WHERE org_id = :org_id
            ORDER BY updated_at DESC, id DESC
            """
        ),
        {'org_id': int(org_id)},
    ).fetchall()
    out = []
    for row in rows:
        item = _row_to_dict(row)
        item['filters_json'] = item.get('filters_json') or {}
        item['sort_json'] = item.get('sort_json') or {}
        out.append(item)
    return out


def upsert_search_preset(
    db: Session,
    *,
    org_id: int,
    preset_id: int | None,
    name: str,
    filters_json: dict[str, Any] | None,
    sort_json: dict[str, Any] | None,
    actor_user_id: int | None,
) -> dict[str, Any]:
    now = _utcnow()
    if preset_id is None:
        row = db.execute(
            text(
                """
                INSERT INTO acquisition_search_presets (
                    org_id, name, filters_json, sort_json, created_by_user_id, updated_by_user_id, created_at, updated_at
                ) VALUES (
                    :org_id, :name, CAST(:filters_json AS JSONB), CAST(:sort_json AS JSONB),
                    :actor_user_id, :actor_user_id, :created_at, :updated_at
                ) RETURNING id
                """
            ),
            {
                'org_id': int(org_id), 'name': name.strip(), 'filters_json': json.dumps(filters_json or {}, default=str),
                'sort_json': json.dumps(sort_json or {}, default=str), 'actor_user_id': actor_user_id,
                'created_at': now, 'updated_at': now,
            },
        ).fetchone()
        db.commit()
        return next((x for x in list_search_presets(db, org_id=org_id) if int(x['id']) == int(row[0])), {})

    db.execute(
        text(
            """
            UPDATE acquisition_search_presets
            SET name = :name,
                filters_json = CAST(:filters_json AS JSONB),
                sort_json = CAST(:sort_json AS JSONB),
                updated_by_user_id = :actor_user_id,
                updated_at = :updated_at
            WHERE org_id = :org_id AND id = :preset_id
            """
        ),
        {
            'org_id': int(org_id), 'preset_id': int(preset_id), 'name': name.strip(),
            'filters_json': json.dumps(filters_json or {}, default=str), 'sort_json': json.dumps(sort_json or {}, default=str),
            'actor_user_id': actor_user_id, 'updated_at': now,
        },
    )
    db.commit()
    return next((x for x in list_search_presets(db, org_id=org_id) if int(x['id']) == int(preset_id)), {})


def delete_search_preset(db: Session, *, org_id: int, preset_id: int) -> bool:
    result = db.execute(
        text('DELETE FROM acquisition_search_presets WHERE org_id = :org_id AND id = :preset_id'),
        {'org_id': int(org_id), 'preset_id': int(preset_id)},
    )
    db.commit()
    return bool(result.rowcount)
