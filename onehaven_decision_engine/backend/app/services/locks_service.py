from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import OrgLock


@dataclass(frozen=True)
class LockResult:
    acquired: bool
    lock_key: str
    holder: str | None
    expires_at: datetime | None
    stale: bool
    org_id: int


def _now() -> datetime:
    return datetime.utcnow()


def _get_expiry_attr_name(row: Any) -> str:
    if hasattr(row, "locked_until"):
        return "locked_until"
    if hasattr(row, "expires_at"):
        return "expires_at"
    raise AttributeError("OrgLock must expose either 'locked_until' or 'expires_at'")


def _get_holder_attr_name(row: Any) -> str:
    if hasattr(row, "owner_token"):
        return "owner_token"
    if hasattr(row, "owner"):
        return "owner"
    raise AttributeError("OrgLock must expose either 'owner_token' or 'owner'")


def _get_expiry(row: Any) -> datetime | None:
    try:
        return getattr(row, _get_expiry_attr_name(row))
    except Exception:
        return None


def _set_expiry(row: Any, value: datetime) -> None:
    setattr(row, _get_expiry_attr_name(row), value)


def _get_holder(row: Any) -> str | None:
    try:
        raw = getattr(row, _get_holder_attr_name(row), None)
        return str(raw) if raw else None
    except Exception:
        return None


def _normalize_owner(owner: str | None) -> str:
    return str(owner or "").strip() or "system"


def _set_holder(row: Any, owner: str | None) -> None:
    setattr(row, _get_holder_attr_name(row), _normalize_owner(owner))


def _touch_timestamps(row: Any, *, now: datetime, acquired: bool = False) -> None:
    if hasattr(row, "updated_at"):
        setattr(row, "updated_at", now)
    if hasattr(row, "created_at") and getattr(row, "created_at", None) is None:
        setattr(row, "created_at", now)
    if hasattr(row, "acquired_at") and (acquired or getattr(row, "acquired_at", None) is None):
        setattr(row, "acquired_at", now)


def _is_stale(row: Any, *, now: datetime | None = None) -> bool:
    now = now or _now()
    expires_at = _get_expiry(row)
    if expires_at is None:
        return False
    return expires_at <= now


def _row_to_result(row: Any, *, acquired: bool, org_id: int, lock_key: str) -> LockResult:
    return LockResult(
        acquired=bool(acquired),
        lock_key=str(lock_key),
        holder=_get_holder(row),
        expires_at=_get_expiry(row),
        stale=_is_stale(row),
        org_id=int(org_id),
    )


def get_lock(
    db: Session,
    *,
    org_id: int,
    lock_key: str,
) -> LockResult:
    row = db.scalar(
        select(OrgLock).where(
            OrgLock.org_id == int(org_id),
            OrgLock.lock_key == str(lock_key),
        )
    )
    if row is None:
        return LockResult(
            acquired=False,
            lock_key=str(lock_key),
            holder=None,
            expires_at=None,
            stale=False,
            org_id=int(org_id),
        )
    return _row_to_result(row, acquired=False, org_id=int(org_id), lock_key=str(lock_key))


def acquire_lock(
    db: Session,
    *,
    org_id: int,
    lock_key: str,
    owner: str | None,
    ttl_seconds: int,
    now: datetime | None = None,
) -> LockResult:
    now = now or _now()
    ttl_seconds = max(1, int(ttl_seconds))
    expires_at = now + timedelta(seconds=ttl_seconds)

    row = db.scalar(
        select(OrgLock).where(
            OrgLock.org_id == int(org_id),
            OrgLock.lock_key == str(lock_key),
        )
    )

    if row is None:
        row = OrgLock(
            org_id=int(org_id),
            lock_key=str(lock_key),
        )
        _set_holder(row, owner)
        _set_expiry(row, expires_at)
        _touch_timestamps(row, now=now, acquired=True)
        db.add(row)
        db.flush()
        return _row_to_result(row, acquired=True, org_id=int(org_id), lock_key=str(lock_key))

    if _is_stale(row, now=now):
        _set_holder(row, owner)
        _set_expiry(row, expires_at)
        _touch_timestamps(row, now=now, acquired=True)
        db.add(row)
        db.flush()
        return _row_to_result(row, acquired=True, org_id=int(org_id), lock_key=str(lock_key))

    current_holder = _get_holder(row)
    desired_holder = str(owner or "").strip() or "system"
    if (current_holder or "") == desired_holder:
        _set_holder(row, desired_holder)
        _set_expiry(row, expires_at)
        _touch_timestamps(row, now=now, acquired=True)
        db.add(row)
        db.flush()
        return _row_to_result(row, acquired=True, org_id=int(org_id), lock_key=str(lock_key))

    return _row_to_result(row, acquired=False, org_id=int(org_id), lock_key=str(lock_key))


def renew_lock(
    db: Session,
    *,
    org_id: int,
    lock_key: str,
    owner: str | None,
    ttl_seconds: int,
    now: datetime | None = None,
) -> LockResult:
    return acquire_lock(
        db,
        org_id=int(org_id),
        lock_key=str(lock_key),
        owner=owner,
        ttl_seconds=int(ttl_seconds),
        now=now,
    )


def release_lock(
    db: Session,
    *,
    org_id: int,
    lock_key: str,
    owner: str | None = None,
    force: bool = False,
    now: datetime | None = None,
) -> LockResult:
    now = now or _now()

    row = db.scalar(
        select(OrgLock).where(
            OrgLock.org_id == int(org_id),
            OrgLock.lock_key == str(lock_key),
        )
    )

    if row is None:
        return LockResult(
            acquired=False,
            lock_key=str(lock_key),
            holder=None,
            expires_at=None,
            stale=False,
            org_id=int(org_id),
        )

    current_holder = _get_holder(row)
    desired_holder = str(owner or "").strip() or "system"

    if not force and owner and current_holder and current_holder != desired_holder:
        return _row_to_result(row, acquired=False, org_id=int(org_id), lock_key=str(lock_key))

    _set_expiry(row, now - timedelta(seconds=1))
    if force or not owner or current_holder == desired_holder:
        _set_holder(row, desired_holder)
    _touch_timestamps(row, now=now, acquired=True)
    db.add(row)
    db.flush()

    return _row_to_result(row, acquired=True, org_id=int(org_id), lock_key=str(lock_key))


def clear_stale_lock(
    db: Session,
    *,
    org_id: int,
    lock_key: str,
    now: datetime | None = None,
) -> LockResult:
    now = now or _now()
    row = db.scalar(
        select(OrgLock).where(
            OrgLock.org_id == int(org_id),
            OrgLock.lock_key == str(lock_key),
        )
    )
    if row is None:
        return LockResult(
            acquired=False,
            lock_key=str(lock_key),
            holder=None,
            expires_at=None,
            stale=False,
            org_id=int(org_id),
        )

    if not _is_stale(row, now=now):
        return _row_to_result(row, acquired=False, org_id=int(org_id), lock_key=str(lock_key))

    _set_expiry(row, now - timedelta(seconds=1))
    _touch_timestamps(row, now=now, acquired=True)
    db.add(row)
    db.flush()
    return _row_to_result(row, acquired=True, org_id=int(org_id), lock_key=str(lock_key))


def is_lock_active(
    db: Session,
    *,
    org_id: int,
    lock_key: str,
    now: datetime | None = None,
) -> bool:
    now = now or _now()
    row = db.scalar(
        select(OrgLock).where(
            OrgLock.org_id == int(org_id),
            OrgLock.lock_key == str(lock_key),
        )
    )
    if row is None:
        return False
    return not _is_stale(row, now=now)


def build_ingestion_execution_lock_key(
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
) -> str:
    return f"ingestion_exec:{int(org_id)}:{int(source_id)}:{dataset_key}"


def build_ingestion_completed_lock_key(
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
) -> str:
    return f"ingestion_done:{int(org_id)}:{int(source_id)}:{dataset_key}"


def acquire_ingestion_execution_lock(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
    owner: str | None,
    ttl_seconds: int,
    now: datetime | None = None,
) -> LockResult:
    return acquire_lock(
        db,
        org_id=int(org_id),
        lock_key=build_ingestion_execution_lock_key(
            org_id=int(org_id),
            source_id=int(source_id),
            dataset_key=str(dataset_key),
        ),
        owner=owner,
        ttl_seconds=int(ttl_seconds),
        now=now,
    )


def release_ingestion_execution_lock(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
    owner: str | None = None,
    force: bool = False,
    now: datetime | None = None,
) -> LockResult:
    return release_lock(
        db,
        org_id=int(org_id),
        lock_key=build_ingestion_execution_lock_key(
            org_id=int(org_id),
            source_id=int(source_id),
            dataset_key=str(dataset_key),
        ),
        owner=owner,
        force=force,
        now=now,
    )


def has_completed_ingestion_dataset(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
    now: datetime | None = None,
) -> bool:
    return is_lock_active(
        db,
        org_id=int(org_id),
        lock_key=build_ingestion_completed_lock_key(
            org_id=int(org_id),
            source_id=int(source_id),
            dataset_key=str(dataset_key),
        ),
        now=now,
    )


def mark_ingestion_dataset_completed(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
    owner: str | None,
    ttl_seconds: int,
    now: datetime | None = None,
) -> LockResult:
    return acquire_lock(
        db,
        org_id=int(org_id),
        lock_key=build_ingestion_completed_lock_key(
            org_id=int(org_id),
            source_id=int(source_id),
            dataset_key=str(dataset_key),
        ),
        owner=owner,
        ttl_seconds=int(ttl_seconds),
        now=now,
    )



def clear_stale_locks_for_prefix(
    db: Session,
    *,
    org_id: int,
    lock_key_prefix: str,
    now: datetime | None = None,
) -> int:
    now = now or _now()
    rows = list(
        db.scalars(
            select(OrgLock).where(
                OrgLock.org_id == int(org_id),
                OrgLock.lock_key.like(f"{str(lock_key_prefix)}%"),
            )
        ).all()
    )
    cleared = 0
    for row in rows:
        if not _is_stale(row, now=now):
            continue
        _set_expiry(row, now - timedelta(seconds=1))
        _touch_timestamps(row, now=now)
        db.add(row)
        cleared += 1
    if cleared:
        db.flush()
    return cleared


def renew_ingestion_execution_lock(
    db: Session,
    *,
    org_id: int,
    source_id: int,
    dataset_key: str,
    owner: str | None,
    ttl_seconds: int,
    now: datetime | None = None,
) -> LockResult:
    return renew_lock(
        db,
        org_id=int(org_id),
        lock_key=build_ingestion_execution_lock_key(
            org_id=int(org_id),
            source_id=int(source_id),
            dataset_key=str(dataset_key),
        ),
        owner=_normalize_owner(owner),
        ttl_seconds=int(ttl_seconds),
        now=now,
    )
