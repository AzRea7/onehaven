# backend/app/services/locks_service.py
from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import OrgLock


def _now() -> datetime:
    return datetime.utcnow()


def acquire_lock(db: Session, *, org_id: int, lock_key: str, owner: str | None, ttl_seconds: int) -> bool:
    """
    Advisory lock in DB.
    - returns True if lock acquired/renewed
    - returns False if held by someone else (and not expired)
    """
    expires = _now() + timedelta(seconds=int(ttl_seconds))

    row = db.scalar(select(OrgLock).where(OrgLock.org_id == int(org_id), OrgLock.lock_key == lock_key))
    if row is None:
        db.add(OrgLock(org_id=int(org_id), lock_key=lock_key, owner=owner, expires_at=expires, created_at=_now()))
        return True

    # expired => steal
    if row.expires_at and row.expires_at <= _now():
        row.owner = owner
        row.expires_at = expires
        db.add(row)
        return True

    # held by same owner => renew
    if (row.owner or "") == (owner or ""):
        row.expires_at = expires
        db.add(row)
        return True

    return False


def release_lock(db: Session, *, org_id: int, lock_key: str, owner: str | None) -> bool:
    row = db.scalar(select(OrgLock).where(OrgLock.org_id == int(org_id), OrgLock.lock_key == lock_key))
    if row is None:
        return True
    if owner and (row.owner or "") != owner:
        # don't release someone else's lock
        return False
    # set expires in past
    row.expires_at = _now() - timedelta(seconds=1)
    db.add(row)
    return True