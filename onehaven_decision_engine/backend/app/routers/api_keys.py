# backend/app/routers/api_keys.py
from __future__ import annotations

import base64
import os
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal, require_owner, _hash_api_key  # noqa
from ..db import get_db
from ..models import ApiKey

router = APIRouter(prefix="/api-keys", tags=["api_keys"])


def _now() -> datetime:
    return datetime.utcnow()


def _new_key() -> str:
    # "ohk_" prefix makes it recognizable
    raw = base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("=")
    return f"ohk_{raw}"


@router.get("/")
def list_keys(db: Session = Depends(get_db), principal=Depends(get_principal)):
    q = select(ApiKey).where(ApiKey.org_id == principal.org_id).order_by(ApiKey.id.desc()).limit(200)
    rows = db.scalars(q).all()
    return [
        {
            "id": int(k.id),
            "name": str(k.name),
            "key_prefix": str(k.key_prefix),
            "revoked_at": k.revoked_at,
            "created_at": k.created_at,
        }
        for k in rows
    ]


@router.post("/")
def create_key(payload: dict[str, Any], db: Session = Depends(get_db), principal=Depends(get_principal)):
    require_owner(principal)

    name = str(payload.get("name") or "").strip() or "default"
    raw = _new_key()
    prefix_len = int(getattr(principal, "api_key_prefix_len", 12) or 12)

    key_prefix = raw[:prefix_len]
    key_hash = _hash_api_key(raw)

    existing = db.scalar(select(ApiKey).where(ApiKey.org_id == principal.org_id, ApiKey.key_prefix == key_prefix))
    if existing is not None:
        raise HTTPException(status_code=409, detail="Key collision; retry")

    row = ApiKey(
        org_id=int(principal.org_id),
        name=name,
        key_prefix=key_prefix,
        key_hash=key_hash,
        created_by_user_id=int(principal.user_id) if principal.user_id else None,
        created_at=_now(),
        revoked_at=None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    # ✅ return the raw key ONCE (store it client-side)
    return {"ok": True, "id": int(row.id), "name": row.name, "api_key": raw, "key_prefix": key_prefix}


@router.post("/{key_id}/revoke")
def revoke_key(key_id: int, db: Session = Depends(get_db), principal=Depends(get_principal)):
    require_owner(principal)

    row = db.scalar(select(ApiKey).where(ApiKey.id == int(key_id), ApiKey.org_id == principal.org_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")

    row.revoked_at = _now()
    db.add(row)
    db.commit()
    return {"ok": True, "id": int(row.id), "revoked_at": row.revoked_at}