# backend/app/routers/api_keys.py
from __future__ import annotations

import base64
import json
import os
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import Principal, _hash_api_key, get_principal, require_owner
from ..db import get_db
from ..models import ApiKey
from ..services import plan_service, usage_service

router = APIRouter(prefix="/api-keys", tags=["api_keys"])


DEFAULT_KEY_SCOPES = ["org:read"]
OWNER_ONLY_SCOPES = {
    "admin:*",
    "admin:apikeys",
    "billing:*",
}
VALID_SCOPES = {
    "org:full",
    "org:read",
    "properties:read",
    "properties:write",
    "agents:read",
    "agents:write",
    "automation:read",
    "automation:write",
    "automation:premium",
    "reports:read",
    "reports:premium",
    "partners:read",
    "partners:export",
    "admin:*",
    "admin:apikeys",
}


def _now() -> datetime:
    return datetime.utcnow()


def _new_key() -> str:
    raw = base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("=")
    return f"ohk_{raw}"


def _normalize_scopes(raw: Any) -> list[str]:
    if raw is None:
        return list(DEFAULT_KEY_SCOPES)

    scopes: list[str] = []
    if isinstance(raw, str):
        scopes = [x.strip().lower() for x in raw.split(",") if x.strip()]
    elif isinstance(raw, list):
        scopes = [str(x).strip().lower() for x in raw if str(x).strip()]

    normalized = sorted(set(scopes or DEFAULT_KEY_SCOPES))
    invalid = [s for s in normalized if s not in VALID_SCOPES]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_api_key_scopes",
                "invalid_scopes": invalid,
            },
        )
    return normalized


def _serialize_scopes(row: Any) -> list[str]:
    scopes: list[str] = []

    if hasattr(row, "scopes_json"):
        try:
            parsed = json.loads(getattr(row, "scopes_json", "[]") or "[]")
            if isinstance(parsed, list):
                scopes.extend([str(x).strip().lower() for x in parsed if str(x).strip()])
        except Exception:
            pass

    if hasattr(row, "scopes_csv"):
        raw_csv = str(getattr(row, "scopes_csv", "") or "")
        scopes.extend([x.strip().lower() for x in raw_csv.split(",") if x.strip()])

    if hasattr(row, "meta_json"):
        try:
            meta = json.loads(getattr(row, "meta_json", "{}") or "{}")
            if isinstance(meta, dict):
                raw_scopes = meta.get("scopes")
                if isinstance(raw_scopes, list):
                    scopes.extend([str(x).strip().lower() for x in raw_scopes if str(x).strip()])
        except Exception:
            pass

    return sorted(set(scopes or DEFAULT_KEY_SCOPES))


def _write_scopes(row: Any, scopes: list[str]) -> None:
    if hasattr(row, "scopes_json"):
        setattr(row, "scopes_json", json.dumps(scopes, separators=(",", ":"), sort_keys=True))
    if hasattr(row, "scopes_csv"):
        setattr(row, "scopes_csv", ",".join(scopes))

    if hasattr(row, "meta_json"):
        try:
            meta = json.loads(getattr(row, "meta_json", "{}") or "{}")
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}
        meta["scopes"] = scopes
        setattr(row, "meta_json", json.dumps(meta, separators=(",", ":"), sort_keys=True))


def _assert_principal_can_issue_scopes(principal: Principal, scopes: list[str]) -> None:
    if principal.role != "owner":
        raise HTTPException(
            status_code=403,
            detail={
                "error": "insufficient_role",
                "message": "Only owners can create API keys",
            },
        )

    forbidden = [s for s in scopes if s in OWNER_ONLY_SCOPES]
    if forbidden and principal.role != "owner":
        raise HTTPException(
            status_code=403,
            detail={
                "error": "owner_scope_required",
                "scopes": forbidden,
            },
        )


@router.get("/")
def list_keys(db: Session = Depends(get_db), principal=Depends(get_principal)):
    q = select(ApiKey).where(ApiKey.org_id == principal.org_id).order_by(ApiKey.id.desc()).limit(200)
    rows = db.scalars(q).all()

    out: list[dict[str, Any]] = []
    for k in rows:
        disabled = False
        if hasattr(k, "disabled_at") and getattr(k, "disabled_at", None) is not None:
            disabled = True
        revoked = getattr(k, "revoked_at", None) is not None

        out.append(
            {
                "id": int(k.id),
                "name": str(k.name),
                "key_prefix": str(k.key_prefix),
                "scopes": _serialize_scopes(k),
                "revoked_at": getattr(k, "revoked_at", None),
                "disabled_at": getattr(k, "disabled_at", None) if hasattr(k, "disabled_at") else None,
                "last_used_at": getattr(k, "last_used_at", None) if hasattr(k, "last_used_at") else None,
                "created_at": getattr(k, "created_at", None),
                "created_by_user_id": getattr(k, "created_by_user_id", None),
                "is_active": not revoked and not disabled,
            }
        )
    return out


@router.post("/")
def create_key(payload: dict[str, Any], db: Session = Depends(get_db), principal=Depends(require_owner)):
    usage_service.assert_can_create_api_key(db, org_id=int(principal.org_id))

    name = str(payload.get("name") or "").strip() or "default"
    scopes = _normalize_scopes(payload.get("scopes"))
    _assert_principal_can_issue_scopes(principal, scopes)
    plan_service.assert_scopes_allowed_for_plan(db, org_id=int(principal.org_id), scopes=scopes)

    raw = _new_key()
    prefix_len = int(getattr(settings, "api_key_prefix_len", 12) or 12) if "settings" in globals() else 12
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
    _write_scopes(row, scopes)

    if hasattr(row, "disabled_at"):
        setattr(row, "disabled_at", None)
    if hasattr(row, "last_used_at"):
        setattr(row, "last_used_at", None)

    db.add(row)
    db.commit()
    db.refresh(row)

    return {
        "ok": True,
        "id": int(row.id),
        "name": row.name,
        "api_key": raw,
        "key_prefix": key_prefix,
        "scopes": scopes,
    }


@router.post("/{key_id}/revoke")
def revoke_key(key_id: int, db: Session = Depends(get_db), principal=Depends(require_owner)):
    row = db.scalar(select(ApiKey).where(ApiKey.id == int(key_id), ApiKey.org_id == principal.org_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")

    row.revoked_at = _now()
    db.add(row)
    db.commit()
    return {"ok": True, "id": int(row.id), "revoked_at": row.revoked_at}
