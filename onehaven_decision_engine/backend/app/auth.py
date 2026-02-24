# backend/app/auth.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

import jwt
from jwt import PyJWKClient

from fastapi import Depends, Header, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .db import get_db
from .models import Organization, AppUser, OrgMembership
from .services.jurisdiction_rules_service import ensure_seeded_for_org


@dataclass(frozen=True)
class Principal:
    org_id: int
    org_slug: str
    user_id: int
    email: str
    role: str  # owner | operator | analyst


ROLE_ORDER = {"analyst": 1, "operator": 2, "owner": 3}


def _require_role(principal: Principal, min_role: str) -> None:
    if ROLE_ORDER.get(principal.role, 0) < ROLE_ORDER.get(min_role, 999):
        raise HTTPException(status_code=403, detail=f"Requires role >= {min_role}")


def _get_or_create_user(db: Session, email: str) -> AppUser:
    user = db.scalar(select(AppUser).where(AppUser.email == email))
    if user:
        return user
    now = datetime.utcnow()
    user = AppUser(email=email, display_name=email.split("@")[0], created_at=now)
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _resolve_org(db: Session, org_slug: str) -> Organization:
    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    if org:
        return org
    if not settings.dev_auto_provision:
        raise HTTPException(status_code=401, detail="Unknown org (auto-provision disabled).")
    now = datetime.utcnow()
    org = Organization(slug=org_slug, name=org_slug, created_at=now)
    db.add(org)
    db.commit()
    db.refresh(org)
    try:
        ensure_seeded_for_org(db, org_id=int(org.id))
    except Exception:
        db.rollback()
    return org


def _get_or_create_membership(db: Session, org_id: int, user_id: int, role: str) -> OrgMembership:
    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == org_id,
            OrgMembership.user_id == user_id,
        )
    )
    if mem:
        return mem
    if not settings.dev_auto_provision:
        raise HTTPException(status_code=401, detail="Not a member of this org.")
    now = datetime.utcnow()
    mem = OrgMembership(org_id=org_id, user_id=user_id, role=role, created_at=now)
    db.add(mem)
    db.commit()
    db.refresh(mem)
    return mem


def _principal_from_db(db: Session, org_slug: str, email: str, role_hint: str | None = None) -> Principal:
    role = ((role_hint or "owner").strip().lower() or "owner")
    if role not in {"owner", "operator", "analyst"}:
        role = "owner"

    org = _resolve_org(db, org_slug=org_slug)
    user = _get_or_create_user(db, email=email)
    mem = _get_or_create_membership(db, org_id=int(org.id), user_id=int(user.id), role=role)

    return Principal(
        org_id=int(org.id),
        org_slug=str(org.slug),
        user_id=int(user.id),
        email=str(user.email),
        role=str(mem.role),
    )


def _verify_clerk_jwt(token: str) -> dict[str, Any]:
    if not settings.clerk_jwks_url:
        raise HTTPException(status_code=500, detail="clerk_jwks_url not configured")

    jwks = PyJWKClient(settings.clerk_jwks_url)
    signing_key = jwks.get_signing_key_from_jwt(token).key

    options = {"verify_aud": bool(settings.clerk_audience), "verify_iss": bool(settings.clerk_issuer)}

    try:
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=settings.clerk_audience,
            issuer=settings.clerk_issuer,
            options=options,
        )
        return dict(claims)
    except jwt.PyJWTError as e:
        raise HTTPException(status_code=401, detail=f"Invalid token: {str(e)}")


def get_principal(
    db: Session = Depends(get_db),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
    x_org_slug: Optional[str] = Header(default=None, alias="X-Org-Slug"),
    x_user_email: Optional[str] = Header(default=None, alias="X-User-Email"),
    x_user_role: Optional[str] = Header(default=None, alias="X-User-Role"),
) -> Principal:
    """
    Auth modes:
      - dev: X-Org-Slug + X-User-Email (+ X-User-Role)
      - clerk: Authorization: Bearer <JWT>, plus X-Org-Slug to select org context
    """
    if settings.auth_mode == "dev":
        org_slug = str(x_org_slug or "").strip()
        email = str(x_user_email or "").strip().lower()
        role = str(x_user_role or "owner").strip().lower() or "owner"

        if not org_slug or not email:
            raise HTTPException(
                status_code=401,
                detail="Missing auth headers. Provide X-Org-Slug and X-User-Email (and optionally X-User-Role).",
            )
        return _principal_from_db(db, org_slug=org_slug, email=email, role_hint=role)

    if settings.auth_mode == "clerk":
        if not authorization or not str(authorization).lower().startswith("bearer "):
            raise HTTPException(status_code=401, detail="Missing Authorization: Bearer <token>")

        org_slug = str(x_org_slug or "").strip()
        if not org_slug:
            raise HTTPException(status_code=401, detail="Missing X-Org-Slug (active org context).")

        token = str(authorization).split(" ", 1)[1].strip()
        claims = _verify_clerk_jwt(token)

        email = (
            (claims.get("email") or "")
            or (claims.get("primary_email") or "")
            or (claims.get("email_address") or "")
        )
        email = str(email).strip().lower()
        if not email:
            raise HTTPException(status_code=401, detail="Token verified, but no email claim present.")

        return _principal_from_db(db, org_slug=org_slug, email=email, role_hint="operator")

    raise HTTPException(status_code=500, detail=f"Unknown auth_mode={settings.auth_mode}")


def require_operator(p: Principal = Depends(get_principal)) -> Principal:
    _require_role(p, "operator")
    return p


def require_owner(p: Principal = Depends(get_principal)) -> Principal:
    _require_role(p, "owner")
    return p
