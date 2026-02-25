# backend/app/services/auth_service.py
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta
from typing import Any

import jwt  # PyJWT (you already have it for Clerk)

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Organization, AppUser, OrgMembership
from app.models_saas import AuthIdentity


def _now() -> datetime:
    return datetime.utcnow()


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    iters = int(os.getenv("AUTH_PBKDF2_ITERS", "210000"))
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return f"pbkdf2_sha256${iters}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        algo, iters_s, salt_b64, dk_b64 = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iters = int(iters_s)
        salt = base64.b64decode(salt_b64.encode())
        dk = base64.b64decode(dk_b64.encode())
        test = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
        return hmac.compare_digest(test, dk)
    except Exception:
        return False


def _jwt_secret() -> str:
    secret = os.getenv("JWT_SECRET", "").strip()
    if not secret:
        raise RuntimeError("JWT_SECRET env var is required for auth_mode=jwt")
    return secret


def create_access_token(*, subject: str, org_slug: str, user_id: int, role: str, minutes: int = 60 * 24) -> str:
    now = _now()
    payload: dict[str, Any] = {
        "sub": subject,
        "org": org_slug,
        "uid": int(user_id),
        "role": str(role),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=int(minutes))).timestamp()),
    }
    return jwt.encode(payload, _jwt_secret(), algorithm="HS256")


def decode_access_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, _jwt_secret(), algorithms=["HS256"])


def get_or_create_org(db: Session, org_slug: str, org_name: str | None = None) -> Organization:
    org_slug = org_slug.strip()
    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    if org:
        return org
    org = Organization(slug=org_slug, name=(org_name or org_slug))
    db.add(org)
    db.commit()
    db.refresh(org)
    return org


def get_or_create_user(db: Session, email: str) -> AppUser:
    email = email.strip().lower()
    u = db.scalar(select(AppUser).where(AppUser.email == email))
    if u:
        return u
    u = AppUser(email=email, display_name=email.split("@")[0])
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def ensure_membership(db: Session, org_id: int, user_id: int, role: str) -> OrgMembership:
    mem = db.scalar(select(OrgMembership).where(OrgMembership.org_id == int(org_id), OrgMembership.user_id == int(user_id)))
    if mem:
        return mem
    mem = OrgMembership(org_id=int(org_id), user_id=int(user_id), role=str(role))
    db.add(mem)
    db.commit()
    db.refresh(mem)
    return mem


def register_local_user(db: Session, *, org_slug: str, org_name: str, email: str, password: str) -> dict[str, Any]:
    org = get_or_create_org(db, org_slug=org_slug, org_name=org_name)
    user = get_or_create_user(db, email=email)

    ident = db.scalar(select(AuthIdentity).where(AuthIdentity.email == email.strip().lower()))
    if ident:
        # already exists -> treat as idempotent register (common SaaS behavior)
        mem = ensure_membership(db, org_id=int(org.id), user_id=int(user.id), role="owner")
        return {"org_id": int(org.id), "user_id": int(user.id), "role": str(mem.role)}

    ident = AuthIdentity(email=email.strip().lower(), password_hash=hash_password(password), created_at=_now())
    db.add(ident)
    db.commit()

    mem = ensure_membership(db, org_id=int(org.id), user_id=int(user.id), role="owner")
    return {"org_id": int(org.id), "user_id": int(user.id), "role": str(mem.role)}


def login_local_user(db: Session, *, org_slug: str, email: str, password: str) -> dict[str, Any]:
    email = email.strip().lower()
    org = db.scalar(select(Organization).where(Organization.slug == org_slug.strip()))
    if not org:
        raise ValueError("org_not_found")

    ident = db.scalar(select(AuthIdentity).where(AuthIdentity.email == email))
    if not ident or not verify_password(password, ident.password_hash):
        raise ValueError("invalid_credentials")

    user = db.scalar(select(AppUser).where(AppUser.email == email))
    if not user:
        raise ValueError("user_not_found")

    mem = db.scalar(select(OrgMembership).where(OrgMembership.org_id == int(org.id), OrgMembership.user_id == int(user.id)))
    if not mem:
        raise ValueError("not_a_member")

    token = create_access_token(subject=email, org_slug=str(org.slug), user_id=int(user.id), role=str(mem.role))
    return {"access_token": token, "org_slug": str(org.slug), "user_id": int(user.id), "role": str(mem.role)}