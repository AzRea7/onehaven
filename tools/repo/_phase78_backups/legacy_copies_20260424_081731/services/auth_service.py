# backend/app/services/auth_service.py
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Organization, AppUser, OrgMembership, AuthIdentity


def _now() -> datetime:
    return datetime.utcnow()


# ---------------------------------------------------------------------
# Password hashing (PBKDF2)
# ---------------------------------------------------------------------
def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    iters = int(os.getenv("AUTH_PBKDF2_ITERS", "210000"))
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iters)
    return (
        f"pbkdf2_sha256${iters}$"
        f"{base64.b64encode(salt).decode()}$"
        f"{base64.b64encode(dk).decode()}"
    )


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


def _has_user_password_col() -> bool:
    # Avoid importing SQLAlchemy inspector here; keep it simple.
    return hasattr(AppUser, "password_hash")


# ---------------------------------------------------------------------
# Org/User helpers
# ---------------------------------------------------------------------
def get_or_create_org(db: Session, org_slug: str, org_name: Optional[str] = None) -> Organization:
    slug = (org_slug or "").strip()
    if not slug:
        raise ValueError("org_slug_required")

    org = db.scalar(select(Organization).where(Organization.slug == slug))
    if org:
        return org

    kwargs: dict[str, Any] = {"slug": slug, "name": (org_name or slug)}
    if hasattr(Organization, "created_at"):
        kwargs["created_at"] = _now()

    org = Organization(**kwargs)
    db.add(org)
    db.commit()
    db.refresh(org)
    return org


def get_or_create_user(db: Session, email: str) -> AppUser:
    em = (email or "").strip().lower()
    if not em:
        raise ValueError("email_required")

    u = db.scalar(select(AppUser).where(AppUser.email == em))
    if u:
        return u

    kwargs: dict[str, Any] = {"email": em, "display_name": em.split("@")[0]}
    if hasattr(AppUser, "created_at"):
        kwargs["created_at"] = _now()

    u = AppUser(**kwargs)
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def ensure_membership(db: Session, org_id: int, user_id: int, role: str = "owner") -> OrgMembership:
    """
    HARD GUARANTEE: membership exists (idempotent).
    This must be the single source-of-truth used by register and any dev provisioning flows.
    """
    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == int(org_id),
            OrgMembership.user_id == int(user_id),
        )
    )
    if mem:
        return mem

    kwargs: dict[str, Any] = {"org_id": int(org_id), "user_id": int(user_id), "role": str(role)}
    if hasattr(OrgMembership, "created_at"):
        kwargs["created_at"] = _now()

    mem = OrgMembership(**kwargs)
    db.add(mem)
    db.commit()
    db.refresh(mem)
    return mem


# ---------------------------------------------------------------------
# Public API used by routers/auth.py
# ---------------------------------------------------------------------
def register_local_user(
    db: Session,
    *,
    org_slug: str,
    org_name: Optional[str],
    email: str,
    password: str,
) -> dict[str, Any]:
    slug = (org_slug or "").strip()
    em = (email or "").strip().lower()
    pw = (password or "").strip()

    if not slug:
        raise ValueError("org_slug_required")
    if not em or not pw:
        raise ValueError("email_password_required")

    org = get_or_create_org(db, org_slug=slug, org_name=org_name)
    user = get_or_create_user(db, email=em)

    # Create AuthIdentity if missing (idempotent)
    ident = db.scalar(select(AuthIdentity).where(AuthIdentity.email == em))
    if ident is None:
        ident = AuthIdentity(email=em, password_hash=hash_password(pw))
        if hasattr(AuthIdentity, "created_at"):
            ident.created_at = _now()
        db.add(ident)
        db.commit()
        db.refresh(ident)

    # OPTIONAL: mirror password into AppUser.password_hash for legacy code paths
    if _has_user_password_col():
        if not getattr(user, "password_hash", None):
            try:
                user.password_hash = hash_password(pw)  # type: ignore[attr-defined]
                if hasattr(user, "email_verified"):
                    user.email_verified = True  # type: ignore[attr-defined]
                db.add(user)
                db.commit()
                db.refresh(user)
            except Exception:
                db.rollback()

    mem = ensure_membership(db, org_id=int(org.id), user_id=int(user.id), role="owner")

    return {
        "org_id": int(org.id),
        "org_slug": str(org.slug),
        "user_id": int(user.id),
        "email": str(user.email),
        "role": str(mem.role),
    }


def login_local_user(
    db: Session,
    *,
    org_slug: str,
    email: str,
    password: str,
) -> dict[str, Any]:
    slug = (org_slug or "").strip()
    em = (email or "").strip().lower()
    pw = (password or "").strip()

    if not slug:
        raise ValueError("org_slug_required")
    if not em or not pw:
        raise ValueError("email_password_required")

    org = db.scalar(select(Organization).where(Organization.slug == slug))
    if org is None:
        raise ValueError("org_not_found")

    user = db.scalar(select(AppUser).where(AppUser.email == em))
    if user is None:
        raise ValueError("invalid_credentials")

    # Check password in AuthIdentity first
    ident = db.scalar(select(AuthIdentity).where(AuthIdentity.email == em))
    ok = False
    if ident is not None and getattr(ident, "password_hash", None):
        ok = verify_password(pw, str(ident.password_hash))

    # Fallback: legacy AppUser.password_hash (if present)
    if not ok and _has_user_password_col():
        ph = getattr(user, "password_hash", None)
        if ph:
            ok = verify_password(pw, str(ph))

    if not ok:
        raise ValueError("invalid_credentials")

    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == int(org.id),
            OrgMembership.user_id == int(user.id),
        )
    )
    if mem is None:
        raise ValueError("not_a_member")

    return {
        "org_id": int(org.id),
        "org_slug": str(org.slug),
        "user_id": int(user.id),
        "email": str(user.email),
        "role": str(mem.role),
    }
