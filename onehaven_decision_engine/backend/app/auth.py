# backend/app/auth.py
from __future__ import annotations

import base64
import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Any

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from .config import settings
from .db import get_db
from .models import Organization, AppUser, OrgMembership, ApiKey

# -----------------------------------------------------------------------------
# ✅ COMPAT IMPORT:
# Your repo defines OrgSubscription (not Subscription).
# Some older code refers to Subscription. We support both safely.
# -----------------------------------------------------------------------------
try:
    from .models import Subscription as Subscription  # type: ignore
except Exception:
    from .models import OrgSubscription as Subscription  # type: ignore


@dataclass(frozen=True)
class Principal:
    org_id: int
    org_slug: str
    user_id: int
    email: str
    role: str  # owner | operator | analyst
    plan_code: str | None = None


ROLE_ORDER = {"analyst": 1, "operator": 2, "owner": 3}


def _require_role(principal: Principal, min_role: str) -> None:
    if ROLE_ORDER.get(principal.role, 0) < ROLE_ORDER.get(min_role, 999):
        raise HTTPException(status_code=403, detail=f"Requires role >= {min_role}")


def require_operator(p: Principal = Depends(lambda: None)) -> Principal:  # overwritten below
    raise RuntimeError("require_operator not wired")


def require_owner(p: Principal = Depends(lambda: None)) -> Principal:  # overwritten below
    raise RuntimeError("require_owner not wired")


# -------------------------
# Password hashing (simple)
# -------------------------
def _hash_password(password: str) -> str:
    salt = base64.urlsafe_b64encode(hashlib.sha256(str(datetime.utcnow()).encode()).digest())[:16]
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 120_000)
    return f"pbkdf2_sha256${salt.decode()}${base64.urlsafe_b64encode(dk).decode()}"


def _verify_password(password: str, stored: str) -> bool:
    try:
        algo, salt_s, hash_s = stored.split("$", 2)
        if algo != "pbkdf2_sha256":
            return False
        salt = salt_s.encode()
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 120_000)
        return hmac.compare_digest(base64.urlsafe_b64encode(dk).decode(), hash_s)
    except Exception:
        return False


# -------------------------
# JWT helpers
# -------------------------
def _jwt_sign(payload: dict[str, Any]) -> str:
    import json

    jwt_secret = str(getattr(settings, "jwt_secret", "dev-secret-change-me"))

    def b64(x: bytes) -> str:
        return base64.urlsafe_b64encode(x).decode().rstrip("=")

    header = {"alg": "HS256", "typ": "JWT"}
    header_b = b64(json.dumps(header, separators=(",", ":")).encode())
    payload_b = b64(json.dumps(payload, separators=(",", ":")).encode())
    msg = f"{header_b}.{payload_b}".encode()
    sig = hmac.new(jwt_secret.encode(), msg, hashlib.sha256).digest()
    sig_b = b64(sig)
    return f"{header_b}.{payload_b}.{sig_b}"


def _jwt_verify(token: str) -> dict[str, Any]:
    import json

    jwt_secret = str(getattr(settings, "jwt_secret", "dev-secret-change-me"))

    def ub64(s: str) -> bytes:
        s2 = s + "=" * (-len(s) % 4)
        return base64.urlsafe_b64decode(s2.encode())

    try:
        header_b, payload_b, sig_b = token.split(".", 2)
        msg = f"{header_b}.{payload_b}".encode()
        sig = ub64(sig_b)
        expected = hmac.new(jwt_secret.encode(), msg, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            raise HTTPException(status_code=401, detail="Invalid token signature")

        payload = json.loads(ub64(payload_b).decode())
        exp = payload.get("exp")
        if exp is not None and int(exp) < int(datetime.utcnow().timestamp()):
            raise HTTPException(status_code=401, detail="Token expired")
        return dict(payload)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


# -------------------------
# Org + membership helpers
# -------------------------
def _resolve_org(db: Session, org_slug: str) -> Organization:
    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    if org:
        return org
    raise HTTPException(status_code=401, detail="Unknown org")


def _get_user_by_email(db: Session, email: str) -> AppUser | None:
    return db.scalar(select(AppUser).where(AppUser.email == email))


def _get_membership(db: Session, org_id: int, user_id: int) -> OrgMembership | None:
    return db.scalar(select(OrgMembership).where(OrgMembership.org_id == org_id, OrgMembership.user_id == user_id))


def _get_plan_code_for_org(db: Session, org_id: int) -> str | None:
    try:
        sub = db.scalar(select(Subscription).where(Subscription.org_id == org_id).order_by(Subscription.id.desc()))
        if sub is not None:
            status = getattr(sub, "status", None)
            plan_code = getattr(sub, "plan_code", None)
            if (status is None or str(status) == "active") and plan_code:
                return str(plan_code)
    except Exception:
        pass

    return str(getattr(settings, "default_plan_code", "free") or "free")


def _principal_from_user(db: Session, *, org_slug: str, user: AppUser) -> Principal:
    org = _resolve_org(db, org_slug=org_slug)
    mem = _get_membership(db, org_id=int(org.id), user_id=int(user.id))
    if mem is None:
        raise HTTPException(status_code=403, detail="Not a member of this org")

    plan_code = _get_plan_code_for_org(db, org_id=int(org.id))
    return Principal(
        org_id=int(org.id),
        org_slug=str(org.slug),
        user_id=int(user.id),
        email=str(user.email),
        role=str(mem.role),
        plan_code=plan_code,
    )


# -------------------------
# API Key helpers
# -------------------------
def _hash_api_key(raw: str) -> str:
    api_key_pepper = str(getattr(settings, "api_key_pepper", "dev-pepper-change-me"))
    digest = hmac.new(api_key_pepper.encode(), raw.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode()


def _verify_api_key(db: Session, raw: str, org_slug: str) -> Principal:
    org = _resolve_org(db, org_slug=org_slug)

    prefix_len = int(getattr(settings, "api_key_prefix_len", 8))
    prefix = raw[:prefix_len]

    row = db.scalar(select(ApiKey).where(ApiKey.org_id == int(org.id), ApiKey.key_prefix == prefix))
    if row is None or getattr(row, "revoked_at", None) is not None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    expected = str(row.key_hash)
    got = _hash_api_key(raw)
    if not hmac.compare_digest(expected, got):
        raise HTTPException(status_code=401, detail="Invalid API key")

    role = "operator"
    if getattr(row, "created_by_user_id", None):
        mem = _get_membership(db, org_id=int(org.id), user_id=int(row.created_by_user_id))
        if mem:
            role = str(mem.role)

    plan_code = _get_plan_code_for_org(db, org_id=int(org.id))
    return Principal(
        org_id=int(org.id),
        org_slug=str(org.slug),
        user_id=int(getattr(row, "created_by_user_id", 0) or 0),
        email="api-key",
        role=role,
        plan_code=plan_code,
    )


# -------------------------
# get_principal
# -------------------------
def get_principal(
    request: Request,
    db: Session = Depends(get_db),
    x_org_slug: Optional[str] = Header(default=None, alias="X-Org-Slug"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> Principal:
    org_slug = str(x_org_slug or "").strip()
    if not org_slug:
        raise HTTPException(status_code=401, detail="Missing X-Org-Slug (active org context).")

    enable_api_keys = bool(getattr(settings, "enable_api_keys", False))
    jwt_cookie_name = str(getattr(settings, "jwt_cookie_name", "oh_jwt") or "oh_jwt")
    auth_mode = str(getattr(settings, "auth_mode", "dev") or "dev").lower()

    # 1) API key
    if enable_api_keys and x_api_key:
        return _verify_api_key(db, raw=str(x_api_key).strip(), org_slug=org_slug)

    # 2) JWT cookie or Bearer token
    token = request.cookies.get(jwt_cookie_name) if jwt_cookie_name else None
    if not token and authorization and str(authorization).lower().startswith("bearer "):
        token = str(authorization).split(" ", 1)[1].strip()

    if token:
        claims = _jwt_verify(token)
        sub = str(claims.get("sub") or "")
        if not sub:
            raise HTTPException(status_code=401, detail="Token missing sub")

        user_id = int(sub)
        user = db.scalar(select(AppUser).where(AppUser.id == user_id))
        if user is None:
            raise HTTPException(status_code=401, detail="Unknown user")

        return _principal_from_user(db, org_slug=org_slug, user=user)

    # 3) Dev spoofing
    if auth_mode == "dev":
        email = (request.headers.get("X-User-Email") or "").strip().lower()
        role_hint = (request.headers.get("X-User-Role") or "owner").strip().lower()
        if not email:
            raise HTTPException(status_code=401, detail="Missing X-User-Email for dev auth")

        dev_auto_provision = bool(getattr(settings, "dev_auto_provision", False))

        org = db.scalar(select(Organization).where(Organization.slug == org_slug))
        if org is None and dev_auto_provision:
            org = Organization(slug=org_slug, name=org_slug, created_at=datetime.utcnow())
            db.add(org)
            db.commit()
            db.refresh(org)

        user = _get_user_by_email(db, email=email)
        if user is None and dev_auto_provision:
            user = AppUser(email=email, display_name=email.split("@")[0], created_at=datetime.utcnow())
            db.add(user)
            db.commit()
            db.refresh(user)

        if org is None or user is None:
            raise HTTPException(status_code=401, detail="Dev auth could not provision user/org")

        mem = db.scalar(select(OrgMembership).where(OrgMembership.org_id == int(org.id), OrgMembership.user_id == int(user.id)))
        if mem is None and dev_auto_provision:
            mem = OrgMembership(
                org_id=int(org.id),
                user_id=int(user.id),
                role=role_hint if role_hint in ROLE_ORDER else "owner",
                created_at=datetime.utcnow(),
            )
            db.add(mem)
            db.commit()
            db.refresh(mem)

        if mem is None:
            raise HTTPException(status_code=403, detail="Not a member of this org")

        plan_code = _get_plan_code_for_org(db, org_id=int(org.id))
        return Principal(
            org_id=int(org.id),
            org_slug=str(org.slug),
            user_id=int(user.id),
            email=str(user.email),
            role=str(mem.role),
            plan_code=plan_code,
        )

    raise HTTPException(status_code=401, detail="Not authenticated")


def require_operator(p: Principal = Depends(get_principal)) -> Principal:
    _require_role(p, "operator")
    return p


def require_owner(p: Principal = Depends(get_principal)) -> Principal:
    _require_role(p, "owner")
    return p
