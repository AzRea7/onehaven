# backend/app/routers/auth.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..config import settings
from ..db import get_db
from ..models import Organization, OrgMembership, Plan, OrgSubscription, AppUser
from ..schemas import PrincipalOut
from ..auth import _jwt_sign, _jwt_verify  # type: ignore
from ..services import auth_service

router = APIRouter(prefix="/auth", tags=["auth"])


def _now() -> datetime:
    return datetime.utcnow()


def _ensure_default_plan_seeded(db: Session) -> None:
    def upsert(code: str, name: str, limits: dict[str, Any]) -> None:
        row = db.scalar(select(Plan).where(Plan.code == code))
        if row:
            return
        db.add(Plan(code=code, name=name, limits_json=str(limits).replace("'", '"'), created_at=_now()))

    upsert(
        "free",
        "Free",
        {"max_properties": 3, "agent_runs_per_day": 20, "external_calls_per_day": 50, "max_concurrent_runs": 2},
    )
    upsert(
        "starter",
        "Starter",
        {"max_properties": 25, "agent_runs_per_day": 200, "external_calls_per_day": 500, "max_concurrent_runs": 5},
    )
    db.commit()


def _cookie_name() -> str:
    # Keep your debug-cookie output consistent with what the backend actually uses.
    return str(getattr(settings, "jwt_cookie_name", "onehaven_jwt") or "onehaven_jwt")


def _is_localhost(request: Request) -> bool:
    host = (request.headers.get("host") or "").lower()
    return host.startswith("localhost") or host.startswith("127.0.0.1")


def _is_https(request: Request) -> bool:
    xf_proto = (request.headers.get("x-forwarded-proto") or "").lower().strip()
    if xf_proto:
        return xf_proto == "https"
    return request.url.scheme == "https"


def _set_auth_cookie(response: Response, request: Request, token: str) -> None:
    """
    Cookie correctness rules (browser enforced):
      - SameSite=None requires Secure=True
      - Secure=True cookies will NOT be stored on http://localhost
    So for localhost dev, force: secure=False, samesite='lax'
    """
    name = _cookie_name()
    max_age = int(getattr(settings, "jwt_exp_minutes", 60)) * 60

    secure_setting = bool(getattr(settings, "jwt_cookie_secure", False))
    samesite_setting = str(getattr(settings, "jwt_cookie_samesite", "lax") or "lax").lower()

    if _is_localhost(request) or not _is_https(request):
        secure = False
        samesite = "lax"
    else:
        secure = secure_setting
        if samesite_setting == "none" and not secure:
            samesite = "lax"
        else:
            samesite = samesite_setting

    response.set_cookie(
        name,
        token,
        httponly=True,
        secure=secure,
        samesite=samesite,
        max_age=max_age,
        path="/",
    )


def _cookie_token(request: Request) -> Optional[str]:
    return request.cookies.get(_cookie_name())


def _require_user_id_from_cookie(request: Request) -> int:
    token = _cookie_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    claims = _jwt_verify(token)
    sub = str(claims.get("sub") or "").strip()
    if not sub:
        raise HTTPException(status_code=401, detail="Token missing sub")
    return int(sub)


def _org_from_cookie(request: Request) -> Optional[str]:
    token = _cookie_token(request)
    if not token:
        return None
    try:
        claims = _jwt_verify(token)
        org = str(claims.get("org") or "").strip()
        return org or None
    except Exception:
        return None


def _principal(db: Session, *, user_id: int, org_slug: str) -> PrincipalOut:
    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    if org is None:
        raise HTTPException(status_code=401, detail="Unknown org")

    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == int(org.id),
            OrgMembership.user_id == int(user_id),
        )
    )
    if mem is None:
        raise HTTPException(
            status_code=403,
            detail=f"Not a member of org (org_slug={org_slug}, org_id={int(org.id)}, user_id={int(user_id)})",
        )

    user = db.scalar(select(AppUser).where(AppUser.id == int(user_id)))
    if user is None:
        raise HTTPException(status_code=401, detail="Unknown user")

    return PrincipalOut(
        org_id=int(org.id),
        org_slug=str(org.slug),
        user_id=int(user.id),
        email=str(user.email),
        role=str(mem.role),
    )


@router.post("/register")
def register(payload: dict[str, Any], request: Request, response: Response, db: Session = Depends(get_db)):
    _ensure_default_plan_seeded(db)

    email = str(payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "").strip()
    org_slug = str(payload.get("org_slug") or "").strip()
    org_name = str(payload.get("org_name") or "").strip() or None

    if not email or not password or not org_slug:
        raise HTTPException(status_code=400, detail="email, password, org_slug required")

    try:
        auth_service.register_local_user(
            db,
            org_slug=org_slug,
            org_name=org_name,
            email=email,
            password=password,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    user = db.scalar(select(AppUser).where(AppUser.email == email))
    if org is None or user is None:
        raise HTTPException(status_code=500, detail="register_inconsistent_state")

    # guarantee membership (idempotent)
    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == int(org.id),
            OrgMembership.user_id == int(user.id),
        )
    )
    if mem is None:
        auth_service.ensure_membership(db, org_id=int(org.id), user_id=int(user.id), role="owner")
        mem = db.scalar(
            select(OrgMembership).where(
                OrgMembership.org_id == int(org.id),
                OrgMembership.user_id == int(user.id),
            )
        )

    sub = db.scalar(select(OrgSubscription).where(OrgSubscription.org_id == int(org.id)))
    if sub is None:
        db.add(
            OrgSubscription(
                org_id=int(org.id),
                plan_code=getattr(settings, "default_plan_code", None) or "free",
                status="active",
                created_at=_now(),
            )
        )
        db.commit()

    exp = int((_now() + timedelta(minutes=int(getattr(settings, "jwt_exp_minutes", 60)))).timestamp())
    token = _jwt_sign({"sub": str(user.id), "org": str(org.slug), "exp": exp})
    _set_auth_cookie(response, request, token)

    return {"ok": True, "user_id": int(user.id), "org_slug": str(org.slug), "role": str(mem.role if mem else "owner")}


@router.post("/login")
def login(payload: dict[str, Any], request: Request, response: Response, db: Session = Depends(get_db)):
    email = str(payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "").strip()
    org_slug = str(payload.get("org_slug") or "").strip()

    if not email or not password or not org_slug:
        raise HTTPException(status_code=400, detail="email, password, org_slug required")

    try:
        out = auth_service.login_local_user(db, org_slug=org_slug, email=email, password=password)
    except ValueError as e:
        msg = str(e)
        if msg == "org_not_found":
            raise HTTPException(status_code=401, detail="Unknown org")
        if msg == "invalid_credentials":
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if msg == "not_a_member":
            raise HTTPException(status_code=403, detail="Not a member of org")
        raise HTTPException(status_code=401, detail=msg)

    exp = int((_now() + timedelta(minutes=int(getattr(settings, "jwt_exp_minutes", 60)))).timestamp())
    token = _jwt_sign({"sub": str(out["user_id"]), "org": str(out["org_slug"]), "exp": exp})
    _set_auth_cookie(response, request, token)

    return {"ok": True, "user_id": int(out["user_id"]), "org_slug": str(out["org_slug"]), "role": str(out["role"])}


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(_cookie_name(), path="/")
    return {"ok": True}


@router.get("/me", response_model=PrincipalOut)
def me(request: Request, db: Session = Depends(get_db)):
    user_id = _require_user_id_from_cookie(request)

    header_slug = (request.headers.get("X-Org-Slug") or "").strip() or None
    cookie_slug = _org_from_cookie(request)

    if not header_slug and not cookie_slug:
        raise HTTPException(status_code=401, detail="Missing org context. Login again or select org.")

    if header_slug:
        try:
            return _principal(db, user_id=user_id, org_slug=header_slug)
        except HTTPException as e:
            if e.status_code in (401, 403) and cookie_slug and cookie_slug != header_slug:
                return _principal(db, user_id=user_id, org_slug=cookie_slug)
            raise

    return _principal(db, user_id=user_id, org_slug=str(cookie_slug))


@router.get("/orgs")
def my_orgs(request: Request, db: Session = Depends(get_db)):
    user_id = _require_user_id_from_cookie(request)

    rows = db.execute(
        select(Organization.slug, Organization.name, OrgMembership.role)
        .select_from(OrgMembership)
        .join(Organization, Organization.id == OrgMembership.org_id)
        .where(OrgMembership.user_id == int(user_id))
        .order_by(Organization.slug.asc())
    ).all()

    return [{"org_slug": r[0], "org_name": r[1], "role": r[2]} for r in rows]


@router.post("/select-org")
def select_org(org_slug: str, request: Request, response: Response, db: Session = Depends(get_db)):
    user_id = _require_user_id_from_cookie(request)
    slug = (org_slug or "").strip()
    if not slug:
        raise HTTPException(status_code=400, detail="org_slug is required")

    org = db.scalar(select(Organization).where(Organization.slug == slug))
    if org is None:
        raise HTTPException(status_code=404, detail="Org not found")

    mem = db.scalar(
        select(OrgMembership).where(
            OrgMembership.org_id == int(org.id),
            OrgMembership.user_id == int(user_id),
        )
    )
    if mem is None:
        raise HTTPException(status_code=403, detail="Not a member of that org")

    exp = int((_now() + timedelta(minutes=int(getattr(settings, "jwt_exp_minutes", 60)))).timestamp())
    token = _jwt_sign({"sub": str(user_id), "org": str(org.slug), "exp": exp})
    _set_auth_cookie(response, request, token)

    return {"ok": True, "org_slug": str(org.slug), "role": str(mem.role)}


@router.get("/debug-cookie")
def debug_cookie(request: Request):
    """
    DEV ONLY endpoint.
    It answers: does the backend actually receive the auth cookie?
    """
    name = _cookie_name()
    raw = request.cookies.get(name)
    return {
        "cookie_name": name,
        "has_cookie": bool(raw),
        "cookie_len": len(raw) if raw else 0,
        "host": request.headers.get("host"),
        "x_forwarded_proto": request.headers.get("x-forwarded-proto"),
        "scheme": request.url.scheme,
        "is_localhost": _is_localhost(request),
    }
