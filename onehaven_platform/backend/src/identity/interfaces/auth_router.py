# backend/app/routers/auth.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from onehaven_platform.backend.src.auth import _jwt_sign, _jwt_verify, get_principal_core, jwt_debug_fingerprint
from onehaven_platform.backend.src.config import settings
from onehaven_platform.backend.src.db import get_db
from onehaven_platform.backend.src.models import AppUser, Organization, OrgMembership, Plan
try:
    from onehaven_platform.backend.src.models import Subscription as OrgSubscription  # type: ignore
except Exception:
    from onehaven_platform.backend.src.models import OrgSubscription  # type: ignore
from onehaven_platform.backend.src.schemas import PrincipalOut
from onehaven_platform.backend.src.services import auth_service, plan_service

router = APIRouter(prefix="/auth", tags=["auth"])


def _now() -> datetime:
    return datetime.utcnow()


def _ensure_default_plan_seeded(db: Session) -> None:
    plan_service.ensure_default_plans(db)


def _cookie_name() -> str:
    return str(getattr(settings, "jwt_cookie_name", "onehaven_jwt") or "onehaven_jwt")


def _set_auth_cookie(response: Response, request: Request, token: str) -> None:
    name = _cookie_name()
    max_age = int(getattr(settings, "jwt_exp_minutes", 60)) * 60

    host = (request.headers.get("host") or "").lower()
    is_local = host.startswith("localhost") or host.startswith("127.0.0.1")

    secure_setting = bool(getattr(settings, "jwt_cookie_secure", False))
    samesite_setting = str(getattr(settings, "jwt_cookie_samesite", "lax") or "lax").lower()

    if is_local:
        secure = False
        samesite = "lax"
    else:
        secure = secure_setting
        if samesite_setting == "none" and not secure:
            secure = True
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


def _issue_token_for_org(*, user_id: int, org_slug: str) -> str:
    exp = int((_now() + timedelta(minutes=int(getattr(settings, "jwt_exp_minutes", 60)))).timestamp())
    return _jwt_sign(
        {
            "sub": str(user_id),
            "org": str(org_slug),
            "exp": exp,
            "iat": int(_now().timestamp()),
        }
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
        kwargs: dict[str, Any] = {
            "org_id": int(org.id),
            "plan_code": getattr(settings, "default_plan_code", None) or "free",
            "status": "active",
        }
        if hasattr(OrgSubscription, "created_at"):
            kwargs["created_at"] = _now()
        db.add(OrgSubscription(**kwargs))
        db.commit()

    token = _issue_token_for_org(user_id=int(user.id), org_slug=str(org.slug))
    _set_auth_cookie(response, request, token)

    return {
        "ok": True,
        "user_id": int(user.id),
        "org_slug": str(org.slug),
        "role": str(mem.role if mem else "owner"),
        "plan_code": plan_service.get_plan_code(db, org_id=int(org.id)),
    }


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

    token = _issue_token_for_org(user_id=int(out["user_id"]), org_slug=str(out["org_slug"]))
    _set_auth_cookie(response, request, token)

    org = db.scalar(select(Organization).where(Organization.slug == str(out["org_slug"])))
    plan_code = plan_service.get_plan_code(db, org_id=int(org.id)) if org else "free"

    return {
        "ok": True,
        "user_id": int(out["user_id"]),
        "org_slug": str(out["org_slug"]),
        "role": str(out["role"]),
        "plan_code": plan_code,
    }


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(_cookie_name(), path="/")
    return {"ok": True}


@router.get("/me", response_model=PrincipalOut)
def me(request: Request, db: Session = Depends(get_db)):
    p = get_principal_core(
        request=request,
        db=db,
        x_org_slug=request.headers.get("X-Org-Slug"),
        x_api_key=request.headers.get("X-API-Key"),
        authorization=request.headers.get("Authorization"),
    )
    return PrincipalOut(
        org_id=int(p.org_id),
        org_slug=str(p.org_slug),
        user_id=int(p.user_id),
        email=str(p.email),
        role=str(p.role),
    )


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
@router.post("/select_org")
def select_org(
    request: Request,
    response: Response,
    db: Session = Depends(get_db),
    org_slug: str = Query(default=""),
):
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

    token = _issue_token_for_org(user_id=int(user_id), org_slug=str(org.slug))
    _set_auth_cookie(response, request, token)

    return {
        "ok": True,
        "org_slug": str(org.slug),
        "role": str(mem.role),
        "plan_code": plan_service.get_plan_code(db, org_id=int(org.id)),
    }


@router.get("/debug-auth")
def debug_auth(request: Request):
    cookie_name = _cookie_name()
    token = request.cookies.get(cookie_name)
    x_org = request.headers.get("X-Org-Slug")
    qp_org = request.query_params.get("org_slug")

    out: dict[str, Any] = {
        "auth_mode": str(getattr(settings, "auth_mode", "dev")),
        "allow_local_auth_bypass": bool(getattr(settings, "allow_local_auth_bypass", False)),
        "enable_api_keys": bool(getattr(settings, "enable_api_keys", False)),
        "jwt_cookie_name": cookie_name,
        "jwt_fp": jwt_debug_fingerprint(),
        "x_org_slug": x_org,
        "qp_org_slug": qp_org,
        "has_cookie": bool(token),
        "cookie_len": len(token) if token else 0,
    }

    if token:
        try:
            claims = _jwt_verify(token)
            out["jwt_verified"] = True
            out["claims"] = claims
        except Exception as e:
            out["jwt_verified"] = False
            out["jwt_error"] = str(e)

    return out