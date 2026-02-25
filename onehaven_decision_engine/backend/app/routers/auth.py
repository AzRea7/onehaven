# backend/app/routers/auth.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..auth import get_principal, _hash_password, _verify_password, _jwt_sign  # noqa
from ..config import settings
from ..db import get_db
from ..models import AppUser, Organization, OrgMembership, Plan, Subscription
from ..schemas import PrincipalOut


router = APIRouter(prefix="/auth", tags=["auth"])


def _now() -> datetime:
    return datetime.utcnow()


def _ensure_default_plan_seeded(db: Session) -> None:
    # Minimal seed: free + starter
    def upsert(code: str, name: str, limits: dict[str, Any]) -> None:
        row = db.scalar(select(Plan).where(Plan.code == code))
        if row:
            return
        db.add(Plan(code=code, name=name, limits_json=str(limits).replace("'", '"'), is_active=True, created_at=_now()))

    upsert("free", "Free", {"max_properties": 3, "agent_runs_per_day": 20, "external_calls_per_day": 50, "max_concurrent_runs": 2})
    upsert("starter", "Starter", {"max_properties": 25, "agent_runs_per_day": 200, "external_calls_per_day": 500, "max_concurrent_runs": 5})
    db.commit()


@router.post("/register")
def register(
    payload: dict[str, Any],
    response: Response,
    db: Session = Depends(get_db),
):
    """
    Create user + (optional) create org + owner membership.
    payload: { email, password, org_slug?, org_name? }
    """
    _ensure_default_plan_seeded(db)

    email = str(payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "").strip()
    org_slug = str(payload.get("org_slug") or "").strip() or None
    org_name = str(payload.get("org_name") or "").strip() or None

    if not email or not password:
        raise HTTPException(status_code=400, detail="email and password are required")

    existing = db.scalar(select(AppUser).where(AppUser.email == email))
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    u = AppUser(
        email=email,
        display_name=email.split("@")[0],
        password_hash=_hash_password(password),
        email_verified=True if settings.dev_auto_verify_email else False,
        created_at=_now(),
    )
    db.add(u)
    db.commit()
    db.refresh(u)

    created_org = None
    if org_slug:
        org = db.scalar(select(Organization).where(Organization.slug == org_slug))
        if org:
            raise HTTPException(status_code=400, detail="org_slug already exists")

        org = Organization(slug=org_slug, name=org_name or org_slug, created_at=_now())
        db.add(org)
        db.commit()
        db.refresh(org)

        mem = OrgMembership(org_id=int(org.id), user_id=int(u.id), role="owner", created_at=_now())
        db.add(mem)

        # create subscription (free by default)
        sub = Subscription(org_id=int(org.id), plan_code=settings.default_plan_code or "free", status="active", created_at=_now())
        db.add(sub)

        db.commit()
        created_org = {"org_id": int(org.id), "org_slug": str(org.slug)}

    # If org was created, log them in for that org.
    if created_org:
        exp = int((_now() + timedelta(minutes=int(settings.jwt_exp_minutes))).timestamp())
        token = _jwt_sign({"sub": str(u.id), "exp": exp})
        response.set_cookie(
            settings.jwt_cookie_name,
            token,
            httponly=True,
            secure=bool(settings.jwt_cookie_secure),
            samesite=str(settings.jwt_cookie_samesite),
            max_age=int(settings.jwt_exp_minutes) * 60,
            path="/",
        )

    return {"ok": True, "user_id": int(u.id), "created_org": created_org}


@router.post("/login")
def login(
    payload: dict[str, Any],
    response: Response,
    db: Session = Depends(get_db),
):
    """
    payload: { email, password, org_slug }
    - verifies membership in org
    - sets JWT cookie
    """
    email = str(payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "").strip()
    org_slug = str(payload.get("org_slug") or "").strip()

    if not email or not password or not org_slug:
        raise HTTPException(status_code=400, detail="email, password, org_slug required")

    user = db.scalar(select(AppUser).where(AppUser.email == email))
    if user is None or not user.password_hash:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if not _verify_password(password, str(user.password_hash)):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    if org is None:
        raise HTTPException(status_code=401, detail="Unknown org")

    mem = db.scalar(select(OrgMembership).where(OrgMembership.org_id == int(org.id), OrgMembership.user_id == int(user.id)))
    if mem is None:
        raise HTTPException(status_code=403, detail="Not a member of org")

    user.last_login_at = _now()
    db.add(user)
    db.commit()

    exp = int((_now() + timedelta(minutes=int(settings.jwt_exp_minutes))).timestamp())
    token = _jwt_sign({"sub": str(user.id), "exp": exp})
    response.set_cookie(
        settings.jwt_cookie_name,
        token,
        httponly=True,
        secure=bool(settings.jwt_cookie_secure),
        samesite=str(settings.jwt_cookie_samesite),
        max_age=int(settings.jwt_exp_minutes) * 60,
        path="/",
    )

    return {"ok": True, "user_id": int(user.id), "org_slug": str(org.slug), "role": str(mem.role)}


@router.post("/logout")
def logout(response: Response):
    response.delete_cookie(settings.jwt_cookie_name, path="/")
    return {"ok": True}


@router.get("/me", response_model=PrincipalOut)
def me(db: Session = Depends(get_db), p=Depends(get_principal)):
    return PrincipalOut(org_id=p.org_id, org_slug=p.org_slug, user_id=p.user_id, email=p.email, role=p.role)


@router.get("/orgs")
def my_orgs(db: Session = Depends(get_db), p=Depends(get_principal)):
    # list orgs for current user (ignoring current org context)
    rows = db.execute(
        select(Organization.slug, Organization.name, OrgMembership.role)
        .select_from(OrgMembership)
        .join(Organization, Organization.id == OrgMembership.org_id)
        .where(OrgMembership.user_id == int(p.user_id))
        .order_by(Organization.slug.asc())
    ).all()

    return [{"org_slug": r[0], "org_name": r[1], "role": r[2]} for r in rows]


@router.post("/select-org")
def select_org(org_slug: str, request: Request, db: Session = Depends(get_db), p=Depends(get_principal)):
    """
    No server-side session state is needed because you pass X-Org-Slug on each request.
    This exists for UX: validate membership before frontend switches org.
    """
    org = db.scalar(select(Organization).where(Organization.slug == org_slug))
    if org is None:
        raise HTTPException(status_code=404, detail="Org not found")

    mem = db.scalar(select(OrgMembership).where(OrgMembership.org_id == int(org.id), OrgMembership.user_id == int(p.user_id)))
    if mem is None:
        raise HTTPException(status_code=403, detail="Not a member of that org")

    return {"ok": True, "org_slug": str(org.slug), "role": str(mem.role)}
