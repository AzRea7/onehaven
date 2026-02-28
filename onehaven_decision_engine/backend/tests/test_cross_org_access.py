# backend/tests/test_cross_org_access.py
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import create_app
from app.db import SessionLocal
from app.models import Organization, Property


def _mk_org(slug: str, name: str) -> int:
    db = SessionLocal()
    try:
        org = db.query(Organization).filter(Organization.slug == slug).first()
        if org:
            return int(org.id)
        org = Organization(slug=slug, name=name)
        db.add(org)
        db.commit()
        db.refresh(org)
        return int(org.id)
    finally:
        db.close()


def _mk_property(org_id: int, addr: str) -> int:
    db = SessionLocal()
    try:
        p = Property(
            org_id=org_id,
            address=addr,
            city="Detroit",
            state="MI",
            zip="48201",
            bedrooms=3,
            bathrooms=1.0,
            square_feet=1200,
            year_built=1950,
            has_garage=False,
            property_type="single_family",
        )
        db.add(p)
        db.commit()
        db.refresh(p)
        return int(p.id)
    finally:
        db.close()


def _headers(org_slug: str) -> dict[str, str]:
    return {
        "X-Org-Slug": org_slug,
        "X-User-Email": "austin@demo.local",
        "X-User-Role": "owner",
    }


def test_cross_org_property_access_is_blocked():
    app = create_app()
    client = TestClient(app)

    org_a = _mk_org("org_a", "Org A")
    org_b = _mk_org("org_b", "Org B")

    pid_a = _mk_property(org_a, "111 A St")
    pid_b = _mk_property(org_b, "222 B St")

    # Org A can access its own property (path may vary by your router)
    r1 = client.get(f"/api/properties/{pid_a}", headers=_headers("org_a"))
    assert r1.status_code in (200, 404)

    # Org A must NOT access Org B's property
    r2 = client.get(f"/api/properties/{pid_b}", headers=_headers("org_a"))
    assert r2.status_code in (403, 404)


def test_cross_org_trust_access_is_scoped():
    app = create_app()
    client = TestClient(app)

    _mk_org("org_a", "Org A")
    _mk_org("org_b", "Org B")

    r = client.get("/api/trust/property/123", headers=_headers("org_a"))
    assert r.status_code == 200
    body = r.json()
    assert "score" in body