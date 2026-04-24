from __future__ import annotations

from types import SimpleNamespace

from app.services import jurisdiction_profile_service as svc


class DummyDB:
    pass


def _row(
    *,
    id: int,
    org_id,
    state: str = "MI",
    county: str | None = None,
    city: str | None = None,
    friction_multiplier: float = 1.0,
    pha_name: str | None = None,
    policy_json: str = "{}",
    notes: str | None = None,
    completeness_status: str | None = None,
    stale_status: str | None = None,
    required_categories_json: str | None = None,
    category_coverage_json: str | None = None,
):
    return SimpleNamespace(
        id=id,
        org_id=org_id,
        state=state,
        county=county,
        city=city,
        friction_multiplier=friction_multiplier,
        pha_name=pha_name,
        policy_json=policy_json,
        notes=notes,
        completeness_status=completeness_status,
        stale_status=stale_status,
        required_categories_json=required_categories_json,
        category_coverage_json=category_coverage_json,
    )


def test_resolve_profile_city_beats_county_beats_state(monkeypatch):
    rows = [
        _row(id=1, org_id=None, state="MI"),
        _row(id=2, org_id=None, state="MI", county="macomb"),
        _row(id=3, org_id=None, state="MI", county="macomb", city="warren"),
    ]

    monkeypatch.setattr(svc, "list_profiles", lambda db, org_id, include_global, state: rows)

    out = svc.resolve_profile(
        DummyDB(),
        org_id=99,
        city="Warren",
        county="Macomb",
        state="MI",
    )

    assert out["matched"] is True
    assert out["match_level"] == "city"
    assert out["profile_id"] == 3


def test_resolve_profile_org_override_wins_on_same_specificity(monkeypatch):
    rows = [
        _row(id=10, org_id=None, state="MI", county="macomb", city="warren", friction_multiplier=1.1),
        _row(id=11, org_id=42, state="MI", county="macomb", city="warren", friction_multiplier=1.5),
    ]

    monkeypatch.setattr(svc, "list_profiles", lambda db, org_id, include_global, state: rows)

    out = svc.resolve_profile(
        DummyDB(),
        org_id=42,
        city="Warren",
        county="Macomb",
        state="MI",
    )

    assert out["scope"] == "org"
    assert out["profile_id"] == 11
    assert out["friction_multiplier"] == 1.5


def test_resolve_profile_exposes_completeness_and_stale_metadata(monkeypatch):
    rows = [
        _row(
            id=21,
            org_id=None,
            state="MI",
            county="wayne",
            city="detroit",
            friction_multiplier=1.25,
            completeness_status="complete",
            stale_status="fresh",
            required_categories_json='["rental_registration","inspection","certificate_of_occupancy"]',
            category_coverage_json='{"rental_registration":"verified","inspection":"verified","certificate_of_occupancy":"verified"}',
        )
    ]

    monkeypatch.setattr(svc, "list_profiles", lambda db, org_id, include_global, state: rows)

    out = svc.resolve_profile(
        DummyDB(),
        org_id=1,
        city="Detroit",
        county="Wayne",
        state="MI",
    )

    assert out["matched"] is True
    assert out["profile_id"] == 21
    assert out["friction_multiplier"] == 1.25
    assert out.get("completeness_status") == "complete"
    assert out.get("stale_status") == "fresh"

    required = out.get("required_categories") or []
    coverage = out.get("category_coverage") or {}

    assert "rental_registration" in required
    assert coverage.get("inspection") == "verified"