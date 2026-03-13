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