from __future__ import annotations

from types import SimpleNamespace

from app.routers import properties as properties_router


async def _fake_enrich_property_geo(*args, **kwargs):
    return {
        "ok": True,
        "property_id": 123,
        "lat": 42.3314,
        "lng": -83.0458,
        "county": "Wayne",
        "is_red_zone": True,
        "geocoded": True,
        "reverse_geocoded": True,
        "warnings": [],
    }


def test_maybe_geo_enrich_property_returns_service_payload(monkeypatch):
    monkeypatch.setattr(
        properties_router,
        "enrich_property_geo",
        _fake_enrich_property_geo,
        raising=True,
    )

    out = properties_router._maybe_geo_enrich_property(
        db=SimpleNamespace(),
        org_id=1,
        property_id=123,
        force=True,
    )

    assert out["ok"] is True
    assert out["property_id"] == 123
    assert out["county"] == "Wayne"
    assert out["is_red_zone"] is True
    assert out["geocoded"] is True