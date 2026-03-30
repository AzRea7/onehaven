from types import SimpleNamespace

from app.domain import rent_learning


class DummyDB:
    def __init__(self, prop):
        self._prop = prop

    def get(self, model, property_id):
        return self._prop



def test_recompute_rent_fields_uses_comp_reference_and_caps_to_fmr(monkeypatch):
    prop = SimpleNamespace(
        id=16,
        zip="48201",
        bedrooms=3,
        units=1,
        property_type="single_family",
    )
    ra = SimpleNamespace(
        market_rent_estimate=1750.0,
        rent_reasonableness_comp=1490.0,
        section8_fmr=1600.0,
        approved_rent_ceiling=None,
    )
    db = DummyDB(prop)

    monkeypatch.setattr(rent_learning, "get_or_create_rent_assumption", lambda db, property_id: ra)
    monkeypatch.setattr(rent_learning, "get_calibration_multiplier", lambda *args, **kwargs: 1.0)

    out = rent_learning.recompute_rent_fields(
        db,
        property_id=16,
        strategy="section8",
        payment_standard_pct=1.0,
    )

    assert out["market_rent_estimate"] == 1750.0
    assert out["rent_reasonableness_comp"] == 1490.0
    assert out["market_reference_rent"] == 1490.0
    assert out["approved_rent_ceiling"] == 1600.0
    assert out["rent_used"] == 1490.0
    assert out["rent_cap_reason"] == "rentcast_under_fmr"



def test_recompute_rent_fields_multifamily_caps_per_unit(monkeypatch):
    prop = SimpleNamespace(
        id=17,
        zip="48201",
        bedrooms=4,
        units=2,
        property_type="multi_family",
    )
    ra = SimpleNamespace(
        market_rent_estimate=3300.0,
        rent_reasonableness_comp=3000.0,
        section8_fmr=2800.0,
        approved_rent_ceiling=None,
    )
    db = DummyDB(prop)

    monkeypatch.setattr(rent_learning, "get_or_create_rent_assumption", lambda db, property_id: ra)
    monkeypatch.setattr(rent_learning, "get_calibration_multiplier", lambda *args, **kwargs: 1.0)

    out = rent_learning.recompute_rent_fields(
        db,
        property_id=17,
        strategy="section8",
        payment_standard_pct=1.0,
    )

    assert out["market_reference_rent"] == 3000.0
    assert out["approved_rent_ceiling"] == 2800.0
    assert out["rent_used"] == 2800.0
    assert out["rent_cap_reason"] == "multifamily_fmr_times_units"
