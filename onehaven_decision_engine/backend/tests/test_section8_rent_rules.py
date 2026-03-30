from types import SimpleNamespace

from app.domain.underwriting import compute_effective_rent_used, select_market_rent_reference


def test_select_market_rent_reference_prefers_conservative_comp_value():
    assert select_market_rent_reference(
        market_rent_estimate=1650.0,
        rent_reasonableness_comp=1485.0,
    ) == 1485.0



def test_compute_effective_rent_used_caps_single_family_at_fmr():
    rent_used, reason = compute_effective_rent_used(
        property_type="single_family",
        bedrooms=3,
        units=1,
        rentcast_rent=1750.0,
        fmr_rent=1600.0,
    )

    assert rent_used == 1600.0
    assert reason == "fmr_cap_applied"



def test_compute_effective_rent_used_multifamily_caps_per_unit_then_multiplies():
    rent_used, reason = compute_effective_rent_used(
        property_type="multi_family",
        bedrooms=4,
        units=2,
        rentcast_rent=3200.0,
        fmr_rent=3000.0,
        unit_rentcast_rent=1600.0,
        unit_fmr_rent=1500.0,
    )

    assert rent_used == 3000.0
    assert reason == "multifamily_fmr_times_units"
