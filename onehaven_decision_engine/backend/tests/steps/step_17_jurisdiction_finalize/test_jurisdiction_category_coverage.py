from __future__ import annotations

from app.domain import jurisdiction_categories as cats


def test_required_categories_for_city_market_contains_core_items():
    required = set(cats.required_categories_for_market(state="MI", county="macomb", city="warren"))

    assert "rental_registration" in required
    assert "inspection" in required
    assert "certificate_of_occupancy" in required
    assert "source_of_income" in required


def test_normalize_rule_category_maps_known_rule_keys():
    assert cats.normalize_rule_category("rental_registration_required") == "rental_registration"
    assert cats.normalize_rule_category("inspection_program_exists") == "inspection"
    assert cats.normalize_rule_category("certificate_required_before_occupancy") == "certificate_of_occupancy"
    assert cats.normalize_rule_category("unknown_rule_key") == "uncategorized"


def test_category_coverage_from_rule_keys_builds_expected_status():
    coverage = cats.category_coverage_from_rule_keys(
        verified_rule_keys={
            "rental_registration_required",
            "inspection_program_exists",
        },
        conditional_rule_keys={
            "certificate_required_before_occupancy",
        },
        required_categories=[
            "rental_registration",
            "inspection",
            "certificate_of_occupancy",
            "source_of_income",
        ],
    )

    assert coverage["rental_registration"] == "verified"
    assert coverage["inspection"] == "verified"
    assert coverage["certificate_of_occupancy"] == "conditional"
    assert coverage["source_of_income"] == "missing"