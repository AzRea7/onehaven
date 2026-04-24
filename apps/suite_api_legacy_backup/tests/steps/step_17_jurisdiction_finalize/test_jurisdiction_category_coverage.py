from __future__ import annotations

from app.domain import jurisdiction_categories as cats


def test_expected_rule_universe_exposes_full_rule_family_inventory():
    universe = cats.expected_rule_universe_for_scope(state="MI", county="macomb", city="warren", include_section8=True)

    assert "registration" in universe.required_categories
    assert "inspection" in universe.required_categories
    assert "occupancy" in universe.required_categories
    assert "source_of_income" in universe.optional_categories or "source_of_income" in universe.required_categories
    assert "registration" in (universe.rule_family_inventory or {})
    assert (universe.rule_family_inventory or {})["registration"]["authority_expectation"] == "authoritative_official"
    assert "documents" in (universe.operational_heuristic_categories or [])
    assert "registration" in (universe.legally_binding_categories or [])


def test_normalize_rule_category_maps_known_rule_keys():
    assert cats.normalize_rule_category("rental_registration_required") == "registration"
    assert cats.normalize_rule_category("inspection_program_exists") == "inspection"
    assert cats.normalize_rule_category("certificate_required_before_occupancy") == "occupancy"
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
            "registration",
            "inspection",
            "occupancy",
            "source_of_income",
        ],
    )

    assert coverage["registration"] == "verified"
    assert coverage["inspection"] == "verified"
    assert coverage["occupancy"] == "conditional"
    assert coverage["source_of_income"] == "missing"
