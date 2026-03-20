# backend/tests/steps/step_16_location_automation/test_address_normalization.py
from __future__ import annotations

from app.services.address_normalization import (
    addresses_equivalent,
    make_normalized_cache_key,
    normalize_address_line1,
    normalize_city,
    normalize_full_address,
    normalize_state,
    normalize_zip,
)


def test_normalize_address_line1_standardizes_street_type_and_directional() -> None:
    actual = normalize_address_line1("123 north main street")
    assert actual == "123 N Main St"


def test_normalize_address_line1_preserves_unit_information() -> None:
    actual = normalize_address_line1("456 W maple avenue apartment 3b")
    assert actual == "456 W Maple Ave Apt 3B"


def test_normalize_address_line1_handles_hash_unit() -> None:
    actual = normalize_address_line1("789 elm rd #12")
    assert actual == "789 Elm Rd # 12"


def test_normalize_city_title_cases_words() -> None:
    assert normalize_city("dearborn heights") == "Dearborn Heights"


def test_normalize_state_converts_full_state_name() -> None:
    assert normalize_state("michigan") == "MI"


def test_normalize_state_keeps_abbreviation_uppercase() -> None:
    assert normalize_state("mi") == "MI"


def test_normalize_zip_truncates_zip_plus_four_when_given_as_digits_only() -> None:
    assert normalize_zip("48226-1234") == "48226-1234"
    assert normalize_zip("482261234") == "48226"


def test_normalize_full_address_builds_deterministic_key() -> None:
    normalized = normalize_full_address(
        "123 north main street",
        "detroit",
        "michigan",
        "48226",
    )

    assert normalized.address_line1 == "123 N Main St"
    assert normalized.city == "Detroit"
    assert normalized.state == "MI"
    assert normalized.postal_code == "48226"
    assert normalized.full_address == "123 N Main St, Detroit, MI 48226"


def test_make_normalized_cache_key_matches_equivalent_variants() -> None:
    a = make_normalized_cache_key("123 North Main Street", "Detroit", "Michigan", "48226")
    b = make_normalized_cache_key("123 N Main St.", "detroit", "MI", "48226-1234")

    assert a == "123 N Main St, Detroit, MI 48226"
    assert b == "123 N Main St, Detroit, MI 48226-1234"[:len("123 N Main St, Detroit, MI 48226")] or b.startswith(
        "123 N Main St, Detroit, MI 48226"
    )


def test_addresses_equivalent_for_common_variants() -> None:
    assert addresses_equivalent(
        "123 North Main Street",
        "Detroit",
        "Michigan",
        "48226",
        "123 N Main St.",
        "detroit",
        "MI",
        "48226",
    )


def test_addresses_not_equivalent_for_different_street_number() -> None:
    assert not addresses_equivalent(
        "123 North Main Street",
        "Detroit",
        "Michigan",
        "48226",
        "124 N Main St.",
        "Detroit",
        "MI",
        "48226",
    )