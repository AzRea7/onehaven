# backend/app/domain/importers/zillow.py
from __future__ import annotations

from .base import NormalizedRow, required, optional_float, optional_int, optional_bool


def normalize_zillow(row: dict[str, str]) -> NormalizedRow:
    """
    Normalize a Zillow "properties for sale" export row into our canonical NormalizedRow.

    Supports Zillow headers like:
      - Street address, City, State, Zip
      - Property price (USD)
      - Living area
      - Bedrooms, Bathrooms
      - Property type
    """

    address = required(row, "Street address", "Address", "Street Address", "Street")
    city = required(row, "City")
    state = required(row, "State", "ST") or "MI"
    zip_code = required(row, "Zip", "ZIP", "Postal Code")

    # Zillow uses exact header "Property price (USD)"
    asking = (
        optional_float(row, "Property price (USD)", "Price", "List Price", "Asking Price")
        or 0.0
    )

    beds = optional_int(row, "Bedrooms", "Beds", "Bed") or 0
    baths = optional_float(row, "Bathrooms", "Baths", "Bath") or 1.0

    # Zillow uses exact header "Living area"
    sqft = optional_int(row, "Living area", "Living Area", "SqFt", "Square Feet")

    # Zillow export usually doesn’t include year built; keep optional
    year = optional_int(row, "Year Built", "YearBuilt")

    # Zillow export doesn’t reliably have garage; keep it false unless present
    has_garage = optional_bool(row, "Has Garage", "Garage", "has_garage")

    # Property type is present in your CSV as "Property type"
    raw_pt = (row.get("Property type") or "").strip().lower()
    if "single" in raw_pt:
        property_type = "single_family"
    elif "multi" in raw_pt or "duplex" in raw_pt or "triplex" in raw_pt or "quad" in raw_pt:
        property_type = "multi_family"
    elif "condo" in raw_pt:
        property_type = "condo"
    elif "town" in raw_pt:
        property_type = "townhouse"
    else:
        property_type = "single_family"

    # Optional rent fields: Zillow export doesn't include these by default
    market_rent = optional_float(row, "Rent Zestimate", "Rent zestimate", "Market Rent", "Estimated Rent")
    fmr = optional_float(row, "FMR", "Section 8 FMR", "HUD FMR")
    approved_ceiling = optional_float(row, "Approved Rent Ceiling", "Rent Ceiling", "Rent Cap")
    rr_comp = optional_float(row, "Rent Reasonableness", "Rent Comp", "RR Comp")

    inventory = optional_int(row, "Inventory", "Listings Count", "Inventory Count")
    starbucks = optional_int(row, "Starbucks Minutes", "Starbucks Min", "Starbucks (min)")

    if not address or not city or not zip_code or asking <= 0:
        raise ValueError("Missing required fields: address/city/zip/price")

    return NormalizedRow(
        address=address,
        city=city,
        state=state,
        zip=zip_code,
        bedrooms=int(beds),
        bathrooms=float(baths),
        square_feet=sqft,
        year_built=year,
        has_garage=bool(has_garage),
        property_type=property_type,
        asking_price=float(asking),
        estimated_purchase_price=None,
        rehab_estimate=0.0,
        source="zillow",
        market_rent_estimate=market_rent,
        section8_fmr=fmr,
        approved_rent_ceiling=approved_ceiling,
        rent_reasonableness_comp=rr_comp,
        inventory_count=inventory,
        starbucks_minutes=starbucks,
        raw=row,
    )
