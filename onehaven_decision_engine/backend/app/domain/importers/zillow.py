from __future__ import annotations

from .base import NormalizedRow, required, optional_float, optional_int, optional_bool

def normalize_zillow(row: dict[str, str]) -> NormalizedRow:
    # Zillow CSV exports vary by product; aliases handle common naming.
    address = required(row, "Address", "Street address", "Street")
    city = required(row, "City")
    state = required(row, "State", "ST") or "MI"
    zip_code = required(row, "Zip", "ZIP")

    beds = optional_int(row, "Bedrooms", "Beds", "Bed") or 0
    baths = optional_float(row, "Bathrooms", "Baths", "Bath") or 1.0
    sqft = optional_int(row, "Living Area", "SqFt", "Square Feet")
    year = optional_int(row, "Year Built", "YearBuilt")

    has_garage = optional_bool(row, "Garage", "Has Garage", "has_garage")

    asking = optional_float(row, "Price", "List Price", "Asking Price") or 0.0
    est_price = optional_float(row, "Zestimate", "Estimated Purchase Price")
    rehab = optional_float(row, "Rehab", "Rehab Estimate", "Estimated Repairs") or 0.0

    market_rent = optional_float(row, "Rent Zestimate", "Market Rent", "Estimated Rent")
    fmr = optional_float(row, "FMR", "Section 8 FMR", "HUD FMR")
    approved_ceiling = optional_float(row, "Approved Rent Ceiling", "Rent Ceiling")
    rr_comp = optional_float(row, "Rent Reasonableness", "Rent Comp")

    inventory = optional_int(row, "Inventory", "Listings Count")
    starbucks = optional_int(row, "Starbucks Minutes", "Starbucks Min")

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
        property_type="single_family",
        asking_price=float(asking),
        estimated_purchase_price=est_price,
        rehab_estimate=float(rehab),
        source="zillow",
        market_rent_estimate=market_rent,
        section8_fmr=fmr,
        approved_rent_ceiling=approved_ceiling,
        rent_reasonableness_comp=rr_comp,
        inventory_count=inventory,
        starbucks_minutes=starbucks,
        raw=row,
    )
