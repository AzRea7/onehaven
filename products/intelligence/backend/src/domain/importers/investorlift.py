# backend/app/domain/importers/investorlift.py
from __future__ import annotations

from .base import NormalizedRow, required, optional_float, optional_int, optional_bool


def normalize_investorlift(row: dict[str, str]) -> NormalizedRow:
    """
    Normalize an InvestorLift export row into our canonical NormalizedRow.

    InvestorLift CSVs can vary a lot depending on the list/export template,
    so we accept a broad set of aliases. Missing critical fields raises ValueError.
    """

    # --- Property identity ---
    address = required(row, "Address", "Property Address", "Street", "Street Address")
    city = required(row, "City", "Property City")
    state = required(row, "State", "ST", "Property State") or "MI"
    zip_code = required(row, "Zip", "ZIP", "Postal Code", "Property Zip")

    # --- Beds / baths / misc ---
    beds = optional_int(row, "Bedrooms", "Beds", "Bed", "Num Bedrooms") or 0
    baths = optional_float(row, "Bathrooms", "Baths", "Bath", "Num Bathrooms") or 1.0
    sqft = optional_int(row, "SqFt", "Square Feet", "Living Area", "Building Sqft", "Building SQFT")
    year = optional_int(row, "Year Built", "YearBuilt", "Build Year")

    # Garages show up as yes/no, 1/0, or count
    has_garage = (
        optional_bool(row, "Has Garage", "Garage", "garage", "Garages")
        or (optional_int(row, "Garage Spaces", "GarageSpace", "Garage Count") or 0) > 0
    )

    # --- Deal numbers ---
    asking = optional_float(row, "Asking Price", "Price", "List Price", "ListPrice", "Asking") or 0.0
    est_price = optional_float(row, "Estimated Purchase Price", "Offer Price", "MAO", "Max Offer", "Target Price")
    rehab = optional_float(row, "Rehab", "Rehab Estimate", "Repairs", "Estimated Repairs", "Repair Cost") or 0.0

    # --- Rent assumptions (optional) ---
    market_rent = optional_float(
        row,
        "Market Rent",
        "Estimated Rent",
        "Rent",
        "Projected Rent",
        "Monthly Rent",
    )
    fmr = optional_float(row, "FMR", "Section 8 FMR", "HUD FMR", "FMR Ceiling")
    approved_ceiling = optional_float(row, "Approved Rent Ceiling", "Rent Ceiling", "Rent Cap")
    rr_comp = optional_float(row, "Rent Reasonableness", "Rent Comp", "RR Comp")

    inventory = optional_int(row, "Inventory", "Listings Count", "Inventory Count")
    starbucks = optional_int(row, "Starbucks Minutes", "Starbucks Min", "Starbucks (min)")

    # --- Validate minimum required fields ---
    if not address or not city or not zip_code or asking <= 0:
        raise ValueError("Missing required fields: address/city/zip/asking_price")

    return NormalizedRow(
        # property
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
        # deal
        asking_price=float(asking),
        estimated_purchase_price=est_price,
        rehab_estimate=float(rehab),
        source="investorlift",
        # rent assumptions
        market_rent_estimate=market_rent,
        section8_fmr=fmr,
        approved_rent_ceiling=approved_ceiling,
        rent_reasonableness_comp=rr_comp,
        inventory_count=inventory,
        starbucks_minutes=starbucks,
        # raw
        raw=row,
    )
