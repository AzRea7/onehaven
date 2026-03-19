from app.services.ingestion_enrichment_service import canonical_listing_payload
from app.services.ingestion_scheduler_service import list_default_daily_markets


def test_canonical_listing_payload_keeps_core_fields():
    row = {
        "listingId": "abc123",
        "formattedAddress": "123 Main St",
        "city": "Detroit",
        "state": "MI",
        "zipCode": "48201",
        "price": 120000,
        "rentEstimate": 1450,
    }
    out = canonical_listing_payload(row)
    assert out["external_record_id"] == "abc123"
    assert out["address"] == "123 Main St"
    assert out["asking_price"] == 120000
    assert out["market_rent_estimate"] == 1450


def test_default_daily_markets_are_seeded_for_southeast_michigan():
    markets = list_default_daily_markets()
    city_names = {m["city"].lower() for m in markets}
    assert "detroit" in city_names
    assert "warren" in city_names
    assert "pontiac" in city_names
