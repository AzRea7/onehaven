from app.services.ingestion_dedupe_service import build_property_fingerprint


def test_property_fingerprint_is_stable():
    a = build_property_fingerprint(
        address="123 Main St",
        city="Detroit",
        state="MI",
        zip_code="48201",
    )
    b = build_property_fingerprint(
        address="123 Main St",
        city="Detroit",
        state="MI",
        zip_code="48201",
    )
    assert a == b