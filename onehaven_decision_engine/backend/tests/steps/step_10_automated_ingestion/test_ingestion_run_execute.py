from app.services.ingestion_dedupe_service import build_property_fingerprint


def test_property_fingerprint_uses_normalized_address():
    a = build_property_fingerprint(
        address="123 Main Street",
        city="Detroit",
        state="MI",
        zip_code="48201",
    )
    b = build_property_fingerprint(
        address="123 main st.",
        city="Detroit",
        state="MI",
        zip_code="48201-1234",
    )
    assert a == b
