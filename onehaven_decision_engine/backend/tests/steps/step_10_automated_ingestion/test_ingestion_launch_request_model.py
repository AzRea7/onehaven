from app.routers.ingestion import IngestionSyncLaunchRequest


def test_ingestion_launch_request_normalizes_zip_codes_and_buckets():
    req = IngestionSyncLaunchRequest(
        state="MI",
        county="wayne",
        city="Detroit",
        zip_code="48228",
        zip_codes="48224, 48219,48235",
        min_price=75000,
        max_price=150000,
        price_buckets=[[75000, 100000], [100001, 125000], [125001, 150000]],
        pages_per_shard=1,
        limit=15,
    )

    payload = req.runtime_config()

    assert payload["state"] == "MI"
    assert payload["county"] == "wayne"
    assert payload["city"] == "Detroit"
    assert payload["zip_codes"] == ["48228", "48224", "48219", "48235"]
    assert payload["price_buckets"] == [
        [75000.0, 100000.0],
        [100001.0, 125000.0],
        [125001.0, 150000.0],
    ]
    assert payload["pages_per_shard"] == 1
    assert payload["limit"] == 15


def test_ingestion_launch_request_blank_county_becomes_none():
    req = IngestionSyncLaunchRequest(
        state="MI",
        county="   ",
        city="Detroit",
        limit=10,
    )

    payload = req.runtime_config()

    assert payload["state"] == "MI"
    assert "county" not in payload
    assert payload["city"] == "Detroit"