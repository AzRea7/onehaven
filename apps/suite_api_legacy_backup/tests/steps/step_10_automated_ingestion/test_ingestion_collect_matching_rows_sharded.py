from types import SimpleNamespace

from products.acquire.backend.src.services.ingestion_run_execute import _collect_matching_rows


class FakeSource:
    provider = "rentcast"
    config_json = {}
    credentials_json = {}
    cursor_json = {"page": 1}


def test_collect_matching_rows_uses_targeted_runtime_and_stops_when_limit_reached(monkeypatch):
    source = FakeSource()

    scanned_pages = []

    def fake_load_rows_page(
        source,
        *,
        trigger_type,
        runtime_config,
        cursor,
        provider_fetch_limit,
    ):
        scanned_pages.append(
            {
                "page": cursor.get("page"),
                "provider_fetch_limit": provider_fetch_limit,
                "zip_codes": runtime_config.get("zip_codes"),
                "price_buckets": runtime_config.get("price_buckets"),
            }
        )

        page = int(cursor.get("page") or 1)

        if page == 1:
            rows = [
                {
                    "external_record_id": "a1",
                    "address": "111 Main St",
                    "city": "Detroit",
                    "county": "Wayne",
                    "state": "MI",
                    "zip": "48228",
                    "bedrooms": 3,
                    "bathrooms": 1,
                    "property_type": "single_family",
                    "asking_price": 90000,
                },
                {
                    "external_record_id": "a2",
                    "address": "222 Main St",
                    "city": "Detroit",
                    "county": "Wayne",
                    "state": "MI",
                    "zip": "48224",
                    "bedrooms": 3,
                    "bathrooms": 1,
                    "property_type": "single_family",
                    "asking_price": 110000,
                },
            ]
            return rows, {"page": 2}, 2

        return [], {"page": page}, 0

    monkeypatch.setattr(
        "products.acquire.backend.src.services.ingestion_run_execute._load_rows_page",
        fake_load_rows_page,
    )

    runtime_config = {
        "state": "MI",
        "county": "wayne",
        "city": "Detroit",
        "zip_codes": ["48228", "48224", "48219"],
        "min_price": 75000,
        "max_price": 150000,
        "min_bedrooms": 2,
        "min_bathrooms": 1,
        "property_type": "single_family",
        "price_buckets": [
            [75000, 100000],
            [100001, 125000],
            [125001, 150000],
        ],
        "pages_per_shard": 1,
        "limit": 2,
    }

    rows, next_cursor, stats = _collect_matching_rows(
        source,
        trigger_type="manual",
        runtime_config=runtime_config,
    )

    assert len(rows) == 2
    assert stats["records_seen"] == 2
    assert stats["invalid_rows"] == 0
    assert stats["filtered_out"] == 0
    assert stats["duplicates_skipped"] == 0
    assert stats["provider_pages_scanned"] >= 1

    assert scanned_pages[0]["zip_codes"] == ["48228", "48224", "48219"]
    assert scanned_pages[0]["price_buckets"] == [
        [75000, 100000],
        [100001, 125000],
        [125001, 150000],
    ]