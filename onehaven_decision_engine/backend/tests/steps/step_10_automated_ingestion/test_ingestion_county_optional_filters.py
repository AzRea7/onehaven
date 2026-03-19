from app.services.ingestion_run_execute import _filter_reason


def test_filter_reason_does_not_reject_when_county_requested_but_payload_missing_county():
    payload = {
        "state": "MI",
        "county": None,
        "city": "Detroit",
        "asking_price": 100000,
        "bedrooms": 3,
        "bathrooms": 1,
        "property_type": "single_family",
    }

    runtime_config = {
        "state": "MI",
        "county": "wayne",
        "city": "Detroit",
        "min_price": 75000,
        "max_price": 150000,
        "min_bedrooms": 2,
        "min_bathrooms": 1,
        "property_type": "single_family",
        "limit": 10,
    }

    assert _filter_reason(payload, runtime_config) is None


def test_filter_reason_rejects_when_county_present_and_mismatched():
    payload = {
        "state": "MI",
        "county": "oakland",
        "city": "Detroit",
        "asking_price": 100000,
        "bedrooms": 3,
        "bathrooms": 1,
        "property_type": "single_family",
    }

    runtime_config = {
        "state": "MI",
        "county": "wayne",
        "city": "Detroit",
        "min_price": 75000,
        "max_price": 150000,
        "min_bedrooms": 2,
        "min_bathrooms": 1,
        "property_type": "single_family",
        "limit": 10,
    }

    assert _filter_reason(payload, runtime_config) == "county"