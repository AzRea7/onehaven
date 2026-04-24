# backend/tests/steps/step_16_location_automation/test_google_geocode_client.py
from __future__ import annotations

from app.clients.google_geocode import GoogleGeocodeClient


def test_google_parse_response_maps_fields_correctly() -> None:
    client = GoogleGeocodeClient(api_key="fake-key")

    payload = {
        "status": "OK",
        "results": [
            {
                "formatted_address": "123 N Main St, Detroit, MI 48226, USA",
                "geometry": {
                    "location": {"lat": 42.3314, "lng": -83.0458},
                    "location_type": "ROOFTOP",
                },
                "address_components": [
                    {"long_name": "123", "short_name": "123", "types": ["street_number"]},
                    {"long_name": "Main Street", "short_name": "Main St", "types": ["route"]},
                    {"long_name": "Detroit", "short_name": "Detroit", "types": ["locality", "political"]},
                    {"long_name": "Wayne County", "short_name": "Wayne County", "types": ["administrative_area_level_2", "political"]},
                    {"long_name": "Michigan", "short_name": "MI", "types": ["administrative_area_level_1", "political"]},
                    {"long_name": "48226", "short_name": "48226", "types": ["postal_code"]},
                ],
            }
        ],
    }

    result = client.parse_response(payload)

    assert result is not None
    assert result.source == "google"
    assert result.formatted_address == "123 N Main St, Detroit, MI 48226, USA"
    assert result.lat == 42.3314
    assert result.lng == -83.0458
    assert result.city == "Detroit"
    assert result.state == "MI"
    assert result.postal_code == "48226"
    assert result.county == "Wayne County"
    assert result.confidence == 0.99
    assert result.provider_status == "OK"


def test_google_parse_response_handles_zero_results() -> None:
    client = GoogleGeocodeClient(api_key="fake-key")

    payload = {
        "status": "ZERO_RESULTS",
        "results": [],
    }

    result = client.parse_response(payload)

    assert result is not None
    assert result.source == "google"
    assert result.lat is None
    assert result.lng is None
    assert result.confidence == 0.0
    assert result.provider_status == "ZERO_RESULTS"