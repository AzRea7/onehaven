# backend/tests/steps/step_16_location_automation/test_nominatim_client.py
from __future__ import annotations

from app.clients.nominatim import NominatimClient


def test_nominatim_parse_response_maps_fields_correctly() -> None:
    client = NominatimClient(base_url="https://example.com")

    payload = [
        {
            "lat": "42.3314",
            "lon": "-83.0458",
            "display_name": "123 N Main St, Detroit, Wayne County, Michigan, 48226, United States",
            "class": "building",
            "type": "house",
            "importance": 0.91,
            "address": {
                "house_number": "123",
                "road": "North Main Street",
                "city": "Detroit",
                "county": "Wayne County",
                "state": "Michigan",
                "state_code": "MI",
                "postcode": "48226",
                "country": "United States",
            },
        }
    ]

    result = client.parse_response(payload)

    assert result is not None
    assert result.source == "nominatim"
    assert result.formatted_address.startswith("123 N Main St")
    assert result.lat == 42.3314
    assert result.lng == -83.0458
    assert result.city == "Detroit"
    assert result.state == "MI"
    assert result.postal_code == "48226"
    assert result.county == "Wayne County"
    assert result.confidence == 0.92
    assert result.provider_status == "OK"


def test_nominatim_parse_response_handles_zero_results() -> None:
    client = NominatimClient(base_url="https://example.com")

    result = client.parse_response([])

    assert result is not None
    assert result.source == "nominatim"
    assert result.lat is None
    assert result.lng is None
    assert result.confidence == 0.0
    assert result.provider_status == "ZERO_RESULTS"