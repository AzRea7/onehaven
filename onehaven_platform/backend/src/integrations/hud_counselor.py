from __future__ import annotations

from typing import Any, Optional

import httpx


class HUDCounselorClient:
    """
    HUD Housing Counselor API client.

    NOTE:
    - This API does NOT use the HUD User token
    - It is completely public
    - Used for agency/location lookup only
    """

    BASE_URL = "http://data.hud.gov/Housing_Counselor"

    def __init__(self, timeout: float = 30.0) -> None:
        self.timeout = timeout

    def search(
        self,
        *,
        agency_name: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        params = {
            "AgencyName": agency_name or "",
            "City": city or "",
            "State": state or "",
        }

        with httpx.Client(timeout=self.timeout) as client:
            resp = client.get(f"{self.BASE_URL}/search", params=params)
            resp.raise_for_status()
            return resp.json()

    def search_by_location(
        self,
        *,
        lat: float,
        lng: float,
        distance: int = 10,
    ) -> list[dict[str, Any]]:
        params = {
            "Lat": str(lat),
            "Long": str(lng),
            "Distance": str(distance),
        }

        with httpx.Client(timeout=self.timeout) as client:
            resp = client.get(f"{self.BASE_URL}/search", params=params)
            resp.raise_for_status()
            return resp.json()