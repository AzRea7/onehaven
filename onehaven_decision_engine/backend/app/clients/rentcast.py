from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx

from ..config import settings


@dataclass(frozen=True)
class RentcastRentEstimate:
    rent: Optional[float]
    rent_range_low: Optional[float]
    rent_range_high: Optional[float]
    raw: dict[str, Any]


class RentcastClient:
    def __init__(self) -> None:
        self.base = settings.rentcast_base_url.rstrip("/")
        self.api_key = settings.rentcast_api_key

    def enabled(self) -> bool:
        return bool(self.api_key)

    def estimate_long_term_rent(
        self,
        *,
        address: str,
        city: str,
        state: str,
        zip_code: str,
        bedrooms: int,
        bathrooms: float,
        square_feet: Optional[int],
    ) -> RentcastRentEstimate:
        if not self.api_key:
            return RentcastRentEstimate(None, None, None, {"error": "rentcast_api_key not set"})

        url = f"{self.base}/avm/rent/long-term"
        payload: dict[str, Any] = {
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
        }
        if square_feet:
            payload["square_feet"] = square_feet

        headers = {"X-Api-Key": self.api_key}

        try:
            with httpx.Client(timeout=20.0) as client:
                r = client.post(url, json=payload, headers=headers)
                r.raise_for_status()
                data = r.json()
        except Exception as e:
            return RentcastRentEstimate(None, None, None, {"error": str(e), "endpoint": url, "request": payload})

        rent = data.get("rent")
        low = data.get("rentRangeLow")
        high = data.get("rentRangeHigh")

        return RentcastRentEstimate(
            rent=float(rent) if isinstance(rent, (int, float)) else None,
            rent_range_low=float(low) if isinstance(low, (int, float)) else None,
            rent_range_high=float(high) if isinstance(high, (int, float)) else None,
            raw=data,
        )
