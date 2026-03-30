from __future__ import annotations

from typing import Any, Optional

from ..config import settings
from ..services.fmr import HudUserClient as _ServiceHudUserClient


class HudUserClient(_ServiceHudUserClient):
    """
    Thin client wrapper around the normalized HUD FMR service implementation.

    This keeps the import seam stable for older callers while making the FMR
    normalization logic live in exactly one place.
    """

    def __init__(self):
        super().__init__(
            token=getattr(settings, "hud_user_token", "") or "",
            base_url=getattr(settings, "hud_base_url", None),
        )

    def fetch_fmr(
        self,
        *,
        state: str,
        area_name: str,
        year: int,
        bedrooms: int,
        zip_code: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Backward-compatible shape:
          {"fmr": 1500.0, "state": "MI", "area_name": "...", "year": 2026, "bedrooms": 3, "raw": {...}}

        Prefer entityid-based lookup in the router/service layer. This method is
        retained so older code paths do not break immediately.
        """
        data = self.fmr_for_area(state=state, area_name=area_name, year=year)
        fmr_val, pick_meta = self.pick_bedroom_fmr(data, bedrooms, zip_code=zip_code)

        if fmr_val is None:
            raise ValueError("HUD response missing usable bedroom FMR")

        return {
            "fmr": float(fmr_val),
            "state": state,
            "area_name": area_name,
            "year": int(year),
            "bedrooms": int(bedrooms),
            "zip_code": zip_code,
            "pick_meta": pick_meta,
            "raw": data,
        }
    