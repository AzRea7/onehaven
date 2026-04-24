from __future__ import annotations

from typing import Any, Optional

from app.config import settings
from products.intelligence.backend.src.services.fmr import HudUserClient as _ServiceHudUserClient


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
          {"fmr": 1500.0, "state": "MI", "area_name": ".", "year": 2026, "bedrooms": 3, "raw": {.}}

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

    # --- Step 8 additive wrappers ---
    def fetch_fmr_by_entityid(
        self,
        *,
        entityid: str,
        year: int,
        bedrooms: int,
        zip_code: Optional[str] = None,
    ) -> dict[str, Any]:
        data = self.fmr_for_entityid(entityid=entityid, year=year)
        fmr_val, pick_meta = self.pick_bedroom_fmr(data, bedrooms, zip_code=zip_code)
        if fmr_val is None:
            raise ValueError("HUD response missing usable bedroom FMR for entityid")
        return {
            "fmr": float(fmr_val),
            "entityid": str(entityid),
            "year": int(year),
            "bedrooms": int(bedrooms),
            "zip_code": zip_code,
            "pick_meta": pick_meta,
            "raw": data,
        }

    def fetch_payment_standard_hint(
        self,
        *,
        fmr_value: float | None,
        payment_standard_pct: float | None,
    ) -> dict[str, Any]:
        if fmr_value is None:
            return {
                "ok": False,
                "payment_standard": None,
                "reason": "missing_fmr",
            }
        pct = float(payment_standard_pct) if payment_standard_pct is not None else 100.0
        if 0 < pct <= 3.0:
            pct *= 100.0
        return {
            "ok": True,
            "payment_standard": round(float(fmr_value) * (pct / 100.0), 2),
            "payment_standard_pct": float(pct),
            "fmr": float(fmr_value),
        }
