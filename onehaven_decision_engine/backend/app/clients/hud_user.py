# backend/app/clients/hud_user.py
from __future__ import annotations

import httpx

from ..config import settings


class HudUserClient:
    """
    Minimal HUD USER API client.
    Cache results in DB (HudFmrRecord). Do NOT call this during underwriting loops unless explicitly requested.
    """

    def __init__(self):
        self.base_url = settings.hud_base_url.rstrip("/")

    def _headers(self) -> dict[str, str]:
        if not settings.hud_user_token:
            return {}
        return {"Authorization": f"Bearer {settings.hud_user_token}"}

    def fetch_fmr(self, *, state: str, area_name: str, year: int, bedrooms: int) -> dict:
        """
        This is intentionally simple and may require you to align 'area_name' with HUD's identifiers.
        You can evolve to using HUD geo queries later.

        Return shape:
          {"fmr": 1500.0, "state": "MI", "area_name": "...", "year": 2026, "bedrooms": 3, "raw": {...}}
        """
        # NOTE: HUD endpoints differ by dataset/version. This is a placeholder "wire" method:
        # - it proves the integration seam
        # - you can adjust the URL/params once you standardize on a HUD endpoint
        #
        # The *engine* remains deterministic because it uses DB cache first.
        url = f"{self.base_url}/fmr/data"
        params = {"state": state, "area_name": area_name, "year": year, "bedrooms": bedrooms}

        with httpx.Client(timeout=20.0) as client:
            r = client.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            raw = r.json()

        # Your normalization rule: you own it.
        fmr_val = raw.get("fmr") or raw.get("data", {}).get("fmr")
        if fmr_val is None:
            raise ValueError("HUD response missing fmr")

        return {
            "fmr": float(fmr_val),
            "state": state,
            "area_name": area_name,
            "year": int(year),
            "bedrooms": int(bedrooms),
            "raw": raw,
        }