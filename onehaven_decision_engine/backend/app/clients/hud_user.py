from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import httpx

from ..config import settings


@dataclass(frozen=True)
class HudFmrResult:
    fmr: Optional[float]
    entityid: Optional[str]
    raw: dict[str, Any]


class HudUserClient:
    def __init__(self) -> None:
        self.base = settings.hud_base_url.rstrip("/")
        self.token = settings.hud_user_token

    def enabled(self) -> bool:
        return bool(self.token)

    def _headers(self) -> dict[str, str]:
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def fmr_by_entityid(self, *, entityid: str, bedrooms: int, year: Optional[int] = None) -> HudFmrResult:
        """
        HUD FMR endpoint uses an "entityid" (often 10 digits).
        We can build it from stateFips + countyFips + '99999' (HUD convention used in many datasets).
        Example: MI(26) + Wayne(163) + 99999 => 2616399999
        """
        if not self.token:
            return HudFmrResult(None, None, {"error": "hud_user_token not set"})

        # Endpoint shape varies across HUD datasets; this matches common FMR API patterns.
        # If your dataset differs, you only update THIS function.
        url = f"{self.base}/fmr/data/{entityid}"

        params: dict[str, Any] = {}
        if year is not None:
            params["year"] = year

        try:
            with httpx.Client(timeout=20.0) as client:
                r = client.get(url, headers=self._headers(), params=params)
                r.raise_for_status()
                data = r.json()
        except Exception as e:
            return HudFmrResult(None, entityid, {"error": str(e), "url": str(url), "params": params})

        # Try common shapes:
        # data might be {"data":{"basicdata":[{"fmr0":...,"fmr1":...}]}}
        # or {"basicdata":[...]} etc.
        basic = None
        if isinstance(data, dict):
            if isinstance(data.get("data"), dict) and isinstance(data["data"].get("basicdata"), list):
                basic = data["data"]["basicdata"][0] if data["data"]["basicdata"] else None
            elif isinstance(data.get("basicdata"), list):
                basic = data["basicdata"][0] if data["basicdata"] else None

        fmr = None
        if isinstance(basic, dict):
            key = f"fmr{bedrooms}"
            v = basic.get(key)
            if isinstance(v, (int, float)):
                fmr = float(v)

        return HudFmrResult(fmr=fmr, entityid=entityid, raw=data)
