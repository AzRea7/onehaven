# backend/app/services/fmr.py
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class HttpResp:
    status: int
    data: Any


def _http_get_json(url: str, headers: dict[str, str], timeout_s: int = 20) -> HttpResp:
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                payload = {"_raw": raw}
            return HttpResp(status=int(resp.status), data=payload)
    except Exception as e:
        return HttpResp(status=0, data={"error": str(e), "url": url})


class HudUserClient:
    """
    HUD User API client for FMR.
    Matches your current in-router behavior:
      - uses Bearer token
      - fetches /fmr/data/{entityid}
      - extracts bedroom value from 'basicdata'
    """

    FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"

    def __init__(self, token: str):
        if not token:
            raise ValueError("HUD_USER_TOKEN is missing")
        self._headers = {"Authorization": f"Bearer {token}"}

    def fmr_for_entityid(self, entityid: str, year: Optional[int] = None) -> dict[str, Any]:
        entityid = str(entityid).strip()
        if not entityid:
            raise ValueError("entityid missing")

        if year:
            url = f"{self.FMR_BASE}/data/{entityid}?{urllib.parse.urlencode({'year': year})}"
        else:
            url = f"{self.FMR_BASE}/data/{entityid}"

        resp = _http_get_json(url, self._headers)
        if resp.status != 200:
            raise RuntimeError(f"HUD FMR failed (status={resp.status}): {resp.data}")

        payload = resp.data
        data = payload.get("data") if isinstance(payload, dict) else payload
        if not data:
            raise RuntimeError(f"HUD FMR empty response: {payload}")
        return data

    @staticmethod
    def pick_bedroom_fmr(fmr_data: dict[str, Any], bedrooms: int) -> Optional[float]:
        b = int(bedrooms or 0)
        if b <= 0:
            key = "Efficiency"
        elif b == 1:
            key = "One-Bedroom"
        elif b == 2:
            key = "Two-Bedroom"
        elif b == 3:
            key = "Three-Bedroom"
        else:
            key = "Four-Bedroom"

        basic = fmr_data.get("basicdata")

        if isinstance(basic, dict):
            val = basic.get(key)
            try:
                return float(val) if val is not None else None
            except Exception:
                return None

        if isinstance(basic, list):
            row = None
            for r in basic:
                if isinstance(r, dict) and str(r.get("zip_code", "")).lower() == "msa level":
                    row = r
                    break
            if row is None and basic and isinstance(basic[0], dict):
                row = basic[0]
            if row:
                val = row.get(key)
                try:
                    return float(val) if val is not None else None
                except Exception:
                    return None

        return None


__all__ = ["HudUserClient"]
