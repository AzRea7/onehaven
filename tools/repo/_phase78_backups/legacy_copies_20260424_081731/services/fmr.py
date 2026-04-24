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


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        return out if out > 0 else None
    except Exception:
        return None


def _norm_zip(zip_code: str | None) -> str:
    raw = "".join(ch for ch in str(zip_code or "") if ch.isdigit())
    return raw[:5]


class HudUserClient:
    """
    Normalized HUD User FMR client.

    Key fixes:
    - one implementation of HUD normalization
    - callers can keep using entityid-based lookup
    - bedroom picking prefers a property ZIP row over generic MSA-level rows
    - base URL normalization always resolves to the HUD FMR endpoint family
    """

    FMR_BASE = "https://www.huduser.gov/hudapi/public/fmr"

    def __init__(self, token: str, base_url: str | None = None):
        if not token:
            raise ValueError("HUD_USER_TOKEN is missing")
        self._headers = {"Authorization": f"Bearer {token}"}

        raw_base = (base_url or "").rstrip("/")
        if raw_base:
            if raw_base.endswith("/hudapi/public/fmr"):
                self._base_url = raw_base
            elif raw_base.endswith("/fmr"):
                self._base_url = raw_base
            elif raw_base.endswith("/public"):
                self._base_url = f"{raw_base}/fmr"
            elif raw_base.endswith("/hudapi"):
                self._base_url = f"{raw_base}/public/fmr"
            else:
                self._base_url = self.FMR_BASE
        else:
            self._base_url = self.FMR_BASE

    def _extract_data(self, payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            data = payload.get("data", payload)
            if isinstance(data, dict):
                return data
        raise RuntimeError(f"HUD FMR empty or invalid response: {payload}")

    def _get(self, url: str) -> dict[str, Any]:
        resp = _http_get_json(url, self._headers)
        if resp.status != 200:
            raise RuntimeError(f"HUD FMR failed (status={resp.status}): {resp.data}")
        return self._extract_data(resp.data)

    def fmr_for_entityid(self, entityid: str, year: Optional[int] = None) -> dict[str, Any]:
        entityid = str(entityid).strip()
        if not entityid:
            raise ValueError("entityid missing")

        params: dict[str, Any] = {}
        if year:
            params["year"] = int(year)

        url = f"{self._base_url}/data/{urllib.parse.quote(entityid)}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"

        return self._get(url)

    def fmr_for_area(self, *, state: str, area_name: str, year: int) -> dict[str, Any]:
        """
        Best-effort area lookup for ZIP/county/city driven fallbacks.

        This keeps your older generic seam usable while allowing the router
        to try ZIP/county/city before falling back to comp-derived entity ids.
        """
        state = str(state or "").strip()
        area_name = str(area_name or "").strip()
        if not state or not area_name:
            raise ValueError("state and area_name are required")

        url = (
            f"{self._base_url}/data?"
            f"{urllib.parse.urlencode({'state': state, 'area_name': area_name, 'year': int(year)})}"
        )
        return self._get(url)

    @staticmethod
    def _bedroom_key(bedrooms: int) -> str:
        b = int(bedrooms or 0)
        if b <= 0:
            return "Efficiency"
        if b == 1:
            return "One-Bedroom"
        if b == 2:
            return "Two-Bedroom"
        if b == 3:
            return "Three-Bedroom"
        return "Four-Bedroom"

    @staticmethod
    def _basic_rows(fmr_data: dict[str, Any]) -> list[dict[str, Any]]:
        basic = fmr_data.get("basicdata")

        if isinstance(basic, dict):
            return [basic]

        if isinstance(basic, list):
            return [row for row in basic if isinstance(row, dict)]

        return []

    @classmethod
    def _score_row_for_zip(cls, row: dict[str, Any], zip_code: str | None) -> tuple[int, int]:
        """
        Lower is better.
        0 -> exact property ZIP match
        1 -> same prefix-ish zipish row
        2 -> explicit MSA-level / aggregate row
        3 -> any other row
        """
        wanted = _norm_zip(zip_code)
        row_zip = _norm_zip(row.get("zip_code") or row.get("ZIP") or row.get("zip"))

        if wanted and row_zip and row_zip == wanted:
            return (0, 0)

        if wanted and row_zip and wanted[:3] and row_zip[:3] == wanted[:3]:
            return (1, 0)

        label = str(row.get("zip_code") or row.get("ZIP") or row.get("zip") or "").strip().lower()
        name = str(row.get("name") or row.get("label") or "").strip().lower()

        if label == "msa level" or name == "msa level":
            return (2, 0)

        return (3, 0)

    @classmethod
    def _best_basic_row(
        cls,
        fmr_data: dict[str, Any],
        zip_code: str | None = None,
    ) -> tuple[Optional[dict[str, Any]], dict[str, Any]]:
        rows = cls._basic_rows(fmr_data)
        if not rows:
            return None, {"row_scope": "none", "zip_code": None}

        if len(rows) == 1:
            only = rows[0]
            row_zip = _norm_zip(only.get("zip_code") or only.get("ZIP") or only.get("zip"))
            scope = "zip" if row_zip else "single_row"
            return only, {"row_scope": scope, "zip_code": row_zip or None}

        ranked = sorted(rows, key=lambda r: cls._score_row_for_zip(r, zip_code))
        best = ranked[0]
        best_rank = cls._score_row_for_zip(best, zip_code)[0]
        best_zip = _norm_zip(best.get("zip_code") or best.get("ZIP") or best.get("zip"))

        if best_rank == 0:
            scope = "zip_exact"
        elif best_rank == 1:
            scope = "zip_prefix"
        elif best_rank == 2:
            scope = "msa_level"
        else:
            scope = "fallback_row"

        return best, {"row_scope": scope, "zip_code": best_zip or None}

    @classmethod
    def pick_bedroom_fmr(
        cls,
        fmr_data: dict[str, Any],
        bedrooms: int,
        zip_code: str | None = None,
    ) -> tuple[Optional[float], dict[str, Any]]:
        key = cls._bedroom_key(bedrooms)
        row, meta = cls._best_basic_row(fmr_data, zip_code=zip_code)

        if row is None:
            return None, meta

        val = _to_float(row.get(key))
        if val is not None:
            return float(val), {**meta, "bedroom_key": key}

        basic = fmr_data.get("basicdata")
        if isinstance(basic, dict):
            val = _to_float(basic.get(key))
            if val is not None:
                return float(val), {**meta, "bedroom_key": key}

        return None, {**meta, "bedroom_key": key}

    @classmethod
    def is_zip_match_good(cls, pick_meta: dict[str, Any], zip_code: str | None) -> bool:
        wanted = _norm_zip(zip_code)
        picked = _norm_zip(pick_meta.get("zip_code"))
        scope = str(pick_meta.get("row_scope") or "").strip().lower()

        if not wanted:
            return scope in {"single_row", "zip", "msa_level", "fallback_row", "zip_exact", "zip_prefix"}

        if scope == "zip_exact" and picked == wanted:
            return True
        if scope == "zip_prefix" and picked and wanted[:3] and picked[:3] == wanted[:3]:
            return True
        if scope in {"single_row", "zip"} and (not picked or picked == wanted):
            return True

        return False


__all__ = ["HudUserClient"]