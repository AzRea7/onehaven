from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class TruthViolation(Exception):
    message: str


def _as_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _as_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def enforce_property_truth(payload: dict[str, Any]) -> None:
    addr = (payload.get("address") or "").strip()
    city = (payload.get("city") or "").strip()
    state = (payload.get("state") or "MI").strip()
    z = (payload.get("zip") or "").strip()

    if not addr:
        raise TruthViolation("Property.address is required")
    if not city:
        raise TruthViolation("Property.city is required")
    if not state or len(state) != 2:
        raise TruthViolation("Property.state must be 2-letter code")
    if not z:
        raise TruthViolation("Property.zip is required")

    beds = _as_int(payload.get("bedrooms"))
    baths = _as_float(payload.get("bathrooms"))
    if beds is None or beds <= 0:
        raise TruthViolation("Property.bedrooms must be a positive integer")
    if baths is None or baths <= 0:
        raise TruthViolation("Property.bathrooms must be a positive number")

    sqft = _as_int(payload.get("square_feet"))
    if sqft is not None and sqft <= 0:
        raise TruthViolation("Property.square_feet must be positive when provided")

    year = _as_int(payload.get("year_built"))
    if year is not None and (year < 1700 or year > 2100):
        raise TruthViolation("Property.year_built out of plausible range")


def enforce_deal_truth(payload: dict[str, Any]) -> None:
    asking = _as_float(payload.get("asking_price"))
    if asking is None or asking <= 0:
        raise TruthViolation("Deal.asking_price must be a positive number")

    rehab = _as_float(payload.get("rehab_estimate"))
    if rehab is not None and rehab < 0:
        raise TruthViolation("Deal.rehab_estimate cannot be negative")

    strategy = (payload.get("strategy") or "section8").strip().lower()
    if strategy not in {"section8", "market"}:
        raise TruthViolation("Deal.strategy must be section8 or market")
