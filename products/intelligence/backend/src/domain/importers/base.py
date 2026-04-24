# backend/app/domain/importers/base.py
from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from io import StringIO
from typing import Any, Optional


def _clean_str(x: Any) -> str:
    return (str(x).strip() if x is not None else "").strip()


def _to_float(x: Any) -> Optional[float]:
    s = _clean_str(x).replace("$", "").replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _to_int(x: Any) -> Optional[int]:
    f = _to_float(x)
    return int(f) if f is not None else None


def _to_bool(x: Any) -> bool:
    s = _clean_str(x).lower()
    return s in {"1", "true", "yes", "y", "t"}


def fingerprint(source: str, address: str, zip_code: str, asking_price: float) -> str:
    key = f"{source}|{address.lower()}|{zip_code}|{asking_price:.2f}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


@dataclass
class NormalizedRow:
    # property
    address: str
    city: str
    state: str
    zip: str
    bedrooms: int
    bathrooms: float
    square_feet: Optional[int]
    year_built: Optional[int]
    has_garage: bool
    property_type: str

    # deal
    asking_price: float
    estimated_purchase_price: Optional[float]
    rehab_estimate: float
    source: str

    # rent assumptions (optional)
    market_rent_estimate: Optional[float]
    section8_fmr: Optional[float]
    approved_rent_ceiling: Optional[float]
    rent_reasonableness_comp: Optional[float]
    inventory_count: Optional[int]
    starbucks_minutes: Optional[int]

    raw: dict[str, Any]


def parse_csv_bytes(data: bytes) -> list[dict[str, str]]:
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(StringIO(text))
    out: list[dict[str, str]] = []
    for row in reader:
        out.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
    return out


def _get_ci(row: dict[str, str], key: str) -> Optional[str]:
    # exact match first
    if key in row:
        return row.get(key)
    # case-insensitive fallback
    key_cf = key.casefold()
    for k, v in row.items():
        if (k or "").casefold() == key_cf:
            return v
    return None


def required(row: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = _get_ci(row, k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def optional_float(row: dict[str, str], *keys: str) -> Optional[float]:
    for k in keys:
        v = _get_ci(row, k)
        f = _to_float(v)
        if f is not None:
            return f
    return None


def optional_int(row: dict[str, str], *keys: str) -> Optional[int]:
    for k in keys:
        v = _get_ci(row, k)
        i = _to_int(v)
        if i is not None:
            return i
    return None


def optional_bool(row: dict[str, str], *keys: str) -> bool:
    for k in keys:
        v = _get_ci(row, k)
        if v is None:
            continue
        if _clean_str(v):
            return _to_bool(v)
    return False
