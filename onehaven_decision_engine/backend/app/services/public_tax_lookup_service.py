from __future__ import annotations

import csv
import html
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx


DEFAULT_BSA_UIDS: dict[tuple[str, str, str], int] = {
    ("mi", "wayne", "detroit"): 155,
    ("mi", "wayne", "lincoln park"): 192,
    ("mi", "wayne", "livonia"): 521,
    ("mi", "wayne", "northville township"): 292,
    ("mi", "wayne", "grosse pointe shores"): 2223,
    ("mi", "macomb", "clinton township"): 254,
    ("mi", "macomb", "macomb township"): 259,
    ("mi", "macomb", "harrison township"): 258,
    ("mi", "macomb", "mount clemens"): 632,
    ("mi", "macomb", "shelby township"): 300,
    ("mi", "oakland", "southfield"): 272,
    ("mi", "oakland", "auburn hills"): 462,
    ("mi", "oakland", "ferndale"): 512,
    ("mi", "oakland", "farmington hills"): 316,
}

INVALID_PARCEL_TOKENS = {
    "identifier",
    "entifier",
    "parcel",
    "parcelid",
    "parcelidentifier",
    "property",
    "number",
    "n/a",
    "none",
    "null",
}


@dataclass(frozen=True)
class PublicTaxLookupResult:
    annual_amount: float | None
    annual_rate: float | None
    source: str | None
    confidence: float | None
    year: int | None
    reason: str | None = None
    parcel_id: str | None = None
    jurisdiction: str | None = None
    lookup_url: str | None = None


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        out = float(str(value).replace(",", "").strip())
        return out if out > 0 else None
    except Exception:
        return None


def _coerce_meta(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _normalize_key(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _clean_parcel_id(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    raw = raw.replace("\u00a0", " ")
    raw = re.sub(r"\s+", "", raw).strip(",;:")
    return raw or None


def _is_valid_parcel_id(value: str | None) -> bool:
    if not value:
        return False

    candidate = _clean_parcel_id(value)
    if not candidate:
        return False

    lowered = re.sub(r"[\s.\-_/]+", "", candidate.lower())
    if lowered in INVALID_PARCEL_TOKENS:
        return False

    if not any(ch.isdigit() for ch in candidate):
        return False

    compact = re.sub(r"[\s.\-_/]+", "", candidate)
    if len(compact) < 6:
        return False

    return True


def _walk_for_first_key(obj: Any, candidate_keys: set[str]) -> Any | None:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if _normalize_key(key) in candidate_keys:
                if value not in (None, "", []):
                    return value
            nested = _walk_for_first_key(value, candidate_keys)
            if nested not in (None, "", []):
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _walk_for_first_key(item, candidate_keys)
            if nested not in (None, "", []):
                return nested
    return None


def infer_parcel_id(row: dict[str, Any]) -> str | None:
    meta = _coerce_meta(row.get("acquisition_metadata_json"))
    candidates = [
        row.get("parcel_id"),
        row.get("parcel_number"),
        row.get("apn"),
        row.get("mls_apn"),
        meta.get("parcelId"),
        meta.get("parcelID"),
        meta.get("parcel_id"),
        meta.get("parcelNumber"),
        meta.get("parcel_number"),
        meta.get("apn"),
        meta.get("apnNumber"),
        meta.get("taxParcel"),
        meta.get("taxParcelId"),
        meta.get("taxParcelID"),
        meta.get("sidwell"),
        _walk_for_first_key(
            meta,
            {
                "parcel id",
                "parcelid",
                "parcel_id",
                "parcel number",
                "parcelnumber",
                "parcel_number",
                "apn",
                "apnnumber",
                "tax parcel",
                "taxparcel",
                "taxparcelid",
                "sidwell",
            },
        ),
    ]
    for candidate in candidates:
        cleaned = _clean_parcel_id(candidate)
        if _is_valid_parcel_id(cleaned):
            return cleaned
    return None


def _load_bsa_uid_overrides() -> dict[tuple[str, str, str], int]:
    raw = os.getenv("FREE_TAX_BSA_UID_MAP_JSON", "").strip()
    if not raw:
        return {}

    try:
        parsed = json.loads(raw)
    except Exception:
        return {}

    resolved: dict[tuple[str, str, str], int] = {}
    if isinstance(parsed, dict):
        for key, value in parsed.items():
            try:
                uid = int(value)
            except Exception:
                continue
            parts = [_normalize_key(x) for x in str(key).split("|")]
            while len(parts) < 3:
                parts.append("")
            resolved[(parts[0], parts[1], parts[2])] = uid
    return resolved


def resolve_bsa_uid(row: dict[str, Any]) -> int | None:
    state = _normalize_key(row.get("state"))
    county = _normalize_key(row.get("county"))
    city = _normalize_key(row.get("city"))

    overrides = _load_bsa_uid_overrides()

    for key in (
        (state, county, city),
        (state, "", city),
        ("", county, city),
        ("", "", city),
    ):
        if key in overrides:
            return overrides[key]
        if key in DEFAULT_BSA_UIDS:
            return DEFAULT_BSA_UIDS[key]

    return None


def _timeout_seconds() -> float:
    try:
        return max(2.0, float(os.getenv("FREE_TAX_HTTP_TIMEOUT_SECONDS", "8").strip()))
    except Exception:
        return 8.0


def _collapse_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _strip_tags(html_text: str) -> str:
    return _collapse_whitespace(re.sub(r"<[^>]+>", " ", html_text))


def _normalize_street(value: str) -> str:
    text = _normalize_key(value)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\bavenue\b", "ave", text)
    text = re.sub(r"\bstreet\b", "st", text)
    text = re.sub(r"\broad\b", "rd", text)
    text = re.sub(r"\bdrive\b", "dr", text)
    text = re.sub(r"\bboulevard\b", "blvd", text)
    text = re.sub(r"\blane\b", "ln", text)
    text = re.sub(r"\bcourt\b", "ct", text)
    text = re.sub(r"\bplace\b", "pl", text)
    text = re.sub(r"\bterrace\b", "ter", text)
    text = re.sub(r"\bparkway\b", "pkwy", text)
    text = re.sub(r"\bcircle\b", "cir", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_street_number_and_name(address: str) -> tuple[str | None, str | None]:
    addr = str(address or "").strip()
    if not addr:
        return None, None
    match = re.match(r"^\s*(\d+[A-Z\-]?)\s+(.*)$", addr, flags=re.IGNORECASE)
    if not match:
        return None, _normalize_street(addr)
    return match.group(1).strip(), _normalize_street(match.group(2))


def _address_match_key(*, address: Any, city: Any, state: Any, zip_code: Any) -> str:
    street_number, street_name = _split_street_number_and_name(str(address or ""))
    city_norm = _normalize_key(city)
    state_norm = _normalize_key(state)
    zip_norm = re.sub(r"[^\d]", "", str(zip_code or ""))[:5]
    return "|".join(
        [
            street_number or "",
            street_name or "",
            city_norm or "",
            state_norm or "",
            zip_norm or "",
        ]
    )


def _candidate_keys_for_row(row: dict[str, Any]) -> list[str]:
    address = str(row.get("address") or "").strip()
    city = str(row.get("city") or "").strip()
    state = str(row.get("state") or "").strip()
    zip_code = str(row.get("zip") or "").strip()

    keys: list[str] = []
    primary = _address_match_key(address=address, city=city, state=state, zip_code=zip_code)
    if primary:
        keys.append(primary)

    # fallback without zip
    street_number, street_name = _split_street_number_and_name(address)
    city_norm = _normalize_key(city)
    state_norm = _normalize_key(state)
    keys.append("|".join([street_number or "", street_name or "", city_norm or "", state_norm or "", ""]))

    # fallback without city/zip
    keys.append("|".join([street_number or "", street_name or "", "", state_norm or "", ""]))

    deduped: list[str] = []
    seen: set[str] = set()
    for item in keys:
        if item and item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _parcel_cache_path() -> str | None:
    raw = os.getenv("FREE_TAX_PARCEL_CACHE_PATH", "").strip()
    return raw or None


def _first_present(d: dict[str, Any], names: list[str]) -> Any:
    lowered = {str(k).strip().lower(): v for k, v in d.items()}
    for name in names:
        if name.lower() in lowered:
            return lowered[name.lower()]
    return None


@lru_cache(maxsize=1)
def _load_parcel_cache() -> dict[str, str]:
    """
    Supported file formats:
    - CSV with columns like address, city, state, zip, parcel_id
    - JSON list of objects with those fields
    - JSON object mapping prebuilt address keys -> parcel_id
    """
    path = _parcel_cache_path()
    if not path:
        return {}

    p = Path(path)
    if not p.exists():
        return {}

    try:
        if p.suffix.lower() == ".csv":
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                mapping: dict[str, str] = {}
                for row in reader:
                    parcel = _clean_parcel_id(
                        _first_present(
                            row,
                            [
                                "parcel_id",
                                "parcelid",
                                "parcel",
                                "parcel_number",
                                "apn",
                                "sidwell",
                            ],
                        )
                    )
                    if not _is_valid_parcel_id(parcel):
                        continue

                    key = _address_match_key(
                        address=_first_present(row, ["address", "property_address", "site_address"]),
                        city=_first_present(row, ["city", "municipality"]),
                        state=_first_present(row, ["state"]),
                        zip_code=_first_present(row, ["zip", "zip_code", "zipcode"]),
                    )
                    if key:
                        mapping[key] = parcel
                return mapping

        with p.open("r", encoding="utf-8-sig") as f:
            parsed = json.load(f)

        if isinstance(parsed, dict):
            out: dict[str, str] = {}
            for key, value in parsed.items():
                parcel = _clean_parcel_id(value)
                if key and _is_valid_parcel_id(parcel):
                    out[str(key)] = parcel
            return out

        if isinstance(parsed, list):
            mapping: dict[str, str] = {}
            for row in parsed:
                if not isinstance(row, dict):
                    continue
                parcel = _clean_parcel_id(
                    _first_present(
                        row,
                        [
                            "parcel_id",
                            "parcelid",
                            "parcel",
                            "parcel_number",
                            "apn",
                            "sidwell",
                        ],
                    )
                )
                if not _is_valid_parcel_id(parcel):
                    continue

                key = _address_match_key(
                    address=_first_present(row, ["address", "property_address", "site_address"]),
                    city=_first_present(row, ["city", "municipality"]),
                    state=_first_present(row, ["state"]),
                    zip_code=_first_present(row, ["zip", "zip_code", "zipcode"]),
                )
                if key:
                    mapping[key] = parcel
            return mapping

    except Exception:
        return {}

    return {}


def _find_parcel_id_from_cache(row: dict[str, Any]) -> tuple[str | None, str | None]:
    cache = _load_parcel_cache()
    if not cache:
        return None, None

    for key in _candidate_keys_for_row(row):
        parcel = cache.get(key)
        if _is_valid_parcel_id(parcel):
            return _clean_parcel_id(parcel), f"parcel_cache:{key}"

    return None, None


def _build_bsa_tax_bill_url(*, uid: int, parcel_id: str, year: int) -> str:
    query = urlencode(
        {
            "PageIndex": 1,
            "RecordKey": parcel_id,
            "RecordKeyDisplayString": parcel_id,
            "RecordKeyType": 0,
            "ReferenceKey": parcel_id,
            "ReferenceType": 0,
            "SearchCategory": "Parcel Number",
            "SearchFocus": "All Records",
            "SearchOrigin": 0,
            "SearchText": parcel_id,
            "TaxSeason": 0,
            "TaxType": 2,
            "TaxYear": year,
            "uid": uid,
        },
        doseq=False,
    )
    return f"https://bsaonline.com/Tax_SiteSearch/PrintTaxBill?{query}"


_CURRENCY_RE = r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?|[0-9]+(?:\.[0-9]{2})?)"


def _extract_labeled_amount(text: str, labels: list[str]) -> float | None:
    for label in labels:
        pattern = re.compile(
            rf"{label}[^0-9$]{{0,80}}{_CURRENCY_RE}",
            flags=re.IGNORECASE,
        )
        match = pattern.search(text)
        if not match:
            continue
        amount = _safe_float(match.group(1))
        if amount is not None:
            return amount
    return None


def _parse_bsa_tax_bill(html_text: str) -> tuple[float | None, str | None]:
    text = _strip_tags(html.unescape(html_text))

    summer = _extract_labeled_amount(
        text,
        [
            r"summer tax amount(?:\(\$\))?",
            r"summer amount",
            r"summer taxes?",
        ],
    )
    winter = _extract_labeled_amount(
        text,
        [
            r"winter tax amount(?:\(\$\))?",
            r"winter amount",
            r"winter taxes?",
        ],
    )
    if summer is not None and winter is not None:
        return round(summer + winter, 2), "summer_plus_winter"

    total = _extract_labeled_amount(
        text,
        [
            r"total due",
            r"total amount due",
            r"total taxes?",
            r"ad valorem taxes?",
            r"tax amount due",
            r"amount due",
        ],
    )
    if total is not None:
        return total, "total_due"

    return None, None


def lookup_public_tax_record(
    *,
    row: dict[str, Any],
    asking_price: float | None,
) -> PublicTaxLookupResult | None:
    uid = resolve_bsa_uid(row)
    if uid is None:
        return None

    year_now = datetime.utcnow().year
    jurisdiction = " / ".join(
        x
        for x in [
            str(row.get("city") or "").strip(),
            str(row.get("county") or "").strip(),
            str(row.get("state") or "").strip(),
        ]
        if x
    ) or None

    headers = {
        "User-Agent": "OneHavenTaxEnrichment/1.0",
        "Accept": "text/html,application/xhtml+xml,text/html;q=0.9,*/*;q=0.8",
    }

    parcel_id = infer_parcel_id(row)
    parcel_lookup_url: str | None = None
    reason = "matched_existing_parcel_metadata"

    if not parcel_id:
        parcel_id, parcel_lookup_url = _find_parcel_id_from_cache(row)
        reason = "matched_county_parcel_cache"

    if not _is_valid_parcel_id(parcel_id):
        return None

    with httpx.Client(
        timeout=_timeout_seconds(),
        headers=headers,
        follow_redirects=True,
    ) as client:
        for year in (year_now, year_now - 1, year_now - 2):
            url = _build_bsa_tax_bill_url(uid=uid, parcel_id=parcel_id, year=year)
            try:
                response = client.get(url)
                if response.status_code != 200:
                    continue
            except Exception:
                continue

            annual_amount, parse_reason = _parse_bsa_tax_bill(response.text)
            if annual_amount is None:
                continue

            annual_rate = None
            if asking_price is not None and asking_price > 0:
                annual_rate = round(float(annual_amount) / float(asking_price), 6)

            confidence = 0.97 if parse_reason == "total_due" else 0.94

            return PublicTaxLookupResult(
                annual_amount=annual_amount,
                annual_rate=annual_rate,
                source="public_bsa_current_tax",
                confidence=confidence,
                year=year,
                reason=f"{reason}:{parse_reason or 'public_tax_bill'}",
                parcel_id=parcel_id,
                jurisdiction=jurisdiction,
                lookup_url=url or parcel_lookup_url,
            )

    return None