from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode

import httpx


@dataclass
class PublicTaxLookupResult:
    found: bool
    annual_amount: float | None = None
    annual_rate: float | None = None
    year: int | None = None
    source: str | None = None
    confidence: float | None = None
    lookup_url: str | None = None
    parcel_id: str | None = None
    jurisdiction: str | None = None
    reason: str | None = None
    raw: dict[str, Any] | None = None


_CURRENCY_RE = re.compile(r"[-+]?\$?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)")
_YEAR_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")
_PARCEL_RE = re.compile(r"\b\d{2,4}-\d{2,4}-\d{2,6}-\d{2,4}\b")




def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            out = float(value)
            return out if out >= 0 else None
        text = str(value).strip().replace(",", "").replace("$", "")
        if not text:
            return None
        out = float(text)
        return out if out >= 0 else None
    except Exception:
        return None


def safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        match = _YEAR_RE.search(str(value))
        return int(match.group(1)) if match else int(value)
    except Exception:
        return None


def clean_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_parcel_id(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    match = _PARCEL_RE.search(text)
    if not match:
        return None
    return match.group(0)

def extract_currency(label: str, text: str) -> float | None:
    if not text:
        return None
    pattern = re.compile(rf"{re.escape(label)}[^0-9$]*\$?\s*([0-9][0-9,]*(?:\.[0-9]{{1,2}})?)", re.I)
    m = pattern.search(text)
    if m:
        return safe_float(m.group(1))
    return None


def extract_first_currency(text: str) -> float | None:
    if not text:
        return None
    m = _CURRENCY_RE.search(text)
    if not m:
        return None
    return safe_float(m.group(1))


def build_wayne_lookup_url(
    *,
    municipality: str | None,
    street_number: str | None,
    street_name: str | None,
) -> str | None:
    if not municipality or not street_number or not street_name:
        return None
    params = {
        "Municipality": municipality,
        "StreetNumber": street_number,
        "StreetName": street_name,
    }
    return f"https://pta.waynecounty.com/?{urlencode(params)}"


def split_street_parts(address: str | None) -> tuple[str | None, str | None]:
    raw = clean_text(address)
    if not raw:
        return None, None
    parts = raw.split(" ", 1)
    if len(parts) == 1:
        return parts[0], None
    return parts[0], parts[1]


def fetch_text(
    url: str,
    *,
    timeout_s: float = 20.0,
    headers: dict[str, str] | None = None,
) -> tuple[int, str]:
    hdrs = {
        "User-Agent": "Mozilla/5.0 (compatible; OneHavenTaxBot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if headers:
        hdrs.update(headers)
    with httpx.Client(timeout=timeout_s, follow_redirects=True, headers=hdrs) as client:
        resp = client.get(url)
        return int(resp.status_code), resp.text or ""


def parse_bsa_like_page(
    *,
    html_text: str,
    lookup_url: str | None,
    parcel_id: str | None = None,
    jurisdiction: str | None = None,
) -> PublicTaxLookupResult:
    text = clean_text(html_text)

    candidates: list[float] = []

    for label in (
        "Total Tax",
        "Total Taxes",
        "Summer Tax",
        "Winter Tax",
        "Amount Due",
        "Tax Due",
        "Total Due",
        "Current Tax",
        "Current Balance",
    ):
        value = extract_currency(label, text)
        if value is not None:
            candidates.append(value)

    # Only accept realistic property-tax numbers.
    # This prevents random tiny numbers like 5.00 from being treated as annual tax.
    candidates = [v for v in candidates if v >= 200]

    annual_amount = max(candidates) if candidates else None

    year = safe_int(text)
    detected_parcel = normalize_parcel_id(parcel_id) or normalize_parcel_id(text)

    if annual_amount is None:
        return PublicTaxLookupResult(
            found=False,
            source="public_record_parse",
            confidence=0.0,
            lookup_url=lookup_url,
            parcel_id=detected_parcel,
            jurisdiction=jurisdiction,
            reason="no_tax_amount_found",
            raw={"preview": text[:500]},
        )

    return PublicTaxLookupResult(
        found=True,
        annual_amount=annual_amount,
        annual_rate=None,
        year=year,
        source="public_record_parse",
        confidence=0.85,
        lookup_url=lookup_url,
        parcel_id=detected_parcel,
        jurisdiction=jurisdiction,
        reason="parsed_public_record",
        raw={"preview": text[:1000]},
    )


def lookup_wayne_public_tax_record(
    *,
    address: str | None,
    city: str | None,
    municipality_code: str | None = None,
) -> PublicTaxLookupResult:
    street_number, street_name = split_street_parts(address)
    lookup_url = build_wayne_lookup_url(
        municipality=municipality_code,
        street_number=street_number,
        street_name=street_name,
    )
    if not lookup_url:
        return PublicTaxLookupResult(
            found=False,
            source="wayne_county_public",
            confidence=0.0,
            reason="missing_lookup_inputs",
            jurisdiction=city,
        )

    try:
        status_code, body = fetch_text(lookup_url)
    except Exception as exc:
        return PublicTaxLookupResult(
            found=False,
            source="wayne_county_public",
            confidence=0.0,
            lookup_url=lookup_url,
            jurisdiction=city,
            reason=f"request_failed:{type(exc).__name__}",
            raw={"error": str(exc)},
        )

    if status_code >= 400:
        return PublicTaxLookupResult(
            found=False,
            source="wayne_county_public",
            confidence=0.0,
            lookup_url=lookup_url,
            jurisdiction=city,
            reason=f"http_{status_code}",
            raw={"status_code": status_code},
        )

    parsed = parse_bsa_like_page(
        html_text=body,
        lookup_url=lookup_url,
        jurisdiction=city,
    )
    parsed.source = "wayne_county_public"
    if parsed.found:
        if parsed.annual_amount is not None and parsed.annual_amount >= 200:
            parsed.confidence = 0.9
        else:
            parsed.confidence = 0.25
            parsed.found = False
            parsed.reason = "unrealistic_tax_amount"
    return parsed


def lookup_public_tax_record(
    *,
    address: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
    county: str | None,
    asking_price: float | None = None,
    parcel_id: str | None = None,
    lookup_url: str | None = None,
) -> dict[str, Any]:
    county_norm = clean_text(county).lower()
    state_norm = clean_text(state).upper()

    # Wayne-first public lookup path
    if state_norm == "MI" and "wayne" in county_norm:
        # Lincoln Park / Wayne county code often used in your existing work.
        municipality_code = "45"
        result = lookup_wayne_public_tax_record(
            address=address,
            city=city,
            municipality_code=municipality_code,
        )
        return {
            "found": result.found,
            "annual_amount": result.annual_amount,
            "annual_rate": result.annual_rate,
            "year": result.year,
            "source": result.source,
            "confidence": result.confidence,
            "lookup_url": result.lookup_url,
            "parcel_id": result.parcel_id,
            "jurisdiction": result.jurisdiction,
            "reason": result.reason,
            "raw": result.raw,
        }

    # Generic fallback result
    return {
        "found": False,
        "annual_amount": None,
        "annual_rate": None,
        "year": None,
        "source": "public_record_lookup",
        "confidence": 0.0,
        "lookup_url": lookup_url,
        "parcel_id": normalize_parcel_id(parcel_id),
        "jurisdiction": city or county,
        "reason": "no_public_adapter_available",
        "raw": {
            "address": address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "county": county,
            "asking_price": asking_price,
        },
    }