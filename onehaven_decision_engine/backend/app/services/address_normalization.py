# backend/app/services/address_normalization.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


_DIRECTIONALS = {
    "north": "N",
    "south": "S",
    "east": "E",
    "west": "W",
    "northeast": "NE",
    "northwest": "NW",
    "southeast": "SE",
    "southwest": "SW",
    "n": "N",
    "s": "S",
    "e": "E",
    "w": "W",
    "ne": "NE",
    "nw": "NW",
    "se": "SE",
    "sw": "SW",
}

_STREET_TYPES = {
    "street": "St",
    "st": "St",
    "avenue": "Ave",
    "ave": "Ave",
    "road": "Rd",
    "rd": "Rd",
    "boulevard": "Blvd",
    "blvd": "Blvd",
    "drive": "Dr",
    "dr": "Dr",
    "lane": "Ln",
    "ln": "Ln",
    "court": "Ct",
    "ct": "Ct",
    "circle": "Cir",
    "cir": "Cir",
    "place": "Pl",
    "pl": "Pl",
    "parkway": "Pkwy",
    "pkwy": "Pkwy",
    "highway": "Hwy",
    "hwy": "Hwy",
    "terrace": "Ter",
    "ter": "Ter",
    "trail": "Trl",
    "trl": "Trl",
    "way": "Way",
    "square": "Sq",
    "sq": "Sq",
}

_UNIT_PREFIXES = {
    "apartment": "Apt",
    "apt": "Apt",
    "unit": "Unit",
    "suite": "Ste",
    "ste": "Ste",
    "#": "#",
}

_STATE_ABBR = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}


@dataclass(frozen=True)
class NormalizedAddress:
    address_line1: str
    city: str
    state: str
    postal_code: str
    full_address: str

    def as_dict(self) -> dict[str, str]:
        return {
            "address_line1": self.address_line1,
            "city": self.city,
            "state": self.state,
            "postal_code": self.postal_code,
            "full_address": self.full_address,
        }


def _clean_whitespace(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\n", " ").replace("\r", " ").strip()
    value = re.sub(r"\s+", " ", value)
    return value


def _strip_punctuation_for_token_matching(value: str) -> str:
    value = value.replace(",", " ")
    value = value.replace(".", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _title_word(word: str) -> str:
    if not word:
        return word
    if word.upper() in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}:
        return word.upper()
    if re.fullmatch(r"\d+[A-Za-z]?", word):
        return word.upper()
    return word.capitalize()


def normalize_city(city: str | None) -> str:
    city = _clean_whitespace(city)
    if not city:
        return ""
    words = re.split(r"\s+", city.lower())
    return " ".join(_title_word(w) for w in words if w)


def normalize_state(state: str | None) -> str:
    state = _clean_whitespace(state)
    if not state:
        return ""
    lowered = state.lower()
    if lowered in _STATE_ABBR:
        return _STATE_ABBR[lowered]
    if len(state) == 2:
        return state.upper()
    return state[:2].upper()


def normalize_zip(postal_code: str | None) -> str:
    postal_code = _clean_whitespace(postal_code)
    if not postal_code:
        return ""
    digits = re.sub(r"[^\d-]", "", postal_code)
    if re.fullmatch(r"\d{5}-\d{4}", digits):
        return digits
    only_digits = re.sub(r"[^\d]", "", postal_code)
    if len(only_digits) >= 5:
        return only_digits[:5]
    return only_digits


def _extract_unit(tokens: list[str]) -> tuple[list[str], str]:
    if not tokens:
        return tokens, ""

    out: list[str] = []
    unit = ""
    i = 0
    while i < len(tokens):
        token = tokens[i]
        normalized = token.lower()

        if normalized in _UNIT_PREFIXES:
            prefix = _UNIT_PREFIXES[normalized]
            value = tokens[i + 1] if i + 1 < len(tokens) else ""
            if prefix == "#" and value:
                unit = f"# {value.upper()}"
                i += 2
                continue
            if value:
                unit = f"{prefix} {value.upper()}"
                i += 2
                continue

        if token.startswith("#") and len(token) > 1:
            unit = f"# {token[1:].upper()}"
            i += 1
            continue

        out.append(token)
        i += 1

    return out, unit


def normalize_address_line1(address: str | None) -> str:
    address = _clean_whitespace(address)
    if not address:
        return ""

    # normalize separators and punctuation
    address = address.replace(",", " ")
    address = address.replace(";", " ")
    address = re.sub(r"\s+", " ", address).strip()

    raw_tokens = _strip_punctuation_for_token_matching(address).split(" ")
    raw_tokens = [t for t in raw_tokens if t]

    tokens, unit = _extract_unit(raw_tokens)

    normalized_tokens: list[str] = []
    for idx, token in enumerate(tokens):
        lowered = token.lower()

        if lowered in _DIRECTIONALS:
            normalized_tokens.append(_DIRECTIONALS[lowered])
            continue

        if lowered in _STREET_TYPES:
            normalized_tokens.append(_STREET_TYPES[lowered])
            continue

        # ordinal handling like 1st, 2nd, 3rd
        if re.fullmatch(r"\d+(st|nd|rd|th)", lowered):
            normalized_tokens.append(lowered.upper())
            continue

        # preserve number/letter combinations like 123B
        if re.fullmatch(r"\d+[a-z]?", lowered):
            normalized_tokens.append(token.upper())
            continue

        normalized_tokens.append(_title_word(token))

    line1 = " ".join(normalized_tokens).strip()
    if unit:
        line1 = f"{line1} {unit}".strip()

    return line1


def normalize_full_address(
    address: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
) -> NormalizedAddress:
    line1 = normalize_address_line1(address)
    norm_city = normalize_city(city)
    norm_state = normalize_state(state)
    norm_zip = normalize_zip(postal_code)

    parts = [part for part in [line1, norm_city, norm_state, norm_zip] if part]
    full = ", ".join([line1, norm_city, f"{norm_state} {norm_zip}".strip()]).strip(", ").strip()

    return NormalizedAddress(
        address_line1=line1,
        city=norm_city,
        state=norm_state,
        postal_code=norm_zip,
        full_address=full if full else ", ".join(parts),
    )


def make_normalized_cache_key(
    address: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
) -> str:
    normalized = normalize_full_address(address, city, state, postal_code)
    return normalized.full_address


def addresses_equivalent(
    left_address: str | None,
    left_city: str | None,
    left_state: str | None,
    left_postal_code: str | None,
    right_address: str | None,
    right_city: str | None,
    right_state: str | None,
    right_postal_code: str | None,
) -> bool:
    return make_normalized_cache_key(
        left_address,
        left_city,
        left_state,
        left_postal_code,
    ) == make_normalized_cache_key(
        right_address,
        right_city,
        right_state,
        right_postal_code,
    )