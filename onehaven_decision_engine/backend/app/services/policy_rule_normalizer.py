from __future__ import annotations

import re
from typing import Any, Optional


def _clean(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def normalize_rule_key(raw_text: str, *, hint: Optional[str] = None) -> Optional[str]:
    whole = f"{_clean(hint)} {_clean(raw_text)}".strip()

    patterns = [
        (r"(rental|property).*(registration|register)", "rental_registration_required"),
        (r"(inspection|inspected).*(required|must)|inspection program", "inspection_required"),
        (r"certificate.*occupancy|occupancy permit", "certificate_required_before_occupancy"),
        (r"local agent|local representative|responsible agent", "local_agent_required"),
        (r"po box.*not allowed|physical address required", "owner_po_box_allowed"),
        (r"fees.*paid|all fees.*paid", "all_fees_must_be_paid"),
        (r"debt.*block|delinquent.*tax|city debts", "city_debts_block_license"),
        (r"source of income|voucher.*discrimination", "source_of_income_discrimination_prohibited"),
        (r"license term|renewal.*year", "rental_license_term_years"),
        (r"renew.*before expiration|renewal.*days", "renewal_days_before_expiration"),
    ]

    for pattern, key in patterns:
        if re.search(pattern, whole):
            return key
    return None


def normalize_value(rule_key: str, raw_text: str) -> dict[str, Any]:
    text = _clean(raw_text)

    if rule_key == "owner_po_box_allowed":
        return {"allowed": not ("not allowed" in text or "cannot" in text)}

    if rule_key.endswith("_required"):
        return {"required": True}

    if rule_key == "source_of_income_discrimination_prohibited":
        return {"prohibited": True}

    m = re.search(r"(\d{1,3})\s*miles?", text)
    if rule_key == "local_agent_required" and m:
        return {"required": True, "max_radius_miles": int(m.group(1))}

    m = re.search(r"(\d{1,3})\s*days?", text)
    if rule_key == "renewal_days_before_expiration" and m:
        return {"days": int(m.group(1))}

    m = re.search(r"(\d{1,2})\s*years?", text)
    if rule_key == "rental_license_term_years" and m:
        return {"years": int(m.group(1))}

    return {"text": raw_text.strip()}
