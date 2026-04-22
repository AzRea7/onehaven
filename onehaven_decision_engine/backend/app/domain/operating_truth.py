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


def _as_text(v: Any) -> str:
    return str(v or "").strip()


def enforce_property_truth(payload: dict[str, Any]) -> None:
    addr = _as_text(payload.get("address"))
    city = _as_text(payload.get("city"))
    state = _as_text(payload.get("state") or "MI")
    z = _as_text(payload.get("zip"))

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

    strategy = _as_text(payload.get("strategy") or "section8").lower()
    if strategy not in {"section8", "market"}:
        raise TruthViolation("Deal.strategy must be section8 or market")


def enforce_jurisdiction_truth(payload: dict[str, Any]) -> None:
    state = _as_text(payload.get("state") or "MI").upper()
    county = _as_text(payload.get("county")).lower()
    city = _as_text(payload.get("city")).lower()

    if not state or len(state) != 2:
        raise TruthViolation("Jurisdiction.state must be a 2-letter code")
    if county == "" and city:
        raise TruthViolation("Jurisdiction.county is required when city is provided")

    categories = payload.get("required_categories")
    if categories is not None and not isinstance(categories, (list, tuple)):
        raise TruthViolation("Jurisdiction.required_categories must be a list when provided")


def enforce_assertion_truth(payload: dict[str, Any]) -> None:
    rule_key = _as_text(payload.get("rule_key"))
    category = _as_text(payload.get("normalized_category") or payload.get("rule_category"))
    validation_state = _as_text(payload.get("validation_state")).lower()
    trust_state = _as_text(payload.get("trust_state")).lower()
    governance_state = _as_text(payload.get("governance_state")).lower()
    review_status = _as_text(payload.get("review_status")).lower()

    if not rule_key:
        raise TruthViolation("Assertion.rule_key is required")
    if not category:
        raise TruthViolation("Assertion.normalized_category is required")

    confidence = _as_float(payload.get("confidence"))
    if confidence is not None and (confidence < 0 or confidence > 1):
        raise TruthViolation("Assertion.confidence must be between 0 and 1")

    if trust_state == "trusted" and validation_state != "validated":
        raise TruthViolation("Assertion.trust_state=trusted requires validation_state=validated")
    if governance_state == "active" and review_status != "verified":
        raise TruthViolation("Assertion.governance_state=active requires review_status=verified")
    if governance_state == "active" and validation_state != "validated":
        raise TruthViolation("Assertion.governance_state=active requires validation_state=validated")


def enforce_evidence_truth(payload: dict[str, Any]) -> None:
    truth_bucket = _as_text(payload.get("truth_bucket")).lower()
    authority_use_type = _as_text(payload.get("authority_use_type")).lower()
    authority_tier = _as_text(payload.get("authority_tier")).lower()
    refresh_state = _as_text(payload.get("refresh_state")).lower()

    if truth_bucket and truth_bucket not in {"binding", "supporting", "weak", "unusable", "unknown"}:
        raise TruthViolation("Evidence.truth_bucket invalid")

    if truth_bucket == "binding":
        if authority_use_type != "binding":
            raise TruthViolation("Evidence.truth_bucket=binding requires authority_use_type=binding")
        if authority_tier != "authoritative_official":
            raise TruthViolation("Evidence.truth_bucket=binding requires authoritative_official tier")
        if refresh_state in {"failed", "blocked"}:
            raise TruthViolation("Evidence.truth_bucket=binding cannot be in failed/blocked refresh_state")
