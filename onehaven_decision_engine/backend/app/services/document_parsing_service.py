from __future__ import annotations

import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any

SUPPORTED_DOCUMENT_KINDS = {
    "purchase_agreement",
    "loan_estimate",
    "loan_documents",
    "closing_disclosure",
    "insurance_binder",
    "inspection_report",
    "title_documents",
}

FIELD_LABELS = {
    "buyer_name": "Buyer name",
    "seller_name": "Seller name",
    "purchase_price": "Purchase price",
    "earnest_money": "Earnest money",
    "financing_contingency": "Financing contingency",
    "inspection_contingency": "Inspection contingency",
    "inspection_contingency_date": "Inspection contingency deadline",
    "financing_contingency_date": "Financing contingency deadline",
    "appraisal_gap_terms": "Appraisal gap terms",
    "appraisal_deadline": "Appraisal deadline",
    "seller_credits": "Seller credits",
    "target_close_date": "Target close date",
    "closing_datetime": "Closing date",
    "occupancy_possession_terms": "Occupancy / possession terms",
    "listing_agent_name": "Listing agent",
    "listing_agent_contact": "Listing agent contact",
    "buyer_agent_name": "Buyer agent",
    "buyer_agent_contact": "Buyer agent contact",
    "brokerage_contacts": "Brokerage contacts",
    "notable_clauses": "Notable clauses",
    "lender_name": "Lender name",
    "loan_officer_contact": "Loan officer contact",
    "loan_type": "Loan type",
    "interest_rate": "Interest rate",
    "monthly_payment_estimate": "Monthly payment estimate",
    "cash_to_close": "Cash to close",
    "points_fees": "Points / fees",
    "prepayment_notes": "Prepayment notes",
    "lock_status": "Rate lock status",
    "lock_date": "Rate lock date",
    "closing_date": "Closing date",
    "title_parties": "Title / escrow parties",
    "fee_breakdown": "Fee breakdown",
    "final_wire_items": "Wire-related items",
    "carrier_name": "Carrier",
    "insurance_agent_contact": "Insurance agent / broker",
    "annual_premium": "Premium",
    "coverage_effective_date": "Coverage effective date",
    "coverage_dwelling": "Dwelling coverage",
    "coverage_liability": "Liability coverage",
    "coverage_deductible": "Deductible",
    "major_defects": "Major defects",
    "safety_issues": "Safety issues",
    "system_flags": "System-level flags",
    "recommended_repairs": "Recommended repairs",
    "negotiation_leverage_items": "Negotiation leverage",
    "title_company": "Title company",
    "escrow_officer": "Escrow officer",
    "title_defects": "Title defects",
    "liens": "Liens",
    "exceptions": "Exceptions",
    "unresolved_objections": "Unresolved objections",
    "title_objection_deadline": "Title objection deadline",
    "earnest_money_deadline": "Earnest money deadline",
    "inspection_company": "Inspection company",
    "inspector_name": "Inspector",
}

CONTACT_FIELD_ROLES = {
    "listing_agent_name": "listing_agent",
    "buyer_agent_name": "buyer_agent",
    "brokerage_contacts": "listing_office",
    "lender_name": "lender",
    "loan_officer_contact": "loan_officer",
    "carrier_name": "insurance_agency",
    "insurance_agent_contact": "insurance_agent",
    "title_company": "title_company",
    "escrow_officer": "escrow_officer",
    "inspection_company": "inspection_company",
    "inspector_name": "inspector",
}

DEADLINE_FIELDS = {
    "inspection_contingency_date",
    "financing_contingency_date",
    "appraisal_deadline",
    "target_close_date",
    "closing_datetime",
    "closing_date",
    "title_objection_deadline",
    "coverage_effective_date",
    "lock_date",
    "earnest_money_deadline",
}

RISK_PATTERNS = [
    ("mold", r"\bmold\b|\bblack mold\b", "Mold mentioned in document."),
    ("water_damage", r"\bwater (?:intrusion|damage|leak|stain)\b|\bleak(?:ing)?\b", "Water intrusion or leak language found."),
    ("roof", r"\broof\b.*\b(damage|leak|repair|replace)\b", "Roof repair or replacement language found."),
    ("foundation", r"\bfoundation\b.*\b(crack|movement|settlement|repair)\b", "Foundation issue language found."),
    ("hvac", r"\b(?:hvac|furnace|boiler|air condition|a/c|ac unit)\b.*\b(repair|replace|defect|old)\b", "HVAC issue language found."),
    ("plumbing", r"\bplumbing\b.*\b(leak|repair|replace|defect)\b", "Plumbing issue language found."),
    ("electrical", r"\belectrical\b.*\b(hazard|repair|replace|defect)\b", "Electrical issue language found."),
    ("as_is", r"\bas[- ]is\b", "As-is language found."),
    ("prepayment_penalty", r"\bprepayment penalty\b", "Prepayment penalty language found."),
    ("liens", r"\blien[s]?\b", "Possible lien language found."),
    ("exception", r"\bexception[s]?\b", "Title exceptions language found."),
]


def _parse_money(text_value: str | None) -> float | None:
    if not text_value:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text_value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _parse_percent(text_value: str | None) -> float | None:
    if not text_value:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", text_value)
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _parse_date(text_value: str | None) -> str | None:
    if not text_value:
        return None
    raw = str(text_value).strip().rstrip(".")
    if not raw:
        return None

    formats = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _extract_text_from_txt(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _extract_text_from_docx(path: Path) -> str:
    try:
        with zipfile.ZipFile(path, "r") as zf:
            xml = zf.read("word/document.xml").decode("utf-8", errors="replace")
        text = re.sub(r"<[^>]+>", " ", xml)
        text = re.sub(r"\s+", " ", text).strip()
        return text
    except Exception:
        return ""


def _extract_text_from_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader  # type: ignore
        reader = PdfReader(str(path))
        pages = []
        for page in reader.pages[:60]:
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(pages).strip()
    except Exception:
        return ""


def _extract_text_for_preview(path: Path, content_type: str | None) -> str:
    suffix = path.suffix.lower()
    if suffix == ".txt":
        return _extract_text_from_txt(path)
    if suffix == ".docx":
        return _extract_text_from_docx(path)
    if suffix == ".pdf":
        return _extract_text_from_pdf(path)
    return ""


def _normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _search_excerpt(text_blob: str, pattern: str) -> tuple[str | None, str | None]:
    match = re.search(pattern, text_blob, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None, None
    value = _normalize_space(match.group(1)) if match.groups() else _normalize_space(match.group(0))
    start = max(0, match.start() - 120)
    end = min(len(text_blob), match.end() + 180)
    excerpt = _normalize_space(text_blob[start:end])
    return value or None, excerpt or None


def _section_slice(text_blob: str, label: str, stop_labels: list[str] | None = None, width: int = 320) -> tuple[str | None, str | None]:
    normalized = _normalize_space(text_blob)
    if not normalized:
        return None, None
    stop_labels = stop_labels or []
    pattern = re.compile(rf"\b{label}\s*[:\-]?\s*", flags=re.IGNORECASE)
    match = pattern.search(normalized)
    if not match:
        return None, None
    start = match.end()
    candidates = [len(normalized), min(len(normalized), start + width)]
    for stop in stop_labels:
        stop_match = re.search(rf"\b{stop}\s*[:\-]?", normalized[start:], flags=re.IGNORECASE)
        if stop_match:
            candidates.append(start + stop_match.start())
    end = min(candidates)
    value = normalized[start:end].strip(' -:;,.')
    excerpt_start = max(0, match.start() - 80)
    excerpt_end = min(len(normalized), end + 80)
    excerpt = normalized[excerpt_start:excerpt_end].strip()
    return value or None, excerpt or None


def _clean_person_name(value: str | None) -> str | None:
    if not value:
        return None
    text = _normalize_space(value)
    for stop in [
        'Purchase Terms', 'Seller', 'Buyer', 'Dates', 'Occupancy', 'Agents', 'Brokerage',
        'Phone', 'Email', 'Brokerage', 'Clauses', 'Signatures', 'Financing Details'
    ]:
        idx = text.lower().find(stop.lower())
        if idx > 0:
            text = text[:idx].strip(' -:;,.')
            break
    text = re.sub(r'\s{2,}', ' ', text).strip(' -:;,.')
    return text or None


def _clean_contact_blob(value: str | None) -> str | None:
    if not value:
        return None
    text = _normalize_space(value)
    for stop in ['Buyer Agent', 'Brokerage', 'Clauses', 'Signatures', 'Dates', 'Occupancy']:
        idx = text.lower().find(stop.lower())
        if idx > 0:
            text = text[:idx].strip(' -:;,.')
            break
    return text or None


def _extract_first_email(value: str | None) -> str | None:
    text = _normalize_space(value)
    if not text:
        return None
    match = re.search(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", text, flags=re.IGNORECASE)
    return match.group(1) if match else None


def _extract_first_phone(value: str | None) -> str | None:
    text = _normalize_space(value)
    if not text:
        return None
    match = re.search(r"(\+?1?[\s\-.]?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4})", text)
    return _normalize_space(match.group(1)) if match else None


def _strip_contact_noise(value: str | None) -> str | None:
    text = _normalize_space(value)
    if not text:
        return None
    text = re.sub(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\+?1?[\s\-.]?(?:\(?\d{3}\)?[\s\-.]?)\d{3}[\s\-.]?\d{4}", " ", text)
    text = re.sub(r"(?:phone|ph|cell|mobile|office|email|e-mail|contact|broker|agent|loan officer|loan originator|carrier)", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" ,;:-")
    return text or None


def _contact_parts_from_fact(field_name: str, fact: dict[str, Any]) -> dict[str, Any]:
    raw_value = fact.get("value")
    raw_text = _normalize_space(raw_value if isinstance(raw_value, str) else None)
    role = CONTACT_FIELD_ROLES.get(field_name)
    email = _extract_first_email(raw_text)
    phone = _extract_first_phone(raw_text)
    cleaned = _strip_contact_noise(raw_text)

    name: str | None = None
    company: str | None = None
    if field_name in {"listing_agent_name", "buyer_agent_name", "lender_name", "title_company", "escrow_officer", "carrier_name"}:
        name = _clean_person_name(cleaned) if field_name.endswith("_name") or field_name == "escrow_officer" else cleaned
    elif field_name in {"loan_officer_contact", "insurance_agent_contact"}:
        if cleaned and " - " in cleaned:
            left, right = [part.strip() for part in cleaned.split(" - ", 1)]
            name = left or None
            company = right or None
        elif cleaned and "," in cleaned:
            left, right = [part.strip() for part in cleaned.split(",", 1)]
            name = left or None
            company = right or None
        else:
            name = cleaned
    elif field_name == "brokerage_contacts":
        company = cleaned
        name = cleaned

    if role == "insurance_agency":
        company = company or cleaned or raw_text
        name = name or company
    elif role == "listing_office":
        company = company or cleaned or raw_text
        name = name or company
    elif role == "lender":
        company = company or cleaned or raw_text
        name = name or company
    elif role == "title_company":
        company = company or cleaned or raw_text
        name = name or company

    return {
        "role": role,
        "name": name or raw_text,
        "company": company,
        "email": email,
        "phone": phone,
        "source_field": field_name,
        "confidence": fact.get("confidence"),
        "excerpt": fact.get("excerpt"),
    }


def _guess_document_kind(text_blob: str, filename: str | None, explicit_kind: str | None) -> str:
    explicit = str(explicit_kind or "").strip().lower()
    if explicit in SUPPORTED_DOCUMENT_KINDS:
        return explicit
    lower = f"{filename or ''} {text_blob[:2000]}".lower()
    if "purchase agreement" in lower or "sales contract" in lower:
        return "purchase_agreement"
    if "closing disclosure" in lower:
        return "closing_disclosure"
    if "loan estimate" in lower:
        return "loan_estimate"
    if "inspection report" in lower or "inspection summary" in lower:
        return "inspection_report"
    if "insurance binder" in lower or "declarations" in lower:
        return "insurance_binder"
    if "title commitment" in lower or "title company" in lower or "escrow officer" in lower:
        return "title_documents"
    if "lender" in lower or "loan amount" in lower or "mortgage" in lower:
        return "loan_documents"
    return "inspection_report"


def _add_fact(facts: dict[str, Any], field_name: str, value: Any, excerpt: str | None, confidence: float, value_type: str | None = None) -> None:
    if value in (None, "", []):
        return
    facts[field_name] = {
        "field_name": field_name,
        "label": FIELD_LABELS.get(field_name, field_name.replace("_", " ").title()),
        "value": value,
        "excerpt": excerpt,
        "confidence": round(float(confidence), 4),
        "value_type": value_type or ("number" if isinstance(value, (int, float)) else "text"),
    }


def _extract_text_field(text_blob: str, patterns: list[str], confidence: float = 0.84) -> tuple[str | None, str | None, float | None]:
    for pattern in patterns:
        value, excerpt = _search_excerpt(text_blob, pattern)
        if value:
            return value, excerpt, confidence
    return None, None, None


def _extract_money_field(text_blob: str, patterns: list[str], confidence: float = 0.9) -> tuple[float | None, str | None, float | None]:
    for pattern in patterns:
        value, excerpt = _search_excerpt(text_blob, pattern)
        parsed = _parse_money(value)
        if parsed is not None:
            return parsed, excerpt, confidence
    return None, None, None


def _extract_percent_field(text_blob: str, patterns: list[str], confidence: float = 0.9) -> tuple[float | None, str | None, float | None]:
    for pattern in patterns:
        value, excerpt = _search_excerpt(text_blob, pattern)
        parsed = _parse_percent(value)
        if parsed is not None:
            return parsed, excerpt, confidence
    return None, None, None


def _extract_date_field(text_blob: str, patterns: list[str], confidence: float = 0.88) -> tuple[str | None, str | None, float | None]:
    for pattern in patterns:
        value, excerpt = _search_excerpt(text_blob, pattern)
        parsed = _parse_date(value)
        if parsed:
            return parsed, excerpt, confidence
    return None, None, None


def _collect_clause_hits(text_blob: str, patterns: list[str]) -> list[str]:
    hits = []
    normalized = _normalize_space(text_blob)
    for pattern in patterns:
        for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
            start = max(0, match.start() - 80)
            end = min(len(normalized), match.end() + 120)
            excerpt = normalized[start:end]
            if excerpt and excerpt not in hits:
                hits.append(excerpt)
    return hits[:8]


def _extract_purchase_agreement(text_blob: str) -> dict[str, Any]:
    facts: dict[str, Any] = {}

    for field, patterns in {
        "purchase_price": [r"(?:purchase|sales|contract)\s+price\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "earnest_money": [r"earnest\s+money(?:\s+deposit)?\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)", r"\bemd\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "seller_credits": [r"seller\s+credit(?:s)?\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
    }.items():
        value, excerpt, confidence = _extract_money_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.9, "number")

    buyer_value, buyer_excerpt = _section_slice(text_blob, "Buyer", ["Seller", "Purchase Terms", "Property Address", "Dates", "Occupancy", "Agents", "Brokerage", "Signatures"])
    _add_fact(facts, "buyer_name", _clean_person_name(buyer_value), buyer_excerpt, 0.9, "text")

    seller_value, seller_excerpt = _section_slice(text_blob, "Seller", ["Purchase Terms", "Dates", "Occupancy", "Agents", "Brokerage", "Signatures"])
    _add_fact(facts, "seller_name", _clean_person_name(seller_value), seller_excerpt, 0.9, "text")

    list_agent_value, list_agent_excerpt = _section_slice(text_blob, "Listing Agent", ["Phone", "Email", "Brokerage", "Buyer Agent", "Clauses", "Signatures"])
    _add_fact(facts, "listing_agent_name", _clean_person_name(list_agent_value), list_agent_excerpt, 0.88, "text")

    buyer_agent_value, buyer_agent_excerpt = _section_slice(text_blob, "Buyer Agent", ["Phone", "Email", "Brokerage", "Clauses", "Signatures"])
    _add_fact(facts, "buyer_agent_name", _clean_person_name(buyer_agent_value), buyer_agent_excerpt, 0.88, "text")

    for field, label, stops, conf in [
        ("financing_contingency", "Financing Contingency", ["Inspection Contingency", "Appraisal Gap", "Seller Credits", "Dates", "Occupancy"], 0.86),
        ("inspection_contingency", "Inspection Contingency", ["Appraisal Gap", "Seller Credits", "Dates", "Occupancy"], 0.86),
        ("appraisal_gap_terms", "Appraisal Gap", ["Seller Credits", "Dates", "Occupancy", "Agents"], 0.86),
        ("occupancy_possession_terms", "Occupancy", ["Agents", "Brokerage", "Clauses", "Signatures"], 0.84),
        ("listing_agent_contact", "Listing Agent", ["Buyer Agent", "Clauses", "Signatures"], 0.82),
        ("buyer_agent_contact", "Buyer Agent", ["Clauses", "Signatures"], 0.82),
        ("brokerage_contacts", "Brokerage", ["Clauses", "Signatures"], 0.8),
    ]:
        value, excerpt = _section_slice(text_blob, label, stops)
        if field.endswith('_contact') or field == 'brokerage_contacts':
            value = _clean_contact_blob(value)
        _add_fact(facts, field, value, excerpt, conf, "text")

    for field, patterns in {
        "inspection_contingency_date": [r"inspection\s+deadline\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})", r"inspection\s+contingency(?:\s+deadline|\s+date)?\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
        "financing_contingency_date": [r"financing\s+deadline\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})", r"financing\s+contingency(?:\s+deadline|\s+date)?\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
        "earnest_money_deadline": [r"earnest\s+money\s+deadline\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
        "appraisal_deadline": [r"appraisal(?:\s+contingency)?(?:\s+deadline|\s+date)?\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
        "target_close_date": [r"(?:target\s+)?closing\s+date\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})", r"close\s+of\s+escrow\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
    }.items():
        value, excerpt, confidence = _extract_date_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.88, "date")

    clauses = _collect_clause_hits(
        text_blob,
        [r"\bas[- ]is\b", r"seller\s+credit", r"appraisal\s+gap", r"non-refundable", r"occupancy", r"possession"],
    )
    if clauses:
        _add_fact(facts, "notable_clauses", clauses, clauses[0], 0.78, "list")
    return facts


def _extract_loan_estimate(text_blob: str) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for field, patterns in {
        "lender_name": [r"(?:lender|mortgage\s+company)\s*[:\-]?\s*([A-Za-z0-9&.,'()\- ]{3,120})"],
        "loan_officer_contact": [r"(?:loan officer|loan originator)\s*[:\-]?\s*([^\n]{5,180})"],
        "loan_type": [r"\b(DSCR|Conventional|FHA|VA|USDA|Cash|Hard Money|Bridge|ARM|Fixed)\b"],
        "prepayment_notes": [r"(prepayment[^\n.]{0,220})"],
        "lock_status": [r"(rate\s+lock[^\n.]{0,140})"],
    }.items():
        value, excerpt, confidence = _extract_text_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.85)

    for field, patterns in {
        "interest_rate": [r"interest\s+rate\s*[:\-]?\s*([0-9.]+\s*%?)"],
    }.items():
        value, excerpt, confidence = _extract_percent_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.9, "number")

    for field, patterns in {
        "monthly_payment_estimate": [r"(?:estimated\s+)?monthly\s+payment\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "cash_to_close": [r"(?:estimated\s+)?cash\s+to\s+close\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "points_fees": [r"(?:points|origination charges|loan costs)\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "loan_amount": [r"loan\s+amount\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)", r"principal\s+amount\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
    }.items():
        value, excerpt, confidence = _extract_money_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.9, "number")

    for field, patterns in {
        "lock_date": [r"lock(?:ed)?\s+(?:through|date)\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
    }.items():
        value, excerpt, confidence = _extract_date_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.88, "date")
    return facts


def _extract_closing_disclosure(text_blob: str) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for field, patterns in {
        "lender_name": [r"(?:lender|mortgage\s+company)\s*[:\-]?\s*([A-Za-z0-9&.,'()\- ]{3,120})"],
        "title_parties": [r"(?:settlement agent|title company|escrow)\s*[:\-]?\s*([^\n]{5,180})"],
        "fee_breakdown": [r"(closing costs[^\n.]{0,240})", r"(cash to close[^\n.]{0,240})"],
        "final_wire_items": [r"(wire[^\n.]{0,240})"],
    }.items():
        value, excerpt, confidence = _extract_text_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.84)
    for field, patterns in {
        "cash_to_close": [r"cash\s+to\s+close\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "seller_credits": [r"seller\s+credit(?:s)?\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "closing_costs": [r"closing\s+costs\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
    }.items():
        value, excerpt, confidence = _extract_money_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.9, "number")
    for field, patterns in {
        "closing_date": [r"closing\s+date\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"],
    }.items():
        value, excerpt, confidence = _extract_date_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.88, "date")
    return facts


def _extract_insurance_binder(text_blob: str) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for field, patterns in {
        "carrier_name": [r"(?:carrier|insurance company)\s*[:\-]?\s*([A-Za-z0-9&.,'()\- ]{3,120})"],
        "insurance_agent_contact": [r"(?:agent|broker)\s*[:\-]?\s*([^\n]{5,180})"],
    }.items():
        value, excerpt, confidence = _extract_text_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.84)
    for field, patterns in {
        "annual_premium": [r"(?:annual\s+premium|premium)\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "coverage_dwelling": [r"(?:dwelling|coverage\s+a)\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "coverage_liability": [r"liability\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
        "coverage_deductible": [r"deductible\s*[:\-]?\s*\$?([0-9,]+(?:\.[0-9]{2})?)"],
    }.items():
        value, excerpt, confidence = _extract_money_field(text_blob, patterns)
        _add_fact(facts, field, value, excerpt, confidence or 0.88, "number")
    value, excerpt, confidence = _extract_date_field(text_blob, [r"(?:effective\s+date|policy\s+effective)\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"])
    _add_fact(facts, "coverage_effective_date", value, excerpt, confidence or 0.88, "date")
    return facts


def _extract_inspection_report(text_blob: str) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for field, patterns in {
        "major_defects": [r"(major defects?[^\n.]{0,260})", r"(defects? observed[^\n.]{0,260})"],
        "safety_issues": [r"(safety issues?[^\n.]{0,260})", r"(hazard[^\n.]{0,260})"],
        "system_flags": [r"((?:roof|foundation|hvac|plumbing|electrical|mold|water)[^\n.]{0,260})"],
        "recommended_repairs": [r"(recommended repairs?[^\n.]{0,260})", r"(repair recommendation[^\n.]{0,260})"],
        "negotiation_leverage_items": [r"(credit[^\n.]{0,220})", r"(repair concession[^\n.]{0,220})"],
    }.items():
        value, excerpt, confidence = _extract_text_field(text_blob, patterns, 0.8)
        _add_fact(facts, field, value, excerpt, confidence or 0.8)
    return facts


def _extract_title_documents(text_blob: str) -> dict[str, Any]:
    facts: dict[str, Any] = {}
    for field, patterns in {
        "title_company": [r"title\s+company\s*[:\-]?\s*([A-Za-z0-9&.,'()\- ]{3,120})"],
        "escrow_officer": [r"(?:escrow officer|closing officer)\s*[:\-]?\s*([A-Za-z0-9&.,'()\- ]{3,120})"],
        "title_defects": [r"(title defect[^\n.]{0,260})"],
        "liens": [r"(lien[^\n.]{0,260})"],
        "exceptions": [r"(exception[^\n.]{0,260})"],
        "unresolved_objections": [r"(unresolved objection[^\n.]{0,260})", r"(objection[^\n.]{0,260})"],
    }.items():
        value, excerpt, confidence = _extract_text_field(text_blob, patterns, 0.83)
        _add_fact(facts, field, value, excerpt, confidence or 0.83)
    value, excerpt, confidence = _extract_date_field(text_blob, [r"title\s+objection(?:\s+deadline|\s+date)?\s*[:\-]?\s*([A-Za-z0-9,\/\- ]{6,40})"], 0.88)
    _add_fact(facts, "title_objection_deadline", value, excerpt, confidence or 0.88, "date")
    return facts


def _extract_facts_for_kind(document_kind: str, text_blob: str) -> dict[str, Any]:
    if document_kind == "purchase_agreement":
        return _extract_purchase_agreement(text_blob)
    if document_kind in {"loan_estimate", "loan_documents"}:
        return _extract_loan_estimate(text_blob)
    if document_kind == "closing_disclosure":
        return _extract_closing_disclosure(text_blob)
    if document_kind == "insurance_binder":
        return _extract_insurance_binder(text_blob)
    if document_kind == "inspection_report":
        return _extract_inspection_report(text_blob)
    if document_kind == "title_documents":
        return _extract_title_documents(text_blob)
    return {}


def _recommended_next_actions(document_kind: str, fact_map: dict[str, Any]) -> list[str]:
    if document_kind == "purchase_agreement":
        return [
            "Confirm offer terms against the live acquisition record.",
            "Calendar inspection, financing, appraisal, and close deadlines.",
            "Send executed agreement to title and lender.",
            "Verify seller credits and appraisal-gap language.",
            "Review occupancy and possession language before final acceptance.",
        ]
    if document_kind in {"loan_estimate", "loan_documents"}:
        return [
            "Confirm lock status and expiration.",
            "Compare fees and points to underwriting expectations.",
            "Verify loan amount and monthly payment against underwriting.",
            "Flag any material change in cash to close.",
        ]
    if document_kind == "closing_disclosure":
        return [
            "Review final wire and cash-to-close numbers with the buyer.",
            "Compare closing disclosure against the loan estimate.",
            "Confirm title, lender, and insurance parties are complete.",
        ]
    if document_kind == "insurance_binder":
        return [
            "Verify lender coverage requirements are met.",
            "Confirm the effective date protects the closing window.",
            "Check dwelling, liability, and deductible amounts against underwriting.",
        ]
    if document_kind == "inspection_report":
        return [
            "Decide whether to proceed, renegotiate, or terminate under inspection contingency.",
            "Request credits or price reduction for major defects.",
            "Collect contractor quotes for high-severity repairs.",
        ]
    if document_kind == "title_documents":
        return [
            "Clear title defects, liens, and objection items.",
            "Confirm escrow and title contacts are correct.",
            "Calendar title objection deadlines and cure follow-up.",
        ]
    return []


def _who_to_contact_next(document_kind: str, fact_map: dict[str, Any]) -> list[dict[str, Any]]:
    contacts: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for field_name, fact in fact_map.items():
        role = CONTACT_FIELD_ROLES.get(field_name)
        if not role or not fact.get("value"):
            continue
        payload = _contact_parts_from_fact(field_name, fact)
        dedupe_key = (str(payload.get("role") or ""), str(payload.get("name") or "").strip().lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        contacts.append(payload)
    if contacts:
        return contacts
    fallback_role = {
        "purchase_agreement": "listing_agent",
        "loan_estimate": "lender",
        "loan_documents": "lender",
        "closing_disclosure": "title_company",
        "insurance_binder": "insurance_agent",
        "inspection_report": "inspection_company",
        "title_documents": "title_company",
    }.get(document_kind)
    return [{"role": fallback_role, "name": None, "company": None, "email": None, "phone": None, "source_field": None, "confidence": 0.25, "excerpt": None}] if fallback_role else []


def _deadline_candidates(fact_map: dict[str, Any]) -> list[dict[str, Any]]:
    items = []
    for field_name, fact in fact_map.items():
        if field_name not in DEADLINE_FIELDS:
            continue
        value = fact.get("value")
        if not value:
            continue
        items.append({
            "field_name": field_name,
            "label": FIELD_LABELS.get(field_name, field_name.replace("_", " ").title()),
            "date": value,
            "confidence": fact.get("confidence"),
            "excerpt": fact.get("excerpt"),
        })
    return items


def _risk_flags(text_blob: str, document_kind: str, facts: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = _normalize_space(text_blob)
    flags = []
    seen = set()
    for code, pattern, label in RISK_PATTERNS:
        match = re.search(pattern, normalized, flags=re.IGNORECASE)
        if not match:
            continue
        start = max(0, match.start() - 90)
        end = min(len(normalized), match.end() + 130)
        excerpt = normalized[start:end]
        key = (code, excerpt.lower())
        if key in seen:
            continue
        seen.add(key)
        flags.append({"code": code, "label": label, "severity": "warning", "excerpt": excerpt})
    if document_kind == "inspection_report" and not flags and facts.get("major_defects"):
        flags.append({"code": "inspection_issue", "label": "Inspection issues were extracted.", "severity": "warning", "excerpt": str(facts['major_defects'].get('excerpt') or '')})
    return flags[:10]


def parse_document(path: Path, content_type: str | None, document_kind: str | None = None, filename: str | None = None) -> dict[str, Any]:
    preview_text = _extract_text_for_preview(path, content_type)
    normalized_kind = _guess_document_kind(preview_text, filename or path.name, document_kind)
    fact_map = _extract_facts_for_kind(normalized_kind, preview_text)

    parser_version = os.getenv("ACQUISITION_PARSER_VERSION", "operator_v3")
    recommended_next_actions = _recommended_next_actions(normalized_kind, fact_map)
    who_to_contact_next = _who_to_contact_next(normalized_kind, fact_map)
    deadline_candidates = _deadline_candidates(fact_map)
    risk_flags = _risk_flags(preview_text, normalized_kind, fact_map)

    flat_fields = {key: value.get("value") for key, value in fact_map.items() if value.get("value") not in (None, "", [])}
    parse_status = "parsed" if preview_text or flat_fields else "no_text"

    return {
        # new keys
        "parse_status": parse_status,
        "parser_version": parser_version,
        "normalized_document_type": normalized_kind,
        "preview_text": preview_text[:12000] if preview_text else None,
        "extracted_text": preview_text[:50000] if preview_text else None,
        "facts": fact_map,
        "recommended_next_actions": recommended_next_actions,
        "who_to_contact_next": who_to_contact_next,
        "deadline_candidates": deadline_candidates,
        "risk_flags": risk_flags,
        "mismatch_indicators": [],
        "warnings": [flag.get("label") for flag in risk_flags],
        "extracted_fields": flat_fields,
        # compatibility keys expected elsewhere
        "status": parse_status,
        "text": preview_text[:50000] if preview_text else None,
        "fields": flat_fields,
    }
