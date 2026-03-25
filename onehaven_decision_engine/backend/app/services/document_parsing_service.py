from __future__ import annotations

import os
import re
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any


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


def _parse_date(text_value: str | None) -> str | None:
    if not text_value:
        return None
    raw = str(text_value).strip()
    if not raw:
        return None

    formats = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%B %d, %Y",
        "%b %d, %Y",
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
    """
    Lightweight fallback parser:
    - tries pypdf if available
    - else returns empty
    """
    try:
        from pypdf import PdfReader  # type: ignore

        reader = PdfReader(str(path))
        pages = []
        for page in reader.pages[:50]:
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


def extract_acquisition_fields(text_blob: str | None) -> dict[str, Any]:
    text_blob = (text_blob or "").strip()
    if not text_blob:
        return {}

    def find_money(patterns: list[str]) -> float | None:
        for pattern in patterns:
            m = re.search(pattern, text_blob, flags=re.IGNORECASE)
            if m:
                return _parse_money(m.group(1))
        return None

    def find_text(patterns: list[str]) -> str | None:
        for pattern in patterns:
            m = re.search(pattern, text_blob, flags=re.IGNORECASE)
            if m:
                value = m.group(1).strip()
                return value or None
        return None

    def find_date(patterns: list[str]) -> str | None:
        for pattern in patterns:
            m = re.search(pattern, text_blob, flags=re.IGNORECASE)
            if m:
                value = _parse_date(m.group(1))
                if value:
                    return value
        return None

    fields = {
        "purchase_price": find_money([
            r"purchase price[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
            r"sales price[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
            r"contract price[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
        ]),
        "earnest_money": find_money([
            r"earnest money(?: deposit)?[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
            r"emd[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
        ]),
        "loan_amount": find_money([
            r"loan amount[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
            r"principal amount[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
        ]),
        "cash_to_close": find_money([
            r"cash to close[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
            r"total cash to close[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
        ]),
        "closing_costs": find_money([
            r"closing costs[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
            r"estimated closing costs[:\s]+\$?([0-9,]+(?:\.[0-9]{2})?)",
        ]),
        "loan_type": find_text([
            r"\b(DSCR|Conventional|FHA|VA|Cash|Hard Money|Bridge)\b",
        ]),
        "lender_name": find_text([
            r"(?:lender|mortgage company)[:\s]+([A-Za-z0-9&.,'()\- ]+)",
        ]),
        "title_company": find_text([
            r"(?:title company|escrow company)[:\s]+([A-Za-z0-9&.,'()\- ]+)",
        ]),
        "escrow_officer": find_text([
            r"(?:escrow officer|closing officer)[:\s]+([A-Za-z0-9&.,'()\- ]+)",
        ]),
        "inspection_contingency_date": find_date([
            r"inspection contingency(?: deadline| date)?[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "financing_contingency_date": find_date([
            r"financing contingency(?: deadline| date)?[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "appraisal_deadline": find_date([
            r"appraisal(?: contingency)?(?: deadline| date)?[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "earnest_money_deadline": find_date([
            r"earnest money(?: deposit)?(?: due| deadline| date)?[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "title_objection_deadline": find_date([
            r"title objection(?: deadline| date)?[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "insurance_due_date": find_date([
            r"insurance(?: binder)?(?: due| deadline| date)?[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "walkthrough_datetime": find_date([
            r"walkthrough(?: date| deadline)?[:\s]+([A-Za-z0-9,/\- ]+)",
            r"final walkthrough[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
        "closing_datetime": find_date([
            r"closing(?: date| datetime)?[:\s]+([A-Za-z0-9,/\- ]+)",
            r"close of escrow[:\s]+([A-Za-z0-9,/\- ]+)",
        ]),
    }

    return {k: v for k, v in fields.items() if v not in (None, "", [])}


def parse_document(path: Path, content_type: str | None) -> dict[str, Any]:
    preview_text = _extract_text_for_preview(path, content_type)
    extracted_fields = extract_acquisition_fields(preview_text)

    parser_version = os.getenv("ACQUISITION_PARSER_VERSION", "v1")

    return {
        "parse_status": "parsed" if preview_text or extracted_fields else "no_text",
        "parser_version": parser_version,
        "preview_text": preview_text[:12000] if preview_text else None,
        "extracted_text": preview_text[:50000] if preview_text else None,
        "extracted_fields": extracted_fields,
    }
