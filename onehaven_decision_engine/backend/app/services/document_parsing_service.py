from __future__ import annotations

import os
import re
import zipfile
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

    return {
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
    }


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