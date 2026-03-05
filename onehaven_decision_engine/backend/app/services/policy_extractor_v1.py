# backend/app/services/policy_extractor_v1.py
from __future__ import annotations

import json
import os
import re
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.policy_models import PolicyAssertion, PolicySource


def _dumps(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _read_text_from_source(src: PolicySource, max_bytes: int = 800_000) -> str:
    """
    Minimal reader:
      - if html/txt/json: read and return as text
      - if pdf: DO NOT try to OCR here (keep pipeline simple & deterministic)
        return empty string; you can add PDF text extraction later.
    """
    path = (src.raw_path or "").strip()
    if not path or not os.path.exists(path):
        return ""

    ct = (src.content_type or "").lower()
    if "pdf" in ct:
        return ""

    with open(path, "rb") as f:
        b = f.read(max_bytes)

    # try UTF-8; fall back
    try:
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return b.decode(errors="ignore")


_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _html_to_text(s: str) -> str:
    s = _TAG_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s)
    return s.strip()


def extract_assertions_v1(
    db: Session,
    *,
    org_id: Optional[int],
    source_id: int,
) -> list[PolicyAssertion]:
    """
    Heuristic extractor v1:
      - creates a summary assertion
      - flags likely “registration / inspection / certificate” keywords as *candidates*
    """
    src = db.get(PolicySource, source_id)
    if not src:
        raise ValueError("source_id not found")

    raw = _read_text_from_source(src)
    text = _html_to_text(raw) if raw else ""
    lower = text.lower()

    out: list[PolicyAssertion] = []

    # 1) Always create a traceable summary assertion (useful in UI review)
    summary = (text[:800] + "…") if len(text) > 800 else text
    out.append(
        PolicyAssertion(
            org_id=org_id,
            source_id=src.id,
            state=src.state,
            county=src.county,
            city=src.city,
            pha_name=src.pha_name,
            program_type=src.program_type,
            rule_key="source_summary",
            value_json=_dumps(
                {
                    "publisher": src.publisher,
                    "title": src.title,
                    "url": src.url,
                    "content_type": src.content_type,
                    "summary_excerpt": summary,
                }
            ),
            confidence=0.2,
            review_status="extracted",
            review_notes="Auto-created summary. Verify rules manually.",
        )
    )

    # 2) Candidate flags (LOW confidence)
    def add_flag(rule_key: str, why: str):
        out.append(
            PolicyAssertion(
                org_id=org_id,
                source_id=src.id,
                state=src.state,
                county=src.county,
                city=src.city,
                pha_name=src.pha_name,
                program_type=src.program_type,
                rule_key=rule_key,
                value_json=_dumps({"candidate": True, "why": why}),
                confidence=0.15,
                review_status="extracted",
                review_notes="Heuristic keyword match only. Requires verification.",
            )
        )

    if lower:
        if "rental registration" in lower or ("register" in lower and "rental" in lower):
            add_flag("rental_registration_candidate", "keyword match: rental registration")
        if "certificate of compliance" in lower or ("certificate" in lower and "rental" in lower):
            add_flag("certificate_required_candidate", "keyword match: certificate")
        if "inspection" in lower:
            add_flag("inspection_program_candidate", "keyword match: inspection")

    db.add_all(out)
    db.commit()
    for a in out:
        db.refresh(a)
    return out