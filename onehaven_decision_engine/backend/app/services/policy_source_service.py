# backend/app/services/policy_source_service.py
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import httpx
from sqlalchemy.orm import Session

from app.policy_models import PolicySource


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v if v else None


def _norm_state(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    v = s.strip().upper()
    return v or None


def _norm_city(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_county(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _safe_text_from_html(html: str, max_len: int = 20_000) -> str:
    """
    Very lightweight HTML -> text.
    Not perfect. We rely on human review for anything important.
    """
    # drop scripts/styles
    html = re.sub(r"(?is)<script.*?>.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?>.*?</style>", " ", html)
    # tags -> spaces
    text = re.sub(r"(?is)<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


@dataclass
class CollectResult:
    source: PolicySource
    changed: bool  # content hash changed vs existing row (if you collected same URL before)


def collect_url(
    db: Session,
    *,
    org_id: Optional[int],
    url: str,
    state: Optional[str] = "MI",
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
    publisher: Optional[str] = None,
    title: Optional[str] = None,
    notes: Optional[str] = None,
    timeout_s: float = 20.0,
) -> CollectResult:
    """
    Fetch a URL, store raw artifact to /app/policy_raw/YYYY-MM-DD/{id}.{ext},
    store hash + metadata in PolicySource.

    This is your evidence capture step.
    """
    st = _norm_state(state)
    cnty = _norm_county(county)
    cty = _norm_city(city)

    url = url.strip()

    with httpx.Client(follow_redirects=True, timeout=timeout_s, headers={"User-Agent": "OneHavenPolicyCollector/1.0"}) as client:
        resp = client.get(url)

    content_type = (resp.headers.get("content-type") or "").split(";")[0].strip() or None
    status = int(resp.status_code)
    body = resp.content or b""

    digest = _sha256(body) if body else None

    # Create new row every collect? For now: upsert by URL+org_id scope.
    # This keeps latest version per URL; you can later add PolicySourceVersion if needed.
    existing = (
        db.query(PolicySource)
        .filter(PolicySource.org_id.is_(org_id) if org_id is None else PolicySource.org_id == org_id)
        .filter(PolicySource.url == url)
        .first()
    )

    changed = False
    now = datetime.utcnow()

    if existing is None:
        row = PolicySource(
            org_id=org_id,
            state=st,
            county=cnty,
            city=cty,
            pha_name=_norm(pha_name),
            program_type=_norm(program_type),
            publisher=_norm(publisher),
            title=_norm(title),
            url=url,
            content_type=content_type,
            http_status=status,
            retrieved_at=now,
            content_sha256=digest,
            notes=_norm(notes),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
    else:
        row = existing
        changed = (row.content_sha256 != digest) and (digest is not None)
        row.state = st
        row.county = cnty
        row.city = cty
        row.pha_name = _norm(pha_name)
        row.program_type = _norm(program_type)
        row.publisher = _norm(publisher) or row.publisher
        row.title = _norm(title) or row.title
        row.content_type = content_type
        row.http_status = status
        row.retrieved_at = now
        row.content_sha256 = digest
        row.notes = _norm(notes) or row.notes
        db.commit()
        db.refresh(row)

    # Write raw file
    day = now.strftime("%Y-%m-%d")
    base_dir = f"/app/policy_raw/{day}"
    os.makedirs(base_dir, exist_ok=True)

    ext = "bin"
    if content_type:
        if "pdf" in content_type:
            ext = "pdf"
        elif "html" in content_type:
            ext = "html"
        elif "json" in content_type:
            ext = "json"
        elif "text" in content_type:
            ext = "txt"

    raw_path = f"{base_dir}/{row.id}.{ext}"
    try:
        with open(raw_path, "wb") as f:
            f.write(body)
        row.raw_path = raw_path
    except Exception:
        # don't fail the request if filesystem write fails
        row.raw_path = None

    # Best-effort extracted text for HTML/text
    extracted_text: Optional[str] = None
    try:
        if content_type and ("html" in content_type):
            extracted_text = _safe_text_from_html(resp.text)
        elif content_type and content_type.startswith("text/"):
            extracted_text = (resp.text or "")[:20_000]
    except Exception:
        extracted_text = None

    row.extracted_text = extracted_text
    db.commit()
    db.refresh(row)

    return CollectResult(source=row, changed=changed)