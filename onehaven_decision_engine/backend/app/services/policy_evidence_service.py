# backend/app/services/policy_evidence_service.py
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import httpx
from sqlalchemy.orm import Session

from app.policy_models import PolicySource


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = s.strip()
    return v if v else None


def _norm_state(s: Optional[str]) -> Optional[str]:
    v = (s or "").strip().upper()
    return v or None


def _norm_city(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _norm_county(s: Optional[str]) -> Optional[str]:
    v = _norm(s)
    return v.lower() if v else None


def _safe_ext_from_ct(content_type: Optional[str]) -> str:
    if not content_type:
        return "bin"
    ct = content_type.lower()
    if "pdf" in ct:
        return "pdf"
    if "html" in ct:
        return "html"
    if "json" in ct:
        return "json"
    if "text" in ct:
        return "txt"
    return "bin"


def _slug_domain(url: str) -> str:
    host = urlparse(url).netloc or "unknown"
    host = host.lower()
    host = re.sub(r"[^a-z0-9\.\-]+", "-", host)
    return host[:80] if host else "unknown"


@dataclass
class CollectResult:
    source: PolicySource
    changed: bool  # if same URL previously existed with same hash, you can decide to no-op later


def collect_policy_source(
    db: Session,
    *,
    org_id: Optional[int],
    url: str,
    state: Optional[str] = None,
    county: Optional[str] = None,
    city: Optional[str] = None,
    pha_name: Optional[str] = None,
    program_type: Optional[str] = None,
    publisher: Optional[str] = None,
    title: Optional[str] = None,
    notes: Optional[str] = None,
    store_dir: str = "/app/policy_raw",
    timeout_s: float = 30.0,
) -> CollectResult:
    """
    Fetch a URL, store the raw artifact on disk, hash it, and persist a PolicySource row.
    This is the foundation for “evidence-driven policy”.
    """
    url = url.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        raise ValueError("url must start with http:// or https://")

    with httpx.Client(follow_redirects=True, timeout=timeout_s) as client:
        r = client.get(url, headers={"User-Agent": "OneHavenPolicyCollector/1.0"})
        r.raise_for_status()
        content = r.content
        content_type = r.headers.get("content-type")

    sha = hashlib.sha256(content).hexdigest()
    ext = _safe_ext_from_ct(content_type)

    domain = _slug_domain(url)
    day = datetime.utcnow().strftime("%Y-%m-%d")
    folder = os.path.join(store_dir, domain, day)
    os.makedirs(folder, exist_ok=True)

    raw_path = os.path.join(folder, f"{sha}.{ext}")
    if not os.path.exists(raw_path):
        with open(raw_path, "wb") as f:
            f.write(content)

    row = PolicySource(
        org_id=org_id,
        state=_norm_state(state),
        county=_norm_county(county),
        city=_norm_city(city),
        pha_name=_norm(pha_name),
        program_type=_norm(program_type),
        publisher=_norm(publisher),
        title=_norm(title),
        url=url,
        content_type=_norm(content_type),
        content_hash=sha,
        raw_path=raw_path,
        notes=_norm(notes),
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return CollectResult(source=row, changed=True)
