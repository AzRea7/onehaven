# backend/app/services/zillow_photo_source.py
from __future__ import annotations

import json
import re
from typing import Any

IMAGE_EXT_RE = re.compile(r"\.(jpg|jpeg|png|webp)(?:\?|#|$)", re.IGNORECASE)
ZILLOW_HOST_RE = re.compile(r"zillow(static)?\.com", re.IGNORECASE)


def _is_image_url(value: str) -> bool:
    v = (value or "").strip()
    if not v.startswith("http://") and not v.startswith("https://"):
        return False
    if not IMAGE_EXT_RE.search(v):
        return False
    return True


def _looks_like_zillow_image(value: str) -> bool:
    v = (value or "").strip()
    return bool(ZILLOW_HOST_RE.search(v)) and _is_image_url(v)


def _walk_collect(obj: Any, out: list[str]) -> None:
    if obj is None:
        return

    if isinstance(obj, str):
        if _looks_like_zillow_image(obj):
            out.append(obj.strip())
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            key = str(k).lower()
            if isinstance(v, str) and key in {"url", "src", "image", "photo", "href"}:
                if _looks_like_zillow_image(v):
                    out.append(v.strip())
            else:
                _walk_collect(v, out)
        return

    if isinstance(obj, list):
        for item in obj:
            _walk_collect(item, out)
        return


def dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        s = (v or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def classify_photo_kind(url: str) -> str:
    """
    Best-effort deterministic classifier:
    - Zillow URLs sometimes contain suffix hints (_p_a, _p_b, etc.) but not always enough.
    - Keep rules conservative.
    """
    u = (url or "").lower()

    if any(token in u for token in ["kitchen", "bath", "bed", "living", "interior", "inside"]):
        return "interior"
    if any(token in u for token in ["front", "backyard", "exterior", "outside", "street"]):
        return "exterior"

    # Fallback: bias unknown, not fake certainty.
    return "unknown"


def extract_zillow_photo_urls(raw_payload: dict[str, Any] | str | None) -> list[str]:
    if raw_payload is None:
        return []

    parsed: Any
    if isinstance(raw_payload, str):
        try:
            parsed = json.loads(raw_payload)
        except Exception:
            return []
    else:
        parsed = raw_payload

    found: list[str] = []
    _walk_collect(parsed, found)
    return dedupe_preserve_order(found)