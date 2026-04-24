from __future__ import annotations

import hashlib
import json
from typing import Any


def fingerprint(*parts: Any) -> str:
    """
    Stable, deterministic fingerprint to dedupe imported rows.
    Accepts any JSON-serializable content, plus primitives.
    """
    blob = json.dumps(parts, sort_keys=True, default=str, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()
