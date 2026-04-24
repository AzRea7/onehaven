#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

FILES = {
    "onehaven_platform/backend/src/services/trust/__init__.py": '''from .trust_gate_service import evaluate_trust_gate

__all__ = ["evaluate_trust_gate"]
''',

    "onehaven_platform/backend/src/services/trust/types.py": '''from __future__ import annotations

from typing import Literal, TypedDict

TrustStatus = Literal["SAFE", "WARNING", "BLOCKED"]


class TrustGateResult(TypedDict, total=False):
    status: TrustStatus
    safe_for_projection: bool
    safe_for_user_reliance: bool
    blocked_reason: str | None
    confidence: str
    reasons: list[str]
    required_actions: list[str]
    financial_impact: dict
''',

    "onehaven_platform/backend/src/services/trust/completeness_service.py": '''from __future__ import annotations

from typing import Any


def evaluate_completeness(payload: dict[str, Any]) -> dict[str, Any]:
    missing = payload.get("missing_required_categories") or []
    score = payload.get("completeness_score")

    return {
        "passed": not missing and (score is None or score >= 0.85),
        "score": score,
        "missing": missing,
        "reason": "missing_required_categories" if missing else None,
    }
''',

    "onehaven_platform/backend/src/services/trust/authority_service.py": '''from __future__ import annotations

from typing import Any


def evaluate_authority(payload: dict[str, Any]) -> dict[str, Any]:
    missing_binding = payload.get("missing_binding_authority") or []
    weak_sources = payload.get("weak_authority_categories") or []

    return {
        "passed": not missing_binding,
        "missing_binding_authority": missing_binding,
        "weak_authority_categories": weak_sources,
        "reason": "missing_binding_authority" if missing_binding else None,
    }
''',

    "onehaven_platform/backend/src/services/trust/freshness_service.py": '''from __future__ import annotations

from typing import Any


def evaluate_freshness(payload: dict[str, Any]) -> dict[str, Any]:
    stale = payload.get("stale_authoritative_sources") or []
    overdue = payload.get("overdue_refresh_categories") or []

    return {
        "passed": not stale,
        "stale_authoritative_sources": stale,
        "overdue_refresh_categories": overdue,
        "reason": "stale_authoritative_sources" if stale else None,
    }
''',

    "onehaven_platform/backend/src/services/trust/trust_gate_service.py": '''from __future__ import annotations

from typing import Any

from .authority_service import evaluate_authority
from .completeness_service import evaluate_completeness
from .freshness_service import evaluate_freshness
from .types import TrustGateResult


def evaluate_trust_gate(payload: dict[str, Any]) -> TrustGateResult:
    completeness = evaluate_completeness(payload)
    authority = evaluate_authority(payload)
    freshness = evaluate_freshness(payload)

    reasons: list[str] = []
    required_actions: list[str] = []

    for check in [completeness, authority, freshness]:
        if not check.get("passed") and check.get("reason"):
            reasons.append(str(check["reason"]))

    if "missing_required_categories" in reasons:
        required_actions.append("Complete required jurisdiction category coverage.")
    if "missing_binding_authority" in reasons:
        required_actions.append("Attach binding authoritative source evidence.")
    if "stale_authoritative_sources" in reasons:
        required_actions.append("Refresh stale authoritative sources.")

    blocked = bool(reasons)

    return {
        "status": "BLOCKED" if blocked else "SAFE",
        "safe_for_projection": not blocked,
        "safe_for_user_reliance": not blocked,
        "blocked_reason": reasons[0] if reasons else None,
        "confidence": "low" if blocked else "high",
        "reasons": reasons,
        "required_actions": required_actions,
        "financial_impact": payload.get("financial_impact") or {},
    }
''',
}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    written = 0
    skipped = 0

    for rel, content in FILES.items():
        path = root / rel
        if path.exists() and not args.overwrite:
            skipped += 1
            continue

        if args.dry_run:
            print("[DRY RUN] write", path)
            written += 1
            continue

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        written += 1

    print("Phase 94 complete.")
    print({"written": written, "skipped": skipped, "dry_run": args.dry_run})

if __name__ == "__main__":
    main()