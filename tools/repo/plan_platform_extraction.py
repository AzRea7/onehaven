#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PRODUCT_DIRS = {
    "backend/app/products/investor_intelligence",
    "backend/app/products/acquire",
    "backend/app/products/compliance",
    "backend/app/products/tenant",
    "backend/app/products/management",
}

LIKELY_PLATFORM_DIRS = {
    "backend/app/auth.py": "platform/backend/src/identity/interfaces",
    "backend/app/config.py": "platform/backend/src/config",
    "backend/app/db.py": "platform/backend/src/db",
    "backend/app/logging_config.py": "platform/backend/src/observability",
    "backend/app/middleware": "platform/backend/src/observability",
    "backend/app/routers/auth.py": "platform/backend/src/identity/interfaces",
    "backend/app/routers/health.py": "apps/suite-api/app/api/health",
    "frontend/src/lib/auth.tsx": "platform/frontend/src/auth",
}

MANUAL_SPLIT_HINTS = [
    "services",
    "routers",
    "domain",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan platform extraction.")
    parser.add_argument("--repo-root", default=".")
    return parser.parse_args()


def main() -> None:
    repo_root = Path(parse_args().repo_root).resolve()
    legacy_root = repo_root / "onehaven_decision_engine"

    if not legacy_root.exists():
        raise SystemExit(f"Missing legacy root: {legacy_root}")

    backend_app = legacy_root / "backend" / "app"
    frontend_src = legacy_root / "frontend" / "src"

    platform_candidates = []
    manual_split = []
    untouched = []

    for path in sorted(backend_app.rglob("*")):
        rel = path.relative_to(legacy_root).as_posix()
        if any(rel.startswith(prod) for prod in PRODUCT_DIRS):
            continue
        if rel in LIKELY_PLATFORM_DIRS:
            platform_candidates.append({
                "source": rel,
                "target": LIKELY_PLATFORM_DIRS[rel],
                "reason": "explicit_platform_mapping",
            })
        elif any(part in rel for part in MANUAL_SPLIT_HINTS):
            manual_split.append({
                "source": rel,
                "reason": "shared_or_cross_cutting_needs_manual_review",
            })
        else:
            untouched.append(rel)

    for path in sorted(frontend_src.rglob("*")):
        rel = path.relative_to(legacy_root).as_posix()
        if rel.startswith("frontend/src/products/"):
            continue
        if rel in LIKELY_PLATFORM_DIRS:
            platform_candidates.append({
                "source": rel,
                "target": LIKELY_PLATFORM_DIRS[rel],
                "reason": "explicit_platform_mapping",
            })
        elif any(part in rel for part in ["components", "lib", "pages"]):
            manual_split.append({
                "source": rel,
                "reason": "frontend_shell_or_shared_ui_needs_manual_review",
            })
        else:
            untouched.append(rel)

    payload = {
        "platform_candidates": platform_candidates,
        "manual_split": manual_split,
        "untouched": untouched,
    }

    out = repo_root / "tools" / "repo" / "platform-extraction-plan.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Platform extraction plan written to {out}")


if __name__ == "__main__":
    main()