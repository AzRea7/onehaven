#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


SCAN_ROOTS = [
    "apps",
    "products",
    "onehaven_platform",
]

REPLS = {
    # legacy app root imports
    "from app.auth import": "from onehaven_platform.backend.src.auth import",
    "from app.config import": "from onehaven_platform.backend.src.config import",
    "from app.db import": "from onehaven_platform.backend.src.db import",
    "from app.models import": "from onehaven_platform.backend.src.models import",
    "from app.policy_models import": "from onehaven_platform.backend.src.policy_models import",
    "from app.schemas import": "from onehaven_platform.backend.src.schemas import",
    "from app.logging_config import": "from onehaven_platform.backend.src.logging_config import",

    # legacy package imports
    "from app.middleware.": "from onehaven_platform.backend.src.middleware.",
    "from app.workers.": "from onehaven_platform.backend.src.jobs.",
    "from app.tasks.": "from onehaven_platform.backend.src.jobs.",
    "from app.clients.": "from onehaven_platform.backend.src.integrations.",
    "from app.integrations.": "from onehaven_platform.backend.src.integrations.",
    "from app.services.": "from onehaven_platform.backend.src.services.",
    "from app.domain.": "from onehaven_platform.backend.src.domain.",

    # bad product rewrites
    "from onehaven_platform.backend.src.products.compliance.": "from products.compliance.backend.src.",
    "from onehaven_platform.backend.src.products.management.": "from products.ops.backend.src.",
    "from onehaven_platform.backend.src.products.investor_intelligence.": "from products.intelligence.backend.src.",
    "from onehaven_platform.backend.src.products.tenant.": "from products.tenants.backend.src.",

    # old product names
    "from products.management.": "from products.ops.",
    "from products.investor_intelligence.": "from products.intelligence.",
    "from products.tenant.": "from products.tenants.",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    changed = 0
    replacements = 0

    for root_name in SCAN_ROOTS:
        scan_root = root / root_name
        if not scan_root.exists():
            continue

        for path in scan_root.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue

            original = path.read_text(encoding="utf-8")
            updated = original

            for old, new in REPLS.items():
                count = updated.count(old)
                if count:
                    replacements += count
                    updated = updated.replace(old, new)

            if updated != original:
                changed += 1
                if args.dry_run:
                    print(f"[DRY RUN] would update {path}")
                else:
                    path.write_text(updated, encoding="utf-8")
                    print(f"updated {path}")

    print("Phase 84 complete.")
    print({
        "files_changed": changed,
        "replacements": replacements,
        "dry_run": args.dry_run,
    })


if __name__ == "__main__":
    main()