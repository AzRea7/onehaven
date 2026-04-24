#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


REPLS = {
    # Compliance facade/use cases
    "from products.compliance.backend.src.services import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",

    "from products.compliance.backend.src.services.trust_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",

    # Intelligence services
    "from products.intelligence.backend.src.services.risk_scoring import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",

    "from products.intelligence.backend.src.services.property_tax_enrichment_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",

    "from products.intelligence.backend.src.services.property_insurance_enrichment_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",

    "from products.intelligence.backend.src.services.rentcast_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",

    # Acquire services
    "from products.acquire.backend.src.services.ingestion_source_service import":
        "from onehaven_platform.backend.src.adapters.acquire_adapter import",

    "from products.acquire.backend.src.services.ingestion_run_execute import":
        "from onehaven_platform.backend.src.adapters.acquire_adapter import",

    "from products.acquire.backend.src.services.ingestion_scheduler_service import":
        "from onehaven_platform.backend.src.adapters.acquire_adapter import",

    # Ops services
    "from products.ops.backend.src.services.properties.inventory_snapshot_service import":
        "from onehaven_platform.backend.src.adapters.ops_adapter import",
}


PLATFORM_ROOT = Path("onehaven_platform/backend/src")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    scan_root = root / PLATFORM_ROOT

    changed = []
    replacements = 0

    for path in scan_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue

        original = path.read_text(encoding="utf-8")
        updated = original
        file_replacements = 0

        for old, new in REPLS.items():
            count = updated.count(old)
            if count:
                file_replacements += count
                updated = updated.replace(old, new)

        if updated != original:
            rel = path.relative_to(root).as_posix()
            replacements += file_replacements
            changed.append({"file": rel, "replacements": file_replacements})

            if args.dry_run:
                print("[DRY RUN] update", rel, file_replacements)
            else:
                path.write_text(updated, encoding="utf-8")
                print("updated", rel, file_replacements)

    payload = {
        "phase": 98,
        "dry_run": args.dry_run,
        "files_changed": len(changed),
        "replacements": replacements,
        "changed": changed,
    }

    out = root / "tools/repo/platform-product-rewrite-phase98-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 98 complete.")
    print({"files_changed": len(changed), "replacements": replacements})
    print("Report written to tools/repo/platform-product-rewrite-phase98-report.json")


if __name__ == "__main__":
    main()