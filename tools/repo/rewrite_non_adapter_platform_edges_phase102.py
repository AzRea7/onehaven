#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PLATFORM_ROOT = Path("onehaven_platform/backend/src")
SKIP_DIRS = {
    "onehaven_platform/backend/src/adapters",
}

REPLS = {
    # intelligence
    "from products.intelligence.backend.src.services.zillow_photo_source import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.market_sync_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.market_catalog_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.portfolio_watchlist_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.property_price_resolution_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.public_tax_lookup_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.fmr import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.services.hud_fmr_service import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.domain.rent_learning import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",
    "from products.intelligence.backend.src.domain.underwriting import":
        "from onehaven_platform.backend.src.adapters.intelligence_adapter import",

    # acquire
    "from products.acquire.backend.src.services.ingestion_run_service import":
        "from onehaven_platform.backend.src.adapters.acquire_adapter import",
    "from products.acquire.backend.src.services.ingestion_enrichment_service import":
        "from onehaven_platform.backend.src.adapters.acquire_adapter import",
    "from products.acquire.backend.src.services.acquisition_tag_service import":
        "from onehaven_platform.backend.src.adapters.acquire_adapter import",

    # compliance
    "from products.compliance.backend.src.services.policy_governance.refresh_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.policy_governance.notification_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.policy_coverage.health_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.workflow_gate_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.compliance_document_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.compliance_photo_analysis_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.compliance_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.inspection_scheduling_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.services.jurisdiction_profile_service import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.domain.inspection.hqs_library import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.domain.inspection.inspection_mapping import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.domain.inspection.top_fail_points import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.domain.inspection.hqs import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
    "from products.compliance.backend.src.domain.inspection.inspection_rules import":
        "from onehaven_platform.backend.src.adapters.compliance_adapter import",
}


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def is_skipped(path: Path, root: Path) -> bool:
    rel = path.relative_to(root).as_posix()
    return any(rel.startswith(skip) for skip in SKIP_DIRS)


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    scan_root = root / PLATFORM_ROOT

    changed = []
    replacements = 0

    for path in scan_root.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        if is_skipped(path, root):
            continue

        original = path.read_text(encoding="utf-8")
        updated = original
        file_replacements = 0

        for old, new in REPLS.items():
            count = updated.count(old)
            if count:
                updated = updated.replace(old, new)
                file_replacements += count

        if updated != original:
            rel = path.relative_to(root).as_posix()
            changed.append({"file": rel, "replacements": file_replacements})
            replacements += file_replacements

            if args.dry_run:
                print("[DRY RUN] update", rel, file_replacements)
            else:
                path.write_text(updated, encoding="utf-8")
                print("updated", rel, file_replacements)

    payload = {
        "phase": 102,
        "dry_run": args.dry_run,
        "files_changed": len(changed),
        "replacements": replacements,
        "changed": changed,
    }

    out = root / "tools/repo/non-adapter-platform-rewrite-phase102-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 102 complete.")
    print({"files_changed": len(changed), "replacements": replacements})
    print("Report written to tools/repo/non-adapter-platform-rewrite-phase102-report.json")


if __name__ == "__main__":
    main()