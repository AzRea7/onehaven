#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


SOURCE_ROOT = "onehaven_decision_engine/backend/app"


PLATFORM_RULES = [
    "services/agent_",
    "services/auth_service",
    "services/address_normalization",
    "services/budget_service",
    "services/agent_orchestrator",
    "domain/agents",
    "domain/policy",
]

PRODUCT_RULES = {
    "ops": ["rehab"],
    "intelligence": ["rent"],
    "acquire": ["imports"],
    "tenants": ["tenant"],
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def matches(path: str, patterns: list[str]) -> bool:
    return any(p in path for p in patterns)


def move(src: Path, dst: Path, dry: bool):
    if dst.exists():
        return "skipped_exists"

    if dry:
        return "would_move"

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return "moved"


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    src_root = root / SOURCE_ROOT

    results = {
        "platform": [],
        "products": [],
        "skipped": [],
    }

    for file in src_root.rglob("*.py"):
        rel = file.relative_to(src_root).as_posix()

        # PLATFORM
        if matches(rel, PLATFORM_RULES):
            target = root / "onehaven_onehaven_platform/backend/src" / rel
            status = move(file, target, args.dry_run)
            results["platform"].append((rel, status))
            continue

        # PRODUCTS
        matched = False
        for product, rules in PRODUCT_RULES.items():
            if matches(rel, rules):
                target = root / f"products/{product}/backend/src" / rel
                status = move(file, target, args.dry_run)
                results["products"].append((rel, status))
                matched = True
                break

        if not matched:
            results["skipped"].append(rel)

    print("Phase 40 complete.")
    print(f"Platform moved: {len(results['platform'])}")
    print(f"Product moved: {len(results['products'])}")
    print(f"Skipped: {len(results['skipped'])}")


if __name__ == "__main__":
    main()