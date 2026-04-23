#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class MappingRule:
    source: str
    destination: str
    kind: str  # tree | file | manual | future
    notes: str


ROOT_DIRS = [
    "apps",
    "products",
    "platform",
    "packages",
    "infra",
    "tools",
    "tests",
    "data",
    "docs/architecture/adr",
    "docs/products",
    "docs/runbooks",
]

APP_DIRS = [
    "apps/suite-web/src/app",
    "apps/suite-web/src/routes",
    "apps/suite-web/src/layouts",
    "apps/suite-web/src/providers",
    "apps/suite-web/src/navigation",
    "apps/suite-web/src/bootstrap",
    "apps/suite-web/public",
    "apps/suite-api/app/bootstrap",
    "apps/suite-api/app/api/gateway",
    "apps/suite-api/app/api/suite",
    "apps/suite-api/app/api/health",
    "apps/suite-api/app/dependencies",
    "apps/suite-api/tests",
    "apps/worker/app/queues",
    "apps/worker/app/schedulers",
    "apps/worker/app/handlers",
    "apps/ops-admin/src/app",
    "apps/ops-admin/src/pages",
    "apps/ops-admin/src/modules",
    "apps/docs-site/content",
]

PLATFORM_BACKEND_DIRS = [
    "platform/backend/src/identity/domain",
    "platform/backend/src/identity/application",
    "platform/backend/src/identity/interfaces",
    "platform/backend/src/identity/infrastructure",
    "platform/backend/src/orgs",
    "platform/backend/src/billing",
    "platform/backend/src/usage",
    "platform/backend/src/audit",
    "platform/backend/src/notifications",
    "platform/backend/src/files",
    "platform/backend/src/workflow",
    "platform/backend/src/search",
    "platform/backend/src/jobs",
    "platform/backend/src/observability",
    "platform/backend/src/integrations",
    "platform/backend/src/config",
    "platform/backend/src/db",
    "platform/backend/src/shared_kernel",
]

PLATFORM_FRONTEND_DIRS = [
    "platform/frontend/src/auth",
    "platform/frontend/src/shell",
    "platform/frontend/src/navigation",
    "platform/frontend/src/org-context",
    "platform/frontend/src/permissions",
    "platform/frontend/src/notifications",
    "platform/frontend/src/file-upload",
    "platform/frontend/src/telemetry",
]

PLATFORM_CONTRACT_DIRS = [
    "platform/contracts/auth",
    "platform/contracts/orgs",
    "platform/contracts/billing",
    "platform/contracts/workflow",
]

PACKAGE_DIRS = [
    "packages/ui/src/components",
    "packages/ui/src/tokens",
    "packages/ui/src/layouts",
    "packages/types",
    "packages/api-client",
    "packages/events",
    "packages/config",
    "packages/utils",
    "packages/testing",
    "packages/eslint-config",
    "packages/tsconfig",
    "packages/python-common",
]

PRODUCTS = [
    "intelligence",
    "acquire",
    "compliance",
    "tenants",
    "ops",
]

PRODUCT_DISPLAY_NAMES = {
    "intelligence": "OneHaven Intelligence",
    "acquire": "OneHaven Acquire",
    "compliance": "OneHaven Compliance",
    "tenants": "OneHaven Tenants",
    "ops": "OneHaven Ops",
}

PRODUCT_DESCRIPTIONS = {
    "intelligence": "Find and rank rental deals.",
    "acquire": "Move from interest to close.",
    "compliance": "Know what applies, what is missing, and what is risky.",
    "tenants": "Match and manage voucher-ready applicants.",
    "ops": "Manage properties, tasks, inspections, and leases.",
}

PRODUCT_BACKEND_COMMON = [
    "backend/src/domain",
    "backend/src/application",
    "backend/src/interfaces/api",
    "backend/src/interfaces/events",
    "backend/src/interfaces/jobs",
    "backend/src/infrastructure",
    "backend/src/tests",
]

PRODUCT_FRONTEND_COMMON = [
    "frontend/src/pages",
    "frontend/src/components",
    "frontend/src/hooks",
    "frontend/src/api",
    "frontend/src/state",
]

PRODUCT_CONTRACT_COMMON = [
    "contracts/api",
    "contracts/events",
    "contracts/schemas",
]

COMPLIANCE_EXTRA = [
    "products/compliance/backend/src/domain/policy",
    "products/compliance/backend/src/domain/evidence",
    "products/compliance/backend/src/domain/trust",
    "products/compliance/backend/src/domain/inspections",
    "products/compliance/backend/src/domain/markets",
    "products/compliance/backend/src/domain/review",
    "products/compliance/backend/src/domain/reporting",
    "products/compliance/backend/src/application/briefs",
    "products/compliance/backend/src/application/rollups",
    "products/compliance/backend/src/application/readiness",
    "products/compliance/backend/src/application/remediation",
    "products/compliance/backend/src/application/market_launch",
    "products/compliance/backend/src/application/review_queue",
    "products/compliance/backend/src/application/rule_change_monitoring",
    "products/compliance/backend/src/application/exports",
    "products/compliance/backend/src/interfaces/cli",
    "products/compliance/backend/src/infrastructure/persistence",
    "products/compliance/backend/src/infrastructure/crawlers",
    "products/compliance/backend/src/infrastructure/parsers",
    "products/compliance/backend/src/infrastructure/document_storage",
    "products/compliance/backend/src/infrastructure/external_clients",
    "products/compliance/backend/src/infrastructure/search",
    "products/compliance/frontend/src/workflows",
]

INFRA_DIRS = [
    "infra/docker",
    "infra/terraform",
    "infra/environments/local",
    "infra/environments/dev",
    "infra/environments/staging",
    "infra/environments/prod",
    "infra/k8s",
    "infra/monitoring/dashboards",
    "infra/monitoring/alerts",
    "infra/monitoring/log-pipelines",
    "infra/scripts",
]

TOOLS_DIRS = [
    "tools/repo",
    "tools/codegen",
    "tools/migrations",
    "tools/local-dev",
]

TEST_DIRS = [
    "tests/contract",
    "tests/integration",
    "tests/e2e",
    "tests/performance",
    "tests/smoke",
]

DATA_DIRS = [
    "data/fixtures",
    "data/demo",
    "data/seeds",
    "data/geo",
]

DOC_FILES = {
    "docs/architecture/system-overview.md": "# System Overview\n",
    "docs/architecture/repo-structure.md": "# Repo Structure\n",
    "docs/architecture/product-boundaries.md": "# Product Boundaries\n",
    "docs/architecture/dependency-rules.md": "# Dependency Rules\n",
    "docs/architecture/event-model.md": "# Event Model\n",
    "docs/architecture/tenancy-model.md": "# Tenancy Model\n",
    "docs/architecture/permissions-model.md": "# Permissions Model\n",
    "docs/architecture/adr/0001-monorepo.md": "# ADR 0001 - Monorepo\n",
    "docs/architecture/adr/0002-platform-vs-product-boundaries.md": "# ADR 0002 - Platform vs Product Boundaries\n",
    "docs/architecture/adr/0003-bff-and-api-splitting.md": "# ADR 0003 - BFF and API Splitting\n",
    "docs/architecture/adr/0004-event-driven-cross-product-workflows.md": "# ADR 0004 - Event Driven Cross Product Workflows\n",
    "docs/products/intelligence.md": "# OneHaven Intelligence\n",
    "docs/products/acquire.md": "# OneHaven Acquire\n",
    "docs/products/compliance.md": "# OneHaven Compliance\n",
    "docs/products/tenants.md": "# OneHaven Tenants\n",
    "docs/products/ops.md": "# OneHaven Ops\n",
    "docs/runbooks/local-dev.md": "# Local Development\n",
    "docs/runbooks/production-deploy.md": "# Production Deploy\n",
    "docs/runbooks/incident-response.md": "# Incident Response\n",
    "docs/runbooks/market-launch-checklist.md": "# Market Launch Checklist\n",
}

MAPPING_RULES: list[MappingRule] = [
    MappingRule(
        source="onehaven_decision_engine/backend/app/products/investor_intelligence",
        destination="products/intelligence/backend/src",
        kind="tree",
        notes="Later move into OneHaven Intelligence backend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/products/acquire",
        destination="products/acquire/backend/src",
        kind="tree",
        notes="Later move into OneHaven Acquire backend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/products/compliance",
        destination="products/compliance/backend/src",
        kind="tree",
        notes="Later move into OneHaven Compliance backend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/products/tenant",
        destination="products/tenants/backend/src",
        kind="tree",
        notes="Later move into OneHaven Tenants backend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/products/management",
        destination="products/ops/backend/src",
        kind="tree",
        notes="Later move into OneHaven Ops backend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/products/investor_intelligence",
        destination="products/intelligence/frontend/src",
        kind="tree",
        notes="Later move into OneHaven Intelligence frontend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/products/acquire/frontend/src",
        destination="products/acquire/frontend/src",
        kind="tree",
        notes="Later move into OneHaven Acquire frontend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/products/compliance",
        destination="products/compliance/frontend/src",
        kind="tree",
        notes="Later move into OneHaven Compliance frontend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/products/tenant",
        destination="products/tenants/frontend/src",
        kind="tree",
        notes="Later move into OneHaven Tenants frontend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/products/management",
        destination="products/ops/frontend/src",
        kind="tree",
        notes="Later move into OneHaven Ops frontend.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/auth.py",
        destination="platform/backend/src/identity/interfaces",
        kind="manual",
        notes="Likely split across identity/config/shared kernel.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/config.py",
        destination="platform/backend/src/config",
        kind="tree",
        notes="Platform-owned config.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/db.py",
        destination="platform/backend/src/db",
        kind="tree",
        notes="Platform-owned db bootstrap.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/logging_config.py",
        destination="platform/backend/src/observability",
        kind="tree",
        notes="Platform-owned logging/observability.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/middleware",
        destination="platform/backend/src/observability",
        kind="tree",
        notes="Likely split across observability/identity.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/routers/auth.py",
        destination="platform/backend/src/identity/interfaces",
        kind="manual",
        notes="Suite/platform auth entrypoint.",
    ),
    MappingRule(
        source="onehaven_decision_engine/backend/app/routers/health.py",
        destination="apps/suite-api/app/api/health",
        kind="tree",
        notes="Suite-api health route.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/App.tsx",
        destination="apps/suite-web/src/app",
        kind="manual",
        notes="Suite shell bootstrap.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/main.tsx",
        destination="apps/suite-web/src/bootstrap",
        kind="manual",
        notes="Suite web entrypoint.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/components",
        destination="platform/frontend/src/shell",
        kind="manual",
        notes="Must be split into platform shell vs packages/ui vs product UI.",
    ),
    MappingRule(
        source="onehaven_decision_engine/frontend/src/lib/auth.tsx",
        destination="platform/frontend/src/auth",
        kind="tree",
        notes="Platform auth client.",
    ),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap new OneHaven architecture without moving source files."
    )
    parser.add_argument("--repo-root", default=".", help="Path to repository root.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite generated files where applicable.",
    )
    parser.add_argument(
        "--write-gitignore",
        action="store_true",
        help="Write .gitkeep into empty generated directories.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_file(path: Path, content: str, force: bool) -> None:
    if path.exists() and not force:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def touch_keep(path: Path, force: bool) -> None:
    keep = path / ".gitkeep"
    if keep.exists() and not force:
        return
    keep.write_text("", encoding="utf-8")


def create_directories(base: Path, dirs: Iterable[str], write_gitkeep: bool, force: bool) -> list[str]:
    created: list[str] = []
    for rel in dirs:
        target = base / rel
        ensure_dir(target)
        created.append(rel)
        if write_gitkeep:
            touch_keep(target, force)
    return created


def build_product_dirs() -> list[str]:
    all_dirs: list[str] = []
    for product in PRODUCTS:
        for suffix in PRODUCT_BACKEND_COMMON:
            all_dirs.append(f"products/{product}/{suffix}")
        for suffix in PRODUCT_FRONTEND_COMMON:
            all_dirs.append(f"products/{product}/{suffix}")
        for suffix in PRODUCT_CONTRACT_COMMON:
            all_dirs.append(f"products/{product}/{suffix}")
    all_dirs.extend(COMPLIANCE_EXTRA)
    return all_dirs


def find_existing_sources(repo_root: Path) -> dict[str, bool]:
    return {
        rule.source: (repo_root / rule.source).exists()
        for rule in MAPPING_RULES
    }


def write_mapping_manifest(repo_root: Path, source_exists: dict[str, bool]) -> None:
    manifest_path = repo_root / "tools/repo/migration-manifest.json"
    payload = {
        "phase": "bootstrap",
        "safe_mode": True,
        "moves_performed": False,
        "import_rewrites_performed": False,
        "shim_files_created": False,
        "products": [
            {
                "slug": slug,
                "display_name": PRODUCT_DISPLAY_NAMES[slug],
                "description": PRODUCT_DESCRIPTIONS[slug],
            }
            for slug in PRODUCTS
        ],
        "rules": [
            {
                **asdict(rule),
                "source_exists": source_exists[rule.source],
            }
            for rule in MAPPING_RULES
        ],
    }
    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_inventory_report(repo_root: Path, source_exists: dict[str, bool], force: bool) -> None:
    lines: list[str] = []
    lines.append("# Architecture Bootstrap Inventory")
    lines.append("")
    lines.append("This file is generated by `tools/repo/bootstrap_new_architecture.py`.")
    lines.append("")
    lines.append("## Product Naming")
    lines.append("")
    for slug in PRODUCTS:
        lines.append(f"- **{PRODUCT_DISPLAY_NAMES[slug]}** (`{slug}`)")
        lines.append(f"  - {PRODUCT_DESCRIPTIONS[slug]}")
    lines.append("")
    lines.append("## Source Mapping Status")
    lines.append("")
    for rule in MAPPING_RULES:
        exists = "YES" if source_exists[rule.source] else "NO"
        lines.append(f"- **Source exists:** {exists}")
        lines.append(f"  - Source: `{rule.source}`")
        lines.append(f"  - Destination: `{rule.destination}`")
        lines.append(f"  - Kind: `{rule.kind}`")
        lines.append(f"  - Notes: {rule.notes}")
        lines.append("")
    write_file(
        repo_root / "docs/architecture/bootstrap-inventory.md",
        "\n".join(lines) + "\n",
        force,
    )


def write_dependency_rules(repo_root: Path, force: bool) -> None:
    content = """# Dependency Rules

## Allowed
- apps may depend on platform, products, and packages
- products may depend on platform and packages
- platform may depend on packages
- packages should not depend on apps
- product A must not deep-import product B internals

## Forbidden
- no new code in legacy catch-all service folders unless explicitly platform-owned
- no cross-product imports into another product's infrastructure internals
- no compatibility shims
- no duplicate business logic in platform

## Migration Rule
Until move/rewrite phase begins, legacy code remains source-of-truth.
The new folders are destination ownership targets only.
"""
    write_file(repo_root / "docs/architecture/dependency-rules.md", content, force)


def write_repo_structure_doc(repo_root: Path, force: bool) -> None:
    content = """# Repo Structure

This repository is transitioning to a platform + products + apps monorepo.

## Products
- OneHaven Intelligence
- OneHaven Acquire
- OneHaven Compliance
- OneHaven Tenants
- OneHaven Ops

## Layers
- `apps/` deployable applications
- `products/` business capabilities that can be sold standalone or bundled
- `platform/` shared operational/platform concerns
- `packages/` reusable shared libraries
- `infra/` deployment and environment infrastructure
- `tools/` migration, codegen, and repo automation
- `tests/` cross-cutting test layers

## Migration Safety
This bootstrap phase creates the target structure only.
It does not move files, rewrite imports, or create shims.
"""
    write_file(repo_root / "docs/architecture/repo-structure.md", content, force)


def write_product_boundaries_doc(repo_root: Path, force: bool) -> None:
    content = """# Product Boundaries

## Products
- OneHaven Intelligence
- OneHaven Acquire
- OneHaven Compliance
- OneHaven Tenants
- OneHaven Ops

## Ownership Rule
Business logic belongs in its product.
Shared infra belongs in platform.
Truly reusable libraries belong in packages.

## Critical Rule
Products may not deep-import each other.
Cross-product collaboration must happen through published contracts, APIs, or events.
"""
    write_file(repo_root / "docs/architecture/product-boundaries.md", content, force)


def write_product_docs(repo_root: Path, force: bool) -> None:
    for slug in PRODUCTS:
        content = (
            f"# {PRODUCT_DISPLAY_NAMES[slug]}\n\n"
            f"{PRODUCT_DESCRIPTIONS[slug]}\n"
        )
        write_file(repo_root / f"docs/products/{slug}.md", content, force)


def write_placeholder_app_files(repo_root: Path, force: bool) -> None:
    files = {
        "apps/suite-api/app/main.py": '"""Suite API entrypoint placeholder."""\n',
        "apps/worker/app/worker_main.py": '"""Worker entrypoint placeholder."""\n',
        "apps/suite-web/package.json": '{\n  "name": "@onehaven/suite-web"\n}\n',
        "apps/ops-admin/package.json": '{\n  "name": "@onehaven/ops-admin"\n}\n',
        "apps/docs-site/package.json": '{\n  "name": "@onehaven/docs-site"\n}\n',
        "apps/suite-api/pyproject.toml": '[project]\nname = "onehaven-suite-api"\nversion = "0.1.0"\n',
        "apps/worker/pyproject.toml": '[project]\nname = "onehaven-worker"\nversion = "0.1.0"\n',
        "pnpm-workspace.yaml": (
            "packages:\n"
            "  - apps/*\n"
            "  - products/*/frontend\n"
            "  - packages/*\n"
            "  - platform/frontend\n"
        ),
        "turbo.json": '{\n  "$schema": "https://turbo.build/schema.json",\n  "tasks": {}\n}\n',
    }
    for rel, content in files.items():
        write_file(repo_root / rel, content, force)


def write_docs(repo_root: Path, force: bool) -> None:
    for rel, content in DOC_FILES.items():
        write_file(repo_root / rel, content, force)


def write_next_steps(repo_root: Path, force: bool) -> None:
    content = """# Next Steps

This repo has been bootstrapped for the new architecture.

## What happened
- target folders were created
- documentation placeholders were created
- migration manifest was generated
- inventory report was generated

## What did NOT happen
- no source files were moved
- no imports were rewritten
- no shims were created
- no runtime behavior was changed

## Recommended phase 2
- freeze legacy-to-target ownership map
- classify every top-level legacy backend service as:
  - platform
  - product-owned
  - package candidate
  - manual split required
- perform one product move at a time with atomic import rewrites
- validate with tests after each batch
"""
    write_file(repo_root / "docs/architecture/next-steps.md", content, force)


def write_summary(repo_root: Path, created_dirs: list[str]) -> None:
    summary = {
        "created_directory_count": len(created_dirs),
        "created_directories": created_dirs,
        "phase": "bootstrap",
        "moves_performed": False,
        "imports_rewritten": False,
        "shims_created": False,
        "products": [
            {
                "slug": slug,
                "display_name": PRODUCT_DISPLAY_NAMES[slug],
            }
            for slug in PRODUCTS
        ],
    }
    (repo_root / "tools/repo/bootstrap-summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )


def validate_repo_root(repo_root: Path) -> None:
    legacy_root = repo_root / "onehaven_decision_engine"
    if not legacy_root.exists():
        raise SystemExit(
            f"Expected legacy root directory at: {legacy_root}\n"
            "Run this script from the repository root or pass --repo-root correctly."
        )


def backup_existing_generated_files(repo_root: Path) -> None:
    generated_paths = [
        repo_root / "tools/repo/migration-manifest.json",
        repo_root / "docs/architecture/bootstrap-inventory.md",
        repo_root / "tools/repo/bootstrap-summary.json",
    ]
    backup_dir = repo_root / "tools/repo/_bootstrap_backups"
    ensure_dir(backup_dir)

    for path in generated_paths:
        if path.exists():
            target = backup_dir / f"{path.name}.bak"
            shutil.copy2(path, target)


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()

    validate_repo_root(repo_root)
    backup_existing_generated_files(repo_root)

    created_dirs: list[str] = []
    created_dirs.extend(create_directories(repo_root, ROOT_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, APP_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, PLATFORM_BACKEND_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, PLATFORM_FRONTEND_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, PLATFORM_CONTRACT_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, PACKAGE_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, build_product_dirs(), args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, INFRA_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, TOOLS_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, TEST_DIRS, args.write_gitignore, args.force))
    created_dirs.extend(create_directories(repo_root, DATA_DIRS, args.write_gitignore, args.force))

    write_docs(repo_root, args.force)
    write_product_docs(repo_root, args.force)
    write_dependency_rules(repo_root, args.force)
    write_repo_structure_doc(repo_root, args.force)
    write_product_boundaries_doc(repo_root, args.force)
    write_placeholder_app_files(repo_root, args.force)
    write_next_steps(repo_root, args.force)

    source_exists = find_existing_sources(repo_root)
    write_mapping_manifest(repo_root, source_exists)
    write_inventory_report(repo_root, source_exists, args.force)
    write_summary(repo_root, created_dirs)

    print("Bootstrap complete.")
    print("No moves performed.")
    print("No imports rewritten.")
    print("No shims created.")
    print("")
    print("Generated:")
    print(" - tools/repo/migration-manifest.json")
    print(" - docs/architecture/bootstrap-inventory.md")
    print(" - tools/repo/bootstrap-summary.json")
    print("")
    print("Products:")
    for slug in PRODUCTS:
        print(f" - {PRODUCT_DISPLAY_NAMES[slug]} ({slug})")
    print("")
    print("Next safe step: move one product at a time with atomic import rewrites.")


if __name__ == "__main__":
    main()