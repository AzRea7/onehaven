#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# Run from repo root:
#   python scripts/restructure_products.py --repo-root .
# Dry run first:
#   python scripts/restructure_products.py --repo-root . --dry-run
#
# This script is intentionally incremental and idempotent:
# - creates target folders if missing
# - moves only files explicitly mapped below
# - skips files already moved
# - rewrites imports after moves
# - does not touch migrations, tests, docs, Docker, or config
#
# Adjust FILE_MOVES if you want to move more files in later passes.

PYTHON_FILE_EXTS = {".py"}
TS_FILE_EXTS = {".ts", ".tsx", ".js", ".jsx"}

BACKEND_PRODUCT_ROOT = Path("backend/app/products")
FRONTEND_PRODUCT_ROOT = Path("frontend/src/products")

BACKEND_SHARED_DIRS = [
    Path("backend/app/domain"),
    Path("backend/app/routers"),
    Path("backend/app/services"),
    Path("backend/app/clients"),
    Path("backend/app/middleware"),
]

FRONTEND_SHARED_DIRS = [
    Path("frontend/src/components"),
    Path("frontend/src/lib"),
    Path("frontend/src/pages"),
]

PRODUCTS = {
    "investor_intelligence": {
        "backend": [
            "deal_intelligence_service.py",
            "risk_scoring.py",
            "pane_dashboard_service.py",
            "pane_summary_snapshot_service.py",
        ],
        "frontend_pages": [
            "InvestorPane.tsx",
        ],
        "frontend_components": [
            "ShortlistBoard.tsx",
            "RiskBadges.tsx",
            "PaneSummaryCards.tsx",
        ],
    },
    "compliance": {
        "backend": [
            "compliance_service.py",
            "compliance_document_service.py",
            "compliance_photo_analysis_service.py",
            "property_compliance_resolution_service.py",
            "workflow_gate_service.py",
        ],
        "backend_dirs": [
            "compliance_engine",
            "inspections",
            "policy_assertions",
            "policy_coverage",
            "policy_governance",
            "policy_pipeline",
            "policy_sources",
        ],
        "frontend_pages": [
            "CompliancePane.tsx",
        ],
        "frontend_components": [
            "ComplianceDocumentStack.tsx",
            "ComplianceDocumentUploader.tsx",
            "CompliancePhotoFindingsPanel.tsx",
            "ComplianceReminderPanel.tsx",
            "InspectionReadiness.tsx",
            "InspectionSchedulerModal.tsx",
            "InspectionTimelineCard.tsx",
            "JurisdictionCoverageBadge.tsx",
            "PropertyCompliancePanel.tsx",
            "PropertyJurisdictionRulesPanel.tsx",
        ],
    },
    "acquire": {
        "backend": [
            "acquisition_service.py",
            "acquisition_workspace_service.py",
            "acquisition_deadline_service.py",
            "acquisition_document_review_service.py",
            "acquisition_participant_service.py",
            "acquisition_tag_service.py",
            "document_ingestion_router_service.py",
            "document_parsing_service.py",
        ],
        "frontend_pages": [
            "AcquisitionPane.tsx",
            "AcquisitionQueue.tsx",
            "DealIntake.tsx",
        ],
        "frontend_components": [
            "AcquisitionDeadlinePanel.tsx",
            "AcquisitionFilters.tsx",
            "AcquisitionParticipantsPanel.tsx",
            "AcquisitionTagBar.tsx",
            "DocumentFieldReviewPanel.tsx",
        ],
    },
    "tenant": {
        "backend": [
            "tenant_match_service.py",
            "inspector_communication_service.py",
        ],
        "frontend_pages": [
            "TenantsPane.tsx",
        ],
        "frontend_components": [
            "TenantPipeline.tsx",
        ],
    },
    "management": {
        "backend": [
            "dashboard_rollups.py",
        ],
        "backend_dirs": [
            "properties",
        ],
        "frontend_pages": [
            "ManagementPane.tsx",
            "Dashboard.tsx",
        ],
        "frontend_components": [
            "NextActionsPanel.tsx",
            "PropertyImage.tsx",
            "PhotoGallery.tsx",
            "PhotoUploader.tsx",
            "StageProgress.tsx",
        ],
    },
}

# Files that should remain shared/platform/core even after product split.
DO_NOT_MOVE_BACKEND = {
    "auth_service.py",
    "locks_service.py",
    "usage_service.py",
    "plan_service.py",
    "runtime_metrics.py",
    "agent_engine.py",
    "agent_orchestrator.py",
    "agent_orchestrator_runtime.py",
    "agent_threads.py",
    "agent_trace.py",
    "auth.py",
    "config.py",
    "db.py",
    "models.py",
    "policy_models.py",
    "schemas.py",
    "main.py",
}

DO_NOT_MOVE_FRONTEND = {
    "App.tsx",
    "main.tsx",
    "Shell.tsx",
    "AppShell.tsx",
    "AppHeader.tsx",
    "AppFooter.tsx",
    "PageShell.tsx",
    "FilterBar.tsx",
    "GlobalFilters.tsx",
    "PageHero.tsx",
    "Spinner.tsx",
}

ROUTER_TO_PRODUCT = {
    "compliance.py": "compliance",
    "inspections.py": "compliance",
    "policy.py": "compliance",
    "policy_catalog_admin.py": "compliance",
    "policy_evidence.py": "compliance",
    "jurisdiction_profiles.py": "compliance",
    "jurisdictions.py": "compliance",
    "markets.py": "compliance",
    "acquisition.py": "acquire",
    "tenants.py": "tenant",
    "dashboard.py": "management",
    "properties.py": "management",
    "deals.py": "investor_intelligence",
    "evaluate.py": "investor_intelligence",
    "cash.py": "investor_intelligence",
    "equity.py": "investor_intelligence",
}

@dataclass(frozen=True)
class MoveOp:
    src: Path
    dst: Path

def normalize(path: Path) -> str:
    return path.as_posix()

def ensure_parent(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    path.parent.mkdir(parents=True, exist_ok=True)

def ensure_package_dir(path: Path, dry_run: bool) -> None:
    if dry_run:
        return
    path.mkdir(parents=True, exist_ok=True)
    init_file = path / "__init__.py"
    if not init_file.exists():
        init_file.write_text("", encoding="utf-8")

def safe_move(src: Path, dst: Path, dry_run: bool) -> str:
    if not src.exists():
        return f"SKIP missing: {src}"
    if dst.exists():
        if src.resolve() == dst.resolve():
            return f"SKIP already moved: {src}"
        return f"SKIP destination exists: {dst}"
    ensure_parent(dst, dry_run=dry_run)
    if dry_run:
        return f"DRY-RUN move: {src} -> {dst}"
    shutil.move(str(src), str(dst))
    return f"MOVED: {src} -> {dst}"

def collect_explicit_moves(repo_root: Path) -> list[MoveOp]:
    moves: list[MoveOp] = []

    services_dir = repo_root / "backend/app/services"
    pages_dir = repo_root / "frontend/src/pages"
    components_dir = repo_root / "frontend/src/components"

    for product, spec in PRODUCTS.items():
        # backend flat service files
        for name in spec.get("backend", []):
            if name in DO_NOT_MOVE_BACKEND:
                continue
            src = services_dir / name
            dst = repo_root / BACKEND_PRODUCT_ROOT / product / "services" / name
            moves.append(MoveOp(src=src, dst=dst))

        # backend service subdirs
        for dirname in spec.get("backend_dirs", []):
            src = services_dir / dirname
            dst = repo_root / BACKEND_PRODUCT_ROOT / product / "services" / dirname
            moves.append(MoveOp(src=src, dst=dst))

        # frontend pages
        for name in spec.get("frontend_pages", []):
            if name in DO_NOT_MOVE_FRONTEND:
                continue
            src = pages_dir / name
            dst = repo_root / FRONTEND_PRODUCT_ROOT / product / "pages" / name
            moves.append(MoveOp(src=src, dst=dst))

        # frontend components
        for name in spec.get("frontend_components", []):
            if name in DO_NOT_MOVE_FRONTEND:
                continue
            src = components_dir / name
            dst = repo_root / FRONTEND_PRODUCT_ROOT / product / "components" / name
            moves.append(MoveOp(src=src, dst=dst))

    # routers into product router folders
    routers_dir = repo_root / "backend/app/routers"
    for router_file, product in ROUTER_TO_PRODUCT.items():
        src = routers_dir / router_file
        dst = repo_root / BACKEND_PRODUCT_ROOT / product / "routers" / router_file
        moves.append(MoveOp(src=src, dst=dst))

    return dedupe_moves(moves)

def dedupe_moves(moves: list[MoveOp]) -> list[MoveOp]:
    seen: set[tuple[str, str]] = set()
    deduped: list[MoveOp] = []
    for move in moves:
        key = (normalize(move.src), normalize(move.dst))
        if key not in seen:
            seen.add(key)
            deduped.append(move)
    return deduped

def build_import_rewrites(repo_root: Path, moves: list[MoveOp]) -> dict[str, str]:
    """
    Build path-to-module rewrites.
    Example:
      app.services.compliance_service -> app.products.compliance.services.compliance_service
      ../components/InspectionReadiness -> ../products/compliance/components/InspectionReadiness
    """
    mapping: dict[str, str] = {}

    for move in moves:
        # backend python module rewrite
        if move.src.suffix in PYTHON_FILE_EXTS:
            old_module = path_to_python_module(repo_root, move.src)
            new_module = path_to_python_module(repo_root, move.dst)
            if old_module and new_module:
                mapping[old_module] = new_module

        # frontend TS/TSX import-ish rewrite
        if move.src.suffix in TS_FILE_EXTS:
            old_front = path_to_frontend_import_fragment(repo_root, move.src)
            new_front = path_to_frontend_import_fragment(repo_root, move.dst)
            if old_front and new_front:
                mapping[old_front] = new_front

    return mapping

def path_to_python_module(repo_root: Path, path: Path) -> str | None:
    try:
        rel = path.relative_to(repo_root)
    except ValueError:
        return None
    if rel.suffix != ".py":
        return None
    return rel.with_suffix("").as_posix().replace("/", ".")

def path_to_frontend_import_fragment(repo_root: Path, path: Path) -> str | None:
    try:
        rel = path.relative_to(repo_root / "frontend/src")
    except ValueError:
        return None
    return "@/{}".format(rel.with_suffix("").as_posix())

def rewrite_python_imports(text: str, mapping: dict[str, str]) -> str:
    for old, new in sorted(mapping.items(), key=lambda x: len(x[0]), reverse=True):
        # from old import X
        text = re.sub(
            rf"(?m)^(\s*from\s+){re.escape(old)}(\s+import\s+)",
            rf"\1{new}\2",
            text,
        )
        # import old
        text = re.sub(
            rf"(?m)^(\s*import\s+){re.escape(old)}(\b)",
            rf"\1{new}\2",
            text,
        )
        # import old as alias
        text = re.sub(
            rf"(?m)^(\s*import\s+){re.escape(old)}(\s+as\s+)",
            rf"\1{new}\2",
            text,
        )
    return text

def rewrite_frontend_imports(text: str, mapping: dict[str, str]) -> str:
    for old, new in sorted(mapping.items(), key=lambda x: len(x[0]), reverse=True):
        text = text.replace(f'"{old}"', f'"{new}"')
        text = text.replace(f"'{old}'", f"'{new}'")
    return text

def iter_code_files(repo_root: Path) -> Iterable[Path]:
    for base in [repo_root / "backend/app", repo_root / "frontend/src"]:
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_dir():
                continue
            if path.suffix in PYTHON_FILE_EXTS | TS_FILE_EXTS:
                yield path

def rewrite_all_imports(repo_root: Path, mapping: dict[str, str], dry_run: bool) -> list[str]:
    logs: list[str] = []
    for file_path in iter_code_files(repo_root):
        original = file_path.read_text(encoding="utf-8")
        updated = original

        if file_path.suffix in PYTHON_FILE_EXTS:
            updated = rewrite_python_imports(updated, mapping)
            updated = normalize_backend_relative_imports(file_path, updated)
        elif file_path.suffix in TS_FILE_EXTS:
            updated = rewrite_frontend_imports(updated, mapping)

        if updated != original:
            if dry_run:
                logs.append(f"DRY-RUN rewrite imports: {file_path}")
            else:
                file_path.write_text(updated, encoding="utf-8")
                logs.append(f"REWROTE imports: {file_path}")
    return logs

def normalize_backend_relative_imports(file_path: Path, text: str) -> str:
    """
    For files moved under backend/app/products/<product>/..., normalize a few common cases:
      from .foo import X
      from ..services.bar import Y
    This doesn't attempt a full AST transform; it handles the common top-level service import cases.
    """
    as_posix = file_path.as_posix()
    marker = "backend/app/products/"
    if marker not in as_posix:
        return text

    m = re.search(r"backend/app/products/([^/]+)/", as_posix)
    if not m:
        return text
    product = m.group(1)

    # same-product relative service imports to absolute app.products.<product>.services...
    text = re.sub(
        r"(?m)^(\s*from\s+)\.([A-Za-z0-9_\.]+)(\s+import\s+)",
        rf"\1app.products.{product}.services.\2\3",
        text,
    )

    # legacy top-level service imports stay explicit absolute
    text = re.sub(
        r"(?m)^(\s*from\s+)\.\.services\.([A-Za-z0-9_\.]+)(\s+import\s+)",
        r"\1app.services.\2\3",
        text,
    )

    return text

def ensure_product_packages(repo_root: Path, dry_run: bool) -> None:
    for product in PRODUCTS:
        ensure_package_dir(repo_root / BACKEND_PRODUCT_ROOT / product, dry_run)
        ensure_package_dir(repo_root / BACKEND_PRODUCT_ROOT / product / "services", dry_run)
        ensure_package_dir(repo_root / BACKEND_PRODUCT_ROOT / product / "routers", dry_run)
        ensure_package_dir(repo_root / FRONTEND_PRODUCT_ROOT / product, dry_run)
        if not dry_run:
            (repo_root / FRONTEND_PRODUCT_ROOT / product / "pages").mkdir(parents=True, exist_ok=True)
            (repo_root / FRONTEND_PRODUCT_ROOT / product / "components").mkdir(parents=True, exist_ok=True)

def write_manifest(repo_root: Path, moves: list[MoveOp], dry_run: bool) -> None:
    manifest = {
        "moves": [
            {"src": normalize(m.src), "dst": normalize(m.dst)}
            for m in moves
        ]
    }
    out = repo_root / "product_restructure_manifest.json"
    if dry_run:
        return
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally restructure OneHaven into product folders.")
    parser.add_argument("--repo-root", default=".", help="Path to onehaven_decision_engine repo root")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without modifying files")
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    dry_run = args.dry_run

    required = [
        repo_root / "backend/app/services",
        repo_root / "backend/app/routers",
        repo_root / "frontend/src/pages",
        repo_root / "frontend/src/components",
    ]
    for req in required:
        if not req.exists():
            raise SystemExit(f"Required path not found: {req}")

    ensure_product_packages(repo_root, dry_run=dry_run)

    moves = collect_explicit_moves(repo_root)
    logs: list[str] = []

    for move in moves:
        logs.append(safe_move(move.src, move.dst, dry_run=dry_run))

    mapping = build_import_rewrites(repo_root, moves)
    logs.extend(rewrite_all_imports(repo_root, mapping=mapping, dry_run=dry_run))

    write_manifest(repo_root, moves, dry_run=dry_run)

    print("=" * 80)
    print("PRODUCT RESTRUCTURE COMPLETE" if not dry_run else "PRODUCT RESTRUCTURE DRY-RUN COMPLETE")
    print("=" * 80)
    for line in logs:
        print(line)

    print("\nProducts created:")
    for product in PRODUCTS:
        print(f" - {product}")

    print("\nNext recommended checks:")
    print("  1. git status")
    print("  2. rg \"from app\\.services|import app\\.services|@/pages|@/components\" backend/app frontend/src")
    print("  3. backend tests / frontend build")
    print("  4. wire main.py and App.tsx route imports if needed")

if __name__ == "__main__":
    main()