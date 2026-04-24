#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class OwnershipRecord:
    source: str
    bucket: str
    target: str | None
    confidence: str
    reason: str
    matched_product: str | None = None


PRODUCT_KEYWORDS = {
    "intelligence": [
        "deal",
        "underwrite",
        "underwriting",
        "cashflow",
        "equity",
        "valuation",
        "rent_learning",
        "rent_explain",
        "risk_scoring",
        "pane_summary_snapshot",
        "deal_intelligence",
    ],
    "acquire": [
        "acquisition",
        "document_review",
        "document_parsing",
        "participant",
        "deadline",
        "tag_service",
        "workspace",
    ],
    "compliance": [
        "compliance",
        "inspection",
        "hqs",
        "nspire",
        "policy",
        "jurisdiction",
        "lockout",
        "coverage",
        "truth_resolution",
        "evidence",
        "readiness",
        "workflow_gate",
    ],
    "tenants": [
        "tenant",
        "voucher",
        "applicant",
        "tenant_match",
        "inspector_communication",
    ],
    "ops": [
        "ops",
        "inventory_snapshot",
        "dashboard_rollups",
        "next_actions",
        "stage_guard",
        "property_stage",
        "turnover",
    ],
}

PLATFORM_KEYWORDS = {
    "identity": [
        "auth",
        "api_key",
        "ownership",
    ],
    "config": [
        "config",
    ],
    "db": [
        "db",
    ],
    "observability": [
        "logging",
        "request_id",
        "structured_logging",
        "runtime_metrics",
        "metrics",
        "audit",
        "agent_trace",
    ],
    "workflow": [
        "workflow",
        "stage_guard",
        "pane_routing",
        "onboarding_flows",
    ],
    "notifications": [
        "notification",
        "reminder",
        "communication",
    ],
    "files": [
        "document",
        "photo",
        "virus_scanning",
        "upload",
    ],
    "search": [
        "search",
        "catalog",
    ],
    "jobs": [
        "scheduler",
        "task",
        "worker",
        "queue",
        "celery",
    ],
    "integrations": [
        "rentcast",
        "zillow",
        "hud",
        "govinfo",
        "federal_register",
        "lm_studio",
        "tax",
        "geocoding",
        "geocode",
        "public_tax",
        "crime",
        "offender",
    ],
    "orgs": [
        "org",
        "plan",
        "usage",
        "locks",
    ],
}

EXPLICIT_PLATFORM_MAP = {
    "backend/app/auth.py": "onehaven_onehaven_platform/backend/src/identity/interfaces/auth.py",
    "backend/app/config.py": "onehaven_onehaven_platform/backend/src/config/config.py",
    "backend/app/db.py": "onehaven_onehaven_platform/backend/src/db/db.py",
    "backend/app/logging_config.py": "onehaven_onehaven_platform/backend/src/observability/logging_config.py",
    "backend/app/middleware/request_id.py": "onehaven_onehaven_platform/backend/src/observability/request_id.py",
    "backend/app/middleware/structured_logging.py": "onehaven_onehaven_platform/backend/src/observability/structured_logging.py",
    "backend/app/routers/auth.py": "onehaven_onehaven_platform/backend/src/identity/interfaces/auth_router.py",
    "backend/app/routers/health.py": "apps/suite-api/app/api/health/health.py",
}

SCAN_ROOTS = [
    "backend/app/services",
    "backend/app/routers",
    "backend/app/domain",
    "backend/app/middleware",
    "backend/app/clients",
    "backend/app/integrations",
    "backend/app/tasks",
    "backend/app/workers",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan backend ownership for legacy OneHaven files."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> tuple[Path, Path]:
    repo_root = repo_root.resolve()
    legacy_root = repo_root / "onehaven_decision_engine"
    if not legacy_root.exists():
        raise SystemExit(f"Missing legacy root: {legacy_root}")
    if not (legacy_root / "backend" / "app").exists():
        raise SystemExit(f"Missing backend app root: {legacy_root / 'backend' / 'app'}")
    return repo_root, legacy_root


def collect_files(legacy_root: Path) -> list[Path]:
    files: list[Path] = []
    for rel_root in SCAN_ROOTS:
        root = legacy_root / rel_root
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if p.is_file() and p.suffix == ".py":
                files.append(p)
    return sorted(files)


def normalize_name(path: Path, legacy_root: Path) -> str:
    return path.relative_to(legacy_root).as_posix().lower()


def classify_product(rel_path: str) -> tuple[str | None, str | None]:
    for product, keywords in PRODUCT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in rel_path:
                return product, keyword
    return None, None


def classify_platform(rel_path: str) -> tuple[str | None, str | None]:
    for area, keywords in PLATFORM_KEYWORDS.items():
        for keyword in keywords:
            if keyword in rel_path:
                return area, keyword
    return None, None


def default_product_target(product: str, rel_path: str) -> str:
    filename = Path(rel_path).name
    if "/routers/" in rel_path:
        return f"products/{product}/backend/src/routers/{filename}"
    if "/services/" in rel_path:
        return f"products/{product}/backend/src/services/{filename}"
    if "/domain/" in rel_path:
        return f"products/{product}/backend/src/domain/{filename}"
    return f"products/{product}/backend/src/{filename}"


def default_platform_target(area: str, rel_path: str) -> str:
    filename = Path(rel_path).name
    return f"onehaven_onehaven_platform/backend/src/{area}/{filename}"


def classify_file(path: Path, legacy_root: Path) -> OwnershipRecord:
    rel_path = path.relative_to(legacy_root).as_posix()

    if rel_path in EXPLICIT_PLATFORM_MAP:
        return OwnershipRecord(
            source=rel_path,
            bucket="platform",
            target=EXPLICIT_PLATFORM_MAP[rel_path],
            confidence="high",
            reason="explicit_platform_mapping",
        )

    rel_lower = rel_path.lower()

    product, product_keyword = classify_product(rel_lower)
    platform_area, platform_keyword = classify_platform(rel_lower)

    if product and not platform_area:
        return OwnershipRecord(
            source=rel_path,
            bucket="product",
            target=default_product_target(product, rel_path),
            confidence="medium",
            reason=f"matched_product_keyword:{product_keyword}",
            matched_product=product,
        )

    if platform_area and not product:
        return OwnershipRecord(
            source=rel_path,
            bucket="platform",
            target=default_platform_target(platform_area, rel_path),
            confidence="medium",
            reason=f"matched_platform_keyword:{platform_keyword}",
        )

    if product and platform_area:
        return OwnershipRecord(
            source=rel_path,
            bucket="manual_split",
            target=None,
            confidence="low",
            reason=f"matched_product:{product_keyword};matched_platform:{platform_keyword}",
            matched_product=product,
        )

    return OwnershipRecord(
        source=rel_path,
        bucket="unclear",
        target=None,
        confidence="low",
        reason="no_keyword_match",
    )


def summarize(records: list[OwnershipRecord]) -> dict:
    summary = {
        "platform": 0,
        "product": 0,
        "manual_split": 0,
        "unclear": 0,
        "products": {
            "intelligence": 0,
            "acquire": 0,
            "compliance": 0,
            "tenants": 0,
            "ops": 0,
        },
    }
    for rec in records:
        summary[rec.bucket] += 1
        if rec.matched_product:
            summary["products"][rec.matched_product] += 1
    return summary


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "backend-ownership-phase7-report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root, legacy_root = ensure_repo_root(Path(args.repo_root))

    files = collect_files(legacy_root)
    records = [classify_file(path, legacy_root) for path in files]

    payload = {
        "phase": 7,
        "description": "Backend ownership planning report",
        "legacy_root": str(legacy_root),
        "summary": summarize(records),
        "records": [asdict(r) for r in records],
        "next_steps": [
            "Review manual_split files first",
            "Review unclear files second",
            "Move high-confidence platform files next",
            "Move product-owned service batches after that",
        ],
    }

    write_report(repo_root, payload)

    print("Phase 7 planning complete.")
    print("Report written to tools/repo/backend-ownership-phase7-report.json")
    print(f"Files scanned: {len(records)}")
    print(f"Platform candidates: {payload['summary']['platform']}")
    print(f"Product candidates: {payload['summary']['product']}")
    print(f"Manual split: {payload['summary']['manual_split']}")
    print(f"Unclear: {payload['summary']['unclear']}")


if __name__ == "__main__":
    main()