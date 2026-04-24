#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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
        "investor_intelligence",
        "riskbadges",
        "panesummarycards",
        "shortlistboard",
        "deal",
        "equity",
        "cash",
        "evaluate",
    ],
    "acquire": [
        "acquire",
        "acquisition",
        "dealintake",
        "documentfieldreviewpanel",
        "acquisitiondeadlinepanel",
        "acquisitionfilters",
        "acquisitionparticipantspanel",
        "acquisitiontagbar",
    ],
    "compliance": [
        "compliance",
        "inspection",
        "jurisdiction",
        "policy",
        "readiness",
        "documentuploader",
        "documentstack",
        "photofindings",
    ],
    "tenants": [
        "tenant",
        "tenantspane",
        "tenantpipeline",
        "voucher",
        "applicant",
    ],
    "ops": [
        "management",
        "dashboard",
        "nextactions",
        "stageprogress",
        "photogallery",
        "photouploader",
        "propertyimage",
    ],
}

PLATFORM_KEYWORDS = {
    "auth": [
        "auth",
        "login",
        "register",
        "authflow",
    ],
    "shell": [
        "appshell",
        "shell",
        "appheader",
        "appfooter",
        "pageshell",
        "pagehero",
        "paneswitcher",
        "main.tsx",
        "app.tsx",
    ],
    "navigation": [
        "navigation",
        "routes",
        "switcher",
    ],
    "notifications": [
        "drawer",
        "errorsdrawer",
        "notification",
        "toast",
    ],
    "telemetry": [
        "analytics",
        "metrics",
    ],
    "org-context": [
        "org",
        "workspace",
    ],
    "file-upload": [
        "upload",
        "uploader",
    ],
}

PACKAGE_UI_KEYWORDS = [
    "glasscard",
    "surface",
    "spinner",
    "statcard",
    "statpill",
    "emptystate",
    "filterbar",
    "appselect",
    "virtuallist",
    "animatedbackdrop",
    "aurorabackground",
    "artwork",
    "golem",
    "kpicard",
]

EXPLICIT_MAP = {
    "frontend/src/lib/auth.tsx": "onehaven_onehaven_platform/frontend/src/auth/auth.tsx",
    "frontend/src/lib/authFlow.ts": "onehaven_onehaven_platform/frontend/src/auth/authFlow.ts",
    "frontend/src/App.tsx": "apps/suite-web/src/app/App.tsx",
    "frontend/src/main.tsx": "apps/suite-web/src/bootstrap/main.tsx",
    "frontend/src/styles.css": "apps/suite-web/src/app/styles.css",
}

SCAN_ROOTS = [
    "frontend/src/components",
    "frontend/src/lib",
    "frontend/src/pages",
    "frontend/src/products",
    "frontend/src/styles",
    "frontend/src/App.tsx",
    "frontend/src/main.tsx",
    "frontend/src/styles.css",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan frontend ownership for legacy OneHaven files."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> tuple[Path, Path]:
    repo_root = repo_root.resolve()
    legacy_root = repo_root / "onehaven_decision_engine"
    frontend_root = legacy_root / "frontend" / "src"
    if not legacy_root.exists():
        raise SystemExit(f"Missing legacy root: {legacy_root}")
    if not frontend_root.exists():
        raise SystemExit(f"Missing frontend root: {frontend_root}")
    return repo_root, legacy_root


def collect_files(legacy_root: Path) -> list[Path]:
    files: list[Path] = []

    for rel in SCAN_ROOTS:
        path = legacy_root / rel
        if not path.exists():
            continue

        if path.is_file():
            files.append(path)
            continue

        for p in path.rglob("*"):
            if p.is_file() and p.suffix.lower() in {".tsx", ".ts", ".js", ".jsx", ".css"}:
                files.append(p)

    return sorted(files)


def classify_product(rel_lower: str) -> tuple[str | None, str | None]:
    for product, keywords in PRODUCT_KEYWORDS.items():
        for keyword in keywords:
            if keyword in rel_lower:
                return product, keyword
    return None, None


def classify_platform(rel_lower: str) -> tuple[str | None, str | None]:
    for area, keywords in PLATFORM_KEYWORDS.items():
        for keyword in keywords:
            if keyword in rel_lower:
                return area, keyword
    return None, None


def classify_package(rel_lower: str) -> str | None:
    for keyword in PACKAGE_UI_KEYWORDS:
        if keyword in rel_lower:
            return keyword
    return None


def default_product_target(product: str, rel_path: str) -> str:
    filename = Path(rel_path).name
    rel_norm = rel_path.lower()

    if "/pages/" in rel_norm:
        return f"products/{product}/frontend/src/pages/{filename}"
    if "/components/" in rel_norm:
        return f"products/{product}/frontend/src/components/{filename}"
    if "/hooks/" in rel_norm:
        return f"products/{product}/frontend/src/hooks/{filename}"
    if "/api/" in rel_norm:
        return f"products/{product}/frontend/src/api/{filename}"
    return f"products/{product}/frontend/src/{filename}"


def default_platform_target(area: str, rel_path: str) -> str:
    filename = Path(rel_path).name
    return f"onehaven_onehaven_platform/frontend/src/{area}/{filename}"


def default_package_target(rel_path: str) -> str:
    filename = Path(rel_path).name
    if filename.endswith(".css"):
        return f"packages/ui/src/tokens/{filename}"
    return f"packages/ui/src/components/{filename}"


def classify_file(path: Path, legacy_root: Path) -> OwnershipRecord:
    rel_path = path.relative_to(legacy_root).as_posix()
    rel_lower = rel_path.lower()

    if rel_path in EXPLICIT_MAP:
        target = EXPLICIT_MAP[rel_path]
        bucket = "platform" if target.startswith("platform/") else "app"
        return OwnershipRecord(
            source=rel_path,
            bucket=bucket,
            target=target,
            confidence="high",
            reason="explicit_mapping",
        )

    package_keyword = classify_package(rel_lower)
    product, product_keyword = classify_product(rel_lower)
    platform_area, platform_keyword = classify_platform(rel_lower)

    if package_keyword and not product and not platform_area:
        return OwnershipRecord(
            source=rel_path,
            bucket="package",
            target=default_package_target(rel_path),
            confidence="medium",
            reason=f"matched_package_keyword:{package_keyword}",
        )

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
        "app": 0,
        "platform": 0,
        "product": 0,
        "package": 0,
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
    out = repo_root / "tools" / "repo" / "frontend-ownership-phase15-report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root, legacy_root = ensure_repo_root(Path(args.repo_root))

    files = collect_files(legacy_root)
    records = [classify_file(path, legacy_root) for path in files]

    payload = {
        "phase": 15,
        "description": "Frontend ownership planning report",
        "legacy_root": str(legacy_root),
        "summary": summarize(records),
        "records": [asdict(r) for r in records],
        "next_steps": [
            "Review manual_split files first",
            "Review unclear files second",
            "Move high-confidence app/platform/package files next",
            "Move product-owned frontend batches after that",
        ],
    }

    write_report(repo_root, payload)

    print("Phase 15 planning complete.")
    print("Report written to tools/repo/frontend-ownership-phase15-report.json")
    print(f"Files scanned: {len(records)}")
    print(f"App candidates: {payload['summary']['app']}")
    print(f"Platform candidates: {payload['summary']['platform']}")
    print(f"Package candidates: {payload['summary']['package']}")
    print(f"Product candidates: {payload['summary']['product']}")
    print(f"Manual split: {payload['summary']['manual_split']}")
    print(f"Unclear: {payload['summary']['unclear']}")


if __name__ == "__main__":
    main()