#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE15_REPORT = "tools/repo/frontend-ownership-phase15-report.json"


@dataclass
class AuditResult:
    product: str
    source: str
    target: str
    source_exists: bool
    target_exists: bool
    source_hash: str | None
    target_hash: str | None
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit skipped frontend product files by comparing legacy and target files."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--product",
        choices=["tenants", "intelligence", "ops", "compliance", "acquire", "all"],
        default="all",
        help="Limit audit to one product.",
    )
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> tuple[Path, Path]:
    repo_root = repo_root.resolve()
    legacy_root = repo_root / "onehaven_decision_engine"
    required = [
        legacy_root,
        repo_root / PHASE15_REPORT,
        repo_root / "products",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- " + "\n- ".join(missing)
        )
    return repo_root, legacy_root


def read_phase15(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE15_REPORT).read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def classify_status(source_exists: bool, target_exists: bool, source_hash: str | None, target_hash: str | None) -> tuple[str, str]:
    if not source_exists and not target_exists:
        return "missing_both", "neither_source_nor_target_exists"
    if source_exists and not target_exists:
        return "missing_target", "legacy_exists_but_target_missing"
    if not source_exists and target_exists:
        return "already_migrated", "target_exists_but_legacy_missing"
    if source_hash == target_hash:
        return "identical", "source_and_target_match"
    return "different", "source_and_target_differ"


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "frontend-product-audit-phase195-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root, legacy_root = ensure_repo_root(Path(args.repo_root))
    phase15 = read_phase15(repo_root)

    records = phase15.get("records", [])
    selected = []

    for record in records:
        if record.get("bucket") != "product":
            continue
        if record.get("confidence") != "medium":
            continue
        product = record.get("matched_product")
        if not product:
            continue
        if args.product != "all" and product != args.product:
            continue
        if not record.get("target"):
            continue
        selected.append(record)

    audits: list[AuditResult] = []

    for record in selected:
        product = record["matched_product"]
        source_rel = record["source"]
        target_rel = record["target"]

        src = legacy_root / source_rel
        dst = repo_root / target_rel

        source_exists = src.exists()
        target_exists = dst.exists()

        source_hash = sha256_file(src) if source_exists and src.is_file() else None
        target_hash = sha256_file(dst) if target_exists and dst.is_file() else None

        status, reason = classify_status(source_exists, target_exists, source_hash, target_hash)

        audits.append(
            AuditResult(
                product=product,
                source=source_rel,
                target=target_rel,
                source_exists=source_exists,
                target_exists=target_exists,
                source_hash=source_hash,
                target_hash=target_hash,
                status=status,
                reason=reason,
            )
        )

    payload = {
        "phase": "19.5",
        "selected_product": args.product,
        "summary": {
            "total": len(audits),
            "identical": sum(1 for a in audits if a.status == "identical"),
            "different": sum(1 for a in audits if a.status == "different"),
            "missing_target": sum(1 for a in audits if a.status == "missing_target"),
            "already_migrated": sum(1 for a in audits if a.status == "already_migrated"),
            "missing_both": sum(1 for a in audits if a.status == "missing_both"),
        },
        "audits": [asdict(a) for a in audits],
    }

    write_report(repo_root, payload)

    print("Phase 19.5 complete.")
    print(f"Audited files: {payload['summary']['total']}")
    print(f"Identical: {payload['summary']['identical']}")
    print(f"Different: {payload['summary']['different']}")
    print(f"Missing target: {payload['summary']['missing_target']}")
    print(f"Already migrated: {payload['summary']['already_migrated']}")
    print("Report written to tools/repo/frontend-product-audit-phase195-report.json")


if __name__ == "__main__":
    main()