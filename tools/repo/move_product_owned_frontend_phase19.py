#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE15_REPORT = "tools/repo/frontend-ownership-phase15-report.json"


@dataclass
class MoveResult:
    source: str
    target: str
    product: str
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move product-owned frontend files from Phase 15 report."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--product",
        choices=["intelligence", "acquire", "compliance", "tenants", "ops", "all"],
        default="all",
        help="Move only one product bucket or all.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    parser.add_argument(
        "--remove-legacy",
        action="store_true",
        help="Delete legacy files after copy. Do not use on first run.",
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
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root, legacy_root


def read_phase15(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE15_REPORT).read_text(encoding="utf-8"))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    shutil.copy2(src, dst)


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools/repo/product-owned-frontend-phase19-report.json"
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

    results: list[MoveResult] = []

    for record in selected:
        source_rel = record["source"]
        target_rel = record["target"]
        product = record["matched_product"]

        src = legacy_root / source_rel
        dst = repo_root / target_rel

        if not src.exists():
            results.append(
                MoveResult(
                    source=source_rel,
                    target=target_rel,
                    product=product,
                    status="skipped",
                    reason="source_missing",
                )
            )
            continue

        if dst.exists():
            results.append(
                MoveResult(
                    source=source_rel,
                    target=target_rel,
                    product=product,
                    status="skipped",
                    reason="target_exists",
                )
            )
            continue

        if args.dry_run:
            results.append(
                MoveResult(
                    source=source_rel,
                    target=target_rel,
                    product=product,
                    status="would_move",
                    reason="dry_run",
                )
            )
            continue

        copy_file(src, dst)

        if args.remove_legacy:
            src.unlink()

        results.append(
            MoveResult(
                source=source_rel,
                target=target_rel,
                product=product,
                status="moved",
                reason="copied_successfully",
            )
        )

    payload = {
        "phase": 19,
        "description": "Move product-owned frontend files",
        "dry_run": args.dry_run,
        "remove_legacy": args.remove_legacy,
        "selected_product": args.product,
        "candidate_count": len(selected),
        "would_move_count": sum(1 for r in results if r.status == "would_move"),
        "moved_count": sum(1 for r in results if r.status == "moved"),
        "skipped_count": sum(1 for r in results if r.status == "skipped"),
        "results": [asdict(r) for r in results],
    }

    write_report(repo_root, payload)

    print("Phase 19 complete.")
    print(f"Candidates: {payload['candidate_count']}")
    print(f"Would move: {payload['would_move_count']}")
    print(f"Moved: {payload['moved_count']}")
    print(f"Skipped: {payload['skipped_count']}")
    print("Report written to tools/repo/product-owned-frontend-phase19-report.json")


if __name__ == "__main__":
    main()