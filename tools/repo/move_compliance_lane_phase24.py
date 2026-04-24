#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE23_REPORT = "tools/repo/compliance-migration-phase23-report.json"


@dataclass
class MoveResult:
    source: str
    target: str | None
    area: str
    status: str
    reason: str


VALID_LANES = {
    "policy_sources",
    "policy_governance",
    "policy_coverage",
    "policy_assertions",
    "inspections",
    "compliance_engine",
    "documents",
    "router",
    "frontend_components",
    "frontend_pages",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move one compliance migration lane from Phase 23."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument(
        "--lane",
        required=True,
        choices=sorted(VALID_LANES),
        help="Compliance lane to move.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    parser.add_argument(
        "--remove-legacy",
        action="store_true",
        help="Delete legacy source file after copy. Do not use on first run.",
    )
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "products" / "compliance",
        repo_root / PHASE23_REPORT,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def read_phase23(repo_root: Path) -> dict:
    return json.loads((repo_root / PHASE23_REPORT).read_text(encoding="utf-8"))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    shutil.copy2(src, dst)


def write_report(repo_root: Path, lane: str, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / f"compliance-{lane}-move-phase24-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    phase23 = read_phase23(repo_root)
    legacy_root = repo_root

    records = phase23.get("records", [])
    selected = [
        record
        for record in records
        if record.get("area") == args.lane and record.get("suggested_target")
    ]

    results: list[MoveResult] = []

    for record in selected:
        source_rel = record["source"]
        target_rel = record["suggested_target"]
        src = legacy_root / source_rel
        dst = repo_root / target_rel

        if not src.exists():
            results.append(
                MoveResult(
                    source=source_rel,
                    target=target_rel,
                    area=args.lane,
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
                    area=args.lane,
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
                    area=args.lane,
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
                area=args.lane,
                status="moved",
                reason="copied_successfully",
            )
        )

    payload = {
        "phase": 24,
        "lane": args.lane,
        "dry_run": args.dry_run,
        "remove_legacy": args.remove_legacy,
        "candidate_count": len(selected),
        "would_move_count": sum(1 for r in results if r.status == "would_move"),
        "moved_count": sum(1 for r in results if r.status == "moved"),
        "skipped_count": sum(1 for r in results if r.status == "skipped"),
        "results": [asdict(r) for r in results],
    }

    write_report(repo_root, args.lane, payload)

    print("Phase 24 complete.")
    print(f"Lane: {args.lane}")
    print(f"Candidates: {payload['candidate_count']}")
    print(f"Would move: {payload['would_move_count']}")
    print(f"Moved: {payload['moved_count']}")
    print(f"Skipped: {payload['skipped_count']}")
    print(f"Report written to tools/repo/compliance-{args.lane}-move-phase24-report.json")


if __name__ == "__main__":
    main()