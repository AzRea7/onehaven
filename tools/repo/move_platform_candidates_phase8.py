#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE7_REPORT = "tools/repo/backend-ownership-phase7-report.json"


@dataclass
class MoveResult:
    source: str
    target: str
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move high-confidence platform candidates from Phase 7."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    parser.add_argument(
        "--remove-legacy",
        action="store_true",
        help="Remove legacy source files after successful copy.",
    )
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / "onehaven_decision_engine",
        repo_root / "platform",
        repo_root / "tools" / "repo" / "backend-ownership-phase7-report.json",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit(
            "Repo root validation failed. Missing required paths:\n- "
            + "\n- ".join(missing)
        )
    return repo_root


def read_phase7(repo_root: Path) -> dict:
    path = repo_root / PHASE7_REPORT
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    shutil.copy2(src, dst)


def write_report(repo_root: Path, payload: dict) -> None:
    out = repo_root / "tools" / "repo" / "platform-move-phase8-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    legacy_root = repo_root / "onehaven_decision_engine"

    phase7 = read_phase7(repo_root)
    records = phase7.get("records", [])

    candidates = [
        record
        for record in records
        if record.get("bucket") == "platform"
        and record.get("confidence") in {"high", "medium"}
        and record.get("target")
    ]

    results: list[MoveResult] = []

    for record in candidates:
        source_rel = record["source"]
        target_rel = record["target"]

        src = legacy_root / source_rel
        dst = repo_root / target_rel

        if not src.exists():
            results.append(
                MoveResult(
                    source=source_rel,
                    target=target_rel,
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
                status="moved",
                reason="copied_successfully",
            )
        )

    summary = {
        "phase": 8,
        "description": "Move high-confidence platform candidates",
        "dry_run": args.dry_run,
        "remove_legacy": args.remove_legacy,
        "candidate_count": len(candidates),
        "would_move_count": sum(1 for r in results if r.status == "would_move"),
        "moved_count": sum(1 for r in results if r.status == "moved"),
        "skipped_count": sum(1 for r in results if r.status == "skipped"),
        "results": [asdict(r) for r in results],
    }

    write_report(repo_root, summary)

    print("Phase 8 complete.")
    print(f"Candidates: {summary['candidate_count']}")
    print(f"Would move: {summary['would_move_count']}")
    print(f"Moved: {summary['moved_count']}")
    print(f"Skipped: {summary['skipped_count']}")
    print("Report written to tools/repo/platform-move-phase8-report.json")


if __name__ == "__main__":
    main()