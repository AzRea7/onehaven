#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from pathlib import Path


PHASE36_REPORT = "tools/repo/router-group-phase36-report.json"


@dataclass
class MoveResult:
    source: str
    target: str | None
    owner: str
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Move mapped routers from Phase 36.")
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only.")
    parser.add_argument("--remove-legacy", action="store_true", help="Delete source after copy.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE36_REPORT,
        repo_root / "onehaven_decision_engine",
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def copy_file(src: Path, dst: Path) -> None:
    ensure_parent(dst)
    shutil.copy2(src, dst)


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))
    report = read_json(repo_root / PHASE36_REPORT)

    results: list[MoveResult] = []

    for plan in report.get("plans", []):
        source = plan["source"]
        target = plan["target"]
        owner = plan["owner"]

        if not target:
            results.append(MoveResult(source, target, owner, "skipped", "manual_review"))
            continue

        src = repo_root / source
        dst = repo_root / target

        if not src.exists():
            results.append(MoveResult(source, target, owner, "skipped", "source_missing"))
            continue

        if dst.exists():
            results.append(MoveResult(source, target, owner, "skipped", "target_exists"))
            continue

        if args.dry_run:
            results.append(MoveResult(source, target, owner, "would_move", "dry_run"))
            continue

        copy_file(src, dst)

        if args.remove_legacy:
            src.unlink()

        results.append(MoveResult(source, target, owner, "moved", "copied_successfully"))

    payload = {
        "phase": 37,
        "dry_run": args.dry_run,
        "remove_legacy": args.remove_legacy,
        "results": [asdict(r) for r in results],
        "summary": {
            "would_move": sum(1 for r in results if r.status == "would_move"),
            "moved": sum(1 for r in results if r.status == "moved"),
            "skipped": sum(1 for r in results if r.status == "skipped"),
        },
    }

    out = repo_root / "tools" / "repo" / "router-group-phase37-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 37 complete.")
    print(f"Would move: {payload['summary']['would_move']}")
    print(f"Moved: {payload['summary']['moved']}")
    print(f"Skipped: {payload['summary']['skipped']}")
    print("Report written to tools/repo/router-group-phase37-report.json")


if __name__ == "__main__":
    main()