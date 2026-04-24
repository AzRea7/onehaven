#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


PLAN_REPORTS = [
    "tools/repo/services-group-phase47-report.json",
    "tools/repo/domain-group-phase52-report.json",
    "tools/repo/frontend-components-phase64-report.json",
    "tools/repo/manual-frontend-components-phase68-report.json",
    "tools/repo/frontend-pages-phase72-report.json",
]


@dataclass
class Result:
    source: str
    target: str | None
    status: str
    reason: str


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def backup(root: Path, src: Path) -> None:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bdir = root / "tools/repo/_phase78_backups" / f"legacy_copies_{ts}" / src.parent.name
    bdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, bdir / src.name)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()

    results: list[Result] = []
    seen: set[str] = set()

    for report_rel in PLAN_REPORTS:
        report_path = root / report_rel
        if not report_path.exists():
            results.append(Result(report_rel, None, "skipped", "missing_report"))
            continue

        report = json.loads(report_path.read_text(encoding="utf-8"))

        for plan in report.get("plans", []):
            source = plan.get("source")
            target = plan.get("target")

            if not source or not target:
                continue

            if source in seen:
                continue
            seen.add(source)

            src = root / source
            dst = root / target

            if not src.exists():
                results.append(Result(source, target, "skipped", "source_missing"))
                continue

            if not dst.exists():
                results.append(Result(source, target, "skipped", "target_missing"))
                continue

            if sha(src) != sha(dst):
                results.append(Result(source, target, "skipped", "different"))
                continue

            if args.dry_run:
                results.append(Result(source, target, "would_remove", "identical"))
                continue

            backup(root, src)
            src.unlink()
            results.append(Result(source, target, "removed", "identical"))

    payload = {
        "phase": 78,
        "dry_run": args.dry_run,
        "summary": {
            "would_remove": sum(r.status == "would_remove" for r in results),
            "removed": sum(r.status == "removed" for r in results),
            "skipped": sum(r.status == "skipped" for r in results),
        },
        "results": [asdict(r) for r in results],
    }

    out = root / "tools/repo/remove-migrated-legacy-copies-phase78-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 78 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/remove-migrated-legacy-copies-phase78-report.json")


if __name__ == "__main__":
    main()