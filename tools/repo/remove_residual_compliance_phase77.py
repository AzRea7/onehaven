#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


PHASE76_REPORT = "tools/repo/residual-compliance-phase76-report.json"


@dataclass
class Result:
    source: str
    status: str
    reason: str


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def backup(root: Path, src: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bdir = root / "tools/repo/_phase77_backups" / f"residual_compliance_{ts}"
    bdir.mkdir(parents=True, exist_ok=True)
    dst = bdir / src.name
    shutil.copy2(src, dst)


def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    report_path = root / PHASE76_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))
    results = []

    for item in report.get("audits", []):
        src = root / item["source"]
        status = item["status"]

        if status not in {"identical", "already_migrated"}:
            results.append(Result(item["source"], "skipped", status))
            continue

        if not src.exists():
            results.append(Result(item["source"], "skipped", "source_missing"))
            continue

        if args.dry_run:
            results.append(Result(item["source"], "would_remove", status))
            continue

        backup(root, src)
        src.unlink()
        results.append(Result(item["source"], "removed", status))

    payload = {
        "phase": 77,
        "dry_run": args.dry_run,
        "summary": {
            "would_remove": sum(r.status == "would_remove" for r in results),
            "removed": sum(r.status == "removed" for r in results),
            "skipped": sum(r.status == "skipped" for r in results),
        },
        "results": [asdict(r) for r in results],
    }

    out = root / "tools/repo/residual-compliance-phase77-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 77 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/residual-compliance-phase77-report.json")


if __name__ == "__main__":
    main()