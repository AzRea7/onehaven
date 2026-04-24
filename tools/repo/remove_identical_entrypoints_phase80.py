#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, shutil
from datetime import datetime
from pathlib import Path

REPORT = "tools/repo/entrypoints-phase79-report.json"

def backup(root: Path, src: Path):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    bdir = root / "tools/repo/_phase80_backups" / f"entrypoints_{ts}"
    bdir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, bdir / src.name)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    root = Path(args.repo_root).resolve()

    report = json.loads((root / REPORT).read_text(encoding="utf-8"))
    removed = skipped = would = 0

    for item in report["audits"]:
        src = root / item["source"]

        if item["status"] != "identical":
            skipped += 1
            continue

        if not src.exists():
            skipped += 1
            continue

        if args.dry_run:
            print(f"[DRY RUN] Would remove: {src}")
            would += 1
            continue

        backup(root, src)
        src.unlink()
        print(f"Removed: {src}")
        removed += 1

    print("Phase 80 complete.")
    print({"would_remove": would, "removed": removed, "skipped": skipped})

if __name__ == "__main__":
    main()