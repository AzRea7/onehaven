#!/usr/bin/env python3
from __future__ import annotations

import argparse, json, re
from pathlib import Path

PHASE73_REPORT = "tools/repo/frontend-pages-phase73-report.json"
TEXT_EXTENSIONS = {".tsx", ".ts", ".js", ".jsx"}

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=".")
    return p.parse_args()

def collect_files(root: Path):
    skip = {".git", "node_modules", "dist", "build"}
    return [
        p for p in root.rglob("*")
        if p.is_file()
        and p.suffix in TEXT_EXTENSIONS
        and not any(part in skip for part in p.parts)
        and "tools/repo" not in p.as_posix()
    ]

def main():
    args = parse_args()
    root = Path(args.repo_root).resolve()
    report_path = root / PHASE73_REPORT

    if not report_path.exists():
        raise SystemExit(f"Missing report: {report_path}")

    report = json.loads(report_path.read_text(encoding="utf-8"))

    patterns = []
    seen = set()

    for item in report.get("results", []):
        source = item.get("source")
        target = item.get("target")
        if not source or not target:
            continue

        name = Path(source).stem
        for legacy in [
            f"@/pages/{name}",
            f"src/pages/{name}",
            f"../pages/{name}",
            f"./pages/{name}",
        ]:
            if legacy in seen:
                continue
            seen.add(legacy)
            patterns.append((re.compile(rf"""['"]{re.escape(legacy)}['"]"""), legacy))

    hits = []

    for file_path in collect_files(root):
        lines = file_path.read_text(encoding="utf-8").splitlines()
        rel = file_path.relative_to(root).as_posix()

        for regex, label in patterns:
            for i, line in enumerate(lines, 1):
                for match in regex.finditer(line):
                    hits.append({
                        "file": rel,
                        "pattern": label,
                        "match": match.group(0),
                        "line": i,
                    })

    payload = {
        "phase": 75,
        "legacy_hit_count": len(hits),
        "legacy_hits": hits,
        "status": "success" if not hits else "needs_attention",
    }

    out = root / "tools/repo/frontend-pages-phase75-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 75 complete.")
    print(f"Remaining legacy page import hits: {len(hits)}")
    print("Report written to tools/repo/frontend-pages-phase75-report.json")

if __name__ == "__main__":
    main()