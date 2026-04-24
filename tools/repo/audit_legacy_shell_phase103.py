#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

LEGACY_ROOT = Path("onehaven_decision_engine")

def main():
    root = Path(".").resolve()
    legacy = root / LEGACY_ROOT

    files = []
    if legacy.exists():
        for p in legacy.rglob("*"):
            if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc":
                files.append(p.relative_to(root).as_posix())

    grouped = {}
    for f in files:
        parts = f.split("/")
        key = "/".join(parts[:2]) if len(parts) >= 2 else f
        grouped.setdefault(key, 0)
        grouped[key] += 1

    payload = {
        "phase": 103,
        "legacy_root": str(LEGACY_ROOT),
        "summary": {
            "files": len(files),
            "groups": len(grouped),
        },
        "groups": grouped,
        "files": files,
    }

    out = root / "tools/repo/legacy-shell-phase103-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 103 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/legacy-shell-phase103-report.json")

if __name__ == "__main__":
    main()