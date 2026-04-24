#!/usr/bin/env python3
from __future__ import annotations

import argparse, hashlib, json
from pathlib import Path

TARGETS = {
    "onehaven_decision_engine/backend/app/main.py": "apps/suite-api/app/main.py",
    "onehaven_decision_engine/frontend/src/App.tsx": "apps/suite-web/src/app/App.tsx",
    "onehaven_decision_engine/frontend/src/main.tsx": "apps/suite-web/src/bootstrap/main.tsx",
}

def sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(65536), b""):
            h.update(b)
    return h.hexdigest()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".")
    args = ap.parse_args()
    root = Path(args.repo_root).resolve()

    audits = []
    for source, target in TARGETS.items():
        src, dst = root / source, root / target
        sx, tx = src.exists(), dst.exists()
        sh = sha(src) if sx else None
        th = sha(dst) if tx else None

        if sx and tx and sh == th:
            status = "identical"
        elif sx and tx:
            status = "different"
        elif sx and not tx:
            status = "missing_target"
        elif not sx and tx:
            status = "already_migrated"
        else:
            status = "missing_both"

        audits.append({
            "source": source,
            "target": target,
            "status": status,
            "source_exists": sx,
            "target_exists": tx,
            "source_hash": sh,
            "target_hash": th,
        })

    payload = {
        "phase": 79,
        "summary": {
            "total": len(audits),
            "identical": sum(a["status"] == "identical" for a in audits),
            "different": sum(a["status"] == "different" for a in audits),
            "missing_target": sum(a["status"] == "missing_target" for a in audits),
        },
        "audits": audits,
    }

    out = root / "tools/repo/entrypoints-phase79-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("Phase 79 complete.")
    print(payload["summary"])
    print("Report written to tools/repo/entrypoints-phase79-report.json")

if __name__ == "__main__":
    main()