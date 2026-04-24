#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


PHASE7 = "tools/repo/backend-ownership-phase7-report.json"
PHASE15 = "tools/repo/frontend-ownership-phase15-report.json"
PHASE23 = "tools/repo/compliance-migration-phase23-report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate remaining manual/unclear leftovers across migration reports."
    )
    parser.add_argument("--repo-root", default=".", help="Repo root.")
    return parser.parse_args()


def ensure_repo_root(repo_root: Path) -> Path:
    repo_root = repo_root.resolve()
    required = [
        repo_root / PHASE7,
        repo_root / PHASE15,
        repo_root / PHASE23,
        repo_root / "tools" / "repo",
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise SystemExit("Missing required paths:\n- " + "\n- ".join(missing))
    return repo_root


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    args = parse_args()
    repo_root = ensure_repo_root(Path(args.repo_root))

    phase7 = read_json(repo_root / PHASE7)
    phase15 = read_json(repo_root / PHASE15)
    phase23 = read_json(repo_root / PHASE23)

    backend_manual = []
    backend_unclear = []
    for rec in phase7.get("records", []):
        if rec.get("bucket") == "manual_split":
            backend_manual.append(rec)
        elif rec.get("bucket") == "unclear":
            backend_unclear.append(rec)

    frontend_manual = []
    frontend_unclear = []
    for rec in phase15.get("records", []):
        if rec.get("bucket") == "manual_split":
            frontend_manual.append(rec)
        elif rec.get("bucket") == "unclear":
            frontend_unclear.append(rec)

    compliance_manual = []
    for rec in phase23.get("records", []):
        if rec.get("area") == "manual_review":
            compliance_manual.append(rec)

    payload = {
        "phase": 33,
        "summary": {
            "backend_manual_split": len(backend_manual),
            "backend_unclear": len(backend_unclear),
            "frontend_manual_split": len(frontend_manual),
            "frontend_unclear": len(frontend_unclear),
            "compliance_manual_review": len(compliance_manual),
            "total_leftovers": (
                len(backend_manual)
                + len(backend_unclear)
                + len(frontend_manual)
                + len(frontend_unclear)
                + len(compliance_manual)
            ),
        },
        "backend_manual_split": backend_manual,
        "backend_unclear": backend_unclear,
        "frontend_manual_split": frontend_manual,
        "frontend_unclear": frontend_unclear,
        "compliance_manual_review": compliance_manual,
    }

    out = repo_root / "tools" / "repo" / "remaining-leftovers-phase33-report.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Phase 33 complete.")
    print(f"Backend manual_split: {len(backend_manual)}")
    print(f"Backend unclear: {len(backend_unclear)}")
    print(f"Frontend manual_split: {len(frontend_manual)}")
    print(f"Frontend unclear: {len(frontend_unclear)}")
    print(f"Compliance manual_review: {len(compliance_manual)}")
    print(f"Total leftovers: {payload['summary']['total_leftovers']}")
    print("Report written to tools/repo/remaining-leftovers-phase33-report.json")


if __name__ == "__main__":
    main()