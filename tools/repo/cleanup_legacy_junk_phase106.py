#!/usr/bin/env python3
from pathlib import Path
import shutil

ROOT = Path("onehaven_decision_engine")

JUNK = [
    "frontend/node_modules",
    "frontend/dist",
    ".pytest_cache",
    "backend/.pytest_cache",
    "frontend/.pytest_cache",
]

FILES = [
    "frontend/tsconfig.tsbuildinfo",
    "backend/celerybeat-schedule",
]

def remove_path(p):
    if p.is_dir():
        shutil.rmtree(p, ignore_errors=True)
        print("removed dir:", p)
    elif p.exists():
        p.unlink()
        print("removed file:", p)

def main():
    for rel in JUNK:
        remove_path(ROOT / rel)

    for rel in FILES:
        remove_path(ROOT / rel)

    print("Phase 106 complete.")

if __name__ == "__main__":
    main()