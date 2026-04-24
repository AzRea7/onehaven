#!/usr/bin/env python3
from pathlib import Path
import shutil

ROOT = Path(".")
LEGACY = ROOT / "onehaven_decision_engine"

MAPPINGS = {
    "backend/data/pdfs": "storage/nspire",
    "backend/policy_raw": "storage/policy_raw",
    "backend/data/acquisition_uploads": "storage/acquisition_docs",
}

def move_dir(src, dst):
    if not src.exists():
        return
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)
    print("moved:", src, "→", dst)

def main():
    for src_rel, dst_rel in MAPPINGS.items():
        move_dir(LEGACY / src_rel, ROOT / dst_rel)

    print("Phase 107 complete.")

if __name__ == "__main__":
    main()