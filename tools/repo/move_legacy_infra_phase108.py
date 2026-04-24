#!/usr/bin/env python3
from pathlib import Path
import shutil

ROOT = Path(".")
LEGACY = ROOT / "onehaven_decision_engine"

FILES = {
    "docker-compose.yml": "infra/docker/docker-compose.yml",
    ".env": "infra/env/.env",
    ".env.example": "infra/env/.env.example",
}

DIRS = {
    "frontend": "apps/suite_web",
    "backend": "apps/suite_api_legacy_backup",
}

def copy_file(src, dst):
    if not src.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    print("copied file:", src)

def copy_dir(src, dst):
    if not src.exists():
        return
    shutil.copytree(src, dst, dirs_exist_ok=True)
    print("copied dir:", src)

def main():
    for src, dst in FILES.items():
        copy_file(LEGACY / src, ROOT / dst)

    for src, dst in DIRS.items():
        copy_dir(LEGACY / src, ROOT / dst)

    print("Phase 108 complete.")

if __name__ == "__main__":
    main()