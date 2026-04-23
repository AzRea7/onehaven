#!/usr/bin/env python3
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_APP = REPO_ROOT / "backend" / "app"

OLD = "app.services.workflow_gate_service"
NEW = "app.products.compliance.services.workflow_gate_service"

def main():
    changed = 0

    for path in BACKEND_APP.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if OLD in text:
            updated = text.replace(OLD, NEW)
            path.write_text(updated, encoding="utf-8")
            print(f"UPDATED: {path.relative_to(REPO_ROOT)}")
            changed += 1

    print(f"\nDone. Updated {changed} file(s).")
    print("Next:")
    print("  python -m compileall backend/app")

if __name__ == "__main__":
    main()