# scripts/run_premium_poc_checks.sh
#!/usr/bin/env bash
set -euo pipefail

BACKEND_SERVICE="${BACKEND_SERVICE:-backend}"

echo "== Running premium PoC validation checks =="

if docker compose exec -T "${BACKEND_SERVICE}" test -f backend/tests/test_premium_poc_contracts.py; then
  docker compose exec -T "${BACKEND_SERVICE}" pytest -q backend/tests/test_premium_poc_contracts.py
else
  echo "premium poc test file not present yet; skipping"
fi

echo "✅ premium PoC checks complete"