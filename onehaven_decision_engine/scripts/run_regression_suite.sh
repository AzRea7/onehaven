# scripts/run_regression_suite.sh
#!/usr/bin/env bash
set -euo pipefail

ROOT="${ROOT:-.}"
BACKEND_SERVICE="${BACKEND_SERVICE:-backend}"

echo "== Running targeted regression suites =="

docker compose exec -T "${BACKEND_SERVICE}" pytest -q \
  backend/tests/steps/step_8_agent_lifecycle/test_agent_engine_runtime_health.py

docker compose exec -T "${BACKEND_SERVICE}" pytest -q \
  backend/tests/steps/step_10_automated_ingestion/test_ingestion_daily_sync_defaults.py \
  backend/tests/steps/step_10_automated_ingestion/test_ingestion_sync_now.py \
  backend/tests/steps/step_10_automated_ingestion/test_ingestion_pipeline_post_import.py

echo "✅ regression suites passed"