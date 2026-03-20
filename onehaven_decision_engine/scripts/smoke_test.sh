# scripts/smoke_test.sh
#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8000/api}"
STATE="${STATE:-MI}"

ORG_SLUG="${ORG_SLUG:-demo}"
USER_EMAIL="${USER_EMAIL:-austin@demo.local}"
USER_ROLE="${USER_ROLE:-owner}"

H_AUTH=(
  -H "X-Org-Slug: ${ORG_SLUG}"
  -H "X-User-Email: ${USER_EMAIL}"
  -H "X-User-Role: ${USER_ROLE}"
)

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }
}

require curl
require jq

CITY="${CITY:-Detroit}"
COUNTY="${COUNTY:-wayne}"
LIMIT="${LIMIT:-25}"

echo "== Smoke test: property-first ingestion sync -> property metrics -> dashboard contract =="
echo "ORG_SLUG=${ORG_SLUG} USER_EMAIL=${USER_EMAIL} USER_ROLE=${USER_ROLE}"

echo "-- Health..."
curl -sS "${BASE}/health" | jq .

echo "-- Ensure ingestion overview..."
curl -sS "${BASE}/ingestion/overview" "${H_AUTH[@]}" | jq '{normal_path, legacy_snapshot_flow_enabled, ui_mode}'

SOURCE_ID="$(
  curl -sS "${BASE}/ingestion/sources" "${H_AUTH[@]}" \
    | jq -r '.[] | select(.provider=="rentcast") | .id' \
    | head -n 1
)"

if [[ -z "${SOURCE_ID}" || "${SOURCE_ID}" == "null" ]]; then
  echo "FAILED: could not resolve an enabled ingestion source"
  exit 1
fi

echo "-- Execute inline ingestion sync..."
curl -sS -X POST "${BASE}/ingestion/sources/${SOURCE_ID}/sync" \
  "${H_AUTH[@]}" \
  -H "Content-Type: application/json" \
  -d "{
    \"trigger_type\": \"manual\",
    \"execute_inline\": true,
    \"state\": \"${STATE}\",
    \"county\": \"${COUNTY}\",
    \"city\": \"${CITY}\",
    \"limit\": ${LIMIT}
  }" | tee /tmp/onehaven_step4_sync.json | jq '{
    ok,
    queued,
    run_id,
    status,
    normal_path,
    pipeline_outcome
  }'

SYNC_OK="$(jq -r '.ok' /tmp/onehaven_step4_sync.json)"
[[ "${SYNC_OK}" == "true" ]] || { echo "FAILED: sync response not ok"; exit 1; }

RUN_STATUS="$(jq -r '.status' /tmp/onehaven_step4_sync.json)"
[[ "${RUN_STATUS}" == "success" ]] || { echo "FAILED: sync did not finish successfully"; exit 1; }

echo "-- Property list contract..."
curl -sS "${BASE}/properties?city=${CITY}&limit=5" \
  "${H_AUTH[@]}" \
  | tee /tmp/onehaven_step4_properties.json \
  | jq '.[0] // {}'

PROP_COUNT="$(jq 'length' /tmp/onehaven_step4_properties.json)"
if ! [[ "${PROP_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "FAILED: property list length is not numeric"
  exit 1
fi

if (( PROP_COUNT > 0 )); then
  jq -e '.[0] | has("asking_price") and has("projected_monthly_cashflow") and has("dscr") and has("crime_score") and has("normalized_decision") and has("current_workflow_stage")' \
    /tmp/onehaven_step4_properties.json >/dev/null \
    || { echo "FAILED: property list row missing required workflow/metrics fields"; exit 1; }
fi

echo "-- Dashboard rollups contract..."
curl -sS "${BASE}/dashboard/rollups?city=${CITY}" \
  "${H_AUTH[@]}" \
  | tee /tmp/onehaven_step4_rollups.json \
  | jq '{kpis, decision_counts, stage_counts}'

jq -e '
  (.decision_counts | keys | all(. == "GOOD" or . == "REVIEW" or . == "REJECT"))
' /tmp/onehaven_step4_rollups.json >/dev/null \
  || { echo "FAILED: dashboard rollups exposed non-normalized decision group"; exit 1; }

echo "✅ PASS"
echo "source_id=${SOURCE_ID}"
echo "saved=/tmp/onehaven_step4_sync.json /tmp/onehaven_step4_properties.json /tmp/onehaven_step4_rollups.json"
