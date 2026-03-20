# scripts/run_pipeline_snapshot.sh
#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8000/api}"
STATE="${STATE:-MI}"
COUNTY="${COUNTY:-wayne}"
CITY="${CITY:-Detroit}"
LIMIT="${LIMIT:-50}"

ORG_SLUG="${ORG_SLUG:-demo}"
USER_EMAIL="${USER_EMAIL:-austin@demo.local}"
USER_ROLE="${USER_ROLE:-owner}"

H_AUTH=(
  -H "X-Org-Slug: ${ORG_SLUG}"
  -H "X-User-Email: ${USER_EMAIL}"
  -H "X-User-Role: ${USER_ROLE}"
)

require() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }; }
require curl
require jq

echo "BASE=${BASE}"
echo "STATE=${STATE} COUNTY=${COUNTY} CITY=${CITY} LIMIT=${LIMIT}"
echo "ORG_SLUG=${ORG_SLUG} USER_EMAIL=${USER_EMAIL} USER_ROLE=${USER_ROLE}"
echo

echo "== 0) Health check =="
curl -sS "${BASE}/health" | jq
echo

echo "== 1) Ingestion overview =="
curl -sS "${BASE}/ingestion/overview" "${H_AUTH[@]}" | jq '{
  normal_path,
  legacy_snapshot_flow_enabled,
  ui_mode,
  daily_markets
}'
echo

echo "== 2) Resolve source =="
SOURCE_ID="$(
  curl -sS "${BASE}/ingestion/sources" "${H_AUTH[@]}" \
    | jq -r '.[] | select(.provider=="rentcast") | .id' \
    | head -n 1
)"

if [[ -z "${SOURCE_ID}" || "${SOURCE_ID}" == "null" ]]; then
  echo "FAILED: source_id missing from /ingestion/sources"
  exit 1
fi

echo "source_id=${SOURCE_ID}"
echo

echo "== 3) Run inline property-first sync =="
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
  }" \
  | tee /tmp/onehaven_pipeline_sync.json \
  | jq '{
    ok,
    queued,
    run_id,
    status,
    source_id,
    trigger_type,
    pipeline_outcome
  }'
echo

echo "== 4) Recent properties =="
curl -sS "${BASE}/properties?city=${CITY}&limit=10" \
  "${H_AUTH[@]}" \
  | tee /tmp/onehaven_pipeline_properties.json \
  | jq 'map({
      id,
      address,
      city,
      asking_price,
      projected_monthly_cashflow,
      dscr,
      crime_score,
      normalized_decision,
      current_workflow_stage
    })'
echo

echo "== 5) Dashboard rollups =="
curl -sS "${BASE}/dashboard/rollups?city=${CITY}" \
  "${H_AUTH[@]}" \
  | tee /tmp/onehaven_pipeline_rollups.json \
  | jq '{
    kpis,
    decision_counts,
    stage_counts,
    sample_rows: (.rows[:5] | map({
      property_id,
      address,
      asking_price,
      projected_monthly_cashflow,
      dscr,
      crime_score,
      decision,
      stage
    }))
  }'
echo

echo "✅ DONE"
echo "Saved:"
echo "  /tmp/onehaven_pipeline_sync.json"
echo "  /tmp/onehaven_pipeline_properties.json"
echo "  /tmp/onehaven_pipeline_rollups.json"
