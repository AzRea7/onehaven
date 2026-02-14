#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
CSV_PATH="${1:-}"
STRATEGY="${STRATEGY:-section8}"
LIMIT="${LIMIT:-50}"

if [[ -z "${CSV_PATH}" ]]; then
  echo "Usage: ./scripts/run_pipeline_snapshot.sh path/to/deals.csv"
  exit 1
fi

require() { command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1" >&2; exit 1; }; }
require curl
require jq

echo "== Import CSV -> snapshot =="
SNAP="$(
  curl -sS -X POST "$BASE/imports/csv" \
    -H "Content-Type: text/csv" \
    --data-binary @"${CSV_PATH}" \
  | tee /tmp/import_resp.json \
  | jq -r '.snapshot_id'
)"

if [[ -z "${SNAP}" || "${SNAP}" == "null" ]]; then
  echo "FAILED: snapshot_id missing"
  cat /tmp/import_resp.json
  exit 1
fi

echo "snapshot_id=${SNAP}"

echo "== Enrich rent batch (snapshot=${SNAP}, limit=${LIMIT}, strategy=${STRATEGY}) =="
curl -sS -X POST "$BASE/rent/enrich/batch?snapshot_id=${SNAP}&limit=${LIMIT}&strategy=${STRATEGY}" \
  | tee /tmp/enrich_resp.json \
  | jq '{snapshot_id, attempted, enriched, stopped_early, stop_reason, errors_count:(.errors|length)}'

echo "== Evaluate snapshot (alias endpoint) =="
curl -sS -X POST "$BASE/evaluate/snapshot/${SNAP}?strategy=${STRATEGY}" \
  | tee /tmp/eval_resp.json \
  | jq '{snapshot_id, total_deals, pass_count, review_count, reject_count, errors}'

echo "== Top results =="
curl -sS "$BASE/evaluate/results?snapshot_id=${SNAP}&limit=25" \
  | tee /tmp/results_resp.json \
  | jq 'map({id, deal_id, decision, score, dscr, cash_flow, gross_rent_used, rent_cap_reason})'

echo "== Survivors (your actual “operator list”) =="
curl -sS "$BASE/deals/survivors?snapshot_id=${SNAP}&decision=PASS&min_dscr=1.2&min_cashflow=400&limit=25" \
  | tee /tmp/survivors_resp.json \
  | jq '.[:10] | map({deal_id, property_id, address, city, zip, score, dscr, cash_flow, gross_rent_used})'

echo "✅ DONE (snapshot_id=${SNAP})"
