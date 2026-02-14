#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
CSV_PATH="${1:-}"
STRATEGY="${STRATEGY:-section8}"
LIMIT="${LIMIT:-50}"

MIN_DSCR="${MIN_DSCR:-1.20}"
MIN_CASHFLOW="${MIN_CASHFLOW:-400}"
SURVIVOR_LIMIT="${SURVIVOR_LIMIT:-25}"

# ---- multitenant identity (defaults match your demo org/user) ----
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

if [[ -z "${CSV_PATH}" ]]; then
  echo "Usage: $0 path/to/zillow.csv"
  echo "Example (Git Bash): $0 \"/c/Users/austin/Downloads/zillow.csv\""
  exit 1
fi

if [[ ! -f "${CSV_PATH}" ]]; then
  echo "FAILED: CSV file not found at: ${CSV_PATH}"
  exit 1
fi

echo "BASE=${BASE}"
echo "CSV_PATH=${CSV_PATH}"
echo "STRATEGY=${STRATEGY}"
echo "LIMIT=${LIMIT}"
echo "ORG_SLUG=${ORG_SLUG} USER_EMAIL=${USER_EMAIL} USER_ROLE=${USER_ROLE}"
echo

echo "== 0) Health check =="
curl -sS "${BASE}/health" | jq
echo

echo "== 1) Who am I? =="
curl -sS "${BASE}/auth/me" "${H_AUTH[@]}" | jq
echo

echo "== 2) Import Zillow CSV -> snapshot =="

IMPORT_RESP="$(mktemp)"
curl -sS -X POST "${BASE}/import/zillow" \
  "${H_AUTH[@]}" \
  -F "file=@${CSV_PATH}" \
  | tee "${IMPORT_RESP}" \
  | jq .

SNAP="$(
  jq -r '
    .snapshot_id //
    .data.snapshot_id //
    .result.snapshot_id //
    .id //
    empty
  ' "${IMPORT_RESP}"
)"

if [[ -z "${SNAP}" || "${SNAP}" == "null" ]]; then
  echo "FAILED: snapshot_id missing from /import/zillow response"
  echo "Raw response:"
  cat "${IMPORT_RESP}"
  exit 1
fi

echo "snapshot_id=${SNAP}"
echo

echo "== 3) Enrich rent batch (snapshot=${SNAP}, limit=${LIMIT}, strategy=${STRATEGY}) =="
curl -sS -X POST "${BASE}/rent/enrich/batch?snapshot_id=${SNAP}&limit=${LIMIT}&strategy=${STRATEGY}" \
  "${H_AUTH[@]}" \
  | tee /tmp/enrich_resp.json \
  | jq '{snapshot_id, attempted, enriched, stopped_early, stop_reason, errors_count:(.errors|length)}'
echo

echo "== 4) Rent explain batch (persist=true) =="
curl -sS "${BASE}/rent/explain/batch?snapshot_id=${SNAP}&strategy=${STRATEGY}&limit=${LIMIT}&persist=true" \
  "${H_AUTH[@]}" \
  | tee /tmp/rent_explain_batch.json \
  | jq '{snapshot_id, strategy, attempted, explained, errors_count:(.errors|length)}'
echo

echo "== 5) Evaluate snapshot =="
curl -sS -X POST "${BASE}/evaluate/snapshot/${SNAP}?strategy=${STRATEGY}" \
  "${H_AUTH[@]}" \
  | tee /tmp/eval_resp.json \
  | jq '{snapshot_id, total_deals, pass_count, review_count, reject_count, errors}'
echo

echo "== 6) Top results =="
curl -sS "${BASE}/evaluate/results?snapshot_id=${SNAP}&limit=25" \
  "${H_AUTH[@]}" \
  | tee /tmp/results_resp.json \
  | jq 'map({id, deal_id, decision, score, dscr, cash_flow, gross_rent_used, rent_cap_reason})'
echo

echo "== 7) Survivors (operator list) =="
curl -sS "${BASE}/deals/survivors?snapshot_id=${SNAP}&decision=PASS&min_dscr=${MIN_DSCR}&min_cashflow=${MIN_CASHFLOW}&limit=${SURVIVOR_LIMIT}" \
  "${H_AUTH[@]}" \
  | tee /tmp/survivors_resp.json \
  | jq '(.[:10] | map({deal_id, property_id, address, city, zip, score, dscr, cash_flow, gross_rent_used}))'
echo

echo "✅ DONE snapshot_id=${SNAP}"
echo "Saved:"
echo "  /tmp/enrich_resp.json"
echo "  /tmp/rent_explain_batch.json"
echo "  /tmp/eval_resp.json"
echo "  /tmp/results_resp.json"
echo "  /tmp/survivors_resp.json"
