#!/usr/bin/env bash
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
STATE="${STATE:-MI}"

# ---- multitenant identity (defaults match your demo org/user) ----
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

echo "== Smoke test: create property -> enrich -> assert rent_assumption + comps + api usage =="
echo "ORG_SLUG=${ORG_SLUG} USER_EMAIL=${USER_EMAIL} USER_ROLE=${USER_ROLE}"

CITY="${CITY:-Detroit}"
ZIP="${ZIP:-48201}"

ADDR="SMOKE TEST $(date +%s) MAIN ST"
BED=3
BATH=1.0
SQFT=1100
YEAR=1950
HAS_GARAGE=false
PROP_TYPE="single_family"

echo "-- Creating property..."
PID="$(
  curl -sS -X POST "$BASE/properties" \
    "${H_AUTH[@]}" \
    -H "Content-Type: application/json" \
    -d "{
      \"address\": \"${ADDR}\",
      \"city\": \"${CITY}\",
      \"state\": \"${STATE}\",
      \"zip\": \"${ZIP}\",
      \"bedrooms\": ${BED},
      \"bathrooms\": ${BATH},
      \"square_feet\": ${SQFT},
      \"year_built\": ${YEAR},
      \"has_garage\": ${HAS_GARAGE},
      \"property_type\": \"${PROP_TYPE}\"
    }" | jq -r '.id'
)"

if [[ -z "${PID}" || "${PID}" == "null" ]]; then
  echo "FAILED: property id missing"
  exit 1
fi
echo "Created property_id=${PID}"

echo "-- Capture budget before (rentcast)..."
BEFORE_USED="$(
  curl -sS "$BASE/rent/enrich/budget?provider=rentcast" \
    "${H_AUTH[@]}" \
    | jq -r '.used'
)"

echo "-- Enrich rent (section8)..."
curl -sS -X POST "$BASE/rent/enrich/${PID}?strategy=section8" \
  "${H_AUTH[@]}" \
  | jq '.' >/tmp/smoke_enrich.json

echo "-- Fetch property..."
curl -sS "$BASE/properties/${PID}" \
  "${H_AUTH[@]}" \
  | jq '.' >/tmp/smoke_property.json

echo "-- Assertions..."

# rent_assumption exists
RA_EXISTS="$(jq -r '.rent_assumption != null' /tmp/smoke_property.json)"
[[ "${RA_EXISTS}" == "true" ]] || { echo "FAILED: rent_assumption missing"; exit 1; }

# comps count > 0
COMPS_COUNT="$(jq -r '.rent_comps | length' /tmp/smoke_property.json)"
if ! [[ "${COMPS_COUNT}" =~ ^[0-9]+$ ]]; then
  echo "FAILED: comps count not numeric: ${COMPS_COUNT}"
  exit 1
fi
(( COMPS_COUNT > 0 )) || { echo "FAILED: comps count is 0"; exit 1; }

# required fields non-null
MEDIAN="$(jq -r '.rent_assumption.rent_reasonableness_comp' /tmp/smoke_property.json)"
MARKET="$(jq -r '.rent_assumption.market_rent_estimate' /tmp/smoke_property.json)"
FMR="$(jq -r '.rent_assumption.section8_fmr' /tmp/smoke_property.json)"
CEIL="$(jq -r '.rent_assumption.approved_rent_ceiling' /tmp/smoke_property.json)"

[[ "${MEDIAN}" != "null" ]] || { echo "FAILED: rent_reasonableness_comp is null"; exit 1; }
[[ "${MARKET}" != "null" ]] || { echo "FAILED: market_rent_estimate is null"; exit 1; }
[[ "${FMR}" != "null" ]] || { echo "FAILED: section8_fmr is null"; exit 1; }
[[ "${CEIL}" != "null" ]] || { echo "FAILED: approved_rent_ceiling is null"; exit 1; }

echo "-- Capture budget after (rentcast)..."
AFTER_USED="$(
  curl -sS "$BASE/rent/enrich/budget?provider=rentcast" \
    "${H_AUTH[@]}" \
    | jq -r '.used'
)"

if ! [[ "${BEFORE_USED}" =~ ^[0-9]+$ && "${AFTER_USED}" =~ ^[0-9]+$ ]]; then
  echo "FAILED: api usage used not numeric (before=${BEFORE_USED}, after=${AFTER_USED})"
  exit 1
fi

(( AFTER_USED > BEFORE_USED )) || {
  echo "FAILED: expected api usage to increment (before=${BEFORE_USED}, after=${AFTER_USED})"
  exit 1
}

echo "✅ PASS"
echo "property_id=${PID}"
echo "comps_count=${COMPS_COUNT}"
echo "before_used=${BEFORE_USED} after_used=${AFTER_USED}"
