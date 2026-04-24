# Step 8 — Drilldowns review and cleanup audit

## Goal
Decide whether standalone drilldown pages still add value. If not, remove them later and absorb the useful information into workflow-centric Dashboard and Property views.

## Survival mapping

| Drilldown | Keep? | Absorb into | What survives | What should be removed later |
|---|---|---|---|---|
| Cashflow | No | Dashboard + Property | Dashboard: portfolio net cash window, collected income, capex totals, top contributors. Property: property cash snapshot, rolling window, ledger/operating detail. | Standalone page, duplicate KPI-only navigation, isolated “bigger version” layout |
| Compliance | No | Property primarily, Dashboard only for portfolio counts | Property: readiness, pass/fail/review counts, blockers, top fail points, remediation actions. Dashboard: compliance portfolio counts only. | Standalone page and duplicate summary-only cards |
| Equity | No | Dashboard + Property | Dashboard: total estimated value, total loan balance, total estimated equity, homes with valuation. Property: latest valuation, equity snapshot, equity timeline/suggestions if useful. | Standalone leaderboard-style page |
| Pipeline | No | Dashboard | Dashboard: decision counts, stage totals, workflow distribution, stage progression visuals. | Separate pipeline page/route |
| Rehab | No | Property primarily | Property: rehab open cost, open task count, done task count, rehab status/progress. Optional dashboard summary only if it helps prioritization. | Standalone rehab drilldown page |
| Trust | No | Dashboard + Property | Dashboard: decision quality / risk distribution. Property: risk badges, location confidence, red-zone/crime/offender/location-quality explanations. | Standalone trust page |

## Dashboard should absorb
- portfolio rollups
- decision counts
- risk distribution
- cashflow totals
- equity totals
- stage totals
- compliance portfolio counts

## Property page should absorb
- property-specific compliance detail
- rehab tasks / rehab status
- next actions
- trust/risk signals
- agent activity
- cashflow breakdown
- equity breakdown
- workflow stage detail

## Remove entirely if low-value
- duplicate summary cards already shown elsewhere
- pages that only restate one KPI with more whitespace
- navigation whose only purpose is “see a bigger version of the same data”

## Implementation notes for later chunks
- Do not delete drilldown routes in this chunk.
- Do not delete drilldown page files in this chunk.
- First move surviving information into Dashboard/Property components.
- Remove routes/files only after Dashboard and Property absorb the needed metrics.