# Michigan Jurisdictions (Operational Model)

This document defines OneHaven’s **jurisdiction reality layer** for Michigan:
a practical model used to predict *timeline friction* and *process complexity*.

This is not legal advice. It’s an operational heuristic you refine based on outcomes.

---

## 1) What a “Jurisdiction Profile” is

A Jurisdiction Profile is a structured record keyed by:
- **state** (required, default MI)
- **county** (optional)
- **city** (optional)

Profiles can exist in two scopes:
- **global** (org_id = null): defaults seeded by OneHaven
- **org** (org_id = your org): overrides you create as you learn

---

## 2) Resolution order (what “wins”)

Most specific wins:

1. org city+state
2. org county+state
3. org state default
4. global city+state
5. global county+state
6. global state default

Policy data is merged from low → high so your org overrides can surgically replace keys.

---

## 3) Friction multiplier

A single scalar: **friction_multiplier** (default 1.00)

Interpretation:
- 1.00 = baseline expected steps and delays
- 1.25 = higher re-inspection likelihood / slower scheduling / more documentation friction
- 1.45+ = “treat as hard mode” until proven otherwise

You will later feed this into:
- stage duration estimates
- “next actions” prioritization
- underwriting buffers and risk scoring

---

## 4) Seeded Michigan defaults (starter set)

Seed includes:
- MI state default
- County defaults: Wayne, Oakland, Macomb, Washtenaw, Genesee, Kent, Ingham
- Detroit city override (Wayne)

These are starter heuristics and should be edited based on your actual outcomes.

---

## 5) How to use this in product behavior (future steps)

Later:
- Step 2: Pipeline enforcement uses this to block/sequence workflow and compute “next actions”
- Step 3/8: Geo + crime/offender are separate risk signals (don’t overload friction with safety)
- Step 11: HQS policies can reference jurisdiction profile for addenda/inspect quirks