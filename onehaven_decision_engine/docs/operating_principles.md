# OneHaven Operating Principles (Operating Truth)

This document is the internal constitution of the system.
Every feature, score, workflow, import, automation, and UI MUST align with these principles.

If any future feature conflicts with these principles, the feature is wrong — not the principles.

---

## 0) Definitions (so we stop arguing with ourselves)

**Deal**: a proposed acquisition with a price + rehab estimate + financing assumptions and a chosen strategy.

**Strategy**:
- `section8`: rent is constrained by policy + comps + condition + time.
- `market`: rent is constrained by comps + condition + time.

**Rent Ceiling (Policy Ceiling)**:
For Section 8 strategy:
approved_rent_ceiling = min( FMR * payment_standard_pct, median_local_comp )
- If one side is missing, the ceiling falls back to the side that exists.
- If a manual override exists, override wins — but must be clearly marked as override.

**Rent Used (Decision Rent)**:
The number underwriting consumes.
- For `market`: use calibrated market estimate (or comps-derived if you add that later).
- For `section8`: use the minimum of (calibrated market estimate, approved_rent_ceiling) when both exist.
- If rent was estimated due to missing data, the system must downgrade PASS → REVIEW.

**Friction**:
Jurisdiction + compliance + time delay risk expressed as:
- a **multiplier** in scoring, and
- a **reason list** for explainability.

---

## 1) Michigan is landlord-friendly only if compliant

Michigan can be investor-friendly in practice, but only if the operator is compliant.
Non-compliance is not a “minor issue”; it is a **deal-killer** via:
- licensing delays
- failed inspections / reinspections
- registration friction
- delayed rent flow

System rule:
- Compliance friction must be represented explicitly (scored + explained).
- Missing jurisdiction rules → neutral friction, but must show “unknown rules” in reasons.

---

## 2) Rent is constrained by reality, not optimism

Rent is constrained by three forces:

### A) HUD FMR (Fair Market Rent)
FMR is a ceiling reference, not a promise.

### B) Rent Reasonableness (local comps)
If local rent reasonableness comps are lower than FMR, **comps win**.

### C) Unit condition (inspection readiness)
A unit that cannot pass inspection does not produce rent.
Condition can also reduce achievable rent or extend time-to-rent.

System rule:
- The decision must show the cap reason (“fmr” vs “comps” vs “override” vs “none”).
- Every rent decision must be explainable in an API response.

---

## 3) Inspection is deterministic

Inspection outcomes are not “random”.
They are predictable from:
- property age (e.g., pre-1978 paint rules)
- known HQS fail patterns (GFCI, handrails, leaks, windows, etc.)
- city/inspector strictness
- rehab scope quality

Therefore:
- the system generates checklists **before** inspection
- the system logs failures to build certainty over time (analytics loop)

---

## 4) Time kills deals

Delays are not neutral. They destroy IRR and can destroy the deal entirely.

Key delay drivers:
- licensing + registration processing time
- inspection scheduling time
- reinspections
- tenant waitlist depth / status

System rule:
- time risk must appear in the output via:
  - friction scoring (jurisdiction multiplier)
  - reasons/explainability

---

## 5) Compliance friction is a cost, not an annoyance

Compliance is not “paperwork”. It is a measurable cost:
- admin labor
- fees
- rework
- time-to-rent delay
- reinvestment into rehabs that should have been done upfront

System rule:
- friction must affect score and decision.
- decisions must not be opaque.

---

## 6) Output truthfulness (anti-BS clauses)

The system is not allowed to:
- PASS a deal when rent is unknown/estimated (must be REVIEW)
- hide missing comps / missing FMR / missing rules
- silently apply overrides without marking them

---

## 7) What we are (and are not)

We provide **operational intelligence** based on:
- public rules
- stored jurisdiction policies
- historical outcomes in the system
- user-entered comps and inspection results

We do not provide legal advice.
