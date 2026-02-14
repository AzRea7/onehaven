# OneHaven Operating Principles (Phase 0 Constitution)

This document is the “constitution” for OneHaven.  
Every feature must align with these truths.

## 1) We optimize for operational truth, not pretty math
- We are not trying to produce the maximum possible ROI on paper.
- We are trying to produce deals that survive real-world friction.

## 2) Rent is constrained (always)
Rent is capped by the minimum of:
- **Payment standard ceiling** (FMR × payment_standard_pct)
- **Rent reasonableness** (local comps / median comp)
- **Unit condition** (inspection + quality affects approval and tenant stability)

**Rule:** Any decision must be explainable in terms of these constraints.

## 3) Inspection is deterministic
We treat inspections as a checklist problem:
- If the unit meets HQS requirements, it passes.
- Failures are not “random”; they are data.

**Rule:** We log fail points and feed them back into rehab templates and scoring.

## 4) Time kills deals
Delays are costs:
- Registration delays
- Housing authority processing delays
- Inspection scheduling delays

**Rule:** Jurisdiction friction must affect scoring and decision output.

## 5) Compliance friction is a cost center
Compliance is not a “note”; it is an input:
- license requirements
- recurring inspection frequency
- typical fail points
- processing time
- fees

## 6) No silent overrides
If a value is overridden (rent ceiling, assumptions, etc.), it must be:
- persisted
- versioned
- explainable
- auditable

## 7) Output must be operator-ready
A “PASS” means:
- a human can actually move it forward
- with a clear checklist of the next actions
- and the biggest risks stated plainly
