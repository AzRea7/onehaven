# OneHaven Operating Principles (Internal Constitution)

These are not “notes”. They are system laws.

## 1) Michigan is landlord-friendly only if compliant
Compliance is not optional. Non-compliance destroys rent flow and increases downside risk.

System rule:
- if compliance requirements are unknown → decision must be REVIEW, not PASS.

## 2) Rent is constrained (ceiling logic)
Rent is capped by:
- HUD FMR (by ZIP + bedroom) * payment standard %
- Rent reasonableness (local comps)
- Unit condition / inspection readiness

System rule:
- rent_ceiling = min(payment_standard_rent, median_local_comp) unless an explicit override exists
- system must store cap_reason = fmr|comps|override|none
- system must be explainable in the API output

## 3) Inspection is deterministic
Inspection outcomes are predictable from:
- year built (pre-1978 paint rules)
- HQS fail patterns (GFCI, handrails, leaks, windows)
- jurisdiction/inspector strictness
- rehab quality

System rule:
- generate a checklist BEFORE inspection
- log failures and resolutions to create an analytics flywheel

## 4) Time kills deals
Delays destroy IRR and can destroy deals entirely:
- licensing + registration processing time
- inspection scheduling
- reinspections
- tenant waitlist depth

System rule:
- time risk must appear in the output via friction scoring + reasons

## 5) Compliance friction is a cost (not an annoyance)
Costs include:
- admin labor
- fees
- rework
- time-to-rent delay

System rule:
- friction must affect score and decision
- decision cannot be opaque

## 6) Output truthfulness (anti-BS)
The system must not:
- PASS when rent is unknown/estimated → must be REVIEW
- hide missing comps / missing FMR / missing rules
- silently apply overrides

## 7) “One screen truth” is the product promise
Property view must answer:
- is this deal alive?
- what blocks rent?
- when does money flow?
- how risky is this city?
- what should I do next?

Tabs:
Deal • Rehab • Compliance • Tenant • Cash • Equity

## 8) Not legal advice
We provide operational intelligence based on:
- public rules and datasets
- jurisdiction policies stored in the system
- historical outcomes recorded in the system
- user-entered comps and inspection results
