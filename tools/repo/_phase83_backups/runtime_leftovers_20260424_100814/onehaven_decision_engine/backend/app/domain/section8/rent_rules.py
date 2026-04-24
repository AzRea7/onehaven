# backend/app/domain/section8/rent_rules.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


def _to_pos_float(x: object) -> Optional[float]:
    try:
        v = float(x)  # type: ignore[arg-type]
        return v if v > 0 else None
    except Exception:
        return None


def _normalize_payment_standard_pct(x: object, default: float = 110.0) -> float:
    """
    Accept either:
      - 110 (meaning 110%)
      - 1.10 (meaning 110% as a ratio)
    Returns a percent number, e.g. 110.0.
    """
    v = _to_pos_float(x)
    if v is None:
        return float(default)

    # If someone stores 1.10 in config, treat it as a ratio.
    if 0 < v <= 3.0:
        return float(v * 100.0)

    return float(v)


@dataclass(frozen=True)
class CeilingCandidate:
    type: str
    value: float


@dataclass(frozen=True)
class RentDecision:
    strategy: str
    market_rent_estimate: Optional[float]
    approved_rent_ceiling: Optional[float]
    rent_used: Optional[float]
    cap_reason: str  # "none" | "capped" | "uncapped"
    explanation: str
    candidates: list[CeilingCandidate]


def compute_approved_ceiling(
    *,
    section8_fmr: Optional[float],
    payment_standard_pct: float,
    rent_reasonableness_comp: Optional[float],
    manual_override: Optional[float],
) -> tuple[Optional[float], list[CeilingCandidate]]:
    """
    Returns (approved_ceiling, candidates).

    Rule (S8):
      - candidate #1: FMR adjusted by payment standard pct (e.g., 110% of FMR)
      - candidate #2: rent reasonableness comp cap (if present)
      - approved ceiling = manual override if present else min(candidates)
    """
    candidates: list[CeilingCandidate] = []
    caps: list[float] = []

    fmr = _to_pos_float(section8_fmr)
    rr = _to_pos_float(rent_reasonableness_comp)

    pct = _normalize_payment_standard_pct(payment_standard_pct, default=110.0)

    # Candidate: adjusted FMR
    if fmr is not None:
        adjusted = float(fmr) * (pct / 100.0)
        if adjusted > 0:
            caps.append(adjusted)
            candidates.append(CeilingCandidate(type="fmr_adjusted", value=adjusted))

    # Candidate: rent reasonableness comp
    if rr is not None:
        caps.append(float(rr))
        candidates.append(CeilingCandidate(type="rent_reasonableness", value=float(rr)))

    computed = min(caps) if caps else None

    manual = _to_pos_float(manual_override)
    approved = manual if manual is not None else computed

    return approved, candidates


def compute_rent_used(
    *,
    strategy: str,
    market: Optional[float],
    approved: Optional[float],
    candidates: Optional[list[CeilingCandidate]] = None,
) -> RentDecision:
    """
    Strategy:
      - market: rent_used = market (no ceiling)
      - section8 (default): rent_used = min(market, approved) when both exist
    """
    s = (strategy or "section8").strip().lower()
    m = _to_pos_float(market)
    a = _to_pos_float(approved)
    cands = candidates or []

    if s == "market":
        if m is None:
            return RentDecision(
                strategy=s,
                market_rent_estimate=m,
                approved_rent_ceiling=a,
                rent_used=None,
                cap_reason="none",
                explanation="Market strategy: market_rent_estimate is missing, so rent_used cannot be computed.",
                candidates=cands,
            )
        return RentDecision(
            strategy=s,
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=float(m),
            cap_reason="none",
            explanation="Market strategy uses market_rent_estimate (no Section 8 ceiling cap applied).",
            candidates=cands,
        )

    # Default: section8
    if m is None and a is None:
        return RentDecision(
            strategy="section8",
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=None,
            cap_reason="none",
            explanation="Section 8 strategy: both market_rent_estimate and ceiling inputs are missing; cannot compute rent_used.",
            candidates=cands,
        )
    if m is None:
        return RentDecision(
            strategy="section8",
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=float(a) if a is not None else None,
            cap_reason="none",
            explanation="Section 8 strategy: market estimate missing; using approved ceiling as rent_used.",
            candidates=cands,
        )
    if a is None:
        return RentDecision(
            strategy="section8",
            market_rent_estimate=m,
            approved_rent_ceiling=a,
            rent_used=float(m),
            cap_reason="none",
            explanation="Section 8 strategy: ceiling missing; using market_rent_estimate as rent_used.",
            candidates=cands,
        )

    rent_used = float(min(float(m), float(a)))
    cap_reason = "capped" if float(m) > float(a) else "uncapped"
    return RentDecision(
        strategy="section8",
        market_rent_estimate=m,
        approved_rent_ceiling=a,
        rent_used=rent_used,
        cap_reason=cap_reason,
        explanation="Section 8 strategy caps rent by the strictest limit (approved ceiling vs market estimate).",
        candidates=cands,
    )


# --- Step 8 additive helpers: federal/HUD/Section 8 compliance layers ---
@dataclass(frozen=True)
class Section8Candidate:
    type: str
    value: float
    source: str | None = None
    note: str | None = None


@dataclass(frozen=True)
class Section8RentCompliance:
    strategy: str
    market_rent_estimate: Optional[float]
    section8_fmr: Optional[float]
    payment_standard_pct: float
    payment_standard_amount: Optional[float]
    utility_allowance: Optional[float]
    gross_rent: Optional[float]
    rent_to_owner_cap: Optional[float]
    approved_rent_ceiling: Optional[float]
    rent_reasonableness_comp: Optional[float]
    rent_to_owner: Optional[float]
    rent_used: Optional[float]
    gross_rent_compliant: bool | None
    rent_reasonable: bool | None
    is_compliant: bool | None
    cap_reason: str
    explanation: str
    missing_inputs: list[str]
    candidates: list[Section8Candidate]


@dataclass(frozen=True)
class FederalRuleUpdate:
    source: str
    document_id: str | None
    title: str
    published_on: str | None = None
    effective_on: str | None = None
    url: str | None = None
    summary: str | None = None
    category: str | None = None


def compute_payment_standard_amount(
    *,
    section8_fmr: Optional[float],
    payment_standard_pct: float,
) -> Optional[float]:
    fmr = _to_pos_float(section8_fmr)
    if fmr is None:
        return None
    pct = _normalize_payment_standard_pct(payment_standard_pct, default=110.0)
    amount = float(fmr) * (pct / 100.0)
    return round(amount, 2) if amount > 0 else None


def compute_gross_rent(
    *,
    rent_to_owner: Optional[float],
    utility_allowance: Optional[float],
) -> Optional[float]:
    rent = _to_pos_float(rent_to_owner)
    utility = _to_pos_float(utility_allowance)
    if rent is None and utility is None:
        return None
    return round(float(rent or 0.0) + float(utility or 0.0), 2)


def compute_rent_to_owner_cap(
    *,
    approved_rent_ceiling: Optional[float],
    utility_allowance: Optional[float],
) -> Optional[float]:
    ceiling = _to_pos_float(approved_rent_ceiling)
    utility = _to_pos_float(utility_allowance)
    if ceiling is None:
        return None
    owner_cap = float(ceiling) - float(utility or 0.0)
    return round(owner_cap, 2) if owner_cap > 0 else 0.0


def build_section8_candidates(
    *,
    section8_fmr: Optional[float],
    payment_standard_pct: float,
    payment_standard_amount: Optional[float],
    approved_rent_ceiling: Optional[float],
    rent_reasonableness_comp: Optional[float],
    utility_allowance: Optional[float],
) -> list[Section8Candidate]:
    out: list[Section8Candidate] = []
    fmr = _to_pos_float(section8_fmr)
    if fmr is not None:
        out.append(
            Section8Candidate(
                type="fmr",
                value=float(fmr),
                source="HUD FMR",
                note="Published HUD Fair Market Rent baseline",
            )
        )
    psa = _to_pos_float(payment_standard_amount)
    if psa is not None:
        out.append(
            Section8Candidate(
                type="payment_standard",
                value=float(psa),
                source="HUD/FMR",
                note=f"Payment standard at {_normalize_payment_standard_pct(payment_standard_pct):.1f}% of FMR",
            )
        )
    rr = _to_pos_float(rent_reasonableness_comp)
    if rr is not None:
        out.append(
            Section8Candidate(
                type="rent_reasonableness",
                value=float(rr),
                source="rent_comps",
                note="Operational rent-reasonableness comparable ceiling",
            )
        )
    approved = _to_pos_float(approved_rent_ceiling)
    if approved is not None:
        out.append(
            Section8Candidate(
                type="approved_ceiling",
                value=float(approved),
                source="engine",
                note="Strictest gross-rent ceiling currently applied",
            )
        )
    utility = _to_pos_float(utility_allowance)
    if utility is not None:
        out.append(
            Section8Candidate(
                type="utility_allowance",
                value=float(utility),
                source="utility",
                note="Utility allowance included in gross-rent evaluation",
            )
        )
    return out


def build_section8_rent_compliance(
    *,
    market_rent_estimate: Optional[float],
    section8_fmr: Optional[float],
    payment_standard_pct: float,
    rent_reasonableness_comp: Optional[float],
    approved_rent_ceiling: Optional[float],
    rent_used: Optional[float],
    utility_allowance: Optional[float] = None,
    requested_rent_to_owner: Optional[float] = None,
    strategy: str = "section8",
) -> Section8RentCompliance:
    payment_standard_amount = compute_payment_standard_amount(
        section8_fmr=section8_fmr,
        payment_standard_pct=payment_standard_pct,
    )
    gross_rent = compute_gross_rent(
        rent_to_owner=requested_rent_to_owner if requested_rent_to_owner is not None else rent_used,
        utility_allowance=utility_allowance,
    )
    rent_to_owner_cap = compute_rent_to_owner_cap(
        approved_rent_ceiling=approved_rent_ceiling,
        utility_allowance=utility_allowance,
    )

    requested_owner = _to_pos_float(requested_rent_to_owner if requested_rent_to_owner is not None else rent_used)
    approved = _to_pos_float(approved_rent_ceiling)
    rr = _to_pos_float(rent_reasonableness_comp)
    gross_rent_compliant = None if gross_rent is None or approved is None else bool(float(gross_rent) <= float(approved))
    rent_reasonable = None if requested_owner is None or rr is None else bool(float(requested_owner) <= float(rr))

    missing_inputs: list[str] = []
    if _to_pos_float(section8_fmr) is None:
        missing_inputs.append("section8_fmr")
    if approved is None:
        missing_inputs.append("approved_rent_ceiling")
    if requested_owner is None:
        missing_inputs.append("requested_rent_to_owner")

    is_compliant: bool | None
    if gross_rent_compliant is None and rent_reasonable is None:
        is_compliant = None
    else:
        checks = [v for v in [gross_rent_compliant, rent_reasonable] if v is not None]
        is_compliant = all(checks) if checks else None

    explanation_parts: list[str] = []
    if payment_standard_amount is not None:
        explanation_parts.append(f"Payment standard amount={payment_standard_amount:.2f}")
    if approved is not None:
        explanation_parts.append(f"approved ceiling={approved:.2f}")
    if gross_rent is not None:
        explanation_parts.append(f"gross rent={gross_rent:.2f}")
    if requested_owner is not None:
        explanation_parts.append(f"rent to owner={requested_owner:.2f}")
    if rr is not None:
        explanation_parts.append(f"rent reasonableness comp={rr:.2f}")
    if missing_inputs:
        explanation_parts.append("missing=" + ",".join(missing_inputs))

    return Section8RentCompliance(
        strategy=(strategy or "section8").strip().lower() or "section8",
        market_rent_estimate=_to_pos_float(market_rent_estimate),
        section8_fmr=_to_pos_float(section8_fmr),
        payment_standard_pct=_normalize_payment_standard_pct(payment_standard_pct),
        payment_standard_amount=payment_standard_amount,
        utility_allowance=_to_pos_float(utility_allowance),
        gross_rent=gross_rent,
        rent_to_owner_cap=rent_to_owner_cap,
        approved_rent_ceiling=approved,
        rent_reasonableness_comp=rr,
        rent_to_owner=requested_owner,
        rent_used=_to_pos_float(rent_used),
        gross_rent_compliant=gross_rent_compliant,
        rent_reasonable=rent_reasonable,
        is_compliant=is_compliant,
        cap_reason="compliant" if is_compliant is True else ("non_compliant" if is_compliant is False else "insufficient_inputs"),
        explanation="; ".join(explanation_parts) if explanation_parts else "Section 8 compliance summary has insufficient inputs.",
        missing_inputs=missing_inputs,
        candidates=build_section8_candidates(
            section8_fmr=section8_fmr,
            payment_standard_pct=payment_standard_pct,
            payment_standard_amount=payment_standard_amount,
            approved_rent_ceiling=approved_rent_ceiling,
            rent_reasonableness_comp=rent_reasonableness_comp,
            utility_allowance=utility_allowance,
        ),
    )


def normalize_federal_rule_updates(rows: list[dict[str, object]] | None, *, source: str) -> list[FederalRuleUpdate]:
    out: list[FederalRuleUpdate] = []
    for row in rows or []:
        title = str(row.get("title") or row.get("document_title") or "").strip()
        if not title:
            continue
        out.append(
            FederalRuleUpdate(
                source=source,
                document_id=str(row.get("document_number") or row.get("packageId") or row.get("package_id") or "").strip() or None,
                title=title,
                published_on=str(row.get("publication_date") or row.get("dateIssued") or "").strip() or None,
                effective_on=str(row.get("effective_on") or row.get("effective_date") or "").strip() or None,
                url=str(row.get("html_url") or row.get("pdf_url") or row.get("detailsLink") or row.get("url") or "").strip() or None,
                summary=str(row.get("abstract") or row.get("summary") or row.get("reason") or "").strip() or None,
                category=str(row.get("category") or row.get("type") or row.get("collectionCode") or "").strip() or None,
            )
        )
    return out


from pathlib import Path
import os


def _candidate_pdf_roots() -> list[Path]:
    roots: list[Path] = []
    for raw in [os.getenv("NSPIRE_PDF_ROOT", ""), os.getenv("POLICY_PDFS_ROOT", ""), os.getenv("POLICY_PDF_ROOTS", ""), "backend/data/pdfs", "/app/backend/data/pdfs", r"/mnt/data/step8_pdf_zip/pdfs"]:
        if not raw:
            continue
        for part in str(raw).split(os.pathsep):
            part = part.strip()
            if not part:
                continue
            p = Path(part)
            if p.exists() and p.is_dir() and p not in roots:
                roots.append(p)
    return roots


def summarize_nspire_pdf_dataset() -> dict[str, object]:
    roots = _candidate_pdf_roots()
    files: list[str] = []
    for root in roots:
        for p in root.rglob("*.pdf"):
            files.append(p.name)
    unique = sorted(set(files))
    return {
        "available": bool(unique),
        "pdf_count": len(unique),
        "roots": [str(r) for r in roots],
        "sample_files": unique[:12],
        "contains_nspire_standards": any("NSPIRE-Standard-" in name for name in unique),
    }
