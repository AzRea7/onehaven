# backend/app/domain/underwriting.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Optional

from ..config import settings


RentCapReason = Literal[
    "rentcast_under_fmr",
    "fmr_cap_applied",
    "fmr_fallback",
    "multifamily_fmr_times_units",
    "multifamily_bedroom_mix",
    "missing_rent_inputs",
]


@dataclass(frozen=True)
class UnderwritingInputs:
    purchase_price: float
    rehab: float
    down_payment_pct: float
    interest_rate: float
    term_years: int

    gross_rent: float

    vacancy_rate: float
    maintenance_rate: float
    management_rate: float
    capex_rate: float

    insurance_monthly: float
    taxes_monthly: float
    utilities_monthly: float


@dataclass(frozen=True)
class UnderwritingOutputs:
    mortgage_payment: float
    operating_expenses: float
    noi: float
    cash_flow: float
    dscr: float
    cash_on_cash: float
    break_even_rent: float
    min_rent_for_target_roi: float


def compute_monthly_housing_costs(
    *,
    asking_price: float | None,
    interest_rate: float,
    term_years: int,
    down_payment_pct: float,
    tax_rate_annual: float | None,
    insurance_annual: float | None,
) -> dict[str, float | None]:
    if asking_price is None or asking_price <= 0:
        return {
            "loan_amount": None,
            "monthly_debt_service": None,
            "monthly_taxes": None,
            "monthly_insurance": None,
            "monthly_housing_cost": None,
        }

    price = float(asking_price)
    down_payment = price * float(down_payment_pct)
    loan_amount = max(price - down_payment, 0.0)

    monthly_rate = float(interest_rate) / 12.0
    num_payments = int(term_years) * 12

    if loan_amount <= 0:
        monthly_pi = 0.0
    elif monthly_rate <= 0:
        monthly_pi = loan_amount / num_payments
    else:
        factor = pow(1.0 + monthly_rate, num_payments)
        monthly_pi = loan_amount * (monthly_rate * factor) / (factor - 1.0)

    monthly_taxes = None
    if tax_rate_annual is not None:
        monthly_taxes = (price * float(tax_rate_annual)) / 12.0

    monthly_insurance = None
    if insurance_annual is not None:
        monthly_insurance = float(insurance_annual) / 12.0

    total = monthly_pi
    if monthly_taxes is not None:
        total += monthly_taxes
    if monthly_insurance is not None:
        total += monthly_insurance

    return {
        "loan_amount": round(loan_amount, 2),
        "monthly_debt_service": round(monthly_pi, 2),
        "monthly_taxes": round(monthly_taxes, 2) if monthly_taxes is not None else None,
        "monthly_insurance": round(monthly_insurance, 2) if monthly_insurance is not None else None,
        "monthly_housing_cost": round(total, 2),
    }


def _monthly_mortgage_payment(principal: float, annual_rate: float, term_years: int) -> float:
    if principal <= 0 or term_years <= 0:
        return 0.0
    r = annual_rate / 12.0
    n = term_years * 12
    if r <= 0:
        return principal / n
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def _finite(x: float, *, fallback: float) -> float:
    if x is None:
        return fallback
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return fallback
    return float(x)


def _to_pos_float(value: float | int | str | None) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
        return out if out > 0 else None
    except Exception:
        return None


def _round_money(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def select_market_rent_reference(
    *,
    market_rent_estimate: float | None,
    rent_reasonableness_comp: float | None,
) -> float | None:
    """
    Conservative market-rent selector used by Section 8 underwriting.

    When both the RentCast estimated rent and the nearby-comps median exist,
    prefer the lower of the two so the underwritten rent stays grounded in the
    surrounding comp set and does not drift above what the local market supports.
    """
    estimate = _to_pos_float(market_rent_estimate)
    comp = _to_pos_float(rent_reasonableness_comp)

    if estimate is not None and comp is not None:
        return _round_money(min(float(estimate), float(comp)))
    if comp is not None:
        return _round_money(comp)
    if estimate is not None:
        return _round_money(estimate)
    return None


def _is_multifamily(property_type: str | None, units: int | None) -> bool:
    ptype = (property_type or "").strip().lower()
    return ("multi" in ptype) and max(int(units or 0), 0) > 1


def compute_effective_rent_used(
    *,
    property_type: str | None,
    bedrooms: int | None,
    units: int | None,
    rentcast_rent: float | None,
    fmr_rent: float | None,
    unit_rentcast_rent: float | None = None,
    unit_fmr_rent: float | None = None,
) -> tuple[float | None, RentCapReason]:
    ptype = (property_type or "").strip().lower()
    beds = max(int(bedrooms or 0), 0)
    unit_count = max(int(units or 0), 0)

    total_rentcast = _to_pos_float(rentcast_rent)
    total_fmr = _to_pos_float(fmr_rent)
    per_unit_rentcast = _to_pos_float(unit_rentcast_rent)
    per_unit_fmr = _to_pos_float(unit_fmr_rent)

    if "multi" in ptype and unit_count > 1:
        if per_unit_rentcast is None and total_rentcast is not None:
            per_unit_rentcast = total_rentcast / float(unit_count)
        if per_unit_fmr is None and total_fmr is not None:
            per_unit_fmr = total_fmr / float(unit_count)

        if per_unit_rentcast is not None and per_unit_fmr is not None:
            per_unit = min(float(per_unit_rentcast), float(per_unit_fmr))
            return round(per_unit * unit_count, 2), "multifamily_fmr_times_units"
        if per_unit_fmr is not None:
            return round(float(per_unit_fmr) * unit_count, 2), "multifamily_fmr_times_units"
        if per_unit_rentcast is not None:
            return round(float(per_unit_rentcast) * unit_count, 2), "multifamily_fmr_times_units"
        return None, "missing_rent_inputs"

    if total_rentcast is not None and total_fmr is not None:
        if float(total_rentcast) <= float(total_fmr):
            return round(float(total_rentcast), 2), "rentcast_under_fmr"
        return round(float(total_fmr), 2), "fmr_cap_applied"

    if total_fmr is not None:
        return round(float(total_fmr), 2), "fmr_fallback"

    if total_rentcast is not None:
        return round(float(total_rentcast), 2), "rentcast_under_fmr"

    return None, "missing_rent_inputs"


def describe_rent_cap_reason(reason: str, *, strategy: str = "section8") -> str:
    normalized = str(reason or "missing_rent_inputs").strip().lower()
    mode = str(strategy or "section8").strip().lower()

    if mode == "market":
        return "Market strategy uses the calibrated market rent estimate without a Section 8 cap."

    mapping = {
        "rentcast_under_fmr": "RentCast market rent is below the Section 8 ceiling, so the lower market-supported rent is used.",
        "fmr_cap_applied": "RentCast market rent is above the Section 8 ceiling, so the FMR-based ceiling is applied.",
        "fmr_fallback": "Market rent is missing, so the FMR-based ceiling is used as the fallback rent assumption.",
        "multifamily_fmr_times_units": "Multifamily rent is computed from a per-unit capped rent and multiplied by the property unit count.",
        "multifamily_bedroom_mix": "Multifamily rent is computed from the stored bedroom mix and capped per unit before summing.",
        "missing_rent_inputs": "Neither usable market rent nor usable FMR inputs were available, so rent_used could not be computed.",
    }
    return mapping.get(normalized, "Rent assumption was computed from the shared underwriting rent rules.")


def run_underwriting(inp: UnderwritingInputs, target_roi: float) -> UnderwritingOutputs:
    # down payment based on ALL-IN cost (purchase + rehab)
    all_in_cost = float(inp.purchase_price) + float(inp.rehab)
    down_payment = all_in_cost * float(inp.down_payment_pct)
    loan_amount = max(all_in_cost - down_payment, 0.0)

    mortgage_payment = _monthly_mortgage_payment(loan_amount, float(inp.interest_rate), int(inp.term_years))

    effective_gross = float(inp.gross_rent) * (1.0 - float(inp.vacancy_rate))

    var_opex = (
        float(inp.gross_rent) * float(inp.maintenance_rate)
        + float(inp.gross_rent) * float(inp.management_rate)
        + float(inp.gross_rent) * float(inp.capex_rate)
    )
    fixed_opex = float(inp.insurance_monthly) + float(inp.taxes_monthly) + float(inp.utilities_monthly)
    operating_expenses = var_opex + fixed_opex

    noi = effective_gross - operating_expenses
    cash_flow = noi - mortgage_payment

    dscr = noi / mortgage_payment if mortgage_payment > 1e-6 else 999.0
    cash_invested = max(down_payment, 0.0)
    annual_cash_flow = cash_flow * 12.0
    cash_on_cash = annual_cash_flow / cash_invested if cash_invested > 1e-6 else 999.0

    # break-even rent:
    # cash_flow = rent * a - b
    a = (1.0 - float(inp.vacancy_rate)) - (
        float(inp.maintenance_rate) + float(inp.management_rate) + float(inp.capex_rate)
    )
    b = fixed_opex + mortgage_payment

    break_even_rent = (b / a) if a > 1e-6 else 999999.0

    required_annual_cash_flow = float(target_roi) * cash_invested
    required_monthly_cash_flow = required_annual_cash_flow / 12.0
    min_rent_for_target_roi = (
        (fixed_opex + mortgage_payment + required_monthly_cash_flow) / a if a > 1e-6 else 999999.0
    )

    dscr = _finite(dscr, fallback=0.0)
    cash_on_cash = _finite(cash_on_cash, fallback=0.0)
    break_even_rent = _finite(break_even_rent, fallback=0.0)
    min_rent_for_target_roi = _finite(min_rent_for_target_roi, fallback=0.0)

    return UnderwritingOutputs(
        mortgage_payment=round(mortgage_payment, 2),
        operating_expenses=round(operating_expenses, 2),
        noi=round(noi, 2),
        cash_flow=round(cash_flow, 2),
        dscr=round(dscr, 3),
        cash_on_cash=round(cash_on_cash, 3),
        break_even_rent=round(break_even_rent, 2),
        min_rent_for_target_roi=round(min_rent_for_target_roi, 2),
    )


def underwrite(
    *,
    # New callers should pass purchase_price
    purchase_price: Optional[float] = None,
    # Back-compat old name
    asking_price: Optional[float] = None,
    down_payment_pct: float,
    interest_rate: float,
    term_years: int,
    gross_rent: float,
    rehab_estimate: float = 0.0,
    taxes_monthly: Optional[float] = None,
    insurance_monthly: Optional[float] = None,
    utilities_monthly: Optional[float] = None,
) -> UnderwritingOutputs:
    """
    Backward compatible wrapper.

    - New callers: purchase_price=...
    - Old callers: asking_price=...
    """
    vacancy_rate = float(getattr(settings, "vacancy_rate", 0.05))
    maintenance_rate = float(getattr(settings, "maintenance_rate", 0.10))
    management_rate = float(getattr(settings, "management_rate", 0.08))
    capex_rate = float(getattr(settings, "capex_rate", 0.05))
    target_roi = float(getattr(settings, "target_roi", 0.15))

    taxes_monthly = float(taxes_monthly) if taxes_monthly is not None else float(
        getattr(settings, "taxes_monthly_default", 0.0)
    )
    insurance_monthly = float(insurance_monthly) if insurance_monthly is not None else float(
        getattr(settings, "insurance_monthly_default", 0.0)
    )
    utilities_monthly = float(utilities_monthly) if utilities_monthly is not None else float(
        getattr(settings, "utilities_monthly_default", 0.0)
    )

    if purchase_price is not None:
        pp = float(purchase_price)
    elif asking_price is not None:
        pp = float(asking_price)
    else:
        raise ValueError("underwrite(): must provide purchase_price or asking_price")

    inp = UnderwritingInputs(
        purchase_price=float(pp),
        rehab=float(rehab_estimate or 0.0),
        down_payment_pct=float(down_payment_pct),
        interest_rate=float(interest_rate),
        term_years=int(term_years),
        gross_rent=float(gross_rent),
        vacancy_rate=float(vacancy_rate),
        maintenance_rate=float(maintenance_rate),
        management_rate=float(management_rate),
        capex_rate=float(capex_rate),
        insurance_monthly=float(insurance_monthly),
        taxes_monthly=float(taxes_monthly),
        utilities_monthly=float(utilities_monthly),
    )

    return run_underwriting(inp, target_roi=float(target_roi))
