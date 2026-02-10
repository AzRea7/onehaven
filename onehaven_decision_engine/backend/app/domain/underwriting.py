# onehaven_decision_engine/backend/app/domain/underwriting.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from ..config import settings


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


def _monthly_mortgage_payment(principal: float, annual_rate: float, term_years: int) -> float:
    if principal <= 0:
        return 0.0
    r = annual_rate / 12.0
    n = term_years * 12
    if r <= 0:
        return principal / n
    return principal * (r * (1 + r) ** n) / ((1 + r) ** n - 1)


def run_underwriting(inp: UnderwritingInputs, target_roi: float) -> UnderwritingOutputs:
    all_in_cost = inp.purchase_price + inp.rehab
    down_payment = all_in_cost * inp.down_payment_pct
    loan_amount = max(all_in_cost - down_payment, 0.0)

    mortgage_payment = _monthly_mortgage_payment(loan_amount, inp.interest_rate, inp.term_years)

    effective_gross = inp.gross_rent * (1.0 - inp.vacancy_rate)

    var_opex = (
        inp.gross_rent * inp.maintenance_rate
        + inp.gross_rent * inp.management_rate
        + inp.gross_rent * inp.capex_rate
    )
    fixed_opex = inp.insurance_monthly + inp.taxes_monthly + inp.utilities_monthly
    operating_expenses = var_opex + fixed_opex

    noi = effective_gross - operating_expenses
    cash_flow = noi - mortgage_payment

    dscr = (noi / mortgage_payment) if mortgage_payment > 1e-9 else float("inf")

    cash_invested = down_payment
    annual_cash_flow = cash_flow * 12.0
    cash_on_cash = (annual_cash_flow / cash_invested) if cash_invested > 1e-9 else float("inf")

    a = (1.0 - inp.vacancy_rate) - (inp.maintenance_rate + inp.management_rate + inp.capex_rate)
    b = fixed_opex + mortgage_payment
    break_even_rent = (b / a) if a > 1e-9 else float("inf")

    required_annual_cash_flow = target_roi * cash_invested
    required_monthly_cash_flow = required_annual_cash_flow / 12.0
    min_rent_for_target_roi = ((fixed_opex + mortgage_payment + required_monthly_cash_flow) / a) if a > 1e-9 else float("inf")

    return UnderwritingOutputs(
        mortgage_payment=round(mortgage_payment, 2),
        operating_expenses=round(operating_expenses, 2),
        noi=round(noi, 2),
        cash_flow=round(cash_flow, 2),
        dscr=round(dscr, 3) if math.isfinite(dscr) else dscr,
        cash_on_cash=round(cash_on_cash, 3) if math.isfinite(cash_on_cash) else cash_on_cash,
        break_even_rent=round(break_even_rent, 2) if math.isfinite(break_even_rent) else break_even_rent,
        min_rent_for_target_roi=round(min_rent_for_target_roi, 2) if math.isfinite(min_rent_for_target_roi) else min_rent_for_target_roi,
    )


# ✅ Router-compatible wrapper
def underwrite(
    asking_price: float,
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
    routers/evaluate.py imports underwrite() directly.
    This function uses your configurable defaults (vacancy/maintenance/management/capex)
    and outputs DSCR + cash-on-cash + break-even rent.
    """

    vacancy_rate = float(getattr(settings, "vacancy_rate", 0.05))
    maintenance_rate = float(getattr(settings, "maintenance_rate", 0.10))
    management_rate = float(getattr(settings, "management_rate", 0.08))
    capex_rate = float(getattr(settings, "capex_rate", 0.05))
    target_roi = float(getattr(settings, "target_roi", 0.15))

    # If not explicitly provided, use configured defaults
    taxes_monthly = float(taxes_monthly) if taxes_monthly is not None else float(getattr(settings, "taxes_monthly_default", 0.0))
    insurance_monthly = float(insurance_monthly) if insurance_monthly is not None else float(getattr(settings, "insurance_monthly_default", 0.0))
    utilities_monthly = float(utilities_monthly) if utilities_monthly is not None else float(getattr(settings, "utilities_monthly_default", 0.0))

    inp = UnderwritingInputs(
        purchase_price=float(asking_price),
        rehab=float(rehab_estimate or 0.0),
        down_payment_pct=float(down_payment_pct),
        interest_rate=float(interest_rate),
        term_years=int(term_years),
        gross_rent=float(gross_rent),
        vacancy_rate=vacancy_rate,
        maintenance_rate=maintenance_rate,
        management_rate=management_rate,
        capex_rate=capex_rate,
        insurance_monthly=float(insurance_monthly),
        taxes_monthly=float(taxes_monthly),
        utilities_monthly=float(utilities_monthly),
    )
    return run_underwriting(inp, target_roi=target_roi)
