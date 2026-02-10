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


def run_underwriting(inp: UnderwritingInputs, target_roi: float) -> UnderwritingOutputs:
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

    # DSCR: clamp instead of infinity
    if mortgage_payment > 1e-6:
        dscr = noi / mortgage_payment
    else:
        dscr = 999.0  # finite sentinel

    cash_invested = max(down_payment, 0.0)
    annual_cash_flow = cash_flow * 12.0

    if cash_invested > 1e-6:
        cash_on_cash = annual_cash_flow / cash_invested
    else:
        cash_on_cash = 999.0  # finite sentinel

    a = (1.0 - float(inp.vacancy_rate)) - (float(inp.maintenance_rate) + float(inp.management_rate) + float(inp.capex_rate))
    b = fixed_opex + mortgage_payment

    if a > 1e-6:
        break_even_rent = b / a
    else:
        break_even_rent = 999999.0

    required_annual_cash_flow = float(target_roi) * cash_invested
    required_monthly_cash_flow = required_annual_cash_flow / 12.0

    if a > 1e-6:
        min_rent_for_target_roi = (fixed_opex + mortgage_payment + required_monthly_cash_flow) / a
    else:
        min_rent_for_target_roi = 999999.0

    # Ensure finite values everywhere
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
    vacancy_rate = float(getattr(settings, "vacancy_rate", 0.05))
    maintenance_rate = float(getattr(settings, "maintenance_rate", 0.10))
    management_rate = float(getattr(settings, "management_rate", 0.08))
    capex_rate = float(getattr(settings, "capex_rate", 0.05))
    target_roi = float(getattr(settings, "target_roi", 0.15))

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
