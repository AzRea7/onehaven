import pytest

from app.domain.workflow.stages import (
    STAGES,
    clamp_stage,
    distinct_stages,
    gate_for_next_stage,
    next_stage,
    prev_stage,
    stage_gte,
    stage_lte,
    stage_rank,
)


def _all_true_overrides(**overrides):
    base = {
        "has_property": True,
        "has_deal": True,
        "has_underwriting": True,
        "decision_is_buy": True,
        "has_acquisition_fields": True,
        "has_rehab_plan_tasks": True,
        "rehab_blockers_open": False,
        "rehab_open_tasks": False,
        "compliance_passed": True,
        "tenant_selected": True,
        "lease_active": True,
        "has_cash_txns": True,
        "has_valuation": True,
    }
    base.update(overrides)
    return base


def test_canonical_stage_order_is_exact():
    assert STAGES == [
        "import",
        "deal",
        "decision",
        "acquisition",
        "rehab_plan",
        "rehab_exec",
        "compliance",
        "tenant",
        "lease",
        "cash",
        "equity",
    ]
    assert distinct_stages() == STAGES


def test_clamp_stage_defaults_unknown_to_import():
    assert clamp_stage(None) == "import"
    assert clamp_stage("") == "import"
    assert clamp_stage("banana-cannon") == "import"
    assert clamp_stage("DEAL") == "deal"
    assert clamp_stage(" rehab_exec ") == "rehab_exec"


def test_stage_rank_and_comparisons_work():
    assert stage_rank("import") < stage_rank("deal")
    assert stage_rank("deal") < stage_rank("equity")
    assert stage_gte("cash", "lease") is True
    assert stage_gte("deal", "compliance") is False
    assert stage_lte("deal", "compliance") is True
    assert stage_lte("equity", "cash") is False


def test_next_and_prev_stage_navigation():
    assert next_stage("import") == "deal"
    assert next_stage("rehab_plan") == "rehab_exec"
    assert next_stage("equity") is None

    assert prev_stage("deal") == "import"
    assert prev_stage("equity") == "cash"
    assert prev_stage("import") is None


def test_gate_import_to_deal_requires_property():
    gate = gate_for_next_stage(
        current_stage="import",
        **_all_true_overrides(has_property=False),
    )
    assert gate.ok is False
    assert "Property must exist first" in str(gate.blocked_reason)


def test_gate_deal_to_decision_requires_deal_and_underwriting():
    gate = gate_for_next_stage(
        current_stage="deal",
        **_all_true_overrides(has_underwriting=False),
    )
    assert gate.ok is False
    assert "Run underwriting evaluation first" in str(gate.blocked_reason)

    gate2 = gate_for_next_stage(
        current_stage="deal",
        **_all_true_overrides(has_deal=False),
    )
    assert gate2.ok is False
    assert "Create a deal first" in str(gate2.blocked_reason)


def test_gate_decision_to_acquisition_requires_buy_decision():
    gate = gate_for_next_stage(
        current_stage="decision",
        **_all_true_overrides(decision_is_buy=False),
    )
    assert gate.ok is False
    assert "BUY-approved" in str(gate.blocked_reason)


def test_gate_acquisition_to_rehab_plan_requires_acquisition_fields():
    gate = gate_for_next_stage(
        current_stage="acquisition",
        **_all_true_overrides(has_acquisition_fields=False),
    )
    assert gate.ok is False
    assert "purchase price" in str(gate.blocked_reason).lower()


def test_gate_rehab_plan_to_rehab_exec_requires_rehab_tasks():
    gate = gate_for_next_stage(
        current_stage="rehab_plan",
        **_all_true_overrides(has_rehab_plan_tasks=False),
    )
    assert gate.ok is False
    assert "Create rehab plan tasks first" in str(gate.blocked_reason)


def test_gate_rehab_exec_to_compliance_blocks_on_open_rehab_items():
    gate_blocked = gate_for_next_stage(
        current_stage="rehab_exec",
        **_all_true_overrides(rehab_blockers_open=True),
    )
    assert gate_blocked.ok is False
    assert "blockers" in str(gate_blocked.blocked_reason).lower()

    gate_open = gate_for_next_stage(
        current_stage="rehab_exec",
        **_all_true_overrides(rehab_open_tasks=True),
    )
    assert gate_open.ok is False
    assert "Complete rehab execution tasks first" in str(gate_open.blocked_reason)


def test_gate_compliance_to_tenant_requires_compliance_passed():
    gate = gate_for_next_stage(
        current_stage="compliance",
        **_all_true_overrides(compliance_passed=False),
    )
    assert gate.ok is False
    assert "Compliance is not passed yet" in str(gate.blocked_reason)


def test_gate_tenant_to_lease_requires_tenant_selected():
    gate = gate_for_next_stage(
        current_stage="tenant",
        **_all_true_overrides(tenant_selected=False),
    )
    assert gate.ok is False
    assert "tenant" in str(gate.blocked_reason).lower()


def test_gate_lease_to_cash_requires_active_lease():
    gate = gate_for_next_stage(
        current_stage="lease",
        **_all_true_overrides(lease_active=False),
    )
    assert gate.ok is False
    assert "Activate a lease first" in str(gate.blocked_reason)


def test_gate_cash_to_equity_requires_cash_and_valuation():
    no_cash = gate_for_next_stage(
        current_stage="cash",
        **_all_true_overrides(has_cash_txns=False),
    )
    assert no_cash.ok is False
    assert "Add cash transactions first" in str(no_cash.blocked_reason)

    no_val = gate_for_next_stage(
        current_stage="cash",
        **_all_true_overrides(has_valuation=False),
    )
    assert no_val.ok is False
    assert "valuation snapshot" in str(no_val.blocked_reason).lower()


@pytest.mark.parametrize(
    "current_stage,expected_next",
    [
        ("import", "deal"),
        ("deal", "decision"),
        ("decision", "acquisition"),
        ("acquisition", "rehab_plan"),
        ("rehab_plan", "rehab_exec"),
        ("rehab_exec", "compliance"),
        ("compliance", "tenant"),
        ("tenant", "lease"),
        ("lease", "cash"),
        ("cash", "equity"),
    ],
)
def test_gate_allows_happy_path_for_each_step(current_stage, expected_next):
    gate = gate_for_next_stage(
        current_stage=current_stage,
        **_all_true_overrides(),
    )
    assert gate.ok is True
    assert gate.allowed_next_stage == expected_next


def test_gate_final_stage_cannot_advance():
    gate = gate_for_next_stage(
        current_stage="equity",
        **_all_true_overrides(),
    )
    assert gate.ok is False
    assert "Already at final stage" in str(gate.blocked_reason)
    assert gate.allowed_next_stage is None