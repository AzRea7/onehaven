from app.services.property_state_machine import normalize_decision_bucket


def test_decision_bucket_collapses_to_three_states():
    assert normalize_decision_bucket("PASS") == "GOOD"
    assert normalize_decision_bucket("GOOD_DEAL") == "GOOD"
    assert normalize_decision_bucket("APPROVED") == "GOOD"

    assert normalize_decision_bucket("REVIEW") == "REVIEW"
    assert normalize_decision_bucket("UNKNOWN") == "REVIEW"
    assert normalize_decision_bucket(None) == "REVIEW"

    assert normalize_decision_bucket("FAIL") == "REJECT"
    assert normalize_decision_bucket("REJECT") == "REJECT"
    assert normalize_decision_bucket("NO_GO") == "REJECT"