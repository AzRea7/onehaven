from app.services.dashboard_rollups import _decision_bucket


def test_decision_bucket_collapses_to_three_states():
    assert _decision_bucket("PASS") == "GOOD"
    assert _decision_bucket("REVIEW") == "REVIEW"
    assert _decision_bucket("FAIL") == "REJECT"
    assert _decision_bucket(None) == "REVIEW"
