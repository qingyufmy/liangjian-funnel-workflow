from copy import deepcopy
from liangjian_funnel.review.verification import comparable_field_status


def test_close_match_does_not_hide_high_or_volume_mismatch():
    checks = {k: {"status": "MATCH"} for k in ("OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "AMOUNT")}
    assert comparable_field_status(checks) == "MATCH"
    for key in ("HIGH", "VOLUME"):
        mismatch = deepcopy(checks)
        mismatch[key]["status"] = "MISMATCH"
        mismatch["AMOUNT"]["status"] = "DATA_LIMITED"
        assert comparable_field_status(mismatch) == "MISMATCH"


def test_unavailable_fields_never_claim_full_match():
    assert comparable_field_status({"CLOSE": {"status": "MATCH"}}) == "DATA_LIMITED"
    assert comparable_field_status({}) == "DATA_LIMITED"
