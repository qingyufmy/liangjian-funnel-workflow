from datetime import datetime
from zoneinfo import ZoneInfo
import pytest
from liangjian_funnel.runtime.plan_expiry import normalize_publication_expiry


@pytest.mark.parametrize("target", ["2026-10-15", "2026-10-12", "2026-10-14", "2026-10-19"])
def test_publication_expiry_preserves_research_source_across_session_boundaries(target):
    original = {"plan_expiry": "2026-10-13T15:00:00+08:00", "symbol": "002693.SZ"}
    expiry = datetime.fromisoformat(target + "T15:00:00+08:00")
    result = normalize_publication_expiry(original, expiry)
    assert result["plan_expiry"] == expiry.isoformat()
    assert result["research_source_expiry"] == original["plan_expiry"]
    assert original["plan_expiry"] == "2026-10-13T15:00:00+08:00"
    assert normalize_publication_expiry(result, expiry) == result


def test_naive_expiry_rejected():
    with pytest.raises(ValueError, match="MUST_BE_AWARE"):
        normalize_publication_expiry({}, datetime(2026, 10, 15, 15))
