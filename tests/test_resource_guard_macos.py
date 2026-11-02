import pytest
from liangjian_funnel.runtime import resource_guard as guard


@pytest.mark.parametrize('platform,raw', [('darwin', 128 * 1024 * 1024),
                                          ('linux', 128 * 1024)])
def test_peak_rss_uses_platform_units_even_for_small_process(monkeypatch, platform, raw):
    monkeypatch.setattr(guard.sys, 'platform', platform)
    assert guard._rss_mb(raw) == 128
