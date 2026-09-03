from datetime import date, datetime
import json
from zoneinfo import ZoneInfo

from liangjian_funnel.data.eastmoney_hot import (
    collect_eastmoney_hot100,
    normalize_eastmoney_hot100,
)


TZ = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 9, 3)
AS_OF = datetime(2026, 9, 3, 15, 10, tzinfo=TZ)


def _payload(*, day: date = DAY, count: int = 100):
    key = f"GUBA_TOP_REAL_TIME{{{day.isoformat()}}}"
    rows = []
    for rank in range(1, count + 1):
        code = f"{600000 + rank:06d}"
        rows.append({
            "SECURITY_CODE": code,
            "SECURITY_SHORT_NAME": f"测试{rank}",
            "MARKET_SHORT_NAME": "沪A",
            key: rank,
            "NEWEST_PRICE": "10.2",
            "CHG": "3.5",
            "TURNOVER_RATE": "8.6",
            "QRR": "1.7",
        })
    return {"code": 100, "data": {"result": {"dataList": rows}}}


def test_normalize_requires_exact_same_day_complete_top100():
    result = normalize_eastmoney_hot100(_payload(), as_of=AS_OF, expected_trade_date=DAY)
    assert result["available"] is True
    assert result["record_count"] == 100
    assert result["records"][0]["rank"] == 1
    assert result["records"][0]["symbol"] == "600001.SH"


def test_collect_fails_closed_on_incomplete_response(tmp_path):
    result = collect_eastmoney_hot100(
        as_of=AS_OF,
        cache_dir=tmp_path,
        expected_trade_date=DAY,
        fetch=lambda _body: _payload(count=99),
        max_attempts=1,
    )
    assert result["available"] is False
    assert result["reason_code"] == "EASTMONEY_HOT100_INCOMPLETE"


def test_previous_day_cache_is_not_reused_as_current(tmp_path):
    prior = date(2026, 9, 2)
    collect_eastmoney_hot100(
        as_of=datetime(2026, 9, 2, 15, 10, tzinfo=TZ),
        cache_dir=tmp_path,
        expected_trade_date=prior,
        fetch=lambda _body: _payload(day=prior),
        max_attempts=1,
    )
    current = collect_eastmoney_hot100(
        as_of=AS_OF,
        cache_dir=tmp_path,
        expected_trade_date=DAY,
        fetch=lambda _body: _payload(count=99),
        max_attempts=1,
    )
    assert current["available"] is False
    assert current["reason_code"] == "EASTMONEY_HOT100_INCOMPLETE"


def test_tampered_same_day_cache_is_refetched_instead_of_trusted(tmp_path):
    first = collect_eastmoney_hot100(
        as_of=AS_OF,
        cache_dir=tmp_path,
        expected_trade_date=DAY,
        fetch=lambda _body: _payload(),
        max_attempts=1,
    )
    cache_path = tmp_path / f"eastmoney-guba-hot100-{DAY.isoformat()}.json"
    tampered = json.loads(cache_path.read_text(encoding="utf-8"))
    tampered["records"][1]["symbol"] = tampered["records"][0]["symbol"]
    cache_path.write_text(json.dumps(tampered, ensure_ascii=False), encoding="utf-8")

    calls = 0

    def fetch(_body):
        nonlocal calls
        calls += 1
        return _payload()

    refreshed = collect_eastmoney_hot100(
        as_of=AS_OF,
        cache_dir=tmp_path,
        expected_trade_date=DAY,
        fetch=fetch,
        max_attempts=1,
    )

    assert first["available"] is True
    assert refreshed["available"] is True
    assert refreshed["cache_status"] == "MISS"
    assert calls == 1
