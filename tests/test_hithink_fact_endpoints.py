from pathlib import Path

import httpx

from liangjian_funnel.pipeline.data_source import HithinkClient
from liangjian_funnel.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env(
        {
            "HITHINK_FINANCE_API_KEY": "unit-secret",
            "ASTOCK_HITHINK_MIN_REQUEST_INTERVAL_SECONDS": "0",
        },
        root=tmp_path,
    )


def test_index_and_auction_endpoints_keep_documented_request_shape(tmp_path: Path):
    calls: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, dict(request.url.params)))
        if request.url.path.endswith("ths-index-list"):
            items = [{"thscode": "881101.TI", "name": "行业A"}]
            data = {"timestamp": 1, "item": items}
        elif request.url.path.endswith("ths-stock-list"):
            data = {"timestamp": 2, "item": [{"thscode": "600519.SH", "ticker": "600519", "name": "A"}]}
        elif request.url.path.endswith("prices/snapshot"):
            data = {"timestamp": 3, "item": [{"thscode": "881101.TI", "last_price": 10}]}
        else:
            data = {
                "timestamp": 4,
                "auction_phase": "final",
                "data_status": "ready",
                "total": 1,
                "item": [{"thscode": "600519.SH", "auction_price": 10}],
            }
        return httpx.Response(200, json={"code": 0, "data": data}, request=request)

    client = HithinkClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    catalog = client.ths_index_catalog(tag="industry")
    members = client.ths_index_constituents("881101.TI")
    index = client.index_snapshot(["881101.TI"])
    auction = client.auction_snapshot(["600519.SH"], stage="final")
    client.close()

    assert catalog.ok and members.ok and index.ok and auction.ok
    assert calls == [
        ("/api/a-share-index/catalog/ths-index-list", {"tag": "industry"}),
        ("/api/a-share-index/constituents/ths-stock-list", {"thscode": "881101.TI"}),
        ("/api/a-share-index/prices/snapshot", {"thscodes": "881101.TI"}),
        ("/api/a-share/auction/snapshot", {"thscodes": "600519.SH", "stage": "final"}),
    ]
    assert auction.metadata == {
        "timestamp": 4,
        "auction_phase": "final",
        "data_status": "ready",
        "total": 1,
    }


def test_limit_pool_uses_page_size_pagination_and_preserves_metadata(tmp_path: Path):
    calls: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        query = dict(request.url.params)
        calls.append(query)
        page = int(query["page"])
        rows = (
            [
                {"thscode": "600001.SH", "continue_day_cnt": 2},
                {"thscode": "600002.SH", "continue_day_cnt": 1},
            ]
            if page == 1
            else [{"thscode": "600003.SH", "continue_day_cnt": 1}]
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "timestamp": 10,
                    "pagination": {"total": 3, "pages": 2, "size": 2, "page": page},
                    "item": rows,
                },
            },
            request=request,
        )

    client = HithinkClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    result = client.limit_up_pool(date_ms=1, size=2, max_pages=3)
    client.close()

    assert result.ok and result.complete
    assert result.pages == 2 and result.total == 3
    assert [row.thscode for row in result.items] == ["600001.SH", "600002.SH", "600003.SH"]
    assert calls == [
        {"page": "1", "size": "2", "sort_field": "continue_day_cnt", "sort_dir": "desc", "date_ms": "1"},
        {"page": "2", "size": "2", "sort_field": "continue_day_cnt", "sort_dir": "desc", "date_ms": "1"},
    ]


def test_non_trading_day_empty_limit_pool_is_successful_empty_fact(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "timestamp": 10,
                    "pagination": {"total": 0, "pages": 0, "size": 200, "page": 1},
                    "item": [],
                },
            },
            request=request,
        )

    client = HithinkClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    result = client.limit_break_pool()
    client.close()
    assert result.ok and result.complete and result.items == () and result.total == 0


def test_ladder_dragon_tiger_and_attention_keep_collections_separate(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("limit-up-ladder"):
            data = {"timestamp": 1, "window": {"length": 30}, "item": [{"date": "20260825", "boards": {}}]}
        elif request.url.path.endswith("dragon-tiger-list"):
            data = {
                "timestamp": 2,
                "board_type": "all",
                "trade_date": "2026-08-25",
                "count": 2,
                "stock_count": 1,
                "stock_items": [{"thscode": "600519.SH", "net_value": 1}],
                "hot_money_items": [{"name": "seat-a", "net_value": 2}],
            }
        else:
            data = {"timestamp": 3, "item": [{"thscode": "600519.SH", "rank": 1}]}
        return httpx.Response(200, json={"code": 0, "data": data}, request=request)

    client = HithinkClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    ladder = client.limit_up_ladder()
    dragon = client.dragon_tiger_list(date="2026-08-25")
    hot = client.hot_stock_list(period="hour")
    client.close()

    assert ladder.ok and ladder.metadata["window"] == {"length": 30}
    assert dragon.ok and [row.collection for row in dragon.items] == ["stock_items", "hot_money_items"]
    assert dragon.metadata["trade_date"] == "2026-08-25"
    assert hot.ok and hot.items[0].rank == 1


def test_fact_endpoint_input_validation_fails_before_network(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500, request=request)

    client = HithinkClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    results = [
        client.ths_index_catalog(tag="sw"),
        client.ths_index_constituents("not-a-symbol"),
        client.index_snapshot([]),
        client.auction_snapshot(["600519.SH"], stage="later"),  # type: ignore[arg-type]
        client.limit_up_pool(size=0),
        client.limit_up_pool(sort_field="unknown"),
        client.dragon_tiger_list(board_type="other"),  # type: ignore[arg-type]
        client.hot_stock_list(period="week"),  # type: ignore[arg-type]
    ]
    client.close()
    assert all(not result.ok for result in results)
    assert calls == 0


def test_balance_and_cash_flow_are_single_page_financial_contracts(tmp_path: Path):
    calls: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, dict(request.url.params)))
        return httpx.Response(
            200,
            json={"code": 0, "data": {"item": [{"period_end": "2026-06-30", "value": 1}]}},
            request=request,
        )

    client = HithinkClient(_settings(tmp_path), transport=httpx.MockTransport(handler), sleep=lambda _: None)
    balance = client.balance_sheets("600519.SH", limit=20)
    cash = client.cash_flow_statements("600519.SH", limit=20)
    client.close()
    assert balance.ok and cash.ok
    assert all(call[1]["period"] == "quarterly" for call in calls)
    assert all(call[1]["limit"] == "20" and call[1]["offset"] == "0" for call in calls)


def test_success_without_documented_collection_fails_closed(tmp_path: Path):
    client = HithinkClient(
        _settings(tmp_path),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"code": 0, "data": {"timestamp": 1}}, request=request)
        ),
        sleep=lambda _: None,
    )
    result = client.limit_up_ladder()
    client.close()
    assert not result.ok and result.reason_code == "MALFORMED_DATA"
