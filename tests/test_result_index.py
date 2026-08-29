import json
from pathlib import Path

from liangjian_funnel.pipeline.result_index import snapshot_name_catalog, write_lane_result_index


def test_result_index_writes_all_4017_rows_without_market_truncation(tmp_path: Path):
    rejected = [
        {"symbol": f"{index:06d}.SZ", "reason_codes": ["LOCAL_SCORE_LOW"]}
        for index in range(1, 4018)
    ]
    manifest = write_lane_result_index(
        tmp_path,
        run_id="run-1",
        lane_id="lane_1",
        stages=[{"stage": "A1", "output": {
            "active_research_pool": [{"symbol": "600519.SH", "name": "贵州茅台"}],
            "monitor_pool": [],
            "rejected_candidates": rejected,
        }}],
    )
    assert manifest["counts"]["A1"] == {"approved": 1, "watch": 0, "rejected": 4017}
    lines = Path(manifest["data_path"]).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4018
    assert json.loads(lines[-1])["symbol"] == "004017.SZ"
    assert manifest["reason_options"]["A1"]["rejected"] == ["LOCAL_SCORE_LOW"]


def test_snapshot_names_are_embedded_in_index_rows(tmp_path: Path):
    names = snapshot_name_catalog({
        "universe_candidates": [{"symbol": "600519.SH", "name": "贵州茅台"}],
    })
    manifest = write_lane_result_index(
        tmp_path,
        run_id="run-2",
        lane_id="lane_2",
        name_catalog=names,
        stages=[{"stage": "A2", "output": {
            "focus_pool": [{"symbol": "600519.SH", "reason_codes": ["ROLE_LEADER"]}],
            "watch_only_pool": [],
            "rejected_candidates": [],
        }}],
    )
    row = json.loads(Path(manifest["data_path"]).read_text(encoding="utf-8"))
    assert row["item"]["name"] == "贵州茅台"
    assert row["item"]["name_source"] == "snapshot_index"
