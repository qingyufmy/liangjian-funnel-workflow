"""Actual standalone CLI against a local independent SQLite+JSONL fixture."""
import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from liangjian_funnel.runtime.shadow_evidence import ShadowEvidenceLedger
from test_w3_shadow_day_adapter import source
from test_w2_shadow_evidence import at, evidence


def cli():
    path=Path(__file__).resolve().parents[1]/"scripts/build_shadow_daily.py"
    spec=importlib.util.spec_from_file_location("shadow_daily_cli_fixture",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.main


def inputs(tmp_path):
    view,census,production=source()
    # Static local fixture dates, not a changed production business timestamp.
    view,census,production=json.loads(json.dumps([view,census,production]).replace("2026-10-12","2026-10-08"))
    ledger=ShadowEvidenceLedger(tmp_path/"source-shadow.sqlite3",tmp_path/"source-shadow.jsonl")
    assert ledger.seal_price_limit_evidence("p1",evidence(symbol="600000.SH"),observed_at=at("09:26"))["ok"]
    plan=census["plans"][0]
    p={**plan,"valid_from":plan["activated_at"],"valid_until":plan["expires_at"],"stop_level":9.5}
    for item in view["signals"]:
        signal={**item["signal"],"schema":"a4-shadow-signal/1","cohort":"REALTIME_SHADOW"}
        assert ledger.record_signal(signal,p,observed_at=at())["ok"]
    summary={**view["minutes"][0],"elapsed_ms":1.}
    assert ledger.record_minute(summary,observed_at=at())["ok"]
    for name,value in (("census.json",census),("production.json",production)):
        (tmp_path/name).write_text(json.dumps(value),encoding="utf-8")
    args=["--ledger",str(tmp_path/"source-shadow.sqlite3"),"--mirror",str(tmp_path/"source-shadow.jsonl"),
        "--census",str(tmp_path/"census.json"),"--production",str(tmp_path/"production.json"),
        "--as-of","2026-10-08T15:30:00+08:00","--output",str(tmp_path/"new-report")]
    return args


def hashes(tmp_path):
    return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in tmp_path.iterdir() if p.is_file()}


def test_real_cli_reads_without_source_mutation_and_binds_output(tmp_path):
    args=inputs(tmp_path);before=hashes(tmp_path)
    assert cli()(args)==0
    assert hashes(tmp_path)==before
    manifest=json.loads((tmp_path/"new-report/manifest.json").read_text(encoding="utf-8"))
    assert manifest["model_calls"]==manifest["provider_calls"]==manifest["notifications"]==0
    assert manifest["production_mutation"] is False
    for name,digest in manifest["output_files"].items():
        assert hashlib.sha256((tmp_path/"new-report"/name).read_bytes()).hexdigest()==digest
    assert json.loads((tmp_path/"new-report/production-equivalence.json").read_text())["status"]=="MATCHED"
    with pytest.raises(SystemExit):cli()(args)
    assert hashes(tmp_path)==before


def test_cli_mirror_divergence_is_limited_not_complete(tmp_path):
    args=inputs(tmp_path)
    (tmp_path/"source-shadow.jsonl").write_bytes(b"{}\n")
    before=hashes(tmp_path)
    assert cli()(args)==2
    result=json.loads((tmp_path/"new-report/report.json").read_text(encoding="utf-8"))
    assert "SHADOW_MIRROR_NOT_SYNCED" in result["gap_codes"]
    assert hashes(tmp_path)==before
