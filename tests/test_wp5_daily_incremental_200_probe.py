import importlib.util
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('probe200', ROOT/'scripts/probe_wp5_daily_incremental_200_readonly.py')
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


def test_special_dates_require_official_primary_source_not_short_history():
    rows = json.loads((ROOT/'artifacts/wp5-20261010/daily-incremental-200-special-reference.json').read_text(encoding='utf-8'))['records']
    probe.validate_specials(rows)
    bad = [{**r} for r in rows]
    bad[0]['announcement_url'] = 'https://example.com/short-history'
    with pytest.raises(ValueError, match='OFFICIAL'):
        probe.validate_specials(bad)
    bad = [{**r} for r in rows]
    bad[0]['effective_date'] = '2026-08-31'
    with pytest.raises(ValueError, match='SEPTEMBER'):
        probe.validate_specials(bad)


def test_manifest_tamper_and_duplicate_rejected():
    path = ROOT/'artifacts/wp5-20261010/daily-incremental-200-manifest.json'
    body = path.read_bytes()
    manifest = probe.validate_manifest(body, probe.digest(body))
    assert len(manifest['records']) == 200
    assert manifest['counts'] == {'MAIN':80, 'CHINEXT':50, 'STAR':40, 'BJ':20, 'SPECIAL':10}
    assert len({r['symbol'] for r in manifest['records']}) == 200
    with pytest.raises(ValueError, match='HASH'):
        probe.validate_manifest(body+b' ', probe.digest(body))
    manifest['records'][0] = manifest['records'][1]
    changed = probe.canonical(manifest)
    with pytest.raises(ValueError, match='DUPLICATE'):
        probe.validate_manifest(changed, probe.digest(changed))


def test_compare_does_not_promote_empty_or_missing_volume():
    row = {'date_ms':1791475200000,'open_price':1,'high_price':2,'low_price':1,
           'close_price':2,'volume':100,'turnover':200}
    assert probe.compare_bars([], [])['status'] == 'DATA_LIMITED'
    assert probe.compare_bars([row], [{**row,'volume':101}])['status'] == 'CONFLICT'
    assert probe.compare_bars([row], [{k:v for k,v in row.items() if k!='volume'}])['status'] == 'DATA_LIMITED'
    comparison = probe.compare_bars([row], [{**row,'volume':100.0}])
    assert comparison['status'] == 'MATCH'
    assert comparison['incremental_sha256'] == comparison['full_sha256']


def test_dry_run_never_constructs_settings_client_or_cache(monkeypatch, capsys):
    monkeypatch.setattr(probe.Settings,'from_env',lambda **kw: pytest.fail('settings read'))
    path = ROOT/'artifacts/wp5-20261010/daily-incremental-200-manifest.json'
    assert probe.main(['--manifest',str(path),'--expected-sha256',probe.digest(path.read_bytes())]) == 0
    assert json.loads(capsys.readouterr().out)['source_calls'] == 0


def test_fixture_three_phases_reuse_core_and_preserve_full_history(tmp_path):
    from liangjian_funnel.pipeline.data_source import HithinkFetchResult, HithinkRow
    from liangjian_funnel.settings import Settings
    tz = ZoneInfo('Asia/Shanghai')
    start = datetime(2026,8,1,tzinfo=tz)
    from datetime import timedelta
    rows = [{'date_ms':int((start+timedelta(days=i)).timestamp()*1000),'open_price':10,
             'high_price':12,'low_price':9,'close_price':11,'volume':1000,'turnover':11000}
            for i in range(70)]
    class Client:
        calls = []
        def history_1d(self,symbol,**kw):
            self.calls.append(kw)
            items = [r for r in rows if kw['start']<=r['date_ms']<kw['end']]
            return HithinkFetchResult(endpoint='history',ok=True,complete=True,reason_code='OK',
                items=tuple(HithinkRow.model_validate(r) for r in items),fetch_time=datetime(2026,10,10,tzinfo=tz))
    client = Client()
    report = probe.run_probe(Settings.from_env({},root=tmp_path),
        [{'symbol':'600000.SH','stratum':'MAIN'}],tmp_path/'run',client=client,
        observed_at=datetime(2026,10,10,tzinfo=tz))
    record = json.loads((tmp_path/'run/records/600000.SH.json').read_text())
    assert record['comparison']['status'] == 'MATCH'
    assert record['incremental']['request']['mode'] == 'INCREMENTAL'
    assert record['comparison']['incremental_count'] > 30
    assert len(client.calls) == 3
    assert report['matched_count'] == 1


def test_output_overwrite_and_trading_window_rejected_before_source(tmp_path):
    from liangjian_funnel.settings import Settings
    settings = Settings.from_env({},root=tmp_path)
    with pytest.raises(ValueError,match='OVERWRITE'):
        probe.run_probe(settings,[],tmp_path)
    with pytest.raises(ValueError,match='TRADING_WINDOW'):
        probe.run_probe(settings,[],tmp_path/'blocked',observed_at=datetime(2026,10,9,10,tzinfo=ZoneInfo('Asia/Shanghai')))
    assert not (tmp_path/'blocked').exists()


def test_source_failure_not_identical_empty_success_and_exception_not_leaked(tmp_path):
    from liangjian_funnel.settings import Settings
    class FailedClient:
        def history_1d(self,*args,**kw):
            raise RuntimeError('Bearer SUPER_SECRET_SHOULD_NOT_LEAK')
    report = probe.run_probe(Settings.from_env({},root=tmp_path),
        [{'symbol':'600929.SH','stratum':'SPECIAL'}],tmp_path/'failed',client=FailedClient(),
        observed_at=datetime(2026,10,10,tzinfo=ZoneInfo('Asia/Shanghai')))
    assert report['matched_count'] == 0
    assert report['exit_code'] == 2
    assert report['status'] == 'DATA_LIMITED'
    assert report['source_calls'] == 1
    body = (tmp_path/'failed/records/600929.SH.json').read_text()
    assert 'SUPER_SECRET' not in body
