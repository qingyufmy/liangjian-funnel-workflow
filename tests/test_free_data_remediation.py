from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.tencent_minute import TencentIntradayAdapter
from liangjian_funnel.data.rotation_theme import _normalize_symbol, _default_tencent_quote_fetch
from liangjian_funnel.data.a2_market import _tencent_symbol
from liangjian_funnel.data.mootdx import map_symbol
from liangjian_funnel.review.verification import A5IndependentVerifier, _field_comparison
from liangjian_funnel.review.indicator_evidence import macd, kdj, audit_event_indicators, daily_macd_check
from liangjian_funnel.pipeline.data_sync import _report_period, _published_at
from liangjian_funnel.pipeline.statement_metrics import derive_statement_metrics


@pytest.mark.parametrize('raw', ['920083', '920083.SH', '920083.SZ', '920083.BJ', 'bj920083'])
def test_beijing_reference_identity_does_not_enable_trading(raw):
    assert _normalize_symbol(raw) == '920083.BJ'
    assert _tencent_symbol(_normalize_symbol(raw)) == 'bj920083'
    with pytest.raises(ValueError):
        map_symbol('920083.BJ')


@pytest.mark.parametrize('symbol,volume', [('688205.SH', 7811), ('000859.SZ', 781100)])
def test_tencent_volume_is_shares_per_market(symbol, volume):
    bar = TencentIntradayAdapter._bar(symbol, '1m', ['202609091000','10','10','10','10','7811'])
    assert bar.volume == volume
    assert bar.amount == volume * 10
    assert bar.normalizer_version == 'tencent-equity-minute-v3'


def test_reference_quote_supports_beijing_and_zero_turnover(monkeypatch):
    fields = [''] * 38
    for k, v in {2:'920083',3:'37.71',4:'37.20',5:'0',6:'0',30:'20260909153449',37:'0'}.items():
        fields[k] = v
    def fetch(url, params, timeout):
        assert params['q'] == 'bj920083'
        return 'v_bj920083="' + '~'.join(fields) + '";'
    monkeypatch.setattr('liangjian_funnel.data.tencent_minute._default_text_fetcher', fetch)
    quote = _default_tencent_quote_fetch('920083.SH')
    assert quote['turnover_cny'] == 0
    assert quote['trade_date'].isoformat() == '2026-09-09'
    fields[37] = '15331.13'
    assert _default_tencent_quote_fetch('920083.BJ')['turnover_cny'] == pytest.approx(153311300)


def test_fiscal_period_is_not_disclosure_date():
    cutoff = datetime(2026, 9, 9, tzinfo=ZoneInfo('Asia/Shanghai'))
    end = datetime(2026, 6, 30, tzinfo=cutoff.tzinfo)
    publish = datetime(2026, 8, 18, tzinfo=cutoff.tzinfo)
    row = {'period_end_ms': end.timestamp() * 1000, 'report_date_ms': publish.timestamp() * 1000}
    assert _report_period(row, cutoff) == '2026-06-30'
    assert _published_at(row, cutoff) == publish
    assert _report_period({'report_date_ms': row['report_date_ms']}, cutoff) != '2026-08-18'


def test_statement_fallback_does_not_mix_periods_or_claim_vendor_coverage():
    rows = [{'_dataset':'INCOME','period_end_ms':2,'net_profit':20,'operating_income':100},
            {'_dataset':'BALANCE','period_end_ms':1,'total_debt':10,'assets_total':50},
            {'_dataset':'CASH_FLOW','period_end_ms':2,'act_cash_flow_net':30}]
    derived = derive_statement_metrics(rows)
    assert derived['metrics']['net_profit_margin_pct']['value'] == 20
    assert derived['metrics']['debt_to_assets_pct']['value'] is None
    assert derived['metrics']['operating_cash_to_net_profit']['value'] == 1.5
    assert not derived['replaces_vendor_indicators']


def test_statement_compaction_keeps_year_on_year_comparison_and_latest_revision():
    from liangjian_funnel.pipeline.statement_metrics import select_statement_periods
    rows=[{'period_end_ms':i,'report_date_ms':i,'fiscal_year':2026 if i>=5 else 2025,
           'fiscal_period':q} for i,q in [(6,'Q2'),(5,'Q1'),(4,'Q4'),(3,'Q3'),(2,'Q2')]]
    rows.append({**rows[-1],'report_date_ms':999})
    result=select_statement_periods(rows)
    assert len(result)==4 and result[0]['period_end_ms']==6
    assert result[-1]['period_end_ms']==2 and result[-1]['report_date_ms']==999


def test_recovery_uses_previous_session_not_opening_price_or_stale_close():
    tz = ZoneInfo('Asia/Shanghai'); cutoff = datetime(2026,9,9,15,tzinfo=tz)
    verifier = A5IndependentVerifier(daily_cache=None,minute_store=None,tencent=None,mootdx=None)
    def bar(day, hour, close):
        return {'bar_end':datetime(2026,9,day,hour,tzinfo=tz), 'close':close}
    fetched = {'600000.SH':{'bars':[bar(8,15,10),bar(9,15,11)]},
               '600001.SH':{'bars':[bar(7,15,10),bar(9,9,10),bar(9,15,11)]}}
    rows = verifier._recover_cross_section([{'symbol':s} for s in fetched],set(fetched),fetched,cutoff)
    assert len(rows) == 1 and rows[0]['symbol'] == '600000.SH'
    assert rows[0]['return'] == pytest.approx(.1)
    assert rows[0]['evidence_scope'].startswith('POST_HOC')


def test_volume_unknown_and_estimated_amount_are_not_falsely_verified():
    a = {'close':10,'volume':100,'volume_unit':'shares','amount':1000,'amount_kind':'ohlc_estimate'}
    b = {**a,'volume':10000,'amount_kind':'reported'}
    check = _field_comparison({'t':a},{'t':b})
    assert check['VOLUME']['status'] == 'MISMATCH'
    assert check['AMOUNT']['status'] == 'DATA_LIMITED'
    assert _field_comparison({'t':{'volume':100}},{'t':b})['VOLUME']['status'] == 'DATA_LIMITED'


def test_independent_macd_kdj_formulas_and_future_input_rejection():
    from datetime import timedelta
    closes = [10 + i * .02 for i in range(40)]
    computed = macd(closes)
    assert daily_macd_check(computed,closes)['formula_status'] == 'MATCH'
    assert daily_macd_check({},closes)['formula_status'] == 'DATA_LIMITED'
    start = datetime(2026,9,9,9,35,tzinfo=ZoneInfo('Asia/Shanghai'))
    rows = [{'end':(start+timedelta(minutes=i*5)).isoformat(),'high':11+i*.02,'low':9+i*.02,'close':v} for i,v in enumerate(closes)]
    declared = {'available':True,'bar_count':40,'closed_bar_end':rows[-1]['end'],'input_series':rows,**kdj(rows)}
    event = {'minute_end':rows[-1]['end'],'payload_json':{'strategy':{'indicator_observations':{'kdj':declared}}}}
    assert audit_event_indicators([event])['counts']['kdj'] == {'MATCH':1}
    event['minute_end'] = rows[-2]['end']
    assert audit_event_indicators([event])['counts']['kdj'] == {'INVALID_INPUT_EVIDENCE':1}


def test_board_history_requires_six_consecutive_closes_and_caches_exact_indices(tmp_path):
    from datetime import date
    from liangjian_funnel.data.board_history import enrich_board_momentum
    dates = ['2026-09-02','2026-09-03','2026-09-04','2026-09-07','2026-09-08','2026-09-09']
    calls = []
    def fetch(code, day):
        calls.append(code)
        return {'board_code':code,'closes':dict(zip(dates,[100,101,102,103,104,105]))}
    rows = [{'component_board_codes':['BK0475']}]
    enrich_board_momentum(rows,tmp_path,date(2026,9,9),fetcher=fetch)
    assert rows[0]['momentum_5d_pct'] == pytest.approx(5)
    assert rows[0]['momentum_3d_pct'] == pytest.approx((105/102-1)*100)
    enrich_board_momentum(rows,tmp_path,date(2026,9,9),fetcher=fetch)
    assert len(calls) == 1
    missing = [{'component_board_codes':['BK0000']}]
    enrich_board_momentum(missing,tmp_path,date(2026,9,9),fetcher=lambda c,d:{'board_code':c,'closes':{dates[-1]:105}})
    assert 'momentum_5d_pct' not in missing[0]
    assert missing[0]['momentum_history_evidence']['status'] == 'DATA_LIMITED'


def test_reported_board_five_day_return_is_not_flow_rank():
    from datetime import date
    from liangjian_funnel.data.board_history import enrich_reported_five_day_momentum
    rows=[{'component_board_codes':['BK0475']}]
    source={'available':True,'period':'5d','trade_date':'2026-09-09','provider_trade_date_verified':True,
            'content_hash':'h','records':[{'code':'BK0475','rank':499,'main_net_cny':9000,'change_pct':1.2}]}
    enrich_reported_five_day_momentum(rows,[source],date(2026,9,9))
    assert rows[0]['momentum_5d_pct'] == 1.2
    stale=[{'component_board_codes':['BK0475']}]
    enrich_reported_five_day_momentum(stale,[source],date(2026,9,10))
    assert 'momentum_5d_pct' not in stale[0]


def test_zero_quote_turnover_is_observed_not_a_manufactured_price():
    from liangjian_funnel.data.rotation_theme import _parse_tencent_reference_quote
    fields=['']*38
    for k,v in {2:'301686',3:'0',4:'0',6:'0',30:'20260909090000',37:'0'}.items(): fields[k]=v
    quote=_parse_tencent_reference_quote('v_sz301686="'+'~'.join(fields)+'";','301686.SZ')
    assert quote['turnover_cny'] == 0 and quote['latest_price'] is None
    assert quote['price_state'] == 'NO_REPORTED_PRICE'
    assert not _parse_tencent_reference_quote('v_sz301686="'+'~'.join(fields)+'";','301699.SZ')


def test_stale_reference_quote_does_not_poison_other_symbols():
    from liangjian_funnel.data.rotation_theme import _same_day_quote_records
    cutoff=datetime(2026,9,9,16,tzinfo=ZoneInfo('Asia/Shanghai'))
    quotes={'600000.SH':{'quote_time':'2026-09-09T15:00:00+08:00'},
            '600001.SH':{'quote_time':'2026-09-08T15:00:00+08:00'}}
    result=_same_day_quote_records(quote_fetch=None,quotes=quotes,symbols=tuple(quotes),expected_trade_date=cutoff.date(),as_of=cutoff)
    assert list(result) == ['600000.SH']


def test_reference_no_trade_mark_is_not_ranked_as_a_counterexample():
    cutoff=datetime(2026,9,9,15,tzinfo=ZoneInfo('Asia/Shanghai'))
    verifier=A5IndependentVerifier(daily_cache=None,minute_store=None,tencent=None,mootdx=None,
        quote_fetch=lambda s:{'symbol':s,'no_reported_trades':True,'turnover_cny':0,'latest_price':18.67,
                              'previous_close':18.67,'quote_time':cutoff,'source_id':'TEST'})
    result=verifier.verify(a2={},market_universe=[{'symbol':'605577.SH'}],plan_rows=[],event_rows=[],cutoff_at=cutoff)
    assert result['a2']['market_cross_section_missing_symbols'] == []
    assert result['a2']['market_cross_section_recovery'][0]['ranking_eligible'] is False
    assert result['counterexamples'] == []


def test_a5_evidence_archive_is_hash_bound_compressed_and_deduplicated(tmp_path):
    import gzip, hashlib, json
    from liangjian_funnel.review.evidence_archive import archive_observation
    payload={'cutoff_at':'2026-09-09T15:00:00+08:00','bars':[{'close':10}]*200}
    ref=archive_observation(tmp_path,payload)
    assert archive_observation(tmp_path,payload)==ref
    path=tmp_path/ref['relative_path'];raw=gzip.decompress(path.read_bytes())
    assert hashlib.sha256(raw).hexdigest()==ref['sha256'] and json.loads(raw)==payload
    assert ref['compressed_bytes'] < ref['uncompressed_bytes']
