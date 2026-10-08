from __future__ import annotations

import json
import struct
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.data.board_reference import (
    BoardReferenceClient, ReferenceError, collect_sina_catalog, collect_sina_members,
    collect_ths_catalog, collect_ths_members, import_tdx_blocks,
    load_reference, write_reference, audit_theme_bindings, project_theme_binding,
    tdx_board_members, resolve_theme_reference, import_tdx_industries,
    collect_em_f10_tags, invert_f10_universe,
)

NOW = datetime(2026, 10, 8, 19, 0, tzinfo=ZoneInfo('Asia/Shanghai'))


class Response:
    def __init__(self, body, status=200):
        self.content = body if isinstance(body, bytes) else body.encode('utf-8')
        self.status_code = status


def client(responses):
    queue = iter(responses)
    return BoardReferenceClient(get=lambda *a, **kw: next(queue), now=lambda: NOW, sleep=lambda _: None)


def stock(code):
    return {'symbol': 'sh'+code, 'code': code, 'name': '样本'+code}


def test_sina_catalog_validates_identity_and_retains_raw_evidence():
    r = collect_sina_catalog(client([Response('var x={"gn_test":"gn_test,测试概念,2,0"};')]), 'concept')
    assert r['available'] and r['records'] == [{'board_id': 'gn_test', 'name': '测试概念', 'display_count': 2}]
    assert r['pages'][0]['body'] and r['source_updated_at'] is None
    assert r['production_publish_forbidden'] and r['execution_scope'] == 'SHADOW'


@pytest.mark.parametrize('body', ['<html>验证</html>', '{}', 'var x={"gn_test":"gn_wrong,错误,2"}', 'var x={"gn_test":"gn_test,重复,2","gn_test":"gn_test,重复,2"}'])
def test_sina_invalid_catalog_not_empty_success(body):
    assert not collect_sina_catalog(client([Response(body)]), 'concept')['available']


def test_sina_all_pages_independent_count_and_duplicates():
    rows = [stock(f'{600000+i:06}') for i in range(81)]
    r = collect_sina_members(client([Response('"81"'), Response(json.dumps(rows[:80])), Response(json.dumps(rows[80:])), Response('"81"')]), 'gn_test', '测试')
    assert r['complete'] and len(r['records']) == 81
    assert r['pagination']['received_count'] == r['pagination']['provider_total'] == 81
    assert len(r['pages']) == 4


@pytest.mark.parametrize('tail,total2', [([stock('600000')], '81'), ([], '81'), ([stock('600080')], '82')])
def test_sina_duplicate_missing_and_changed_total_block(tail, total2):
    head = [stock(f'{600000+i:06}') for i in range(80)]
    r = collect_sina_members(client([Response('81'), Response(json.dumps(head)), Response(json.dumps(tail)), Response(total2)]), 'gn_test', '测试')
    assert not r['available'] and not r['complete'] and r['records'] == []
    assert r['pages']  # Failed attempts remain evidence, never a complete cache.


def test_transport_failure_and_request_budget_are_bounded():
    r = collect_sina_members(client([Response('81'), Response('',502)]), 'gn_test', '测试')
    assert r['reason_code'] == 'REFERENCE_HTTP_502'
    c = client([Response('81')]); c.max_requests = 1
    assert collect_sina_members(c,'gn_test','测试')['reason_code'] == 'REFERENCE_REQUEST_BUDGET'


def test_sina_more_than_independent_count_is_conflict_not_silently_truncated():
    r=collect_sina_members(client([Response('1'),Response(json.dumps([stock('600001'),stock('600002')]))]),'gn_test','测试')
    assert r['reason_code']=='REFERENCE_PROVIDER_COUNT_CONFLICT' and r['records']==[]
    assert r['pagination_conflict']['expected_page_count']==1 and r['pagination_conflict']['received_page_count']==2


def html_page(page, last, codes):
    return '<title>概念详情</title><h3>AI PC<span>886071</span></h3><input id="clid" value="886071"><table class="m-table">' + ''.join(f'<tr><td><a href="https://stockpage.10jqka.com.cn/{c}/">名字{c}</a></td></tr>' for c in codes) + f'</table><span class="page_info">{page}/{last}</span>'


def test_ths_catalog_and_members_no_cookie_or_challenge_bypass():
    catalog = collect_ths_catalog(client([Response('<a href="http://q.10jqka.com.cn/gn/detail/code/309121/">AI PC</a>')]))
    assert catalog['records'][0]['board_id'] == '309121'
    r = collect_ths_members(client([Response(html_page(1,2,['600001','600002'])), Response(html_page(2,2,['600003']))]), '309121','AI PC')
    assert r['complete'] and len(r['records']) == 3
    assert r['pagination']['provider_total'] is None  # Page count is not independent member count.


@pytest.mark.parametrize('tail', ['', html_page(1,2,['600003']), html_page(2,3,['600003']), html_page(2,2,['600001'])])
def test_ths_empty_wrong_page_total_change_and_duplicate_block(tail):
    r = collect_ths_members(client([Response(html_page(1,2,['600001','600002'])),Response(tail)]), '309121','AI PC')
    assert not r['complete'] and not r['available']


def test_ths_wrong_board_and_sidebar_not_members():
    body=html_page(1,1,['600001'])
    wrong=collect_ths_members(client([Response(body.replace('AI PC','另一个板块'))]),'309121','AI PC')
    assert wrong['reason_code']=='REFERENCE_BOARD_PAGE_IDENTITY_MISMATCH'
    body+='<table><tr><td><a href="https://stockpage.10jqka.com.cn/600999/">旁栏</a></td></tr></table>'
    result=collect_ths_members(client([Response(body)]),'309121','AI PC')
    assert result['available'] and len(result['records'])==1


def test_snapshot_fallback_preserves_date_and_checks_hash(tmp_path):
    r = collect_sina_members(client([Response('1'),Response(json.dumps([stock('600001')])),Response('1')]),'gn_test','测试')
    path = write_reference(tmp_path,r)
    assert write_reference(tmp_path,r) == path
    loaded = load_reference(tmp_path,'SINA','gn_test',now=NOW+timedelta(days=8),update_failed=True)
    assert loaded['fallback_reused'] and loaded['observed_at'] == r['observed_at']
    catalog=collect_sina_catalog(client([Response('var x={"gn_test":"gn_test,测试,1"}')]),'concept')
    binding={'theme_id':'TEST','source_id':'SINA','board_id':'gn_test','board_name':'测试','approved':True}
    assert project_theme_binding(binding,catalog,loaded,now=NOW+timedelta(days=8))['source_membership_hash']==r['content_hash']
    assert not load_reference(tmp_path,'SINA','gn_test',now=NOW+timedelta(days=15))['available']
    assert not load_reference(tmp_path,'SINA','gn_test',now=NOW-timedelta(seconds=1))['available']
    raw=json.loads(path.read_text()); raw['records'][0]['symbol']='600999.SH';path.write_text(json.dumps(raw))
    assert not load_reference(tmp_path,'SINA','gn_test',now=NOW)['available']


def test_tdx_import_exact_bytes_counts_and_operator_source_time(tmp_path):
    block=bytearray(2813);name='人工智能'.encode('gbk');block[:len(name)]=name
    struct.pack_into('<HH',block,9,2,0);block[13:27]=b'600001\0' + b'000001\0'
    path=tmp_path/'block_gn.dat';path.write_bytes(bytes(384)+struct.pack('<H',1)+block)
    r=import_tdx_blocks(path,now=NOW,source_updated_at=NOW-timedelta(hours=1))
    assert r['complete'] and len(r['records']) == 1
    assert r['records'][0]['member_count']==2 and r['source_updated_at']!=r['observed_at']
    member=tdx_board_members(r,r['records'][0]['board_id'],now=NOW)
    binding={'theme_id':'TEST','source_id':'TDX_LOCAL','board_id':member['board_id'],'board_name':'人工智能','approved':True}
    assert project_theme_binding(binding,r,member,now=NOW)['source_id']=='TDX_LOCAL'
    with pytest.raises(ReferenceError):tdx_board_members(r,r['records'][0]['board_id'],now=NOW+timedelta(days=15))
    path.write_bytes(path.read_bytes()[:-1])
    assert not import_tdx_blocks(path,now=NOW,source_updated_at=NOW)['available']
    assert not import_tdx_blocks(path,now=NOW,source_updated_at=NOW+timedelta(hours=1))['available']


def test_explicit_binding_required_and_source_identity_not_renamed():
    catalog=collect_sina_catalog(client([Response('var x={"gn_test":"gn_test,测试方向,1"}')]),'concept')
    themes=[{'theme_id':'TEST','name':'测试方向','aliases':['测试方向']}]
    assert audit_theme_bindings(themes,[catalog])['themes'][0]['status']=='REVIEW_REQUIRED'
    member=collect_sina_members(client([Response('1'),Response(json.dumps([stock('600001')])),Response('1')]),'gn_test','测试方向')
    binding={'theme_id':'TEST','source_id':'SINA','board_id':'gn_test','board_name':'测试方向','approved':False}
    with pytest.raises(ReferenceError): project_theme_binding(binding,catalog,member,now=NOW)
    binding['approved']=True
    projected=project_theme_binding(binding,catalog,member,now=NOW)
    assert projected['source_id']=='SINA' and projected['source_board_id']=='gn_test'
    assert projected['production_publish_forbidden'] and projected['source_membership_hash']==member['content_hash']
    binding['board_name']='换了名称'
    with pytest.raises(ReferenceError):project_theme_binding(binding,catalog,member,now=NOW)


def test_tdx_industry_prefix_membership_preserves_market(tmp_path):
    catalog=tmp_path/'tdxzs3.cfg';stocks=tmp_path/'tdxhy.cfg'
    catalog.write_bytes('电子设备|881001|2|||T1101\n半导体|881002|2|||T110101\n'.encode('gbk'))
    stocks.write_bytes('1|600001|T110101|||\n0|000001|T110102|||\n'.encode('gbk'))
    r=import_tdx_industries(catalog,stocks,now=NOW,source_updated_at=NOW)
    assert r['available'] and [b['member_count'] for b in r['records']]==[2,1]
    assert r['records'][0]['records'][1]['symbol']=='600001.SH'
    stocks.write_bytes('0|600001|T110101|||\n'.encode('gbk'))
    assert not import_tdx_industries(catalog,stocks,now=NOW,source_updated_at=NOW)['available']


def test_reference_failover_is_explicit_no_union_and_no_relaxed_age():
    catalog=collect_sina_catalog(client([Response('var x={"gn_test":"gn_test,测试,1"}')]),'concept')
    members=collect_sina_members(client([Response('1'),Response(json.dumps([stock('600001')])),Response('1')]),'gn_test','测试')
    bindings=[{'theme_id':'TEST','source_id':'THS_WEB','board_id':'309121','board_name':'测试','approved':True,'priority':1},
              {'theme_id':'TEST','source_id':'SINA','board_id':'gn_test','board_name':'测试','approved':True,'priority':2}]
    result=resolve_theme_reference('TEST',bindings,[catalog],[members],now=NOW)
    assert result['available'] and result['projection']['source_id']=='SINA'
    assert result['fallback_reused'] and result['attempts'][0]['reason_code']=='REFERENCE_SOURCE_NOT_FOUND'
    assert not resolve_theme_reference('TEST',bindings,[catalog],[members],now=NOW+timedelta(days=15))['available']
    bindings[1]['priority']=1
    with pytest.raises(ReferenceError):resolve_theme_reference('TEST',bindings,[catalog],[members],now=NOW)


def test_deadline_and_evidence_budget_fail_closed():
    c=client([Response('{}')]);c.deadline=0
    assert collect_sina_catalog(c)['reason_code']=='REFERENCE_DEADLINE'
    c=client([Response('{}')]);c.max_evidence_bytes=1
    assert collect_sina_catalog(c)['reason_code']=='REFERENCE_EVIDENCE_LIMIT'


def test_raw_bytes_survive_json_storage_and_utf8_or_gbk(tmp_path):
    import base64,hashlib
    raw='var x={"gn_test":"gn_test,测试,1"}'.encode('gbk')
    r=collect_sina_catalog(client([Response(raw)]))
    stored=json.loads(write_reference(tmp_path,r).read_text(encoding='utf-8'))
    assert stored==r
    assert base64.b64decode(stored['pages'][0]['body_base64'])==raw
    assert hashlib.sha256(raw).hexdigest()==stored['pages'][0]['body_sha256']


def f10(symbol,code='1134',name='算力概念'):
    return Response(json.dumps({'ssbk':[{'SECUCODE':symbol,'SECURITY_CODE':symbol[:6],'SECURITY_NAME_ABBR':'样本','BOARD_CODE':code,'BOARD_NAME':name}]}))


def test_f10_reverse_graph_requires_entire_input_universe_and_preserves_identity():
    a=collect_em_f10_tags(client([f10('600001.SH')]),'600001.SH')
    b=collect_em_f10_tags(client([f10('000001.SZ')]),'000001.SZ')
    r=invert_f10_universe(['600001.SH','000001.SZ'],[a,b],now=NOW)
    assert r['available'] and r['records'][0]['member_count']==2
    assert r['records'][0]['board_id']=='1134' and r['source_id']=='EM_F10'
    assert r['catalog_scope']=='INPUT_UNIVERSE_ONLY_NOT_PROVIDER_BOARD_TOTAL'
    missing=invert_f10_universe(['600001.SH','000001.SZ'],[a],now=NOW)
    assert not missing['available'] and missing['records']==[] and missing['missing_symbols']==['000001.SZ']
    assert not invert_f10_universe(['600001.SH'],[a],now=NOW+timedelta(days=15))['available']
    with pytest.raises(ReferenceError):invert_f10_universe(['600001.SH','600001.SH'],[a],now=NOW)


def test_f10_wrong_stock_and_board_name_conflict_block():
    assert not collect_em_f10_tags(client([f10('600002.SH')]),'600001.SH')['available']
    a=collect_em_f10_tags(client([f10('600001.SH')]),'600001.SH')
    b=collect_em_f10_tags(client([f10('000001.SZ',name='另一板块')]),'000001.SZ')
    assert invert_f10_universe(['600001.SH','000001.SZ'],[a,b],now=NOW)['reason_code']=='REFERENCE_BOARD_NAME_CONFLICT'


def test_lazy_f10_fetches_use_receive_clock_not_initial_cutoff():
    later=NOW+timedelta(minutes=1)
    def references():
        c=client([f10('600001.SH')]);c.now=lambda:later
        yield collect_em_f10_tags(c,'600001.SH')
    assert invert_f10_universe(['600001.SH'],references(),now_provider=lambda:later)['available']


def test_non_a_members_keep_provider_count_but_never_enter_projection():
    catalog=collect_sina_catalog(client([Response('var x={"gn_test":"gn_test,测试,2"}')]),'concept')
    rows=[stock('600001'),{'symbol':'sh900901','code':'900901','name':'B股'}]
    member=collect_sina_members(client([Response('2'),Response(json.dumps(rows)),Response('2')]),'gn_test','测试')
    assert member['pagination']['provider_total']==2
    result=project_theme_binding({'theme_id':'TEST','source_id':'SINA','board_id':'gn_test','board_name':'测试','approved':True},catalog,member,now=NOW)
    assert len(result['records'])==1 and result['excluded_non_a_share_symbols']==['900901.SH']


def test_f10_cli_resumes_without_refetch_and_never_claims_full_market(tmp_path,monkeypatch):
    import importlib.util,sys,types
    from pathlib import Path
    spec=importlib.util.spec_from_file_location('collect_board_references_test',Path(__file__).parents[1]/'scripts/collect_board_references.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    clients=iter([client([f10('600001.SH')]),client([f10('000001.SZ')])])
    monkeypatch.setattr(module,'BoardReferenceClient',lambda **kw:next(clients))
    monkeypatch.setattr(module,'datetime',types.SimpleNamespace(now=lambda tz:NOW))
    universe=tmp_path/'universe.json';universe.write_text(json.dumps(['600001.SH','000001.SZ']))
    root=tmp_path/'shadow'
    monkeypatch.setattr(sys,'argv',['collect_board_references.py','--source','em-f10','--universe-json',str(universe),'--output-dir',str(root),'--batch-limit','1'])
    assert module.main()==2
    assert module.main()==0
    reports=[json.loads(p.read_text(encoding='utf-8')) for p in root.glob('reference-audit-*.json')]
    resumed=next(r for r in reports if r['catalog_available'])
    assert resumed['incremental_refresh']['fresh_attempts']==1 and resumed['incremental_refresh']['cache_reused']==1
    assert resumed['universe_audit']['covered_stock_count']==2 and resumed['market_mapping_complete'] is False
