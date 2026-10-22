"""Offline counterexamples + real small-scale constructor/monitor subprocess."""
from copy import deepcopy
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

SCRIPT = Path(__file__).resolve().parents[1]/'scripts/profile_monitor_parent_0589.py'
spec = importlib.util.spec_from_file_location('profile_monitor_parent_0589',SCRIPT)
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


def packet():
    from liangjian_funnel.data.session_windows import closed_window_ends
    at = datetime.fromisoformat('2026-10-22T14:25:00+08:00')
    ends = closed_window_ends(at.replace(minute=24),'1m')
    symbols = ['000333.SZ','002270.SZ','301487.SZ']
    return dict(schema_version=profile.SCHEMA,as_of=at.isoformat(),
        plans=[dict(plan_id='profile-'+symbol,symbol=symbol,payload=dict(
            strategy_profile='TREND_MA5',stock_behavior_type='TREND',trigger_low=109,trigger_high=110,
            stop_level=89,no_chase_price=112,first_resistance=130,minimum_reward_risk=2,
            source_run_id='CONTROLLED_PROFILE')) for symbol in symbols],
        bars=[dict(symbol=symbol,interval='1m',bar_end=end.isoformat(),open=100,high=100.2,
            low=99.8,close=100,volume=10000,amount=1000000,source_id='CONTROLLED_PROFILE',
            volume_unit='shares',adjust_mode='none',amount_kind='reported',normalizer_version='controlled/1')
            for symbol in symbols for end in ends],
        quotes=[dict(symbol=symbol,quote_time=at.isoformat(),price=100,open=100,
            previous_close=100,volume=1000000,amount=100000000,source_id='CONTROLLED_PROFILE') for symbol in symbols],
        market_state=dict(status='READY',entry_permission='ALLOW',as_of=at.isoformat(),trade_date='2026-10-22'),
        provenance={category:dict(kind='CONTROLLED',reason='Offline test input, not historical evidence')
            for category in ('plans','bars','quotes','market_state')})


def save(tmp_path, body):
    path=tmp_path/'packet.json'
    path.write_text(json.dumps(body),encoding='utf-8')
    return path,profile.digest(path)


def test_packet_original_requires_exact_bytes_and_subtree(tmp_path):
    body=packet()
    source=tmp_path/'original.json'
    source.write_text(json.dumps(dict(data=body['quotes'])),encoding='utf-8')
    body['provenance']['quotes']=dict(kind='ORIGINAL',path=str(source),sha256=profile.digest(source),pointer='/data')
    path,sha=save(tmp_path,body)
    assert profile.load_packet(path,sha)['quotes']==body['quotes']
    body['quotes'][0]['price']=101
    path,sha=save(tmp_path,body)
    with pytest.raises(ValueError,match='SUBTREE_MISMATCH'):profile.load_packet(path,sha)


@pytest.mark.parametrize('change,reason',[
    (lambda b:b['provenance']['bars'].update(kind='UNKNOWN'),'PROVENANCE_REQUIRED'),
    (lambda b:b['provenance']['market_state'].update(reason=''),'CONTROLLED_REASON_REQUIRED'),
    (lambda b:b['plans'].pop(),'THREE_DISTINCT'),
    (lambda b:b['bars'].append(deepcopy(b['bars'][0])),'DUPLICATE_BAR'),
    (lambda b:b['bars'][0].update(bar_end=b['as_of']),'FROZEN_CLOSED'),
    (lambda b:b['quotes'][0].update(quote_time='2026-10-22T14:26:00+08:00'),'QUOTE_CLOCK'),
])
def test_bad_packet_fails_before_monitor(tmp_path,change,reason):
    body=packet();change(body);path,sha=save(tmp_path,body)
    with pytest.raises(ValueError,match=reason):profile.load_packet(path,sha)


def test_wrong_packet_sha_rejected(tmp_path):
    path,_=save(tmp_path,packet())
    with pytest.raises(ValueError,match='SHA_MISMATCH'):profile.load_packet(path,'0'*64)


def test_percentile_nearest_rank_and_phase_totals():
    rows=[dict(parent_wall_ms=i,timing_spans=[dict(name='database',duration_ms=i),
        dict(name='database',duration_ms=i)]) for i in range(1,11)]
    result=profile.summarize(rows)
    assert result['p50_ms']==5 and result['p95_ms']==10
    assert result['phases']['database']['max_ms']==20


@pytest.mark.parametrize('missing_fact',[False,True])
def test_real_constructor_monitor_and_fresh_process_small_profile(tmp_path,missing_fact):
    body=packet()
    if missing_fact:
        for plan in body['plans']:plan['payload'].update(trigger_low=90,trigger_high=91)
    path,sha=save(tmp_path,body)
    proc=subprocess.run([sys.executable,'-B',str(SCRIPT),'--directory',str(tmp_path),
        '--packet',str(path),'--packet-sha256',sha,'--bytes','0','--samples','2','--cleanup'],
        capture_output=True,text=True,timeout=90)
    # Include diagnostics only in offline test failure, never actual provider payloads.
    assert proc.returncode==(1 if missing_fact else 0),(proc.stdout,proc.stderr,
        [(str(p),p.read_text()) for p in tmp_path.glob('monitor-parent-*/FAILURE.json')])
    result=json.loads(proc.stdout)
    assert result['status']==('EVIDENCE_LIMITED_SCOPE_BLOCKED' if missing_fact else 'NOT_GATE_SCALE')
    assert result['model_http_calls']==0
    assert len({row['pid'] for row in result['samples']})==2
    for sample in result['samples']:
        assert sample['constructor_ms']>0 and sample['monitor_ms']>0
        assert len(sample['required_scope'])==3
        assert len(sample['ready_scope'])==(0 if missing_fact else 3)
        assert len(sample['blocked_scope'])==(3 if missing_fact else 0)
        assert sample['acquisition_calls']['market']==1
        assert sample['forbidden_attempts']==[]
        assert any(span['name']=='round_total' for span in sample['timing_spans'])
    assert result['provenance']['bars']=='CONTROLLED'
    assert result['generated_files_removed'] and not Path(result['output_root']).exists()
    assert 'parent:constructor_ms' in result['summary']['phases']


def test_guards_count_swallowed_model_and_block_external_write_and_socket(tmp_path):
    # Isolate permanent audit hooks in their own short-lived interpreter.
    code='''
import importlib.util,json,pathlib,sys,socket
sys.path.insert(0,sys.argv[1])
s=importlib.util.spec_from_file_location('p',sys.argv[2]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
seen=m.install_guards(pathlib.Path(sys.argv[3]))
for action in (lambda:OpenAICompatibleModelClient.complete(None),
               lambda:pathlib.Path(sys.argv[4]).write_text('never'),
               lambda:socket.create_connection(('127.0.0.1',1))):
 try:action()
 except RuntimeError:pass
print(json.dumps(seen))
'''
    proc=subprocess.run([sys.executable,'-B','-c',code,str(profile.ROOT/'src'),str(SCRIPT),str(tmp_path/'owned'),
        str(tmp_path/'must-not-write')],capture_output=True,text=True,timeout=30)
    assert proc.returncode==0,proc.stderr
    assert json.loads(proc.stdout)==['MODEL','WRITE_OUTSIDE_OWNED_ROOT','NETWORK']
    assert not (tmp_path/'must-not-write').exists()
