"""Exact1211/1214 continuation, not a general closed-capture extension.

Claude1214 authorizes only the exact continuation below. The
ordinary close, snapshot/replay and research-only resume gates stay unchanged.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import hashlib,re
from .closed_session_capture import KEY,aware,validate_research_capture_proof
from .calendar import latest_closed_research_session
from .publication_receipt import BoundObject,object_hash

AUTHORIZATION=dict(authority='Claude1214',capture_authority='Claude1211',authority_sha256='49f4693a4322e62c715293ff9b2c0fe15229277569955fd1fadd789818b420da',
    clock_fix_root='c678cc4349f25dd9235a1a84d3f448d877b62487',
    source_trade_date='2026-10-29',target_trade_date='2026-10-30',
    not_before='2026-10-30T11:31:00+08:00',publication_deadline='2026-10-30T14:00:00+08:00',
    capture_deadline='2026-10-30T09:15:00+08:00',
    snapshot_id='snapshot-20261030T084937+0800-f8c29f0f7b39',
    snapshot_hash='f8c29f0f7b39b7761fb3de883c2e73056dfa418f795bf519943d65536d29b49e',
    snapshot_file_sha256='dac7b2bfdcebf005254701a5494b1c5d002db620fe79e970ab961a4dccc21062',
    a1_generation_id='a1-incremental-20261030T073156197283Z-bf0ff36210f4',
    a1_manifest_hash='a2a1e9b11f45c8b74bd44a5849efed9012ce9f0e2fc4e573973d99ff36c6e88c',
    a1_payload_hash='f5e1a0c621b2de4a7ba7c3f8ab36c59ca354c6d1c8851c11ae974201d7253c20',
    a2_news_node_sha256='0d4daae5adcfb7417a38b8e54f64b98542c018fa31e5b82700a8b2a1bab62d02')

@dataclass(frozen=True)
class ClosedRecoveryResume:
    authorization: Mapping
    run_id: str
    news: BoundObject

def need(ok,reason):
    if not ok:raise ValueError('CLOSED_SESSION_RESUME_'+reason)

def require_window(binding,calendar,*,now):
    need(type(binding)is ClosedRecoveryResume and isinstance(binding.authorization,Mapping),'EXACT_AUTHORITY_REQUIRED')
    grant=dict(binding.authorization);head=grant.pop('H',None)
    need(grant==AUTHORIZATION and isinstance(head,str) and re.fullmatch('[0-9a-f]{40}',head)is not None,'EXACT_AUTHORITY_REQUIRED')
    need(re.fullmatch(r'recovery-close-20261029-[0-9a-f]{7}-1211-r4',binding.run_id)is not None,'NEW_R4_ID_REQUIRED')
    current=aware(now);grant=binding.authorization;session=latest_closed_research_session(current,calendar)
    need(aware(grant['not_before'])<=current<aware(grant['publication_deadline'])
        and session.source_trade_date.isoformat()==grant['source_trade_date']
        and session.target_trade_date.isoformat()==grant['target_trade_date'],'WINDOW_OR_SESSION_INVALID')
    need(type(binding.news)is BoundObject and binding.news.sha256==grant['a2_news_node_sha256']
        and object_hash(binding.news.value)==binding.news.sha256,'EXACT_NEWS_NODE_REQUIRED')
    return session

def require_generation(app,binding,*,now):
    require_window(binding,app.trading_calendar,now=now);require_head(app.settings.root,binding);g=app.a1_registry.get_active_generation();a=binding.authorization
    need(g is not None and g.is_sealed and g.generation_id==a['a1_generation_id']
        and g.manifest_hash==a['a1_manifest_hash']and g.payload_hash==a['a1_payload_hash'],'SEALED_ACTIVE_A1_CHANGED')
    daily=g.manifest.get('delta',{}).get('daily_refresh_proof',{})
    need(daily.get('snapshot_id')==g.snapshot_id and daily.get('snapshot_hash')==g.snapshot_hash,'A1_CAPTURE_BINDING_INVALID')
    validate_research_capture_proof(daily.get('closed_session'),captured_at=g.as_of,
        calendar=app.trading_calendar,now=now,source_trade_date=a['source_trade_date'])
    return g

def load_seed(app,binding,*,now,prepared=None):
    require_generation(app,binding,now=now);a=binding.authorization;p=app.settings.snapshot_dir/(a['snapshot_id']+'.json')
    need(p.is_file()and not p.is_symlink()and p.resolve().is_relative_to(app.settings.snapshot_dir.resolve()),'SEED_PATH_INVALID')
    digest=hashlib.sha256()
    with p.open('rb')as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
    need(digest.hexdigest()==a['snapshot_file_sha256'],'SEED_FILE_CHANGED')
    if prepared is None:prepared=app._load_research_snapshot_by_id(a['snapshot_id'],expected_date=a['target_trade_date'])
    s=prepared.snapshot
    need(s.snapshot_id==a['snapshot_id']and s.snapshot_hash==a['snapshot_hash']
        and object_hash(s.data)==a['snapshot_hash']and prepared.path.resolve()==p.resolve(),'SEED_OBJECT_CHANGED')
    proof=validate_research_capture_proof(s.data.get(KEY),captured_at=s.as_of,
        calendar=app.trading_calendar,now=now,source_trade_date=a['source_trade_date'])
    need(proof['target_trade_date']==a['target_trade_date']and proof['publication_deadline']==a['capture_deadline']
        and aware(proof['capture_completed_at'])<aware(a['capture_deadline'])
        and s.data.get('MARKET_DATA_AS_OF')==proof['source_market_close_at']
        and s.data.get('snapshot_manifest',{}).get('as_of')==s.as_of.isoformat(),'ORIGINAL_CAPTURE_OR_MARKET_CHANGED')
    return prepared

def require_arguments(binding,*,slot,as_of,historical_replay,comparison_run,primary_only,from_active_a1,
        publish_plans,record_runtime,run_id,snapshot_id,snapshot_expected_hash,active_a1_generation_id,
        regeneration,research_update,frozen_a2_stock_news,target_trade_date,market_data_as_of,
        schedule_comparison,allow_non_trading_source,auction_refresh,same_day_recovery,models,primary_lane_ids):
    need(type(binding)is ClosedRecoveryResume,'EXACT_AUTHORITY_REQUIRED');a=binding.authorization
    need(slot=='close'and as_of is None and primary_only is True and from_active_a1 is True
        and publish_plans is True and record_runtime is True and run_id==binding.run_id
        and snapshot_id is None and snapshot_expected_hash==a['snapshot_hash']
        and active_a1_generation_id==a['a1_generation_id']and regeneration is None and research_update is None
        and frozen_a2_stock_news is None and target_trade_date is None and market_data_as_of is None
        and not any((historical_replay,comparison_run,schedule_comparison,allow_non_trading_source,
                    auction_refresh,same_day_recovery))and models is None and primary_lane_ids is None,'ARGUMENTS_INVALID')


def require_head(root,binding):
    """Bind the actual installed implementation to its deployed Git identity."""
    import subprocess
    root=Path(root)
    def git(*args):return subprocess.check_output(['git','-C',str(root),*args],stderr=subprocess.DEVNULL)
    head=binding.authorization['H']
    need(git('rev-parse','HEAD').decode().strip()==head,'HEAD_CHANGED')
    need(subprocess.run(['git','-C',str(root),'merge-base','--is-ancestor',AUTHORIZATION['clock_fix_root'],head],
        stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0,'CLOCK_ROOT_CHANGED')
    package=Path(__file__).resolve().parent.parent
    for name in ('runtime/closed_recovery_resume.py','workflow.py',
            'pipeline/research/common.py','pipeline/team_v2_research_time.py',
            'pipeline/team_v2_research.py','pipeline/team_v2_a2_research.py','runtime/research_update.py'):
        need(hashlib.sha256((package/name).read_bytes()).digest()==
            hashlib.sha256(git('show',head+':src/liangjian_funnel/'+name)).digest(),'INSTALLED_SOURCE_CHANGED')
