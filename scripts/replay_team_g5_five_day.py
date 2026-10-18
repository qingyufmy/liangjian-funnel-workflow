"""Five-day G5 over sealed ORIGINAL caches, preserving unknown emotion facts.

No new acquisition/model/order. Uses the production five-minute state machine
with its original minute-isolation decision clock; missing facts are not OFF.
"""
import argparse,base64,gzip,hashlib,json,subprocess,sys
from collections import Counter
from contextlib import closing
from datetime import datetime,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a5_frozen_dryrun import save
from liangjian_funnel.runtime.team_v2_regime import load_rules,frozen_candidates
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore,canonical
from liangjian_funnel.runtime.trader_guardrails import is_closed_trader_tick
from liangjian_funnel.evaluation.team_v2_replay_source import session_minutes
TZ=ZoneInfo('Asia/Shanghai');DAYS=tuple('2026-10-'+str(n) for n in range(12,17))


def sealed_caches(directory):
    receipt=json.loads((directory/'EXPORT_RECEIPT.json').read_bytes());archive=directory/'source.jsonl.gz'
    with archive.open('rb') as stream:
        if hashlib.file_digest(stream,'sha256').hexdigest()!=receipt['source_gzip_sha256']:
            raise ValueError('G5_SEALED_ARCHIVE_HASH_MISMATCH')
    result={};files=[]
    with gzip.open(archive,'rt',encoding='utf-8') as stream:
        for line in stream:
            row=json.loads(line)
            if row.get('kind')!='file' or not row.get('relative','').startswith('a4_live_market/'):continue
            rel=Path(row['relative']);day=rel.parent.name
            if day not in DAYS:continue
            raw=base64.b64decode(row['data_b64'],validate=True)
            if hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('G5_ORIGINAL_FILE_HASH_MISMATCH')
            local=directory/'files'/rel
            if local.read_bytes()!=raw:raise ValueError('G5_LOCAL_CACHE_COPY_CHANGED')
            tick=datetime.fromisoformat(day+'T'+rel.stem[:2]+':'+rel.stem[2:]+':00+08:00')
            if (day,tick.isoformat()) in result:raise ValueError('G5_DUPLICATE_ORIGINAL_CACHE')
            body=json.loads(raw)
            if body.get('trade_date')!=day:raise ValueError('G5_ORIGINAL_CACHE_DATE_MISMATCH')
            result[(day,tick.isoformat())]=(body,row['sha256'])
            files.append(dict(relative=row['relative'],sha256=row['sha256']))
    return result,files


def original_facts(cache,*,tick,observed_at):
    if cache is None:
        return dict(market={},emotion={},sector_cycle={},sector_cycle_known_at=None),['ORIGINAL_CACHE_MISSING']
    body,pin=cache;reasons=[]
    if body.get('as_of'):
        at=datetime.fromisoformat(body['as_of'])
        if at.utcoffset() is None or at>observed_at:reasons.append('ORIGINAL_MARKET_NOT_YET_KNOWN_AT_DECISION')
        if at.utcoffset() is not None and at<tick:reasons.append('ORIGINAL_MARKET_CACHE_TOO_OLD')
    else:reasons.append('ORIGINAL_MARKET_SOURCE_CLOCK_MISSING')
    source=body.get('team_v2_source') or {}
    emotion=source.get('emotion') or {}
    # No old summary contains a proved full-universe emotion statistic. Never
    # derive broken-board intraday counts from end-of-day prices or breadth.
    if not emotion:reasons.append('ORIGINAL_EMOTION_FULL_UNIVERSE_COUNTS_NOT_ARCHIVED')
    reasons.append('ORIGINAL_AUCTION_PARENT_SECTOR_GEOMETRY_NOT_ARCHIVED')
    facts=dict(market=body,emotion=emotion,sector_cycle={},sector_cycle_known_at=None,
        original_cache_sha256=pin,source_basis='ORIGINAL_V1_SUMMARY_NOT_TEAM_G5_CAPTURE')
    return facts,reasons


def drive(caches,path,rules):
    store=TeamMeetingStore(path);records=[];daily={day:Counter() for day in DAYS}
    for day in DAYS:
        for decision in session_minutes(day):
            tick=decision-timedelta(minutes=1)
            if not is_closed_trader_tick(tick):continue
            cache=caches.get((day,tick.isoformat()));facts,reasons=original_facts(cache,tick=tick,observed_at=decision)
            primary,emotion=frozen_candidates(facts,tick=tick,observed_at=decision,rules=rules)
            state=store.record_live_regime(facts=facts,tick=tick,observed_at=decision,prior='DATA_BLOCK',rules=rules)
            daily[day]['regular5m_ticks']+=1;daily[day]['primary:'+state['primary_status']]+=1
            daily[day]['emotion:'+state['emotion_state']]+=1
            daily[day]['original_cache_present']+=cache is not None
            daily[day]['leader_open_permission_ticks']+=state['leader_open']
            daily[day]['radar_open_permission_ticks']+=state['radar_open']
            records.append(dict(day=day,tick=tick.isoformat(),decision_at=decision.isoformat(),
                original_cache_sha256=cache[1] if cache else None,
                original_legacy_regime=cache[0].get('regime') if cache else None,
                primary_candidate=primary,emotion_candidate=emotion,result=state,limitations=reasons))
    with closing(store._connect()) as db:
        transitions=[dict(trade_date=r[0],tick=r[1],evidence=json.loads(r[2])) for r in db.execute(
            'SELECT trade_date,tick,evidence_json FROM regime_transitions ORDER BY trade_date,tick,transition_id')]
    return dict(records=records,daily=daily,transitions=transitions)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);a=p.parse_args();output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    rules=load_rules(ROOT/'config/regime_live.yaml');caches,files=sealed_caches(a.source);output.mkdir(parents=True)
    started=datetime.now(TZ).isoformat();first=drive(caches,output/'first.db',rules);second=drive(caches,output/'second.db',rules)
    one=canonical(first).encode();two=canonical(second).encode()
    if one!=two:raise ValueError('G5_REPLAY_TWO_PASS_DIFFERENT')
    save(output/'TRACE.json',first)
    result=dict(status='G5_FIVE_DAY_ORIGINAL_EVIDENCE_LIMITED_NOT_FULL_RELEASE_GATE',actual_started_at=started,
        actual_finished_at=datetime.now(TZ).isoformat(),head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
        source_files=files,config_sha256=rules['config_sha256'],two_pass_equal=True,
        trace_sha256=hashlib.sha256((output/'TRACE.json').read_bytes()).hexdigest(),
        canonical_two_pass_sha256=hashlib.sha256(one).hexdigest(),daily=first['daily'],transitions=first['transitions'],
        emotion_switch_history='UNAVAILABLE_NOT_ASSUMED_ALWAYS_OFF',leader_open_history='UNAVAILABLE_NOT_CLAIMED',
        runtime_permission_during_unknown='CLOSED_DATA_BLOCK',
        original_emotion_proved_ticks=sum(r['emotion_candidate']!='DATA_BLOCK' for r in first['records']),
        minimum_cache_clock_basis='ORIGINAL_TICK_PLUS_ONE_MINUTE_NO_FUTURE_BUCKET_OR_BACKFILL',
        production_writes=0,models=0,orders=0,source_acquisition=0,strategy_performance='NOT_CLAIMED')
    save(output/'RECEIPT.json',result);print(json.dumps(dict(status=result['status'],ticks=len(first['records']),
        raw_cache_files=len(files),two_pass_equal=True,emotion_proved_ticks=result['original_emotion_proved_ticks'],
        sha256=hashlib.sha256((output/'RECEIPT.json').read_bytes()).hexdigest())));return 0

if __name__=='__main__':sys.exit(main())
