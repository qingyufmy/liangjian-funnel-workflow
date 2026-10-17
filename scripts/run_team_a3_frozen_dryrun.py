"""Independent original A2 -> new A3 blind review, no upstream model rerun.

Uses the actual quantitative candidate builder, A3 hard gate and batch review.
Only the closed snapshot/original A2 output is consumed; no publication, live
collector, broker, WorkflowApplication or historical row mutation is involved.
"""
import argparse
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from run_team_a2_frozen_dryrun import source
from run_team_a5_frozen_dryrun import model_settings,save,field_diff,digest
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
TZ=ZoneInfo('Asia/Shanghai')


class ActualCaptureJournal(ModelJudgmentJournal):
    """Historical decision clock is not the real provider capture clock."""
    def capture(self,**kwargs):
        return super().capture(**{**kwargs,'captured_at':datetime.now(TZ)})


def technical_overlay(original,upstream,pit_source,day,proof):
    """Rebuild only measured stage facts from hash-checked existing PIT rows.

    A2 route qualifications are original server-canonical fields, not the new
    role's nominations or judgments. No source is upgraded to historical truth.
    """
    from liangjian_funnel.pipeline.factors import FactorEngine
    from liangjian_funnel.pipeline.technical_aggregates import build_technical_aggregates
    from liangjian_funnel.workflow import _compact_factor
    raw=(pit_source/'source.json.gz').read_bytes()
    receipt=json.loads((pit_source/'RECEIPT.json').read_bytes())
    if hashlib.sha256(raw).hexdigest()!=receipt['sha256']:raise ValueError('A3_PIT_ARCHIVE_HASH_INVALID')
    body=json.loads(gzip.decompress(raw));requests=[r for r in body['request'] if r['day']==day]
    if (len(requests)!=1 or requests[0]['as_of']!=original['as_of']
            or requests[0]['source_proofs']!=proof):raise ValueError('A3_PIT_PARENT_BINDING_INVALID')
    symbols={r['symbol'] for r in upstream['focus_pool']}
    rows=[r for r in body['rows'] if r['day']==day]
    if len(rows)!=len(symbols) or {r['symbol'] for r in rows}!=symbols:raise ValueError('A3_PIT_SCOPE_INVALID')
    cutoff=datetime.fromisoformat(original['as_of']);data=original['data']
    overlay=dict(FACTOR_SNAPSHOT={},KLINE_PATTERNS={},PRICE_LEVELS={},
        A2_BOTTLENECK_CONTEXT={r['symbol']:dict(r) for r in upstream['focus_pool']},
        A3_TECHNICAL_SCOPE=sorted(symbols),A3_TECHNICAL_READY=[])
    for row in rows:
        if datetime.fromisoformat(row['cutoff'])!=cutoff:raise ValueError('A3_PIT_CUTOFF_INVALID')
        for version in row['versions']:
            if (datetime.fromisoformat(version['fetched_at'])>cutoff
                    or datetime.fromisoformat(version['bar_timestamp'])>cutoff
                    or hashlib.sha256(version['payload_json'].encode()).hexdigest()!=version['content_hash']):
                raise ValueError('A3_PIT_VERSION_INVALID')
        factor=FactorEngine(row['symbol']).compute(daily_bars=[json.loads(v['payload_json']) for v in row['versions']],as_of=cutoff)
        aggregates=build_technical_aggregates(factor,minimum_reward_risk=data['MIN_REWARD_RISK'],max_stop_distance_pct=data['MAX_STOP_DISTANCE'])
        overlay['FACTOR_SNAPSHOT'][row['symbol']]=_compact_factor(factor.model_dump(mode='json'))
        overlay['KLINE_PATTERNS'][row['symbol']]=aggregates['KLINE_PATTERNS']
        overlay['PRICE_LEVELS'][row['symbol']]=aggregates['PRICE_LEVELS']
        if factor.a3_ready:overlay['A3_TECHNICAL_READY'].append(row['symbol'])
    return overlay,dict(archive_sha256=receipt['sha256'],source_kind=body['source'],
        cutoff=original['as_of'],original_a2_output_sha256=digest(upstream),
        overlay_sha256=digest(overlay),reconstructed_not_original_stage_snapshot=True)


def parity_issues(gate,old):
    """No empty NOOP may stand in for original nonempty blind-review coverage."""
    rows=[r for pool in ('core_watch_pool','secondary_watch_pool','rejected_candidates') for r in (old.get('output') or {}).get(pool,[])]
    expected={r['symbol']:r for r in rows}
    issues=[]
    if set(expected)!={r['symbol'] for r in gate.decisions}:issues.append('A3_ORIGINAL_GATE_DOMAIN_MISMATCH')
    for row in gate.decisions:
        prior=expected.get(row['symbol'],{})
        for key,old_key in (('status','deterministic_gate_status'),('strategy_profile','deterministic_strategy_profile'),
                ('entry_reference_zone','entry_reference_zone'),('daily_invalidation','daily_invalidation'),('no_chase_price','no_chase_price')):
            if prior.get(old_key) is not None and row.get(key)!=prior[old_key]:issues.append('A3_ORIGINAL_GATE_FIELD_MISMATCH:'+row['symbol']+':'+key)
    expected_review={r['symbol'] for r in rows if r.get('deterministic_gate_status')=='REVIEW_CANDIDATE'}
    if set(gate.review_symbols)!=expected_review:issues.append('A3_ORIGINAL_REVIEW_SCOPE_MISMATCH')
    return issues


def prepare(sources,output,day,settings,provider,pit_source):
    import ijson
    from liangjian_funnel.pipeline.research import FrozenInputSnapshot,ResearchPipeline
    from liangjian_funnel.pipeline.research.common import (_build_a3_candidate_domain,
        _with_a3_candidate_context,_with_a3_deterministic_context,screen_a3)
    from liangjian_funnel.pipeline.team_v2_wiring import configured_research_adapter
    from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
    from liangjian_funnel.runtime.team_v2_store import canonical
    original,_,a2,proof=source(sources,day)
    if not a2['status'].startswith('VALIDATED') or not a2.get('output'):
        raise ValueError('A3_DRYRUN_ORIGINAL_VALID_A2_REQUIRED')
    lane=sources/'files'/proof['lane']['relative']
    with lane.open('rb') as stream:
        original_a3=next(r for r in ijson.items(stream,'stages.item',use_float=True) if r['stage']=='A3')
    if (not original_a3['status'].startswith('VALIDATED') or not original_a3.get('output')
            or digest(original_a3['output'])!=original_a3['output_hash']
            or digest(a2['output'])!=a2['output_hash']):raise ValueError('A3_ORIGINAL_OUTPUT_HASH_INVALID')
    snapshot=FrozenInputSnapshot(original['snapshot_id'],original['data'],original['snapshot_hash'],
        datetime.fromisoformat(original['as_of']))
    upstream,origins=_build_a3_candidate_domain(a2['output'])
    overlay,pit_proof=technical_overlay(original,upstream,pit_source,day,proof)
    builder=ResearchPipeline(settings,prompt_repository=ROOT/'prompts',enable_feature_store=False,
        stage_snapshot_enricher=lambda **_:overlay)
    snapshot=builder._enrich_stage_snapshot(stage='A3',lane_id='lane_1',model=settings.review_model,
        upstream_symbols=set(origins),snapshot=snapshot)
    snapshot=_with_a3_candidate_context(snapshot,origins)
    gate=screen_a3(snapshot.data,upstream)
    issues=parity_issues(gate,original_a3)
    save(output/'SOURCE_PARITY.json',dict(status='PASS' if not issues else 'BLOCKED',issues=issues,
        original_review_count=original_a3['diagnostics']['local_screen']['sent_to_llm_count'],
        reconstructed_review_count=len(gate.review_symbols),pit_proof=pit_proof,model_calls=0))
    if issues:raise ValueError('A3_RECONSTRUCTED_SOURCE_NOT_ORIGINAL_PARITY')
    snapshot=_with_a3_deterministic_context(snapshot,gate)
    adapter=configured_research_adapter(settings,calendar=ExchangeTradingCalendar())
    adapter.journal=ActualCaptureJournal(adapter.journal.path)
    run='team-a3-frozen-dryrun:'+day+':'+output.name
    header=dict(classification='POST_HOC_INDEPENDENT_STAGE_FROZEN_DRYRUN',actual_prepared_at=datetime.now(TZ).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),run_id=run,
        source_trade_date=day,context_time=original['as_of'],source_proofs=proof,
        decision_clock='ORIGINAL_FROZEN_AS_OF',model_capture_clock='ACTUAL_ASIA_SHANGHAI',
        parent_snapshot_sha256=original['snapshot_hash'],derived_snapshot_sha256=snapshot.snapshot_hash,
        original_a2_output_sha256=a2['output_hash'],original_a3_output_sha256=original_a3.get('output_hash'),
        candidate_origins=origins,quant_review_symbols=sorted(gate.review_symbols),pit_source=pit_proof,
        quant_decisions_sha256=__import__('hashlib').sha256(canonical(gate.decisions).encode()).hexdigest(),
        evaluated_count=len(gate.decisions),provider_identity=provider,production_writes=0,customer_notifications=0)
    save(output/'PREPARED.json',header)
    return snapshot,upstream,origins,gate,adapter,original_a3,header


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--day',choices=['2026-10-14','2026-10-15','2026-10-16'],required=True)
    p.add_argument('--pit-source',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--execute-real-model',action='store_true');a=p.parse_args()
    output=a.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('DRYRUN_NEW_OWNED_DIRECTORY_REQUIRED')
    output.mkdir(parents=True)
    settings,provider=model_settings(output,'vm-runtime')
    settings=settings.model_copy(update={'source_config_path':ROOT/'config/funnel_config_v2.yaml','prompt_dir':ROOT/'prompts'})
    snapshot,upstream,origins,gate,adapter,old,header=prepare(a.sources.resolve(),output,a.day,settings,provider,a.pit_source.resolve())
    if not a.execute_real_model:
        print(json.dumps(dict(status='PREPARED_NOT_CALLED',candidates=len(origins),review=len(gate.review_symbols))));return 0
    from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient,ModelClientError
    from liangjian_funnel.pipeline.research import ResearchPipeline
    requests=[];failures=[]
    class AuditedClient(OpenAICompatibleModelClient):
        def complete(self,*args,**kwargs):
            requests.append(dict(actual_started_at=datetime.now(TZ).isoformat(),stage=kwargs.get('stage')))
            try:return super().complete(*args,**kwargs)
            except ModelClientError as exc:
                failures.append(dict(reason_code=exc.reason_code,status_code=exc.status_code,attempts=exc.attempts));raise
    pipeline=ResearchPipeline(settings,prompt_repository=ROOT/'prompts',model_client=AuditedClient(settings,max_attempts=1),
        now=lambda:snapshot.as_of,enable_feature_store=False,team_research_adapter=adapter,output_dir=output/'research')
    audit=pipeline._run_v2_downstream_review(lane_id='lane_1',model=settings.review_model,stage='A3',snapshot=snapshot,
        upstream_output=upstream,full_upstream_symbols=set(origins),gate=gate,bundle=pipeline.prompts.load(),run_id=header['run_id'])
    save(output/'STAGE_AUDIT.json',audit.as_dict())
    save(output/'FIELD_DIFF.json',dict(original_schema='PRODUCTION_A3',new_schema='agent-3/4.0.5',
        fields=field_diff(old.get('output'),audit.output)))
    def core(body):return sorted(r['symbol'] for r in (body or {}).get('core_watch_pool',[]))
    previous,current=core(old.get('output')),core(audit.output)
    valid=audit.status.startswith('VALIDATED')
    result=dict(status=audit.status,reason_codes=list(audit.reason_codes),source_head=header['head'],
        actual_finished_at=datetime.now(TZ).isoformat(),source_trade_date=a.day,model_requests=len(requests),
        requests=requests,provider_failures=failures,original_core=previous,new_core=current,
        pass_count_decline_gt20pct=(len(current)*100<len(previous)*80) if previous and valid else None,
        classification=header['classification'],original_upstream='PRODUCTION_A2_NOT_NEW_ROLE_A2',
        production_writes=0,customer_notifications=0,publication=False,natural_acceptance=False)
    save(output/'RESULT.json',result);print(json.dumps(result,ensure_ascii=True))
    return 0 if valid else 2


if __name__=='__main__':sys.exit(main())
