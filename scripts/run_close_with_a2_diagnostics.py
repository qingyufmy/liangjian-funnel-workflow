"""Run same-day close recovery with auditable A2 decision projections.

Instrumentation records already-parsed model JSON and deterministic output;
it does not alter decisions, bypass validation or retain model reasoning.
"""
from datetime import datetime
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowApplication, _active_a1_downstream_scope, _A1_MAX_AGE
from liangjian_funnel.reporting import atomic_write_json
from liangjian_funnel.pipeline import research


def main():
    now=datetime.now(ZoneInfo('Asia/Shanghai'))
    app=WorkflowApplication(Settings.from_env(root=Path.cwd()))
    a1=app.a1_registry.require_active(as_of=now,max_age=_A1_MAX_AGE)
    prepared=app._load_research_resume_snapshot('close',now,candidate_symbols=_active_a1_downstream_scope(a1.payload))
    if prepared is None:
        raise RuntimeError('VERIFIED_SAME_DAY_SNAPSHOT_REQUIRED')
    rid=now.strftime('%Y-%m-%d-close-a2-audit-%H%M%S')
    directory=app.settings.workflow_output_dir/'audits'/rid
    complete=app.model_client.complete
    count=0
    def capture(*args,**kwargs):
        nonlocal count
        result=complete(*args,**kwargs)
        if kwargs.get('stage')=='A2':
            count+=1
            atomic_write_json(directory/f'model-{count}.json',{'output':result.output,'input_hash':result.input_hash,'attempts':result.attempts})
        return result
    app.model_client.complete=capture
    validate=research._validate_a2_rotation_focus_coverage
    checks=0
    def coverage(output,snapshot,upstream):
        nonlocal checks
        reasons=validate(output,snapshot,upstream)
        checks+=1
        contexts=research._lineage_context_rows(snapshot,'A2_BOTTLENECK_CONTEXT')
        fields=('theme_id','primary_theme','rotation_direction_id','trend_core_eligible','top_rotation_theme','deterministic_status','rotation_reserve_eligible','selected_board')
        atomic_write_json(directory/f'coverage-{checks}.json',{'output':output,'reasons':reasons,
            'contexts':{s:{k:contexts.get(s,{}).get(k) for k in fields} for s in sorted(upstream)}})
        print('A2_COVERAGE_AUDIT',json.dumps({'path':str(directory/f'coverage-{checks}.json'),'reasons':reasons}),flush=True)
        return reasons
    research._validate_a2_rotation_focus_coverage=coverage
    print('RUN_ID',rid,flush=True)
    result=app.run_research('close',as_of=now,primary_only=True,schedule_comparison=False,
        publish_plans=True,run_id_override=rid,reuse_resume_snapshot=True,from_active_a1=True)
    print('CLOSE_RESULT',json.dumps({k:result.get(k) for k in ('run_id','status','plan_publication')},ensure_ascii=False),flush=True)


if __name__=='__main__':
    main()
