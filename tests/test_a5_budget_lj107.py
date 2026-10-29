"""Original29 replay: retained facts, bounded degraded view, no model dispatch."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime
import json,hashlib
import httpx,pytest
from liangjian_funnel.pipeline.team_v2_a5_budget import priority_facts,deterministic_review,plan_fact_lines,LIMIT,PROTECTED
from liangjian_funnel.pipeline.team_v2_a5_projection import expand_review_facts
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
from liangjian_funnel.runtime.team_v2_review import run_team_review
from liangjian_funnel.pipeline.model_client import OpenAICompatibleModelClient
from liangjian_funnel.settings import Settings

O=Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/a4-monitor-20261021')
W=Path(__file__).resolve().parents[1]
def text(v):return json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'))

def test_only_historical_full_report_roots_trim_and_today_sections_unchanged():
    original=dict(metrics={'n':2},plan_causal_chains={'by_plan':{'p':{'plan':{'symbol':'000001.SZ'},'raw_text':'TODAY CRITICAL'}}},
        a6={'full_text':'TODAY ORIGINAL OPINION'},review_history=['OLD REPORT'],input_hash='source')
    before=deepcopy(original);projected,audit=priority_facts(original)
    assert original==before and 'review_history'not in projected
    assert projected['plan_causal_chains']==original['plan_causal_chains'] and projected['a6']==original['a6']
    assert audit['truncated']and audit['critical_material_removed']is False

@pytest.mark.parametrize('kind',['MIDDAY','POST_CLOSE'])
def test_today_exact_original_oversize_returns_factual_degrade_idempotently_without_wire(tmp_path,monkeypatch,kind):
    archive=json.loads((O/'C2_TRACK2_1800_ORIGINALS_RO_20261029.json').read_bytes())['result']
    row=next(x for x in archive['A5_original_contexts']if x['body']['facts']['review_kind']==kind)
    context=row['body'];facts=context['facts'];before=text(facts)
    frozen=json.loads((O/f'LJ107_FIRST_LOSSLESS_{kind}_20261029.json').read_bytes())
    # Reuse the actual prior native projection only after exact inverse proof.
    # No fake client.complete() or invented model answer is used.
    assert expand_review_facts(frozen)==facts
    from liangjian_funnel.pipeline import team_v2_a5_projection
    def verified_reuse(value):
        assert value==facts
        return frozen,dict(lossless_inverse_verified=True,source_sha256=hashlib.sha256(text(facts).encode()).hexdigest(),transport_chars=len(text(frozen)),reused_verified_original_projection=True)
    monkeypatch.setattr(team_v2_a5_projection,'project_review_facts',verified_reuse)
    seen=[]
    def deny(request):seen.append(request);raise AssertionError('OVERSIZE_MODEL_WIRE_MUST_NOT_HAPPEN')
    settings=Settings.from_env({'LIANGJIAN_MODEL_API_KEY':'CONTROLLED_NOT_REAL'},root=tmp_path)
    client=OpenAICompatibleModelClient(settings,transport=httpx.MockTransport(deny),max_attempts=1)
    store=TeamMeetingStore(tmp_path/'team.db')
    args={k:v for k,v in context.items()if k!='prompt_hashes'}
    args['counterexample_domain']=set(args['counterexample_domain'])
    kwargs=dict(trade_date='2026-10-29',review_kind=kind,clock=lambda:datetime.fromisoformat(context['required_envelope']['as_of']),
        review_id=context['run_id'],allow_model_call=True,repository=TeamPromptRepository(W/'prompts/team_v2'),client=client,
        journal=ModelJudgmentJournal(tmp_path/'model.db'),store=store)
    first=run_team_review(**args,**kwargs);second=run_team_review(**args,**kwargs)
    assert first==second and first['status']=='A5_DEGRADED_TRUNCATED_FACTS_ONLY'
    assert not seen and first['model_reviewed']is False and first['report']is None
    view=first['deterministic_review'];assert len(text(view))<=LIMIT
    assert view['metrics']==facts['metrics'] and len(view['plans'])==25 and view['approved_report']is False
    assert all(v['status']=='NOT_MODEL_REVIEWED'for v in view['conclusions'].values())
    assert text(facts)==before
    with store._connect()as db:
        original=json.loads(db.execute('SELECT body_json FROM control_minute_context WHERE decision_id=?',('a5-review:'+context['run_id'],)).fetchone()[0])
        assert original['facts']==facts
        assert db.execute('SELECT COUNT(*)FROM team_a5_reviews').fetchone()[0]==1
        card=json.loads(db.execute('SELECT body_json FROM team_internal_cards').fetchone()[0])
        assert '输入超限' in card['content']['text'] and '不视为审核通过' in card['content']['text']
        assert len(card['content']['text'].encode())<32768

def test_no_data_invents_neither_zero_performance_nor_approved_conclusion():
    f=dict(trade_date='2026-10-29',review_kind='POST_CLOSE',input_hash='original',metrics={},plan_causal_chains={})
    out=deterministic_review(f,{'request_chars':300000})
    assert not out['approved_report']and out['metrics']=={}and out['plans']==[]
    assert '缺少冻结计划原件' in plan_fact_lines(f)[0]

def test_trimmed_history_never_promotes_partial_model_report_or_hypotheses(tmp_path):
    from test_team_v2_review import fixture,hashed
    kwargs,seen=fixture(tmp_path)
    facts={k:v for k,v in kwargs['facts'].items()if k!='input_hash'}
    facts['review_history']=['old unscoped prose']
    kwargs['facts']=hashed(facts)
    out=run_team_review(**kwargs)
    assert len(seen)==1 and out['model_report_validated']is True
    assert out['status']=='A5_DEGRADED_TRUNCATED_REPORT'and out['approved_report']is False
    assert out['report']is None and out['partial_model_report']is not None
    with kwargs['store']._connect()as db:
        assert db.execute('SELECT COUNT(*)FROM team_hypothesis_queue').fetchone()[0]==0
def test_non_news_data_gap_is_not_mislabeled_as_news_failure():
    from liangjian_funnel.pipeline.team_v2_a5_budget import plan_fact_lines
    facts={'plan_causal_chains':{'by_plan':{'p':{'original_frozen_plan':{
        'symbol':'002920.SZ','source_data_limits':{'reason_codes':['HISTORY_INCOMPLETE']}}}}}}
    text='\n'.join(plan_fact_lines(facts))
    assert 'NEWS 覆盖状态待原件证明' in text and 'NEWS 受限：HISTORY' not in text
