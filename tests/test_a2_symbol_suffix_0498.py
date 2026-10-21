from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import hashlib,json
from types import SimpleNamespace
from liangjian_funnel.pipeline.team_v2_symbol_suffix import canonicalize_a2_symbols,canonical_symbol
from liangjian_funnel.pipeline.team_v2_a2_schema import validate_a2_response
from liangjian_funnel.pipeline.team_v2_a2_recovery import validation_feedback
from liangjian_funnel.runtime.close_regeneration import require_raw_research_pass

def originals():
    return json.loads((Path(__file__).parent/'fixtures/a2_0494_symbol_suffix_originals.json').read_text(encoding='utf-8'))['rounds']

def args(row):
    return dict(candidate_ids=set(row['candidate_ids']),nomination_scope=set(row['nomination_scope']),
        direction_symbols={k:set(v) for k,v in row['direction_symbols'].items()},catalog=row['catalog'])

def validate(raw,row):
    return validate_a2_response(raw,**args(row),required_envelope=row['envelope'],
        symbol_themes=row['symbol_themes'],allowed_theme_ids=set(row['allowed_theme_ids']),
        engine_emotion_cycle=row['engine_emotion_cycle'])

def test_real_three_originals_immutable_strict_vs_suffix_only_validation():
    rounds=originals();valid=[]
    for row in rounds:
        captured=row['captured'];text=captured['output_json'];assert hashlib.sha256(text.encode()).hexdigest()==captured['output_hash']
        raw=json.loads(text);before=deepcopy(raw);assert not validate(raw,row).valid
        canonical,audit=canonicalize_a2_symbols(raw,**args(row))
        assert raw==before and all(a['rule_id']=='SYMBOL_SUFFIX_CANONICAL' for a in audit)
        assert all(r['symbol']!='603127.SZ' for r in canonical['focus_decisions'])
        assert any(a['raw']=='603127.SZ' and a['canonical']=='603127.SH' for a in audit)
        valid.append(validate(canonical,row).valid)
    # The old validator short-circuited on the bad security ID. Correcting that
    # identity exposes an existing NO_FOCUS row for a now-covered direction;
    # suffix authority must NOT silently authorize deleting that row.
    assert valid==[False,False,False]
    last=rounds[-1]
    canonical,_=canonicalize_a2_symbols(json.loads(last['captured']['output_json']),**args(last))
    assert validate(canonical,last).issues==('A2_ROTATION_DOMAIN_MISMATCH',)

def test_0501_three_originals_keep_other_errors_but_remove_only_contradictions():
    from liangjian_funnel.pipeline.team_v2_a2_recovery import drop_focused_rotation
    valid=[]
    for row in originals():
        raw=json.loads(row['captured']['output_json'])
        canonical,_=canonicalize_a2_symbols(raw,**args(row))
        projection=drop_focused_rotation(canonical,row['catalog'],args(row)['direction_symbols'])
        effective=canonical if projection is None else projection[0]
        assert effective['focus_decisions']==canonical['focus_decisions']
        if projection is not None:
            assert all(a['rule_id']=='CONTRADICTORY_NO_FOCUS_DROPPED' and a['canonical'] is None for a in projection[1]['actions'])
        valid.append(validate(effective,row).valid)
    assert valid==[False,True,True]

def test_ambiguous_or_unknown_six_digits_never_canonicalized():
    assert canonical_symbol('603127.SZ',{'603127.SH','603127.BJ'})=='603127.SZ'
    assert canonical_symbol('603127.SZ',{'600001.SH'})=='603127.SZ'
    assert canonical_symbol('603127.US',{'603127.SH'})=='603127.US'
    assert canonical_symbol('603127',{'603127.SH'})=='603127'

def test_foreign_ref_namespace_and_duplicate_securities_still_rejected():
    row=originals()[-1];raw=json.loads(row['captured']['output_json'])
    raw['focus_decisions'].append(deepcopy(raw['focus_decisions'][0]))
    raw['focus_decisions'][0]['supporting_refs']=['UNKNOWN:QUANT:603127.SZ']
    canonical,audit=canonicalize_a2_symbols(raw,**args(row))
    assert canonical['focus_decisions'][0]['supporting_refs']==['UNKNOWN:QUANT:603127.SZ']
    assert not validate(canonical,row).valid

def test_feedback_exact_field_and_no_illegal_symbol_echo_lists():
    row=originals()[-1];raw=json.loads(row['captured']['output_json']);issues=validate(raw,row).issues
    fb=validation_feedback(raw,row['catalog'],issues,envelope=row['envelope'],symbols=set(row['candidate_ids']),
        themes=set(row['allowed_theme_ids']),directions=row['direction_symbols'],nomination_scope=set(row['nomination_scope']))
    bad=[v for v in fb['violations'] if v.get('actual')=='603127.SZ']
    assert len(bad)==1 and bad[0]['field'].endswith('.symbol') and bad[0]['allowed']['values']==['603127.SH']
    assert '603127.SZ' not in fb['unchanged_focus_symbols']+fb['provider_raw_focus_symbols']
    assert '603127.SZ' not in fb['coverage_domain']['final_focus_symbols']

def test_runtime_capture_stays_strict_invalid_while_same_security_projection_passes(tmp_path):
    from test_team_v2_a2_research import fixture
    def wrong_suffix(raw,_):raw['focus_decisions'][0]['symbol']='000001.SH'
    app,kw,seen=fixture(tmp_path,wrong_suffix);result=app._run_stage(**kw)
    assert result.status=='VALIDATED' and len(seen)==1
    d=result.diagnostics;assert not d['first_round_strict_raw_pass'] and d['first_round_canonical_pass']
    assert d['provider_validation_history'] and d['provider_validation_history'][0]
    assert d['canonical_validation_history']==[[]] and d['symbol_suffix_audits'][0]['provider_capture_unchanged']
    with app.team_research_adapter.journal._connect() as db:
        raw,valid=db.execute('select output_json,validated from model_calls').fetchone()
    assert not valid and json.loads(raw)['focus_decisions'][0]['symbol']=='000001.SH'
    stage=replace(result,stage='A2');lane=SimpleNamespace(lane='lane_1',status='READY',stages=[stage])
    assert require_raw_research_pass(SimpleNamespace(status='READY',lanes=[lane]),'lane_1')
    d['canonicalization_audits'][0]['source_call_id']='UNRELATED_OLD_CALL'
    assert not require_raw_research_pass(SimpleNamespace(status='READY',lanes=[lane]),'lane_1')

def test_suffix_then_0501_contradiction_drop_strictly_validates_once(tmp_path):
    from test_team_v2_a2_research import fixture
    def wrong(raw,attempt):
        if attempt!=1:return
        raw['focus_decisions'][0]['symbol']='000001.SH'
        raw['rotation_reviews']=[dict(rotation_direction_id='D',decision='NO_FOCUS',
            reviewed_symbols=['000002.SZ'],reason_codes=['OBSERVE'],explanation='Declared test row',
            supporting_refs=['A2:QUANT:000002.SZ'],opposing_refs=['A2:NEWS'])]
    app,kw,seen=fixture(tmp_path,wrong,rotation=True)
    result=app._run_stage(**kw)
    assert result.status=='VALIDATED' and len(seen)==1
    d=result.diagnostics
    assert d['provider_validation_history'][0] and d['canonical_validation_history']==[[]]
    audit=d['canonicalization_audits'][0]
    assert {a['rule_id'] for a in audit['actions']}=={'SYMBOL_SUFFIX_CANONICAL','CONTRADICTORY_NO_FOCUS_DROPPED'}
    assert audit['canonical_raw']['focus_decisions'][0]['symbol']=='000001.SZ'
    assert audit['canonical_raw']['rotation_reviews']==[]
