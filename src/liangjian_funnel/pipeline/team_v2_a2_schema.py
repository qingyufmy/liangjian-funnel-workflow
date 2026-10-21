"""Claude A2 3.0.2 response schema, separate from engine eligibility.

Caller supplies the frozen domains; no domain is inferred from model claims.
An invalid response is returned intact to the existing degradation policy.
This module performs no scoring, data access, plan publication or repair.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
import json
from typing import Any

from .team_v2_contract import TeamValidation, validate_team_output

from .emotion_vocabulary import POLICIES,STAGES
_POLICIES = dict(POLICIES)
_ENGINE_EMOTION_STAGES = STAGES
_ROOT = frozenset(("envelope", "reason_codes", "market_read", "theme_reviews", "focus_decisions",
                   "rotation_reviews", "reject_decisions", "model_nominations", "handoff_notes",
                   "what_would_change_my_mind", "unresolved_questions"))
_EVIDENCE = frozenset(("supporting_refs", "opposing_refs"))
_OPTIONAL = frozenset(("opposing_search_scope",))
_FIELDS = {
    "market_read": frozenset(("regime", "dominant_themes", "emotion_cycle")) | _EVIDENCE,
    "theme_reviews": frozenset(("theme_id", "stage", "new_entry_policy", "penalty_codes",
        "support_codes", "risk_codes", "what_would_change_my_mind")) | _EVIDENCE,
    "focus_decisions": frozenset(("symbol", "support_codes", "risk_codes")) | _EVIDENCE,
    "rotation_reviews": frozenset(("rotation_direction_id", "decision", "reviewed_symbols",
        "reason_codes", "explanation")) | _EVIDENCE,
    "reject_decisions": frozenset(("symbol", "risk_codes")) | _EVIDENCE,
    "model_nominations": frozenset(("symbol", "theme_id", "nomination_thesis", "quant_filter_reason",
        "why_i_disagree", "falsifiers")) | _EVIDENCE,
}


def a2_output_contract(*,required_envelope,candidate_ids,nomination_scope,symbol_themes,
                       allowed_theme_ids,direction_symbols,catalog,engine_emotion_cycle=None,
                       penalty_codes=None,nomination_facts=None):
    """One closed-domain projection for first user, validator and correction.

    The input is frozen server authority. Open judgment strings stay open;
    this contract creates no source fact, selection or trade permission.
    """
    from .team_v2_a2_score import POLICY
    from .team_v2_rotation_scope import rotation_output_contract,rotation_reason_contract
    penalties=sorted(POLICY['penalty_points'] if penalty_codes is None else penalty_codes)
    nominations={r['symbol']:{k:r[k] for k in ('theme_id','quant_filter_reason')}
        for r in nomination_facts or ()}
    refs=sorted(r['id'] for r in catalog['items'])
    return dict(**rotation_output_contract(direction_symbols),contract_version='A2_OUTPUT_CLOSED_DOMAINS_0515_V1',
        authority='FROZEN_VALIDATOR_DOMAIN_NOT_MODEL_SELECTION_OR_PERMISSION',
        required_root_fields=sorted(_ROOT),required_item_fields={k:sorted(v) for k,v in _FIELDS.items()},
        optional_item_fields=sorted(_OPTIONAL),required_envelope=deepcopy(dict(required_envelope)),
        domains=dict(candidate_ids=sorted(candidate_ids),nomination_scope=sorted(set(nomination_scope)-set(candidate_ids)),
            symbol_themes=dict(symbol_themes),theme_ids=sorted(allowed_theme_ids),
            stage_policy=dict(_POLICIES),penalty_codes=penalties,nomination_facts=nominations,
            evidence_reference_ids=refs,evidence_categories=sorted(catalog.get('categories',[]))),
        field_contracts={
            'envelope':dict(rule='EXACT_COPY_ALL_FIELDS',value=deepcopy(dict(required_envelope))),
            'reason_codes':dict(minItems=0,maxItems=4,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'market_read.regime':dict(values=[required_envelope['market_regime']] if 'market_regime' in required_envelope else None),
            'market_read.emotion_cycle':dict(values=list(_POLICIES),engine_fact=engine_emotion_cycle,
                rule='Ten canonical stages only; STARTUP is historical read alias, never a new output.'),
            'market_read.dominant_themes':dict(domain='theme_ids',minItems=0,maxItems=3,uniqueItems=True),
            'theme_reviews[*].theme_id':dict(domain='theme_ids',coverage='EACH_EXACTLY_ONCE'),
            'theme_reviews[*].stage':dict(domain='stage_policy.keys'),
            'theme_reviews[*].new_entry_policy':dict(rule='EXACT_POLICY_FOR_THIS_ROW_STAGE',values=dict(_POLICIES)),
            'theme_reviews[*].support_codes':dict(minItems=0,maxItems=2,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'theme_reviews[*].risk_codes':dict(minItems=0,maxItems=2,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'theme_reviews[*].what_would_change_my_mind':dict(minItems=1,maxItems=2,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'theme_reviews[*].penalty_codes':dict(values=penalties,minItems=0,maxItems=3,uniqueItems=True,
                case_sensitive=True,rule='Only six exact lower-case score keys. Missing capital-flow facts are not a penalty code; never move a risk/support code here or auto-rename unknown codes.'),
            'focus_decisions[*].symbol':dict(domain='candidate_ids',unique_across=['focus_decisions','reject_decisions','model_nominations']),
            'focus_decisions[*].support_codes':dict(minItems=0,maxItems=2,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'focus_decisions[*].risk_codes':dict(minItems=0,maxItems=2,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'reject_decisions[*].symbol':dict(domain='candidate_ids'),
            'reject_decisions[*].risk_codes':dict(minItems=1,maxItems=3,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'model_nominations':dict(maxItems=5,scope='RESEARCH_ONLY_NOT_A3_EXECUTION_EXPANSION'),
            'model_nominations[*].symbol':dict(domain='nomination_scope'),
            'model_nominations[*].theme_id':dict(rule='EXACT_FROZEN_NOMINATION_THEME_BY_SYMBOL',domain='nomination_facts'),
            'model_nominations[*].quant_filter_reason':dict(rule='EXACT_FROZEN_NOMINATION_FILTER_REASON_BY_SYMBOL',domain='nomination_facts'),
            'model_nominations[*].nomination_thesis':dict(type='nonempty_string',positive_enum=None),
            'model_nominations[*].why_i_disagree':dict(type='nonempty_string',positive_enum=None),
            'model_nominations[*].falsifiers':dict(minItems=1,maxItems=2,uniqueItems=True,items='nonempty_string',positive_enum=None),
            'model_nominations[*].supporting_refs':dict(required='A2:NOMINATION:<same exact symbol>',domain='evidence_reference_ids'),
            'rotation_reviews[*].rotation_direction_id':dict(domain='allowed_direction_members.keys',rule='CONDITIONAL_RAW_FOCUS_MEMBERSHIP_COVERAGE'),
            'rotation_reviews[*].decision':dict(values=['NO_FOCUS']),
            'rotation_reviews[*].reviewed_symbols':dict(minItems=1,maxItems=3,uniqueItems=True,domain='same_direction_members'),
            'rotation_reviews[*].reason_codes':next(iter(rotation_reason_contract({'CONTRACT':[]}).values())),
            'rotation_reviews[*].explanation':dict(type='string',minLength=10,strip_before_length=True),
            '*.supporting_refs':dict(domain='evidence_reference_ids',minItems=1,maxItems=3,uniqueItems=True,sentinel_forbidden=True),
            '*.opposing_refs':dict(domain='evidence_reference_ids',minItems=1,maxItems=3,uniqueItems=True,
                exclusive_sentinel='NO_OPPOSING_EVIDENCE_FOUND'),
            '*.opposing_search_scope':dict(required_when='opposing_refs is ONLY sentinel',domain='evidence_categories',minItems=2,uniqueItems=True),
            'open_judgment_strings':dict(positive_enum=None,rule='reason_codes/support_codes/risk_codes are not penalty enums.',
                lengths={'reason_codes':[0,4],'theme_reviews.support_codes':[0,2],'theme_reviews.risk_codes':[0,2],
                    'focus_decisions.support_codes':[0,2],'focus_decisions.risk_codes':[0,2],'reject_decisions.risk_codes':[1,3]}),
            'handoff_notes':dict(required_keys=['to_a3'],text_list_minItems=1,text_list_maxItems=3,max_text_length=2000),
            'what_would_change_my_mind':dict(minItems=1,maxItems=2,max_text_length=2000),
            'unresolved_questions':dict(type='list',items='nonempty_string',positive_enum=None)})


def a2_field_violations(output,contract,*,effective_focus_symbols=None):
    """Concrete original path/actual/legal values; never repairs provider JSON."""
    result=[];domains=contract['domains'];laws=contract['field_contracts']
    def add(code,field,actual,allowed):
        result.append(dict(code=code,field=field,actual=deepcopy(actual),allowed=deepcopy(allowed)))
    def strings(value,low,high):return _strings(value,high,low)
    def texts(value,low,high):return (isinstance(value,list) and low<=len(value)<=high
        and all(_nonempty(s) and len(s)<=2000 for s in value))
    if not isinstance(output,Mapping):
        add('A2_RESPONSE_OBJECT_REQUIRED','$',output,dict(type='object'));return result
    if set(output)!=_ROOT:add('A2_RESPONSE_ROOT_FIELDS_INVALID','$',sorted(map(str,output)),contract['required_root_fields'])
    envelope=output.get('envelope');required=contract['required_envelope']
    if not _same_json(envelope,required):
        if not isinstance(envelope,Mapping):add('A2_ENVELOPE_BINDING_MISMATCH','envelope',envelope,required)
        else:
            for key in sorted(set(envelope)|set(required)):
                if key not in envelope or key not in required or not _same_json(envelope.get(key),required.get(key)):
                    add('A2_ENVELOPE_BINDING_MISMATCH','envelope.'+key,envelope.get(key),dict(exact=required.get(key)))
    if not strings(output.get('reason_codes'),0,4):add('A2_REASON_CODES_INVALID','reason_codes',output.get('reason_codes'),dict(minItems=0,maxItems=4,items='nonempty_string',positive_enum=None))
    notes=output.get('handoff_notes')
    if not isinstance(notes,Mapping) or set(notes)!={'to_a3'}:add('A2_HANDOFF_RECIPIENT_INVALID','handoff_notes',notes,laws['handoff_notes'])
    elif not texts(notes['to_a3'],1,3):add('TEAM_HANDOFF_NOTES_INVALID','handoff_notes.to_a3',notes['to_a3'],laws['handoff_notes'])
    if not texts(output.get('what_would_change_my_mind'),1,2):add('TEAM_FALSIFIERS_INVALID','what_would_change_my_mind',output.get('what_would_change_my_mind'),laws['what_would_change_my_mind'])
    unresolved=output.get('unresolved_questions')
    if not isinstance(unresolved,list) or any(not _nonempty(s) for s in unresolved):add('A2_UNRESOLVED_QUESTIONS_INVALID','unresolved_questions',unresolved,laws['unresolved_questions'])
    seen_symbols={};seen_themes=[];rotation_rows=[]
    refids=set(domains['evidence_reference_ids']);categories=set(domains['evidence_categories'])
    for pool,fields in _FIELDS.items():
        rows=[output.get(pool)] if pool=='market_read' else output.get(pool)
        if not isinstance(rows,list):add('A2_POOL_INVALID:'+pool,pool,rows,dict(type='list'));continue
        if pool=='model_nominations' and len(rows)>5:add('TEAM_NOMINATION_DOMAIN_INVALID',pool,rows,laws['model_nominations'])
        for index,row in enumerate(rows):
            path=pool if pool=='market_read' else f'{pool}[{index}]'
            if not isinstance(row,Mapping) or not fields.issubset(row) or not set(row).issubset(fields|_OPTIONAL):
                add('A2_ITEM_FIELDS_INVALID:'+pool,path,row,dict(required=sorted(fields),optional=sorted(_OPTIONAL)));continue
            if pool=='market_read':
                if 'market_regime' in required and row['regime']!=required['market_regime']:add('A2_MARKET_REGIME_BINDING_MISMATCH',path+'.regime',row['regime'],laws['market_read.regime'])
                if not isinstance(row['emotion_cycle'],str) or row['emotion_cycle'] not in _POLICIES:add('A2_MARKET_READ_INVALID',path+'.emotion_cycle',row['emotion_cycle'],laws['market_read.emotion_cycle'])
                dominant=row['dominant_themes']
                if not strings(dominant,0,3):add('A2_MARKET_READ_INVALID',path+'.dominant_themes',dominant,laws['market_read.dominant_themes'])
                else:
                    for i,theme in enumerate(dominant):
                        if theme not in domains['theme_ids']:add('A2_THEME_OUTSIDE_FROZEN_DOMAIN',f'{path}.dominant_themes[{i}]',theme,dict(values=domains['theme_ids']))
            if pool=='theme_reviews':
                theme=row['theme_id']
                if not isinstance(theme,str) or theme not in domains['theme_ids']:add('A2_THEME_OUTSIDE_FROZEN_DOMAIN',path+'.theme_id',theme,dict(values=domains['theme_ids']))
                else:seen_themes.append(theme)
                stage=row['stage']
                if not isinstance(stage,str) or stage not in _POLICIES:add('A2_STAGE_POLICY_MISMATCH',path+'.stage',stage,dict(values=list(_POLICIES)))
                elif row['new_entry_policy']!=_POLICIES[stage]:add('A2_STAGE_POLICY_MISMATCH',path+'.new_entry_policy',row['new_entry_policy'],dict(values=[_POLICIES[stage]],stage=stage))
                penalties=row['penalty_codes']
                if not strings(penalties,0,3):add('A2_THEME_FIELDS_INVALID',path+'.penalty_codes',penalties,laws['theme_reviews[*].penalty_codes'])
                elif isinstance(penalties,list):
                    for i,code in enumerate(penalties):
                        if code not in domains['penalty_codes']:add('A2_SCORE_PENALTY_CODE_UNKNOWN',f'{path}.penalty_codes[{i}]',code,laws['theme_reviews[*].penalty_codes'])
                for key in ('support_codes','risk_codes'):
                    if not strings(row[key],0,2):add('A2_THEME_FIELDS_INVALID',path+'.'+key,row[key],dict(minItems=0,maxItems=2,positive_enum=None,items='nonempty_string'))
                if not strings(row['what_would_change_my_mind'],1,2):add('A2_THEME_FIELDS_INVALID',path+'.what_would_change_my_mind',row['what_would_change_my_mind'],dict(minItems=1,maxItems=2,items='nonempty_string'))
            if pool in ('focus_decisions','reject_decisions','model_nominations'):
                symbol=row['symbol'];allowed=domains['nomination_scope'] if pool=='model_nominations' else domains['candidate_ids']
                if not isinstance(symbol,str) or symbol not in allowed:
                    from .team_v2_symbol_suffix import canonical_symbol
                    suggestion=canonical_symbol(symbol,set(domains['candidate_ids'])|set(domains['nomination_scope']))
                    legal=[suggestion] if suggestion!=symbol and suggestion in allowed else allowed
                    add('A2_SYMBOL_OUTSIDE_FROZEN_DOMAIN:'+pool,path+'.symbol',symbol,dict(values=legal,case_sensitive=True))
                elif symbol in seen_symbols:add('A2_DUPLICATE_SYMBOL_DECISION',path+'.symbol',symbol,dict(unique_across=['focus_decisions','reject_decisions','model_nominations'],first_path=seen_symbols[symbol]))
                else:seen_symbols[symbol]=path
                limits={'focus_decisions':(('support_codes',0,2),('risk_codes',0,2)),
                    'reject_decisions':(('risk_codes',1,3),),'model_nominations':(('falsifiers',1,2),)}[pool]
                for key,low,high in limits:
                    if not strings(row[key],low,high):add({'focus_decisions':'A2_FOCUS_CODES_INVALID','reject_decisions':'A2_REJECT_CODES_INVALID','model_nominations':'A2_NOMINATION_FIELDS_INVALID'}[pool],path+'.'+key,row[key],dict(minItems=low,maxItems=high,positive_enum=None,items='nonempty_string'))
                if pool=='model_nominations':
                    for key in ('theme_id','nomination_thesis','quant_filter_reason','why_i_disagree'):
                        if not _nonempty(row[key]):add('A2_NOMINATION_FIELDS_INVALID',path+'.'+key,row[key],dict(type='nonempty_string'))
                    source=domains['nomination_facts'].get(symbol) if isinstance(symbol,str) else None
                    if source:
                        for key,code in (('theme_id','A2_NOMINATION_THEME_BINDING_MISMATCH'),('quant_filter_reason','A2_NOMINATION_FILTER_REASON_MISMATCH')):
                            if row[key]!=source[key]:add(code,path+'.'+key,row[key],dict(exact=source[key]))
                        if not isinstance(row['supporting_refs'],list) or 'A2:NOMINATION:'+symbol not in row['supporting_refs']:add('A2_NOMINATION_ORIGINAL_FACT_REFERENCE_REQUIRED',path+'.supporting_refs',row['supporting_refs'],dict(required='A2:NOMINATION:'+symbol,values=domains['evidence_reference_ids']))
            if pool=='rotation_reviews':rotation_rows.append((index,row))
            for key,code in (('supporting_refs','TEAM_SUPPORTING_REFS_INVALID'),('opposing_refs','TEAM_OPPOSING_REFS_INVALID')):
                refs=row[key]
                if key=='opposing_refs' and refs==['NO_OPPOSING_EVIDENCE_FOUND']:
                    scope=row.get('opposing_search_scope')
                    if not strings(scope,2,len(categories)) or not set(scope).issubset(categories):add('TEAM_OPPOSING_SEARCH_SCOPE_INVALID:'+path,path+'.opposing_search_scope',scope,dict(minItems=2,values=domains['evidence_categories'],uniqueItems=True))
                    continue
                if not strings(refs,1,3):add(code+':'+path,path+'.'+key,refs,laws['*.'+key]);continue
                for i,ref in enumerate(refs):
                    if ref not in refids or ref=='NO_OPPOSING_EVIDENCE_FOUND':add(code+':'+path,f'{path}.{key}[{i}]',ref,dict(values=domains['evidence_reference_ids'],sentinel_forbidden=key=='supporting_refs'))
    if len(seen_themes)!=len(set(seen_themes)) or set(seen_themes)!=set(domains['theme_ids']):add('A2_FOCUS_THEME_COVERAGE_MISMATCH','theme_reviews',seen_themes,dict(each_exactly_once=domains['theme_ids']))
    from .team_v2_rotation_scope import rotation_coverage,ENTRY_GATE_ONLY_REASON_CODES
    focus={r.get('symbol') for r in output.get('focus_decisions',[]) if isinstance(r,Mapping) and isinstance(r.get('symbol'),str)} if isinstance(output.get('focus_decisions'),list) else set()
    expected=rotation_coverage(focus if effective_focus_symbols is None else effective_focus_symbols,contract['allowed_direction_members'])['required_no_focus'];seen=[]
    for index,row in rotation_rows:
        path=f'rotation_reviews[{index}]';direction=row['rotation_direction_id']
        if not isinstance(direction,str) or direction not in expected:add('A2_ROTATION_DOMAIN_MISMATCH',path+'.rotation_direction_id',direction,dict(values=sorted(expected)));continue
        seen.append(direction)
        if row['decision']!='NO_FOCUS':add('A2_ROTATION_REVIEW_INVALID',path+'.decision',row['decision'],dict(values=['NO_FOCUS']))
        members=row['reviewed_symbols']
        if not strings(members,1,3):add('A2_ROTATION_REVIEW_INVALID',path+'.reviewed_symbols',members,dict(minItems=1,maxItems=3,values=expected[direction],uniqueItems=True))
        else:
            for i,symbol in enumerate(members):
                if symbol not in expected[direction]:add('A2_ROTATION_REVIEW_INVALID',f'{path}.reviewed_symbols[{i}]',symbol,dict(values=expected[direction]))
        reasons=row['reason_codes']
        if not strings(reasons,1,4):add('A2_ROTATION_REASON_CODES_INVALID:'+direction,path+'.reason_codes',reasons,laws['rotation_reviews[*].reason_codes'])
        elif all(code.strip().upper() in ENTRY_GATE_ONLY_REASON_CODES for code in reasons):add('A2_ROTATION_REASON_CODES_ENTRY_GATE_ONLY:'+direction,path+'.reason_codes',reasons,laws['rotation_reviews[*].reason_codes'])
        explanation=row['explanation']
        if not isinstance(explanation,str) or len(explanation.strip())<10:add('A2_ROTATION_REVIEW_INVALID',path+'.explanation',explanation,laws['rotation_reviews[*].explanation'])
    if len(seen)!=len(set(seen)) or set(seen)!=set(expected):add('A2_ROTATION_COVERAGE_MISMATCH','rotation_reviews',[r.get('rotation_direction_id') for _,r in rotation_rows],dict(each_exactly_once=sorted(expected)))
    return result


def frozen_domain_diagnostics(output, *, candidate_ids, allowed_theme_ids, direction_symbols):
    """Name original offending values without replacing or accepting any ID."""
    if not isinstance(output,Mapping):return {'status':'RAW_NOT_OBJECT'}
    def rows(name):
        value=output.get(name)
        return [r for r in value if isinstance(r,Mapping)] if isinstance(value,list) else []
    focused={r['symbol'] for r in rows('focus_decisions') if isinstance(r.get('symbol'),str)}
    expected={key for key,values in direction_symbols.items() if not focused.intersection(values)}
    outside_symbols=[dict(pool=pool,symbol=r.get('symbol')) for pool in ('focus_decisions','reject_decisions')
        for r in rows(pool) if not isinstance(r.get('symbol'),str) or r['symbol'] not in candidate_ids]
    themes=[r.get('theme_id') for r in rows('theme_reviews')]
    market=output.get('market_read')
    if isinstance(market,Mapping) and isinstance(market.get('dominant_themes'),list):themes+=market['dominant_themes']
    return dict(outside_symbols=outside_symbols,
        outside_themes=[t for t in themes if not isinstance(t,str) or t not in allowed_theme_ids],
        expected_no_focus_directions=sorted(expected),
        invalid_direction_reviews=[dict(direction=r.get('rotation_direction_id'),reviewed_symbols=r.get('reviewed_symbols'))
            for r in rows('rotation_reviews') if not isinstance(r.get('rotation_direction_id'),str)
            or r['rotation_direction_id'] not in expected
            or not isinstance(r.get('reviewed_symbols'),list)
            or any(not isinstance(s,str) or s not in direction_symbols.get(r['rotation_direction_id'],set()) for s in r['reviewed_symbols'])])


def _strings(value: Any, maximum: int, minimum: int = 0) -> bool:
    return (isinstance(value, list) and minimum <= len(value) <= maximum
            and all(isinstance(item, str) and item.strip() for item in value)
            and len(value) == len(set(value)))


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _same_json(left: Any, right: Any) -> bool:
    try:
        return json.dumps(left, sort_keys=True, allow_nan=False, ensure_ascii=False) == json.dumps(
            right, sort_keys=True, allow_nan=False, ensure_ascii=False)
    except (TypeError, ValueError):
        return False


def validate_a2_response(output: Any, catalog: Any, *, required_envelope: Mapping[str, Any],
                         candidate_ids: set[str], nomination_scope: set[str],
                         symbol_themes: Mapping[str, str], allowed_theme_ids: set[str],
                         direction_symbols: Mapping[str, set[str]],
                         engine_emotion_cycle: str | None = None,
                         effective_focus_symbols: set[str] | None = None,
                         penalty_codes=None,nomination_facts=None) -> TeamValidation:
    issues: list[str] = []
    if not isinstance(output, Mapping):
        return TeamValidation(False, ("A2_RESPONSE_OBJECT_REQUIRED",), (), 0)
    if set(output) != _ROOT:
        issues.append("A2_RESPONSE_ROOT_FIELDS_INVALID")
    if not _same_json(output.get("envelope"), required_envelope):
        issues.append("A2_ENVELOPE_BINDING_MISMATCH")
    if not _strings(output.get("reason_codes"), 4):
        issues.append("A2_REASON_CODES_INVALID")
    if not isinstance(output.get("unresolved_questions"), list) or any(
            not _nonempty(item) for item in output["unresolved_questions"]):
        issues.append("A2_UNRESOLVED_QUESTIONS_INVALID")
    notes = output.get("handoff_notes")
    if not isinstance(notes, Mapping) or set(notes) != {"to_a3"}:
        issues.append("A2_HANDOFF_RECIPIENT_INVALID")

    # Structural checks precede domain operations so malformed JSON never throws.
    pools: dict[str, list[Mapping[str, Any]]] = {}
    for name, fields in _FIELDS.items():
        rows = [output.get(name)] if name == "market_read" else output.get(name)
        if not isinstance(rows, list):
            issues.append("A2_POOL_INVALID:" + name); pools[name] = []; continue
        pools[name] = []
        for row in rows:
            if (not isinstance(row, Mapping) or not fields.issubset(row)
                    or not set(row).issubset(fields | _OPTIONAL)):
                issues.append("A2_ITEM_FIELDS_INVALID:" + name); continue
            pools[name].append(row)

    for row in pools["market_read"]:
        from .emotion_vocabulary import read_emotion_stage
        try:
            canonical_engine=read_emotion_stage(engine_emotion_cycle,
                source_trade_date=required_envelope.get('source_trade_date'))
        except (ValueError,TypeError):canonical_engine=None
        engine_match = canonical_engine is not None and row["emotion_cycle"] == canonical_engine
        if (row["emotion_cycle"] not in tuple(_POLICIES) and not engine_match
                or not _strings(row["dominant_themes"], 3)):
            issues.append("A2_MARKET_READ_INVALID")
    focused: list[str] = []; rejected: list[str] = []; nominated: list[str] = []
    for name, ids in (("focus_decisions", focused), ("reject_decisions", rejected),
                      ("model_nominations", nominated)):
        for row in pools[name]:
            symbol = row["symbol"]
            domain = nomination_scope - candidate_ids if name == "model_nominations" else candidate_ids
            if not isinstance(symbol, str) or symbol not in domain:
                issues.append("A2_SYMBOL_OUTSIDE_FROZEN_DOMAIN:" + name)
            else:
                ids.append(symbol)
            if name == "focus_decisions":
                if not _strings(row["support_codes"], 2) or not _strings(row["risk_codes"], 2):
                    issues.append("A2_FOCUS_CODES_INVALID")
            elif name == "reject_decisions":
                if not _strings(row["risk_codes"], 3, 1): issues.append("A2_REJECT_CODES_INVALID")
            elif (not all(_nonempty(row[key]) for key in ("theme_id", "nomination_thesis",
                      "quant_filter_reason", "why_i_disagree")) or not _strings(row["falsifiers"], 2, 1)):
                issues.append("A2_NOMINATION_FIELDS_INVALID")
    all_ids = focused + rejected + nominated
    if len(all_ids) != len(set(all_ids)):
        issues.append("A2_DUPLICATE_SYMBOL_DECISION")

    theme_ids: list[str] = []
    for row in pools["theme_reviews"]:
        theme = row["theme_id"]
        if not isinstance(theme, str) or theme not in allowed_theme_ids:
            issues.append("A2_THEME_OUTSIDE_FROZEN_DOMAIN")
        else:
            theme_ids.append(theme)
        stage = row["stage"]
        if not isinstance(stage, str) or _POLICIES.get(stage) != row["new_entry_policy"]:
            issues.append("A2_STAGE_POLICY_MISMATCH")
        if (not _strings(row["penalty_codes"], 3) or not _strings(row["support_codes"], 2)
                or not _strings(row["risk_codes"], 2)
                or not _strings(row["what_would_change_my_mind"], 2, 1)):
            issues.append("A2_THEME_FIELDS_INVALID")
    focus_themes = {symbol_themes[symbol] for symbol in focused if symbol in symbol_themes}
    if (any(symbol not in symbol_themes for symbol in focused)
            or len(theme_ids) != len(set(theme_ids)) or set(theme_ids) != allowed_theme_ids
            or not focus_themes.issubset(allowed_theme_ids)):
        issues.append("A2_FOCUS_THEME_COVERAGE_MISMATCH")

    from .team_v2_rotation_scope import rotation_coverage,rotation_reason_issues
    coverage=rotation_coverage(set(focused) if effective_focus_symbols is None else effective_focus_symbols,
        direction_symbols)
    expected=set(coverage['required_no_focus'])
    reviewed: list[str] = []
    for row in pools["rotation_reviews"]:
        direction = row["rotation_direction_id"]
        if not isinstance(direction, str) or direction not in expected:
            issues.append("A2_ROTATION_DOMAIN_MISMATCH"); continue
        reviewed.append(direction)
        issues.extend(rotation_reason_issues(direction,row['reason_codes'],direction_symbols))
        symbols = row["reviewed_symbols"]
        if (row["decision"] != "NO_FOCUS" or not _strings(symbols, 3, 1)
                or not set(symbols).issubset(direction_symbols[direction])
                or not _strings(row["reason_codes"], 4) or not _nonempty(row["explanation"])):
            issues.append("A2_ROTATION_REVIEW_INVALID")
    if len(reviewed) != len(set(reviewed)) or set(reviewed) != expected:
        issues.append("A2_ROTATION_COVERAGE_MISMATCH")
    contract=a2_output_contract(required_envelope=required_envelope,candidate_ids=candidate_ids,
        nomination_scope=nomination_scope,symbol_themes=symbol_themes,allowed_theme_ids=allowed_theme_ids,
        direction_symbols=direction_symbols,catalog=catalog,engine_emotion_cycle=engine_emotion_cycle,
        penalty_codes=penalty_codes,nomination_facts=nomination_facts)
    issues.extend(v['code'] for v in a2_field_violations(output,contract,effective_focus_symbols=effective_focus_symbols))
    common = validate_team_output("A2", output, catalog, nomination_scope=nomination_scope)
    issues.extend(common.issues)
    return TeamValidation(not issues, tuple(dict.fromkeys(issues)), common.judgments, common.sentinel_count)
