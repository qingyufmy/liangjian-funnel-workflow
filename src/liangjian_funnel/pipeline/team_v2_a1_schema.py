"""Claude A1 3.0.1 additions. Existing discovery/company hard gates still apply."""
from collections.abc import Mapping
from datetime import date

from .team_v2_contract import TeamValidation, validate_team_output

_COMMON=set('envelope analysis_summary macro_regime policy_dossiers policy_calendar structural_themes '
    'industry_chain_graph taxonomy_links industry_theme_mappings source_health unresolved_questions '
    'handoff_notes what_would_change_my_mind'.split())
_COMPANY={'active_research_pool','monitor_pool','rejected_candidates','invalidated_theses'}


def _texts(value,maximum):
    return isinstance(value,list) and 1<=len(value)<=maximum and all(isinstance(s,str) and s.strip() for s in value)


def validate_a1_response(output,catalog,*,required_envelope,mode,candidate_symbols):
    if not isinstance(output,Mapping):return TeamValidation(False,('A1_RESPONSE_OBJECT_REQUIRED',),(),0)
    common=validate_team_output('A1',output,catalog);issues=list(common.issues)
    def issue(code):issues.append('A1_'+code)
    if output.get('envelope')!=required_envelope:issue('ENVELOPE_BINDING_MISMATCH')
    allowed=_COMMON|(_COMPANY if mode=='COMPANY_MAPPING' else set())
    if set(output)-allowed:issue('RESPONSE_ROOT_FIELDS_INVALID')
    if mode not in {'POLICY_MACRO_DISCOVERY','COMPANY_MAPPING'}:issue('SESSION_INVALID')
    if mode=='COMPANY_MAPPING':
        symbols=[]
        from .research.common import _first_symbol
        for pool in ('active_research_pool','monitor_pool','rejected_candidates'):
            rows=output.get(pool)
            if not isinstance(rows,list):issue('POOL_SCHEMA_INVALID');continue
            for row in rows:
                if not isinstance(row,Mapping) or not _first_symbol(row.get('symbol')):issue('POOL_SYMBOL_INVALID');continue
                symbols.append(_first_symbol(row['symbol']))
                if pool!='rejected_candidates' and not _texts(row.get('what_would_change_my_mind'),2):
                    issue('COMPANY_FALSIFIER_MISSING')
        if len(symbols)!=len(set(symbols)) or set(symbols)!=candidate_symbols:issue('CANDIDATE_DOMAIN_MISMATCH')
    for theme in output.get('structural_themes',[]) if isinstance(output.get('structural_themes'),list) else []:
        if not isinstance(theme,Mapping):continue
        if not _texts(theme.get('falsifiers'),2) or not _texts(theme.get('handoff_to_a2'),3):issue('THEME_HANDOFF_OR_FALSIFIERS_MISSING')
        catalysts=theme.get('catalysts')
        if not isinstance(catalysts,list) or not 1<=len(catalysts)<=3:issue('CATALYSTS_MISSING');continue
        refs={r['id'] for r in catalog.get('items',[]) if isinstance(r,Mapping) and isinstance(r.get('id'),str)}
        for row in catalysts:
            if (not isinstance(row,Mapping) or set(row)!= {'event','expected_date','source_ref','priced_in_assessment'}
                    or not isinstance(row['event'],str) or not row['event'].strip() or row['source_ref'] not in refs
                    or row['priced_in_assessment'] not in {'NOT_PRICED','PARTIAL','PRICED','UNKNOWN'}):
                issue('CATALYST_INVALID');continue
            value=row['expected_date']
            try:
                if value!='UNKNOWN':
                    if not isinstance(value,str) or len(value) not in {7,10}:raise ValueError
                    date.fromisoformat(value+'-01' if len(value)==7 else value)
            except (ValueError,TypeError):issue('CATALYST_DATE_INVALID')
    return TeamValidation(not issues,tuple(dict.fromkeys(issues)),common.judgments,common.sentinel_count)
