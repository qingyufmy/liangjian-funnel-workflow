"""0319 offline original A1 dates/source families; zero providers or models."""
import argparse,hashlib,json,re,sqlite3,subprocess,sys
from collections import Counter
from contextlib import closing
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'src'),str(ROOT/'scripts')]
from run_team_a2_frozen_dryrun import source
from run_team_a5_frozen_dryrun import save
from liangjian_funnel.pipeline.a1_packet import build_a1_research_packet
from liangjian_funnel.pipeline.monthly_strategy import build_monthly_strategy_context
TZ=ZoneInfo('Asia/Shanghai')


def walk(value,path='$'):
    if isinstance(value,dict):
        yield path,value
        for key,row in value.items():yield from walk(row,path+'.'+key)
    elif isinstance(value,list):
        for i,row in enumerate(value):yield from walk(row,path+'['+str(i)+']')


def audit(snapshot,raw_response):
    at=datetime.fromisoformat(snapshot['as_of']);data=snapshot['data']
    context=build_monthly_strategy_context(data,as_of=at,prior_registry=None,policy_lookback_days=120,policy_document_limit=60)
    packet=build_a1_research_packet(data,as_of=at,snapshot_id=snapshot['snapshot_id'],snapshot_hash=snapshot['snapshot_hash'],monthly_strategy_context=context)
    from liangjian_funnel.pipeline.team_v2_a1_research import _catalog
    catalog=_catalog(dict(A1_RESEARCH_PACKET=packet))
    themes=raw_response.get('structural_themes',[]);nodes=raw_response.get('industry_chain_graph',[])
    dates=[];families=[]
    date_keys=('date','expected_date','event_time','publish_time','published_at','event_date','release_date','scheduled_at')
    family_keys=('source_family','source_type','source_category','official_category')
    for path,row in walk(packet):
        metadata={k:row[k] for k in family_keys if isinstance(row.get(k),str) and row[k].strip()}
        if metadata:families.append(dict(path=path,metadata=metadata,source_refs=row.get('source_refs'),fact_id=row.get('fact_id')))
        found={k:v for k in date_keys if isinstance((v:=row.get(k)),str) and re.search(r'20\d{2}[-年/]\d{1,2}',v)}
        if not found:continue
        # A date alone (e.g. a price series observation) is not a catalyst.
        kind=('PUBLISHED_POLICY_DOCUMENT' if path.startswith('$.policy_dossiers[') else
            'EXPLICIT_CALENDAR' if 'calendar' in path.lower() else 'DATED_OTHER_NOT_ASSUMED_SCHEDULE')
        text=' '.join(str(row.get(k,'')) for k in ('title','summary','event','industry_name','theme','name'))
        direct=[]
        for theme in themes:
            tid=theme.get('theme_id');name=theme.get('display_name','')
            linked=[n for n in nodes if tid in n.get('theme_ids',[])]
            refs=set(theme.get('source_refs',[]))|{r for n in linked for r in n.get('source_refs',[])}
            original_refs=set(row.get('source_refs',[]))|{row.get(k) for k in ('source_ref','source_url','fact_id') if isinstance(row.get(k),str)}
            matches=sorted(refs&original_refs)
            if matches or name and name in text:
                direct.append(dict(theme_id=tid,display_name=name,shared_original_refs=matches,
                    basis='ORIGINAL_MODEL_EVIDENCE_LINK_NOT_INDEPENDENT_SCHEDULE_RELEVANCE' if matches else 'EXACT_THEME_NAME_TEXT_MATCH'))
        dates.append(dict(path=path,kind=kind,dates=found,title=row.get('title'),event=row.get('event'),
            source_refs=row.get('source_refs'),fact_id=row.get('fact_id'),source_url=row.get('source_url'),
            text_replacement_character_count=text.count('\ufffd'),theme_links=direct))
    return dict(source_snapshot_id=snapshot['snapshot_id'],source_snapshot_sha256=snapshot['snapshot_hash'],
        packet_sha256=hashlib.sha256(json.dumps(packet,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        raw_source_fields={k:dict(is_none=data.get(k) is None,type=type(data.get(k)).__name__,
            available=data.get(k,{}).get('available') if isinstance(data.get(k),dict) else None)
            for k in ('MACRO_POLICY_FEED','INDUSTRY_PROFIT_DATA','INDUSTRY_ACTIVITY_DATA')},
        discovery_raw_fields_basis='INTENTIONALLY_NONE_IN_PROMPT_BY_EXISTING_COMMON_PROJECTION_PACKET_IS_SOLE_SOURCE',
        policy_window=context.get('policy_window',{}).get('full_document_count'),packet_policy_document_count=len(packet.get('policy_dossiers',[])),
        expanded_catalog=catalog,catalog_category_counts=dict(Counter(r['category'] for r in catalog['items'])),
        counts_by_kind=dict(Counter(r['kind'] for r in dates)),source_family_metadata=families,dated_records=dates,
        theme_count=len(themes),themes_with_direct_evidence_links=sorted({r['theme_id'] for d in dates for r in d['theme_links']}),
        future_scheduled_calendar_count=sum(d['kind']=='EXPLICIT_CALENDAR' and any(v[:10]>at.date().isoformat() for v in d['dates'].values()) for d in dates),
        source_replacement_character_total=sum(d['text_replacement_character_count'] for d in dates),
        limitation='SHARED_ORIGINAL_REFS_OR_NAME_MATCH_IS_NOT_PROOF_OF_A_FUTURE_CATALYST;NO_INVENTED_THEME_MAPPING')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--response-db',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    out=a.output.resolve()
    if out.exists() or not out.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):raise ValueError('NEW_OWNED_OUTPUT_REQUIRED')
    original,_,_,proof=source(a.sources.resolve(),'2026-10-15')
    with closing(sqlite3.connect(a.response_db.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        row=db.execute("SELECT output_json,output_hash FROM model_calls WHERE role='A1' AND session='POLICY_MACRO_DISCOVERY'").fetchone()
        old_catalog=json.loads(db.execute('SELECT catalog_json FROM model_call_evidence').fetchone()[0])
    if row is None:raise ValueError('ORIGINAL_A1_RESPONSE_REQUIRED')
    result=audit(original,json.loads(row[0]));old_refs={r['id'] for r in old_catalog['items']}
    new_refs={r['id'] for r in result['expanded_catalog']['items']}
    result['catalog_domain_comparison']=dict(original_ref_count=len(old_refs),expanded_ref_count=len(new_refs),
        identical_ids=old_refs==new_refs,added=sorted(new_refs-old_refs),removed=sorted(old_refs-new_refs))
    if old_refs!=new_refs:raise ValueError('A1_CATALOG_REFERENCE_DOMAIN_CHANGED')
    result.update(actual_at=datetime.now(TZ).isoformat(),
        head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),source_proofs=proof,
        response_sha256=row[1],models=0,providers=0,production_writes=0)
    out.mkdir(parents=True);save(out/'AUDIT.json',result)
    print(json.dumps(dict(path=str(out/'AUDIT.json'),sha256=hashlib.sha256((out/'AUDIT.json').read_bytes()).hexdigest(),
        raw_source_fields=result['raw_source_fields'],counts=result['counts_by_kind'],policy_documents=result['packet_policy_document_count'],
        original_family_values={k:dict(Counter(str(r['metadata'].get(k)) for r in result['source_family_metadata'] if k in r['metadata'])) for k in ('source_family','source_type','source_category','official_category')},
        linked_themes=len(result['themes_with_direct_evidence_links']),source_replacement_chars=result['source_replacement_character_total'],models=0)));return 0

if __name__=='__main__':sys.exit(main())
