"""Offline author-review proposal only. Never wired to a model or runtime.

Reuse the existing approved meeting-plan fact projection; retain complete bars
and technical contexts. Keep original full publication payloads in the sealed
source, not repeated as multi-megabyte evidence catalog values.
"""
import argparse
from contextlib import closing
from copy import deepcopy
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'src'))
from liangjian_funnel.pipeline.team_v2_prompts import TeamPromptRepository
from liangjian_funnel.runtime.team_v2_sources import meeting_plan_facts
from profile_team_a4_frozen_input import describe
from run_team_a5_frozen_dryrun import save


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();output=args.output.resolve()
    if output.exists() or not output.is_relative_to((ROOT/'artifacts/team-v2-20261016').resolve()):
        raise ValueError('PROPOSAL_NEW_OWNED_DIRECTORY_REQUIRED')
    source=json.loads((args.profile/'PROFILE.json').read_bytes())
    original=gzip.decompress((args.profile/'ORIGINAL_RENDERED_INPUT.json.gz').read_bytes())
    if hashlib.sha256(original).hexdigest()!=source['original_rendered_input_sha256']:
        raise ValueError('PROPOSAL_ORIGINAL_RENDERED_INPUT_HASH_MISMATCH')
    original_values=json.loads(original)['values'];values=deepcopy(original_values)
    database=Path(json.loads((ROOT/'artifacts/team-v2-20261016/correctness-source-20261017T0909/PREPARATION.json').read_bytes())['source_path'])
    with database.open('rb') as stream:
        if hashlib.file_digest(stream,'sha256').hexdigest()!=source['source']['source_database_sha256']:
            raise ValueError('PROPOSAL_ORIGINAL_DATABASE_HASH_MISMATCH')
    ids=[row['plan_id'] for row in values['ACTIVE_PLANS']]
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row;db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        rows=[dict(db.execute('SELECT * FROM execution_plans WHERE plan_id=?',(pid,)).fetchone()) for pid in ids]
        db.rollback()
    facts={row['plan_id']:row for row in meeting_plan_facts(rows)}
    items=values['EVIDENCE_CATALOG']['items'];tech={item['id'].removeprefix('REPLAY:TECH:'):item['value']
        for item in items if item['category']=='STOCK_TECH'}
    if set(tech)!=set(ids):raise ValueError('PROPOSAL_EXACT_TECH_PLAN_SCOPE_REQUIRED')
    original_catalog=deepcopy(values['EVIDENCE_CATALOG'])
    values['BARS']=tech
    values['TRADABILITY']={pid:dict(input_ref=['BARS',pid,'original_tradability_contract'],
        original_value_sha256=hashlib.sha256(json.dumps(tech[pid]['original_tradability_contract'],sort_keys=True,separators=(',',':')).encode()).hexdigest())
        for pid in ids}
    for index,row in enumerate(values['ACTIVE_PLANS']):
        fact=facts[row['plan_id']]
        row.update(strategy_profile=fact['publication_facts'].get('strategy_profile'),
            publication_facts=fact['publication_facts'],missing_fields=fact['missing_fields'],
            source_row_sha256=fact['source_row_sha256'],original_payload_sha256=fact['original_payload_sha256'])
    positions={pid:index for index,pid in enumerate(ids)}
    field_manifest=[]
    for item in items:
        if item['category']=='PLAN':
            pid=item['id'].removeprefix('REPLAY:PLAN:')
            raw=item['value'];publication=facts[pid]['publication_facts']
            if any(raw.get(key)!=value for key,value in publication.items()):
                raise ValueError('PROPOSAL_PLAN_PROJECTION_CHANGED_ORIGINAL_VALUE')
            field_manifest.append(dict(plan_id=pid,retained_publication_fields=sorted(publication),
                excluded_original_fields=sorted(set(raw)-set(publication)),
                original_payload_sha256=facts[pid]['original_payload_sha256']))
            item['value']=dict(input_ref=['ACTIVE_PLANS',positions[pid]],source_row_sha256=facts[pid]['source_row_sha256'])
        elif item['category']=='STOCK_TECH':
            pid=item['id'].removeprefix('REPLAY:TECH:');item['value']=dict(input_ref=['BARS',pid])
    # Verify complete original tech, reference IDs and server risk inputs; no
    # bar count/precision or engine permissions changed in this proposed layout.
    assert values['BARS']==tech
    assert [(item['id'],item['category']) for item in items]==[(item['id'],item['category']) for item in original_catalog['items']]
    for key in ('RISK_ENVELOPES','TECH_REFERENCE','POSITIONS','TIME_RULES','LIVE_MARKET_STATE'):
        assert values[key]==original_values[key]
    repository=TeamPromptRepository(ROOT/'prompts/team_v2')
    request=repository.prepare_messages('A4',values,required_envelope=source['source']['required_envelope'],session='INTRADAY')
    profile=describe(values,request)
    output.mkdir(parents=True)
    save(output/'PROPOSAL.json',dict(status='AUTHOR_REVIEW_ONLY_NOT_WIRED_NOT_CALLED',
        actual_collected_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
        original_profile_sha256=hashlib.sha256((args.profile/'PROFILE.json').read_bytes()).hexdigest(),
        original_message_chars=source['profile']['exact_message_content_chars'],proposed_profile=profile,
        author_approval_required=['Existing meeting_plan_facts whitelist reused for A4','Catalog input_ref structure',
            'BARS carries complete original context, TRADABILITY references exact official proof','Missing original trade brief remains missing'],
        plan_projection_manifest=field_manifest,model_calls=0,orders=0,production_writes=0,
        unchanged_engine_inputs=True,unchanged_prompt_bytes=True,unchanged_complete_tech_contexts=True,
        release_gate='NOT_RUN',latency_effect='UNTESTED'))
    body=json.dumps(dict(values=values,messages=[dict(m) for m in request.messages]),ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()
    with gzip.open(output/'PROPOSED_RENDERED_INPUT.json.gz','wb') as stream:stream.write(body)
    print(json.dumps(dict(status='AUTHOR_REVIEW_ONLY',original_chars=source['profile']['exact_message_content_chars'],
        proposed_chars=profile['exact_message_content_chars'],model_calls=0)))


if __name__=='__main__':sys.exit(main())
