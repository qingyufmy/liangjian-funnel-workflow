import copy,json,hashlib
from pathlib import Path
from decimal import Decimal
import pytest
from liangjian_funnel.pipeline.numeric_fact_domain import numeric_fact_domain
from liangjian_funnel.pipeline.team_v2_review_schema import validate_daily_review


def original():
    b=json.loads((Path(__file__).parent/'fixtures/a5_midday_20261021_original.json').read_text(encoding='utf-8'))
    c=b['context_projection'][0];call=b['call'];raw=call['output_json']
    assert hashlib.sha256(raw.encode()).hexdigest()==call['output_hash']
    args=dict(c['validator_args']);args.update(trade_date='2026-10-21',review_kind='MIDDAY')
    args['counterexample_domain']=set(args['counterexample_domain'])
    return json.loads(raw),json.loads(call['catalog']['catalog_json']),args,c


def test_actual_1137_reply_old_metric_only_rejects_real_a6_budget_new_same_projection_accepts():
    output,catalog,args,source=original()
    old=validate_daily_review(output,catalog,**args,metrics=source['old_metric_tokens'])
    assert old.issues==('A5_SCORECARD_NUMBER_NOT_IN_METRICS',)
    domain=source['scorecard_claim_source_paths']
    assert '1.5E+4' in domain
    assert domain['1.5E+4'][0]=='$.a6.rows.risk_meetings[0].result_json.envelopes[0].risk_budget'
    assert validate_daily_review(output,catalog,**args,numeric_domain=domain).valid
    wrong=copy.deepcopy(output);wrong['team_scorecard']['drawdown']='987654.321%'
    assert 'A5_SCORECARD_NUMBER_NOT_IN_METRICS' in validate_daily_review(wrong,catalog,**args,numeric_domain=domain).issues


@pytest.mark.parametrize('value',[0,1,15000,1234.5,'15000.00',Decimal('.125')])
def test_any_sent_numeric_fact_is_collected_with_exact_path(value):
    domain=numeric_fact_domain({'nested':{'rows':[{'risk_budget':value}]}})
    assert domain[str(Decimal(str(value)).normalize())]==['$.nested.rows[0].risk_budget']


def test_encoded_source_json_is_not_a_manual_whitelist_and_ids_are_not_numbers():
    facts={'row_json':'{"budget":15000,"labels":["A6","2026-10-21","002270.SZ"],"truth":true}',
        'new_future_metric':{'arbitrary_name':98765}}
    domain=numeric_fact_domain(facts)
    assert set(domain)=={'1.5E+4','98765'}
    assert domain['1.5E+4']==['$.row_json::json.budget']
    assert not numeric_fact_domain({'nan':float('nan'),'infinity':float('inf'),'bool':True,'missing':None})
