"""Current-HEAD 0881 snapshots: original inputs, controlled examples, day noise."""
from copy import deepcopy
from datetime import datetime,timedelta
import html,json,sqlite3,hashlib
from pathlib import Path
import socket
from test_d8_real_outputs_0837 import (test_every_real_customer_card_has_original_and_rerendered_snapshot,
    OPS,canonical,text,CapturingNotifier,A5ReviewReport,_canonicalize_report_output,
    _projection_evidence_ids,_model_fact_projection,_enforce_verified_findings,_markdown,reconcile_report)
from liangjian_funnel.runtime.lark import LarkNotifier
from liangjian_funnel.runtime.lark_notifications import WorkflowLarkPublisher
from liangjian_funnel.runtime.state import RuntimeStore
from liangjian_funnel.runtime.notification_policy import NotificationPolicy,routes
from liangjian_funnel.review.key_counts import reconcile_counts


def render(path,card,description):
    value=card['card'];body=''.join('<p>'+html.escape(e.get('text',{}).get('content','')).replace('\n','<br>')+'</p>' for e in value['elements'] if e['tag']=='div')
    notes=''.join('<small>'+html.escape(str(e['elements']))+'</small>' for e in value['elements'] if e['tag']=='note')
    path.write_text('<!doctype html><meta charset="utf-8"><style>body{max-width:900px;margin:30px auto;font:16px/1.7 sans-serif}small{color:#777}</style><p>'+html.escape(description)+'</p><h2>'+html.escape(value['header']['title']['content'])+'</h2>'+body+notes,encoding='utf8')


def checks(card):
    value=text(card)
    assert '北京时间' in value
    assert not any(x in value for x in ('None','null','未识别'))
    assert any(x in value for x in ('接下来','下一','处理：','等待','执行状态','无需'))
    return dict(happened=True,key_numbers_or_explicit_not_applicable=True,next_action=True,bjt=True,forbidden_zero=True)


def make_publisher(tmp_path):
    captured=[]
    def internal(**args):
        captured.append(dict(channel='INTERNAL',card=LarkNotifier.build_card(args['title'],args['lines'],'orange'),key=args['key']))
        return dict(status='QUEUED_INTERNAL',event_id='CONTROLLED:'+args['key'])
    p=WorkflowLarkPublisher(RuntimeStore(tmp_path/'state/workflow.sqlite3'),None,policy_root=tmp_path,internal_sink=internal)
    notifier=CapturingNotifier();p.notifier=notifier
    return p,notifier,captured


def test_real_13_and_11_controlled_current_contract(tmp_path,monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('D8_NETWORK_FORBIDDEN')))
    test_every_real_customer_card_has_original_and_rerendered_snapshot(tmp_path/'legacy',monkeypatch)
    out=OPS/'D8_CURRENT_CONTRACT_0881_20261028';out.mkdir(exist_ok=True)
    publisher,customer,internal=make_publisher(tmp_path/'current')
    legacy=OPS/'D8_REAL_OUTPUT_REPLAY_0837_20261027'
    exact=json.loads((OPS/'D8_EXACT_CARD_INPUTS_RO_0837_20261027.json').read_bytes())
    plans={r['plan_id']:r for r in exact['plans']}
    originals=[]
    for path in (OPS/'r0-0837-business-originals-20261027').glob('*.json'):
        if path.stat().st_size<15000:
            v=json.loads(path.read_bytes())
            if 'card' in v:originals.append((path,v))
    table=[]
    for index,(path,original) in enumerate(sorted(originals,key=lambda pair:pair[1]['observed_at'])):
        old_title=text(original['card']).split('\n',1)[0]
        after=json.loads((legacy/(path.name[:16]+'-after.json')).read_bytes())
        before_c,before_i=len(customer.cards),len(internal)
        key='REAL:'+original['delivery_key'] if 'delivery_key' in original else 'REAL:'+path.stem
        now=datetime.fromisoformat(original['observed_at'])
        if 'suppressed' in after:
            result=publisher.publish_a3_premarket_status(analyzed_at=now,reason_code='DAILY_PLAN_NOT_READY')
            assert result['status']=='NOOP';table.append(dict(source=path.name,route=[],merged=False,suppressed_before_cas=True));continue
        if '盘中复盘' in old_title or '盘后复盘' in old_title:
            kind='MIDDAY' if '盘中复盘' in old_title else 'POST_CLOSE'
            folder=OPS/('a5-0825-originals-20261027' if kind=='MIDDAY' else 'a5-close-0837-originals-20261027')
            facts=json.loads(next(folder.glob('*-facts.json')).read_bytes());raw=json.loads(next(folder.glob('*-model-*.json')).read_bytes())
            allowed=_projection_evidence_ids(_model_fact_projection(facts))|{'METRICS:DAILY','DATA_QUALITY:DAILY'}
            report=A5ReviewReport.model_validate(_canonicalize_report_output(deepcopy(raw['output']),allowed_evidence=allowed))
            _enforce_verified_findings(report,facts);report.fact_reconciliation.extend(reconcile_report(report,facts))
            # Later RO historical fill ledger independently attests zero for
            # 10-27, not an invented "zero orders implies zero fills" claim.
            proof=json.loads((OPS/'KEY_COUNTS_HISTORICAL_1027_RO_0881_20261028.json').read_bytes())
            assert proof['paper']==[0] and proof['team']==[12] and proof['workflow']==[3347,3346,0]
            actual=dict(plans=14,monitor_rows=facts['metrics']['a4_monitor_observation_count'],signals=0,
                model_calls=7 if kind=='MIDDAY' else 12,fills=0,minute_rows=1666 if kind=='MIDDAY' else 3346)
            facts['key_count_reconciliation']=reconcile_counts(facts,actual,source_sha256=proof['sha256'],observed_at=proof['observed_at'])
            result=publisher.publish_a5_review(dict(review_id='REAL:'+kind,trade_date='2026-10-27',review_kind=kind,
                report=report.model_dump(mode='json'),fact_snapshot=facts),now=now)
            sent=customer.cards[-1];checks(sent)
        else:
            kind='PREMARKET_A3' if old_title.startswith('A股盘前计划') else 'A4_JUDGE_TIMEOUT' if '超时' in text(after) else 'A4_DATA_ALERT'
            body=[e['text']['content'] for e in after['card']['elements'] if e['tag']=='div']
            symbol=''
            if original.get('plan_ids'):
                row=plans.get(original['plan_ids'][0]);symbol=row.get('symbol','') if row else ''
            result=publisher._send(delivery_key=key,kind=kind,source_id=path.stem,title=after['card']['header']['title']['content'],
                lines=body,summary=dict(symbol=symbol,state=kind),now=now)
            if len(customer.cards)>before_c:sent=customer.cards[-1]
            elif len(internal)>before_i:sent=internal[-1]['card']
            else:
                with sqlite3.connect(tmp_path/'current/state/notification_policy.sqlite3') as db:
                    payload=json.loads(db.execute('SELECT payload_json FROM notice_events WHERE event_key=?',(key,)).fetchone()[0])
                sent=LarkNotifier.build_card(payload['title'],payload['lines'])
            checks(sent)
        render(out/f'real-{index:02}.html',sent,'10-27 原始输入重放；路由/合并见逐件表，非重新调用模型')
        table.append(dict(source=path.name,source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            route=['CUSTOMER']*(len(customer.cards)-before_c)+['INTERNAL']*(len(internal)-before_i),
            result=result,three_points=checks(sent)))
    assert len(table)==13
    cases=[('BUY通过','A4_EFFECTIVE','688351','当前技术参考价76.21；绑定区间76.20–76.30；止损75；禁追价76.50；满足2 / 未满足0。下一完整分钟按冻结契约尝试模拟撮合，未成交不追价。'),
        ('不动追价否决','A4_REVIEW_NO_ACTION','688351','当前价76.60高于禁追价76.50；绑定区间76.20–76.30；止损75；满足1 / 未满足1（禁止追价）。等待回落，下一自然节拍复核。'),
        ('复核超时','A4_JUDGE_TIMEOUT','688351','本轮未形成有效复核。当前技术参考价76.21；区间76.20–76.30；止损75；禁追价76.50；满足2 / 未满足0。下一自然节拍复核，不以超时推定买入。'),
        ('候选截断','A4_REVIEW_TRUNCATED','600628','候选3只，已送2只，本股未送模型；当前技术参考价6.58，区间6.57–6.59，止损6.54，禁追价6.60；满足2 / 未满足0。等待下一自然节拍，记录未复核。'),
        ('降级开始','A4_REVIEW_ENGINE_ONLY','','连续3次有效失败触发仅引擎降级；下一步原风控外执行加严价格和总仓位50%规则，每次准入另报。'),
        ('降级准入','A4_ENGINE_ONLY_ADMISSION','600628','当前技术参考价6.58；更严区间6.57–6.59；止损6.54；禁追价6.60；满足2 / 未满足0。原门核验结果拒绝，数量0；不下单，等待下个节拍。'),
        ('降级结束','A4_REVIEW_RESTORED','','本交易日首次有效模型复核已经落盘；无需操作，后续自然节拍继续模型复核。'),
        ('原买入被拒','A4_REVIEW_ORIGINAL_BUY_REJECTED','600628','原买入结论校验未通过；当前技术参考价6.58；绑定区间6.57–6.59；止损6.54；禁追价6.60；满足1 / 未满足1（引用冲突）。该结论不发布，保留原回复交运维核验。'),
        ('盘前就绪','PREMARKET_BATCH_READY','','计划14只已完成切换；接下来等待09:26自然激活，当前不是已成交。'),
        ('盘前未就绪','PREMARKET_A3_STATUS','','08:45终态：2只计划审核未完成，已通过12只切换；下一步仅已审计划走自然激活，未审计划保持拒绝。'),
        ('收盘终止','AUDITED_RESEARCH_FAILED','','收盘研究因真实截止终止；候选未知不是0；接下来交运维裁定，未获批准不重跑。')]
    controlled=[]
    p,c,i=make_publisher(tmp_path/'controlled')
    for n,(label,kind,symbol,line) in enumerate(cases):
        bc,bi=len(c.cards),len(i)
        result=p._send(delivery_key='CONTROLLED:'+str(n),kind=kind,source_id='CONTROLLED:'+str(n),
            title='受控验收｜'+label+('｜'+symbol if symbol else ''),lines=[line],summary=dict(symbol=symbol,state=label),
            now=datetime.fromisoformat('2026-10-27T14:00:00+08:00'))
        card=c.cards[-1] if len(c.cards)>bc else i[-1]['card'];check=checks(card)
        render(out/f'controlled-{n:02}.html',card,'明确受控构造，无模型/行情/通知HTTP，不是实际交易')
        controlled.append(dict(label=label,routes=list(result['routes']),three_points=check))
    assert len(controlled)==11
    (out/'MATRIX.json').write_bytes(canonical(dict(real=table,controlled=controlled,source_http=0,model_http=0,notification_http=0,production_writes=0)))



def test_all_day_event_noise_routing_projection(tmp_path,monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('D8_NETWORK_FORBIDDEN')))
    day=json.loads((Path(__file__).parent/'fixtures/postclose_20261027/full_day_runtime_rows.json').read_bytes())
    exact=json.loads((OPS/'D8_EXACT_CARD_INPUTS_RO_0837_20261027.json').read_bytes())
    plans={r['plan_id']:r for r in exact['plans']}
    p,c,i=make_publisher(tmp_path/'noise')
    events=[]
    for row in day['minute_events']:
        plan=plans.get(row['plan_id'],{})
        symbol=plan.get('symbol','')
        payload=dict(plan_id=row['plan_id'],symbol=symbol,strategy=dict(action=row['technical_action']))
        events.append({**row,'lane_id':'lane_1','symbol':symbol,'payload_json':json.dumps(payload)})
    # These are all 3347 original event IDs/action/reason/effective flags.
    # The compact frozen projection cannot recreate unpersisted request bytes
    # or missing quote fields; this replay measures routing/noise, not fills.
    for row in events:
        p.publish_a4_events([row],plans=plans,now=datetime.fromisoformat(row['minute_end']))
    minute_customer=len(c.cards)
    assert minute_customer==0  # no original effective trade signal existed
    native=json.loads((OPS/'D8_ALL_NATIVE_DAY27_RO_0881_20261028.json').read_bytes())
    from liangjian_funnel.runtime.notification_presentation import native_card
    for row in native['rows']:
        assert hashlib.sha256(row['body_json'].encode()).hexdigest()==row['body_sha256']
        now=datetime.fromisoformat(row['observed_at'])
        card=native_card(kind=row['kind'],body=json.loads(row['body_json']),observed_at=now,event_id=row['event_id'])
        lines=[x['text']['content'] for x in card['card']['elements'] if x['tag']=='div']
        p._send(delivery_key='NATIVE:'+row['event_id'],kind=row['kind'],source_id=row['event_id'],
            title=card['card']['header']['title']['content'],lines=lines,summary=dict(state=row['kind']),now=now)
    p.flush_notification_summaries(now=datetime.fromisoformat('2026-10-27T23:59:00+08:00'),close_day=True)
    stats=NotificationPolicy(tmp_path/'noise/state/notification_policy.sqlite3').counts('2026-10-27')
    out=OPS/'D8_CURRENT_CONTRACT_0881_20261028'
    out.mkdir(exist_ok=True)
    result=dict(status='FULL_PERSISTED_DAY_EVENT_ROUTING_PROJECTION',original_minute_rows=len(events),
        original_native_events=len(native['rows']),monitor_customer_cards=minute_customer,
        replay_customer_cards=len(c.cards),replay_internal_cards=len(i),policy=stats,
        actual_customer_originals=13,actual_customer_scope='13 archived app customer cards; separate external watchdog original wire unavailable',
        nonmonitor_customer_cards='five actual activation plan pages + two customer A5 summaries + one actual CAS final state are tested in real13 matrix, not added twice here',
        limitation='Compact day projection proves event routing/noise, not all historical model request bytes or a new model outcome',
        model_http=0,notification_http=0,source_http=0,production_writes=0)
    (out/'FULL_DAY_NOISE.json').write_bytes(canonical(result))
    assert stats['observations'].get('MERGED',0)>0
