"""Only fake internal endpoints via HTTP mock; no customer sender available."""
from datetime import datetime
from pathlib import Path
import hashlib
import json
import sqlite3

import httpx
import pytest

from liangjian_funnel.runtime.team_v2_internal import InternalOutbox,InternalSender
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore

NOW=datetime.fromisoformat('2026-10-19T10:01:00+08:00')
URL='https://open.feishu.cn/open-apis/bot/v2/hook/FAKE_NOT_A_CREDENTIAL'
BODY=dict(msg_type='text',content={'text':'模拟盘｜裁决\n批准100股'})


def outbox(tmp_path):return InternalOutbox(TeamMeetingStore(tmp_path/'team.db'))


def enqueue(box,source='decision-1',body=None):
    return box.enqueue(account_id='local-paper',trade_date='2026-10-19',kind='DECISION',source_id=source,
        body=body or BODY,observed_at=NOW,priority=50)


def sender(tmp_path,handler=None,env=None):
    seen=[]
    def reply(request):
        seen.append(request)
        return handler(request) if handler else httpx.Response(200,json={'code':0})
    return InternalSender(tmp_path/'internal_alert.env',env=env or {},transport=httpx.MockTransport(reply)),seen


def test_atomic_original_body_binding_and_success_ack_survive_restart(tmp_path):
    box=outbox(tmp_path);event=enqueue(box);send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    assert enqueue(box)==event
    assert box.flush(send,now=NOW,maximum=3)['sent']==1 and len(seen)==1
    assert outbox(tmp_path).flush(send,now=NOW,maximum=3)['sent']==0 and len(seen)==1
    with sqlite3.connect(box.store.path) as db:
        row=db.execute('SELECT body_json,body_sha256,status FROM team_notifications').fetchone()
        assert hashlib.sha256(row[0].encode()).hexdigest()==row[1] and row[2]=='SENT'
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()
    with pytest.raises(ValueError,match='BINDING_CONFLICT'):
        enqueue(box,body=dict(msg_type='text',content={'text':'模拟盘｜different'}))


def test_missing_internal_secret_does_not_fall_back_or_consume_pending(tmp_path):
    box=outbox(tmp_path);enqueue(box)
    send,seen=sender(tmp_path,env={'LARK_WEBHOOK_URL':URL})
    assert box.flush(send,now=NOW,maximum=3)['reason_code']=='INTERNAL_WEBHOOK_NOT_CONFIGURED'
    assert not seen
    with sqlite3.connect(box.store.path) as db:assert db.execute('SELECT status FROM team_notifications').fetchone()==('PENDING',)


@pytest.mark.parametrize('fault',['timeout','bad_json','redirect','api_rejected','large','unknown_code'])
def test_uncertain_transport_is_not_sent_and_never_automatically_retried(tmp_path,fault):
    def reply(request):
        if fault=='timeout':raise httpx.ReadTimeout('PRIVATE_URL:'+URL,request=request)
        if fault=='bad_json':return httpx.Response(200,text='bad PRIVATE_BODY')
        if fault=='redirect':return httpx.Response(302,headers={'location':'https://customer.invalid'})
        if fault=='api_rejected':return httpx.Response(200,json={'code':1,'msg':'PRIVATE_PROVIDER_BODY'})
        if fault=='large':return httpx.Response(200,text='x'*20000)
        return httpx.Response(200,json={'ok':True})
    box=outbox(tmp_path);enqueue(box);send,seen=sender(tmp_path,reply,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    result=box.flush(send,now=NOW,maximum=3)
    assert result['sent']==0 and result['attempted']==1
    assert box.flush(send,now=NOW,maximum=3)['attempted']==0 and len(seen)==1
    raw=box.store.path.read_bytes()
    assert b'PRIVATE_' not in raw and b'FAKE_NOT_A_CREDENTIAL' not in raw


def test_sending_crash_is_not_resubmitted_and_old_day_not_sent_as_today(tmp_path):
    box=outbox(tmp_path);enqueue(box)
    with sqlite3.connect(box.store.path) as db:db.execute("UPDATE team_notifications SET status='SENDING'")
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    assert box.flush(send,now=NOW,maximum=3)['attempted']==0 and not seen
    enqueue(box,'second')
    assert box.flush(send,now=datetime.fromisoformat('2026-10-20T10:01:00+08:00'),maximum=3)['attempted']==0


def test_independent_env_selects_only_authorized_key_and_rejects_duplicate(tmp_path):
    file=tmp_path/'internal_alert.env'
    file.write_text("OTHER_KEY=NOT_IMPORTED\nSHADOW_RESEARCH_WEBHOOK='"+URL+"'\n",encoding='utf-8')
    send,seen=sender(tmp_path);assert send.ready()
    assert send.send(BODY)=='SENT' and len(seen)==1
    assert URL.encode() not in seen[0].content
    file.write_text('SHADOW_RESEARCH_WEBHOOK='+URL+'\nSHADOW_RESEARCH_WEBHOOK='+URL,encoding='utf-8')
    assert not send.ready()


@pytest.mark.parametrize('url',[URL+'?token=x','http://open.feishu.cn/x','https://open.feishu.cn.evil/hook/x',
    'https://u:p@open.feishu.cn/open-apis/bot/v2/hook/x'])
def test_invalid_destination_never_makes_http_request(tmp_path,url):
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':url})
    assert not send.ready() and send.send(BODY)=='NOT_CONFIGURED' and not seen


def test_card_secret_rejected_before_enqueue_and_integrity_failure_before_send(tmp_path):
    box=outbox(tmp_path)
    with pytest.raises(ValueError,match='UNSAFE'):
        enqueue(box,body=dict(msg_type='text',content={'text':'simulate '+URL}))
    enqueue(box)
    with sqlite3.connect(box.store.path) as db:db.execute("UPDATE team_notifications SET body_json='{}'")
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    with pytest.raises(ValueError,match='HASH_MISMATCH'):box.flush(send,now=NOW,maximum=3)
    assert not seen


def test_priority_bounded_flush_and_deduped_guard_count(tmp_path):
    box=outbox(tmp_path)
    for i in range(7):
        box.enqueue(account_id='local-paper',trade_date='2026-10-19',kind='DECISION',source_id=str(i),
            body=BODY,observed_at=NOW,priority=50)
        assert box.record_guard_rejection('local-paper','2026-10-19',str(i))==i+1
        assert box.record_guard_rejection('local-paper','2026-10-19',str(i))==i+1
    box.enqueue(account_id='local-paper',trade_date='2026-10-19',kind='ALERT',source_id='halt',
        body={'msg_type':'text','content':{'text':'模拟盘｜停止开仓'}},observed_at=NOW,priority=0)
    send,seen=sender(tmp_path,env={'SHADOW_RESEARCH_WEBHOOK':URL})
    assert box.flush(send,now=NOW,maximum=3)['sent']==3 and len(seen)==3
    assert '停止' in json.loads(seen[0].content)['content']['text']
