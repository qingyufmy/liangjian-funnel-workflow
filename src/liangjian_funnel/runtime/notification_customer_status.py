"""0881 fan-out of existing native status events; no new business event."""
from contextlib import closing
from datetime import datetime
import json


def publish_customer_status(settings, store, *, now):
    from .notification_policy import DUAL_KINDS
    from .notification_presentation import native_card
    from .lark_notifications import WorkflowLarkPublisher
    from .state import RuntimeStore
    # The caller is the dedicated notification command, not the A4 model path.
    publisher=WorkflowLarkPublisher(RuntimeStore(settings.state_db_path),None,
        webhook_path=settings.lark_webhook_path,timeout_seconds=settings.lark_timeout_seconds,
        policy_root=settings.root)
    day=now.date().isoformat()
    with closing(store._connect()) as db:
        rows=db.execute('SELECT event_id,kind,body_json,observed_at,status FROM team_notifications '
            'WHERE trade_date=? ORDER BY observed_at,event_id',(day,)).fetchall()
    output=[]
    for event_id,kind,raw,at,status in rows:
        if kind not in DUAL_KINDS:
            continue
        card=native_card(kind=kind,body=json.loads(raw),observed_at=at,event_id=event_id)['card']
        lines=[element['text']['content'] for element in card['elements'] if element['tag']=='div']
        output.append(publisher._send(delivery_key='native-customer:'+event_id,kind=kind,source_id=event_id,
            title=card['header']['title']['content'],lines=lines,
            summary=dict(native_internal_event_id=event_id,native_internal_status=status,
                trade_date=day,state=kind),now=datetime.fromisoformat(at)))
    output.extend(publisher.flush_notification_summaries(now=now,close_day=(now.hour,now.minute)>=(17,55)))
    return output
