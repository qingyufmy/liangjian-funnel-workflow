from contextlib import closing
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.node_identity import observe_node_identity
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


def test_only_identity_changes_enqueue_once_and_survive_observer_restart(tmp_path):
    settings=SimpleNamespace(root=tmp_path);store=TeamMeetingStore(tmp_path/'team.db')
    now=datetime(2026,10,26,13,tzinfo=ZoneInfo('Asia/Shanghai'))
    first=dict(root=str(tmp_path.resolve()),boot_id='old-boot',pid=1,start_ticks='100')
    assert observe_node_identity(settings,store,now=now,identity=first)['status']=='BASELINED'
    second=dict(first,boot_id='new-boot',pid=2,start_ticks='20')
    assert observe_node_identity(settings,store,now=now,identity=second)['status']=='CHANGED'
    again=TeamMeetingStore(store.path)
    assert observe_node_identity(settings,again,now=now,identity=second)['status']=='UNCHANGED'
    with closing(store._connect()) as db:
        rows=db.execute('SELECT source_id,body_json FROM team_notifications').fetchall()
    assert len(rows)==1 and '主机启动ID变化' in rows[0][1]
    assert 'OOM' not in rows[0][1]


def test_wrong_root_never_updates_marker(tmp_path):
    store=TeamMeetingStore(tmp_path/'team.db');settings=SimpleNamespace(root=tmp_path)
    with pytest.raises(ValueError,match='IDENTITY_INVALID'):
        observe_node_identity(settings,store,now=datetime.now().astimezone(),
            identity=dict(root='other',boot_id='b',pid=1,start_ticks='1'))
