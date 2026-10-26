from datetime import datetime,timedelta
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from liangjian_funnel.runtime.monitor_slow_warning import project_slow_warning
from liangjian_funnel.runtime.team_v2_store import TeamMeetingStore


def test_real_parent_warning_queues_once_without_claiming_termination(tmp_path):
    settings=SimpleNamespace(root=tmp_path);store=TeamMeetingStore(tmp_path/'state/team_v2/team.db')
    now=datetime(2026,10,26,10,26,tzinfo=ZoneInfo('Asia/Shanghai'))
    path=tmp_path/'outputs/node/slow-monitor/2026-10-26.json';path.parent.mkdir(parents=True)
    body=dict(schema='monitor-slow-warning/1',run_id='monitor-original',observed_at=now.isoformat(),
        started_at=(now-timedelta(seconds=20)).isoformat(),threshold_ms=20000,elapsed_ms=20000,task_terminated=False)
    path.write_text(json.dumps(body))
    first=project_slow_warning(settings,now=now);again=project_slow_warning(settings,now=now)
    assert first==again
    with store._connect() as db:
        rows=db.execute('SELECT body_json FROM team_notifications').fetchall()
    assert len(rows)==1 and '不等于超时终止' in rows[0][0]
    body['observed_at']=(now+timedelta(minutes=1)).isoformat();path.write_text(json.dumps(body))
    with pytest.raises(ValueError,match='BINDING_INVALID'):project_slow_warning(settings,now=now)
