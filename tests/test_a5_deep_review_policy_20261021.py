from datetime import datetime
import pytest
from liangjian_funnel.runtime.team_v2_review_policy import load_review_policy,review_wall_budget


def test_a5_has_600_seconds_only_and_midday_never_runs_into_1250(tmp_path):
    policy=load_review_policy(tmp_path)
    stamp=lambda t:datetime.fromisoformat('2026-10-21T'+t+'+08:00')
    assert review_wall_budget(policy,now=stamp('11:35:00'),review_kind='MIDDAY')==600
    assert review_wall_budget(policy,now=stamp('12:49:30'),review_kind='MIDDAY')==30
    assert review_wall_budget(policy,now=stamp('12:50:00'),review_kind='MIDDAY')==0
    assert review_wall_budget(policy,now=stamp('17:00:00'),review_kind='POST_CLOSE')==600


def test_config_does_not_allow_expanding_a5_past600_or_moving_midday_cutoff(tmp_path):
    import yaml
    folder=tmp_path/'config';folder.mkdir();path=folder/'team_v2_a5.yaml'
    policy=load_review_policy(tmp_path)
    for change in ({'wall_clock_seconds':601},{'midday_deadline':'13:00'}):
        path.write_text(yaml.safe_dump(dict(policy,**change)))
        with pytest.raises(ValueError,match='A5_RUNTIME_POLICY_INVALID'):load_review_policy(tmp_path)


def test_a5_stream_observation_does_not_enable_pm_policy_or_change_other_roles(tmp_path):
    import httpx,json
    from test_pm_stream_telemetry import Clock,Stream,event,client
    clock=Clock();seen=[]
    def handler(request):
        seen.append(json.loads(request.read()))
        return httpx.Response(200,headers={'content-type':'text/event-stream'},stream=Stream(clock,[
            (1,b': heartbeat\n\n'),(2,event({'choices':[{'delta':{'content':'{"ok":true}'}}]})),
            (3,event({'choices':[],'usage':{'completion_tokens':10}})),(3,b'data: [DONE]\n\n')]))
    result=client(tmp_path,handler,clock,max_attempts=1).complete('deepseek-v4-pro',
        [{'role':'user','content':'test'}],stage='A5',team_session='POST_CLOSE',timeout_seconds=600,max_output_tokens=16384)
    assert result.output=={'ok':True} and result.pm_stream_telemetry is None
    assert result.a5_stream_telemetry['ttft_ms']==2000
    assert result.a5_stream_telemetry['output_tokens']==10
    assert seen[0]['stream_options']=={'include_usage':True}
