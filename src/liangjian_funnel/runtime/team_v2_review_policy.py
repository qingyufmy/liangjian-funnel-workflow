"""A5-only deep review wall clock, never an intraday A4/A6/PM budget."""
from pathlib import Path
from datetime import datetime,time
import math


def load_review_policy(root):
    import yaml
    path=Path(root)/'config/team_v2_a5.yaml'
    value=yaml.safe_load(path.read_text(encoding='utf-8')) if path.is_file() else dict(
        schema_version='team-a5-runtime/1',wall_clock_seconds=600,midday_deadline='12:50',max_output_tokens=16384)
    if (not isinstance(value,dict) or set(value)!={'schema_version','wall_clock_seconds','midday_deadline','max_output_tokens'}
            or value['schema_version']!='team-a5-runtime/1' or type(value['wall_clock_seconds']) not in (int,float)
            or not math.isfinite(value['wall_clock_seconds']) or not 0<value['wall_clock_seconds']<=600
            or value['midday_deadline']!='12:50' or type(value['max_output_tokens']) is not int
            or not 0<value['max_output_tokens']<=16384):raise ValueError('A5_RUNTIME_POLICY_INVALID')
    return value


def review_wall_budget(policy,*,now,review_kind):
    from zoneinfo import ZoneInfo
    at=now.astimezone(ZoneInfo('Asia/Shanghai'))
    if review_kind!='MIDDAY':return float(policy['wall_clock_seconds'])
    cutoff=datetime.combine(at.date(),time(12,50),tzinfo=at.tzinfo)
    return max(0,min(float(policy['wall_clock_seconds']),(cutoff-at).total_seconds()))
