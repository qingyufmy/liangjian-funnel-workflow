"""Presentation of the same instant in BJT; originals retain source bytes."""
from datetime import datetime,timedelta
import re
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Shanghai')
_ISO=re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$')
_UTC_CLOCK=re.compile(r'UTC\s*([01]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?')


def _text(value):
    if _ISO.fullmatch(value):
        return datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(TZ).isoformat()
    def clock(match):
        hour=int(match[1])+8
        return '北京时间'+('次日' if hour>=24 else '')+f'{hour%24:02}:{match[2]}'+(':'+match[3] if match[3] else '')
    return _UTC_CLOCK.sub(clock,value)


def bjt_presentation(value):
    if isinstance(value,dict):return {key:bjt_presentation(item) for key,item in value.items()}
    if isinstance(value,list):return [bjt_presentation(item) for item in value]
    if isinstance(value,tuple):return tuple(bjt_presentation(item) for item in value)
    if isinstance(value,str):return _text(value)
    return value


def public_fact_view(value):
    """Display absence explicitly; never writes back or substitutes numeric 0."""
    value=bjt_presentation(value)
    if value is None:return '未取得'
    if isinstance(value,dict):return {key:public_fact_view(item) for key,item in value.items()}
    if isinstance(value,list):return [public_fact_view(item) for item in value]
    if isinstance(value,tuple):return tuple(public_fact_view(item) for item in value)
    if value=='UNKNOWN':return '未核实'
    if value=='UNKNOWN_PENDING':return '待核实'
    return value
