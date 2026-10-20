"""Shared ten-stage contract. STARTUP is a dated read-only historical alias."""
from datetime import date
from types import MappingProxyType

VERSION='emotion-vocabulary/1'
POLICIES=MappingProxyType({'ICE_POINT':'NO_NEW_ENTRY','LATENT':'WATCH_ONLY',
    'IGNITION':'PROBE_ONLY','CONFIRMATION':'ALLOW','ACCELERATION':'ALLOW',
    'CLIMAX':'WATCH_ONLY','DIVERGENCE':'WATCH_ONLY','RETREAT':'NO_NEW_ENTRY',
    'REPAIR':'ALLOW','FADE':'NO_NEW_ENTRY'})
STAGES=frozenset(POLICIES)
STARTUP_READ_ALIAS_UNTIL=date(2026,10,27)


def read_emotion_stage(value,*,source_trade_date):
    """Only decode the explicitly dated input alias; writers emit IGNITION."""
    if value=='STARTUP':
        if date.fromisoformat(str(source_trade_date))>STARTUP_READ_ALIAS_UNTIL:
            raise ValueError('EMOTION_STARTUP_READ_ALIAS_EXPIRED')
        return 'IGNITION'
    if value not in STAGES:raise ValueError('EMOTION_STAGE_INVALID')
    return value
