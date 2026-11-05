"""Immutable union of canonical closed bars, preserving first observation.

Historical bars in a newly captured response remain retrospective. A new
capture day is not inferred from a bar's date, and no five-day gate is passed.
"""
from dataclasses import asdict
from datetime import datetime, timedelta
import hashlib
import json

from .bars15m import TZ
from .dk_shadow import json_value
from .ssl import PERIODS, evaluate_ssl, initial_ssl, ssl_append, ssl_series


def bar_identity(bar):
    return json_value(asdict(bar))


def merge_bars(previous, current, *, captured_at, source_sha256, observed_at, calendar):
    """Return a new canonical history or reject any overlap revision/gap.

    Previous rows are {bar: Bar15m, first_seen_at, source_sha256}; current bars
    must already be composed from one verified original response.
    """
    if (not isinstance(captured_at, datetime) or captured_at.utcoffset() is None
            or captured_at > observed_at or len(source_sha256)!=64
            or any(c not in '0123456789abcdef' for c in source_sha256)):
        raise ValueError('DK_ARCHIVE_CAPTURE_BINDING_INVALID')
    merged={}
    for item in previous:
        bar=item['bar'];key=bar.bar_end
        first=datetime.fromisoformat(item['first_seen_at'])
        if (first.utcoffset() is None or first>captured_at or key in merged
                or first<bar.bar_end+timedelta(seconds=60)):
            raise ValueError('DK_ARCHIVE_FIRST_SEEN_OR_DUPLICATE_INVALID')
        merged[key]=dict(item)
    before={key:bar_identity(item['bar']) for key,item in merged.items()}
    current_seen=set()
    for bar in current:
        if bar.bar_end in current_seen:raise ValueError('DK_ARCHIVE_CURRENT_DUPLICATE')
        current_seen.add(bar.bar_end)
        if bar.bar_end+timedelta(seconds=60)>captured_at:
            raise ValueError('DK_ARCHIVE_BAR_AFTER_CAPTURE')
        prior=merged.get(bar.bar_end)
        if prior is not None:
            if bar_identity(prior['bar'])!=bar_identity(bar):
                raise ValueError('DK_ARCHIVE_ORIGINAL_REVISION_CONFLICT')
        else:
            merged[bar.bar_end]=dict(bar=bar,first_seen_at=captured_at.isoformat(),source_sha256=source_sha256)
    result=[merged[key] for key in sorted(merged)]
    if not result:raise ValueError('DK_ARCHIVE_NO_BARS')
    # Validate all canonical identities, internal gaps and session closures.
    ssl_series([item['bar'] for item in result],period=20,observed_at=observed_at,calendar=calendar)
    if any(bar_identity(merged[key]['bar'])!=value for key,value in before.items()):
        raise ValueError('DK_ARCHIVE_PREVIOUS_MUTATED')
    return result


def archive_evaluation(items, *, observed_at, calendar):
    bars=tuple(item['bar'] for item in items)
    evaluations={};equality={}
    for period in PERIODS:
        memory=initial_ssl(period);points=[]
        for bar in bars:
            memory,point=ssl_append(memory,bar,observed_at=observed_at,calendar=calendar);points.append(point)
        batch=ssl_series(bars,period=period,observed_at=observed_at,calendar=calendar)
        equality[str(period)]=tuple(points)==batch
        evaluations[str(period)]=evaluate_ssl(bars,period=period,observed_at=observed_at,calendar=calendar)
    captures=sorted({datetime.fromisoformat(item['first_seen_at']).astimezone(TZ).date() for item in items})
    # The date of actual first capture, not the retrospective bar dates.
    capture_consecutive=all(calendar.next_trading_day(a)==b for a,b in zip(captures,captures[1:]))
    return dict(periods=evaluations,incremental_equal=equality,
        actual_capture_dates=[d.isoformat() for d in captures],actual_capture_days=len(captures),
        capture_dates_consecutive=capture_consecutive,
        five_day_shadow_acceptance='NOT_AUTOMATICALLY_ACCEPTED',
        first_bar=bars[0].bar_end.isoformat(),last_bar=bars[-1].bar_end.isoformat(),bars=len(bars),
        timeline_sha256=hashlib.sha256(json.dumps([bar_identity(b) for b in bars],sort_keys=True).encode()).hexdigest())
