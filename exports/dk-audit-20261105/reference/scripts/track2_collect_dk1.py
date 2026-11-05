"""T2-1124: one EM + one Tencent request after same-source offline preflight.

No model client, VM, database, runtime configuration or retry is used here.
All response bytes, failures and cumulative reservations remain owned artifacts.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime, time
from decimal import Decimal
import argparse
import hashlib
import json
from pathlib import Path
import sys
import traceback
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'src'))

import httpx
from liangjian_funnel.market_state.bars15m import compose_15m, dk_reference_anchor
from liangjian_funnel.market_state.dk_sources import (
    EASTMONEY_URL, EASTMONEY_PARAMS, TENCENT_URL, TENCENT_PARAMS,
    parse_eastmoney, parse_tencent, compare_overlap, warmup_n_limit,
)
from liangjian_funnel.market_state.research_http import (
    ProbeLedger, AuditedTransport, NOT_BEFORE, SOURCE_DEADLINE, TZ, verify_preflight,
    source_head,
)
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar

ARTIFACTS = Path('D:/dev_A股/liangjian_team_v2_20261016/artifacts/track2/20261029')


def _encode(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError('DK_OUTPUT_TYPE_INVALID')


def write_json(path, value):
    with path.open('x', encoding='utf-8') as output:
        json.dump(value, output, ensure_ascii=False, sort_keys=True, indent=2, default=_encode)


def _payload(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('DK_DUPLICATE_JSON_KEY')
            result[key] = value
        return result
    def constant(_):
        raise ValueError('DK_NONFINITE_JSON')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def fetch_one(client, source, url, params, directory, parser):
    record = dict(source=source, requested_at=datetime.now(TZ).isoformat(),
                  endpoint=url, parameters=dict(params))
    partial = bytearray()
    try:
        with client.stream('GET', url, params=params) as response:
            record['status_code'] = response.status_code
            for chunk in response.iter_bytes():
                partial.extend(chunk)
            record['response_fully_read'] = True
        record['response_closed'] = response.is_closed
        with (directory/(source.lower()+'-response.body')).open('xb') as output:
            output.write(partial)
        record.update(body_sha256=hashlib.sha256(partial).hexdigest(), bytes=len(partial))
        if response.status_code != 200:
            raise ValueError('DK_SOURCE_HTTP_NOT_200')
        rows = parser(_payload(bytes(partial)))
        if not rows:
            raise ValueError('DK_SOURCE_EMPTY_ROWS')
        record.update(status='PARSED_NOT_YET_ELIGIBLE', row_count=len(rows),
                      first_bar=rows[0]['bar_end'], last_bar=rows[-1]['bar_end'])
        return rows, record
    except Exception as exc:
        path = directory/(source.lower()+'-response.body')
        if partial and not path.exists():
            with path.open('xb') as output:
                output.write(partial)
        record.update(status='UNAVAILABLE', exception_type=type(exc).__name__,
            reason_code=str(exc) if type(exc) is ValueError and str(exc).replace('_','').isalnum()
                and len(str(exc)) <= 100 else 'DK_SOURCE_EXCEPTION',
            frames=[dict(file=Path(frame.filename).name, function=frame.name, line=frame.lineno)
                    for frame in traceback.extract_tb(exc.__traceback__)[-8:]],
            bytes=len(partial), partial_body_sha256=hashlib.sha256(partial).hexdigest())
        return [], record
    finally:
        record['completed_at'] = datetime.now(TZ).isoformat()
        write_json(directory/(source.lower()+'-attempt.json'), record)


def closed_days(rows, now, calendar):
    selected = []
    for row in rows:
        if row['bar_end'] > now:
            raise ValueError('DK_FUTURE_SOURCE_BAR')
        if date(2026, 9, 15) <= row['bar_end'].date() <= date(2026, 10, 29):
            selected.append(row)
    days = []
    for day in sorted({row['bar_end'].date() for row in selected}):
        days.append(compose_15m([row for row in selected if row['bar_end'].date() == day],
            symbol='002463.SZ', trade_date=day, observed_at=now,
            market_close_at=datetime.combine(day, time(15), tzinfo=TZ), calendar=calendar))
    return days


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--preflight', required=True, type=Path)
    args = parser.parse_args()
    now = datetime.now(TZ)
    if not NOT_BEFORE <= now < SOURCE_DEADLINE:
        raise ValueError('T2_SOURCE_TIME_WINDOW_CLOSED')
    preflight_sha = verify_preflight(args.preflight, ROOT)
    directory = ARTIFACTS/'dk-runs'/(now.strftime('%H%M%S')+'-'+uuid.uuid4().hex[:8])
    directory.mkdir(parents=True, exist_ok=False)
    # All continuations share this ledger. A new run directory never resets it.
    ledger = ProbeLedger(ARTIFACTS/'dk-probe-ledger.jsonl')
    before = ledger.summary()
    report = dict(schema_version='track2-dk1-acquisition/1', status='STARTED',
        actual_started_at=now.isoformat(), preflight_path=str(args.preflight), preflight_sha256=preflight_sha,
        counts_before=before, provider_model_calls=0, production_writes=0, vm_access=0,
        retry_count=0, source_deadline=SOURCE_DEADLINE.isoformat(),
        ledger_path=str(ledger.path), head=source_head(ROOT))
    try:
        with httpx.Client(transport=AuditedTransport(httpx.HTTPTransport(retries=0, trust_env=False), ledger),
                          timeout=httpx.Timeout(15, connect=4), trust_env=False, follow_redirects=False) as client:
            em, em_attempt = fetch_one(client, 'EASTMONEY', EASTMONEY_URL, EASTMONEY_PARAMS, directory, parse_eastmoney)
            qq, qq_attempt = fetch_one(client, 'TENCENT', TENCENT_URL, TENCENT_PARAMS, directory, parse_tencent)
        report['attempts'] = [em_attempt, qq_attempt]
        overlap = compare_overlap(em, qq)
        report['overlap'] = overlap
        em_eligible = overlap['status'] == 'MATCH'
        chosen = em if em_eligible else qq
        report['chosen_source'] = 'EASTMONEY_OVERLAP_VERIFIED' if em_eligible else 'TENCENT_ONLY'
        report['eastmoney_usable'] = em_eligible
        if chosen:
            days = closed_days(chosen, datetime.now(TZ), ExchangeTradingCalendar())
            write_json(directory/'bars15m.json', [asdict(day) for day in days])
            anchor_day = next((day for day in days if day.trade_date == date(2026, 10, 26)), None)
            report['anchor'] = dk_reference_anchor(anchor_day) if anchor_day else dict(status='DATA_BLOCK', reason='ANCHOR_DATE_MISSING')
            closed = [bar for day in days for bar in day.bars if bar.status == 'CLOSED']
            anchor_at = datetime(2026, 10, 26, 14, 45, tzinfo=TZ)
            report['available_window'] = dict(first=closed[0].bar_end if closed else None,
                last=closed[-1].bar_end if closed else None, closed_15m_count=len(closed))
            report['warmup_bound_at_anchor'] = warmup_n_limit(sum(bar.bar_end < anchor_at for bar in closed))
            report['status'] = ('ANCHOR_VOLUME_CONFIRMED_NOT_CALIBRATED'
                if report['anchor'].get('volume_hard_check_passed') is True else
                'HISTORY_INSUFFICIENT' if not closed else 'ANCHOR_HARD_CHECK_NOT_CONFIRMED')
        else:
            report['status'] = 'DATA_BLOCK'
        report['signal_evidence'] = dict(status='信号日期部分可证', screenshot='NOT_REQUIRED_T2_1125',
            D_dates=['2026-10-09','2026-10-21','2026-10-23'], K_dates=['2026-10-15','2026-10-22'],
            expected_sequence=['D','K','D','K','D','K','D','K'], total_required=8,
            remaining_dates='DIRECTION_AND_ORDER_ONLY', calibration_run=False)
    except Exception as exc:
        report.update(status='FAILED_ORIGINAL_RETAINED', exception_type=type(exc).__name__,
            reason_code=str(exc) if type(exc) is ValueError and str(exc).replace('_','').isalnum()
                and len(str(exc)) <= 100 else 'DK_ANALYSIS_EXCEPTION',
            frames=[dict(file=Path(frame.filename).name, function=frame.name, line=frame.lineno)
                    for frame in traceback.extract_tb(exc.__traceback__)[-8:]])
        raise
    finally:
        try:
            report['counts_after'] = ledger.summary()
        except Exception as accounting_exc:
            report['counts_after'] = None
            report['accounting_exception_type'] = type(accounting_exc).__name__
            report['status'] = 'FAILED_LEDGER_ACCOUNTING_UNCONFIRMED'
        report['actual_completed_at'] = datetime.now(TZ).isoformat()
        write_json(directory/'report.json', report)
        print(json.dumps(dict(report_path=str(directory/'report.json'), status=report['status'],
                              cumulative=report['counts_after']), ensure_ascii=True))
    if report['counts_after'] is None:
        raise ValueError('T2_LEDGER_ACCOUNTING_UNCONFIRMED')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Entry/pre-send failures also keep concrete conditions and frames.
        failure = dict(schema_version='track2-dk1-entry-failure/1',
            actual_at=datetime.now(TZ).isoformat(), exception_type=type(exc).__name__,
            reason_code=str(exc) if type(exc) is ValueError and str(exc).replace('_','').isalnum()
                and len(str(exc)) <= 100 else 'DK_ENTRY_EXCEPTION',
            not_before=NOT_BEFORE.isoformat(), deadline=SOURCE_DEADLINE.isoformat(), head=source_head(ROOT),
            frames=[dict(file=Path(frame.filename).name, function=frame.name, line=frame.lineno)
                    for frame in traceback.extract_tb(exc.__traceback__)[-8:]])
        try:
            failure['cumulative_reservations'] = ProbeLedger(ARTIFACTS/'dk-probe-ledger.jsonl').summary()
        except Exception as budget_exc:
            failure['ledger_exception_type'] = type(budget_exc).__name__
        directory = ARTIFACTS/'dk-entry-failures'
        directory.mkdir(parents=True, exist_ok=True)
        path = directory/(datetime.now(TZ).strftime('%H%M%S')+'-'+uuid.uuid4().hex[:8]+'.json')
        write_json(path, failure)
        print(json.dumps(dict(entry_failure=str(path), exception_type=type(exc).__name__), ensure_ascii=True))
        raise
