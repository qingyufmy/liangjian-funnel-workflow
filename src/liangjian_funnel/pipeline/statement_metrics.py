"""Transparent optional metrics from same-period financial statements, not vendor indicator imputation."""
from collections.abc import Mapping, Sequence
import hashlib
import json
import math


def select_statement_periods(rows: Sequence[Mapping], limit: int = 4) -> list[Mapping]:
    ordered = sorted(rows, key=lambda r: (r.get('period_end_ms') or 0,r.get('report_date_ms') or 0),reverse=True)
    distinct = []
    seen = set()
    for row in ordered:
        identity = row.get('period_end_ms') or (row.get('fiscal_year'),row.get('fiscal_period'))
        if identity not in seen:
            distinct.append(row);seen.add(identity)
    kept = distinct[:limit]
    if kept and limit >= 2:
        current=kept[0]
        prior=next((r for r in distinct if isinstance(current.get('fiscal_year'),int)
                    and r.get('fiscal_year') == current['fiscal_year']-1
                    and r.get('fiscal_period') == current.get('fiscal_period')),None)
        if prior and prior not in kept:
            kept[-1]=prior
    return kept


def derive_statement_metrics(rows: Sequence[Mapping]) -> dict:
    def number(value):
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else None
    grouped = {}
    for row in rows:
        period = row.get('period_end_ms')
        dataset = row.get('_dataset')
        if period and dataset in {'INCOME', 'BALANCE', 'CASH_FLOW'}:
            key = (dataset, period)
            old = grouped.get(key)
            if old is None or (row.get('report_date_ms') or 0) > (old.get('report_date_ms') or 0):
                grouped[key] = row
    periods = sorted({p for d, p in grouped if d == 'INCOME'}, reverse=True)
    if not periods:
        return {'status': 'DATA_LIMITED', 'reason': 'INCOME_PERIOD_MISSING', 'metrics': {}}
    period = periods[0]
    selected = {d: grouped.get((d, period), {}) for d in ('INCOME', 'BALANCE', 'CASH_FLOW')}
    definitions = {
        'net_profit_margin_pct': ('INCOME', 'net_profit', 'INCOME', 'operating_income', 100),
        'debt_to_assets_pct': ('BALANCE', 'total_debt', 'BALANCE', 'assets_total', 100),
        'operating_cash_to_net_profit': ('CASH_FLOW', 'act_cash_flow_net', 'INCOME', 'net_profit', 1),
    }
    metrics = {}
    for key, (d1, k1, d2, k2, scale) in definitions.items():
        numerator, denominator = number(selected[d1].get(k1)), number(selected[d2].get(k2))
        valid = numerator is not None and denominator is not None and denominator > 0
        if d1 != d2 and selected[d1].get('period') != selected[d2].get('period'):
            valid = False
        metrics[key] = {'value': round(numerator / denominator * scale, 6) if valid else None,
                        'status': 'READY' if valid else 'DATA_LIMITED',
                        'formula': f'{d1}.{k1}/{d2}.{k2}*{scale}',
                        'numerator': numerator, 'denominator': denominator}
    return {'status': 'READY' if all(v['status'] == 'READY' for v in metrics.values()) else 'DATA_LIMITED',
            'source': 'LOCAL_DERIVED_SAME_PERIOD_STATEMENTS', 'period_end_ms': period,
            'period_basis': selected['INCOME'].get('period'),
            'input_hash': hashlib.sha256(json.dumps(selected, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest(),
            'replaces_vendor_indicators': False, 'metrics': metrics,
            'not_derived': ['weighted_roe', 'valuation_percentile', 'analyst_expectation_surprise']}
