"""Pure scope bookkeeping. This module does not admit stocks or fetch facts."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable


def build_scope_ledger(*, a1_symbols: Iterable[str], hot_symbols: Iterable[str],
                       discovery_symbols: Iterable[str], g0_symbols: Iterable[str]) -> dict:
    """Mirror the existing union/intersection, retaining all source overlaps.

    Inputs must be the original selected sets, not lists reconstructed from
    later caches. The caller owns evidence validation and original timestamps.
    This receipt has no execution authority and applies no new size limit.
    """
    a1, hot, discovery, g0 = (set(value) for value in
        (a1_symbols, hot_symbols, discovery_symbols, g0_symbols))
    requested = a1 | hot | discovery
    selected = requested & g0
    sources = [('A1',a1), ('HOT100',hot), ('EARLY_DISCOVERY',discovery)]
    rows = [{'symbol':symbol,'sources':[name for name, members in sources if symbol in members]}
            for symbol in sorted(selected)]
    payload = {'schema_version':'close-scope-ledger/1',
        'execution_authority':False, 'status':'RECORDED' if selected else 'EMPTY',
        'source_sets':{name:sorted(members) for name,members in sources},
        'g0_symbols':sorted(g0), 'selected_symbols':sorted(selected),
        'selected_records':rows, 'added':[row for row in rows if row['symbol'] not in a1],
        'excluded_a1':sorted(a1-selected), 'excluded_requested':sorted(requested-selected),
        'counts':{'a1':len(a1),'selected':len(selected),'added':len(selected-a1),
                  'excluded_a1':len(a1-selected),'net_growth':len(selected)-len(a1)}}
    payload['scope_hash'] = hashlib.sha256(json.dumps(payload,ensure_ascii=False,
        sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return payload
