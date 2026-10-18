"""0350: weekend capture is not the closed-market clock; no live transport."""
from copy import deepcopy
from datetime import datetime
import json

import pytest

from liangjian_funnel.pipeline.research import FrozenInputSnapshot
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar


@pytest.mark.parametrize('stage', ['A2', 'A3'])
def test_sunday_research_uses_friday_market_and_sunday_knowledge(tmp_path, stage):
    if stage == 'A2':
        from test_team_v2_a2_research import fixture
    else:
        from test_team_v2_research_adapter import actual_fixture as fixture
    app, args, seen = fixture(tmp_path)
    captured = datetime.fromisoformat('2026-10-18T12:27:27+08:00')
    market = '2026-10-16T15:10:00+08:00'
    data = deepcopy(dict(args['snapshot'].data))
    data['MARKET_DATA_AS_OF'] = market
    args['snapshot'] = FrozenInputSnapshot('sunday-original', data, as_of=captured)
    app.now = lambda: captured
    app.team_research_adapter.calendar = ExchangeTradingCalendar()

    result = app._run_stage(**args)

    assert result.status == 'VALIDATED', result.reason_codes
    assert len(seen) == 1
    runtime = json.loads(seen[0]['messages'][1]['content'].removeprefix('RUNTIME_INPUT\n'))
    envelope = runtime['required_envelope']
    assert envelope['source_trade_date'] == '2026-10-16'
    assert envelope['execution_trade_date'] == '2026-10-19'
    assert envelope['as_of'] == envelope['knowledge_cutoff'] == captured.isoformat()
    assert args['snapshot'].as_of == captured and dict(args['snapshot'].data) == data


@pytest.mark.parametrize('market', [None, 'invalid', '2026-10-16T14:59:00+08:00',
    '2026-10-15T15:10:00+08:00', '2026-10-19T15:10:00+08:00'])
def test_weekend_cannot_invent_a_missing_stale_unclosed_or_future_market_binding(market):
    from liangjian_funnel.pipeline.team_v2_research_time import research_times
    snapshot = FrozenInputSnapshot('original', {'MARKET_DATA_AS_OF': market},
        as_of=datetime.fromisoformat('2026-10-18T12:27:27+08:00'))
    with pytest.raises(ValueError, match='^TEAM_RESEARCH_CLOSED_SOURCE_REQUIRED$'):
        research_times(snapshot, ExchangeTradingCalendar())
