import json
import re
from collections import Counter
from pathlib import Path

from liangjian_funnel.pipeline.a3_display import A3_REASON_LABELS, a3_nonqualified_explanation
from liangjian_funnel.runtime.lark_notifications import _display_text


def test_python_web_lark_labels_are_identical():
    source = (Path(__file__).parents[1] / "shared/a3-display.ts").read_text(encoding="utf-8")
    labels = dict(re.findall(r'^  ([A-Z0-9_]+): "([^"]+)",', source, re.M))
    for code, expected in A3_REASON_LABELS.items():
        assert labels[code] == expected
        assert _display_text(code) == expected


def test_frozen_nonqualified_rows_do_not_use_background_as_blocker():
    root = Path(__file__).parents[1]
    fixture = json.loads((root / "test/fixtures/a3-20260907-display.json").read_text(encoding="utf-8"))
    rows = [r['item'] for r in fixture['rows'] if r['pool'] == 'rejected']
    before = json.dumps(rows)
    result = [a3_nonqualified_explanation(row) for row in rows]
    assert Counter(r['状态'] for r in result) == {'技术待观察': 16, '技术否决': 5, '证据待核': 2}
    assert all('盘中确认' not in r['主要原因'] and '背景偏弱' not in r['主要原因'] for r in result)
    assert json.dumps(rows) == before
