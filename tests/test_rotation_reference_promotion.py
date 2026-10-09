from datetime import datetime
import json
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import scripts.promote_rotation_reference as promotion


def fixture(tmp_path, monkeypatch):
    (tmp_path / 'config').mkdir()
    (tmp_path / 'config/rotation_reference_bindings_v1.json').write_text('[]', encoding='utf-8')
    (tmp_path / '.env').write_text('# unchanged\nSECRET=unit-private\nLIANGJIAN_ROTATION_MEMBERSHIP_SOURCE=EASTMONEY\n', encoding='utf-8')
    monkeypatch.setattr(promotion, 'load_rotation_theme_config', lambda _: SimpleNamespace(active=lambda _: [SimpleNamespace(theme_id='A')]))
    monkeypatch.setattr(promotion, 'load_rotation_references', lambda *a, **k: {'A': {'available': True, 'content_hash': 'hash'}})
    for key in ('LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE', 'LIANGJIAN_ROTATION_REFERENCE_DIR', 'LIANGJIAN_ROTATION_REFERENCE_BINDINGS_PATH'):
        monkeypatch.delenv(key, raising=False)
    return datetime(2026, 10, 9, 8, 0, tzinfo=ZoneInfo('Asia/Shanghai'))


def test_promotion_is_explicit_preserves_unrelated_keys_and_does_not_claim_prices(tmp_path, monkeypatch):
    now = fixture(tmp_path, monkeypatch)
    original = (tmp_path / '.env').read_bytes()
    preview = promotion.promote(tmp_path, tmp_path / 'refs', now=now)
    assert not preview['executed'] and (tmp_path / '.env').read_bytes() == original
    result = promotion.promote(tmp_path, tmp_path / 'refs', now=now, execute=True)
    assert result['executed'] and 'NOT_PROVEN' in result['daily_quote_flow_coverage']
    assert 'unit-private' not in json.dumps(result)
    assert '# unchanged\nSECRET=unit-private\n' in (tmp_path / '.env').read_text()
    assert (tmp_path / result['backup_name']).read_bytes() == original


def test_incomplete_mapping_or_trading_time_cannot_change_env(tmp_path, monkeypatch):
    now = fixture(tmp_path, monkeypatch)
    original = (tmp_path / '.env').read_bytes()
    with pytest.raises(ValueError, match='WINDOW_PROTECTED'):
        promotion.promote(tmp_path, tmp_path / 'refs', now=now.replace(hour=10), execute=True)
    monkeypatch.setattr(promotion, 'load_rotation_references', lambda *a, **k: {'A': {'available': False, 'reason_code': 'MISSING'}})
    with pytest.raises(ValueError, match='INCOMPLETE'):
        promotion.promote(tmp_path, tmp_path / 'refs', now=now, execute=True)
    assert (tmp_path / '.env').read_bytes() == original
