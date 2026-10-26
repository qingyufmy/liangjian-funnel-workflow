from datetime import datetime,date,timedelta
import json,sqlite3
from pathlib import Path
import pytest
from liangjian_funnel.data.cache import MinuteBarStore
from liangjian_funnel.data.mootdx import MinuteBar
from liangjian_funnel.runtime.calendar import ExchangeTradingCalendar
from liangjian_funnel.runtime.history_freshness import session_grid
from liangjian_funnel.runtime.published_session_seed import read_published_session_seed,rowset_hash

AT=datetime.fromisoformat('2026-10-26T20:00:00+08:00')
def fixture(tmp_path):
    bars=tuple(MinuteBar(symbol='300817.SZ',interval='5m',bar_end=t,open=10,high=11,low=9,close=10,
        volume=10000,amount=100000,source_id='CONTROLLED_NATIVE_PROVIDER',volume_unit='shares',amount_kind='reported',
        normalizer_version='controlled-original-v1',provider_bar_end=t) for t in session_grid(date(2026,10,26)))
    store=MinuteBarStore(tmp_path);store.write(bars)
    return store,bars
def read(store,digest,**kwargs):
    return read_published_session_seed(store.path,'300817.SZ',target_trade_date=date(2026,10,27),
        calendar=ExchangeTradingCalendar(),as_of=AT,expected_rowset_sha256=digest,**kwargs)

def test_complete_original_versions_restore_exact_bound_rowset_without_db_write(tmp_path):
    store,original=fixture(tmp_path)
    assert rowset_hash(store.load_latest('300817.SZ','5m',limit=48))!=rowset_hash(original)
    before=store.path.read_bytes()
    bars,proof=read(store,rowset_hash(original))
    assert bars==original and proof['status']=='READY'
    assert proof['rowset_binding']=='IMMUTABLE_CAPTURED_VERSION_EXACT_HASH' and proof['restored_original_count']==48
    assert store.path.read_bytes()==before

def test_original_legacy_rowset_is_preserved_instead_of_relabelled(tmp_path):
    store,_=fixture(tmp_path);legacy=store.load_latest('300817.SZ','5m',limit=48)
    bars,proof=read(store,rowset_hash(legacy))
    assert bars==legacy and proof['rowset_binding']=='CANONICAL_CACHE_EXACT_HASH'
    assert all(b.volume_unit=='legacy_unspecified' for b in bars)

@pytest.mark.parametrize('change',['market','payload','missing','future','ambiguous','wrong_original_hash'])
def test_market_version_time_identity_and_original_hash_are_never_bypassed(tmp_path,change):
    store,original=fixture(tmp_path);digest=rowset_hash(original)
    with sqlite3.connect(store.path) as db:
        if change=='market':db.execute("UPDATE minute_bars SET close_value='10.1' WHERE bar_end=?",(original[0].bar_end.isoformat(),))
        elif change=='payload':db.execute("UPDATE minute_bar_versions SET payload_json='{}' WHERE bar_end=?",(original[0].bar_end.isoformat(),))
        elif change=='missing':db.execute('DELETE FROM minute_bar_versions WHERE bar_end=?',(original[0].bar_end.isoformat(),))
        elif change=='future':db.execute("UPDATE minute_bar_versions SET observed_at='2100-01-01T00:00:00+08:00'")
        elif change=='ambiguous':
            other=original[0].model_copy(update={'normalizer_version':'other-captured-version'})
            store._record_version(db,other,datetime.now().astimezone().isoformat())
    if change=='wrong_original_hash':digest='f'*64
    bars,proof=read(store,digest)
    assert not bars and proof['status']=='DATA_BLOCKED'

def test_non_native_and_missing_previous_session_still_reject(tmp_path):
    store,_=fixture(tmp_path)
    with sqlite3.connect(store.path) as db:db.execute('DELETE FROM minute_bars WHERE bar_end=(SELECT max(bar_end) FROM minute_bars)')
    bars,proof=read(store,'f'*64)
    assert not bars and proof['status']=='DATA_BLOCKED' and proof['available_5m_count']==47


def test_oversized_version_payload_cannot_be_materialized_as_an_original(tmp_path):
    store,original=fixture(tmp_path)
    with sqlite3.connect(store.path) as db:
        db.execute('UPDATE minute_bar_versions SET payload_json=? WHERE bar_end=?',
            ('x'*16385,original[0].bar_end.isoformat()))
    bars,proof=read(store,rowset_hash(original))
    assert not bars and proof['reason_code']=='HISTORICAL_5M_VERSION_PAYLOAD_BOUND'
