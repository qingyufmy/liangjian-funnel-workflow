from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import hashlib
import json
import sqlite3

import pytest

from test_team_v2_monitor_source import prepared
from test_team_v2_minute_projection import source
from liangjian_funnel.data.tencent_minute import MarketQuote
from liangjian_funnel.runtime.team_v2_monitor_source import collect_monitor_packet
from liangjian_funnel.runtime.team_v2_quote_evidence import (
    REPLAY_BASIS,archive_monitor_quotes,assert_original_market_quote,replay_quote_from_closed_bar)
from liangjian_funnel.runtime.team_v2_store import canonical


def test_replay_quote_is_declared_close_at_original_bar_end_never_a_real_quote():
    bar=source();quote=replay_quote_from_closed_bar(bar,decision_as_of=bar.bar_end+timedelta(minutes=1))
    assert quote.last==Decimal(str(bar.close)) and quote.quote_time==bar.bar_end
    assert quote.quote_basis==quote.source_id==REPLAY_BASIS and quote.bid is quote.ask is None
    assert quote.original_bar_sha256==hashlib.sha256(bar.model_dump_json().encode()).hexdigest()
    with pytest.raises(ValueError,match='CONTROL_REPLAY_QUOTE_FORBIDDEN'):assert_original_market_quote(quote)
    with pytest.raises(ValueError,match='REPLAY_CLOSED_BAR_REQUIRED'):
        replay_quote_from_closed_bar(bar,decision_as_of=bar.bar_end)
    with pytest.raises(ValueError,match='REPLAY_QUOTE_CONTRACT_INVALID'):replace(quote,bid=10)
    with pytest.raises(ValueError,match='REPLAY_QUOTE_CONTRACT_INVALID'):replace(quote,quote_basis='ORIGINAL')


def test_replay_marker_survives_typed_production_serialization_guard():
    bar=source()
    # Even stripping a replay-only subtype's fields cannot erase its reserved
    # source identity and acquire production authority through MarketQuote.
    fake=MarketQuote(symbol=bar.symbol,quote_time=bar.bar_end,price=bar.close,open=bar.open,
        previous_close=bar.close,volume=bar.volume,amount=bar.amount,source_id=REPLAY_BASIS)
    roundtrip=MarketQuote.model_validate(fake.model_dump(mode='json'))
    with pytest.raises(ValueError,match='CONTROL_REPLAY_QUOTE_FORBIDDEN'):assert_original_market_quote(roundtrip)


def test_archive_keeps_every_original_quote_field_and_result_metadata_once(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    packet=collect_monitor_packet(settings,scope,**args)
    before=deepcopy(packet);receipt=archive_monitor_quotes(settings,packet,used_at=clock.now)
    assert receipt==archive_monitor_quotes(settings,packet,used_at=clock.now+timedelta(seconds=1))
    assert packet==before and len(calls)==2
    database=settings.root/'state/team_v2/market_quotes.sqlite3'
    with sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True) as db:
        rows=db.execute('SELECT body_json,body_sha256,quote_json,quote_sha256 FROM original_market_quotes').fetchall()
    assert len(rows)==1
    body,body_hash,quote,quote_hash=rows[0]
    assert json.loads(body)['quote_result']==packet['sources']['000001.SZ']['quote']
    assert json.loads(quote)==packet['sources']['000001.SZ']['quote']['quote']
    assert hashlib.sha256(body.encode()).hexdigest()==body_hash and hashlib.sha256(quote.encode()).hexdigest()==quote_hash
    with sqlite3.connect(database) as db:
        with pytest.raises(sqlite3.IntegrityError,match='QUOTE_ARCHIVE_IMMUTABLE'):
            db.execute('UPDATE original_market_quotes SET quote_json=?',('{}',))
        with pytest.raises(sqlite3.IntegrityError,match='QUOTE_ARCHIVE_IMMUTABLE'):
            db.execute('DELETE FROM original_market_quotes')


def test_failed_quote_stays_unavailable_not_a_fabricated_original(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path)
    def missing(*_):raise RuntimeError('PRIVATE_NOT_LOGGED')
    packet=collect_monitor_packet(settings,scope,**{**args,'fetch_quote':missing})
    archive_monitor_quotes(settings,packet,used_at=clock.now)
    with sqlite3.connect(settings.root/'state/team_v2/market_quotes.sqlite3') as db:
        raw=db.execute('SELECT body_json FROM original_market_quotes').fetchone()[0]
    assert json.loads(raw)['quote_basis']=='ORIGINAL_QUOTE_UNAVAILABLE' and 'PRIVATE' not in raw


def test_archive_rejects_tampered_packet_or_replay_before_creating_database(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);packet=collect_monitor_packet(settings,scope,**args)
    tampered=deepcopy(packet);tampered['sources']['000001.SZ']['quote']['quote']['price']=99
    with pytest.raises(ValueError,match='PACKET_BINDING'):archive_monitor_quotes(settings,tampered,used_at=clock.now)
    tampered['sources']['000001.SZ']['quote']['quote']['source_id']=REPLAY_BASIS
    tampered['packet_sha256']=hashlib.sha256(canonical({k:v for k,v in tampered.items() if k!='packet_sha256'}).encode()).hexdigest()
    with pytest.raises(ValueError,match='CONTROL_REPLAY_QUOTE_FORBIDDEN'):
        archive_monitor_quotes(settings,tampered,used_at=clock.now)
    assert not (settings.root/'state/team_v2/market_quotes.sqlite3').exists()


def test_existing_foreign_database_is_not_migrated_or_repaired(tmp_path):
    settings,clock,scope,calls,args=prepared(tmp_path);packet=collect_monitor_packet(settings,scope,**args)
    database=settings.root/'state/team_v2/market_quotes.sqlite3'
    with sqlite3.connect(database) as db:db.execute('CREATE TABLE execution_plans(id)')
    before=database.read_bytes()
    with pytest.raises(ValueError,match='ARCHIVE_SCOPE_INVALID'):
        archive_monitor_quotes(settings,packet,used_at=clock.now)
    assert database.read_bytes()==before
