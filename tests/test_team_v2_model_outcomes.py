from contextlib import closing
from datetime import date,datetime
import json
from types import SimpleNamespace
import sqlite3

from liangjian_funnel.pipeline.local_fact_cache import canonical_json_hash
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal
from liangjian_funnel.runtime.team_v2_model_outcomes import refresh_model_outcomes
from test_team_v2_feedback import CAL,ASOF,capture


def fixture(tmp_path):
    directory=tmp_path/'state/team_v2';directory.mkdir(parents=True)
    journal=ModelJudgmentJournal(directory/'model.db')
    cache=tmp_path/'facts.db'
    with closing(sqlite3.connect(cache)) as db,db:
        db.execute('CREATE TABLE daily_bars(symbol TEXT,bar_timestamp TEXT,adjust TEXT,fetched_at TEXT,content_hash TEXT,payload_json TEXT)')
    settings=SimpleNamespace(root=tmp_path,fact_cache_db_path=cache)
    return settings,journal


def price(settings,day,close,*,fetched=None):
    payload={'close':close}
    with closing(sqlite3.connect(settings.fact_cache_db_path)) as db,db:
        db.execute('INSERT INTO daily_bars VALUES (?,?,?,?,?,?)',('000001.SZ',day+'T07:00:00+00:00','none',
            fetched or day+'T07:30:00+00:00',canonical_json_hash(payload),json.dumps(payload)))


def test_absent_owned_journal_is_noop_without_file_creation(tmp_path):
    result=refresh_model_outcomes(SimpleNamespace(root=tmp_path,fact_cache_db_path=tmp_path/'missing.db'),clock=lambda:ASOF,calendar=CAL)
    assert result['status']=='NOOP' and not (tmp_path/'state').exists()


def test_existing_daily_cache_only_to_owned_labels_source_bytes_unchanged(tmp_path):
    settings,journal=fixture(tmp_path);capture(journal,date(2026,9,1))
    price(settings,'2026-09-01',10);price(settings,'2026-09-02',11);price(settings,'2026-09-04',12)
    before=settings.fact_cache_db_path.read_bytes()
    result=refresh_model_outcomes(settings,clock=lambda:ASOF,calendar=CAL)
    assert result['status']=='COMPLETED' and result['written']==2
    assert before==settings.fact_cache_db_path.read_bytes() and result['network_used'] is False
    assert refresh_model_outcomes(settings,clock=lambda:ASOF,calendar=CAL)['written']==0
    with closing(journal._connect()) as db:
        values=[json.loads(r[0]) for r in db.execute('SELECT body_json FROM model_judgment_outcomes ORDER BY horizon')]
        assert [r['forward_return'] for r in values]==['0.1','0.2']
        assert db.execute('SELECT t1_return,t3_return FROM model_judgments').fetchone()==(None,None)
        assert not db.execute("SELECT name FROM sqlite_master WHERE name='execution_plans'").fetchall()


def test_missing_target_stays_unlabeled_and_has_no_provider_fallback(tmp_path):
    settings,journal=fixture(tmp_path);capture(journal,date(2026,9,1));price(settings,'2026-09-01',10)
    result=refresh_model_outcomes(settings,clock=lambda:ASOF,calendar=CAL)
    assert result['status']=='DATA_LIMITED' and result['missing_price']==2 and result['written']==0


def test_later_t3_reuses_t1_original_reference_not_a_cache_revision(tmp_path):
    settings,journal=fixture(tmp_path);capture(journal,date(2026,9,1))
    price(settings,'2026-09-01',10);price(settings,'2026-09-02',11)
    early=datetime.fromisoformat('2026-09-02T18:00:00+08:00')
    assert refresh_model_outcomes(settings,clock=lambda:early,calendar=CAL)['written']==1
    price(settings,'2026-09-01',20,fetched='2026-09-25T07:30:00+00:00');price(settings,'2026-09-04',11)
    assert refresh_model_outcomes(settings,clock=lambda:ASOF,calendar=CAL)['written']==1
    with closing(journal._connect()) as db:
        body=json.loads(db.execute('SELECT body_json FROM model_judgment_outcomes WHERE horizon=3').fetchone()[0])
        assert body['forward_return']=='0.1' and body['reference']['row']['close']==10


def test_corrupt_source_cannot_generate_a_return_or_change_existing_record(tmp_path):
    settings,journal=fixture(tmp_path);capture(journal,date(2026,9,1))
    price(settings,'2026-09-01',10);price(settings,'2026-09-02',11)
    with closing(sqlite3.connect(settings.fact_cache_db_path)) as db,db:
        db.execute("UPDATE daily_bars SET payload_json='{\"close\":999}' WHERE bar_timestamp LIKE '2026-09-02%'")
    result=refresh_model_outcomes(settings,clock=lambda:ASOF,calendar=CAL)
    assert result['status']=='DATA_LIMITED' and result['written']==0 and result['invalid_evidence']>0


def test_before_close_or_weekend_does_not_write_labels(tmp_path):
    settings,journal=fixture(tmp_path);capture(journal,date(2026,9,1))
    for stamp in ('2026-09-25T10:00:00+08:00','2026-09-26T16:00:00+08:00'):
        result=refresh_model_outcomes(settings,clock=lambda:datetime.fromisoformat(stamp),calendar=CAL)
        assert result['status']=='NOOP' and result['reason_code']=='MODEL_LABEL_WINDOW_CLOSED'
