"""Research-only judgment capture; never an execution or fill ledger."""
from datetime import datetime
from dataclasses import replace
import json
import sqlite3

import pytest

from liangjian_funnel.pipeline.model_client import ModelCallResult
from liangjian_funnel.pipeline.team_v2_contract import TeamValidation
from liangjian_funnel.pipeline.team_v2_judgments import ModelJudgmentJournal


def args(**changes):
    result = ModelCallResult("review-model", {"focus_decisions": [{"symbol": "000001.SZ"}]},
                             "p" * 64, "i" * 64, 250, 2, "disabled")
    return dict(call_id="run:A2:attempt1", run_id="run", role="A2", session="CLOSE",
        source_trade_date="2026-10-16", execution_trade_date="2026-10-19",
        captured_at=datetime.fromisoformat("2026-10-16T18:36:00+08:00"),
        catalog_hash="c" * 64, response=result,
        validation=TeamValidation(True, (), ({"path": "focus_decisions[0]", "symbol": "000001.SZ",
            "supporting_refs": ["M"], "opposing_refs": ["N"]},), 0), **changes)


def test_call_and_judgments_commit_together_and_replay_is_idempotent(tmp_path):
    path = tmp_path / "research.sqlite3"; journal = ModelJudgmentJournal(path)
    result = journal.capture(**args())
    assert result == {"call_id": "run:A2:attempt1", "judgment_count": 1, "reused": False}
    assert journal.capture(**args())["reused"] is True
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM model_calls").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM model_judgments").fetchone()[0] == 1
        row = db.execute("SELECT decision_id,symbol,t1_return,t3_return FROM model_judgments").fetchone()
        assert len(row[0]) == 64 and row[1:] == ("000001.SZ", None, None)
        assert not db.execute("SELECT name FROM sqlite_master WHERE name IN ('execution_plans','sim_fills','astock_outcome_labels')").fetchall()


def test_invalid_model_judgments_remain_visible_but_not_validated(tmp_path):
    journal = ModelJudgmentJournal(tmp_path / "research.sqlite3")
    data = args(); data["validation"] = TeamValidation(False, ("TEAM_SUPPORTING_REFS_INVALID:x",),
        ({"path": "focus_decisions[0]", "symbol": "000001.SZ", "supporting_refs": ["OUTSIDE"]},), 0)
    journal.capture(**data)
    with sqlite3.connect(journal.path) as db:
        row = db.execute("SELECT validated, judgment_json FROM model_judgments").fetchone()
        assert row[0] == 0 and json.loads(row[1])["supporting_refs"] == ["OUTSIDE"]
        assert json.loads(db.execute("SELECT issues_json FROM model_calls").fetchone()[0]) == list(data["validation"].issues)


def test_reused_call_id_with_changed_input_cannot_overwrite_evidence(tmp_path):
    journal = ModelJudgmentJournal(tmp_path / "research.sqlite3"); journal.capture(**args())
    data = args(); data["response"] = replace(data["response"],
        output={"injected": "different original output"}, output_hash="")
    with pytest.raises(ValueError, match="BINDING_CONFLICT"):
        journal.capture(**data)


def test_mid_transaction_failure_leaves_neither_call_nor_judgment(tmp_path):
    journal = ModelJudgmentJournal(tmp_path / "research.sqlite3")
    with sqlite3.connect(journal.path) as db:
        db.execute("CREATE TRIGGER fault BEFORE INSERT ON model_judgments BEGIN SELECT RAISE(ABORT,'local-fault'); END")
    with pytest.raises(sqlite3.IntegrityError): journal.capture(**args())
    with sqlite3.connect(journal.path) as db:
        assert db.execute("SELECT count(*) FROM model_calls").fetchone()[0] == 0


def test_summary_distinguishes_calls_provider_attempts_and_judgment_count(tmp_path):
    journal = ModelJudgmentJournal(tmp_path / "research.sqlite3")
    journal.capture(**args())
    data = args(); data["call_id"] = "run:A2:attempt2"
    data["validation"] = TeamValidation(True, (), (), 0); journal.capture(**data)
    assert journal.summary() == [{"role": "A2", "session": "CLOSE", "logical_calls": 2,
        "provider_attempts": 4, "latency_ms": 500, "valid_calls": 2, "judgments": 1}]


def test_naive_capture_time_and_forged_output_hash_fail_before_write(tmp_path):
    journal = ModelJudgmentJournal(tmp_path / "research.sqlite3")
    data = args(); data["captured_at"] = datetime(2026, 10, 16)
    with pytest.raises(ValueError, match="CAPTURE_TIME_INVALID"): journal.capture(**data)
    data = args(); object.__setattr__(data["response"], "output_hash", "forged")
    with pytest.raises(ValueError, match="OUTPUT_HASH_MISMATCH"): journal.capture(**data)
