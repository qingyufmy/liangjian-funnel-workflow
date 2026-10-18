"""Risk admission and paper reservation share one serialized transaction."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
import sqlite3
import pytest

from test_team_v2_paper_ledger import broker, bar, drive
from liangjian_paper_broker.matching import PaperRules

DAY = "2026-10-19"


def binding(plan="p1"):
    return dict(plan_id=plan, symbol="000001.SZ", trade_date=DAY,
                cluster="same-cluster", snapshot_id="frozen-1000", policy="fixture")


def decide(snapshot):
    # Test cap of one is deliberately a fixture, not a production limit change.
    accepted = [row for row in snapshot["admissions"] if row["dispatched"]]
    if accepted:
        return dict(judgment={"status": "REJECTED", "reason": "FIXTURE_ONE_SLOT"}, order=None)
    return dict(judgment={"status": "APPROVED", "shares": 1000}, order=dict(
        side="BUY", qty=1000, order_type="LIMIT", limit_price="10",
        no_chase_price="10.2", tradable=True))


def test_concurrent_signals_cannot_both_consume_the_last_slot(tmp_path):
    path = tmp_path / "paper.db"
    first, second = broker(path), broker(path)
    barrier = Barrier(2)
    def run(item):
        n, ledger = item
        barrier.wait()
        return ledger.admit_signal(f"s{n}", binding=binding(f"p{n}"), decide=decide)
    with ThreadPoolExecutor(2) as pool:
        rows = list(pool.map(run, [(1, first), (2, second)]))
    assert sorted(row["dispatched"] for row in rows) == [False, True]
    assert len(first.get_orders(DAY)) == 1
    assert len(first.get_admissions(DAY)) == 2


def test_restart_retry_reuses_decision_without_callback_or_second_reservation(tmp_path):
    path = tmp_path / "paper.db"
    b = broker(path)
    row = b.admit_signal("s1", binding=binding(), decide=decide)
    before = b.snapshot_bytes()
    def forbidden(_):
        raise AssertionError("MUST_NOT_REEVALUATE_A_COMMITTED_SIGNAL")
    assert broker(path).admit_signal("s1", binding=binding(), decide=forbidden) == row
    assert b.snapshot_bytes() == before
    with pytest.raises(ValueError, match="ADMISSION_BINDING_CONFLICT"):
        b.admit_signal("s1", binding=binding("other"), decide=decide)


def test_admission_insert_fault_rolls_back_order_cash_reservation_and_event(tmp_path):
    b = broker(tmp_path / "paper.db")
    with sqlite3.connect(b.path) as c:
        c.execute("CREATE TRIGGER fault BEFORE INSERT ON paper_admissions BEGIN SELECT RAISE(ABORT,'LOCAL_ADMISSION_FAULT'); END")
    before = b.snapshot_bytes()
    with pytest.raises(sqlite3.IntegrityError, match="LOCAL_ADMISSION_FAULT"):
        b.admit_signal("s1", binding=binding(), decide=decide)
    assert b.snapshot_bytes() == before
    assert b.get_orders(DAY) == ()


def test_record_mode_never_places_order_or_reserves_slot(tmp_path):
    b = broker(tmp_path / "paper.db")
    row = b.admit_signal("s1", binding=binding(), decide=decide, mode="record")
    assert row["dispatched"] is False and row["order_id"] is None
    assert b.get_orders(DAY) == () and b.get_account()["frozen"] == "0"
    assert b.admit_signal("s2", binding=binding("p2"), decide=decide)["dispatched"] is True


def test_atomic_order_goes_through_real_matching_and_t1_ledger(tmp_path):
    b = broker(tmp_path / "paper.db")
    row = b.admit_signal("s1", binding=binding(), decide=decide)
    fills = drive(b, bar(), PaperRules(DAY))
    assert len(fills) == 1 and fills[0]["order_id"] == row["order_id"]
    assert b.get_positions()[0]["qty"] == 1000 and b.get_positions()[0]["sellable_qty"] == 0
    assert b.reconcile()["status"] == "MATCH"


def test_refused_order_does_not_commit_an_approval(tmp_path):
    b = broker(tmp_path / "paper.db")
    def unaffordable(snapshot):
        row = decide(snapshot)
        row["order"]["qty"] = 200000
        return row
    with pytest.raises(ValueError, match="INSUFFICIENT_CASH"):
        b.admit_signal("s1", binding=binding(), decide=unaffordable)
    assert b.get_admissions(DAY) == () and b.get_orders(DAY) == ()
