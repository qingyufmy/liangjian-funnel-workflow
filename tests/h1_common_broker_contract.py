"""Backend-neutral H1 invariants; copied byte-identically into Team V2 tests.

Matching policies/fees need not be equal. Ledger atomicity, replay idempotence
and terminal protection must be equal. snapshot() projects cash/fills/inventory
and the authoritative lifecycle/order status, not mutable market marks.
"""
def assert_atomic_failure(attempt, snapshot, *, allowed_errors):
    before=snapshot()
    try:
        attempt()
    except Exception as exc:
        assert str(exc) in allowed_errors
    else:
        raise AssertionError('INJECTED_TRANSACTION_FAULT_NOT_REACHED')
    assert snapshot()==before, 'ORPHAN_FILL_OR_ACCOUNT_OR_TERMINAL_MUTATION'


def assert_replay_idempotent(attempt,snapshot):
    attempt()
    first=snapshot()
    assert first['fills'], 'POSITIVE_CONTROL_MUST_REACH_REAL_MATCHING'
    attempt()
    assert snapshot()==first, 'REPLAY_CHANGED_CASH_FILL_INVENTORY_OR_LIFECYCLE'


def assert_terminal_protected(attempt,snapshot,*,allowed_errors=()):
    before=snapshot()
    try:attempt()
    except Exception as exc:assert str(exc) in allowed_errors
    assert snapshot()==before, 'TERMINAL_REOPENED_OR_MONEY_REPLAYED'
