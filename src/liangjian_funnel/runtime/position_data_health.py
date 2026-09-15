"""Separate position observation from new-entry history readiness."""
HISTORY_ONLY_ERRORS = frozenset({"TENCENT_INSUFFICIENT_BARS", "INSUFFICIENT_BARS", "MINUTE_DATA_GAP"})


def position_data_health(bar, *, at, reason=None, integrity_ok=True):
    current = bar is not None and bar.interval == "1m" and bar.bar_end == at
    trusted = integrity_ok and current and (not reason or reason in HISTORY_ONLY_ERRORS)
    return {"status": "READY" if trusted and not reason else "HARD_STOP_ONLY" if trusted else "UNOBSERVABLE",
            "current_price_trusted": bool(trusted), "multi_period_ready": bool(trusted and not reason),
            "reason_code": reason or ("OK" if trusted else "CURRENT_PRICE_UNAVAILABLE"),
            "entry_blocked": bool(reason or not trusted),
            "t_plus_one_unchanged": True}
