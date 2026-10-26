"""Run metadata follows proved market date, retaining the real capture clock."""
def runtime_trade_date(snapshot, *, generated_at, slot):
    if slot!='close':return generated_at.date().isoformat()
    from .closed_session_capture import KEY,validate_capture_proof
    proof=snapshot.data.get(KEY)
    if proof is None:return generated_at.date().isoformat()
    from .calendar import ExchangeTradingCalendar
    checked=validate_capture_proof(proof,captured_at=snapshot.as_of,calendar=ExchangeTradingCalendar())
    return checked['source_trade_date']
