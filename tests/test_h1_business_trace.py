import json
from liangjian_funnel.runtime.monitor_phase_trace import MonitorPhaseTrace


def test_receipt_distinguishes_timing_success_from_business_quarantine(tmp_path):
    trace=MonitorPhaseTrace(tmp_path,'h1-local')
    try:
        assert trace.state()['business_status_observed'] is False
        trace.note_business_reasons({'STATE_CONFLICT_QUARANTINED':1,'A4_TERMINAL_STATE_CONFLICT':2})
        trace.note_business_reasons({'STATE_CONFLICT_QUARANTINED':1})
        final=trace.finish()
        assert final['trace_status']=='FINALIZED'
        assert final['business_status_inferred'] is False
        assert final['business_status_observed'] is True
        assert final['business_reason_counts']=={'STATE_CONFLICT_QUARANTINED':1,'A4_TERMINAL_STATE_CONFLICT':2}
        assert json.loads((tmp_path/'h1-local.receipt.json').read_text())['business_reason_counts']==final['business_reason_counts']
    finally: trace.close()


def test_trace_never_persists_exception_strings_or_credentials(tmp_path):
    trace=MonitorPhaseTrace(tmp_path,'h1-local')
    try:
        trace.note_business_reasons({'https://secret.invalid/FAKE_TOKEN':1,'PERSISTENCE_FAILED':'FAKE_TOKEN','X':True})
        final=trace.finish()
        assert final['business_reason_counts']=={}
        assert 'FAKE_TOKEN' not in (tmp_path/'h1-local.receipt.json').read_text()
    finally: trace.close()


def test_quarantine_identity_is_hashed_stable_and_not_just_the_failure_count(tmp_path):
    trace=MonitorPhaseTrace(tmp_path,'h1-local')
    try:
        for identity in ('lane_1:600621.SH','lane_1:600621.SH','lane_1:600000.SH'):
            trace.note_business_reasons({'STATE_CONFLICT_QUARANTINED':1},incident_key=identity)
        result=trace.finish()
        assert len(result['business_incident_ids'])==2
        assert all(len(value)==64 for value in result['business_incident_ids'])
        assert '600621' not in json.dumps(result['business_incident_ids'])
    finally:trace.close()
