"""Post-unit release acceptance; health inside a deploy unit is insufficient."""
import time


def verify_after_unit_exit(*, unit_active, observe, before, sleep=time.sleep,
                          monotonic=time.monotonic, wait_seconds=60, timeout=180):
    if wait_seconds<60:raise ValueError('RELEASE_SURVIVAL_WINDOW_TOO_SHORT')
    deadline=monotonic()+timeout
    while unit_active():
        if monotonic()>=deadline:raise ValueError('RELEASE_UNIT_NOT_FINISHED')
        sleep(1)
    exited=monotonic();first=observe();pid=first.get('node_pid')
    def check(value):
        if (not pid or value.get('node_pid')!=pid or value.get('health_status')!=200
                or value.get('business_hashes')!=before):
            raise ValueError('RELEASE_POST_UNIT_ACCEPTANCE_FAILED')
    check(first)
    while monotonic()-exited<wait_seconds:
        if unit_active():raise ValueError('RELEASE_UNIT_RESTARTED')
        sleep(min(5,wait_seconds-(monotonic()-exited)));check(observe())
    return dict(status='PASS',node_pid=pid,post_unit_elapsed_seconds=monotonic()-exited,
                business_hashes_unchanged=True)
