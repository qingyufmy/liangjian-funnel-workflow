"""Bound only pure Team intraday HTTP; late workers cannot validate or trade."""
import time

from ..pipeline.model_client import ModelClientError
from .bounded_work import BoundedWorkGate

_HTTP = BoundedWorkGate(4)


class BoundedTeamClient:
    def __init__(self, client, *, deadline, monotonic=time.monotonic, gate=_HTTP):
        self.client = client
        self.deadline = deadline
        self.monotonic = monotonic
        self.gate = gate

    def complete(self, *args, **kwargs):
        # The frozen request's timeout is not rewritten. Both A4 and A6 keep
        # the same caller deadline and reserve local validation/commit time.
        end = min(self.deadline-2, self.monotonic()+kwargs['timeout_seconds'])
        def transport():
            try:
                return self.client.complete(*args, **kwargs), None
            except ModelClientError as exc:
                return None, exc
        work = self.gate.call(transport, deadline=end, clock=self.monotonic,
            name='team-intraday-http', operation_key=(kwargs.get('input_hash'), kwargs.get('team_session')))
        if work.status != 'READY':
            raise ModelClientError('WALL_CLOCK_TIMEOUT' if work.status == 'TIMED_OUT'
                else 'TEAM_HTTP_BACKPRESSURE' if work.status == 'BACKPRESSURE' else 'TEAM_HTTP_FAILED')
        response, error = work.value
        if error is not None:
            raise error
        return response
