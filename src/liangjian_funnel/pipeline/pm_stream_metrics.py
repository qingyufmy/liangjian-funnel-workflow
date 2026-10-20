"""PM-only safe transport measurements; never response text or estimated usage."""
from collections.abc import Mapping
import math


class PMStreamMetrics:
    def __init__(self, *, clock, started):
        self.clock = clock
        self.started = started
        self.first_packet = None
        self.first_content = None
        self.output_tokens = None
        self.mode = 'NOT_OBSERVED'

    def packet(self):
        if self.first_packet is None:
            self.first_packet = self.clock()

    def streaming(self, *, observable=True):
        self.mode = 'SSE' if observable else 'BUFFERED_SSE_TTFT_UNOBSERVABLE'

    def json_response(self):
        self.mode = 'JSON_TTFT_UNOBSERVABLE'

    def content(self):
        if self.mode == 'SSE' and self.first_content is None:
            self.first_content = self.clock()

    def usage(self, payload):
        usage = payload.get('usage') if isinstance(payload, Mapping) else None
        if not isinstance(usage, Mapping):
            return
        for key in ('completion_tokens', 'output_tokens'):
            value = usage.get(key)
            if type(value) is int and value >= 0:
                self.output_tokens = value
                return

    def snapshot(self):
        ended = self.clock()
        generation_seconds = None if self.first_content is None else ended-self.first_content
        rate = (self.output_tokens/generation_seconds if self.output_tokens is not None
                and generation_seconds is not None and generation_seconds > 0 else None)
        return dict(schema_version='pm-stream-telemetry/1', mode=self.mode,
            elapsed_ms=max(0, round((ended-self.started)*1000)),
            first_packet_received=self.first_packet is not None,
            first_packet_ms=None if self.first_packet is None else max(0, round((self.first_packet-self.started)*1000)),
            ttft_ms=None if self.first_content is None else max(0, round((self.first_content-self.started)*1000)),
            output_tokens=self.output_tokens,
            token_usage_status='PROVIDER_REPORTED' if self.output_tokens is not None else 'NOT_REPORTED',
            tokens_per_second=round(rate, 3) if rate is not None else None,
            tokens_per_second_basis='PROVIDER_OUTPUT_TOKENS_OVER_OBSERVED_CONTENT_TO_COMPLETION' if rate is not None else 'NOT_OBSERVABLE')


def safe_pm_measurements(value):
    """Reject fabricated/unsafe diagnostic fields from alternate client objects."""
    if not isinstance(value, Mapping) or value.get('schema_version') != 'pm-stream-telemetry/1':
        return None
    modes = {'NOT_OBSERVED', 'SSE', 'BUFFERED_SSE_TTFT_UNOBSERVABLE', 'JSON_TTFT_UNOBSERVABLE'}
    if value.get('mode') not in modes or type(value.get('first_packet_received')) is not bool:
        return None
    fields = ('elapsed_ms', 'first_packet_ms', 'ttft_ms', 'output_tokens', 'tokens_per_second')
    for key in fields:
        number = value.get(key)
        if number is not None and (type(number) not in (int, float) or not math.isfinite(number) or number < 0):
            return None
    if value.get('output_tokens') is not None and type(value['output_tokens']) is not int:
        return None
    if value.get('token_usage_status') != ('PROVIDER_REPORTED' if value.get('output_tokens') is not None else 'NOT_REPORTED'):
        return None
    basis = value.get('tokens_per_second_basis')
    if basis not in {'NOT_OBSERVABLE', 'PROVIDER_OUTPUT_TOKENS_OVER_OBSERVED_CONTENT_TO_COMPLETION'}:
        return None
    if type(value.get('elapsed_ms')) is not int:
        return None
    if value['first_packet_received'] != (value.get('first_packet_ms') is not None):
        return None
    if value.get('ttft_ms') is not None and not value['first_packet_received']:
        return None
    if (value.get('tokens_per_second') is not None) != (basis != 'NOT_OBSERVABLE'):
        return None
    if value.get('tokens_per_second') is not None and (value.get('ttft_ms') is None or value.get('output_tokens') is None):
        return None
    if value['mode'] != 'SSE' and (value.get('ttft_ms') is not None or value.get('tokens_per_second') is not None):
        return None
    return {key: value.get(key) for key in ('schema_version', 'mode', 'first_packet_received', *fields,
        'token_usage_status', 'tokens_per_second_basis')}
