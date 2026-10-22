"""Content-free Finpoints streaming measurements for every model role."""
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import hashlib
import json
import os
import uuid

from .pm_stream_metrics import PMStreamMetrics


class ProviderStreamMetrics(PMStreamMetrics):
    def __init__(self, *, clock, started):
        super().__init__(clock=clock, started=started)
        self.dispatched = 0
        self.reasoning_chars = 0
        self.reported_usage = None

    def reasoning(self, value):
        if isinstance(value, str):
            self.reasoning_chars += len(value)

    def packet(self):
        # An upstream wrapper may already have consumed the complete body.
        # Parsing its cached lines cannot prove when a network packet arrived.
        if self.mode != 'BUFFERED_SSE_TTFT_UNOBSERVABLE':
            super().packet()

    def usage(self, payload):
        super().usage(payload)
        value = payload.get('usage') if isinstance(payload, dict) else None
        if not isinstance(value, dict):
            return
        allowed = {}
        for key in ('prompt_tokens', 'completion_tokens', 'total_tokens', 'input_tokens', 'output_tokens'):
            if type(value.get(key)) is int and value[key] >= 0:
                allowed[key] = value[key]
        for key in ('completion_tokens_details', 'output_tokens_details', 'prompt_tokens_details'):
            details = value.get(key)
            if isinstance(details, dict):
                selected = {k: details[k] for k in ('reasoning_tokens', 'cached_tokens')
                            if type(details.get(k)) is int and details[k] >= 0}
                if selected:
                    allowed[key] = selected
        if allowed:
            self.reported_usage = allowed

    def observation(self, *, status, reason_code=None):
        return {**self.snapshot(), 'schema_version': 'provider-stream-telemetry/1',
                'status': status, 'failure_code': reason_code,
                'thinking_disabled_requested': True, 'stream_requested': True,
                'actual_attempts': self.dispatched, 'provider_usage': self.reported_usage,
                'reasoning_chars_discarded': self.reasoning_chars,
                'reasoning_text_saved': False, 'business_content_saved': False}

    def persist(self, settings, *, model, stage, session, input_hash, prompt_hash, status, reason_code=None):
        value = self.observation(status=status, reason_code=reason_code)
        value.update(model=model, role=stage, session=session, input_hash=input_hash, prompt_hash=prompt_hash,
                     captured_at=datetime.now(ZoneInfo('Asia/Shanghai')).isoformat())
        root = Path(settings.root).resolve()
        output = Path(settings.workflow_output_dir).resolve()
        if not output.is_relative_to(root):
            raise ValueError('MODEL_STREAM_TELEMETRY_SCOPE_INVALID')
        directory = output / 'model_stream_telemetry' / value['captured_at'][:10]
        if directory.resolve() != directory:
            raise ValueError('MODEL_STREAM_TELEMETRY_SYMLINK_INVALID')
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = directory / (uuid.uuid4().hex + '.json')
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        return {**value, 'receipt_path': str(path), 'receipt_sha256': hashlib.sha256(raw).hexdigest()}
