"""Immutable business request/response evidence, separate from telemetry.

No endpoint, headers or credentials are recorded. The wire JSON includes the
complete rendered messages actually sent, including a JSON retry instruction.
The response file preserves received body bytes, including incomplete streams.
These are business originals, never disposable test diagnostics.
"""
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import uuid
from zoneinfo import ZoneInfo

import httpx


class ModelEvidenceError(RuntimeError):
    pass


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def _write(path, data):
    with path.open('xb') as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return dict(path=str(path), sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))


class ModelCallEvidence:
    def __init__(self, settings, *, model, stage, session, input_hash, prompt_hash):
        root = Path(settings.root).absolute()
        output = Path(settings.workflow_output_dir).absolute()
        if root.resolve() != root or output.resolve() != output or not output.is_relative_to(root):
            raise ModelEvidenceError('MODEL_EVIDENCE_SCOPE_INVALID')
        self.at = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat()
        self.directory = output / 'model_call_originals' / self.at[:10] / uuid.uuid4().hex
        if self.directory.resolve() != self.directory:
            raise ModelEvidenceError('MODEL_EVIDENCE_SCOPE_INVALID')
        self.directory.mkdir(parents=True, mode=0o700)
        self.metadata = dict(schema_version='model-call-originals/1', model=model, stage=stage,
            session=session, input_hash=input_hash, prompt_hash=prompt_hash, created_at=self.at)
        self.attempts = []

    def begin(self, *, attempt, body, timeout_seconds, clock):
        return ModelAttemptEvidence(self, attempt, body, timeout_seconds, clock)

    def receipt(self):
        return dict(**self.metadata, directory=str(self.directory), attempts=list(self.attempts))


class ModelAttemptEvidence:
    def __init__(self, owner, attempt, body, timeout_seconds, clock):
        self.owner, self.clock = owner, clock
        self.started = clock()
        self.phase = 'CONNECT_OR_RESPONSE_HEADERS'
        self.error = None
        self.status = None
        self.received = 0
        self.digest = hashlib.sha256()
        self.finished = False
        self.response_closed = False
        self.path = owner.directory / f'{attempt:03d}-response.body'
        # This durable record precedes client.stream and the paid dispatch.
        self.request = _write(owner.directory / f'{attempt:03d}-request.json', _json(dict(
            **owner.metadata, attempt=attempt, timeout_seconds=timeout_seconds, body=body)))
        self.handle = self.path.open('xb')
        self.attempt = attempt

    def attach(self, response):
        self.status = response.status_code
        self.phase = 'READ_RESPONSE_BODY'
        if response.is_stream_consumed:
            self.record(response.content)
            self.response_closed = response.is_closed
        else:
            response.stream = _EvidenceStream(response.stream, self)

    def record(self, chunk):
        self.handle.write(chunk)
        self.digest.update(chunk)
        self.received += len(chunk)

    def failed(self, exc):
        # Only classes and phase, never str(exc), URL, headers or a traceback.
        chain, seen = [], set()
        current = exc
        while current is not None and id(current) not in seen and len(chain) < 8:
            seen.add(id(current))
            chain.append(type(current).__name__)
            current = current.__cause__ or current.__context__
        self.error = dict(exception_class=type(exc).__name__, cause_classes=chain, phase=self.phase)

    def finish(self):
        if self.finished:
            return
        self.finished = True
        try:
            self.handle.flush()
            os.fsync(self.handle.fileno())
        finally:
            self.handle.close()
        value = dict(attempt=self.attempt, request=self.request,
            response=dict(path=str(self.path), sha256=self.digest.hexdigest(), bytes=self.received),
            http_status=self.status, elapsed_ms=max(0, round((self.clock()-self.started)*1000)),
            error=self.error, response_closed=self.response_closed)
        value['receipt'] = _write(self.owner.directory / f'{self.attempt:03d}-receipt.json', _json(value))
        self.owner.attempts.append(value)


class _EvidenceStream(httpx.SyncByteStream):
    def __init__(self, stream, evidence):
        self.stream, self.evidence = stream, evidence

    def __iter__(self):
        for chunk in self.stream:
            self.evidence.record(chunk)
            yield chunk

    def close(self):
        try:
            self.stream.close()
        finally:
            self.evidence.response_closed = True
