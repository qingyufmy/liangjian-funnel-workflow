import json
import os
import tracemalloc
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import BaseModel

import liangjian_funnel.reporting as reporting
from liangjian_funnel.redaction import sanitize


def test_streaming_json_matches_sanitized_pretty_json(tmp_path: Path) -> None:
    value = {
        "z": "中文",
        "authorization": "unit-secret-value",
        "items": [
            1,
            ("sk-abcdefghijk", {"api_key": "another-secret", "name": "保留"}),
        ],
        "date": datetime(2026, 8, 28, 9, 25, 0),
    }
    target = tmp_path / "nested.json"

    reporting.atomic_write_json_streaming(target, value)

    expected = json.dumps(
        sanitize(value),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        default=str,
    ) + "\n"
    assert target.read_text(encoding="utf-8") == expected


def test_streaming_json_redacts_keys_and_values(tmp_path: Path) -> None:
    target = tmp_path / "redacted.json"
    reporting.atomic_write_json_streaming(
        target,
        {
            "token": "do-not-persist",
            "nested": "bearer sk-abcdefghijk",
            "safe": "risk-safe",
        },
    )

    text = target.read_text(encoding="utf-8")
    assert "do-not-persist" not in text
    assert "sk-abcdefghijk" not in text
    assert '"token": "[REDACTED]"' in text
    assert "risk-safe" in text


def test_streaming_json_does_not_call_json_dumps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args, **_kwargs):
        raise AssertionError("streaming writer must not call json.dumps")

    monkeypatch.setattr(reporting.json, "dumps", fail)
    target = tmp_path / "large.json"
    reporting.atomic_write_json_streaming(target, {"rows": [{"value": i} for i in range(10_000)]})

    assert json.loads(target.read_text(encoding="utf-8"))["rows"][-1] == {"value": 9_999}


def test_streaming_json_keeps_row_wrapper_memory_bounded(tmp_path: Path) -> None:
    rows = [{"value": index, "text": "x" * 40} for index in range(20_000)]
    target = tmp_path / "bounded.json"

    tracemalloc.start()
    try:
        reporting.atomic_write_json_streaming(target, {"rows": rows})
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    # Source rows are allocated before tracing.  The writer should retain only
    # the active row wrapper, not one wrapper per completed row.
    assert peak < 5 * 1024 * 1024


def test_streaming_json_cleans_temporary_file_on_encoder_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = tmp_path / "failed.json"

    def broken_iterencode(_self, _value):
        yield "{"
        raise RuntimeError("encoder failed")

    monkeypatch.setattr(reporting.json.JSONEncoder, "iterencode", broken_iterencode)
    with pytest.raises(RuntimeError, match="encoder failed"):
        reporting.atomic_write_json_streaming(target, {"value": 1})

    assert not target.exists()
    assert list(tmp_path.glob(f".{target.name}.*.tmp")) == []


def test_streaming_json_supports_pydantic_model_and_file_mode(tmp_path: Path) -> None:
    class Report(BaseModel):
        name: str
        generated_at: datetime
        details: dict[str, str]

    target = tmp_path / "model.json"
    report = Report(
        name="测试",
        generated_at=datetime(2026, 8, 28, 9, 25, 0),
        details={"secret": "safe-value", "api_key": "hidden"},
    )

    reporting.atomic_write_json_streaming(target, report, mode=0o640)

    expected = json.dumps(
        sanitize(report.model_dump(mode="json")),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    assert target.read_text(encoding="utf-8") == expected
    if os.name != "nt":
        assert target.stat().st_mode & 0o777 == 0o640


def test_streaming_json_redacts_sensitive_pydantic_fields(tmp_path: Path) -> None:
    class SecretModel(BaseModel):
        token: str
        safe: str

    target = tmp_path / "model-secret.json"

    reporting.atomic_write_json_streaming(
        target,
        SecretModel(token="must-not-persist", safe="visible"),
    )

    payload = json.loads(target.read_text(encoding="utf-8"))
    assert payload == {"safe": "visible", "token": "[REDACTED]"}
