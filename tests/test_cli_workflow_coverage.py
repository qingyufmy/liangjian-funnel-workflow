"""CLI dispatch coverage for the durable workflow commands.

The command tests assert the returned exit code and the arguments crossing the
CLI/application boundary.  No command here talks to a data provider or sends
an order.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import liangjian_funnel.cli as cli
from liangjian_funnel.pipeline.outcomes import project_run_status
from liangjian_funnel.settings import Settings
from liangjian_funnel.workflow import WorkflowError


def _settings(tmp_path: Path) -> Settings:
    return Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-test-key",
            "HITHINK_FINANCE_API_KEY": "hithink-test-key",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "legacy",
        },
        root=tmp_path,
    )


class _FakeApplication:
    calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
    response: dict[str, object] = {"status": "READY", "outcome_v2": {}}
    error: BaseException | None = None

    def __init__(self, _settings: Settings):
        type(self).calls = []

    def _invoke(self, name: str, *args: object, **kwargs: object) -> dict[str, object]:
        type(self).calls.append((name, args, kwargs))
        if type(self).error is not None:
            raise type(self).error
        return dict(type(self).response)

    def run_research(self, *args: object, **kwargs: object):
        return self._invoke("run_research", *args, **kwargs)

    def run_comparison(self, *args: object, **kwargs: object):
        return self._invoke("run_comparison", *args, **kwargs)

    def run_due(self, *args: object, **kwargs: object):
        return self._invoke("run_due", *args, **kwargs)

    def run_scheduled(self, *args: object, **kwargs: object):
        return self._invoke("run_scheduled", *args, **kwargs)


def _ready_response() -> dict[str, object]:
    outcome = project_run_status("READY", run_id="cli-run").as_dict()
    # The v3 wire shape intentionally keeps ``opportunity_state`` alongside
    # the richer stage-specific opportunity axes.  Add it explicitly here to
    # model the production payload passed to ``cli_exit_code``.
    outcome["opportunity_state"] = "PRESENT"
    return {"status": "READY", "outcome_v2": outcome}


def test_cli_primary_only_forwards_explicit_lane_contract_and_returns_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "WorkflowApplication", _FakeApplication)
    _FakeApplication.response = _ready_response()
    enabled_settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-test-key",
            "HITHINK_FINANCE_API_KEY": "hithink-test-key",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "legacy",
            "LIANGJIAN_COMPARISON_ENABLED": "true",
        },
        root=tmp_path,
    )
    result = cli.main(
        ["run-research", "--slot", "close", "--primary-only"],
        settings=enabled_settings,
    )
    assert result == 0
    name, args, kwargs = _FakeApplication.calls[0]
    assert name == "run_research"
    assert args == ("close",)
    assert kwargs["primary_only"] is True
    assert kwargs["schedule_comparison"] is True
    assert kwargs["historical_replay"] is False
    printed = json.loads(capsys.readouterr().out)
    assert printed["status"] == "READY"


def test_cli_primary_only_stable_mode_does_not_enqueue_comparison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "WorkflowApplication", _FakeApplication)
    _FakeApplication.response = _ready_response()
    settings = Settings.from_env(
        {
            "LIANGJIAN_MODEL_API_KEY": "model-test-key",
            "HITHINK_FINANCE_API_KEY": "hithink-test-key",
            "LIANGJIAN_RESEARCH_PIPELINE_MODE": "legacy",
            "LIANGJIAN_COMPARISON_ENABLED": "false",
        },
        root=tmp_path,
    )

    assert cli.main(["run-research", "--slot", "close", "--primary-only"], settings=settings) == 0
    _name, _args, kwargs = _FakeApplication.calls[0]
    assert kwargs["primary_only"] is True
    assert kwargs["schedule_comparison"] is False


def test_cli_historical_research_parses_as_of_and_snapshot_without_mutating_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "WorkflowApplication", _FakeApplication)
    _FakeApplication.response = _ready_response()
    result = cli.main(
        [
            "run-research",
            "--slot",
            "close",
            "--as-of",
            "2026-08-28T15:10:00+08:00",
            "--snapshot-id",
            "snapshot-20260828-test",
        ],
        settings=_settings(tmp_path),
    )
    assert result == 0
    _name, _args, kwargs = _FakeApplication.calls[0]
    assert kwargs["historical_replay"] is True
    assert kwargs["snapshot_id"] == "snapshot-20260828-test"
    assert kwargs["as_of"].isoformat() == "2026-08-28T15:10:00+08:00"


def test_cli_comparison_and_scheduled_dispatch_have_stable_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "WorkflowApplication", _FakeApplication)
    assert cli.main(["run-comparison", "--parent-run-id", "parent-1"], settings=_settings(tmp_path)) == 0
    name, args, kwargs = _FakeApplication.calls[0]
    assert (name, args, kwargs) == ("run_comparison", (), {"parent_run_id": "parent-1"})

    _FakeApplication.response = {"status": "READY", "dispatch": [{"status": "MISSED"}]}
    assert cli.main(["run-close"], settings=_settings(tmp_path)) == 2
    name, args, kwargs = _FakeApplication.calls[0]
    assert name == "run_scheduled"
    assert args and getattr(args[0], "value", args[0]) == "close_1510"
    assert kwargs == {}
    _FakeApplication.response = _ready_response()


def test_cli_feature_maintenance_is_a_separate_safe_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    calls: list[dict[str, object]] = []

    def maintain(settings, *, full, now):
        calls.append({"settings": settings, "full": full, "now": now})
        return {"status": "PUBLISHED", "generation_id": "g-maint"}

    import liangjian_funnel.pipeline.feature_maintenance as feature_maintenance

    monkeypatch.setattr(feature_maintenance, "run_feature_maintenance", maintain)
    result = cli.main(["maintain-features", "--full"], settings=_settings(tmp_path))
    assert result == 0
    assert calls and calls[0]["full"] is True
    assert json.loads(capsys.readouterr().out)["generation_id"] == "g-maint"


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (WorkflowError("CLI_BLOCKED"), 2),
        (KeyboardInterrupt(), 130),
        (ValueError("bad input"), 4),
        (RuntimeError("unexpected"), 3),
    ],
)
def test_cli_dispatch_errors_are_redacted_and_have_distinct_exit_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    error: BaseException,
    expected: int,
) -> None:
    _FakeApplication.error = error
    monkeypatch.setattr(cli, "WorkflowApplication", _FakeApplication)
    result = cli.main(["run-comparison"], settings=_settings(tmp_path))
    assert result == expected
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] in {"BLOCKED", "CANCELLED", "FAILED"}
    if isinstance(error, WorkflowError):
        assert payload["reason_code"] == "CLI_BLOCKED"
    else:
        assert "bad input" not in json.dumps(payload)
    _FakeApplication.error = None


def test_doctor_missing_installation_contract_fails_closed_without_network(tmp_path: Path, capsys) -> None:
    result = cli.main(["doctor"], settings=_settings(tmp_path))
    assert result == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["phase0_ready_for_live_probe"] is False
    assert payload["files"]
