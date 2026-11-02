from datetime import datetime
from zoneinfo import ZoneInfo

import httpx

from liangjian_funnel.contracts import CapabilityStatus
from liangjian_funnel.probes.models import ModelProbe
from liangjian_funnel.settings import Settings


def test_configured_roles_do_not_request_legacy_compatibility_routes(tmp_path):
    requested = []

    def handler(request):
        import json
        model = json.loads(request.content)["model"]
        requested.append(model)
        assert model == "deepseek-v4-pro-0813"
        return httpx.Response(200, json={"choices": [{"message": {
            "content": '{"ok":true}', "reasoning_content": "probe"
        }}]}, request=request)

    settings = Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "test-key"}, root=tmp_path)
    report = ModelProbe(settings, transport=httpx.MockTransport(handler)).run(
        now=datetime(2026, 11, 2, tzinfo=ZoneInfo("Asia/Shanghai")),
        models=settings.research_models,
    )
    assert report.overall_status is CapabilityStatus.PASS
    assert requested == list(settings.research_models)


def test_cli_probes_all_configured_roles_once_without_mutating_settings(tmp_path, monkeypatch):
    from liangjian_funnel import cli
    from liangjian_funnel.contracts import CapabilityReport

    settings = Settings.from_env({"LIANGJIAN_MODEL_API_KEY": "test-key"}, root=tmp_path)
    expected = tuple(dict.fromkeys((*settings.research_models, settings.review_model,
                                   settings.monitor_model, settings.team_intraday_model)))
    original = settings.model_dump()
    requested = []

    def probe(self, *, models):
        requested.extend(models)
        return CapabilityReport(provider="MODEL_GATEWAY",
                                generated_at=datetime.now(ZoneInfo(settings.timezone)),
                                overall_status=CapabilityStatus.PASS, checks=())

    monkeypatch.setattr(ModelProbe, "run", probe)
    assert cli.main(["probe-models"], settings=settings) == 0
    assert requested == list(expected)
    assert settings.model_dump() == original
