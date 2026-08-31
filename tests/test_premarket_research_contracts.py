from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_a1_premarket_contract_separates_fact_hypothesis_and_stale_quote() -> None:
    prompt = (ROOT / "prompts" / "agent_1_macro_chain_v2.txt").read_text(encoding="utf-8")
    assert "ORIGINAL_FACT" in prompt
    assert "TRANSMISSION_HYPOTHESIS" in prompt
    assert "MARKET_CONFIRMATION" in prompt
    assert "ACTION_HYPOTHESIS" in prompt
    assert "previous_close/as_of_previous_session" in prompt
    assert "复合稿中的个股名单、仓位建议和涨停预期不得进入" in prompt


def test_a2_premarket_contract_requires_full_lineage_and_lhb_provenance() -> None:
    prompt = (ROOT / "prompts" / "agent_2_theme_sentiment_v2.txt").read_text(encoding="utf-8")
    assert "盘前复合分析只允许作为验证清单" in prompt
    assert "A2_THEME_METRICS" in prompt
    assert "LHB_PROVENANCE_INCOMPLETE" in prompt
    assert "单日净买入不能证明中期机构建仓" in prompt
    assert "09:30 前的 A 股" in prompt


def test_a2_contract_allows_degraded_optional_facts_on_market_core() -> None:
    prompt = (ROOT / "prompts" / "agent_2_theme_sentiment_v2.txt").read_text(encoding="utf-8")
    assert "宽进规则" in prompt
    assert "MARKET_CORE hard route coverage" in prompt
    assert "overall factor_coverage、critical_factor_coverage 只用于诊断降级" in prompt
    assert "SUPPLY_CHAIN_ALPHA 的稀缺环节证据规则" in prompt


def test_a2_contract_defers_capacity_to_global_merge_and_separates_cooling_state() -> None:
    prompt = (ROOT / "prompts" / "agent_2_theme_sentiment_v2.txt").read_text(encoding="utf-8")
    assert "单个 transport batch 不得判断全局容量" in prompt
    assert "POOL_CAPACITY_FULL" in prompt
    assert "A2_GLOBAL_FOCUS_LIMIT" in prompt
    assert "eligible_routes 含 MARKET_CORE" in prompt
    assert "rejected_candidates 仅用于服务端硬事实" in prompt
    assert "COOLING 只能写入 weekly_momentum_state" in prompt
    assert '"theme_stage": "IGNITION|CONFIRMATION|ACCELERATION|CLIMAX|DIVERGENCE|RETREAT|REPAIR|FADE"' in prompt
