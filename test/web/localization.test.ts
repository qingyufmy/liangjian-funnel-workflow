import { describe, expect, it } from "vitest";
import {
  codeLabel,
  displayValue,
  humanizeText,
  modelNameLabel,
  planPriorityText,
  stockSymbolLabel,
} from "../../web/src/localization";

describe("中文展示词典", () => {
  it("将内部状态和策略原因转换为中文", () => {
    expect(codeLabel("READY")).toBe("就绪");
    expect(codeLabel("NO_NEW_ENTRY")).toBe("暂不追高开仓");
    expect(humanizeText("A1_ACTIVE_REUSED; BEAR_RISK")).toBe("沿用本月有效研究池; 偏弱防守");
    expect(humanizeText("AI算力")).toBe("人工智能算力");
  });

  it("不会把未知内部枚举原样暴露给界面", () => {
    expect(humanizeText("SOME_NEW_INTERNAL_CODE")).toBe("系统内部状态");
    expect(displayValue({ route: "MARKET_CORE", marketRole: "TREND_CORE" }))
      .toBe("入池路线：市场核心；市场角色：趋势核心");
  });

  it("以中文名称显示模型、优先级和证券市场", () => {
    expect(modelNameLabel("deepseek-v4-pro-0813")).toBe("深度求索");
    expect(modelNameLabel("lane_2")).toBe("月之暗面");
    expect(planPriorityText("P3")).toBe("试探观察");
    expect(stockSymbolLabel("000001.SZ")).toBe("000001 · 深市");
  });
});
