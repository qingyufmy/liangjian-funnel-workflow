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

  it("未知术语明确待补充，不伪装成有效技术解释", () => {
    expect(humanizeText("SOME_NEW_INTERNAL_CODE")).toBe("说明待补充（SOME_NEW_INTERNAL_CODE）");
    expect(displayValue({ route: "MARKET_CORE", marketRole: "TREND_CORE" }))
      .toBe("入池路线：市场核心；市场角色：趋势核心");
  });

  it("保留日线、分钟线与均价技术含义", () => {
    expect(humanizeText("价格进入A3日线MA5回踩区")).toBe("价格进入A3日线5日均线回踩区");
    expect(humanizeText("5分钟收回VWAP，MA20向上")).toBe("5分钟收回成交量加权均价，二十周期均线向上");
    expect(humanizeText("MACD和KDJ共振")).toBe("指数平滑异同移动平均指标和随机指标共振");
  });

  it("以中文名称显示模型、优先级和证券市场", () => {
    expect(modelNameLabel("deepseek-v4-pro-0813")).toBe("深度求索");
    expect(modelNameLabel("lane_2")).toBe("月之暗面");
    expect(planPriorityText("P3")).toBe("试探观察");
    expect(stockSymbolLabel("000001.SZ")).toBe("000001 · 深市");
  });

  it("将A4生命周期和入场前风险原因转换为中文", () => {
    expect(codeLabel("SIGNALLED")).toBe("已发出入场信号");
    expect(codeLabel("OPEN")).toBe("持仓中");
    expect(codeLabel("HARD_STOP_BEFORE_ENTRY")).toBe("入场前触及保护位");
    expect(codeLabel("CURRENT_1M_HARD_STOP")).toBe("当前一分钟触及保护位");
    expect(codeLabel("A4_BEHAVIOR_TYPE_MISSING")).toBe("股票类型尚未确定，不能选择盘中策略");
    expect(codeLabel("TREND_PRE_ENTRY_STRUCTURE_INVALIDATED")).toBe("趋势策略入场前结构失效");
    expect(codeLabel("TREND_5M_FAILED_MA5_RECLAIM")).toBe("趋势股连续跌破五日线参考且回抽失败");
    expect(humanizeText("MA520_PRE_ENTRY_STRUCTURE_INVALIDATED")).toBe("五日与二十日均线入场前结构失效");
    expect(codeLabel("LARK_NOTIFICATION_FAILED")).toBe("飞书消息发送失败");
  });
});
