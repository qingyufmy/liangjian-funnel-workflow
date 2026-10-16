# 既有 Tencent 分钟不能自动成为真实 VWAP：匹配口径问题

实际采集：2026-10-16 23:48 Asia/Shanghai。

源码 `f136dc84ba345d734c54628d94333d2d9195bb4f` 的 `team_v2_minute_projection.py` 已复用真实 MinuteBar / execution_evidence，32 项转换及关联反例退出 0，证据见同日 CONTROL_SOURCE_CHECKPOINT。

事实：`data/tencent_minute.py:250–264` 明文把 amount 标为 `ohlc_estimate`；MinuteBar 另有 `reported` / `legacy_unspecified`。A6 任务书 §12 承诺下一根 1m VWAP 撮合。若直接 amount/volume 而丢掉 amount_kind，会将 OHLC 估价伪装成真实成交额 VWAP。这不是模型可解决的缺项。

当前实现不猜：既有 reported MootDX 分钟可投影；estimated/unspecified 返回 `CONTROL_REPORTED_MINUTE_VWAP_UNPROVEN`，不会给纸面撮合填一个“精确 VWAP”。交易规则证明也独立待接，不因 reported 成交额就授予 tradability_known。

建议维持精确口径，优先复用既有 reported 源；源不足的撮合记 DATA_LIMITED，保留真实 OHLC 供技术/风险分析。请确认：若周末六日/周一自然源只有 Tencent estimated，是否另建明确标为 `OHLC_ESTIMATED_PAPER_FILL` 的研究撮合组（不得与严格 VWAP 成交/收益混合），还是严格账本保持缺样本？不自行用估算组顶发布门，不新增供应商请求或限流。该问题只阻塞 estimated 源的撮合/收益口径，不阻塞 reported 路径、监控接线、A6 异常等独立任务。
