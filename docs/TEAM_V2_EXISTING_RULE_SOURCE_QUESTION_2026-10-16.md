# 0237 现有源补查：不能把接口缺项当已存在

实际采集：2026-10-16 23:26 Asia/Shanghai。

1. `runtime/stock_trading_rules.py:8–47` 目前只有手数/数量上限/tick，不含涨跌停比例或定价函数，且不支持北交所。真正既有定价逻辑在 `evaluation/ablation/price_limits.py` 和 `runtime/shadow_price_limits.py`；后者明确 shadow-only，并依赖实际规则原件回执，不能绕过其 reviewed-through/同日确认硬门直接当生产规则。建议复用这套严格定价的公共核心、保留规则确认入口（非 ST、上市≥5日域）；缺正式规则/身份的股票不计为已覆盖，不复制新比例表。请确认这属于你 0237 的“既有规则”实现口径，而不是给 stock_trading_rules 新抄一张比率表。

2. `data/open_macro.py:771–789` 的 GLOBAL_MACRO_SNAPSHOT 实际 required 只有 usd_momentum_percentile/fed_easing_probability_percentile/us_rate/cn_rate；`_collect_markets` 只给 US/KOREA/TAIWAN/JAPAN 的 20/60 日动量，不是隔夜纳指涨跌、A50、VIX 等七项。现有代码不能声称已有全套外围实时源。本轮先按已裁定路径实际读取并校验既有缓存，缺失/过期/缺字段下移 10pp，标 EXTERNAL_SNAPSHOT_MISSING，不采新源、不造字段值。

3. master §10 的“显著扩张上移10pp”没有量化条件；纳指≤−1.5% / A50≤−1%的收缩条件已有。建议缺扩张定义时仅实现已明确的收缩/缺失规则，不对正涨幅自行镜像出新阈值；请给出扩张的确定性定义或确认暂不加仓。

以上不阻塞 monitor 接线、现有源的保守路径和离线反例；不是请求 Tony 重复批准情绪阈值。
