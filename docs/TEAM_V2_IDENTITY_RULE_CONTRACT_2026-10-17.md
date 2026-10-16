# 0247 身份与交易许可拆分

实际采集/实施时段：2026-10-17 01:27–01:39 Asia/Shanghai。授权：0247-reply，已全文读取；本轮未访问生产、未调用模型、未发通知、未 push/deploy。

## 已完成

- `DERIVED_IDENTITY_V1` 仅用于情绪统计。使用已核实的目录字段与全市场快照字段；沪深支持域、ST、退市、新股五交易日、无成交分别封存字段来源、as_of 和排除原因。北交所单列排除；分母是目录沪深 A 股，沿用 breadth 的 500 样本/75% 覆盖门。行情原 timestamp 必须当日盘中、非未来且不超过五分钟，不能用采集时钟替代。已有涨停的股票缺晨封前日连板证据时不猜 0。
- 舍入函数从现有 `shadow_price_limits` 数值核心提为 `round_price_limits`，strict wrapper 与情绪统计共用；普通比例直接引用 frozen `RATES`，没有另一套普通比例表。ST 主板 5% 是 0247 显式规定，仅作派生身份标签且 ST 不计情绪。冻结历史 `price_limits.py` 和 `shadow_rule_preflight.py` 的字节 pin、旧常量和 API 均未修改。
- CONTROL 默认不再调用严格影子 PIT。生产合同适配器沿用已发布原计划、当前闭合分钟、实时合法报价和既有显式涨跌停/锁板字段；没有新增身份或规则日期门。缺报价时仍把有效闭合分钟交持仓保护。较晚实时价触及涨停只能阻止新入场，不能倒灌为已闭合分钟涨停、影响原订单撮合。
- 新增明确授权的 `config/rule_revalidation_plan.yaml`，只开放 10-19 至 10-30 的三 URL 原正文复核。新增命名空间验证器复用原 archive pins、原正文解析器、原响应元数据合同；不修改历史 `PLANNED_DUE` / `RULE_REVIEWED_THROUGH`。有限配置不可自行扩大日期。原 receipts 和扩展 receipts 不冒称同一合同。
- 规则 DATA_LIMITED 不拦新开仓。检测到原正文变化则当日 BLOCK，封存原观察和内部 PENDING 告警；CONTROL 不再新开仓，持仓退出独立。单个源已证明正文变化时立即记录，不再等待后续源失败把它降成普通缺源。网络硬截止后迟到工人不能发后续请求或补写结果；同日 claim 不重复抓取。

## 现行生产合同源码位置

- `workflow.py:6824` `_publish_plans`，`6982` 起使用冻结 `TRADABILITY_FLAGS` 拒绝缺失/不可交易发布。
- `runtime/monitor.py:97` `MonitorEngine.process_minute`，`283` 起检查当前 1m，随后调用 `evaluate_strategy`。
- `runtime/strategies.py:2045` `_locked_limit_up`，读取既有显式上限和锁板字段，不引入新板块比例推测。
- `runtime/simulation.py:359` `PaperBroker.apply`，`415–445` 当前闭合窗口/成交量/可执行 bar，`569` T+1。
- Team 自有 Broker 的 T+1、现金、数量、次分钟和持仓风控仍独立执行。该适配不是完整生产逐分钟等价证明；交易员对七门的权限仍按既有 Team 任务书处理。

## 测试与原始回执

1. 情绪派生/原 strict/分类关联首次 162 通过；新生产适配首轮因 paper bar 字段误用 `bar_end` 而 6 失败，改为真实 `minute_end` 后相关 30 通过。原失败留存。
2. 新续期首轮 2 失败：误造不在原 RawResponse 合同中的 clock_basis，改用原合法枚举后相关 46 通过。原失败留存。
3. 汇总关联 231 通过/0 失败：`artifacts/team-v2-20261016/20261017T013650210945-identity-contract-renewal-0247-final-receipt.json`，SHA `f5357dd3f03c2cb0b30401cfd3cd584956c655db43da06b8631d43e61111c845`。
4. 最后增加“实时价不能重标闭合分钟”反例及对应改动，受影响 production adapter/真实 CONTROL 路径 14 通过/0 失败：`20261017T013745555411-production-limit-clock-0247-delta-receipt.json`，SHA `2d861032d6c02ec5da72a41d764777117b8d277db248c216b2ff3bf58a9b1a92`。与 231 有重叠，不相加，不称最终 HEAD 全量。

## 尚未完成 / 四层验收

CODE：以上组件及真实 CONTROL 默认路径通过隔离关联检查。规则告警仅 PENDING，尚未发送；A5 的规则注记展示还待接线。

REPLAY：当前为声明式反例与原 strict 回归，不是五日情绪开关回放，也不是六日交易团队回放。

OPERATIONS：未部署；目录/快照/晨封连板的实际采集 producer、G5 定时写入、内部告警投递还需接线和 VM 闲时最小验收。H1 仍按 0234 周日白天独立发布，不受此组件提交替代。

STRATEGY：未证明收益、真实触发率或策略自然有效。下一步先接真实四量 source 和 G5 状态，再 A6 异常/角色/A5 剩余接线及正式回放干跑。
