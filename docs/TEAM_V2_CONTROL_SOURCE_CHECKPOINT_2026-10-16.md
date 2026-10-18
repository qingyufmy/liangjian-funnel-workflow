# 盘中协调层与外围保守路径检查点

实际采集：2026-10-16 23:48 Asia/Shanghai。

源码提交：`f136dc84ba345d734c54628d94333d2d9195bb4f`。这是隔离开发检查点，不是发布候选或生产接线完成声明。

## 本轮实现

- 真实 `LocalPaperAdapter` 先撮合已有委托，再执行独立的一分钟持仓硬保护，最后才在五分钟窗口内调用交易员；单股票失败不打断另一股票。采用原一分钟隔离，10:00 闭合数据在 10:01 消费，不倒签信号或成交。
- 调用前、模型返回后以及 broker 锁内提交前复核调用者同一个绝对截止；合法但迟到的模型结果保留原判断账本，不下新模拟单。重启保留原分钟账户视图、额度、模型请求；账户/模型/调用/PM 绑定变化不得复用旧分钟。
- 0237 外围缺失路径已接到真实晨会 engine 入口：只读既有 A1 缓存，核对缓存 hash、08:30 截止、逐字段时刻和 12 小时有效性；缺失/陈旧/源契约不齐将使用率区间下移 10pp。真实 A1 现有缓存不是完整隔夜指数源，不将缺字段补零。扩张阈值未定义，不自动加仓；问题已投 0238。
- `regime_live.yaml.route_gating` 写清现行 TREND/520 权限不变、RISK_OFF 使用 PROBE、新龙头/雷达仅 EMOTION_ON、持仓退出独立；核心配置拒绝另一份旧全关映射。
- 冻结分钟转换复用实际 `MinuteBar` 和 `execution_evidence`：全日到当前 cutoff 的窗口必须连续、去重、原始价和 shares 单位；拒绝 synthetic quote。只有 reported amount / volume 可称该分钟 VWAP，保留原源 hash。转换尚不证明当前交易规则；不会凭正成交量自动设置 tradability_known。

## 红绿证据

目录均为 `artifacts/team-v2-20261016/`。命令、stdout/stderr、各文件实际内容 SHA 在原 receipt；以下测试重叠，不相加，不是最终 Team 全量。

| 原件前缀 | 实际结果 | receipt SHA256 |
|---|---|---|
| 20261016T232727856378-external-conservative-source-green | 75 passed，退出 0 | e57d82f0352ccd39485bfdf144471a12fc2abf8a341525e717b73e0a1257f6a7 |
| 20261016T234255368082-control-minute-ownership-final | 73 passed，退出 0 | 72827618d2b633db73c3d6e5f2667f6c3c9a3c137b005a09ea5e6c49eaf6e401 |
| 20261016T234354215399-route-policy-0237-green | 75 passed，退出 0 | f24b5f722e8d8e8227a1090a76b9f0967bcd6438747022206e483dde775f8acf |
| 20261016T234618564675-frozen-minute-projection-qualified | 32 passed，退出 0 | 07b0beeb79358c03d9f40b577e500170a48059b4812815071d44a011e23e0a6f |

原红件保留：232638 原入口将外围缺失当中性；233203 为旧 API 不支持全局 deadline 的 TypeError（不是线上迟到下单事故证明）；234549 首轮转换有重复窗口异常未收敛、以及测试夹具使用不在既有 enum 的 qfq，两者区分记录。前者新增明确窗口完整检查，后者改为真实 enum/interval 反例，没有修改上游 contract。

## 尚未完成与下一步

1. 上述协调层尚未接实际 Workflow/CLI 的控制路径；不能在原 legacy lane 写单以后直接追加第二条交易员路径。先接现有只读来源和单一执行分流，保留 record 回退。
2. 0238 的规则复用确认仍待答；现有 stock_trading_rules 没有涨跌停计算，不能抄比例表绕过严格原件门。情绪四量和五日回放未完成。
3. Tencent amount_kind=ohlc_estimate 不是交易所上报成交额，不能悄悄称精确下一分钟 VWAP。继续优先使用既有 reported MootDX；其余不足如实 DATA_LIMITED。此匹配口径另投技术 question，不阻止其他独立工作。
4. 六日正式回放、三日真实模型干跑、VM 最小切片、A6 异常会话、雷达与最终全量/回退/封包/发布均未通过最终门。

四层：CODE 为上述关联本地绿；REPLAY 仅隔离合成反例，不是六日正式通过；OPERATIONS 未部署、未验自然分钟；STRATEGY 未证明收益。生产、客户通道、原计划、冻结原件、secret 均未改。H-1 仍按 0234 已批准的周日白天独立顺序执行，不提前发布。
