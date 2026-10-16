# 实时市场/情绪开关批准接入回执

实际记录：2026-10-16 22:47 Asia/Shanghai。0234实际于22:37读取，文件written_at22:45不是实际读取时刻；0234b实际于22:40读取，按Tony22:38裁定生效。源码 `04637898e8786d9a5a45ab2c47d95e854062b430`，新增分类对照测试 `4656b691503b5474bd60d3095f5fbbc6ec329f8b`。

`config/regime_live.yaml` 已写 APPROVED/tony/22:38、enabled=true；只有整包正式发布后接入，未修改生产配置。第一层0.35/0.40沿用原值；情绪开40/35%/3/10、关20/50%/20/退潮、其他保持前态，均来自该yaml，不写死进分类代码。源缺字段或未证明排除ST/上市不足5日，返回DATA_BLOCK，不能拿昨日状态/报价涨幅猜四个情绪量。

实现两根相邻5m确认、午休/断档清除待确认计数、09:45 live权威、同日重复poll不计第二根；缺当前bucket数据立即关闭新LEADER许可但不动退出。`regime_observations` 与 `regime_transitions` 在独立Team库同事务，重复精确输入幂等、同tick换事实阻断；切换记录内部投递PENDING，不伪报已发Lark。故障注入中途插入失败整体回滚，恢复可重放。

提示词重投A6 1.0.3/README已逐字节复制并重封，SHA分别 `4a790958d04dce655bf5be2b8aef8c657664efb888a26bceb1c4f3806faf6199`、`4a1b2261b385dea6b84f04188a6114b6739536f87fa0bd1b34745e166a415aad`，其他六文件不变。只更新loader版本，不改角色正文。

## 原件

`artifacts/team-v2-20261016/20261016T224202242903-live-regime-initial-red` 是模块未实现的collection错误，不冒充业务红测。

`20261016T224430830169-live-regime-source-store-green`：137 passed、0 failed、exit0（分类/来源时钟/owned事务/提示词全8字节pin及受新A6版本影响的两会话关联集），receipt SHA `30a4f5905740b6a9447ffcbd168b1094d2b842b7fbdde8380d16c5c6850a10f0`。

`20261016T224615637598-live-regime-primary-parity`：1项测试内部42个合法输入组合，与原workflow分类逐项零差异；receipt SHA `32c8d6bddbead83023004e1a372d2ff4a84fde95cf1cb179f429b471e813e4d5`。不是五日真实回放，未采集新行情/调用真实模型。源码SHA绑定的提交前回执，不冒称最终HEAD全量。

## 0234要求的现有路线与来源能力

| 证据位置 | 实际含义 |
|---|---|
| `config/funnel_config_v2.yaml:615`、`:695` | 全局三个路线LEADER_INTRADAY/MA520_SWING/TREND_MA5；没有按regime逐profile的唯一开放表 |
| 同文件`:810`、`:825` | TREND/ROTATION调整池/持有天数/风险倍率；未单列关闭上述任一profile |
| 同文件`:840–844` | 老RISK_OFF覆盖声明NO_NEW_ENTRY、generate_buy_plans=false、buy_signal_allowed=false |
| `pipeline/a3_strategy.py:472`、`:860–877` | 实际A3把RISK_OFF当BEAR_RISK先验，MARKET_RISK_OFF_CONTEXT_ONLY不是硬否决 |
| `runtime/strategies.py:2267–2304` | READY当前盘中状态的BLOCK_NEW_ENTRY才拦新进，CAUTION保留信号/标低仓；不是三档regime硬映射 |
| `runtime/risk_engine.py:246`附近 | 新Team当前A6/引擎对RISK_OFF采用PROBE，遵守master§10；与老yaml“全关”不能混为一表 |

因此三档分类内核已闭环，正式路线接线仍需明确以当前引擎权限为准，不能仅凭旧yaml把PROBE改成全关。新LEADER/雷达按Tony情绪开关控制已明确，不另请Tony重复审批；剩余技术解释交Claude。

`runtime/live_market.py:32–100 classify_full_market` 现有输出只有涨跌平、breadth、median、coverage、旧PANIC/WEAK_ROTATION/NORMAL_REPAIR分类，没有涨停/跌停/炸板率/最高连板，没有ST/上市日排除证明。不能从此输出推导四个情绪数。需要正式源适配额外提供严格PIT池统计，否则emotion DATA_BLOCK；本候选尚未声称真实盘中取到。

CODE上述局部通过；REPLAY仅合成/分类原函数对照，10-12～10-16逐日切换与LEADER开放时段仍未开始；OPERATIONS未部署、当前源与Node/CLI待接线、内部投递未发；STRATEGY五日及收益未证。H1已最终ACCEPT，仅周日白天安装→部署→唯一内部探针，今晚不提前部署。
