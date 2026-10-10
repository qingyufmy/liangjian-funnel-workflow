# W3：独立影子日报、累计和五日周报

本切片不调用模型、供应商或通知，不写正式计划/账户/生产任务槽。只消费显式冻结文件与独立影子 SQLite+JSONL。CODE 不能代替 REPLAY / OPERATIONS / STRATEGY。

## 输入与结果

`shadow_day_adapter.build_shadow_day` 校验 W2 只读视图 canonical hash、事件链、独立镜像、当日计划 census、有效期、完整变体计数与首次状态；原 A4 外层 action / first_cause / event_id / event_sha256 逐窗口对照。内层 warmup 判断不是原动作，不能作为此对照的另一端。空集合、不完整窗口不报告“0差异通过”。这些 hash 只绑定显式来源，不认证外部行情或调用方 census 的真实性。

收益按真实成交观察投影：FIRST_TRIGGER 单计划日去重，PENDING 不算 FILLED；T+1/3/5 必须已过交易日历对应收盘，并且 outcome.available_at 已到达。缺腿 null；正式均值、胜率只在完整且至少20成交样本时给出，观察均值单独注明。价格回报不含费用、不冒充账户盈亏。

可选 `--prices` 为 `shadow-validity-price-archive/1`，包含 trade_date/source_ref/bars/sha256。每根显式1m原价、来源/到达时钟和hash引用；逐分钟全有效期闭合窗口齐备后，未触发计划按 `max(high)/entry_zone_upper-1` 提交精确逐计划机会成本。缺一分钟或有效期未闭合为UNKNOWN，不以部分高点代替，不由均值反推sum。当前只是本地显式价格归档消费者，没有自动补行情。

既有 WP7 累计新增 REALTIME_SHADOW cohort。与 WP1_RESEARCH、REALTIME_PAPER_LEDGER 分开；V1–V6 分开；龙头与Flash不伪造0。WP7原20交易日窗口仍不改。新增五连续交易日 `shadow_week` 投影，缺日保持未知；每日first-trigger observations为周度真实值集合，不从均值×数量推算样本。五天采集不能自动证明正期望，也不产生参数批准。

## 独立入口

`scripts/build_shadow_daily.py --ledger <shadow.sqlite3> --mirror <signals.jsonl> --census <census.json> --production <outer-records.json> --as-of <aware-time> --output <new-dir> [--prices <price-archive.json>]`。

`scripts/build_shadow_week.py --report <daily-report.json> ... --session 2026-10-12 ... --session 2026-10-16 --as-of 2026-10-16T16:30:00+08:00 --output <new-dir>`。

两个入口显式绑定本 checkout，仅读输入、拒绝覆盖既有输出、写manifest和文件SHA。COMPLETE=0，缺证=2。没有默认发现生产库/日期，没有客户飞书。生产任务书15:30与“现有A5之后”存在时点冲突，现有A5为16:00，已提交桥接0040；尚未接调度。

## 实际测试

固定 cwd `D:/dev_A股/liangjian_wp5_hotfix_20261009`，`PYTHONPATH=src`，可执行 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。所有命令为 `-m pytest -q <tests> -o addopts='' --junitxml=<artifact>`，保留原生退出码。

- 6个累计反例原实现退出1；修改后48通过退出0：w3-accumulation-before/after.xml。
- 日投影原接口缺失退出1；后61通过。与W1真实字段形状、初始状态、有界有效期继续集成：w3-integration-before.xml 为3失败13通过，不隐藏。
- 实际T1未收盘反例1失败16通过退出1；修复后65通过退出0：w3-horizon-before/after.xml。
- 机会成本首次命令误用系统hermes Python（无pytest）退出1，**不是反例跑红**；显式项目解释器后71通过退出0：w3-opportunity-after-v1.xml。
- 独立SQLite+JSONL实际CLI发现mirror分叉仍COMPLETE：1失败1通过退出1；修复后73通过退出0，源文件前后SHA相同、重复输出拒绝：w3-daily-cli-before/after.xml。
- 五日周报原模块不存在退出1；新增后80通过退出0（含原累计/day/CLI）：w3-week-before/after-v1.xml。

这些是固定临时库/文件反例，不是自然交易日、不是旧六日全变体黄金。客户飞书零投递仍需独立账本审计，目前 NOT_AUDITED；没有后台任务或真实数据源自动接线。

## 四层与未完成

CODE：本地切片已可运行；完整HEAD回归与Claude评审另记录。REPLAY：真实旧计划缺影子字段，W1黄金未通过，不以零输入假通过。OPERATIONS：尚未发布、没有09:26原PIT采集/同源到达provider/安装目录适配/日报调度、没有自然≤5秒运行证据。STRATEGY：等待新交易日样本，正期望未建立，生产参数不变。
