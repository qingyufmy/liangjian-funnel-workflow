# 另一台机器的核对和迁移步骤

## 1. 只取独立分支并核对

仓库：`https://github.com/qingyufmy/liangjian-funnel-workflow`；分支：`codex/dk-audit-export-20261105`。在已有remote确认相同归属后fetch该分支，使用新的审查worktree，不覆盖当前策略目录或带dirty工作区切换。

```
git fetch origin refs/heads/codex/dk-audit-export-20261105:refs/remotes/origin/codex/dk-audit-export-20261105
git worktree add ../dk-audit-review origin/codex/dk-audit-export-20261105
```

`exports/dk-audit-20261105`是唯一新增区域。按MANIFEST逐件SHA核对；GIT_BLOB文件还可用git hash-object核对应blob ID。原8d源码提交本身不必能从GitHub按SHA fetch：本分支携带了其必要文件的原字节，未把整份旧轨道二历史合并或推送。

## 2. 环境和依赖

运行时Python≥3.10（本机验证3.11），需要pydantic v2、PyYAML、exchange-calendars和时区库；pytest用于已有定向测试。`requirements-offline.txt`记录本次本机实际已装版本，不是声称2026-10-30原环境完全相同，也未在本轮安装/升级任何包。

只用ssl.py/bars15m.py及注入日历时主体为标准库。高层shadow/PIT/config需要pydantic/PyYAML；生产XSHG日历需exchange-calendars/tzdata。legacy dk_sources.parse_tencent会依赖原Tencent适配器及mootdx数据契约，故保留了最小相关源文件，requests/httpx/tdxpy仅为这条历史依赖链；纯SSL不创建它们的客户端。

`bootstrap.enable()`有意不执行原pipeline/data包facade。请在独立Python进程使用本目录入口；若外部liangjian_funnel已先导入，会拒绝混用，不偷偷替换现行项目模块。需要原生产包整体加载时，由接线方在其隔离环境补齐原项目依赖并验证，不把本导出的namespace包装接入生产。

## 3. 离线定向验证

在本开发机必须走既有项目维护wrapper；下面是相对目录内容，不要将D盘绝对路径误当Mac/Linux路径：

```
python exports/dk-audit-20261105/run_offline_tests.py --result /path/to/new-dk-result.json --junit /path/to/new-dk-junit.xml
```

另一台机器先服从其AGENTS/测试入口；runner只执行snapshot/tests中7个原文件及checks中3个包装边界检查，使用新的TemporaryDirectory，全部输入/配置固定。网络socket连接/DNS/bind均被拦，测试缓存/临时库位于本轮临时目录，退出清理。结果路径需由调用者明确指定到自己的测试证据目录。

本轮Windows实际命令见VALIDATION.json；结果114通过/0失败/0跳过。新增任何接线后不能借这个PASS冒新HEAD通过。原参考collect脚本包含真实HTTP代码，**本次不运行，迁移时也不能照旧任务书自动继承旧额度/批准**。

## 4. 最小离线例程：需要已有真实冻结数据

```
python exports/dk-audit-20261105/examples/replay_closed_bars.py --input /path/to/closed15m.json --sha256 ACTUAL_FILE_SHA256 --observed-at ACTUAL_OBSERVATION_WITH_TIMEZONE --period 20
```

这里的占位路径/时刻必须替换成已有原件的实际值，脚本不会下载或补造数据。输入为原`bar_identity(Bar15m)`编码的非空JSON数组，字段必须与Bar15m完整结构相等；Decimal用字符串，时间为带时区ISO串，expected_5m/present_5m为实际三根5m结束时间列表，缺量/额为null，reasons为列表。原decode_bar负责恢复类型。

SHA只证明文件字节绑定，**不是供应商身份/PIT真实性证明**；结果始终SHADOW_ONLY、enabled=false，并标CALLER_SUPPLIED_HASH_CHECKED_NOT_PROVIDER_ATTESTATION。要审计真实来源，应从合法腾讯原件经canonical_minute_input/build_dk_shadow走完整PIT链，不能把随手构造的Bar15m当真实行情。未闭合/缺口/混源/forward等原守卫继续拒绝。

本次没有提供新的真实行情样本或跑20日回放。例程的包装测试仅使用原受控fixture证明数据穿过CLI边界，不是模型、回测盈利或真实成交证明。

## 5. 接线前待核对

1. 选定自研SSL v0，不把它改称站点原公式。确定计划股/指数范围，显式注册源身份、复权和交易时段；按原规则处理除权、缺bar、未闭合和数据修订。
2. 分开核对：固定输入下因果性、真实20日预热后完整session评估、5个实际采集交易日影子、真实计划范围。历史短窗口和历史摘要不代替这些门。
3. 确认输出打算仅作研究事实/排序/参考还是金融硬门；目前全未接。A/B/C、MACD/KDJ、现有趋势/风控不因复制DK就改变。
4. 新的金融权限必须由新的A6、独立PM及风控CAS裁定；HALT、资金上限、止损与其他风控保持原规则。不得仅改enabled=true或以SHADOW_RECORDED视为准入。
5. 若需完整网络采集，参考reference的旧scripts/HTTP守卫只是溯源材料，需重新核当前依赖、授权和真实客户端零网络预检；本交付没有运行或批准任何采集/生产调度。
