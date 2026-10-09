# WP5 热修：范围基线与隔离分支

本文件保留 19:14 的第一轮基线。后续 64 只原缓存复算、提前范围封存与周末入口修正见 `WP5_SCOPE_RECONCILIATION_2026-10-09.md`；“今晚恢复”已经放弃，不应据本文件的旧下一步执行恢复。

按更新后的任务书第 8.1 节执行，原任务书 SHA256 为 `4ccd90e689470325b6247bf00adec48363639efdd79dee24ffc0c3410df332b3`。只做范围基线和热修开工，不执行独立授权的恢复或发布。分支 `codex/wp5-hotfix-close-scope`，工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，基于 WP0 本地提交 `7402e7fc570b7a91c5e19405308737715b0ad5c1`；没有合并 WP1。

## 1. 真实原因

通过既有 `audit_preopen_readonly.py` 扩展的 `--close-scope-day` 模式，只读查询原收盘窗口 15:10—16:40 的不可变公告缓存和 A1 代际，读取当日人气文件及 Node 日志。没有重新采集行情、扫描当前技术条件、连接模型或构造 WorkflowApplication。

2026-10-09 19:14:39 采集结果，生产为 `47f6ef03b042c0f9a207400c72d2ecffff338244`，地址 `192.168.1.254`、主机 `debian`：

| 原始证据 | 数量/结论 |
| --- | --- |
| 收盘公告同步日志 total | 1,825，只证明目标数量，不含完整股票集合 |
| 收盘开始前封存 A1 的 active pool | 1,749；代际 `a1-full-20261008T180015072172Z-fe5a5e7ecc11` |
| 收盘窗口内新增公告缓存涉及标的 | 1,774，只是已查询子集，不等于 1,825 完整范围 |
| 上述缓存标的中 A1 外股票 | 111，逐只记录已导出 |
| 111 只中命中当日人气文件 | 47；其余 64 只没有足够原始 EARLY_DISCOVERY/G0 来源证据，不武断归类 |
| 当日人气榜 A1 外股票 | 49；不得把另两只直接算入收盘选择 |
| 缓存内容哈希错误 | 0 |
| 当日完整/原始冻结快照 | 0 |
| 当日 close 恢复标记 | 不存在 |
| 18:00 A1 任务 | 截至该采集点日志只有启动，没有完成记录，不能声称自然结束 |

因此 `1825-1749=76` 是数量净差，不是已证实的新增股票集合。G0 会过滤并集；只有同时拿到原 selected 集合，才能准确计算 `added=selected-A1` 和 `excluded=A1-selected`。缓存时间窗口不含 run_id，还应保留“同一时段可能存在其他缓存生产者”的边界，不把缓存子集强称为原 selected。

证据：`artifacts/wp5-20261009/close-scope-baseline-20261009-v3.json`，SHA256 `b80520f3128331f304ef989f699aaaaffc399f34f7ff254329371b7cf2be3dfe`。全量 A1 清单、人气榜清单、窗口查询清单及 111 只逐股证据均在其中。完整 1,825 只逐股来源验收仍为 DATA_LIMITED；没有补造 76 只名单。

## 2. 代码修改

- `scripts/audit_preopen_readonly.py`：新增无行情探测的范围导出分支；SQLite 全部 `mode=ro`、事务只回滚；A1 只读取一个原始封存代际，流式计算文件哈希，输出文件排他创建。没有上传或安装脚本到生产。
- 首次探针退出 1，远端报告进程被杀；没有内核证据，不能宣称一定 OOM。旧探针读取全部历史代际 payload 的内存风险已消除，只查询一代。随后 v2 退出 0，但时间字符串混用 UTC/+08:00 导致缓存计数假 0；v3 改用 SQLite `julianday` 比较，退出 0。v2 错误结果保留，不能用于覆盖率结论。
- `pipeline/close_scope.py`：纯函数镜像现有 `A1 ∪ HOT100 ∪ EARLY_DISCOVERY` 与 G0 的交集，保留每只股票全部来源、原始集合、新增、排除、净增与范围哈希。没有执行权限、没有数量上限，也未接入生产研究路径。
- `tests/test_wp5_close_scope.py`：先运行缺模块反例，退出 1；随后覆盖“净增不等于新增”、重叠来源、G0 排除、超过 300 不截断、同集合不同来源哈希、空池不称就绪。

## 3. 测试与回放

```powershell
# 首次在 WP0 隔离目录执行探针；证据导出后才建立 WP5 分支
python scripts/audit_preopen_readonly.py --close-scope-day 2026-10-09 --output artifacts/wp5-20261009/close-scope-baseline-20261009-v3.json
# exit 0；读取生产，不写生产。

git worktree add -b codex/wp5-hotfix-close-scope D:/dev_A股/liangjian_wp5_hotfix_20261009 7402e7fc570b7a91c5e19405308737715b0ad5c1
# exit 0；导出脚本和证据复制到该分支，WP0 的脚本恢复为其已提交版本。

$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_close_scope.py -o addopts='' -q
# 实现前 exit 1；缺模块反例。

& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_wp5_close_scope.py tests/test_wp0_baseline_gate.py -o addopts='' -q --junitxml=artifacts/wp5-20261009/scope-tests.xml
# 实现后 exit 0；16 passed（相关切片，不是全量，不与其他测试数相加）。

git diff --check
# exit 0。
```

尚未运行 WP5 全量、200 只日线逐根对照或候选域重放。WP0 既有 2,345 项前后全量证据不能冒充本包全量。

## 4. 四层验收

- CODE：范围账本第一切片 16 项相关测试通过；热修任务 1、2 业务实现未完成。
- REPLAY：待证据；1,825 完整原始集合没有封存，不宣称 76 只逐股来源已全部核实；窗口查询子集已导出。
- OPERATIONS：只读基线取得；没有发布、重启或恢复，连续五日/60 分钟目标未验收。
- STRATEGY：不适用；没有修改策略阈值、模型审核或交易权限。

## 5. 未完成边界

当日未保存完整快照与范围集合，不能拿后来 A1 更新或日线缓存重新扫描来伪装当时的 EARLY_DISCOVERY。下一步在原始范围确定时封存来源账本，随后实施不依赖公告的确定性预筛和日线增量，沿用 fail-closed 证据门，不借最终 A2 结果裁剪公告范围。

夜间 22:00 公告任务、日线复权变化/新股/停牌处理及相关回放均未完成。WP5 任务 3、4 和其他未授权包未实施。决策 8（发布）与 9（今晚恢复）仍待用户分别授权；截至此基线点 A1 未证明结束且当日快照不存在，不满足恢复条件。

## 6. 下一步

继续 WP5 任务 1、2：先封存可穿透的原始范围，再用反例测试落地确定性预筛候选域和日线增量；不并发启动生产恢复或发布。
