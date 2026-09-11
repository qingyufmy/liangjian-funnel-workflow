# 9月11日周度A1与统一发布接续记录

## 尚未完成，不得提前报成功

2026年9月11日18:25：修复代码已经提交推送，生产仍为`a20f15e432fe31e8aea96698f06659ea81ccd2cb`，尚未部署。周度全市场快照仍在采集公告；尚未生成新的正式A1、下周一A2/A3或新版A5。不要用隔离候选结果充当正式发布。

主诊断及修复依据：`docs/A1_A5_WEEKLY_DIAGNOSIS_2026-09-11.md`。最新功能提交`2fe5e7a`；后续文档提交也属于本次待发布主分支，以实际Git为准。368项联合回归通过；随后研究管线112项通过；表现行情补齐相关Python29项、服务端59项通过；类型检查通过。不合并宣称这些是互不重复的测试总数。

后续最新功能提交`25c92dc`：当前表现刷新任务完成状态与全历史资料完整性分开，当前到期全部就绪仍保留历史DATA_LIMITED；今日缺数据或源错误继续失败。最终完整Python回归1524项通过（`full-regression-release.xml`），服务端84项通过，均无失败。此后只有文档变化不重复完整回归。

## 正在执行的唯一关键采集

- 虚拟机：`aurum-vm`，192.168.31.254。
- 工作目录：`/www/wwwroot/Agu/liangjian-funnel-workflow`。
- Python进程43527（核对PID及命令，防止PID复用），父进程43526/43525。
- 命令：`.venv/bin/python -m liangjian_funnel prepare-snapshot`，以www运行，16:31开始。
- 原始日志：`outputs/runs/weekly-prepare-20260911.log`。CLI没有传递进度对象，因此完成前日志为空不代表停滞。
- 当前周五全市场5571只，研究资格4036只；周四/上周范围不能代替今日范围。今日日线4035只已同步，另有8只研究域外跟踪股票已补齐。
- 公告刷新可通过只读SQLite `storage/facts/market_fact_cache.sqlite3` 的 `cached_results` 查询`CNINFO_ANNOUNCEMENTS`最新`fetched_at`，不可因为日志暂空杀进程。近期查询缓存6小时，长周期报告7天，当前大量过期，等待重新取数是实际工作。

**18:00防重叠：**现有生产Node只具有进程内任务互斥，不能识别本次SSH手动采集。若18:00自然调度又启动无参数`run-a1-maintenance`，必须先核对其父进程为本项目Node、本次手动43527采集仍存在，或本次已启动的带明确`--mode incremental --snapshot-id`任务仍存在。仅取消这个新启动、重复且仍使用旧部署版本的自然A1子进程，保留原始取消记录；不得杀主Node、原采集、其他业务进程，不得将取消伪装成功。Node不会自动重试失败任务，只会重试BUSY跳过。若自然任务已经独立完成或无法确认重复，先分析证据，不盲目停止。

**此项已执行，不重复取消：**18:00自然子进程47501已核对父Node11692，于18:00:35仅取消该子进程；原43527保留。取消后仅将对应运行编号的悬挂进度从RUNNING终止为FAILED，前后证据见`a1-duplicate-progress-finalization-20260911.json`；没有创建新的A1版本。自动化`a`由本轮前台暂停，完成后仍须恢复工作日15:35。

## 已完成的实测

1. A4今日4个计划09:45全部入场前失效，56次实际判断可复算，0买卖信号、0成交。应判断56、实判断56、缺0；市场行情仍全日归档。旧A5错误将失效后的观察分钟算作漏执行。
2. 隔离A2 `2026-09-11-close-candidate-v8b`：827→32送审，5聚焦，2次有界模型调用通过。初次A3修正调用耗尽600秒，未发布。
3. 隔离A3 `2026-09-11-close-a3-candidate-v8c`：同一已验证A2，32只量化筛出12合格、18观察、2否决；一次176秒模型调用通过12计划，无生产发布。
4. T+N：本轮采集补齐原38只缺口中的30只。另8只被今日5000万元成交额门槛排除研究域，但并未停牌，使用新`evaluation/price_sync.py`在隔离源码中8次请求全部补齐。`outputs/runs/outcome-price-repair-8-20260911.json`保留采集证据。
5. `outputs/runs/outcomes-after-price-repair-20260911.json`已落盘：当前到期A2为1584/1584、A3为28/28，缺0；全历史561只仍有不同日期缺口，状态仍资料受限，不改历史判断，不称全部数据完整。该命令已经退出，无需再查旧执行会话。
6. 五家银行最新半年报在当前缓存、当前主营解析规则下都有页码证据：平安89/11/19页，浦发38页，招商43页，工商19/20页，中国银行135/136页。不能在新A1运行前断言已全部入池。

## 完成采集后的执行顺序

1. 从完整日志解析新快照编号及路径；核对哈希、日期、`selected_count == research_universe_count`。今日827只日度快照绝对不能用于周度A1维护。
2. 确认关键进程全部空闲，只执行原有`bash deploy.sh`统一发布。核验Git实际HEAD、非editable安装包与源码一致、3210健康、Node调度；不回显env秘密，不换模型，继续火山方舟deepseek-v4-pro。
   部署后先用新安装代码再次`prepare_snapshot`，传递明确进度对象，并复用热缓存重建完整快照。原长采集读入日线后，中微公司688012的临时日线缺口才补齐；不要沿用旧内存中的缺口，也不要修改旧快照。其余步骤全部使用这份新完整快照。
3. 先做只读新事实A1审计：`scripts/audit_a1_weekly.py --snapshot <全量快照路径> --output outputs/runs/a1-weekly-freshfacts-20260911.json`。这是用旧封存月度方向重算新事实，不能称新的模型A1。
4. 使用已经取得的全量快照显式周度维护，避免再次准备全市场：`.venv/bin/python -m liangjian_funnel run-a1-maintenance --mode incremental --snapshot-id <全量编号>`。保留运行日志。A1失败时保留旧封存池，不强制放宽或直接晋级；按具体冻结错误修复。
5. 用`audit_a1_weekly.py --baseline outputs/runs/a1-weekly-baseline-detailed-20260911.json --output outputs/runs/a1-weekly-after-20260911.json`核对新增/退出/保留，主营证据、半年报、银行及13个方向主归属与多主题关系。原782只中670半年报支撑、76未双增长、36缺同比；不能把所有旧池都说成半年报双增长。不得为200+凑数。
6. 最终A2/A3必须基于刚发布的新A1。CLI `run-research`默认不复用活跃A1，使用现有方法明确传参：`WorkflowApplication(settings).run_research('close', snapshot_id=<全量编号>, primary_only=True, schedule_comparison=False, from_active_a1=True, run_id_override='2026-09-11-close-weekly-final-<部署短版本>')`。这会正常生成待盘前复核计划，不调用历史回放模式。
7. `scripts/verify_day_plan_readiness.py --run-id <最终批次> --target-date 2026-09-14`全部断言通过；尤其A3⊆A2⊆当日有效A1、来源安装代码、唯一发布批次、520日线MACD及跨日前置预热。待9月14日盘前复核不等于已经允许买入。
8. 新版调度T+N命令为`run-outcomes-refresh`，独立补最近10交易日仍跟踪股票的当前行情，最多500只，有延期则明确记录。原`run-outcomes`/`label-outcomes`仍完全离线。执行并核对当前到期统计；全历史资料受限不能伪装完成。
9. 新版A5直接使用`WorkflowApplication(settings).run_a5_review('POST_CLOSE')`，防止旧调度完成记录导致NOOP。v8合同及提示词哈希应生成新的结果，原16:04:56报告保留。确认它仍归因今日实际Sep10上游计划，而非新Monday批次；应56/实56/缺0、交易信号0、计划失效4，通达信无独立重合行情不得改写为一致。检查实际飞书SENT。
10. 只读对比A4原事件、归档、持仓和成交与`outputs/audits/session-20260911/full-readonly-before-release.json`，确认不改原事件、不补发历史交易信号。再次核对磁盘、服务、下周一调度和待复核计划。

## 中文A1表格交付

本地已按Spreadsheets技能准备`outputs/session-20260911/build_a1_workbook.mjs`，尚未执行。已执行一次create操作标记，不重复调用；模板选择工具返回declined。Node依赖链接`outputs/session-20260911/node_modules`指向Codex bundled dependencies。

先复制三份真实审计到本地：after、baseline-detailed、freshfacts。使用bundled Node执行构建脚本，按这三个路径顺序传参。脚本拒绝沿用旧封存池，生成代码/名称/主方向/板块/新增保留/具体入选理由/半年报同比/主营/披露来源；另页列退出理由，未知理由必须准确补译，不能用“说明待补充”。

目标文件：`outputs/session-20260911/A1周度股票池_2026-09-11.xlsx`。必须检查实际渲染、公式计数与真实池数量、中文及前导零。最终用技能要求的文件citation交付一次，不交付构建脚本或假数据预览。保留用户无关未跟踪文件。临时隔离目录`/tmp/liangjian-review-20260911-hnRnDr`在确认无使用进程后可精确清理，不删除outputs审计。节点依赖junction只移除链接本身，不能递归删除其目标。

## 自动化收口

本任务heartbeat id为`a`。周度长采集期间临时接续，状态未变保持安静；完成统一发布、A1表格、正式A2/A3和新版A5验收后，恢复原工作日15:35，并删除仅9月11日的临时接续内容。不要新建重复自动化。最终报告明确已部署版本、新A1变化、下周计划状态及仍未解决的免费源限制。
