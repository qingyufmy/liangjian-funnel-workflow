# 量见 A 股独立漏斗工作流

这是一个独立于 `liangjian-astock-ai` 的 A 股 Shadow/内部模拟工作流。它从同花顺完整证券目录开始，经确定性研究质量门得到 G0，分别让三个研究模型执行 A1→A2→A3，再由 Flash 模型按 lane 每分钟做 A4 否决复核。所有结果写入本地 JSON/Markdown/SQLite，不连接 GM、券商、掘金模拟盘或真实账户。

## 已接通的流程

```text
同花顺当日完整证券目录（当前实测 5,562 只）+ mootdx 原始 1m/5m
  → 确定性 G0 门：成交额≥5000万、非 ST、价格/成交量/成交额有效，北交所合格标的保留为只研究
  → G0 按一级行业/细分产业节点均衡排序（不做 Top-N 裁剪）
  → 全市场日线/财务和正式事实本地落盘，冻结 A1 快照
  → 确定性市场情绪、K线形态、触发区/失效位/盈亏比
  → deepseek / kimi / glm 三个并行隔离 lane
  → 每 lane 严格 A1(全量 G0) → A2(仅 A1 ACTIVE) → A3(仅 A2 focus)
  → 仅在 A3 为上游子集按需读取 5m 长历史并计算技术因子
  → 收盘计划待次日早盘 tighten-only 复核
  → Flash 每分钟每非空 lane 一次批量 veto-only
  → 三个隔离虚拟账户（100 股整手、T+1、费用、滑点、幂等）
  → 研究与有效盯盘事件 Markdown
```

北交所只进入研究域，不进入模拟交易域。FAST_TRACK、外部委托和真实交易永久关闭。

## A1–A4 职责边界

- **A1 宏观/产业链/基本面**：G0 先按配置过滤日成交额低于 5000 万、ST、停牌/新股限制和价格/成交量/成交额无效标的；符合同一质量门的北交所标的保留研究，但不进入模拟交易。A1 AI 必须按“官方政策/宏观变化 → 结构性主题 → 产业链节点 → 公司主营与财务传导”选择 `ACTIVE`。单批 20 只只是模型传输边界，不是全局名额；已通过 G0 质量门的股票不得再因性能上限被静默裁剪。
- **A2 主题/情绪/市场角色**：只在 A1 `ACTIVE` 内按主题分批判断市场正在交易什么，先排产业链卡点层级，再评估板块宽度、资金、梯队、周期与龙头/中军辨识度。每个 `focus_pool` 标的都要给出卡住的环节、产业链位置、至少两条冻结证据、缺失证明和证伪条件；全量交易候选池目标为 100–200 只，但证据门槛优先，禁止为凑数放宽标准。
- **A3 技术设置/次日计划**：只在 A2 聚焦池内检查周线、日线、120m、15m 与 5m 确认，由确定性引擎回填触发区、失效位、止损距离和盈亏比，产出条件计划，不能越级新增标的。
- **A4 盘中信号复核**：对已有 A3 计划做分钟级确认；确定性触发先行，Flash 只有否决权，无权创建候选、放宽价位或提高风险单位。

因此，G0 的行业均衡只用于防止候选域被当日成交热点污染，绝不代表 A1 已完成选股。A1 数量少不应通过降低证据标准来“凑数”；正确处理是保留足够宽的 G0 输入，由 AI 严格执行政策/宏观传导，并把证据不足标的明确放入 `MONITOR` 或 `REJECT`，再由 A2/A3 完成市场主线与技术面收窄。

对应主观私募的分层带宽，本项目采用以下职责映射（均为容量目标，不是强制配额）：

| 项目层 | 机构池角色 | 目标带宽 | 是否进入下一层 |
|---|---|---:|---|
| 全市场目录 | P0 母池 | 当日完整目录，通常 5,000+ | 进入确定性质量门 |
| 质量过滤后 G0 | P1 可研究池 | 当前实测 3,886（含 70 只北交所） | 全部进入 A1 |
| G0 + A1 `ACTIVE/MONITOR` | P2 线索池 | 输入为完整 G0；有效线索目标 300–800 | 仅 A1 `ACTIVE` 下传 |
| A1 `ACTIVE` | P3 研究覆盖池 | 100–250 | 下传 A2 |
| A2 `focus_pool` | 当期主线/轮动与龙头中军候选 | 100–200 | 下传 A3 |
| A3 `core_watch_pool` | P8 日前正式计划池 | 5–10 | 可发布给 A4 |
| A3 `secondary_watch_pool` | P8.5 影子信号池 | 3–8 | 不发布订单，只留痕复盘 |
| A4 活动计划 | P9 盘中行动池 | 新候选 2–5；连同持仓重点处理通常不超过 8 | 只做条件确认 |

项目把机构 P4 深度研究和 P5 投资批准所需的主营证据、财务质量、证伪条件、主题传导、市场角色与风险否决分别嵌入 A1/A2 的强制字段和服务端校验，而不是另设可越级下单的名单。外部真实订单仍永久关闭；“0 个合格计划”始终是合法结果。

## 安装与配置

项目已经有独立虚拟环境：

```powershell
cd D:\dev_A股\liangjian_funnel_workflow
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
```

密钥只放在 `.env`。程序自动读取该文件，进程环境变量优先；日志和报告不保存密钥、认证头或模型思考正文。提示词默认读取项目根目录下的 `prompts`，也可用 `LIANGJIAN_PROMPT_DIR` 覆盖。模型思考模式按角色显式配置：研究模型默认开启，独立盯盘模型默认关闭，可分别通过 `LIANGJIAN_RESEARCH_THINKING_ENABLED` 和 `LIANGJIAN_MONITOR_THINKING_ENABLED` 调整。

漏斗参数默认读取项目内的 `config/funnel_config_v2.yaml`，也可用 `LIANGJIAN_SOURCE_CONFIG_PATH` 覆盖。资讯源读取 `config/news_sources.json`；该目录来自 Vibe-Research 的12赛道、106个公开RSS源，可用 `LIANGJIAN_NEWS_SOURCE_CONFIG_PATH` 覆盖。

月度宏观补充层默认启用固定版本 AKShare，缓存写入 `storage/facts/open_macro/`；可用 `LIANGJIAN_OPEN_MACRO_ENABLED=false` 关闭，或用 `LIANGJIAN_OPEN_MACRO_CACHE_DIR` 改变缓存目录。AKShare 仅作为公开接口适配和字段归一化层，事实仍保留原始端点、时间、质量等级和失败原因。A2 的产业链卡点框架吸收 `muxuuu/serenity-skill` 的 MIT 方法论，但不会导入其示例股票，所有公司必须来自本次 A1 池。

## 常用命令

```powershell
.\.venv\Scripts\python.exe -m liangjian_funnel doctor
.\.venv\Scripts\python.exe -m liangjian_funnel probe-all
.\.venv\Scripts\python.exe -m liangjian_funnel sync-data
.\.venv\Scripts\python.exe -m liangjian_funnel prepare-snapshot
.\.venv\Scripts\python.exe -m liangjian_funnel run-research --slot close
.\.venv\Scripts\python.exe -m liangjian_funnel run-research --slot morning
.\.venv\Scripts\python.exe -m liangjian_funnel monitor-once
.\.venv\Scripts\python.exe -m liangjian_funnel run-due
.\.venv\Scripts\python.exe -m liangjian_funnel run-morning
.\.venv\Scripts\python.exe -m liangjian_funnel run-close
.\.venv\Scripts\python.exe -m liangjian_funnel run-monitor
.\.venv\Scripts\python.exe -m liangjian_funnel status
```

历史收盘研究使用同一条 A1→A2→A3 主链和正式状态库，但不会把模拟账户交易日回退：

```bash
.venv/bin/python -m liangjian_funnel run-research --slot close --as-of 2026-08-25T15:10:00+08:00
```

正式流程从同花顺完整证券目录开始，先执行配置化质量门得到 G0，再按 881*/884* 行业节点均衡编排；节点内仅按流动性确定传输顺序，不执行 Top-N 截断。系统完整冻结 G0 的日线、基本面与正式证据；事实源失败的单票仍保留在 G0/A1 并写入失败清单。符合质量门的北交所标的保留在 G0/A1，但在 A3 计划发布前因 `trade_eligible=false` 失败关闭。个股资讯到 A2 才仅按 A1 `ACTIVE` 子集抓取；5 分钟长历史到 A3 才仅按 A2 `focus_pool` 读取和计算。
完整 A1 默认按产业节点每 20 只一批（`LIANGJIAN_A1_BATCH_SIZE`），A2 按 A1 主题每 40 只一批（`LIANGJIAN_A2_BATCH_SIZE`）并在合并后执行全局排名。模型阶段默认总时限 600 秒，输出预算按 `393216 → 262144 → 131072` tokens 三级降档；只有网关明确拒绝容量（或最终思考参数变体返回兼容网关的通用容量错误）时才降档。这些限制只影响模型投影；完整冻结快照与哈希不会被截断。

对已冻结快照重放或从已验证的上游阶段恢复：

```bash
.venv/bin/python scripts/replay_frozen_research.py --snapshot storage/snapshots/snapshot-....json --slot close --publish
.venv/bin/python scripts/replay_frozen_research.py --snapshot storage/snapshots/snapshot-....json --resume-audit outputs/research/research_..._lane_2.json --stage A3
```

重放会先校验快照 SHA-256，且只允许读取配置的快照/研究输出目录。

## 自动运行

已安装三个 Windows 计划任务：

- `LiangjianAStockResearchMorning`：每天 09:26；只冻结最新闭合行情，对前一收盘 A3 计划执行失效/追高门禁后原子激活，不再重跑完整三模型漏斗。
- `LiangjianAStockResearchClose`：每天 15:10。
- `LiangjianAStockMonitor`：每天 09:25 起每分钟一次，15:00 停止；内部用交易所日历跳过法定休市日、午休、重复任务和过期补买。15:10 收盘研究不再与盯盘任务竞争。

生产稳定模式设置 `LIANGJIAN_COMPARISON_ENABLED=false`。此时收盘研究只由主 lane 的
`deepseek-v4-pro-0813` 连续执行 A1→A2→A3，Kimi/GLM 不会在 Node 启动或主结果发布后
自动运行；它们仍保留在模型清单中，供显式离线对比使用。稳定模式不改变选股门槛、
A3 发布门禁或 A4 的“仅否决、不创设计划”边界。

重新安装或卸载：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\uninstall_scheduled_tasks.ps1
```

Linux、systemd/cron、容器、部署门禁和回滚步骤见 [DEPLOYMENT.md](DEPLOYMENT.md)。生产调度分别调用 `run-morning`、`run-close`、`run-monitor`，不会让三种任务在同一 `run-due` 进程中争抢执行权。

## 结果位置

- `outputs/research/*.md`：三模型研究汇总。
- `outputs/research/*_lane_*.json`：逐 lane 阶段、hash、血缘和阻断原因。
- `outputs/monitor/effective_signals.md`：只含有效事件，不含每分钟 `NO_ACTION`。
- `outputs/scheduler/*.log`：计划任务脱敏输出。
- `storage/snapshots/`：模型事实包；`raw/` 保存完整全市场和源数据冻结快照。
- `storage/facts/`：同花顺、巨潮、国务院政策与开放资讯的联合事实清单及逐文件 SHA-256 校验侧车；`ths_industry/` 保存每日完整行业成分缓存。
- `storage/facts/market_fact_cache.sqlite3`：WAL/FULL 本地事实库，保留日线与财务修订、数据同步水位、巨潮查询、PDF 证据卡与个股资讯缓存。
- `state/research_checkpoints/`：按模型/lane/阶段/快照/提示词/股票批次绑定的原子检查点；同日中断会恢复原冻结快照。
- `state/workflow_progress.json`：前端只读的脱敏进度摘要，包含数据、缓存、lane 和 A1–A3 批次进度。
- `storage/cninfo_pdfs/`：巨潮 PDF 的有界下载/解析工作目录；默认在页码证据卡和 PDF SHA-256 已落入事实库后删除可重新下载的原文，可用 `LIANGJIAN_CNINFO_PDF_RETAIN_RAW=true` 选择长期保留。模型始终只读带页码的短证据卡。
- `PHASE1_PHASE2_ACCEPTANCE_REPORT.md`：事实源和本地确定性聚合的中间验收证据。
- `state/workflow.sqlite3`：计划、信号、三个虚拟账户、持仓、成交和租约。

## 当前真实运行结论

数据链、冻结、并行模型编排和失败关闭已真实跑通。同花顺已补齐涨停/跌停/炸板、连板天梯、龙虎榜、热榜、集合竞价、行业/概念目录、行业当前成分关系及行业指数历史，以及利润表、资产负债表、现金流量表和财务指标；巨潮公开公告元数据与高价值 PDF 页码证据已按候选接入；国务院政策文件库按 90 日窗口接入正式文件，并区分确认零记录与查询失败。开放资讯包含财联社电报、东财7×24、逐候选个股新闻，以及Vibe-Research的12赛道/106个RSS源。AKShare 补充层已接入 PMI、CPI、PPI、M1/M2、社融、新增信贷、中美利率、股票/黄金/债券/现金 ETF 动量和国家统计局工业增加值行业数据；美元指数、美联储降息概率和海外市场先行指数在接口不可用时独立降级。每次运行都会先冻结来源 URL、发布/抓取时间、来源状态、内容哈希与缺失原因。同花顺是当前主行业口径，申万仅作为未来可选校验源。资讯统一是T3不可信线索，先去重、记录转载数并隔离疑似提示注入，不能替代政策、公告、财报或主营收入证据，也不能单独让 A1 标的进入 `ACTIVE`。行业利润、真实板块资金流和完整持仓拥挤等事实尚未接入时，快照会显式标记不可用，模型不得用新闻、常识或成交额代理补值。

2026-08-25 同一真实冻结快照 `snapshot-20260825T163244+0800-4d48ab813482` 的 v10 重放中，DeepSeek、Kimi、GLM 三个 lane 全部 `READY`，A1–A3 全部 `VALIDATED`。A1 对 20 只验收样本逐只完整分区：DeepSeek `0/20/0`、Kimi `7/13/0`、GLM `3/16/1`（`ACTIVE/MONITOR/REJECT`），不再受单批 5 只的全局截断。Kimi 最终保留 `002837.SZ` 为核心观察，但确定性大趋势门禁将其固定为 `NO_ENTRY`；GLM 无核心可执行计划，DeepSeek 因 A1 无证据合格 ACTIVE 而合法返回 `NO_ACTION`。当前没有模拟入场是正常的安全结果，不是 AI 链路故障。真实 A4→模拟买入→减仓/离场仍需等待自然产生的可执行计划样本。完整处置表见 [AUDIT_REMEDIATION_2026-08-25.md](AUDIT_REMEDIATION_2026-08-25.md)。

> 内部研究与模拟，不构成投资建议。

## Node 运行控制台

仓库根目录现在同时是可由宝塔管理的 Node 20+ 项目。Node 作为常驻控制面，负责固定时刻调度 Python 命令、聚合工作流状态、写入脱敏 JSONL 日志并提供响应式 Web 页面；A1–A4、交易日历、SQLite 租约和模拟账户仍由 Python 引擎负责。

```bash
npm ci
npm run typecheck
npm test
npm run build
npm start
```

默认只监听 `127.0.0.1:3210`。生产环境建议设置 `LIANGJIAN_DASHBOARD_TOKEN`，并通过宝塔 Nginx、IP 白名单或 VPN 访问。Node 调度启用后，不要再同时安装 Windows 计划任务、systemd timer 或 cron。完整参数见 [DEPLOYMENT.md](DEPLOYMENT.md) 和 [deploy/baota/README.md](deploy/baota/README.md)。
