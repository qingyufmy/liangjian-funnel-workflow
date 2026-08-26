# 量见 A 股独立漏斗工作流

这是一个独立于 `liangjian-astock-ai` 的 A 股 Shadow/内部模拟工作流。它从全市场证券域开始，经过确定性 G0 预筛，分别让三个研究模型执行 A1→A2→A3，再由 Flash 模型按 lane 每分钟做 A4 否决复核。所有结果写入本地 JSON/Markdown/SQLite，不连接 GM、券商、掘金模拟盘或真实账户。

## 已接通的流程

```text
同花顺全市场 5,559 只 + mootdx 原始 1m/5m
  → G0 与跨一级行业均衡的细分产业节点预筛
  → 冻结日线/财务/技术因子、同花顺行业与市场事实、巨潮公告/PDF证据、国务院正式政策及开放资讯
  → 确定性市场情绪、K线形态、触发区/失效位/盈亏比
  → deepseek / kimi / glm 三个并行隔离 lane
  → 每 lane 严格 A1 → A2 → A3
  → 收盘计划待次日早盘 tighten-only 复核
  → Flash 每分钟每非空 lane 一次批量 veto-only
  → 三个隔离虚拟账户（100 股整手、T+1、费用、滑点、幂等）
  → 研究与有效盯盘事件 Markdown
```

北交所只进入研究域，不进入模拟交易域。FAST_TRACK、外部委托和真实交易永久关闭。

## A1–A4 职责边界

- **A1 宏观/产业链/基本面**：G0 只提供中性的完整可研究股票域；A1 AI 必须按“官方政策/宏观变化 → 结构性主题 → 产业链节点 → 公司主营与财务传导”选择 `ACTIVE`。它不做技术面入场判断；T3 资讯只能发现线索，不能单独证明主题或公司受益。单批 20 只只是模型传输边界，不是全局名额；每个输入必须恰好归入 `ACTIVE` / `MONITOR` / `REJECT` 之一。正式流程覆盖全部 G0 可交易股票，事实源失败逐票记录并关闭该票，不以性能上限裁剪股票池。
- **A2 主题/情绪/市场角色**：只在 A1 `ACTIVE` 内按主题分批判断市场正在交易什么，评估板块宽度、资金、梯队、周期与龙头/中军辨识度；全量交易候选池目标为 100–200 只，但证据门槛优先，禁止为凑数放宽标准。
- **A3 技术设置/次日计划**：只在 A2 聚焦池内检查周线、日线、120m、15m 与 5m 确认，由确定性引擎回填触发区、失效位、止损距离和盈亏比，产出条件计划，不能越级新增标的。
- **A4 盘中信号复核**：对已有 A3 计划做分钟级确认；确定性触发先行，Flash 只有否决权，无权创建候选、放宽价位或提高风险单位。

因此，G0 的行业均衡只用于防止候选域被当日成交热点污染，绝不代表 A1 已完成选股。A1 数量少不应通过降低证据标准来“凑数”；正确处理是保留足够宽的 G0 输入，由 AI 严格执行政策/宏观传导，并把证据不足标的明确放入 `MONITOR` 或 `REJECT`，再由 A2/A3 完成市场主线与技术面收窄。

对应主观私募的分层带宽，本项目采用以下职责映射（均为容量目标，不是强制配额）：

| 项目层 | 机构池角色 | 目标带宽 | 是否进入下一层 |
|---|---|---:|---|
| 全市场与合规门 | P0/P1 母池、合规可交易池 | 全市场 / 约 1,500–3,000 | 仅合规域可进入 G0 |
| G0 + A1 `ACTIVE/MONITOR` | P2 线索池 | 输入上限 1,000；有效线索目标 300–800 | 仅 A1 `ACTIVE` 下传 |
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

密钥只放在 `.env`。程序自动读取该文件，进程环境变量优先；日志和报告不保存密钥、认证头或模型思考正文。提示词默认读取项目根目录下的 `prompts`，也可用 `LIANGJIAN_PROMPT_DIR` 覆盖。

漏斗参数默认读取项目内的 `config/funnel_config_v2.yaml`，也可用 `LIANGJIAN_SOURCE_CONFIG_PATH` 覆盖。资讯源读取 `config/news_sources.json`；该目录来自 Vibe-Research 的12赛道、106个公开RSS源，可用 `LIANGJIAN_NEWS_SOURCE_CONFIG_PATH` 覆盖。

## 常用命令

```powershell
.\.venv\Scripts\python.exe -m liangjian_funnel doctor
.\.venv\Scripts\python.exe -m liangjian_funnel probe-all
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

正式流程从 G0 全市场可交易股票全集开始，先按同花顺 881* 一级行业均衡编排、再按 884* 细分节点轮询，节点内仅按流动性确定传输顺序，不执行 Top-N 截断。系统完整冻结全集的日线、基本面与正式证据；事实源失败的单票仍保留在 G0/A1，并把缺失原因写入候选失败清单，交由 Agent 按证据门槛分类，不会被性能上限静默裁剪。5 分钟技术因子允许缺失并留到 A3 处理，不能提前缩小 A1/A2。初始域仍保存全市场计数和淘汰血缘。
完整 A1 默认按产业节点每 20 只一批（`LIANGJIAN_A1_BATCH_SIZE`），A2 按 A1 主题每 40 只一批（`LIANGJIAN_A2_BATCH_SIZE`）并在合并后执行全局排名。模型阶段默认总时限 600 秒、最大输出 12,000 tokens。这些限制只影响模型投影；完整冻结快照与哈希不会被截断。

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
- `storage/cninfo_pdfs/`：按需下载的巨潮原始 PDF 与哈希侧车；模型只读取带页码的短证据卡，不读取整篇 PDF。
- `PHASE1_PHASE2_ACCEPTANCE_REPORT.md`：事实源和本地确定性聚合的中间验收证据。
- `state/workflow.sqlite3`：计划、信号、三个虚拟账户、持仓、成交和租约。

## 当前真实运行结论

数据链、冻结、并行模型编排和失败关闭已真实跑通。同花顺已补齐涨停/跌停/炸板、连板天梯、龙虎榜、热榜、集合竞价、行业/概念目录、行业当前成分关系及行业指数历史，以及利润表、资产负债表、现金流量表和财务指标；巨潮公开公告元数据与高价值 PDF 页码证据已按候选接入；国务院政策文件库按 90 日窗口接入正式文件，并区分确认零记录与查询失败。开放资讯包含财联社电报、东财7×24、逐候选个股新闻，以及Vibe-Research的12赛道/106个RSS源。每次运行都会先冻结来源 URL、发布/抓取时间、来源状态、内容哈希与缺失原因。同花顺是当前主行业口径，申万仅作为未来可选校验源。资讯统一是T3不可信线索，先去重、记录转载数并隔离疑似提示注入，不能替代政策、公告、财报或主营收入证据，也不能单独让 A1 标的进入 `ACTIVE`。GDP/CPI/PPI/PMI/社融信贷、行业利润、真实板块资金流和完整持仓拥挤等事实尚未接入时，快照会显式标记不可用，模型不得用新闻、常识或成交额代理补值。

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
