# 量见 A 股独立漏斗工作流

这是一个独立于 `liangjian-astock-ai` 的 A 股 Shadow/内部模拟工作流。它从全市场证券域开始，经过确定性 G0 预筛，分别让三个研究模型执行 A1→A2→A3，再由 Flash 模型按 lane 每分钟做 A4 否决复核。所有结果写入本地 JSON/Markdown/SQLite，不连接 GM、券商、掘金模拟盘或真实账户。

## 已接通的流程

```text
同花顺全市场 5,559 只 + mootdx 原始 1m/5m
  → G0 与成交额确定性预筛
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
.\.venv\Scripts\python.exe -m liangjian_funnel prepare-snapshot --max-candidates 20
.\.venv\Scripts\python.exe -m liangjian_funnel run-research --slot close
.\.venv\Scripts\python.exe -m liangjian_funnel run-research --slot morning
.\.venv\Scripts\python.exe -m liangjian_funnel monitor-once
.\.venv\Scripts\python.exe -m liangjian_funnel run-due
.\.venv\Scripts\python.exe -m liangjian_funnel status
```

默认 G0 后取成交额最高的 20 只；初始域仍保存全市场计数和淘汰血缘。可用 `LIANGJIAN_RESEARCH_MAX_CANDIDATES` 调整到 1–300。

## 自动运行

已安装三个 Windows 计划任务：

- `LiangjianAStockResearchMorning`：每天 09:25；内部等到 09:26 冻结竞价数据。
- `LiangjianAStockResearchClose`：每天 15:10。
- `LiangjianAStockMonitor`：每天 09:25 起每分钟一次，持续 5 小时 45 分；内部跳过午休、周末、重复任务和过期补买。

重新安装或卸载：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\install_scheduled_tasks.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\uninstall_scheduled_tasks.ps1
```

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

数据链、冻结、并行模型编排和失败关闭已真实跑通。同花顺已补齐涨停/跌停/炸板、连板天梯、龙虎榜、热榜、集合竞价、行业/概念目录、行业当前成分关系，以及利润表、资产负债表、现金流量表和财务指标；巨潮公开公告元数据与高价值 PDF 页码证据已按候选接入；国务院政策文件库已接入近 7 日正式文件，并区分确认零记录与查询失败。开放资讯包含财联社电报、东财7×24、逐候选个股新闻，以及Vibe-Research的12赛道/106个RSS源。每次运行都会先冻结来源 URL、发布/抓取时间、来源状态、内容哈希与缺失原因。同花顺是当前主行业口径，申万仅作为未来可选校验源。资讯统一是T3不可信线索，先去重、记录转载数并隔离疑似提示注入，不能替代政策、公告、财报或主营收入证据。行业统计、完整资金流和完整持仓拥挤等事实缺失时，相关阶段继续fail-closed，不会用其他模型代答或绕过血缘。

> 内部研究与模拟，不构成投资建议。
