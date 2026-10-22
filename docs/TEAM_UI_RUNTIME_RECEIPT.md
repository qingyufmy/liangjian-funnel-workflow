# 只读 UI 运行证明合同

固定位置：`state/team_v2/ui_runtime_status.json`。只有发布验收 OPS 采集并写这一个最新小文件；UI API 不创建、更新或修补它，不调用业务对象、模型、行情或通知。缺失或绑定失败返回 `UNVERIFIED`，不拿默认配置冒充实际加载。

封装：`schema=team-ui-runtime-status/1`、`body`、`body_sha256`。正文 SHA 是 UTF-8 `json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)` 的 SHA256。

正文必需字段：

- `pid`：实际服务 Node 整数 PID。
- `process_start_ticks`：Linux `/proc/<pid>/stat` 第 22 字段，十进制字符串。Node 读取 `/proc/self/stat` 自行注入，和 PID 一起精确比较。`process_started_at` 可作人工展示，不以 JS uptime 派生近似 ISO 作身份门。
- `head`：实际项目 SHA1；读取当前 `.git/HEAD`、loose ref 或 packed ref 比较，支持 Git worktree。
- `observed_at`：带时区的真实采集时间，不得晚于当前时间；只用于当天运行展示，不是历史重建。
- `settings`：仅 `a4_trader_mode`、`broker_adapter`、`monitor_model`、`team_intraday_model`、`research_models`、`review_model`。从真实 Node 环境下实际 Settings 采集，不复制完整环境或 secret。
- `sources`：必须恰好包含 `.env`、`config/risk_limits.yaml`、`config/team_v2_deployment.yaml` 三个固定路径及其原字节 SHA256。任一文件变化使旧证明失效；UI 不返回文件正文。
- `risk_limits`：可选白名单 `max_new_entries`、`max_cluster_signals`、`min_cash_reserve_pct`、`max_position_pct`、`probe_max_position_pct`、`no_new_entry_after`。这是配置上限，不证明每一单已经按上限计算。

Node 的身份由只读 worker 调用方内部注入 `_runtime`，URL query 不参与。receipt 最大 16 KiB，配置源各最大 256 KiB；不启动 shell/git 进程，不超过既有两 worker/450ms 截止。

每笔实际仓位只投影 `paper_admissions.result_json.judgment.quantity_audit` 的原件：`raw_risk_qty`、`risk_round_lot_qty`、`cap20_qty`、`probe10_qty`、`final_qty`、`binding_limits` 等白名单。缺失就显示未记录，不能以 receipt 的当前配置回填。今天的旧 SELL 原件没有 BUY 数量计算明细，不补造。

Windows 本机无 `/proc` 时不伪造进程证明；冻结 QA 可以展示受核原件，但始终标为冻结、非实时，不构成 VM 已加载验收。
