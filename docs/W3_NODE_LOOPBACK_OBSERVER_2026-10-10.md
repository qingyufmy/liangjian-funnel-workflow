# W3 Node loopback 原 DTO 接线（0062 路线A）

本切片完成 Python 独立采样器、原件 validator、reporting CLI/timer 接线。Node 路由由根代理独立实现/测试，不归本切片。部署、真实 HTTP 取样和自然 A5 联合观察均未执行；本地 fixtures 不是 OPERATIONS 或 STRATEGY 证据。原 W3 文档中的“Node原件自动来源未接线”是旧切片边界；本切片在 CODE 层补入口，不将该旧冻结回执冒充新接线验证。

## 入口与只读合同

`LoopbackNodeSampler(endpoint, archive_root, token_env_name='LIANGJIAN_DASHBOARD_TOKEN', clock=None, monotonic=None, transport=None)` 构造不发请求。`sample_if_due(observed_at=...)` 才在同日16:00–16:45进行一次 GET，每60秒至多一次；错过轮次不 catch-up、不重试。默认 reporting 的5秒 poll与HTTP60秒节流分开。真实请求端点固定 `/api/shadow/job-runs-readonly`，只接受显式 http literal `127.0.0.1` 或 `[::1]` 和 port，拒绝localhost、非loopback、userinfo/query/fragment/其它path；不解析DNS、不使用旧overview。

原接口字段：`{recentJobRuns:[原 JobRunRecord],node:{pid,startedAtEpochMs,observedAt}}`，startedAtEpochMs为 Node createApp 一次实际计算的原float，observedAt为本次响应真实 aware ISO。Python不重排/过滤原bytes、不伪造历史完成钟。JSON重复key/NaN、错类型/时钟、未来或跨日均不放行。字段名确立epoch-ms单位；本模块不能独立认证OS真实进程启动时刻，不由它猜精确历史钟。

默认 `HTTPConnection` 直接连接，不采用HTTP_PROXY/HTTPS_PROXY，不跟redirect。GET头只Accept JSON、Accept-Encoding identity及可选Bearer。Bearer只读指定env变量（实际现有Node配置来源 `LIANGJIAN_DASHBOARD_TOKEN`），命令参数只传env名字，不打印值/headers/原异常。单请求connect/read总接受预算<=2s，读取前按剩余时间设置原socket timeout；拒绝monotonic回退、迟到和wall-clock回退。冻结文件写入不是可硬抢占IO，不能宣称任意同步文件操作都<=2s。

200 JSON identity encoding才可 READY。整body上限1MiB，超出DATA_LIMITED，不将截断数据当原件，也不保留超上限整body；重定向/压缩/其它status不跟随。上限内实际返回body即便迟到/不合法也私有封存，不因拒绝资格丢掉原response证据。

## 原件与重启

每次请求前 O_EXCL 创建独立 `.request.json` reservation，再发HTTP；原 `.body` 与 `.sample.json` 分别保存exact body SHA和元数据SHA。Unix0600，拒绝symlink/覆盖，限定独立报告archive。request reservation不能宣称response完成；进程被杀后原槽保持DATA_LIMITED，重启不重复同60s槽。新构造从原日档案恢复节流/identity，重验meta与body hash；缓存复用也重验原bytes。

READY要求 request<=server observed<=received，实际day与16:45窗口一致。Node PID/start改变或start晚于目标16:00区间标 `NODE_RESTART_OR_HISTORY_UNPROVEN`，该day持续DATA_LIMITED；空 recentRuns不能证明“没完成”或“完成”。原bodySHA和loopback来源仍不证明网络身份认证，`source_authenticated=false`保持。

`verify_ready_sample(raw,sample,observed_at=...)` 重验原response与receipt，不能仅靠自报READY；原件新鲜度<=62s。新observer模式额外校验parent began不早于Node start、finishedAt不晚于原server observedAt，再执行原 `UNIQUE_NONOVERLAPPING_INTERVAL` ledger+approved JSON+stdout关联，不改变A5完成合同/DEGRADED语义或16:45兜底。旧纯归档API参数兼容，但不自动具备新endpoint身份验证证据。

## CLI 与模板

`run_shadow_reporting.py --node-endpoint <literal-loopback-url> --node-sample-archive <archive-root子目录> --node-token-env LIANGJIAN_DASHBOARD_TOKEN` 是显式开启网络取样的唯一新入口；缺配置仍不取网络。与 `--node-receipt`/fixture `--as-of` 互斥，失败不回退旧文件冒称新Node原件。node_sampling只输出狭义clock/node/hash/reason，不把原job body、模型文本或token进日报/prompt。取样异常不会重跑A4/A5或写生产DB。

独立 daily service 模板改为新endpoint入口，网络限制 `IPAddressDeny=any` / `IPAddressAllow=localhost` / AF_INET+AF_INET6，仅为本机取样。其它A1–A5调度/生产env、预开盘一次性timer/原session策略不改。env模板只有空token占位符；实际独立env权限/认证来源/单位语法/目标OS必须在决策14安装前核验，本切片没有安装或修改VM。

## 反例与四层状态

保留 `artifacts/w3-reporting-20261010/node-before.xml`（缺模块exit2）、node-wiring-before.xml（2fail exit1）、node-preserve-before.xml（2fail exit1）、node-reserve-before.xml（1fail exit1）。覆盖非literalIP/错path/proxy/redirect、原SHA/日期/重复key/NaN、客户端与Node重启、2s迟到、缓存篡改、无法封存零HTTP、被杀reservation、无旧文件fallback、A5原合同成功与缺证16:45兜底；全部transport为注入fixture或假HTTPConnection，不访问真实端口。

最终测试命令与精确文件SHA放在 `artifacts/w3-reporting-20261010/node-freeze.json`。CODE/局部REPLAY通过，不是全量qualified；OPERATIONS未部署/未真实HTTP，STRATEGY未证明，PIT/成交消费依旧UNWIRED，技术触发研究不报成交就绪。根代理负责统一全量、决策14与自然证据。
