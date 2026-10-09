# 免费板块参考数据落地与验收（2026-10-08）

## 结论与边界

独立采集、不可变本地版本、增量补采、严格跨源回退和27方向对账已落地。新源统一SHADOW，未接入生产选股、未改评分/阈值/候选数量、未推送或部署。本轮没有调用模型、改数据库、发送业务通知或启用真实下单。

不能宣布免费板块来源已经全部恢复：新浪49个行业全量尝试只有34个满足计数/分页契约；同花顺目录可取，但AI PC第二页401；通达信按用户确认应通过MCP接入，实际MCP工具契约和板块能力尚待核验。F10的6股逆向图只代表6股输入范围，不代表板块完整成员。此前将客户端安装目录当成通达信接入前置条件不正确；已实现的本地文件导入仅作为可选离线备份，不代表MCP接入已经实现。

## 已改代码

- `src/liangjian_funnel/data/board_reference.py`：新浪目录、独立前后计数及完整分页；同花顺目录、可见板块名称/指数标识、指定成员表及分页；通达信概念/风格/指数文件及行业分类文件；F10逐股归属和输入股票范围逆向图。
- 网络请求频率、请求数、截止时间、单响应及累计证据大小有界。不生成Cookie、不绕过401或验证页。
- 原始公开响应保留字节Base64、SHA256、参数、接收时间；文件保留文件SHA256与来源日期依据。供应商更新日期未知保持null，不把采集日期、文件mtime伪装成供应商更新日期。
- 本地成功版本不可覆盖；失败独立留存。回退保持原日期/哈希，7日预警、超过14日阻断，无未来版本回退。
- 明确来源和板块身份；同一方向按已批准的独立优先级选择来源，不把成员并集当成同口径成员，不生成伪BK代码。B股/基金等保留原始计数，技术/策略投影排除非A股。
- `scripts/collect_board_references.py`：显式来源采集、目录或指定板块/全目录范围、完整性审计。F10默认每批200只成功版本可续采，单股独立目录避免全市场缓存N平方扫描；逆向图只保留小字段，不重复持有全市场原始响应。
- `scripts/audit_board_reference_bindings.py`：离线合并目录、输出27方向的精确名称候选和缺口；已批准映射才允许生成影子投影。少数股票逆向图禁止投影成完整板块。
- `tests/test_board_reference.py`及6股样本：反例、真实入口增量恢复切片。

## 测试与真实来源证据

实施前：pytest收集失败（模块尚未存在），退出1。实施后最终相关套件：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_board_reference.py tests/test_rotation_theme.py tests/test_ths_taxonomy.py tests/test_ths_industry.py
```

62 passed，退出0。覆盖供应商计数冲突、缺页/重复/总数变化、空响应/401/502、错股票/板块、旁栏误收、GBK/UTF8、哈希篡改、过期/未来缓存、回退日期、请求/证据预算、通达信文件截断与行业层级、B股隔离、F10分批恢复与少样本禁止冒充全量。

实采命令均为独立输出目录，不使用生产运行入口：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_a4_20260923/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/collect_board_references.py --source sina --category industry --all --deadline-seconds 120 --max-requests 250 --output-dir artifacts/board-reference-20261008/sina-industry-full
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/collect_board_references.py --source ths --board 309121 --deadline-seconds 45 --max-requests 10 --output-dir artifacts/board-reference-20261008/ths-members-verified
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/collect_board_references.py --source em-f10 --universe-json tests/fixtures/board_reference_sample_universe.json --output-dir artifacts/board-reference-20261008/f10-sample --batch-limit 3
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/audit_board_reference_bindings.py --references artifacts/board-reference-20261008 --output artifacts/board-reference-20261008/theme-coverage-initial.json
```

| 验收 | 真实结果 | 解释 |
|---|---|---|
| 新浪概念样本 | 175目录；华为汽车97/97、两页80+17 | 本机及www用户隔离VM均通过，入口退出0 |
| 新浪行业全目录 | 49方向；34完整、15拒绝 | 例如电力独立计数61却返回62，有色计数69却返回72。没有截断多余成员或忽略计数矛盾。脚本原生退出2（首轮PowerShell外层呈现1） |
| 同花顺目录/成员 | 361代码；AI PC首根页可读、第二页401 | 保留原响应；完整性拒绝、入口退出2。通用title不是身份，以可见h3与隐藏指数标识交叉验证 |
| F10增量样本 | 第一批3/6退出2；第二批复用3缓存、补到6/6退出0 | 102个输入范围内板块标签，8个策略方向精确名称候选；不是全市场完整成员 |
| 综合27方向对账 | 12方向有精确名称候选，15未映射 | 全部仍需映射审核；“候选”不等于已批准、成员完整或策略合格 |
| 文件/响应对账 | 首次取回的78份版本，内容哈希/原始字节哈希错误0 | 证据在artifacts/board-reference-20261008，保留失败和旧实现的真实结果 |
| 通达信 | 可选离线文件解析反例切片通过；MCP未验收 | 用户确认接入方式为MCP，不要求客户端安装目录。当前会话工具/资源清单及本机Codex配置未发现通达信MCP入口；需确认实际服务名、工具契约及完整成员能力 |

VM重新核验aurum-vm=192.168.1.254/debian，生产HEAD=522ec802f00c800c6b70611101efef8e3929a502。新模块只在`/tmp/liangjian-board-reference-acceptance-20261008`独立导入，以www执行，不安装到生产site-packages。最终模块SHA256=8faf7691af72b2cb21b17fc5760ac4b4ff96aebdfde3dd9cea1b92b2abbde1db。本机/VM同源码；证据只读取回，不覆盖生产数据。

最终源码再次隔离验证：Sina入口退出0；F10入口退出0、6份既有成功缓存复用（不是再次采集6只）。最终取回的92份版本重新计算内容与原始响应字节哈希，错误0。隔离导入脚本与夹具临时副本收尾删除，原始证据保留。通达信MCP尚未验收，同花顺401仍未解除，不把故障拦截测试通过当成来源恢复。

## 后续可执行操作

1. 2026-10-09用户改为停止通达信MCP路线，复用既有同花顺API。已落地本地映射和日度/竞价统一入口，18/27方向真实采集，生产尚未切换。后续以`docs/HITHINK_ROTATION_REFERENCE_2026-10-09.md`的剩余9方向核对与验收为准；此前通达信记录仅为历史边界，不再作为前置条件。
2. 复用真实、完整、有日期/哈希的市场股票目录，在独立目录执行F10增量采集。全量股票未覆盖前，逆向图始终不充当完整板块；已有6股夹具不是市场股票目录。
3. 对27方向逐项确定不同来源的代码/名称与覆盖边界，保存显式映射及优先级，再运行离线映射审核。禁止因名称接近自动批准或合并。
4. 通过完整成员、日期和影子对比后，再接生产来源选择，并用原有腾讯动态价格/资金与原评分验证下游。不得改供应商强度定义或放宽资金/行情覆盖门槛制造结果。

## 分层验收

- CODE：通过，62项相关测试；默认旁路，旧生产调用路径未改。
- REPLAY：反例和增量恢复切片通过；未进行真实全市场、整交易日A2—A4回放。
- OPERATIONS：本机与VM独立样本已验证；全目录与通达信MCP仍有上述待验收项，生产未切换。
- STRATEGY：阈值、三策略、T+1未改；未证明新来源改善收益或已解决生产零计划。

## 格式参考

新浪请求方式核对AKShare的`stock_industry.py`、`stock_classify_sina.py`；通达信文件布局参考trading4/tdx_block_data（Apache-2.0）。本模块独立实现，没有执行参考项目的CSV写入，也没有移植受限代码。AKShare包装器不等于新数据源；同花顺网页不是付费iFind，F10不同路径也不等于独立供应商。
