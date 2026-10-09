# 同花顺本地板块映射与A2轮动入口迭代（2026-10-09）

## 当前结论

隔离分支 `feat/free-board-reference-20261008` 已落地同花顺API慢变映射、增量采集和A2实际入口适配。日度研究与集合竞价刷新共用来源配置，避免只修一个入口。生产环境未修改，未提交、推送、合并或部署；未调用模型、写生产数据库、补历史信号或发业务通知。通达信MCP不再作为本轮前置条件；既有通达信价格校验及可选离线导入没有删除。

**242项相关测试通过；真实采集18/27方向。剩余9个宽行业未解决，因此不能批准全量生产切换，也不能宣称板块链路已彻底恢复。**

## 根因与本轮修改

旧日度流程先执行东财BK目录/成员轮动，再采集同花顺行业、概念图。目录失败且缓存超过14天时，已有同花顺数据不能接替轮动入口。集合竞价还有独立调用，单改日度流程不能解决。

| 需求 | 修改 | 测试/证据 |
|---|---|---|
| 复用既有同花顺API | `hithink_board_reference.py`，调用已有HithinkClient，保留`.TI`代码 | 最终实采概念390、行业320；18成员请求HTTP200 |
| 不误称完整列表 | THS列表严格核对声明总数、分页、续页、身份、重复JSON键；无独立总数时明确记录全列表接口契约 | 声明缺页、total冲突、错板块、重复字段反例 |
| 本地版本与增量 | hash地址版本，7日刷新/预警、14日到期；失败不覆盖成功版本 | 20份最终版本；第二次执行新增请求0 |
| 明确策略映射 | `rotation_reference_bindings_v1.json`，18条逐项名称/语义核对；机器人概念不替换成机器人行业、贵金属不替换成黄金概念 | 绑定版本hash、生效日期、错名/错类别/目录换版测试 |
| 新来源不依赖东财 | `collect_rotation_theme_snapshot(reference_memberships=...)`，本地成员进入既有腾讯当日报价/资金和原评分 | 三个东财入口及历史全部禁止访问时离线切片通过 |
| 日度与竞价一致 | `configured_rotation_memberships`与`rotation_snapshot_directory`共用 | research/auction配置、竞价回归 |
| 不混历史 | LOCAL_REFERENCE独立日度目录；不同来源归档拒绝互读；不把BK历史、资金套到THS成员 | 归档来源不符拒绝，原文件字节不变 |
| 不误标绿色 | 新来源缺任意已配置方向时`ROTATION_REFERENCE_MAPPING_PARTIAL`；保留诊断行，不宣称全市场top5 | 两方向仅一方向完整的反例通过 |
| 不放宽技术/数据条件 | 资金/报价覆盖门及评分权重不变；新来源无3/5日证据保留null | 报价缺失、7日年龄预警、过期/未来/篡改反例 |

API源端timestamp只保留作响应元数据，**不是板块成员修订日期**。供应商修订日期未知保持null。原始HTTP正文仅留SHA256/字节数；保存的可复核数据是客户端规范化后的公开记录及内容hash，不冒充逐字节原始响应副本。单次完整接口响应不证明供应商长期稳定或其成员分类绝对正确。

## 实际采集

最终版本目录：`artifacts/board-reference-20261009/hithink-final`。初轮目录`hithink`保留，不覆盖早期证据。

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_a4_20260923/src'
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/collect_hithink_board_references.py --env-root D:/dev_A股/liangjian_funnel_workflow --output-dir artifacts/board-reference-20261009/hithink-final --max-requests 20 --deadline-seconds 120
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe scripts/collect_hithink_board_references.py --env-root D:/dev_A股/liangjian_funnel_workflow --output-dir artifacts/board-reference-20261009/hithink-final --max-requests 1 --deadline-seconds 10
```

两条均退出0。首条20次逻辑请求（两个目录＋18个成员），第二条0请求，全量复用有效本地版本。4455条成员边、2780个唯一股票只说明18个声明方向的参考范围，**不是A1候选数或A2入选数**。

- 首次最终报告：`refresh-audit-e9aa1690d89e0e6cfb34a839bcd54e4908541eace3f356cb0cf10881d366481a.json`。
- 缓存恢复报告：`refresh-audit-bc4d63cb34ca49dd6af0fcd24bd6b6ee70f3b87fc1bde87a9a7334071cb713c9.json`。
- 绑定文件字节SHA256：`43cfe58cb1de3e29cf29b203983e164e50ce589a4aa1e2170b6cb503d5a8d3b1`。
- 旧初轮20份版本内容hash复核错误0。HTTP响应hash并不等于已经重新复核原始字节，正文未保存不能作此声称。
- CLI截止时间在下一次提交前检查，单请求重试沿用既有客户端；不是强制取消已开始请求的运行时截止时间。

## 剩余9方向的分类核对明细

以下是实际目录支持的**拟议组件**，没有写入已批准绑定，更没有自动合并成供应商指数。要覆盖这些宽方向，应实现显式的自定义策略复合行业口径，并记录每个组件成员/hash、重叠去重与覆盖边界。不能把复合集合叫作东财或同花顺原生同名板块。

| 当前方向 | 同花顺实际目录中的拟议组成 | 尚需确认 |
|---|---|---|
| 化工 | 化学原料881108、化学制品881109、农化制品881263、化学纤维881264、塑料制品881265、橡胶制品881266 | 电子化学品是否单列，排除煤炭/石油与非金属重复 |
| 有色金属 | 金属新材料881114、工业金属881168、小金属881170、能源金属881267 | 与贵金属保持独立，不能无意重复纳入黄金方向 |
| 非银金融 | 保险881156、证券881157、多元金融881283 | 父方向与保险/证券子方向的继承，不重复占top5 |
| 农业与粮食安全 | 种植业与林业881101、养殖业881102、农产品加工881103、农业综合884012 | 渔业分类与覆盖边界继续核对；农业种植窄概念不能代表全方向 |
| 医药生物 | 化学制药881140、中药881141、生物制品881142、医药商业881143、医疗器械881144、医疗服务881175 | 区分宽医药与创新药，不能只用创新药概念代替 |
| 电力设备 | 电机881277、电网设备881278、光伏设备881279、风电设备881280、电池881281、其他电源设备881282 | 不把发电运营行业并入设备行业 |
| 社会服务 | 旅游及酒店881160、教育881178、其他社会服务881179 | 排除零售/食品，与现有月度定义核对 |
| 商贸零售 | 零售881158、贸易881159、互联网电商881177 | 一般零售子方向保留自身真实成员 |
| 食品饮料 | 饮料制造881133、食品加工制造881134、白酒881273 | 确认分类重叠并去重，不重复计数/成交额 |

表中代码都为`.TI`，省略后缀仅为表格简洁；未使用代码前缀推断供应商层级。目录存在不代表所有组件成员已经采集或跨源覆盖已验收。

## 执行命令和退出码

首次新测试收集退出1：目标模块尚未实现。实现后的切片逐步通过25、113、153、154、155、241项，范围有重叠，不累计为总量。最后补部分覆盖反例时出现1个fixture失败（复制了另一方向的aliases，触发既有歧义门禁）；修正fixture，不放宽该门禁。

最终唯一计数：

```powershell
& D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest -o addopts='' -q tests/test_hithink_board_reference.py tests/test_board_reference.py tests/test_rotation_theme.py tests/test_rotation_diagnostics.py tests/test_rotation_evidence_overlay.py tests/test_a2_a5_rotation_remediation.py tests/test_ths_taxonomy.py tests/test_ths_industry.py tests/test_hithink_fact_endpoints.py tests/test_settings.py tests/test_auction_base.py tests/test_auction_refresh.py tests/test_auction_data_repair.py tests/test_feature_source_materialization.py tests/test_hithink_fact_normalization.py tests/test_hithink_probe.py tests/test_a1_sources.py
```

退出0，**242 passed**。实际workflow源码导入路径为本隔离目录；`git diff --check`通过。没有用health或单测宣称生产业务稳定。

## 生产入口配置与验收边界

默认`LIANGJIAN_ROTATION_MEMBERSHIP_SOURCE=EASTMONEY`，不会因为更新代码自动切换。未来完整验证后才显式选择`LOCAL_REFERENCE`，并指定`LIANGJIAN_ROTATION_REFERENCE_DIR`与`LIANGJIAN_ROTATION_REFERENCE_BINDINGS_PATH`。本轮没有修改任何env。

新入口只读成员版本，没有把新来源的全量采集塞进每次研究/竞价；定期维护由独立采集CLI承担。新来源日度文件进入`rotation_theme/local_reference`，旧档案保留。新来源18/27的部分映射即使可以计算部分方向，也不满足正式全量轮动门禁。

- CODE：本轮切片通过；不是全仓库验收。
- REPLAY：离线入口、故障、历史归档反例通过；真实整日A2/A3影子对照待完成。
- OPERATIONS：本机API与增量缓存验收通过；虚拟机、自然调度、通知未验收，生产未切换。
- STRATEGY：评分/覆盖阈值、三套执行策略、T+1未改；收益和信号改善未证实。

下一条具体操作：根据上表实现可审查的9个自定义复合行业映射，完成逐组件真实成员核验；补齐27方向之后，使用独立冻结行情做新旧A2/A3影子对照，再决定生产切换。不要为27/27的数字随意合并分类或缩减注册方向。
