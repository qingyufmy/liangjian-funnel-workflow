# T2 D2 本机门槛准备（10-30）

本件是候选材料，不是 FINAL 或发布许可。依据看板 M28、M101、M124、M158、M169、M215、M225；实际工作树 `D:/dev_A股/.team-v2-track2-20261029`、分支 `codex/track2-20261029`。轨道一 DATA 在途时不批量取数、不跑 full、不部署。

## 已有证据

| 项目 | 本机结果 | 限制 |
|---|---|---|
| MKT1 | 273 个冻结对象投影相等，7 原文件 SHA 不变；BEAR_RISK 独立命名空间 | 62 格为 UNCOVERED_NO_ORIGINAL；原件缺失的 PIT 仍 DATA_BLOCK |
| MKT2 | 六指数腾讯日线/供应商身份与时钟通过，收盘事实 READY；周趋势采用 M28 | 10-30 首次采到的历史序列，不是各历史日的原始 PIT |
| DK v0 | 六指数各106根闭合15m，三档 N 增量相等；1/4/16根收益、间隔、4根反向翻转及右删失分母输出 | 20日未覆盖；完整预热后交易日 N10=4、N20=2、N30=1；5日影子未完成；当期计划股范围待原件 |
| TH-1 | TH1_RADAR/4；710成员组只读复用；A07/A08与B002–5六指数×两日12行价格子事实独立核对；M215逐成员排除原因 | 历史PIT未证；704指数价格历史、成员行情/历史资格/日期获证封板事实仍缺；两日全目录排名均null，不能报告前十通过 |
| FE1/FE2 | 表头12列/数据12列；400/1024横向表格；旧API冻结25计划×11字段+6KPI共281字段相等 | 10-29冻结快照，不证明当前生产、登录、非空账户或DB/journal一致性 |
| P1 | 真实只读计划详情入口接A6会议原件/容量/降级；原生Store受控四类×三宽展示 | 受控用例不是生产原件；C9显式接管事件待T1合同，显示缺源 |

原件聚合：`artifacts/track2/20261030/t2-followup-r1/six-indices-real-v3.json`。六指数原始响应留在 `artifacts/track2/tencent-canonical/2026-10-30/`。HTTPX 应用层原始响应体与压缩传输字节的计数分别保留，不混称原始 wire frame。

腾讯既有源12次（指数日线6、指数分钟6）按M158合并计入MKT12/30；旧DK1累计7/20与东财历史1仅留审计，旧额度已经退休，不占新额度；未新增东财请求。16:31的一次T1租约撤销在发送前暂停，源0。首次上证日线321行触发的解析上界错误保留原失败，修复后用同SHA原件离线通过，没有重取上证日线。

TH-1新增同花顺专项当前0/1500，独立于MKT日30。A01/A02目录、既有板块历史/成员与生产FactCache优先复用；每批≤200、间隔≥1秒、首错留原件即停、不重试。当前成员标 CURRENT_MEMBERS_PROXY。不早于候选发布；无空档排周六D1部署12:00后。T1 DATA在途全部暂停。

## 定向检查

- `run-20261030T093953029820`：MKT/DK/TH1/传输预检 **203 passed**，受控 transport、真实 ModelClient.complete→httpx.stream，零真实源/模型。`tencent-preflight-v4.json` 绑定当时源码 SHA。
- `run-20261030T094009331469`：新PIT收紧后的六指数同原件离线聚合通过，18条增量比较相等，原文件重验不变。
- `run-20261030T094013637055`：P1、既有只读投影及失败隔离 **36 passed**。包括真实原生Store产出的降级/停止结果、错身份/时钟/输入SHA/证明/计数反例、只读API入口、源字节不变。
- `run-20261030T093840578071`：headless 三宽与P1受控12场景通过，输出 `ui-headless-v4`；400宽容量/停止图已目检。旧v3顶部遮挡截图保留，v4修复验证脚本滚动位置后重拍。
- `run-20261030T093753459532`：v3旧冻结API→DOM 281字段相等；其表格/KPI产品源码与v4相同，v4变化为P1原因说明与截图滚动位置。不是新增原件。
- 以上通过 owned 本机测试入口，成功或失败 scratch 当场删除。没有VM测试、VM临时根或生产库手改。

## 尚未通过的发布门

| 门 | 状态与下一项证据 |
|---|---|
| H full / R full | NOT_RUN；T1 DATA在途不跑。待同列车H/R与轨道一脚本统一 |
| runtime 40/10/10 | NOT_RUN；没有以单测数冒充运行采样 |
| 三批 | NOT_RUN；三份采集报告不是三批业务验收 |
| D5 10-29、10-30整日 | UNPROVEN；需完整元数据原件，不把10-30回溯价格冒充10-29当日原件 |
| D8 / F7 | PARTIAL；已获M101认可的headless旧冻结展示；候选发布后补当刻HEAD/flag/API与同库账本原件 |
| D1 下游清单 | 初稿：市场模块只由离线工具和定向测试消费；新增UI投影仅由只读计划详情消费。收盘链、闭市续跑、MinuteBarStore、风险/订单/通知/调度不接线。合列车后须复核 |
| Claude FINAL ACCEPT | NOT_RECEIVED；部署必须取得看板 kind=decision 的 FINAL ACCEPT，且无工作流在跑 |

不以以上定向结果替代 full、D5、runtime、当前生产视觉/业务或发布审批。无放宽门的改动。

## 18:45 补充：M158 / M169

- 腾讯新建独立每日归档台账，原7/20及父账SHA冻结保留。日线6+指数分钟6+P（≤18）合计计入MKT30/日，同日同身份不重复；今日既有12次按合并日额保守继承。没有逐15m或后台调度；安装仍等D2 FINAL。`m158-budget-migration-readback.json` 核旧账未变、MKT12/30、真实源/模型新增0。
- 六指数与计划股共用显式分钟身份/PIT解析入口，指数必须用registry key，误把指数qualified code送个股分支会拒绝。计划股须来自同日冻结scope；当前真实scope尚未提供，不用旧失效计划替代。
- `DK_DAILY_ORIGINAL_ARCHIVE/1` 离线归档保留first_seen_at与源SHA，重叠修订/断档拒绝，不改旧数据；真实采集日与回溯bar日分列。10-30第一份归档的actual_capture_days=1，不是5日影子通过。
- TH1为 `TH1_RADAR/2`。M158量比用volume/前5闭合日均值，原单位不换算并标同源比值；封板总占比、制度拆分独立证明。M169对各榜统一剔除部分缺失项，余项等权；披露有效N/剔除原因，不补0、不缩榜。
- `th1-full-catalog-partial-v1` 已零HTTP跑出10-28与29每天320行业+390概念。现仅有两行业价格和三组当前成员、没有个股缓存报价/已证日期封板事实，因此两榜无全目录共同指标，排名与得分全null，前十仍UNPROVEN。19份原件SHA重验不变，独立核对目录、normalized DTO SHA、现有价格子事实和schema同型通过（run104345035228）。归一化选择对象明确不是供应商原件，更不是历史PIT。
- 最新定向组合 `run-20261030T104516694822`：**249 passed in12.32s**，53份相关源码/配置/schema在检查前后字节相等。`d2-targeted-input-pin-v1.json`、`d2-targeted-result-v1.json`、`tencent-m158-preflight-v3.json`、`th1-m101-m169-preflight-v3.json` 保留工作树字节pin、测试结论和新预算下真实客户端受控stream证明。真实源/模型HTTP为0；不是H/R full。

M101全榜请求清单当前预案为1415次/8批，每批≤200，正式发送前必须先扣除T1/T3后续成功原件。T1 DATA与候选未发布期间不发送；无窗口排周六D1部署12:00后。未向VM创建测试根、复制在用DB或部署。

## 19:50 补充：M188 / M209 / M215，只读原件复用

- T1 index原件SHA `513942b600b445419549fc9eb503d7f8adc75b68157bc5165a68ff8348ed39b4`：2目录+710成员组，80,865原生成员行、5,581唯一标识。按M215保留全部原件，仅把1行NQ排除出A股统计分母并披露NON_A_SHARE_EXCHANGE，仍保留CURRENT_MEMBERS_PROXY/历史PIT UNPROVEN。个股行情、历史ST/上市资格、日期获证封板事实未提供，不补零。
- T3 B index原件SHA `3bc30927d8c040744720155f78773710daa8375b53c380aef69cdcb7b3965b43`：四份指数各501根，原请求/响应/流/terminal/binding分别绑定。六份可复用价格历史使待取从1415→708→704，当前4批为200/200/200/104，成员请求0。正式取数仍须候选发布、所有DATA空档、新鲜看板读回与同字节预检；不是现在发送许可。
- `th1-full-catalog-m215-m209-v1` 对10-28/29各保留320行业+390概念，并从10-21开始顺序生成五个前序交易日派生榜。750份原件绑定与两日全部成员选择独立核对通过；各榜没有全目录共同数值项，得分/排名/前十仍UNPROVEN。缺龙头资格/收益不从子集冒称前3；缺leader_top3的前序榜不是重合0只。
- `th1-m209-price-facts-v2.json`：六指数×两日12行，只是回溯价格事实。直接从原始gzip、腾讯HS300 raw独立Decimal核涨幅/RS3/RS5/同源量比、120高点和MA，不是交易收益或策略验收。
- M215的指数聚合量口径另有M216待裁定；答复前885540.TI量比明确缺项，不把已排除NQ当成上涨/封板行情缺失。
- `load_m101_prices` 只读消费已结束的采集账本，核原采集计划/哈希链/预记账/流关闭/原SHA/时钟/覆盖；在途batch.lock拒绝，后续批失败不抹掉已成功原件，发送前取消不冒作已发。只在受控真实Hithink调用链验证，目前没有M101正式账本或源请求。
- `d2-targeted-result-v2.json`：16文件290项本机组合通过，71源码/Schema/测试文件前后SHA一致，两份真实ModelClient流式预检匹配；不是H/R full。之后补了零有效A股的缺因展示，最终候选仍须同字节收口。
- 所有新增操作源HTTP0、模型HTTP0、生产写入0。OPS根因与修正见 `m188-ops-diagnostics-v1.json`；公共清理器glob/stat竞态在child前失败，改为串行调用，不改共享清理器。

当前尚无候选发布及当前API/底账原件，full、runtime40/10/10、三批、D5、当前F7/D8、合列车D1与FINAL仍未完成。前述18:45预案1415仅为当时历史记录，已经被本节704预案替代。

## 20:07 最终本机收口（M225）

- M216已由M225答复，没有待裁定的量比规则：维持M158公式；逐行volume_scope标INDEX_NATIVE_AGGREGATE，原生指数聚合口径可能包含非A股成分、组成未经核实，不拆成分量。非A股成员不影响量比可用性。当前机器合同TH1_RADAR/4、contracts/th1_radar_v4.schema.json；v1–v3留历史，不混作当前合同。
- 当前派生回放为 `th1-full-catalog-m225-v2`；750原件绑定、两日全成员选择/有效A股分母、Schema4与七日派生榜SHA独立核对通过run120438890963。当前12行价格子事实为 `th1-m225-price-facts-v3.json`，含volume_scope/member_scope披露，独立Decimal核对通过run120444243143。两个榜的排名仍全部null，前十UNPROVEN。
- 当前704请求预案 `th1-m101-backfill-plan-m225-v2.json` 还把NQ排除出未来A股缓存查询名单；原始成员组和原件不删。源请求仍0/1500，无M101正式采集账本。
- `run-20261030T120515112588`：16文件 **292 passed**，58个唯一源码/Schema/测试输入SHA前后相等（原76个键含18个路径别名，已去重）。`d2-targeted-input-pin-v5.json`、`d2-targeted-result-v4.json`、`tencent-m158-preflight-v5.json`、`th1-m101-m225-preflight-v5.json` 绑定最终工作树字节。真实Hithink/httpx与ModelClient流式链均走受控transport，socket/DNS拒绝，真实源/模型HTTP0；不等于H/R full。
- 腾讯补采真实当期计划股可显式用 --only-plan-stocks --kind MINUTE --plan-scope，不会重新请求已经归档的指数；P≤18、日合计≤30、同身份一次、T1空档和同字节预检仍在。当前真实计划股范围仍未提供，不能把旧失效计划当范围或自选18只。未安装任何后台/15m调度。
- 20:00轨道一M226仍NO_CANDIDATE，资源门未达。取数、当前FE原件、full/运行门、FINAL/部署均继续等待；本节没有发布批准含义。

20:12 输入计数与发布边界校正：Windows反斜杠路径和斜杠路径曾并列为76个pin键；统一后为58个唯一文件，18个重复别名的SHA一致，没有改源码或重跑冒作新结果。292项结论仍是run120515112588，唯一pin-v5/result-v4保留等价链。提交 `6b845a182a6d90665e27a9b406815a1826cfa7d8` 的58文件都存在，3份Schema JSON的工作树CRLF与Git blob LF仅有行尾差异；Python/脚本等可执行源无该差异。见d2-commit-readback-v3.json。此为本机字节证明，合列车后必须对实际发布bundle重新核SHA和执行H/R full，不能把本机292项或Git HEAD当成已部署H。M188 OPS诊断v2记录路径查找误判的根因，旧readback-v2/result-v3不再作当前入口。

## 后续零 HTTP 收口（M252 / M267）

成员收益保护已提交 `18cf473f85e643ea43f4b354d2cbf06f97ceb4d9`。585份获证原件只读复用，54份未匹配不读不用；629股两日两窗口共2516窗，身份不足212、逐日限幅未证1150、相邻窗口不足1154，实际可入指标数值0。`RATE_UNPROVEN` 不改成统一10/20/30%上限，未把受控测试限幅当真实来源。详见TH1合同的M252/M267节与 `th1-member-return-guard-m267-v1.json`；全榜排名和第一天前十仍未证。

另完成D3的MKT4宽度子事实（`0a643ae`），r1因list_date缺失DATA_BLOCK，r2可用5020/5224、adv816/dec4134/flat70；官方量能A、同输入最终情绪范围/完整率及整个observer运行时性能仍未证。见 `MKT4_BREADTH_SHADOW_V0_20261030.md`，不视为D3验收或D2门替代。

采集前本机审计发现TH1发送时合同未校验负数/非有限volume/amount，而离线读回会拒绝；现与同一 `native_index_prices` 校验统一。6个真实Hithink客户端受控反例均完整保留第一份响应、首错停止且不发第二笔。合法缺量仍是指标缺项，不混为源错误。修复没有发送真实请求，旧292项保留为旧字节结果。

当前 `run-20261030T134418859946`：18文件 **321 passed**，64个唯一输入前后同SHA，JUnit逐case计数，无跳过/失败；两份受控真实源/ModelClient流式预检同字节通过。当前证据为 `d2-d3-targeted-input-pin-v6.json` / `d2-d3-targeted-result-v6.json`、`tencent-m158-preflight-v6.json`、`th1-m101-m267-preflight-v6.json`。新增源/模型HTTP及生产写入均0；仍非H/R full、runtime或三批。

M277只改变T1恢复运行安排，没有解除T2候选发布和DATA让路门。当前full/运行门/D5、当前FE原件、真实计划股scope及FINAL仍待；不安装后台归档，不部署。

## 22:08 原始输入独立核对

- `run-20261030T140516652936`：直接读取六份腾讯5m原始响应，独立合成各106根15m，再逐点重算N10/20/30共18序列、1908个阈值/状态/事件点和B3.5的D/K分组1/4/16根收益、间隔、4根内反向翻转及右删失分母，全数相等。未导入market模块；`dk-six-indices-independent-audit-v1.json`。完整预热后交易日仍仅4/2/1，20日未覆盖、真实采集日1和实际计划股缺口不变；K后价格收益不是卖空收益。
- `run-20261030T140748658447`：直接读取六份腾讯日线响应与原采集报告，用已安装XSHG交易日表独立分周，核M28三条件UP/DOWN/RANGE、MA5/MA10/MA10 lag2以及市场收盘/原生provider/真实采集三时刻，六指数全部相等；上证321行完整保留。`mkt2-six-indices-clocks-weekly-independent-audit-v1.json`。未导入market公式/解析器，回溯价格仍不是每个历史交易日原始PIT。
- `run-20261030T140156623250`：M291共享r4原件manifest与transport LEDGER按SHA/长度绑定；1870新dispatch中，TH1所需目录/成员/指数历史/个股日线/报价端点0，不能增加行情覆盖。这里只做metadata库存，不冒作9007响应逐份重审。原件时钟与字节不变。

上述为独立离线证据，不改变321项同字节定向组合的源码或full状态。M294在22:00证明NO_CANDIDATE；实际PID已退但progress残留RUNNING，以真实终态/各自观察时钟区分，不手改生产progress。704请求继续暂停，M282的FE-P2规范源对接另候裁定。

本机收尾：仅删除本次安装的 `t2-followup-r1/tooling`（121文件、逻辑13,658,871B），删除前逐文件SHA库存已落盘；17份既有冻结证据SHA前后未变，共享全局headless浏览器保留，T2的VM临时根0。见 `owned-tooling-cleanup-v1.json` 与 `owned-tooling-inventory-v1.json`。PS无BOM Unicode路径在删除前失败、回执OrderedDictionary计数在删除后失败，两条原日志保留；从已落盘库存补回执并复核通过run141338607821，没有重复删除。独占实际FS释放量因free测量未持久化而UNPROVEN，不以逻辑字节或Win32报告大小代替。

M306说明T1的TLS诊断未取得VERIFIED，r5未启动；这不产生T2源许可。18:05续接后的新增provider/model HTTP和VM查询均0，MKT当日12/30继承、TH1专项0/1500不重置。D2与D3缺口按前述表保留，后续先读看板和同列车状态，再做获准取数/当前FE原件核验或全门。

22:49收口读回：`run-20261030T144928718919` 以采集门实际使用的标准MCP stdio入口只读读取T1/T2/T3，通过；分别读到T1 blocked/latest M312、T2 doing/latest M305、T3 doing/latest M295。`hub-stdio-readonly-smoke-v1.json` 绑定原门脚本SHA，明确source_clearance=false、final_accept=false；不调用供应商/模型、不代替候选发布或DATA空档核验。M312仅给T1定时TLS诊断与有条件r5安排；M282的FE-P2规范源仍待裁，未开工五页签。旧20:17索引保持原SHA，新交接入口使用 `CURRENT_REVIEW_INDEX_20261030_v2.json`，不覆盖旧历史件。
