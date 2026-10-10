# 周一19计划保护与通知归属：只读核验

核验时点：2026-10-10 19:28，北京时间。生产代码仍为已批准的 `499ed6abc3fd159de23b59dd76b02329300582db`，公告模式仍SHADOW。没有再次研究、发布、改配置、激活计划或发送通知。

## 当前19条

只读SQLite `mode=ro/query_only` 同一事务读取19条完整行，全部PENDING_MORNING_REVIEW，目标日10-12、到期15:00；payload均无shadow_inputs。逐行原payload SHA、完整行SHA、有效期和source_run_id保存在：

`artifacts/weekend-closeout-20261010/20261010T192820-monday-readonly.json`

证据SHA：`84d450daff36cecde731c702f248fa3dff34b8be4e09580efb8cad5b22f7d007`。

完整计划集合规范化SHA：`1366914cf81f93a76714c63c2154cc414849e850558d4c80cfed02653bb66105`；与19:27第一次独立读取一致。不存在用health代替计划核对。

Node真实PID6387、用户www、cwd为生产目录，3210监听正常。进程由宝塔Node项目管理，不是 `liangjian.service`；该不存在/未启动systemd名称不能判为生产停机。环境未显式覆盖调度开关时，现有config默认 scheduler=true、features=true、comparison=false；没有输出token或webhook。

## 计划清单是谁发

`run-next-session-prep`负责研究与PENDING计划发布，不负责本次客户通知。因此手动prep没有投递账本不构成漏发。

真实自然路径：Node `premarket` → CLI `run-premarket` → `run_scheduled(PREMARKET_0830)` → `publish_a3_premarket_analysis` → `publish_a3_premarket_analysis` publisher。08:30从primary lane读取当天未过期PENDING，按最新source_run_id选一批，发送总览及分页清单，**不取报价、不写计划、不调用模型**。09:26 `run-morning` → `review_pending_morning`独立竞价复核；只有合格激活计划才由 `publish_premarket`发送晨审结果。两类通知不是同一条账本。

production `state/lark_webhook.json`存在，仅验证配置文件存在，不证明10-12实际投递。周一08:30与09:26真实任务/通知仍待自然验收；本次不补发。

## 周日晚到周一07:00是否覆盖

当前Node scheduler周日hard NOOP；周一03:30 features维护不研究/发布A3；07:00—07:14 auction-base使用活动A1准备新的独立基线，代码明确不调用模型、不发布计划。08:30只读计划，09:26晨审是正式激活所有者。竞价后研究刷新明确不替换执行计划。本次没有加入22:00夜间公告任务，也没有自然next-session-prep定时任务。

只读系统timer检查：唯一量见timer为03:41 storage-retention；未发现root/www cron或/etc/cron.d中的量见研究/prep任务。保留维护只归档列举文件，不写execution_plans；周末新19条也在72h保留窗口内。未重启、未禁用既有任务。

因此**按当前已核实调度，没有周日或07:00任务会重跑本次prep或替换19条**。这不是对未来配置变化的保证。auction-base资料准备仍有独立耗时/来源健康风险，需周一自然验收，不改变计划集合本身。

## 决策14前后保护

当前证据是部署前基线；不声称已取得未来部署后逐行一致证明。审批后正式部署前后分别运行同一只读脚本，比较完整row SHA、payload原字节SHA、status、valid_from、expires_at及集合SHA；任一变化必须解释并停止验收，不以计数仍19掩盖替换。

影子sidecar仅写独立数据库，以原plan_id+payload SHA+目标日绑定，不写生产execution_plans。正式晨审路径不消费影子侧车；sidecar失败只影响研究。前向回退以499ed6a为业务基线，保留runtime/state/outputs，不reset生产目录。

## 资料失败的准确分类

2084是PDF文档数，不是公告查询股票数。50个PDF缓存未命中，2034命中；2063可用、21失败。对本次原冻结快照逐文档去重后的失败全部为 `CNINFO_PDF_TEXT_EMPTY`（21），不是21个公告接口请求失败。

证据：`artifacts/weekend-closeout-20261010/20261010T193145-disclosure-failures.json`，SHA `110698b7f25ebc478e0040d5e21f56045f74f08920fdcacab487b4f66c0d6c47`。原事实、原缺口及完整性要求未修改；不把PDF空文本当已补齐。

## 周一15:10公告耗时估算（预测，不是验收）

本次新进程实耗44m52s；CNINFO查询14m28s、PDF17s、公司事实4m22s、模型研究13m36s。phase包含嵌套/等待，不能将全部totals直接相加。此前同run_id历史累计展示不用于本次预算。

SHADOW仍对1624股票全域同步，而不是影子候选1025。生产CNINFO共用0.5s起始限流、4工人，不能按4倍速度估算。周一最近风险查询6h TTL已过期，450日报告可在真实增量覆盖成立时复用7日历史；既有差量需完整新公告才能合成，不预先认定缓存命中。1624个单页请求的限流占用约13m32s，双查询全失效约27m04s（不计分页、网络、身份查询、重试及其他官方源）。

以本次其他阶段近似不变，仅公告由14m28s变为27m04s，约57m28s；距离Node默认90m约32m32s。但周一日线增量、资料范围和网络都未实测，该余量不是稳定上界。原manual prep预算UNKNOWN，不能称已验证Node90m。

保留SHADOW；只有周一真实进度显示资料阶段耗尽余量、且已审离线/自然影子范围覆盖证据齐全时，才提交单独CANDIDATE_DOMAIN gate（精确范围回执、非A2结果反向定义、任何SCOPE_MISS回退SHADOW）。本轮不自动切换、不重跑prep、不改变限流或门槛。

## 四层

CODE：上述源路径及58项相关回归通过；最终发布HEAD全量另行封存。

REPLAY：当前19条两次只读行SHA一致；不是未来部署后或晨审结果。

OPERATIONS：现有宝塔Node与已批准prep成功；周一自然调度/投递、新影子timer以及决策14部署后保护仍待。

STRATEGY：不评估收益、不改参数；周一影子仅技术触发研究，PIT/成交腿未接线。
