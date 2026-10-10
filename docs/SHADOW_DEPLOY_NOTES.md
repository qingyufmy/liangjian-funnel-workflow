# 周末影子发布准备（2026-10-10）

本文件是待评审的运行清单，不是影子发布批准或已上线回执。2026-10-10当前生产基线为已批准发布的 W0 `499ed6abc3fd159de23b59dd76b02329300582db`；已通过既有deploy.sh部署，随后一次盘前准备成功形成10-12的19条待晨审计划。影子整包另走决策14，不能把W0批准扩大为整包批准。历史adafe50回执只作历史记录。

## 已完成与仍缺项

W1：六个固定变体、独立SQLite/JSONL、完成标记后消费冻结决策、两阶段触发状态提交、5秒接受截止及安装路径绑定已有可运行切片。G-B最新日期约束源码 f2339adb 的全六日门已完成：20,820窗口/123,402比较，missing/差异/调用方改动/首次触发差异均0，真实求值与独立封存exit0。原件在WP1的gb-rebind-final-20261010T1932，freeze SHA f47cea6a7413184b8e4abfec7fa109d5a632f223101b1d5cdb1dc548f784ec7b；POST_HOC_ENGINE_ONLY且双方触发0，不证明收益。独立preopen sidecar仅补算原19条计划的影子输入，不改计划；229项初版及87项复审切片通过，不相加作为全量。真实晨审fixture激活前后payload原字节SHA相同、sidecar仍可消费，几何/字节变化继续拒绝。

W2：独立身份/模拟成交/T+N合同及原HTTP响应旁路封存器已实现。原始上市日期响应、当日09:26身份、多个板块显式涨跌停字段的真实映射仍待证据。不能把解析后的文本或JSON投影SHA冒充原HTTP SHA，也不能把历史收盘字段当当日竞价证据。无这些证据可仅发布触发研究，不宣称成交腿就绪。

W3：独立日报/周报、只读census、逐原动作的生产等价证明和桥接输出已在本地入口接线，111项初版相关测试通过。15:30草稿、16:45兜底及周五16:00周报的独立timer模板已准备；另有10-12 08:40 preopen和09:30 session的一次性模板。未安装到VM，不代表自然调度验收。0062裁定路线A，新增Node只读观测端点 `/api/shadow/job-runs-readonly`，不改任务/策略路径，实际socket仅loopback/GET、原内存DTO、Node身份与真实服务端钟，16项反例通过并单列于 `W3_NODE_READONLY_ENDPOINT_2026-10-10.md`。Python客户端138项相关回归已冻结（SHA 57ba572cfd8f85d6a03f3e34d7340b5d0f96a1112b30dac32a1024e713ba054c），请求前独立reservation、60s节流、2s接受截止和重启身份核验均在内；缺原件仍16:45兜底，不伪造A5完成时间。本机小报告只读交付组件56项相关回归通过，真实轮询接线待0066裁定；VM shadow_outbox存在不等于Claude已收到。

W4：固定输入18种A-LABEL提示字节比较完全相同，包括真实A2送审的因子上下文；资源采样只读接线已在候选代码。空闲fsync基准完成，未接生产writer。W8加载270.8MB/4264范围真实VM快照的packet/hash峰值1.271GiB，不等于完整A1 FULL峰值。Tony已决定A1在VM、现有内存够用，不扩容、不改调度；完整FULL峰值只作为后续观测。

## 发布前清单

当前运行连接（2026-10-10 16:12核验）：`aurum-vm` → `192.168.0.254:22`，主机 `debian`，原主机密钥验证成功。下文旧IP只表示当时核验记录，不是后续连接配置。W0 `499ed6a` 及验收后一次盘前准备已获Tony当前对话批准；决策14仍单独审批。

1. 冻结唯一发布HEAD，补全该HEAD全量测试及独立验证；保存原失败和跳过项目，不用旧HEAD回执。
2. G-A/G-B/G-C回执逐项绑定实际引擎、依赖、冻结输入与窗口计数。G-B `POST_HOC_ENGINE_ONLY` 单独标签，不进入策略收益统计。
3. 影子service使用显式源DB、分钟DB、lane、原 `outputs/monitor/latest.json` 和独立输出；不得猜路径、构造RuntimeStore或调用A4。CLI显式指定自身仓库root，实际imported模块与checkout逐文件SHA一致才运行。

   CLI原完成标记不能与源DB、影子DB、mirror或receipt共用路径；检查在任何ledger创建之前完成。三个真实入口反例旧代码3失败（其中receipt路径可成功启动形成错误混用）；修复后与等待/安装/事务联合92通过，exit0。证据为 `shadow-cli-output-alias-before.xml` 与 `shadow-cli-output-alias-final.xml`。安装路径注入反例2失败及修复64通过另留在 `shadow-cli-checkout-before.xml` / `shadow-cli-checkout-final-v2.xml`。
4. 影子失败不得改变A4动作；缺PIT/下一完整分钟/交易日腿记null或DATA_LIMITED。影子不发客户飞书，不写execution_plans，不占生产JobRunner/BoundedWorkGate。
5. 真实调度配置先在隔离目录验收；最终service/timer/env接线与部署命令集中入决策14包，不提前在VM执行。
6. 发布前核验SSH别名、debian身份、真实运行目录、无活动研究及deploy保护；安装后逐模块SHA、Node、调度与实际计划回执分开验收。

## 现有deploy与回退限制

现有 `deploy.sh` 固定fetch/pull `origin/main` 并要求目标HEAD绑定的全量回执；它没有 `--target` 或安装旧提交选项。**不能直接运行脚本声称已回退adafe50，也不能为此force-push或reset生产目录。**

桥接0046已裁定：在隔离分支预构建“前向回退提交”，撤回影子及A3附加字段，业务字节恢复到 **W0 `499ed6a`**，保留已发布不可变事件和数据库；对回退HEAD全量测试，随最终发布包送审，需要时仍经Tony批准正常push/deploy。adafe50包含已知receipt缺陷，只作文档备注，不预构建纯复原。本轮尚未完成最终目标HEAD的回退提交，不执行回退。

2026-10-10只读核验：`ssh -G aurum-vm`解析root/192.168.1.254/22，主机debian；以仓库所有者www读取当前HEAD=adafe50，`git cat-file -t <adafe50>`返回commit，原deploy.sh SHA256为 `4a91e1114f49ab072c8131194650dec92da2f4b5c3732e55ac8a34a897ff8f29`。SSH命令exit0，仅证明原Git对象与脚本可读，未演练重装。首次用root运行git被原所有权检查拒绝exit1，随后改用既有www身份；没有添加safe.directory或变更Git安全设置。

## 下周验收

2026-10-10 20:37只读Settings实际路径证据 SHA `78c4b91dd1cd32db88c016d11c5e84028480f4ecf71158cc995da24bd6ce0a6a`：生产state/facts/minute/A5/monitor/Node原目录均存在且www可读；Node3210未配置token。本机已据此将9个模板渲染到 `artifacts/weekend-closeout-20261010/install-proposal-2037`，manifest SHA `5221b9d3a7fb883603dd07e5af84a8030064fff24df2109574ad7a0c9cd58940`，不是已安装配置。新输出建议根 `/www/wwwroot/Agu/liangjian-shadow` 与生产目录隔离；报告不写生产checkout的docs。目标OS的日历解析通过，仅证明calendar语法；unit sandbox/权限/实际运行与端点尚待决策14后的验收。当前session模板只覆盖周一，不称周二至周五已接线。W2真实PIT与成交消费仍缺证，整体周末任务未完成；可以提交明确降级的技术触发研究候选，不能承诺周五已有收益结论。

CODE、REPLAY通过不能替代OPERATIONS。周一逐分钟验证原完成标记/影子接受时刻、缺失及预算占比、逐动作/首因零差异、客户通知0；观察到完整PIT和下一根真实分钟后才验收模拟成交。五日样本不足20笔则继续观察，不据此修改生产参数。
