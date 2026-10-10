# 周末影子发布准备（2026-10-10）

本文件是待评审的运行清单，不是发布批准或已上线回执。当前生产基线为 `adafe5054b9ff283ea03925e2b0c6baf2146dcfe`；W0最小热修为 `499ed6abc3fd159de23b59dd76b02329300582db`，桥接0045等待Tony批准。影子整包另走决策14，不能把W0批准扩大为整包批准。

## 已完成与仍缺项

W1：六个固定变体、独立SQLite/JSONL、完成标记后消费冻结决策、两阶段触发状态提交、5秒接受截止及安装路径绑定已有可运行切片。CURRENT/CURRENT原冻结20,820窗口零差异；G-B新发布引擎的全六日门仍在执行，旧版本结果不能代替。PIT/行情缺项按DATA_LIMITED留存，不参与模拟成交或收益。

W2：独立身份/模拟成交/T+N合同及原HTTP响应旁路封存器已实现。原始上市日期响应、当日09:26身份、多个板块显式涨跌停字段的真实映射仍待证据。不能把解析后的文本或JSON投影SHA冒充原HTTP SHA，也不能把历史收盘字段当当日竞价证据。无这些证据可仅发布触发研究，不宣称成交腿就绪。

W3：独立日报/周报与15:30草稿、A5完成后正式报告、16:45截止协调器已有测试。真实定时单元、A5只读状态观察器、census归档和桥接交付尚未接线；纯协调器测试不代表自然调度验收。

W4：固定输入A-LABEL不进入模型提示词；资源采样只读接线已在候选代码。空闲fsync基准完成，未接生产writer。W8加载270.8MB/4264范围真实VM快照的packet/hash峰值1.271GiB，不等于完整A1 FULL峰值。

## 发布前清单

1. 冻结唯一发布HEAD，补全该HEAD全量测试及独立验证；保存原失败和跳过项目，不用旧HEAD回执。
2. G-A/G-B/G-C回执逐项绑定实际引擎、依赖、冻结输入与窗口计数。G-B `POST_HOC_ENGINE_ONLY` 单独标签，不进入策略收益统计。
3. 影子service使用显式源DB、分钟DB、lane、原 `outputs/monitor/latest.json` 和独立输出；不得猜路径、构造RuntimeStore或调用A4。CLI显式指定自身仓库root，实际imported模块与checkout逐文件SHA一致才运行。

   CLI原完成标记不能与源DB、影子DB、mirror或receipt共用路径；检查在任何ledger创建之前完成。三个真实入口反例旧代码3失败（其中receipt路径可成功启动形成错误混用）；修复后与等待/安装/事务联合92通过，exit0。证据为 `shadow-cli-output-alias-before.xml` 与 `shadow-cli-output-alias-final.xml`。安装路径注入反例2失败及修复64通过另留在 `shadow-cli-checkout-before.xml` / `shadow-cli-checkout-final-v2.xml`。
4. 影子失败不得改变A4动作；缺PIT/下一完整分钟/交易日腿记null或DATA_LIMITED。影子不发客户飞书，不写execution_plans，不占生产JobRunner/BoundedWorkGate。
5. 真实调度配置先在隔离目录验收；最终service/timer/env接线与部署命令集中入决策14包，不提前在VM执行。
6. 发布前核验SSH别名、debian身份、真实运行目录、无活动研究及deploy保护；安装后逐模块SHA、Node、调度与实际计划回执分开验收。

## 现有deploy与回退限制

现有 `deploy.sh` 固定fetch/pull `origin/main` 并要求目标HEAD绑定的全量回执；它没有 `--target` 或安装旧提交选项。**不能直接运行脚本声称已回退adafe50，也不能为此force-push或reset生产目录。**

建议在隔离分支准备“前向回退提交”：恢复需要撤回的业务代码到adafe50字节，保留已发布不可变事件和数据库；对新回退HEAD全量测试并经Tony批准正常push/deploy。若W0已获批上线，回退到adafe50会恢复已知receipt缺陷，建议工程评审区分“原始基线复原”与“保留W0、撤回影子”。具体回退方式及目标需桥接裁定后写进最终决策14包。当前只读核对，不执行任何回退。

2026-10-10只读核验：`ssh -G aurum-vm`解析root/192.168.1.254/22，主机debian；以仓库所有者www读取当前HEAD=adafe50，`git cat-file -t <adafe50>`返回commit，原deploy.sh SHA256为 `4a91e1114f49ab072c8131194650dec92da2f4b5c3732e55ac8a34a897ff8f29`。SSH命令exit0，仅证明原Git对象与脚本可读，未演练重装。首次用root运行git被原所有权检查拒绝exit1，随后改用既有www身份；没有添加safe.directory或变更Git安全设置。

## 下周验收

CODE、REPLAY通过不能替代OPERATIONS。周一逐分钟验证原完成标记/影子接受时刻、缺失及预算占比、逐动作/首因零差异、客户通知0；观察到完整PIT和下一根真实分钟后才验收模拟成交。五日样本不足20笔则继续观察，不据此修改生产参数。
