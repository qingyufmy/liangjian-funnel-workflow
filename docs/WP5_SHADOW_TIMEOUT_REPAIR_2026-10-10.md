# WP5 一次性影子任务有效超时修复

## 1. 原因

首次决策14部署的8个服务均为Type=oneshot，RuntimeMaxSec被systemd明确忽略；TimeoutStartUSec=infinity。原退出0只是语法检查，不是有效截止证据。原始日志及回退证据保留在artifacts/weekend-closeout-20261010/decision14-operations-20261010。生产已回到15dd63b，19条计划不变、影子全停。

## 2. 修改与反例

4个service模板保留oneshot，改为TimeoutStartSec，预算15/331/80/5分钟不变。week明确10秒停止宽限。只读脚本scripts/validate_shadow_service_timeouts.py按服务生命周期拒绝无效组合；新tests/test_shadow_service_timeouts.py覆盖模板、无限/零/非法值、旧写法以及simple/exec只有启动超时的反例。

旧模板先跑20项：4失败/16通过，native exit1（artifacts/runtime-timeout-repair-20261010/before.xml）。失败正是4个模板的ONESHOT_RUNTIME_MAX_INEFFECTIVE，不是导入错误或数据缺失。修改后的切片、实际命令与退出码将封存于同目录评审包，不覆盖旧证据。

## 3. 血缘与测试

按0069先快进15dd、revert该前向回退得到6af53aa，再提交本修复；不reset、不force、不重推7521。新候选及其新前向回退分别运行scripts/test_all.ps1和独立全量回执验证。回退TREE必须与W0 499ed6a完全一致。

对7521和黄金冻结目录列出的源逐文件SHA核对；相同则复用既有G-A 20820窗口、G-B 123402比较的零差异证据，不重新读取9GB样本、不声称产生新回放或成交证明。若源差异即不得复用。

## 4. 四层验收

- CODE：以新HEAD完整pytest+Node+typecheck回执和独立核验为准。
- REPLAY：源SHA一致后复用已冻结零差异证据，POST_HOC_ENGINE_ONLY；不是到达、收益或自然5秒证明。
- OPERATIONS：待新批准后的逐服务有效属性、VM真实5秒探针、SQLite同属性沙箱、新端点、定时器与交付验收；本轮不部署。
- STRATEGY：无阈值、候选数、限流、模型或客户通知变化，不证明收益。

## 5. 未完成边界

真实VM超时探针和新影子运行尚未执行；旧沙箱未执行不算通过。W2 PIT/成交腿仍未接生产。仅影子验收失败停止影子保留生产；生产验收失败使用新回退包。两级处置及周一08:40启用硬门已写入SHADOW_DEPLOY_NOTES.md。

## 6. 下一步

两份新全量回执、相对7521补丁、源SHA对照送Claude复审；接受后等待Tony新一次发布批准。
