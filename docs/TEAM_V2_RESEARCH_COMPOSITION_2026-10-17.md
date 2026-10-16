# 自然研究入口的 Team 组合接线

实际验证：2026-10-17 07:44–07:45 Asia/Shanghai；仅本机隔离代码验证，未执行真实研究或部署。

CLI 的 A1 维护、研究、恢复准备、收盘、竞价刷新、comparison、run-due 入口显式选择 Team 研究组合。Workflow 的 A1 维护和常规 A1/A2/A3 两处真实管线构造共用一个工厂，保留原槽、父预算、并发、feature-store 配置和客户发布器。record 仍只回退 A4，不暗中改变研究角色。

实际进入研究才加载：八份批准提示词／README 的精确文件域、字节数、SHA，版本化 A2 评分政策，原配置 hash/version 计算规则，以及与盘中角色共用的 `state/team_v2/model.db` 和 `team.db`。无源文件、篡改 SHA、域外 manifest、未批准评分政策或路径越界，均阻断，不回落到旧角色或主观主题分。此工厂不构造 RuntimeStore，不调模型，不读取凭证。

非研究 CLI（status、基线、晨审、盘前通知、monitor 等）不因构造 Workflow 而初始化研究账本。直接使用旧库 API 的消费者保持显式调用的行为，正式 CLI 的选择另有边界测试。

验证包括实际 Workflow 工厂 → A2 HTTP mock → Team 原响应／判断账本 → 原量化硬门。确认 A1/A2/A3 三角色均选中，研究账本位置正确，不创建测试中的生产 workflow 库；另覆盖六个实际 CLI 调度入口以及库 API 保留旧路径。

初轮 18/0，相关 107/0，最后精确文件域增量 13/0，均有重叠，不相加，不是最终候选全量。

- 关联回执 `20261017T074510338894-research-workflow-composition-related-receipt.json`，SHA `fcdfc55f6ee1958df0d406a91bff0f347591147193a94c8eeaec3b9633f003cc`。
- 最终增量 `20261017T074545869814-research-composition-exact-bundle-final-receipt.json`，SHA `bfdcc6c03f218463508a48d41e15444faf7fdbc5e97f6cf7c5cff5573fc4375c`。

上述回执位于 `artifacts/team-v2-20261016/`。CODE 仅本增量通过；正式三日真实模型干跑、六日 CONTROL 回放、最终全量／VM／回退包仍待完成。提名池过滤样本生产者及其完整域验证仍待接线；无样本时继续明确空输入，不能伪称完整覆盖。
