# A3发布与晨审日期契约修复

## 已实施

1. 普通close发布显式取得交易日历的下一交易日。`_plan_expiry`默认路径同样使用交易日历，不再只跳周末；显式日期若非交易日也推进至下一交易日。模型不得把有效期跨到别的交易日。日历不可用时阻断，不用weekday兜底。
2. 新增共享日期校验`runtime/plan_validity.py`。晨审先检查到期时间与目标日，再取报价；已过期计划进入EXPIRED，不延长日期。未来计划保留待晨审，今日不激活。无有效候选返回BLOCKED，记录具体原因。
3. `activate_pending_plan_batch`在事务内再次检查，混入一条过期或错日计划时整批回滚。`activate_latest_a3_plan_batch`同样检查；保留已有显式恢复对尚有效旧版多日计划缩短期限的能力，但不能延长过期计划、不能改写明确的未来目标日。
4. 晚于09:32的晨审以实际执行时间激活，不倒填09:32。
5. 状态API的ACTIVE_TODAY计数与monitor_plans按当日时间窗口过滤，不再将过期状态行展示为今日有效计划。未来未到期的待晨审计划仍可见。输出计数口径说明。
6. A4每分钟完成核心执行后检查执行计划范围：09:32之后空范围发送独立业务告警，按交易日去重；首次恢复后发恢复通知。使用现有飞书账本，不改动持仓保护、行情要求、策略门槛或历史信号。

## 验证

命令（工作目录D:/dev_A股/liangjian_a4_20260923，PYTHONPATH=src）：

```
D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -m pytest tests/test_plan_session_contract.py tests/test_a4_runtime_repair.py tests/test_acceptance_nonempty_plan.py tests/test_workflow_integration.py tests/test_cli_workflow_coverage.py tests/test_workflow_lark_notifications.py -o addopts='' -q --tb=short
```

最终结果：84 passed，退出0。覆盖9月24日跨中秋至9月28日、普通交易日、跨年假日假件、日历失败、混批原子回滚、过期晨审不取行情、未来计划拒激活、显式恢复不延长过期计划、旧版多日计划安全缩短及幂等、页面时间过滤、告警和恢复各一次。通知使用FakeNotifier，数据库均临时目录。

中间扩大回归发现已有显式恢复允许缩短旧版多日计划；保留这一已授权能力，并增加明确目标日不可提前激活的反例，最终全通过。

2026-09-28 11:00只读探针：生产715条计划中的5条ACTIVE_TODAY均为9月25日到期，新过滤逻辑得到有效数0、5条PLAN_EXPIRED。探针只通过标准输入载入新纯函数，生产SQLite使用mode=ro，无落库、无激活、无通知。

## 验收状态和剩余操作

- CODE：相关84项测试通过，不代表全仓库测试。
- REPLAY：今日真实过期5计划纯函数验证通过；没有重演当日完整A4或评估策略收益。
- OPERATIONS：尚未提交推送或部署，生产仍c358d28；未恢复今日执行池。
- STRATEGY：策略和阈值未修改，未宣称改进收益或保证有信号。

下一步是发布窗口内执行既有deploy.sh，并检查实际安装包、状态API和飞书运行链路。若授权盘中恢复，须另行对09:33最新研究结果按当前报价和正式发布契约重新审核，仅从恢复时刻生效，不延长旧计划、不回填上午信号。此补丁本身不自动将竞价研究19只全部变为可执行计划。
