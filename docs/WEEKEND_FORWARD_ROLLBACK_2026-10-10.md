# 周末影子版本前向回退合同

回退业务基线固定为已批准、实际运行的W0 `499ed6abc3fd159de23b59dd76b02329300582db`，不回退到含已知receipt缺陷的adafe50。

现有deploy.sh只安装origin/main，没有target参数。因此采用**以最终候选HEAD为父、TREE精确等于W0 TREE的新前向提交**，独立测试该回退HEAD；不是reset/revert生产目录或force-push。完整命令、HEAD/TREE/SHA与native退出在最终发布包外部manifest封存，不为记录验收再改待发布源码HEAD。

部署仍需Tony逐次批准。获批后才将指定前向提交正常fast-forward推送origin/main，再由www在正确生产目录运行既有bash deploy.sh。部署前核验aurum-vm=192.168.0.254/debian、原主机公钥、生产HEAD、保护时段和全部活动工作流；不终止正常任务或绕过保护。安装包和全量回执绑定目标HEAD，不能拿499原回执冒充新的回退HEAD。

新影子timer/service/env是独立安装层，不在Git TREE回退中自动卸载。回退发布必须按已批准安装清单先停止/禁用**这次安装的独立影子单位**，不能停止生产A4/A5或删除账本。禁用哪些单位由决策14包明确列出；未安装时不得空喊已卸载。原生产env、RuntimeStore、execution_plans、冻结事实、分钟归档、通知和不可变影子账本均保留。

业务核验前后读取相同生产计划批次的行SHA/payload原字节SHA/状态/有效期集合；交易时段的合法晨审状态变化须与真实事件对账。计数仍19不等于内容不变。不激活旧计划、不补发历史信号、不真实下单。

本文件是准备合同，不是已执行回退。CODE全量/安装健康不替代OPERATIONS自然盘中或STRATEGY收益验收。
