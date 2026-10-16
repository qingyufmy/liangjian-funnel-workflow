# H-1 0222 两项修订（隔离开发，未部署）

实际记录：2026-10-16 20:17 Asia/Shanghai。

R1：异常兜底只隔离同账户、同股票、当日未正常终结的生命周期；历史 CLOSED 不写入 reconciliation。A5 改用精确 signal_id 查对账异常，不再把新入场用的同股隔离范围拿来排除历史业绩。同股新入场保护保留。

R2：自然盘后 A5 入口在建事实前执行收尾，仅当前真实交易日生效。只读单事务核对原 monitor 239 个适用分钟快照、逐包原 SHA、采集时钟、其余全部 1m 窗口；原窗在任一原包或版本账本出现，均不能声称缺失。不读取未来日、不补 bar、不补成交。完整采集账本中仅原窗缺失时，同事务封存 DATA_MISSING matching_attempt 和 DATA_ERROR / ENTRY_WINDOW_DATA_MISSING，排除绩效 cohort；缺采集、额外缺 bar、哈希坏、15:00 超出原执行截止等均维持 SIGNALLED 并发 A4_ENTRY_WINDOW_UNRESOLVED 内部告警。迟到的盘后 bar 不用于反向撮合。

这里“完整”指 239 个原采集记录全部可验证且其余所有应执行窗口齐全；不把只有最后一包、health 或墙钟收盘视为证明。若真实生产未形成这份完整证据，正确结果是 UNRESOLVED，而不是关闭。

真实红绿：200955 原 HEAD：历史正常行也被隔离，反例失败；R2 尚无模块的接口失败分别保留，不冒充交易语义红测。201347 关联 Python 66/0（含真实 monitor 12×239、A5、状态库与新收尾入口）。201207 Node：两项新告警缺失及一项 mock 未清的测试隔离失败保留；201240 Node 13/0。后续独立告警文件路径改动仍需最终 HEAD 全量及 VM 受影响切片，不沿用旧 HEAD 证明。

权限只读取证：201625 shadow.env 含 LIANGJIAN_DASHBOARD_TOKEN，且没有 SHADOW_RESEARCH_WEBHOOK。不扩大该文件读取权限，不改内容。Node 接线改为独立 internal_alert.env，值的安全来源需 Claude 确认；未安装文件、未发测试告警。原决策15本机指定秘密文件存在，未输出或记录其值。

旧 74cd 全量、回退 26d 全量、7 原包反事实、VM33+11 保留为旧版本证据。新候选另跑正式全量；回退树仍为 2b077f3。CODE/REPLAY 局部通过，OPERATIONS 未发布，STRATEGY 不适用。只有最终候选实际 ACCEPT 与安装权限门完成后，周日白天发布。
