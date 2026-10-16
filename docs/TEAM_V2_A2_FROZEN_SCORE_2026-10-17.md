# A2 冻结量化成员主题分核心（0263）

实际验证：2026-10-17 05:41–05:43 Asia/Shanghai，源码 `37b8c529d675c003c64c063e7c9996780926319c`，仅本机隔离。纯函数/显式新config，尚未接生产研究构造器，不宣称A2自然接线完成。

`pipeline/team_v2_a2_score.py` 从全体量化送审域（非模型FOCUS/单批projection）按原A1 theme_id分组，每因子available=true、有限0–100且至少3个成员才取算术平均。保留全成员数/有效数、原字段路径、来源引用与字段hash；完整记录连同原快照SHA、新policy、原权重形成不可变score_snapshot SHA。源字段或成员改变会改变hash；不允许模型选择成员。

沿用原资金因子全局缺失去权重；其余缺项按原权重（排除全局资金缺项）合计≤0.15才归一，显式 `SCORE_RENORMALIZED_MINOR`；超过则分数NULL、`SCORE_DATA_LIMITED`、要求WATCH。原十权重、60参考线、相对TOP5例外不动；本核心不替代技术门或发布。

扣分只允许冻结规则中唯一非正值，同code一次。未知INVALID；区间无法唯一解析时NULL/WATCH，不猜。已发现原六项全是区间，独立question请Claude定取值；主题局部资金缺失0.12如何计入也已明确列问。后续实际适配器必须把聚合分/原模型分与FOCUS集合差异列入三日真实干跑，不将差异伪装零差。

红：15失败exit1，`artifacts/team-v2-20261016/20261017T054140250481-a2-score-red-receipt.json`，SHA `7d4a3f28cd5ed4a2e8ef8243eba21c128980db1073d46b4cd1998fe6c2b989f7`。

绿：95关联通过exit0（新核心15+既有A2 schema和A3研究adapter），`20261017T054228960214-a2-score-green-receipt.json`，SHA `8303065cd02f82e50492a24466a8a4b6df4e42775f49fe07824cc682bd70a72b`。保留红件，不与其他切片计数相加，不充全量/真实模型干跑。

CODE核心本机通过；REPLAY规则反例通过、正式三日/六日未做；OPERATIONS未VM未部署；STRATEGY收益未证。H1最终包f79/周日独立上线安排不动。
