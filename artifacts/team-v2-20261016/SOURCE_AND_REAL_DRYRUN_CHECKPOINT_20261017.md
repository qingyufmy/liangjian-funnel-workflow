# 本轮独立推进：六日原源 + 真实 A4 + A6 超时取证

源码 HEAD `5058558f553fc2c534fc8f493c9bd78e2054e565`。所有新原件在本机 Team worktree；没有 push/deploy、生产业务库写入、客户通知或历史成交补写。

## CODE

- `a4bb6ee` 六日原事件/计划/1m 快照只读绑定；`c423406` 历史执行时钟/真实采集时钟分离；`8c8bc51` 终态计划排除。
- 最终相关 45/0，回执 `20261017T090810976857-replay-terminal-scope-final-receipt.json`，SHA `a53841f8778e92887171c69e67c8c638b4a3441f0c0d4f6d7b4a6f4bd8e5b0d3`。21/44 是重叠证据，不相加，不是最终候选全量/VM。
- `5058558` A6/PM 晨会结构化原计划投影：不重复全量 A3 审计 blob，原行、引擎信封相等，几何/交接/证伪/退出规则不改；41/0，回执 `20261017T091518337450-morning-compact-source-actual-green-receipt.json` SHA `3229451bcff53f4afeb3915de8909f4d503d0c8a64cd483aaa601c95bda817a7`。

## REPLAY

- 实际 09:02:11–09:02:59 封存全部六日（10-08/09/12/13/14/15），21,542 去重计划分钟，21,338 原快照可绑定、204 原缺项/窗口不符；10-09 的 240 EMPTY_SCOPE 不算计划样本。未补历史缺口。
- `correctness-source-20261017T0909/PREPARATION.json` SHA `baefa7848700c12626d9341d7bae252342d646c4e373f8e6529ef8dd40e8d824`；gzip SHA `0617498b9074c5b961eec11218a9555b1055a5757c553277426454cb5a0c5a2c`。目录标签不是采集时间。
- 首个 A4 三计划模型返回虽 schema 合法，但源域误含一条原 PLAN_INVALIDATED；原件不改，`SOURCE_DOMAIN_REVIEW.json` 排除正式门。已修，45/0 包含反例。
- 修正后 2 有效计划，新输入真实调用 09:08:33–09:08:52，19,427ms、schema 0 问题、0 订单、对账 MATCH。RESULT SHA `d8bc668b6cb46fb7c0db3531fe2d95b15664d7bc72b602098b51f6f2e5a12dad`。两条原 START_CONFIRMATION 对新 NO_ACTION 已逐项封存。
- 每条报价标 REPLAY_BAR_DERIVED。历史 PM 缺失保持禁入；不能用本次零订单证明双跑账户曲线、99% 六日合法率或收益。六日实际执行仍未过门，emotion 四量缺失，LEADER 保持 DATA_BLOCK。

## A6 新失败与待裁定

- 原 5 发布计划与 A3 源严格绑定。原 460,121 字符和紧凑 33,175 字符分别真实调用，均 100s `MODEL_WALL_CLOCK_TIMEOUT`、1 attempt、无完整响应；不是 schema 通过或 0ms。
- 0282 question 已投：现有 Ark A6 默认 thinking enabled/low，而 A2/A4 disabled；建议先准许 A6 PREMARKET 隔离用同模型/同提示词/同预算关闭 thinking 验证。当前没有改该选项，不扩大预算。不能断言输入大小或 thinking 是唯一原因。
- 0278/0279 作者格式、0280 已有持仓 record 回退语义仍待回复，没有重复投递。

## OPERATIONS / STRATEGY

- 截至本轮共 9 个真实请求：5 份完整响应、4 个 provider 失败；2 份 A4 schema 合法中，首份因源域问题排除正式门，只有 1 份正确域探针。没有把请求数当合法响应数。
- 最新 `20261017T091846158131-REAL_DRYRUN_SUMMARY.json` SHA `9ad2b2c26508d7a085ff0c503a7e2b5482cf260472579dc7a0c29550e928366b`；逐角色 N/p50/p95/预算齐全，N=0 为 NULL。模型进程已退出；80 个生成文件对两条已知 provider key 的原字节扫描无匹配（限定这批目录，不声称全机/未知凭证审计）。
- 生产仍由严格 SSH pin/当前配置读取保护核实为 2b077f3，本轮无生产改动。H1 final f79 已获 0234 ACCEPT，周日白天安装/发布/唯一内部探针不变。只给旧 H1 execution state 补了最终候选指针，未动代码或重测。
- 所有 Team 发布门仍 NOT_MET；没有收益/胜率主张，没有把局部完成当最终候选批准。周六 12:00 进度表继续按 0277 A/B/C 写。
