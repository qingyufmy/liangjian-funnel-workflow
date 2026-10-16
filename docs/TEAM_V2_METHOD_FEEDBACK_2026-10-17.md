# 方法反馈与独立标签（开发检查点）

实际采集：2026-10-17 07:13:04 Asia/Shanghai。源码 `2c2c921`。生产、VM、凭证、模型供应商与通知均未触碰；真实干跑次数仍为 0。

## 实现与验证

原账本的 nullable `t1_return/t3_return` 没有标签可用时间，不能单凭非空宣称 PIT 成熟。新增独立 `model_judgment_outcomes` 追加记录，绑定原判断/模型输出 hash、真实日历的 T+1/T+3 日期、原日线字段与 hash、实际观测时间和实际标签捕获时间。原判断、旧收益列不重写。同一标签不可改写，T+3 复用 T+1 的原基准价格，不随缓存修订更换基准。

新调用同时封存实际证据目录。没有目录原件、没有 availability 原件的旧记录不升级。实际 `run-outcomes/run-outcomes-refresh` 自然入口尾部已接现有缓存只读采样；无 Team 账本为 NOOP，不创建；不额外抓行情，不调模型，不改客户/计划/成交。研究标签故障只记独立 DATA_LIMITED，不改变原 outcomes 成败或触发重跑。

口径是研究判断日收盘到未来收盘的观察收益，不是可成交收益：A1/A2/A3 取 source_trade_date，盘中 A4/A6 取 execution_trade_date；复用现有 RAW_PLUS_EXPLICIT_FACTOR 校验。只有原判断可明确绑定 symbol 才可计算，PM 槽位/账户级等未绑定标的的项不猜、不冒称收益齐全。

聚合使用最后 20 个交易日，每方法桶要求至少 10 个不同、T+1/T+3 均成熟的交易日；只包含证据类别、判断类别、样本数、日期范围和均值/正收益比例。无个股身份、逐股收益或模型自由文本。A5 事实快照新增 method_calibration 与 `A5:METHOD:<role>` 引用。A5 合法输出中显式引用 method_id 后，A1/A2/A3 才读取对应统计；读取时再次按原 cutoff 重算底层不可变证据，核对 A5 源快照、目录、模型原响应与已选 ID。任意旧快照 READY 字段不能绕过。

一条真实客户端 HTTP mock 链已验证：十日合成标签 → A5 源事实/引用 → A5 模型 schema/独立账本 → 后续研究反馈；只保留匿名统计，不复制 A5 中的个股文字。底层标签损坏后读者拒绝旧 READY。所有“真实”仅指实际代码入口与 HTTP mock，绝非真实供应商样本。

## 回执

原件均在 `artifacts/team-v2-20261016/`：

- `20261017T070524708311-feedback-source-first-receipt.json`：34 通过，退出 0；SHA `9b8bf3f634a6df6afc4ecbd0814c970271b0d4a994c9ac0f3d53550f6cb1ee48`。
- `20261017T071122609863-feedback-labels-related-final-receipt.json`：186 相关通过，退出 0；含 A1/A2/A3、A5、原 outcomes/价格刷新路径；SHA `5d0243c61bf6f643765d1e2ad03d558a219c2f98aa76a22c5411e3afabe5594a`。
- `20261017T071220695220-feedback-historical-proof-final-receipt.json`：最终读者重验/标签源增量 22 通过，退出 0；SHA `1bb9d080d25ccc196a67a5181152b75f723ed017cc49fced126af3f3f8c40fbe`。

回执逐文件 hash 绑定实测源，以上测试有重叠，不能相加，不是最终 HEAD 全量。没有为了该项重复 H1 全量、旧 B3 六日或供应商模型。

CODE：该链路相关检查通过。REPLAY：正式 Team 六日未完成。OPERATIONS：未发布，H1 仍按周日窗口。STRATEGY：真实十日统计不存在，收益未证。每方法十日门与 A5 method_id 机器选择口径另交 Claude 确认，不把本项当整包 ACCEPT；不改角色提示词正文。
