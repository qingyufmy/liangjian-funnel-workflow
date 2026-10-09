# WP5 G3-2 公司行动真实源证据（2026-10-10）

## 结论

CODE：新增隔离只读脚本，39 项相关测试通过。SOURCE：20/20 股票取得真实 HTTP 200、业务 code=0 响应；所有行校验通过，20/20 除权日与官方实施公告一致。19 个已有现金参考均与端点一致；688808.SH 的现金参考未核实，不能把端点的 0 当作公告证据。REPLAY/OPERATIONS/STRATEGY：本次未做因子实现、研究回放、部署或策略验收。

这证明端点当前可访问、基础事件字段和这 20 个生效日可得，不证明送转/现金/配股三类因子契约完整，更不证明当前 A3 已使用前复权。

## 授权与执行边界

授权来源：docs/CODEX_TASK_BRIEF_2026-10-09.md 的 2026-10-09 23:30 授权记录（9 月已知除权 20 股，只读、限流不变），与 Claude G1/G2 复审 G3-2 任务。

本地 Settings 未配置同花顺 key，先记录 HITHINK_API_KEY_MISSING（脚本 exit=3），未调用真实端点。真实探测通过既有 aurum-vm SSH，在 /www/wwwroot/Agu/liangjian-funnel-workflow 以 www（UID 1001）运行；只在 /tmp/wp5-corporate-probe.NmX349V7 放临时脚本、参考文件和隔离输出。使用生产原 Settings，原 0.5 秒限流、30 秒超时，原 HithinkClient._get_with_retries；初始化 trust_env=False。生产 key 没有复制到本地，没有打印凭证、响应 message、request_id、未知值、原请求 URL、异常文本或请求头。没有 RuntimeStore、数据库访问、模型、通知、安装、重启、生产 env/业务文件改写、commit/push。

_get_with_retries 源码 SHA256：2d4462fa3e5830ff518c92f5810a53ff5d8d60b4d8b2d6bc86bdae052c333de4。_paginate 未使用，因为现有行身份没有 ex_date_ms，不能让其折叠同股不同事件。全部 20 个请求都只用 1 次 attempt，无重试；每个返回 1 条 9 月事件，无额外事件日。

## 请求与真实字段

官方文档：[除复权](https://fuyao.aicubes.cn/docs/api-reference/corporate-actions/)。GET /api/a-share/corporate-actions/adjustment-factors，单股票 thscode，from=2026-09-01、to=2026-09-30。认证只由既有客户端 X-api-key 头处理。

本次实际安全保留字段：data.thscode、data.ticker、data.item[].ticker/ex_date_ms/dividend_per_share/per_share_bonus。逐行验证股票一致、整数毫秒、上海时区午夜、9 月窗口、有限非负数值、重复生效日和预期生效日；响应字节 SHA256 与安全行 canonical SHA256 分开记录。返回没有 event_type、record_date、adjust_factor、factor_basis、rights_issue_ratio 或 rights_issue_price 的证明；不能靠模块介绍推断这些能力。

## 逐只对照

所有下表生效日均 MATCH；详尽原始字节哈希和官方 PDF 哈希/URL 在 corporate-action-probe.json 与 corporate-action-reference-20.json。

| 股票 | 除权日 | API dividend_per_share | API per_share_bonus | 参考/因子状态 |
| --- | --- | ---: | ---: | --- |
| 601208.SH | 2026-09-29 | 0.1 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 603999.SH | 2026-09-30 | 0.015 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 601026.SH | 2026-09-29 | 0.1 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 600621.SH | 2026-09-29 | 0.069 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 600742.SH | 2026-09-30 | 0.15 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 603291.SH | 2026-09-30 | 0.08 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 605028.SH | 2026-09-29 | 0.3 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 603223.SH | 2026-09-30 | 0.07 | 0 | 现金 MATCH；差异化价参 UNKNOWN |
| 605016.SH | 2026-09-29 | 0.075 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 688088.SH | 2026-09-29 | 0.12 | 0 | 现金 MATCH；差异化价参 UNKNOWN |
| 603931.SH | 2026-09-14 | 0.065 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 603192.SH | 2026-09-14 | 0.125 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 600309.SH | 2026-09-11 | 0.81 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 603018.SH | 2026-09-21 | 0.04 | 0 | 现金 MATCH；差异化价参 UNKNOWN |
| 688808.SH | 2026-09-29 | 0 | 0.48 | 现金参考 UNKNOWN；资本公积转增与 bonus 映射 UNKNOWN |
| 002653.SZ | 2026-09-04 | 0.526 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 300196.SZ | 2026-09-09 | 0.2 | 0 | 现金 MATCH；价参现金 CONFLICT |
| 301373.SZ | 2026-09-23 | 0.2 | 0 | 现金 MATCH；因子契约 UNVERIFIED |
| 603939.SH | 2026-09-30 | 0.3 | 0 | 现金 MATCH；差异化价参 UNKNOWN |
| 920065.BJ | 2026-09-28 | 0.334 | 0 | 现金 MATCH；因子契约 UNVERIFIED |

## 仍然未知或冲突

- 300196.SZ：API dividend_per_share=0.2，与实派现金一致；官方公告价参现金是 0.1974617。直接把实派现金用于该股票的除权价格公式会混淆口径，因此记录 CONFLICT_DISTRIBUTED_CASH_VS_PRICE_REFERENCE_CASH。
- 603223.SH、688088.SH、603018.SH、603939.SH：参考公告确认差异化分配，但目前参考文件未核实价参现金；均记录 UNKNOWN_DIFFERENTIAL_PRICE_REFERENCE，没有猜系数。
- 688808.SH：API per_share_bonus=0.48，与公告资本公积每股转增 0.48 数值相同；官方端点文档只定义送股。数值相同不证明股本类型映射，记录 UNKNOWN_CAPITAL_RESERVE_TRANSFER_IS_NOT_VERIFIED_STOCK_BONUS。
- 没有配股样本，端点也没有已核实的配股价格/比例或预计算因子字段。本次不实现本地因子、不改变 raw 日线。整体 factor_contract_status=UNVERIFIED_NO_FACTOR_IMPLEMENTATION；CLI exit=0 只表示这组源调用/schema/生效日与可比现金通过，不表示因子通过。

## 本地复现与证据

脚本：scripts/probe_wp5_corporate_actions_readonly.py。测试：tests/test_wp5_corporate_actions_probe.py。脚本会拒绝覆盖输出和低于 0.5 秒的限流配置。

测试命令（设置 PYTHONPATH=src 后）：

    python -B -m pytest tests/test_wp5_corporate_actions_probe.py tests/test_hithink_probe.py tests/test_hithink_fact_endpoints.py --junitxml=artifacts/wp5-20261009/corporate-action-probe-tests-final.xml

39 passed，exit=0。覆盖坏 envelope/code/JSON、错误第二行、非午夜/类型错误日期、股票错配、缺日期/空事件、重复事件、数值异常、短 PDF 哈希、错误 URL、秘密/未知值不落盘、现金与价参冲突、转增 UNKNOWN、缺 key、限流保护、transport 错误与逐股继续。先行测试首轮 23 passed/4 failed（测试夹具直接构造 Settings 缺路径），改为 Settings.from_env({}, root=...) 后 27 passed，再连既有源测试共 39 passed。

真实调用同样使用上述脚本，--reference 指向临时参考文件，--output 指向临时隔离文件，--env-root 指向既有生产根；在 www 身份下使用既有 .venv/bin/python -B，不读取生产库。探测时段始于 2026-10-10T00:00:31.426155+08:00，非交易时段。

- 官方参考文件 SHA256：6278b5451e82d59a209c9e4b445c0270be6b3ae3f888a143f30f0bda68e9efd6
- 实际脚本 SHA256：3645497b1978ba01ac840c6c7b4be1800a47b46bf5b52f864f44b29c25745c8b
- 真实探测 JSON SHA256：7714d13a0213d3a837db16c2bd3f516ce8a0de2e57d43ce94ad871319b3264af

本地与远端上述文件字节哈希一致。再次独立验证 20 个唯一股票、全部 PDF/响应哈希 64 位、ref 与 report 股票/日期/官方 PDF 哈希匹配、安全行哈希可复算、20 个 HTTP200/schema/date MATCH、19 个现金 MATCH。官方参考为公告反查，不冒充端点响应；真实响应未存原始 body，只保存字节哈希与类型校验后的字段。

本地证据校验完成后已删除上述三个远端临时文件并移除空临时目录；本地脚本、参考、真实回执和测试 XML 保留，可复查。没有生产持久业务写入。
