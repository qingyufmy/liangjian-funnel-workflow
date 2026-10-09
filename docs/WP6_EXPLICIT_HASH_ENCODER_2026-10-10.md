# WP6 显式哈希编码器（2026-10-10）

## 1. 原因

Claude 0019 已接受 cfed3a5 的流式合同，但真实组件剖析发现 Python iterencode 比 C dumps 慢。需要保留两条可测路径，不能由环境自动猜测，也不能把节约内存直接当收盘耗时改善。

## 2. 修改

`pipeline/a1_contract.py:stable_digest(value, *, encoder='STREAM')` 增加显式 C / STREAM 选项。STREAM 维持本切片既有默认；C 使用原 canonical_json 全文再 UTF-8/hash。所有实际调用点保持原样，没有环境开关、生产接线或改哈希字段。无效选项拒绝。

`tests/test_wp6_explicit_hash_encoder.py` 验证相同黄金字节、非字符串同型键、NaN/Infinity、nested/tuple/-0.0，以及 lone surrogate 和混合键排序的原异常。保留原特殊 float JSON 合同，不擅自改 allow_nan。

## 3. 验证

原码新测试 `hash-encoder-before.xml`：10 failed / 1 passed，exit1。最后单项是双方错误类型恰好同为 TypeError，不能说11项全红。

正确相关命令：`pytest tests/test_wp6_explicit_hash_encoder.py tests/test_a1_streaming_diagnostics.py tests/test_a1_packet.py -q -o addopts='' --junitxml=artifacts/wp5-20261010/hash-encoder-final-v3.xml`，39 passed，exit0。此前两轮命令分别错填了不存在的测试文件名，no tests ran / exit1，原XML保留，不视为产品回归失败。

## 4. 四层验收

CODE：39相关项通过，统一新HEAD全量待执行。REPLAY：黄金边界和已封存真实组件证据，未对92MB整快照新增计时。OPERATIONS：未发布，VM自然收盘旧/新各一次 wall time 待证据。STRATEGY：不适用。

## 5. 边界

没有在生产运行时选择 C 或 STREAM。是否改变实际调用点，要同时比较时间、RSS及收盘总预算，单独发布；Windows fixture计时不能代替Debian自然证据。源头数据数量、完整性和预算均不改。

## 6. 下一步

随消费者/资本隔离新提交统一全量送审；采样器另保持未接线，VM观测按后续单独发布授权执行，不挤入周六热修验收。
