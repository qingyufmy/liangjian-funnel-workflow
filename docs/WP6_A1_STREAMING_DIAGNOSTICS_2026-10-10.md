# WP6 A1 canonical hash / packet diagnostics 小切片

结论：A1 的 stable_digest 与 packet_diagnostics 不再为了 hash/字符计数生成整份 JSON 字符串。旧字符串 API、canonical 字节、SHA、字符数、token 公式、section 排除规则、最大 12 个 section 排序、包字段与预算不变。相称组件 fixture 的瞬时分配峰值明显下降，但 iterencode 有 CPU 开销；这不是完整 A1 资源验收，更不是 Linux RSS、92MB 实际输入或生产请求耗时证据。

工作目录 `D:/dev_A股/liangjian_wp5_hotfix_20261009`，起点 HEAD `3839a6be563873a8b7ce62e75c0a6af127a2aead`。ownership 仅 a1_contract.py 的 canonical helper/hash、a1_packet.py 的 diagnostics/token 链、新 tests/test_a1_streaming_diagnostics.py 与本文件。WP1 源冻结，不读取其正式矩阵，不修改 workflow/research/其他同步或策略模块；没有提交、生产 DB/网络/模型调用，也没有读取 root 独立负责的 92MB 文件。

## 实际链路与最小修改

- `pipeline/a1_contract.py:56` canonical_json：保留原 json.dumps 字符串 API，ensure_ascii=False、sort_keys=True、separators=(",", ":")、default=str 不变。
- 新 canonical_json_chunks：同选项 JSONEncoder.iterencode；仍保留标准 allow_nan=True 与循环检查，不能复用 snapshot.py 的 allow_nan=False/不同 default，因为那会改变已有 A1 合同。
- 新 canonical_json_length：只 sum(len(chunk))，不统计无需使用的 ASCII；sections 调此方法，不复制全串。
- 新 measure_canonical_json：单次编码累计 total_chars/ascii_chars/non_ascii_chars；ASCII chunk 走 isascii 快路，混合 chunk 精确统计编码后字符（含 JSON 引号和控制字符转义），不是源字段字符。
- stable_digest：逐 chunk UTF-8 编码并更新 SHA256，不先 join 成整份 Unicode 再 encode。canonical_json 字符串调用者不变。
- `pipeline/a1_packet.py:258` packet_diagnostics：sections 原条件 `str(key) not in {packet_hash, diagnostics}` 不变；总包仅排除原始 key == diagnostics，**仍包括 packet_hash**。总字符和 token 共用一次 measure，不再为它们各 json.dumps 一次。
- `_estimate_tokens(text)` 原入口保留；纯 counts helper 仍为 `max(1, ceil(ascii_chars/4 + non_ascii_chars/1.5) + 8)`，累计全包后只 ceil 一次，不分 chunk 舍入。不调整 100000 token 上限或估算系数。
- build_a1_research_packet 中 snapshot fallback digest、body hash、budget fixed-point diagnostics、raise_on_budget 与 assert_packet_budget 路径继续原样调用这些 helper，没有改包构建/投影/候选数/决策数。

实际调用者仍是 research.common._prompt_replacements 构建 A1 packet，研究 runtime 读取嵌入 diagnostics。没有改这些调用点，也没有把模型请求的 canonical_json 字符串 API 替成其他 wire 格式。包 body hash 的原计算时点（budget metadata 之前）未改变。

## 等价反例与测试

新测试内明确保存旧 json.dumps/hash/diagnostics/token 实现作为 oracle，不靠新 helper 互相证明。验证 None/bool/int/负零/极小极大 float/NaN/±Infinity；ensure_ascii=False 中文/emoji/é/U+2028/U+2029；0–31 控制字符、引号、反斜杠；default=str 的日期/Decimal/Path/bytes；tuple、排序数字键、长单字符串。逐项比较 join 的 UTF-8 字节、SHA、len、ASCII/nonASCII 字符数与 token。

循环引用、混合不可排序 key、非法 tuple key 维持同类 ValueError/TypeError；孤立 surrogate 的字符长度可测，UTF-8 digest 与旧实现同类 UnicodeEncodeError。不改变特殊 float 为严格 JSON 拒绝。检查最大 section 的稳定顺序、前 12 截断、packet_hash 只被 section 排除、diagnostics 被总包排除。

测试禁止 stable_digest / packet_diagnostics 调 canonical_json 整串，监测 total measurement 仅一次；section-only 反例禁止调用 ASCII measurement。实际旧包 fixture 的构造器对照包括完整 packet dict、packet_hash、coverage/budget fixed point，以及 token 上限为 1 时的异常内容和属性。原合同/packet/discovery repair/coverage 切片通过。

首次新测试：18 fail / 4 pass，exit 1，含旧峰值无下降与整串调用的真实反例。实现后 22 pass；扩实际控制字符和整包/异常对照后，五文件共 72 pass / 0 fail / 0 error / 0 skipped。root 性能审查追加 section 不扫 ASCII 反例：1 fail / exit 1；加入 length-only helper 后最终新文件 25 pass / exit 0。之后没有重复全量或五文件大切片；最终统一集成/全量由 root 负责。

## 组件分配与带采样 elapsed

1500 条中型 JSON records，完整输入 1306677 个 canonical 字符，SHA `92e4d95dad730b087f31986f9e9ba5ee0dc10b2b9a12b3d0a6b8c35b6a88f65d`。同一 packet 对象被保留并复用，条数不变；构造在 tracemalloc 窗口外，没有 gc、delete 或减少样本。

| 操作 | 旧 traced 峰值 bytes | 最终新 traced 峰值 bytes | 旧 traced elapsed s | 新 traced elapsed s |
| --- | ---: | ---: | ---: | ---: |
| SHA256 | 11130698 | 10741 | 0.028531700 | 0.056739700 |
| diagnostics | 11130722 | 12436 | 0.418501300 | 0.448803700 |

两边 hash、diagnostics 全字段一致；峰值包括操作期间新增分配，不包括已存在的 fixture 图。表中 elapsed 是 **带 tracemalloc 的本地组件时间**，不是自然请求 wall time。tracemalloc 会放大 Python 迭代开销；峰值降低不能解释成 CPU 更快，也不能替代 OS RSS 或 A1 full-run lifetime peak。

最终组件属性在 `artifacts/wp5-20261010/a1-streaming-final-v2.xml`（record_count/input_packet_chars/same_hash/input_canonical_sha256/旧新峰值/旧新 elapsed）。前轮 XML 原样保留，旧时点源码 hash 不能当成最终文件 hash。

## 未采样的一次实际 packet 构造器对照

新独立 artifact：`artifacts/wp5-20261010/a1-streaming-component-20261010T0256/`。small/medium 均由真实 build_a1_research_packet 方法构造**本地固定合成 fixture**；82 行业、20 canonical 月决策、一个 g0 symbol 不变。medium 仅增大 fixture 行业文本，不修改任何生产选择或计数逻辑。构造不计入下表，每操作仅一次，未开 tracemalloc、未强制 gc、未删样本。

| fixture | canonical chars | 旧/新 hash ms | 旧/新 diagnostics ms | 同 hash/diagnostics |
| --- | ---: | --- | --- | --- |
| small | 47199 | 0.663 / 1.985 | 2.830 / 3.278 | 是 |
| medium | 251882 | 2.404 / 3.830 | 14.814 / 13.847 | 是 |

这是最终 length-only 版的单次对照，不据此推断稳定提速。small hash/诊断更慢；medium 本次诊断较快，但 hash 仍更慢。原版流式（section 也统计 ASCII）的独立回执保留：small diagnostics 2.631→3.472ms；medium 13.492→21.764ms。去掉 section 的多余扫描有具体路径依据，前后 packet 字符数及 SHA 完全一致，不把不同 fixture 的时间混成改善。

small SHA `b8b5e2a01236ffca097ebdf2cf80e69b8697e9b8a7b553869c8c6b9b273f1165`；medium SHA `8611fa4c88014558b6f8dfcab7459aa05e02987ad681b6514ba601acf298e914`。medium 为 115620 estimated tokens，超过既有 100000 上限，原 within_budget=false 保留；它只用于组件测量，不是获准送模型的包。small 为 11966 tokens。

`untraced-packets.json` SHA `d8eab8d552b887692d776226d0c3f0fc14afd5b401366a815d0fe01599b4ba11`；最终 `untraced-packets-v2.json` SHA `bfbd1ac86a2090c481dd783f22ea7f838d010a57f753aca1354bab8b4c221c09`，含最终 code/fixture/script bindings 和旧回执 SHA。scope 明确 `NO_FULL_A1_NO_LINUX_PEAK`。

## 实际命令、退出码与文件绑定

cwd 为本 WP5 worktree，所有 Python 命令先 `$env:PYTHONPATH=(Resolve-Path src).Path`，解释器 `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。末尾统一保存 native code：`$taskExit=$LASTEXITCODE; Write-Output "PYTHON_EXIT=$taskExit"; exit $taskExit`。

| Python 参数 | 回执 / exit |
| --- | --- |
| -m pytest tests/test_a1_streaming_diagnostics.py -q --junitxml=artifacts/wp5-20261010/a1-streaming-before.xml | 18 fail / 4 pass，1；红 XML 保留 |
| -m pytest tests/test_a1_streaming_diagnostics.py -q -o junit_family=legacy --junitxml=artifacts/wp5-20261010/a1-streaming-first.xml | 22 pass，0 |
| -m pytest tests/test_a1_streaming_diagnostics.py tests/test_a1_contract.py tests/test_a1_packet.py tests/test_a1_discovery_repair.py tests/iteration/test_a1_coverage.py -q -o junit_family=legacy --junitxml=artifacts/wp5-20261010/a1-streaming-final.xml | 72 pass，0 |
| -m pytest tests/test_a1_streaming_diagnostics.py -q -k medium_records -o junit_family=legacy --junitxml=artifacts/wp5-20261010/a1-streaming-component-profile.xml | 1 pass，0；增加 traced elapsed 属性 |
| artifacts/wp5-20261010/a1-streaming-component-20261010T0256/profile_component.py | 初版 untraced 回执，0 |
| -m pytest tests/test_a1_streaming_diagnostics.py -q -k section_lengths -o junit_family=legacy --junitxml=artifacts/wp5-20261010/a1-streaming-section-length-before.xml | 1 fail，1；section 多余扫描反例 |
| -m pytest tests/test_a1_streaming_diagnostics.py -q -o junit_family=legacy --junitxml=artifacts/wp5-20261010/a1-streaming-final-v2.xml | 最终新组件 25 pass，0 |
| artifacts/wp5-20261010/a1-streaming-component-20261010T0256/profile_component_v2.py | 最终 untraced once 对照，0 |
| git diff --check | 0 |

| 文件 | 最终 SHA256 |
| --- | --- |
| src/liangjian_funnel/pipeline/a1_contract.py | 17c8c44b43b909e990638dd21a3b4af4d78fa76c0f7bc6811a3653cff7337b68 |
| src/liangjian_funnel/pipeline/a1_packet.py | 658b2e25448e23fa38034411c98debea88b2a07d6782c6dda6002dcaf539d05e |
| tests/test_a1_streaming_diagnostics.py | 1971efa67ac8327400ac46fd53baf253b65c184ffb60f5c1f2f359fd2a779507 |
| a1-streaming-before.xml | 56216d737bc40a42728401950d1ace7f11bb872392ea4b35d3ec3975babbcaa5 |
| a1-streaming-section-length-before.xml | 24ecec2c45b4a8956e4d4e3acd591378dcc0b08022e4d275c2b43085d6b47094 |
| a1-streaming-final.xml（72 相关） | ef7a92034b577f5183a2afa6cbe3381af4d7cbf3088da7cdb05d19863f8ab709 |
| a1-streaming-final-v2.xml（最终25） | 4890f3133d296a2e41d19af93246dbb9760581ae6775451fe49785de39c9065a |

## 明确未完成的全链事项

iterencode 不是 constant-memory：sort_keys 可分配 mapping items 排序数组，递归遍历保留栈，大单字符串可以作为一个完整编码 chunk；digest 的该 chunk UTF-8 副本仍存在。输入 packet/snapshot 的完整对象、总包排除 diagnostics 的浅 dict、section 结果与预算 fixed-point 多次遍历都保留。canonical_json 的字符串调用者、prompt/request 文本和模型 SDK payload 仍可能创建完整字符串。

本轮没有节点/audit/output spool、append 索引、流式合并、释放跨 batch 输出、snapshot sanitize/write/read 全链改造或 resource observer 接线；不能说 A1 整体峰值已解决。没有改字段/范围/排名/策略/数量/限制/预算，没有 OOM/failure/full natural run/Linux cgroup/swap/PSI/RSS 证据。root 独立核 92MB 数据与后续统一验收；本切片源码/测试/本文件完成后冻结，不提交。
