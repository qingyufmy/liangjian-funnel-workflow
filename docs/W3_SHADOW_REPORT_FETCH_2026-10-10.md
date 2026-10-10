# W3 本机只读报告交付组件（未接线）

落实canonical inbox0066的独立纯部分，不安装调度、不开SSH、不改变生产 reporting/A4/provider。0066尚无答复；由Claude轮询调用还是本机工作日16:50跟踪，仍待工程裁定和决策14接线，不称Claude已收到报告。

## 实际生产者合同

`runtime/shadow_reporting.py::_finalize` 原输出最后写 `archive_root/<day>/final-manifest.json`：schema_version=`shadow-reporting/1`，trade_date、as_of、a5_status、draft_status、draft_sha256、outputs。outputs恰四件，键为原绝对路径、值为原bytes SHA256：`archive/<day>/report.json`、`docs/SHADOW_DAILY_<day>.md`、`docs/PRODUCTION_EQUIVALENCE_<day>.json`、`bridge/<day>/W3_SHADOW_DAILY.json`。bridge原receipt同时绑定前三文件outputs/report_sha256。现模板各路径仍显式占位符，不能猜实际发布目录。

新增独立 `scripts/fetch_shadow_report_readonly.py::fetch_report` 接收注入 `reader(path)->bytes`，显式remote_root/archive_root/report_root/bridge_root/date/manifest原bytes SHA/local shadow_outbox/aware observed_at。仅接daily合同，weekly未实现。remote子根必须在同一个显式独立根内且互不包含；部署若不满足须由根代理明确选择，不自行放宽。manifest必须同日、有时区且不晚于本机观察时刻，路径集合必须准确匹配四件；错日期、额外文件（含数据库）、traversal、重复JSON键、错pin均拒绝。

每件最多1MiB，整个读取包最多10MiB（含manifest的二次读取），仅传输上限，不是策略阈值。bridge原outputs/report hash、report原trade_date、report内production_equivalence与独立proof逐项核验，最后再读manifest确认bytes未变。以上全部完成，且全部既有本机目标无冲突后，才创建输出目录；源失败不产生本机包。

本机输出必须显式绝对 `shadow_outbox`，允许 `.claude_bridge/shadow_outbox/<day>/`；拒inbox/outbox/state祖先、`..`、symlink、Windows reparse/junction、硬链文件alias。五原小件原bytes保持：四件和final-manifest；新fetch-receipt最后写，只证明BYTES_VERIFIED，含original→local pin映射，不声称生产等价/PIT/收益已通过或Claude review收到。写入O_CREAT|O_EXCL（0600）后flush/fsync；samebytes重试ALREADY_PRESENT，任何conflict拒覆盖。磁盘写失败可能留下已校验的新部分小件，但无最终receipt；下次逐件校验后补齐，不能把部分目录当交付完成。不是跨文件原子事务，亦非hard realtime。

## 独立 SSH 入口（本轮未调用）

`ssh_reader`固定host `aurum-vm`，BatchMode=yes、StrictHostKeyChecking=yes、ConnectTimeout=5、ConnectionAttempts=1；不增加密钥、不修改known_hosts、不关验证、不改源限流。总预算默认30秒（允许显式组件1..45），单次SSH最多8秒/剩余预算；失效后不再起请求。默认CLI不构造SSH reader，除非调用者显式 `--enable-ssh-readonly`。

远端只执行内联 `python3 -c`：拒realpath alias，以O_RDONLY|O_NOFOLLOW|O_NONBLOCK打开常规文件，读前大小门禁、读前后size/mtime/inode/dev核验，stdout只输出原body的base64和SHA/path。无远端文件上传、落盘、provider、数据库、通知或模型动作；本机重新核base64 bytes/pin。不会输出remote stderr异常正文或密钥。SSH配置中的固定host实际地址/用户与发布目录尚未在本轮远端实测，不把本地fixture当网络验收；需已合法known_host/python3和发布包确定的独立roots。

未来调用必须逐项提供：`python -B scripts/fetch_shadow_report_readonly.py --remote-root <published-independent-root> --archive-root <published-archive> --report-root <published-docs> --bridge-root <published-shadow-outbox> --trade-date <explicit-ISO-day> --manifest-sha256 <producer-final-manifest-original-byte-SHA> --shadow-outbox <absolute-local-shadow_outbox> --enable-ssh-readonly`。本轮未执行该命令，未配置任何真实root/date/hash，未注册计划任务。manifest pin须由明确原receipt取得，不以本机canonical reserialization冒充原SHA，也不把本地hash视作源认证。

## 实际测试与四层边界

Python `D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`；cwd WP5、PYTHONPATH本树src，每次末尾保存/打印/返回 `$LASTEXITCODE`。

- 新测试先写，`pytest tests/test_w3_shadow_report_fetch.py -o addopts='' -q --tb=short --junitxml=artifacts/w3-fetch-20261010/before.xml`：模块不存在，collection error/native2。不是声称8个旧实现语义反例已执行失败。
- 实现后同命令 `after-v1.xml`：26 passed/1 failed，native1；真实W3 producer fixture子目录未mkdir，保留失败回执，仅修fixture。
- `pytest tests/test_w3_shadow_report_fetch.py tests/test_w3_shadow_reporting.py -o addopts='' -q --tb=short --junitxml=artifacts/w3-fetch-20261010/final.xml`：55 passed/1 failed，native1；新reparse stub遗漏st_mode，保留失败回执，仅修stub。
- 同相关命令 `final-v2.xml`：56 passed，native0，35新fetch tests。覆盖hash/日/未来/naive/额外或缺文件/duplicateJSON/traversal/冲突/大小/源失败/两次manifest变动/非法root/canonical保护/hardlink/reparse/SSH命令和回包/default未接线/samebytes幂等。实际W3 producer在临时SQLite fixtures运行，其report/doc/proof原bytes进入fixture transport；Windows→POSIX路径是明确合成映射，仅bridge paths和manifest因映射重签，非生产原件。

CODE：局部56回归，非clean HEAD full；REPLAY：只有本地fixture transport和本地真实producer代码，无远端报告；OPERATIONS：UNWIRED/未SSH/未调度/未部署；STRATEGY：仅字节交付，无计数/门槛/收益/模型政策变化，DATA_LIMITED原样保留。新文件冻结SHA和XML见 `artifacts/w3-fetch-20261010/freeze-manifest.json`；本代理不commit/push、也不写canonical bridge/state。
