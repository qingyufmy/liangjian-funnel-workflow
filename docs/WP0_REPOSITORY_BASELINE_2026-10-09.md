# WP0 仓库收敛与测试基线

## 1. 真实原因

隔离分支 `codex/wp0-repository-baseline-20261009` 从实际 HEAD `89e6a2d92d5e2886cdaecc54c359559555109720` 开始。原工作目录的用户未提交文件未修改。任务书在原目录 `docs/CODEX_TASK_BRIEF_2026-10-09.md`，没有将其一并提交。

`artifacts/wp0-20261009/worktree-inventory.json` 记录五份旧副本的 HEAD、分支、未提交路径和差异提交。五个 HEAD 均已进入当前历史，差异提交均为空；已建立本地 `archive/<目录名>-20261009` 标签。`liangjian_funnel_workflow` 和 `liangjian_sources_20260923` 有未提交文件。标签不能保存这些文件或被忽略的虚拟环境，因此五个目录均未删除。

首次全量 Python：2,230 通过、6 跳过、1 失败，退出 1；见 `artifacts/wp0-20261009/pre-split-pytest.xml`。失败为 `test_late_response_is_not_decision_input` 的两值时钟耗尽。首个组合命令又发现 A5 测试把已消耗时间后的预算断言为精确 600 秒，见 `full-before-split/evidence.json`。未改 A4/A5 的业务逻辑。

## 2. 代码修改

| 修改 | 对应验证与修复前结果 |
| --- | --- |
| `scripts/test_all.ps1`、`test_all.sh`、`full_test_baseline.py` | 固定运行 Python、Node 测试和类型检查，记录各命令退出码、失败清单、合并计数、HEAD/源码树及不可变日志哈希；基线门测试初次缺模块导致收集失败，退出 2 |
| `deploy.sh` | 拉取后、安装/重启前验证待发布提交的完整测试回执及日志；读取目标提交内的校验器，不依赖旧版本已安装新脚本；不接受脏源码、错误 HEAD、缺步骤或改过的日志 |
| `tests/test_wp0_baseline_gate.py` | 反例先行：测试过程中 HEAD/tree 改变时旧校验器错误放行，2 失败/8 通过、退出 1；加校验后通过 |
| `tests/test_remaining_repairs.py` | 原断言保留，严格 `xfail(raises=StopIteration)` 并记录原因；其他异常/断言失败仍失败，意外通过也失败 |
| `tests/test_a5_daily_review.py` | 冻结该测试的 `monotonic`，保留精确预算及幂等断言；实际倒计时预算代码未变 |
| `pipeline/research/{a1,a2,a3,common}.py` | 从 13,329 行文件机械移动 79 个阶段函数/方法；函数体、签名、装饰器和外部导入关系不变；`workflow.py` 未改 |
| `research/__init__.py` | 兼容原导入、类路径和共享依赖注入；旧测试 monkeypatch 仍作用于同一个命名空间 |
| `verify_research_equivalence.py`、`split_research_mechanically.py` | 原源文件 SHA、函数 AST/签名及解析后的导入图写入 manifest；改变函数体/默认参数的反例能被检测 |
| `audit_preopen_readonly.py` | 同时支持旧单文件和新 package 的源码哈希核对，增加四个实际阶段模块；本轮未连接生产运行 |
| `inventory_worktrees.py`、README | 仅本地标签和清单；README 不再硬编码旧仓库目录；部署根目录从脚本位置解析 |

## 3. 测试与回放

所有命令工作目录：`D:/dev_A股/liangjian_wp0_20261009`。解释器只读复用旧目录 `.venv/Scripts/python.exe`，并将 `PYTHONPATH` 指向本 worktree 的 `src`；未迁移或删除该环境。

```powershell
python scripts/inventory_worktrees.py --primary D:/dev_A股/liangjian_a4_20260923 --tag --output artifacts/wp0-20261009/worktree-inventory.json
# exit 0; 5 标签，差异提交 0，有未提交文件的目录 2

python -m pytest tests/test_wp0_baseline_gate.py tests/test_a5_daily_review.py::test_frozen_retry_uses_original_evidence_and_is_idempotent -o addopts='' -q
# exit 0; 11 passed（切片，不与全量相加）

scripts/test_all.ps1 -PythonPath D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -OutputDirectory artifacts/wp0-20261009/full-before-relocation-final
# exit 0; 2345 total = 2338 passed + 6 skipped + 1 strict xfail

python scripts/split_research_mechanically.py --execute
python scripts/verify_research_equivalence.py --manifest artifacts/wp0-20261009/research-relocation-manifest.json --output artifacts/wp0-20261009/research-ast-equivalence.json
# exit 0; 239 definitions / 97 external edges; missing/extra/changed/import_diff 均 0

python -m pytest tests/test_wp0_research_relocation.py -o addopts='' -q
# exit 0; 4 passed（切片，不相加）

scripts/test_all.ps1 -PythonPath D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe -OutputDirectory artifacts/wp0-20261009/full-after-relocation
# exit 0; 同样 2345 total = 2338 passed + 6 skipped + 1 strict xfail

bash -n deploy.sh
bash -n scripts/test_all.sh
git diff --check
# exit 0
```

原研究文件 SHA256：`6ee3fccf3656b85eaea0098d528e45183487c5bdc79e31f07de2033e16f0b649`。拆分前最终回执 SHA256：`2293bf6371784809936699905fa5a2175fdf12910dec8612bd6e35f72cd32212`。

拆分后回执 SHA256：`69cbbda13e900ae18165620cf355636fe052ad3031ccbe4933298cff5752c019`。Python 为 2,244 通过、6 跳过、1 严格预期失败，Node 为 94 通过，类型检查退出 0；前后计数及失败清单一致。

回放不适用：本包没有改策略、重算行情、调用模型、通知或订单。完整测试回执明确区分测试通过与发布资格：开发时源码未提交，`release_qualified=false`，不能用于部署；发布前须针对最终干净的待发布 HEAD 再生成回执，传输回执和三个日志并给 `deploy.sh` 两个 `LIANGJIAN_TEST_EVIDENCE*` 参数。

## 4. 四层验收

- CODE：拆分前后全量均退出 0、计数相同；AST 等价通过；包含 1 项已知严格预期失败，不宣称该 A4 时钟测试缺陷已修复。
- REPLAY：不适用；静态 AST/测试不是自然交易回放。
- OPERATIONS：待授权及证据；没有发布、重启、生产数据库读写或自然调度验收。
- STRATEGY：不适用；阈值、策略版本和信号数量均未改。

## 5. 未完成边界

决策 5 仍待用户确认：五份旧副本删除、主目录改名及虚拟环境迁移尚未执行。有未提交/被忽略文件需先另外保全，不能仅凭标签删除。各标签的完整 HEAD 与路径在库存清单；没有差异提交需要合并，但这不代表未提交内容已合并。

已知 A4 时钟测试问题只记录为严格预期失败，本包没有顺手改执行代码。`common.py` 仍约 8,946 行，本轮是第一刀，不借此扩张至业务重构。发布、自然调度及目录迁移后的系统计划任务路径，需要用户分别授权后验收。其余 WP2—WP7 未获实施授权。

## 6. 下一步

拆分后全量对照通过后并行启动 WP1 的离线反事实引擎与收益统计；只用已有冻结资料，不改生产参数，不执行未授权的 1—8 月历史扩采。
