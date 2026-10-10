# W1 独立影子进程：安装布局下的来源定位

日期：2026-10-10。本切片仅修正 shadow reader 的 helper 定位和源码回执，不修改 A4、策略、影子事务算法、PIT 源或调度。已完整阅读 canonical outbox 0041–0044。没有安装包部署、VM/生产访问、网络、模型、通知或交易操作。

## 接口与边界

`ReadOnlyShadowSource(state_db, minute_db, *, lanes, monitor_latest=None, checkout_root=None)` 新增显式 checkout 根目录输入。`source_hashes(repo_root=None)` 保留原 canonical relative-path → SHA256 返回形状。

默认兼容仅适用于实际 imported `shadow_session.__file__` 位于严格的 `src/liangjian_funnel/runtime/shadow_session.py` 布局，且根目录内 `pyproject.toml`、session 源文件和既有 audit helper 确实存在。site-packages 没有显式根目录时返回 `SHADOW_CHECKOUT_ROOT_UNWIRED`；不搜索 cwd、环境变量或父目录，不假设安装包中存在 scripts。错误目录、缺源码、指向根目录外的引用分别 fail closed。

显式 checkout 是工程源引用，不是行情或发布证书。生产是否安装了这套代码、是否存在正确 checkout、是否连接 monitor marker 与 PIT/arrival provider，仍须独立验证。本切片不能据本地 fixture 宣称实时可用。

## 实际加载与 hash

session、workflow、strategies、decision_observability、shadow_variants、shadow_evidence 均从实际 imported module 的 `__file__` 读取源码字节，记录真实绝对路径、模块名、SHA256。安装路径与 checkout 不同的情况下，逐文件要求两处字节相等，否则 `SHADOW_IMPORTED_CHECKOUT_SOURCE_MISMATCH`，不能用 checkout 的 src 副本冒充 imported 源。没有可读 `.py` 原件时明确 `SHADOW_IMPORTED_SOURCE_UNAVAILABLE`，不以零值或猜测 hash 代替。

CLI 与 audit script 单独标注 `EXPLICIT_CHECKOUT_CLI_NOT_IMPORTED` / `EXPLICIT_CHECKOUT_AST_HELPER_SOURCE`，不冒称 imported Python 模块。poll 回执新增 `source_references` 和 `audit_helper_source` 元数据，旧 key→hash 回执保留。Session 构造时再次核对 Source 初始化记录，期间漂移拒绝启动。这里记录的是实际路径上的源码字节，不是操作系统加载映像或第三方依赖完整 attestation；部署文件应保持不可变，仍需部署端验收。

helper 不执行整个脚本。只从明确 checkout 的 `scripts/audit_frozen_a4_decisions.py` 原字节解析 AST，提取现有 `replay_clocks(payload, stamp)`、`frozen_market_overlay(context, strategy)` 两个无 decorator/default 的顶层函数，以正常 module 承载；实际两个函数仅依赖注入的 datetime 与受限 builtin。原文件 SHA256 与选中 AST SHA256 均记录，读取后再次核对原字节。脚本 import、顶层语句、main、REMOTE 均不执行；未新增第二套 replay/market 算法。

## 红绿验证

Python 绝对路径：`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。工作目录：`D:/dev_A股/liangjian_wp5_hotfix_20261009`。仅本地 tmp_path 小 fixture，没有读取生产数据库。

反例命令：

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_w1_shadow_installation.py --junitxml=artifacts/wp5-20261010/shadow-installation-before.xml
```

旧实现真实 exit 1，9 failed（0.63s）：模拟 site-packages 布局会猜错 scripts 路径；缺少显式接口/真实源引用。XML 保留，不改成成功。

最终命令：

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_w1_shadow_installation.py tests/test_shadow_session.py tests/test_w1_shadow_session_wait.py tests/test_w1_shadow_commit.py tests/test_w1_shadow_variants.py tests/test_w2_shadow_evidence.py tests/test_audit_frozen_a4_clocks.py --junitxml=artifacts/wp5-20261010/shadow-installation-final.xml
```

实际 exit 0，164 passed，11.55s。9 项安装布局反例覆盖无显式 root、真实显式 root、checkout 兼容、两类错 root、installed session 字节漂移、实际 imported variants 漂移、缺 imported 源、script 顶层不能执行。其余 slice 覆盖既有 CLI、等待与事务、ledger、原 helper 时钟合同。

中间 `shadow-installation-v1.xml` 真实 exit 1，163 passed / 1 failed（11.37s）：新增真实中文绝对路径以 UTF-8 写入 JSONL，旧测试使用 Windows 默认 GBK `read_text()` 解码失败；CLI subprocess 本身 exit 0。root 明确授权仅将该测试行改为 `read_text(encoding='utf-8')`，保留真实路径，不隐去来源。本切片未修改 CLI；root 的显式 `checkout_root` CLI 接线和其测试由 root 独立负责。

## 冻结 SHA256

| 文件 | SHA256 |
| --- | --- |
| runtime/shadow_session.py | `2a27c37dde0ada6564b63744cd08c784c06f8dac3432fdd08473a9f8b700bbb8` |
| tests/test_w1_shadow_installation.py | `c6e55ae78ec7332242aface30a49ddee08d5463d6ef9453e26e983960ff692c6` |
| tests/test_shadow_session.py | `f979fa2657f273202e49b689b319926fb0bfb384f08bcdd739814bf60d527a1f` |
| shadow-installation-before.xml | `bc0517e098c3978584cb21ff1b8a6a642f71d006654372d21f0f98c755adaa48` |
| shadow-installation-v1.xml | `78fca8c8c1b4495e07625daf6b2e3944820f4c126f334f2f052372a349a69c79` |
| shadow-installation-final.xml | `48eaedd788eb4690d9eaa5fe782501a2f259e31ae96bd61fef499edcf9f7d5d6` |

本轮复核未变：shadow_variants `adc1de89ecc1661de813ebaa8394718c204dad0807c78fd91e8c226c382ed76a`；workflow `7bf62d19640973f102830f612be2e1dea05812dfaf4d7359b4a4605b1bb30c09`；strategies `079600edc90a6cdb69060ea272a63a46fe2cd19e61ea2746ccb43bb3840b05b3`。`git diff --check`（session 与授权旧测试）exit 0。

四层状态：源码接线 IMPLEMENTED；本地相关测试 PASS；安装/VM运行 NOT_RUN；自然业务分钟/PIT/成交证据 DATA_LIMITED。源码 hash 通过不是生产运行成功，也未消除默认未接线 provider 的缺口。无 commit/push。
