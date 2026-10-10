# W4 close-scope JSON 往返修复候选

## 1. 结论与原失败

生产 `adafe5054b9ff283ea03925e2b0c6baf2146dcfe` 的 08:00 prep 失败源证据已冻结于 `artifacts/wp5-20261010/PREP_FAILURE_SOURCE_20261010.json/.md`。原 journal 只有 ValueError 类名，没有 traceback；systemd `4/NOPERMISSION` 是 exit4 的展示名称，不是权限失败证据。

同原两份回执的纯函数复现稳定得到 `CLOSE_SCOPE_RECEIPT_HASH_MISMATCH`：`early_discovery.py:31–32` 用整数键 5/20/60，producer 在落盘前排序为 5/20/60，读回后字符串排序为 20/5/60。只在内存恢复原 2004 个 `discovery.records[].evidence.ma/previous_ma` map，原 receipt/observation 两 hash 精确恢复；原文件 SHA 前后不变。不是空格/ascii 或脱敏问题，来源集合均相等。

本次开发基于本地 `fd0e872f1a018f570c3800654b2d318428ab9976`，不修改生产、原回执、workflow、池或阈值，不重启/重跑失败 unit。新源码尚未提交/部署。

## 2. 新 schema/3

`seal_scope_receipt` 仅新建 `close-scope-receipt/3`。递归检查键按 JSON 规则字符串化后是否碰撞；1 与 "1" 等碰撞、非法键或 NaN/Infinity 均拒绝。按实际写入器的脱敏规则取得 JSON 形状并 detach，然后计算 stable receipt_hash 与 observation_hash。包含时间的 observation 与不包含 recorded_at 的稳定身份仍分离。

读回验证仍严格检查两 hash，但不再对读到的对象二次脱敏，否则可能把持久字段的篡改掩盖。已添加专门反例。相同 v3 输入重试复用原文件及原 observation_time，原 bytes 不变；新 v3 不原地升级旧回执，不改变旧身份。

## 3. 旧 schema/1、/2

旧 v1 全文 hash 算法不变，篡改仍拒绝。旧 v2 先原算法验真；只有 stable hash 不符时，才在副本对 `discovery.records[].evidence.ma/previous_ma` 且键集恰为字符串 {5,20,60} 的 map 恢复整数键。恢复结果必须同时与原 receipt_hash/observation_hash 精确一致，不删除字段、不修数值、不重算签名来替换原值、不修改输入。

其他路径、其他数字键、额外 MA key、任一 hash 或任一其他内容改变，都没有“兼容忽略”。旧 v2 非 JSON 形状的公开 producer 行为不改，以保留原算法供验真。兼容层不是来源认证或执行资格。

## 4. 消费者与修改范围

`disclosure_maintenance.build_maintenance_queue` 只调用 `validate_scope_receipt`，没有 close-scope schema 硬编码，因此无须改维护业务函数。原 SHADOW 队列仍 `execution_authority=false/changes_query_scope=false`，候选/延期分区、来源集合、A1 绑定和市场日期继续检查。

只改 `pipeline/close_scope.py`；新增 `tests/test_w4_scope_json_roundtrip.py` 和本文。root 另明确授权既有 `tests/test_wp5_disclosure_maintenance.py:43` fixture 一行允许 schema2/3，用于正确重签该测试自己修改的 recorded_at，业务 assert 不变。其余源文件与共享状态未动。

## 5. 实际测试与原件只读校验

反例先行：新测试原实现 9 failed/9 passed，native exit1，`scope-json-roundtrip-before.xml`。首次修复 72 passed exit0；追加“读回二次脱敏可能掩盖篡改”反例 1 failed/18 passed exit1，`scope-json-roundtrip-strict-before.xml`，修复后：

```powershell
$env:PYTHONPATH='D:/dev_A股/liangjian_wp5_hotfix_20261009/src'
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' -m pytest tests/test_w4_scope_json_roundtrip.py tests/test_wp5_close_scope.py tests/test_wp5_disclosure_maintenance.py tests/test_wp5_disclosure_prefilter.py -q -o addopts='' --junitxml=artifacts/wp5-20261010/scope-json-roundtrip-final-v2.xml
```

73 passed，native exit0。覆盖 producer→atomic JSON→validator、retry、维护 queue、v1/v2、严格双 hash、范围外键拒绝、碰撞、nonfinite、脱敏后持久值篡改。

```powershell
& 'D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe' artifacts/wp5-20261010/scope_json_roundtrip_real_readonly.py --output artifacts/wp5-20261010/scope-json-roundtrip-real-candidate-run2.json
```

真实原两份回执定向只读传输，在本地候选源码验证旧 v2，并完成真实 maintenance queue build+JSON往返 validate；native exit0，原文件前后 SHA 相等，1636 selected/1035 candidate/601 deferred，保留原两 hash。队列只在内存生成，未写生产或本地 queue；raw 对象未保存。run1 对应首次候选，run2 对应最终严格候选，独立保留，不混版本。

## 6. 验收边界

源码合同与相关本地测试通过；实际失败输入的本地纯合同修复获证。原运行的异常文本仍未留存，不将复现当原 traceback。没有调用 workflow/provider/model，也没有证明重跑准备链、A2/A3 发布或后续模型业务成功。部署及失败 unit 重跑仍由 root 在独立批准后执行；此次仅候选包，原失败源证据永不覆盖。
