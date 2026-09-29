# A1内存优化：2026-09-29

## 实施范围

保留现有未提交的A1补偿调度和A2/A3证据修复。此次只优化内存使用，不改变股票池范围、模型审核、提示词内容、策略阈值或历史证据。

1. `pipeline/research.py`：原生JSON快照不再递归复制成第二份dict/list树。含日期、Path、模型对象、非字符串键等特殊值仍使用原有标准化语义，不缓存可变输入。
2. 研究哈希及`pipeline/a1_registry.py`注册表哈希采用流式计算，避免完整JSON字符串与UTF-8字节副本同时驻留。
3. 提示词占用统计采用流式字符计数，不为统计再次构造完整JSON字符串；长度口径不变。
4. 新增`scripts/profile_a1_serialization.py`，只读本地冻结快照，无模型、网络、生产数据库或通知调用。

## 真实冻结数据实验

输入为9月28日生产快照的只读副本，大小92154548字节。每种模式在独立Python进程执行，文件解析完毕后启动tracemalloc。测量的是序列化阶段的额外Python分配峰值，不含已加载快照、不等于进程总RSS或A1全程峰值。

| 模式 | 额外分配峰值 | 追踪开启耗时 |
|---|---:|---:|
| 原实现 | 284633006字节（约271.45MiB） | 6.730秒 |
| 优化后 | 244024字节（约0.23MiB） | 9.203秒 |

两种模式的SHA256均为`e8bbe3734e35100c67fdf92f6af78dc0600aba7e58033b1c5c2f42c79acde75a`，与生产快照标识一致。流式实现用一定CPU开销换取峰值内存下降，不能将追踪模式耗时当作生产延迟结论。

实际命令（均设置PYTHONPATH=src）：

```
python scripts/profile_a1_serialization.py artifacts/readonly-snapshot-20260928.json --mode legacy
python scripts/profile_a1_serialization.py artifacts/readonly-snapshot-20260928.json --mode optimized
python -m pytest tests/test_research_streaming_hash.py tests/test_a1_registry.py tests/test_a1_packet.py tests/test_pipeline_research.py tests/test_a1_contract.py tests/test_a1_selection_logic.py -o addopts='' -q --tb=short
git diff --check
```

实际Python为`D:/dev_A股/liangjian_funnel_workflow/.venv/Scripts/python.exe`。两个剖析命令退出0；179项测试通过，退出0；diff检查退出0。测试覆盖原生对象不复制、特殊类型兼容、修改输入不复用旧哈希、字符数一致、注册表哈希一致及现有A1筛选/研究契约。

## 验收边界与下一步

- CODE：179项针对性测试通过。
- REPLAY：真实92MB快照序列化兼容通过；不是完整A1模型研究回放。
- OPERATIONS：尚未部署、未重跑生产A1、未修改VM内存或服务。
- STRATEGY：筛选和交易门槛不变，不宣称收益改善。

不能据本实验宣称4GB虚拟机已能稳定完成所有A1任务。下一次正式A1运行需记录数据同步、局部筛选、模型投影和代际发布各阶段的进程RSS/Swap峰值及系统可用内存，并保留成功发布回执；不能仅看到进程退出0就认定更新完成。
