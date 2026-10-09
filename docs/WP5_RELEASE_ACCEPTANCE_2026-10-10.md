# WP5 精确发布验收：2026-10-10

## 结论

Tony 06:55 授权的 `adafe5054b9ff283ea03925e2b0c6baf2146dcfe` 已发布到 origin/main 和虚拟机；既有 `deploy.sh` 于 07:04:17 退出 0。没有包含 WP1/WP7 的 `63ccc8c`，没有修改生产 env、切换候选公告范围、运行延迟研究、调用模型或发送交易通知。

生产：`aurum-vm → 192.168.1.254`，hostname `debian`，目录 `/www/wwwroot/Agu/liangjian-funnel-workflow`。发布前 HEAD `47f6ef03b042c0f9a207400c72d2ecffff338244`，发布后 HEAD 为批准的完整 SHA。

## 代码与实际运行

原全量回执：3113 total / 3106 passed / 0 failed / 6 skipped / 1 xfail，exit 0。回执 SHA256 `60ed07c2a59ab89078a94921dbe77e1af2b81e56e2241cdf9e7f0c384481a321`。在旧版部署脚本开始前，用目标提交自身的验证器核验原始 Python、Node、typecheck 日志，exit 0，未以手写摘要替代原回执。

发布后只读核验 32 个变更 Python 模块：实际 site-packages 字节哈希全部与 src 一致。Node PID 80561、cwd 正确、scheduler_enabled=true，启动日志确认调度器启动；health HTTP 200。未过期活动租约为 0，旧 close 租约的 ACTIVE 字样已经过期，不等同于仍有活动进程。未修改另一套 aurum-ai 的 Node 进程。

实际 Settings：disclosure scope=SHADOW，rotation membership=LOCAL_REFERENCE，quote backup=SHADOW；研究、监控、复盘模型均 deepseek-v4-pro。没有切 CANDIDATE_DOMAIN，也没有让资金标签授权隐式切换原评分政策。

## 业务准备边界

当前 A1 活动代际 `a1-incremental-20261009T180041188447Z-a2074f8fdabf` 为 SEALED，快照 `snapshot-20261009T191648+0800-3289851d3f2d`，上一晚维护 SUCCEEDED。4264 为代际输入分区数，1506 为维护回执 selected，不将二者混称股票池数量。

**2026-10-12 正式计划查询为 0，仍未完成周一盘前准备。** 周末 run-next-session-prep 是独立决策 9，本次未获授权、不执行；不激活过期计划。10-09 午间和盘后 A5 投递账本均 SENT，是旧版本历史业务事实，不是新版本自然调度验收。

## 两个真实阻断

1. 决策 10 精确授权的 `/www/tmp` 不存在。实际 benchmark 原命令退出 3，`PARENT_NOT_EXISTING_DIRECTORY`，iterations=0、bytes_written=0、cleanup=NOT_REQUIRED。`/tmp` 是 tmpfs，不替代生产盘基准。建议另批准既有 www 所有且可写、与 state 同 `/dev/sda1` ext4/dev2049 的 `/www/wwwroot/Agu` 为父目录；使用原脚本 UUID 子目录，不改权限、不测忙时。
2. 决策 12 六日原快照：94 个激活 plan-days 的 source run/研究哈希已对账，但指定 storage/snapshots、storage/facts 内原引用及精确标识只读探查均未找到。copy allowlist 为空，复制脚本退出 2，`EMPTY_PROVEN_COPY_ALLOWLIST`，实际 0 文件/0 字节。缺文件不能用研究摘要、次日快照或新行情代替。只说明获批范围内未找到，不断言全部备份均不存在。

## 证据与命令

目录 `artifacts/wp5-20261010/release-adafe50-20261010T0703/`：`commands.json` 记录 push、逐文件 scp、目标验证器、既有 deploy.sh 的实际退出码；`deploy.log` SHA256 `4bb3a6a21623c86aa6f7d9067a1974b678c3f474703ba74f7e0e537e02fc02e3`。

`production-acceptance-readonly.json` SHA256 `2ae4f3177d5d8f5dea501c204309161fe8ed0b5841f19113fe863a7a06a6f28d`；无 RuntimeStore、SQLite mode=ro、无 DML、无模型、无数据源采集、无通知。

`vm-fsync-latency-idle.json` SHA256 `ca539009838867cf6f7d584a6344bf074d063184110dca8f11aa742b73487eed`，命令及 native exit 3 单独留存。

六日复制证据位于 `D:/dev_A股/liangjian_wp1_20261009/artifacts/wp1-20261010/raw-source-copy-plan-20261010-run1/`；manifest SHA256 `4dbdddc33a0d72330e02e60979d3a6113b95d43015fd21732c6eac64d77260f0`，copy acceptance SHA256 `3c5fd3b41728a7f2703d3e5a62fbbeb9ab64cf8a87d1c2fea65ff2ad090f131c`。本地原数据库前后 SHA 相同，无新 journal/WAL/shm。

## 四层验收

- CODE：批准提交的全量回执与生产源码一致，已发布。
- REPLAY：原离线证据保留；六日缺原快照，15 日收益/成交因果性未解锁。
- OPERATIONS：安装、Node 启动、调度开启、只读环境验收通过；新版本自然交易日与周一计划仍待证据。
- STRATEGY：不由发布或测试推定有效；没有改确认层、阈值、数量、权重或真实下单。

下一步：提交精确 fsync 父目录调整 gate；继续 A-LABEL/B-SHADOW 离线实现；决策 9 单独审批后才执行周末研究。
