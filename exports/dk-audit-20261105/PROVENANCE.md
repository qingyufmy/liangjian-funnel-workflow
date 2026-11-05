# 代码、版本与运行证据溯源

## 查找范围与保全

读取全局与A股AGENTS；在写入前记录主项目和旧A4目录的HEAD、完整status、tracked修改SHA及index SHA，原有6项主树修改、旧A4的2项修改和未跟踪文件均未合入本分支。源工作区已退役，因此从共同Git对象库保全ref及源码/总结备份读取，未重建旧Track2树、未启动旧任务或VM。

本机Git ref `refs/archive/d-cleanup-20261103/team-v2-track2-20261029`仍保存`8d3d6dda445a5a0d0116b21fddeab4d171888983`。逐文件来源以MANIFEST为准：GIT_BLOB保留完整来源提交、最近修改提交、Git blob ID、SHA256；LOCAL_UNTRACKED_DOCUMENT只有本机原路径/SHA，没有编造Git提交。

对本机当前源码/文档目录（排除行情、artifacts、依赖、密钥、Git内件）及全部Git历史检索DK/SSL/MKT相关符号。物理目录里主要剩任务书，真实后续实现位于保全轨道二历史；因此不能仅据生产目录没DK就否认开发过。

## 版本时间线

|时间/提交|类别|真实完成内容与边界|
|---|---|---|
|10-26附录A/B、10-27盘后任务书|设计|附录A曾取消无公式DK；附录B提出站点逆向网格和自研SSL后备；10-27晚暂缓并不代表10-30没有实现。附录A/B和10-28任务书的部分本机文档未跟踪，不冒Git版本。|
|019971f及后续MKT草稿|隔离实验|市场状态/站点DK1草稿，不能冒最终算法或生产接线。|
|0ee99683cae6ccaf272108f06a24a8b0ce1d2b52（10-29）|冻结定义|T2-1144停止追站点同款，冻结“自研、非站点同款”、SSL/SMA、N20及10/20/30比较；本提交文档明确纯函数、防重绘、20日和影子输出尚未实现。早期配置/文档原样在reference/definition-0ee9968。|
|38e8ed6be183dfd1870a810aa255ae8a0b1c6ff8（10-30 16:21）|实际算法与影子代码|首次新增ssl.py、dk_shadow.py及对应测试，腾讯ONLY闭合5m→15m、不可变增量与评估。|
|e59c4797407ec3a106e6f2ca380036dab35fb677（10-30 17:42）|PIT补强|分钟输入绑定真实源/目标日、provider/capture/as_of，新增独立组装及手算阈值负例；不是把旧不合规取数洗成成功。|
|f5c543f2d1f9552e71a77ee33a67332487c4f0da（10-30 18:48）|归档与采集日|新增dk_archive.py、first_seen/SHA、重叠修订拒绝；实际采集日期与回溯bar日期分开。|
|8d3d6dda445a5a0d0116b21fddeab4d171888983（最终保全点）|本轮导出版本|选择最新保全版本；逐文件last_change_commit更精确，不说每文件都在8d首次引入。该版本有历史短窗口六指数影子摘要；没有金融上线验收。|

旧站点逆向和最终自研是两个版本路线：站点锚点量2,390,800与参考2,389,400、收98.58与98.500不一致，不能把修复守卫或定义冻结当站点同款标定成功。后续不再跨源拼接，最终SSL仅腾讯未复权数据；早期dk_sources.py的东财/腾讯比较仅作溯源参考，不能用于绕过最终源校验。

## 历史证据能证明什么

本机根外源码/总结ZIP的四份摘要按原保全manifest逐件验SHA后，生成脱敏/裁剪的[HISTORICAL_EVIDENCE.json](HISTORICAL_EVIDENCE.json)，不上传原始行情、账户、持仓、数据库或敏感运行日志。

- `dk-six-indices-independent-audit-v1.json`记录六指数各106根15m、三个N的逐点独立核对。N20的SSE/SZSE/CYB/HS300/CSI1000/KC50均记录3个事件、2个预热后完整session；20日仍UNCOVERED，真实采集日1，5日NOT_COMPLETED，实际计划股UNPROVEN。它证明当时确有DK计算/影子输出的留档，不是本轮重新获取行情或证明盈利。
- 当时轨道二18个文件321项定向回归通过；**这不是321项DK专测**。CURRENT_REVIEW_INDEX最终绑定8d3d6，deployed=false；H/R full、三业务批、D5、当前F7/D8及FINAL仍未通过。
- 这次114项原测试/包装定向验证是新的受控证据，和上述真实短窗口历史摘要分开。未导出大输入，故本轮不能独立重算历史106根的实际数值。
- 没有找到DK作为实际研究排序、A4硬门/退出、A6/PM权限或已部署生产策略的证据。未访问VM，不宣称今天已对生产做新鲜检查。

## 本次导出与旧实现的差异

原snapshot/reference字节保留；原测试也未改断言。新增bootstrap仅创建原模块名的局部命名空间，避免执行无关pipeline/data包级facade导入整个生产依赖。原__init__.py仍保留供对比，但本离线入口不执行它们；这不是假ModelClient，也没有替换DK函数。

新增例程只接收调用方已有、SHA核对的Bar15m JSON，不生成行情，不视任意“source_id”文本为供应商证明。缺数据就报错；示例边界测试复用原test_market_state_ssl.py的受控fixture，并明确不是真实市场。新测试runner用独占临时目录并在同任务清理，socket/DNS/bind审计守卫会拦截网络尝试。
