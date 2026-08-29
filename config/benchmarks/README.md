# 券商月度金股盲测数据

本目录只保存导入格式示例，不保存未经核验的股票推荐数据。真实数据导入后默认落到
`storage/benchmarks/broker_gold/YYYY-MM.json`，仅用于 A1 运行后的独立盲测，不会进入
A1、A2 或 A3 的输入。

必填字段：`month`、`broker`、`symbol`、`source_ref`。`publish_time` 必须是不晚于回放
时点的可核验发布时间；未来版本会被点时过滤。建议优先配置“三中一华”等研究团队
公开发布的月度组合，但每一行必须保留原始链接或文档编号。

导入命令：

```text
liangjian-funnel import-broker-gold path/to/verified.csv --as-of 2026-08-29T18:00:00+08:00
```

系统会校验字段、代码、时间、重复记录，并按月份原子写入。未配置真实数据时，盲测
报告会明确写出 `BROKER_GOLD_NOT_CONFIGURED`，不会生成伪造命中率。
