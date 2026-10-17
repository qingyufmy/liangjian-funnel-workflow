# A2 3.0.2 新冻结调用仍拒绝：请求作者明确复制路径及枚举，不放宽证据门

`0283-reply` 五个新文件 SHA 全部相符，全文已读并按原字节复制封存。A2 主题覆盖已改为 scope 全键；非哨兵多余 search scope 只从派生判断剥离并记 `WARN_EXTRA_SEARCH_SCOPE`，原响应保留。234 项关联测试通过。

新调用 `a2-frozen-1014-author302-real`，实际 09:44:51 准备、09:45:27 结束，provider 延迟 31513ms、1 次调用，原始结果 BLOCKED：

- `A2_MARKET_READ_INVALID`：emotion_cycle=`ICE_POINT`，不在作者要求的八阶段枚举内。
- market_read.regime 复制了整个 MARKET_REGIME_SNAPSHOT 对象，而非其中 `regime=RISK_OFF_RETREAT` 字符串；提示词示例“复制 MARKET_REGIME_SNAPSHOT”有歧义，待上面问题消除后也会与信封绑定不符。
- `A2_ROTATION_DOMAIN_MISMATCH`：原响应自造 `SELECTED_BOARD:SHIPPING`，未逐字复制合同方向 ID。
- `TEAM_OPPOSING_REFS_INVALID:theme_reviews[2]`：CONSUMER_SERVICES 引用 `A2:QUANT:601579.SH`，实际目录没有这个 ID。

原 JSON 在独立 model.db 的 model_calls 中，原始 input_hash=76af5bcfc20ad2c454804da7fcacce29bcacf9cc92efeb630c454608abb6b11a，prompt_hash=90cdb1fd9a3c550ab6db1efb8be482bd25d799452d006e9f54b682fd6fae2ed9。没有默认通过、丢弃无效 review 或把旧响应改判。

最小建议：作者把 regime 的复制路径写为 `MARKET_REGIME_SNAPSHOT.regime`；market_read.emotion_cycle 明列八值并禁止照搬 ICE_POINT；方向 ID 与 refs 仅可逐字取对应合同与目录。请作者决定提示词修订，不自行重写角色/规则。其余独立工程继续。
