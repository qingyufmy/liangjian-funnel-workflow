# 10-21 前端本机实现与交付记录

## 0444 第三轮返工（10:54 本机验证）

0444 三项修改已在独立前端 checkout 落地：未知角色只显示姓名，舞台标签/计划/记录改用批准文案；姓名使用相机投影放在各自工位下方，和 960px 窄屏舞台共享滚动；头部再缩10%，眼镜整体缩30%且线宽减半，桌沿按浅深主题使用 #C9D1DC/#3A4658，地面细网格透明度0.035。保留“未知账本”与“已确认空仓”的区别：前者“持仓将在开盘后更新”，真实空数组才显示“还没有持仓”，未把未知改成零。

本轮 Node 全量26文件/190项通过，前后端 typecheck、JS syntax 与 diff 检查通过。机械检测器缺HTML/CSS解析模块，只退化为regex，不能据此称完整对比度审计通过；现有告警样式警告不在本次批准修改范围，未知数字的“—”是事实占位而非文案破折号。根 PRODUCT.md 的旧 Register 字段未参与本轮决定，也未顺带改动。

`.impeccable/review/team-20261021-0444-r3/` 保存1440/1024/400浅深六张真实首页截图、400横滑与真实WEBGL_lose_context降级截图、92帧6.490374秒连续本机synthetic接力/拦截录屏。400px实测页面scrollWidth≤400，舞台960px内部横滑，姓名与工位同动；WebGL丢失后画布隐藏且六人文字降级仍完整。旧IAB标签截图超时，新建同源本机标签后截图正常，没有连接VM。保留原先局部历史/非生产约束；本轮没有独立连续动效终审，也没有新的Claude视觉ACCEPT。真实生产只读API与当天完整数据门仍欠。

当前交付为本机只读 Operate 样板，位于独立 checkout `D:/dev_A股/.team-v2-frontend-20261021`，分支 `codex/frontend-20261021`、起点 `fa2b9b9`。本轮没有 push 或 deploy，没有连接虚拟机或生产数据库。根 DESIGN.md 与既有控制台不在本轮设计范围。

最新视觉合同为 MAIN 的 `artifacts/frontend-role-design-20261019/claude-reference-20261021/{README_REFERENCE.md,HOME_REFERENCE.dc.html,DESIGN_SPEC.dc.html}`，以及 canonical `0441-notice-design-reference.md`、`0441-reply-b-character-professional.md`。它们覆盖冲突的旧 HOME、ROLE_CARDS_2.0 和旧调色决定。六角色 accent 已明确并落地，不再把配色写为待 Claude 选值。Impeccable 按此参考忠实实现与记录，不另选视觉方向。

本机启动：`node scripts/serve_team_preview.mjs`，首页 `http://127.0.0.1:8787/team`，角色评审 `/team/roles-preview`。仅监听 loopback。历史原件可由 PowerShell 设置 `$env:TEAM_UI_LOCAL_EVIDENCE_ROOT='D:/dev_A股/liangjian_team_v2_20261016'` 后手动载入；这是本机证据目录，不是生产实现。

当前源码已实现白色资产栏 → 深色交易室 → 今日计划与团队记录 → 持仓与挂单。原焦点、条件、任务和来源摘要移至“观测条件与任务证据”折叠区。最终内容宽上限 1280px；760px 以下单列。六工位按工作流形成浅弧，窄屏舞台内部横向滑动，保持完整连续曲线。

人物采用专业深色服装与六种小面积 accent：基本面 #8B78E0、题材 #3FA3AD、组合 #5C8BE6、风控 #A7B2C6、交易 #E0A62B、复盘 #55B282。最新整改修复浅弧朝向、连续 CatmullRom 接力曲线、金色圆点与光晕、剪影负空间、idle 静态姿态。接力沿曲线 720ms ease-out，减少动态时直接到位。配套视觉规范为 `docs/design-team.md`，扩展侧车为 `.impeccable/design-team.json`。

本机只读边界保持在实现中：未知数值为“—”，实际空数组与缺源分开；净值不由前端推算。演示默认关闭，SSE 明确 opt-in 且同源。无写接口和申请持久化；任务书列举的 14 类事件均保留，包含 risk。happy 只作新增持仓后的短暂完成姿态，不代表收益。模型校验失败只保留任务证据，不当作账户风控停止。

三份 10-20 冻结原件经 SHA 校验后投影为 102 条事件，覆盖 33 份原计划、27 个 Team 时点与 32 条条件原件；末态为 32 active、1 invalidated。回放为 PARTIAL_FROZEN_EVIDENCE，缺完整逐分钟旧客户流水与 PM 模型原件。缺源的 theme / market / say / relay 不生成。九个 GET/HEAD 签名仅实现于本机 mock，缺统计、会话或配置原件返回 MISSING_SOURCE / null；真实成交原件为 0 才返回空列表。它们不构成生产 Node 接口、当日完整数据或端到端执行验收。

主交付最新验证记录为 Node 全量 26 文件 / 190 通过 / 0 失败，前后端 typecheck 通过。既有原件 SHA、102 事件 schema、本机 mock 错误状态、初始缺源、手动历史加载、末态计划和保护锁、窄屏滚动与依赖缺失文字降级已做本机验证。真实 WebGL context loss 未验证。测试数与源码检查只说明本机覆盖范围，不能代替当前视觉复评。

当前真实证据目录为 `.impeccable/review/team-20261021-0441/`：三档浅深六张首页、六角色近景、同场、灰阶、无标签剪影 PNG；有效参考 JPG 与参考/实现对照 JPG；`relay-intercept.mp4` 为约 6 秒的本机 synthetic 接力与拦截录屏，不是市场回放。最终整改源码的原路径截图已由主代理重采；最新本机测量为交易室整体高 495.5px、计划区域起点 724.5px，角色名称 16px / 500、状态 14px。证据与结论仍须对应本批 recapture 和复评记录，不能依据目录存在或旧截图时间宣称通过。旧目录的 GIF 仅属历史局部节选，不替代本轮证据。

本轮独立 reviewer 的五项 fix 经同一 reviewer 复评均 resolved（PM 杯剪影最后窄修后单项复核），disposition=ship 仅覆盖这五项，不是连续视频动效验收。Claude 尚未给本轮六项分数，未获得 UED ACCEPT；有限范围的 ship / resolved 不构成本轮整体通过。仍欠真实生产只读 API、当天完整数据逐条核验、Claude 视觉/UED 评分与生产发布门。没有把本机样板交付写成正式上线。
