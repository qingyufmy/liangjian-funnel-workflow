---
name: 量见交易团队 /team
description: 白色资产栏与深色六人交易室的本机只读 Operate 页面
colors:
  brand: "#f0b429"
  bg: "#f3f4f6"
  card: "#ffffff"
  fg: "#12161d"
  muted: "#f0f2f5"
  muted-fg: "#5b6472"
  border: "#e4e7ec"
  stage: "#141b26"
  stage-light: "#223047"
  stage-line: "#2c3747"
  stage-sub: "#dde2e9"
  up: "#d23b3b"
  down: "#12915a"
  warn: "#7a5200"
  a1-accent: "#8B78E0"
  a2-accent: "#3FA3AD"
  pm-accent: "#5C8BE6"
  rk-accent: "#A7B2C6"
  tr-accent: "#E0A62B"
  rv-accent: "#55B282"
typography:
  display:
    fontFamily: "IBM Plex Sans, Noto Sans SC, PingFang SC, Microsoft YaHei, sans-serif"
    fontSize: "40px"
    fontWeight: 600
    lineHeight: 1.25
  headline:
    fontSize: "28px"
    fontWeight: 600
    lineHeight: 1.25
  title:
    fontSize: "20px"
    fontWeight: 600
    lineHeight: 1.25
  emphasis:
    fontSize: "16px"
    fontWeight: 500
    lineHeight: 1.25
  body:
    fontFamily: "IBM Plex Sans, Noto Sans SC, PingFang SC, Microsoft YaHei, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.5
  label:
    fontSize: "13px"
    fontWeight: 400
rounded:
  stage: "20px"
  panel: "12px"
  control: "8px"
  data: "0"
spacing:
  compact: "8px"
  row: "12px"
  content: "16px"
  panel: "20px"
  section: "24px"
components:
  button-default:
    backgroundColor: "{colors.card}"
    textColor: "{colors.fg}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
  button-default-hover:
    backgroundColor: "{colors.muted}"
    textColor: "{colors.fg}"
  panel:
    backgroundColor: "{colors.card}"
    textColor: "{colors.fg}"
    rounded: "{rounded.panel}"
  account-band:
    backgroundColor: "{colors.card}"
    textColor: "{colors.fg}"
    rounded: "{rounded.panel}"
    padding: "20px 24px"
  stage:
    backgroundColor: "{colors.stage}"
    textColor: "{colors.card}"
    rounded: "{rounded.stage}"
---

# Design System: 量见交易团队 /team

## Overview

**Creative North Star: "深色六人交易室"**

本规范仅覆盖 `web/team` 的 Operate 场景。视觉权威为 MAIN 中 `artifacts/frontend-role-design-20261019/claude-reference-20261021/` 的 README_REFERENCE、HOME_REFERENCE 与 DESIGN_SPEC，以及 canonical `0441-notice-design-reference.md`、`0441-reply-b-character-professional.md`。最新参考覆盖冲突的旧 HOME 与 ROLE_CARDS_2.0 决定。本文件按当前 styles.css、roles.js、stage.js、index.html、app.js 的最终生效值记录；根 DESIGN.md 继续属于既有控制台。

白色资产栏提供平静的账户入口，深色交易室及地面接力线承担主要记忆点，计划与团队记录服务日常观察。未知数据保持未知，角色姿态只表现已收到的状态或明确标识的设计演示。当前仍为本机样板，文档完成不代表 Claude 视觉验收或生产交付。

**Key Characteristics:**

- 浅色主题以白色资产栏、深色交易室、计划与记录并排形成层级。
- 六人穿专业深色服装，身份由发型、道具、文字与六种点缀色共同表达。
- 常驻信息面平整；体积、柔和投影与连续接力曲线集中在 3D 舞台。
- 窄屏保留完整浅弧工位，可在舞台内部横向滑动。

## Colors

色值以上方 token 为准；浅色页面中性表面与深色舞台分工明确。配套侧车的合成色阶仅用于面板预览，不是新增运行时 token。

### Primary

- **品牌金**（brand）：品牌、当前导航下划线、键盘焦点与接力圆点、已走线段。

### Secondary

- **六角色点缀色**（a1 / a2 / pm / rk / tr / rv-accent）：依次对应基本面、题材、组合、风控、交易、复盘。已由最新参考明确，旧五角色 token 与六角色卡冲突不再是待决事项。实现用于挂绳、胸牌、领带、屏幕顶边和工位环，不能铺满整件服装。
- **涨红、跌绿**（up / down）：有源价格和盈亏的业务颜色，保留数字与符号。
- **等待提醒**（warn）：等待类说明与关注区域。状态标签使用浅底深字；当前计划实现有中性、等待与过期三类外观。

### Neutral

- **画布、卡面、墨色、次级文字与细线**（bg / card / fg / muted-fg / border）：资产栏、计划表、记录和证据区。
- **舞台底、顶部光、未走线、辅助文字**（stage / stage-light / stage-line / stage-sub）：舞台在浅深主题下均保持深色。
- 深色主题把页面底改为 fg 对应深墨、卡面改为 stage、主字改为白色、次级字改为 stage-sub、边线改为 stage-line；角色点缀和涨跌色不另造一套。

**The Color Separation Rule.** 品牌、角色点缀和涨跌色各自表达既定语义，不借角色色暗示成交、风险或收益。

## Typography

**Display Font / Body Font:** IBM Plex Sans、Noto Sans SC、PingFang SC、Microsoft YaHei、sans-serif。index.html 请求 Google Fonts 的 IBM Plex Sans 与 Noto Sans SC（400 / 500 / 600）；字体请求失败时使用本机回退字体，不能仅凭声明认定远程字体已加载。

净值使用 display，页面标题使用 headline，区块标题使用 title，角色名称使用 emphasis。正文和表格使用 body；时间、代码、说明使用（12–13px），舞台角色状态与动作使用（14px）。次级账户数字使用（20px / 500）。全页数字采用 `tabular-nums`。评审页标题在窄屏降为（20px）；字重仅（400 / 500 / 600）。中文说明在对应组件中限制为（65ch 或 75ch），不把局部约束扩大为全页统一字数。

**The Evidence Text Rule.** 缺源、来源、只读与演示说明保留可读文字，不以颜色或姿态代替。

## Layout

最终容器上限为（1328px，含左右各 24px 的正文内边距），实际内容最大宽（1280px）。区块间距（24px）；页头高（60px），导航当前项底线（2px）。阅读顺序为资产栏 → 深色交易室 → 今日计划与团队记录 → 持仓与挂单 → 默认折叠的观测条件与任务证据。当前工作、下一件事与关注摘要已进入证据折叠区，不再作为舞台上方三列大摘要占据首屏。

计划与记录采用（自适应 / 280–360px）两列。计划默认按状态排序显示前（10）条，支持展开全部；表头吸顶，表格在自身容器滚动。任务列表在证据区内最大高（240px），新记录在前，允许键盘聚焦。长 ID 与 SHA 可折行。

页面在（760px）以下改为单列，正文左右内边距（12px），导航换到下一行且项高（46px）。舞台桌面最终高（300px）、手机（380px）；评审舞台桌面（500px）、手机（420px），实际尺寸评审模式（360px）。舞台容器小于（720px）且显示六人时，WebGL 画布维持（960px）宽，通过舞台自身横向滚动查看完整浅弧工位。

## Elevation & Depth

常驻页面面板使用单像素细线与底色分层，没有 CSS 卡片阴影。舞台采用哑光 MeshStandardMaterial（默认 roughness 0.92）、柔和投影、半球光与方向光。Three.js r149 使用 LinearEncoding、ACESFilmicToneMapping、曝光（0.9）与 PCFSoftShadowMap；这些是舞台渲染设置，不能当作页面阴影 token。

角色和桌面沿浅弧排列；每个工位略微朝向弧心。接力线是同一条 centripetal CatmullRom 曲线的连续分段，圆点沿同一曲线移动（720ms，cubic ease-out），完成段以品牌金与（0.85）透明度保留；圆点另有柔和光晕。减少动态效果时直接到位。舞台暂停、页面隐藏或离开可视区域时停止持续角色动画；idle 保持静态姿态，不加入周期性晃头或眨眼。

**The Material Boundary Rule.** 页面使用平整信息面，立体材质与空间投影留在角色工作室中。

## Shapes

舞台、卡片、按钮与标签、表格分别使用 stage、panel、control、data 圆角。人物用基元几何塑造宽肩、收腰、衬衫与外套层次，面部为小眼、细眉、平直嘴线；服装以深灰蓝、藏青、炭灰、墨绿灰等深色为主。角色色只做点缀。剪影评审隐藏环境和文字标签，使用浅底深色模型，依靠头肩、发型和手臂间的负空间检查识别。

## Components

### Buttons / Selects / Navigation

按钮和下拉框最小高（44px），主题表面、细边线、control 圆角。悬停转为 muted；键盘焦点为（3px solid 品牌金），偏移（3px）；禁用透明度（0.6）。五项横向文字导航以金色下划线标记当前页；今日与角色评审台已实现，团队、计划、复盘、设置仍为只读占位。本批没有文本输入框。

### Account / Plans / Records

资产栏使用主题卡面、正文色与细边线；初始“模拟盘 100 万”明确标注为初始资金配置，不代表当前净值。有源净值直接显示账本值；缺失数据用“—”。计划保留原计划与状态更新各自的记录 ID、SHA 和时间来源；原计划时间尚未单独投影。业务文案采用中文，策略、条件和风控原码留在 details。团队记录显示最近（25）条，并保留角色与时刻。

### Six-person Room

工作流顺序为基本面 → 题材 → 组合 → 风控 → 交易 → 复盘。基本面有灰白短发、方框眼镜、地球仪与年报；题材有高马尾、颈挂耳机与三联屏；组合有圆框眼镜、领带、俯看姿态与资金表盘；风控有短胡须、抱臂、盾牌与拦截护罩；交易有耳麦、卷袖前臂、双屏与急停；复盘有盘发、细框眼镜、笔与三本复盘本。屏幕缺源时显示待源字段，不画虚构走势。

`idle / work / talk / stop / happy / rest` 是表现层六态。显式 relay 更新双方说明并驱动地面圆点，接收方进入 work。happy 最长（1500ms）后回到 work；首页仅在初次同步后出现新增持仓记录时触发，不代表盈利。A6 stop 显示护罩，A4 stop 显示急停灯。模型校验失败和本机加载失败不冒充账户保护锁。WebGL 初始化或依赖不可用时保留六张文字工作卡；真实 context loss 尚未验证。

### Historical Evidence

历史按钮只由用户手动载入 2026-10-20 本机冻结原件，明确标注局部、非实时、非完整全天。载入后出现回放控制与来源 details，并停止动作演示、断开既有事件连接、禁用演示入口。10-20 fixture 没有 relay 原件，不补造历史接力；评审台的接力与拦截按钮属于 synthetic 演示。

## Do's and Don'ts

### Do:

- **Do** 采用白色资产栏、深色交易室、计划与记录并排的当前层级。
- **Do** 通过深色服装、六种点缀、发型道具和文字共同辨认六人。
- **Do** 让窄屏舞台内部横向滑动，保持浅弧工位与连续接力线。
- **Do** 区分缺源、已证实空列表、局部历史原件与设计演示。

### Don't:

- **Don't** 用动作、装饰数字或缺源屏幕暗示未经来源证明的成交与收益。
- **Don't** 把本机测试、截图或独立复评写成 Claude UED ACCEPT、真实当日数据通过或生产发布完成。
- **Don't** 把 /team 的设计系统覆盖到根 DESIGN.md 所属旧控制台。

未固化为规范：评审页的旧“2.0”标题文字、后续页面占位引导语、局部条件字形和休息 z z 文字符号不作为未来页面通用组件；GLB 骨骼模型与悬浮发言气泡尚未交付。源代码值记录不替代像素与逐角色视觉验收；当前评审状态见配套实现记录。
