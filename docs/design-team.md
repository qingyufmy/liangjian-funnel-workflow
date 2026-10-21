---
name: 量见交易团队 /team
description: 沿用黄金参考 token 的六人工作室与本机局部原件回放
colors:
  brand: "#f0b429"
  bg: "#f4f5f7"
  card: "#ffffff"
  fg: "#14171c"
  muted: "#f0f2f5"
  muted-fg: "#667080"
  border: "#e6e8ec"
  up: "#d32f4b"
  down: "#0a7f54"
  hero: "#14171c"
  hero-sub: "#a3acb9"
  hero-line: "#2b313b"
  dark-bg: "#0c0e12"
  dark-card: "#16191f"
  dark-fg: "#eef0f3"
  dark-muted: "#20242c"
  dark-muted-fg: "#8b94a3"
  dark-border: "#2a2f38"
  dark-up: "#f4516a"
  dark-down: "#1fc98a"
  dark-hero: "#1d2129"
  dark-hero-sub: "#8b94a3"
  dark-hero-line: "#2f3540"
typography:
  display:
    fontFamily: "IBM Plex Sans, PingFang SC, Microsoft YaHei, Noto Sans SC, sans-serif"
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
  section:
    fontSize: "16px"
    fontWeight: 600
    lineHeight: 1.25
  body:
    fontFamily: "IBM Plex Sans, PingFang SC, Microsoft YaHei, Noto Sans SC, sans-serif"
    fontSize: "14px"
    lineHeight: 1.5
  label:
    fontSize: "12px"
rounded:
  stage: "20px"
  panel: "12px"
  control: "8px"
  data: "0"
spacing:
  row: "8px"
  control: "12px"
  panel: "16px"
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
    rounded: "{rounded.panel}"
  stage:
    backgroundColor: "{colors.card}"
    rounded: "{rounded.stage}"
  account-band:
    backgroundColor: "{colors.hero}"
    textColor: "{colors.card}"
    rounded: "{rounded.panel}"
    padding: "24px"
---

# Design System: 量见交易团队 /team

## Overview

**Creative North Star: "六人交易工作室"**

范围仅限 `web/team`。横向导航、深墨账户带和六人工位来自 Claude 的 `HOME_V1_7wFXR7RS.html` 与 `ROLE_CARDS_2.0.md`；最新视觉约束来自 canonical `0438-notice-d-frontend-aesthetics.md` 与任务书 §10，色彩和字体采用 `claude-baseline/src/ref_style.css`，涨跌色按 A 股覆盖。本记录按当前源码末尾覆盖规则整理，不扩展新视觉方向。根 `DESIGN.md` 继续描述既有青绿侧栏运行控制台，两套范围不互相覆盖。

当前交付是本机只读样板：职业角色造型提供工作分工线索，账户、任务、计划和工作记录保留来源位置。缺少原件时使用「—」和缺源说明，只有实际收到空数组才呈现已证实的空列表。动作演示默认关闭，不生成账户数字或角色发言。现已支持手动载入 2026-10-20 三份本机冻结原件的局部回放；§4 九个 GET mock 已齐，但仍仅运行于 loopback，不是真实生产接口。完整旧日事件流、真实生产 API 和 Claude 的六项评分与 UED ACCEPT 仍未完成。

**Key Characteristics:**

- 金色负责品牌和当前导航，深墨账户带建立数字层级。
- 页面表面平整，体积与柔和投影集中在六人工位。
- 角色通过轮廓、发型、眼镜和道具区分，并有文字工作卡。
- 浅深主题、窄屏重排与减少动态效果共用同一信息结构。

依据为 `web/team/styles.css`、`roles.js`、`stage.js`、`app.js`、`labels.js`、`index.html` 与本机 fixture loader / projection。`.impeccable/review/team-20261021/REVIEW.json` 保留首批历史快照；先前回放三项修正的受限 `ship` 不替代当前 §10 全视觉审查。最终独立复审五项中：首屏、中文、字重与文档四项 resolved；配色 partial，已通过 0440 等待 Claude 给出具体值。最终 disposition 为 `fix`，不能视为 §10 通过。主交付报告 Node 全量测试 26 文件 / 189 通过 / 0 失败，其中含 19 项新增测试，typecheck 通过；Claude 六项评分与 UED ACCEPT 尚未取得。

当前视觉证据为同目录 `home-{1440,1024,400}-{light,dark}.png` 六档首页、`v2-roles-{color,grayscale,silhouette,stage,mobile}.png` 五组角色图、`reduced-motion-after-happy.png` 与 `event-replay-1020.gif`。GIF 是 13.34 秒 / 8 帧的历史局部节选，不证明完整全天或连续全帧事件覆盖；最终源版本以本批 current manifest 为准。扩展侧车 `.impeccable/design-team.json` 与本文件配对，不占用旧控制台全局侧车；合成色阶只供面板预览，不是新增运行时 token。

## Colors

浅灰画布、白色面板和深墨正文承载信息；主题通过语义变量整体切换。

### Primary

- **品牌金**（`brand`）：当前实现用于品牌标志、选中导航与控件焦点；选中导航文字仍用正文色。§10 将品牌、角色、涨跌三套语义分开，不能因舞台造型共享近似金色就互借其业务含义。

### Secondary

- **角色色待裁定范围**：页面已声明参考稿五个 `r1..r5`，角色几何仍按 ROLE_CARDS_2.0 的六人配色，PM 保留独有蓝色，且当前不随浅深主题重着色。这是六角色设定与五角色参考的已知冲突，由 Claude 裁定；不把六人硬编码值写成全局通用 token，也不宣称两份来源已经一致。
- **涨红与跌绿**（`up`、`down` 及深色对应项）：用于有源账户涨跌，同时保留数字与正负号。

### Neutral

- **浅灰画布、白色面板、正文墨色**（`bg`、`card`、`fg`）：页面底、信息域与正文。浅色账户带复用正文墨色，白色数字与较淡来源文字形成层级。
- **次级表面、辅助文字、分隔边线**（`muted`、`muted-fg`、`border`）：控件悬停、说明、空态与面板边界。
- **深色对应项**（`dark-*`）：由主题按钮明确启用；初始主题为浅色。角色服装色当前不随主题换色。

**The Non-color Identity Rule.** 每个角色保留至少两条来自造型或道具的识别线索，不能以角色色替代身份和状态文字。

## Typography

**Display Font / Body Font:** 声明栈为 IBM Plex Sans、PingFang SC、Microsoft YaHei、Noto Sans SC、sans-serif。构建没有加载字体文件或外部字体样式，实际字体由本机可用项决定；不能宣称 IBM Plex Sans 已交付。舞台屏幕纹理另用 Microsoft YaHei / sans-serif。

字号仅用（12 / 14 / 16 / 20 / 28 / 40px），§10 许可字重为（400 / 500 / 600）。主账户数字在桌面和手机均为（40px / 600），次级数字桌面（20px / 600）、手机（16px / 600）。页面与占位大标题为（28px），评审标题手机为（20px）；区块标题（16px），正文（14px），辅助文字（12px）。数字与标题行高（1.25），正文（1.5），数字统一 `tabular-nums`；局部中文正文按（38em）限制行宽，不能扩大宣称所有文案均已逐行核过。

最后覆盖规则已将 logo 与 h1 / h2 / h3 统一为（600），不再继承先前的 700；粗体与关键数字也使用（600）。

**The Evidence Text Rule.** 来源、缺源、演示和只读说明必须保持可读文本，不以颜色或角色姿态代替。

## Layout

页头和正文共享最大宽度（1200px），主区块与工作区间距（24px），常用卡内间距（16px），行级节奏（8px）。桌面页头单行、导航最小高度（64px），账户带允许换行。后置样式把工作区改为（自适应 / 280–360px）两列：焦点在左，工作记录在右，任务列表排在两者下方横跨整行；不沿用前部已被覆盖的三列定义。

在（760px）以下页头换行，横向导航另起一行且项高（46px），工作区按焦点、工作记录、任务顺序变为单列，账户指标两列。后置两列规则覆盖旧的 1100px 工作区定义；不能再把旧断点描述为新的布局切换。表格只在自身容器内横滚。当前首页已具备 1440 / 1024 / 400 三档、浅深两套截图，具体验收仍对应其版本。

任务历史列表的最终最大高度为（240px），覆盖前部旧的 360px 定义，在列表内纵向滚动并阻止滚动串出；标题保留记录总数。记录按最新在前排列，列表可通过键盘聚焦。长 ID、SHA 与原件正文允许折行，避免历史记录把中间焦点与工作记录推到页面之外。

舞台独立采用容器宽度阈值（720px）：桌面六工位一排，窄容器三列两排。首页舞台高（420px），手机高（330px）；评审台高（480px），手机高（380px），实际舞台尺度模式高（360px）。近景由用户明确选择，不把裁切全景视为六人完整展示。

## Elevation & Depth

页面面板依靠底色、单像素边框与间隔分层，没有 CSS 卡片阴影或通用悬浮抬升。3D 工位通过实体几何、柔和投影、半球光与方向光形成体积；通用材质 roughness 为（0.7）。渲染采用 r149 LinearEncoding、ACESFilmicToneMapping、曝光（0.9）与 PCFSoftShadowMap，这些是舞台设置，不是页面阴影 token。

持续动画在暂停、减少动态效果、页面隐藏或舞台离开可视区域时停止。`happy` 使用独立（1500ms）计时返回，不依赖动画帧；暂停评审可冻结姿态。本次浏览器已通过 CDP Emulation 设置 `prefers-reduced-motion: reduce`：happy 解除暂停后，舞台 aria 状态全部返回 work，截图为 `reduced-motion-after-happy.png`；随后已恢复媒体设置。此证据确认静态状态切换与返回，不扩大为所有平台验证。

明确收到 `relay` 事件时，舞台下方文字交接条播放（700ms ease-out）中性色线条，同时显示双方角色与记录时刻；减少动态效果时 CSS 禁止其动画。10-20 冻结 fixture 没有 relay 来源，因此回放不生成交接，也不能将单独的表现层测试当成该日真实协作证据。

**The Material Boundary Rule.** 页面使用平整信息面，立体材质与空间投影留在角色工作室中。

## Shapes

圆角按舞台（20px）、常驻卡与账户带（12px）、按钮与下拉框（8px）、数据格（0）分层。文字降级卡仍有局部 10px 遗留，不能推成新的圆角级别。面板内部使用分隔线。角色以基元几何保留肩线、服装、发型、眼镜和手臂动作；Blender / GLB 骨骼模型尚未交付。

## Components

### Buttons / Selects

默认按钮为主题表面、单像素边框、control 圆角及（44px）最小高度；悬停转为次级表面。焦点为（3px solid 品牌金），偏移（3px）；禁用透明度（0.6）。桌面角色选择存在（36px）紧凑按钮，手机角色选择已由后置规则提高为（44px）；手机“六人同场”仍保留局部 36px 差异，不能据此降低全产品触达标准。下拉框共用控件外观；本批没有文本输入框或独立 chip。

### Navigation

五项横向文字导航，当前项使用品牌金下划线（3px）和加重正文。今日与角色评审台已实现；团队、计划、复盘、设置为说明后续范围的只读占位，导航存在不代表功能完成。

### Cards / Account Band

信息面板围合任务、焦点、工作记录、计划与持仓；标题区与主要卡内间距最终为（16px），账户带最终内边距为（24px），不采用前部已被覆盖的旧值。账户带的深墨底、白色数字与辅助来源建立账本层级；净值直接显示源值，不由前端反推。

舞台标题区新增“当前工作 / 下一件事 / 需要你看”三项来源摘要，桌面三列、手机单列。当前工作只取收到的 work / talk 状态；下一件事只取已收到的待执行或运行任务，没有原调度就显示缺源；关注提示区分实际只记录保护锁、已有异常与异常来源未接入，不凭当前画面预测调度。

### Historical Replay / Provenance

载入按钮由用户手动触发，只请求本机 `/api/ui/replay?trade_date=2026-10-20`；载入前不自动接源。服务只绑定 `127.0.0.1`，校验三份既有本机文件的固定 SHA 后做白名单投影，并用 `PARTIAL_FROZEN_EVIDENCE` 明确标识。该日期目前投影 102 条事件：33 份原计划，后续为 32 份 active 与 1 份 invalidated；27 个 Team 时点及 32 条条件原件。它是部分原件覆盖，不能用“已到末条”表示全天完整或执行链畅通。没有原件的 theme / market / say / relay 不生成，缺失收益保持未知。

载入后出现既有白色面板样式的回放控制区，提供“下一条原件事件”和“查看末条状态”，显示已读 / 总条数，覆盖范围与已知缺口默认收进 details，读完后禁用前进按钮。历史时钟标明“该记录时刻”，当前来源折叠区显示 source、记录 ID、完整 SHA 与观测时间。载入回放会停止动作演示、关闭既有事件连接并禁用演示入口，避免历史和演示混淆；载入失败用明确文本说明，不渲染为风控停止。

主视图通过 `labels.js` 将策略、条件、计划状态与分组显示为交易员可读的中文；未收录项说明待补充，不直接抛出原码。原始策略、条件键与风控码保留在“查看原始策略、条件和风控码”详情中，计划表保留原策略、原状态、原分组与来源详情。风险和角色主文案使用“只记录保护锁”，详情保留原始 record 模式。

账户来源显示观测时间、记录 ID 和完整 SHA。计划表通过“来源”折叠详情分别显示原计划 ID / SHA 与状态更新 ID / 时间 / SHA，不把原计划证据冒充后续状态证据；原计划时间当前尚未单独投影和显示。任务主行显示中文状态，如“校验通过”“校验未通过”“引擎跟踪，没有需模型评审的候选”；折叠详情保留原状态、记录 ID 与 SHA。

模型校验失败仅作为任务与角色文字证据，角色状态保持 idle，不映射为账户风控 `stop`。本批历史投影只有实际 13:23 RECORD 保护锁触发风控 stop / 护罩；不能把模型格式失败或本机文件加载失败当作该账户保护锁。

### Six-person Room

A1 为灰白短发、方框眼镜、地球仪与年报；A2 为高马尾、颈挂耳机与三联屏；PM 为圆框眼镜、领带与资金表盘；A4 为耳麦、马甲、裸露前臂、双屏与急停；A6 为胡须、盾牌与护罩；A5 为盘发、细框眼镜、笔与三色复盘本。屏幕呈现工作结构，缺源时不生成虚假行情。`stop` 强信号集中在 A6 护罩与 A4 急停灯。

`idle / work / talk / stop / happy / rest` 是表现层状态。首页 A4 的 `happy` 只在初次同步之后新增持仓记录时触发，最多（1.5 秒），不代表盈亏；评审手动状态标注设计状态。`bubble()` 当前更新文字工作卡的状态文字，未交付基线的悬浮发言气泡。

Three.js 缺失或 WebGL 初始化失败时隐藏舞台，以六张文字工作卡提供角色信息。截图验证了依赖不可用降级，尚未模拟真实 WebGL context loss，不能混作同一验收。

## Do's and Don'ts

### Do:

- **Do** 沿用金色横向导航、深墨账户带与平整信息面的层级。
- **Do** 让角色身份同时可由轮廓、道具和文字辨识。
- **Do** 保留缺源、演示和只读说明，真实空列表与尚未接源分别呈现。
- **Do** 同时检查浅深主题、400px 布局、六态与无 WebGL 文字工作卡。

### Don't:

- **Don't** 用动作、气泡或装饰数字暗示未经来源证明的工作、成交或收益。
- **Don't** 把本机独立审查 `ship` 写成 Claude UED ACCEPT、真实数据验收或生产发布完成。
- **Don't** 把 `/team` 的金色工作室设计覆盖到既有青绿侧栏控制台。

未固化为规范：后续页面占位引导语、条件中的 `✓ / ○ / ?` 字形标记与工位 `z z` 文字属于当前局部实现，不是未来页面可继承的图标规范；基线悬浮气泡、GLB 模型与完整全天真实事件剧情仍为未交付方向。局部 10px 降级卡圆角不固化为规范；六人 ROLE2.0 配色与五角色参考 token 的冲突已通过 0440 交 Claude 裁定，当前未获例外批准。本次回放复用既有视觉系统，不新增视觉世界。
