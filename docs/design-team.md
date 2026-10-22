---
name: 量见交易团队 /team HOME_STUDIO_V3
description: 锌黑画布、琥珀微光与六人工作室的本机只读页面
colors:
  brand: "#fbbf24"
  amber: "#f59e0b"
  bg: "#09090b"
  card: "#111113"
  fg: "#fafafa"
  muted: "#202023"
  muted-fg: "#a1a1aa"
  secondary-fg: "#d4d4d8"
  border: "rgba(255,255,255,.08)"
  control: "#18181b"
  stage: "#101319"
  stage-3d: "#141b26"
  stage-light: "#223047"
  relay: "#f0b429"
  warn: "#fcd34d"
  up: "#f87171"
  down: "#34d399"
  price: "#e4e4e7"
  volume: "#71717a"
  a1-accent: "#8B78E0"
  a2-accent: "#3FA3AD"
  pm-accent: "#5C8BE6"
  rk-accent: "#A7B2C6"
  tr-accent: "#E0A62B"
  rv-accent: "#55B282"
typography:
  display:
    fontFamily: "Inter, Noto Sans SC, PingFang SC, Microsoft YaHei, sans-serif"
    fontSize: "34px"
    fontWeight: 600
    lineHeight: 1.2
    letterSpacing: "-.6px"
  metric:
    fontSize: "24px"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "-.4px"
  title:
    fontSize: "16px"
    fontWeight: 600
    lineHeight: 1.25
  body:
    fontFamily: "Inter, Noto Sans SC, PingFang SC, Microsoft YaHei, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.5
  table:
    fontSize: "13px"
    lineHeight: 1.5
  label:
    fontSize: "12px"
    lineHeight: 1.5
rounded:
  panel: "20px"
  stat: "16px"
  stage: "14px"
  navigation: "12px"
  screen: "10px"
  control: "8px"
  pill: "999px"
spacing:
  compact: "8px"
  row: "12px"
  grid: "14px"
  content: "16px"
  panel: "20px"
  section: "24px"
  page: "32px"
components:
  button-default:
    backgroundColor: "{colors.control}"
    textColor: "{colors.secondary-fg}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
  button-default-hover:
    backgroundColor: "{colors.muted}"
    textColor: "{colors.fg}"
  panel:
    textColor: "{colors.fg}"
    rounded: "{rounded.panel}"
  stat:
    rounded: "{rounded.stat}"
    padding: "16px"
  workstation:
    backgroundColor: "{colors.stage}"
    rounded: "{rounded.stage}"
  status-active:
    backgroundColor: "rgba(245,158,11,.14)"
    textColor: "{colors.warn}"
    rounded: "{rounded.pill}"
    padding: "3px 10px"
---

# Design System: 量见交易团队 /team HOME_STUDIO_V3

## Overview

**Creative North Star: "锌黑六人工作室"**

本规范只覆盖 `web/team` 的只读 Operate 页面。当前视觉权威为用户锁定的 `0458-notice-d / HOME_STUDIO_V3`；旧浅色资产栏与计划、记录并排的首页说明已经过时。按 `index.html`、后加载的 `studio.css`、`studio.js`、`plan-detail.js`、`app.js` 和 `stage.js` 的实际实现记录，根 `DESIGN.md` 继续属于旧控制台。

锌黑画布上方有受边界约束的琥珀与天蓝径向微光。结论标题、六项指标、六人工位与团队记录、全宽计划表依次呈现。未知数值保留未知；角色与图表只表达有来源的观测或明确标识的演示。当前为本机实现记录：独立 review 已确认本轮原三项修正均 resolved，disposition 为 ship，仅覆盖该 fix-list；Claude 的完整 V3 六项视觉评分与批准仍待，F5/F6/F7 与 VM 发布仍待门禁。

**Key Characteristics:**

- 深色单主题；琥珀用于品牌、关注与选中，细线和透明表面划分信息。
- 六项指标保持相同权重，未知历史显示文字，不生成装饰曲线。
- 默认二维六人工位，用户手动展开专业六角色 3D 舞台。
- 交易室与记录约为 8:4；计划占满内容宽度，窄屏滚动留在组件内部。

## Colors

上方 token 是源码色值；侧车合成色阶仅供设计面板预览，不是运行时新增色阶。

### Primary

- **琥珀品牌金**（brand / amber / warn）：品牌、焦点、工作中工位光、计划区间与有效状态。3D 接力沿用 relay 色，不能误写成与页面品牌 token 完全一致。

### Secondary

- **涨红与跌绿**（up / down）：有源价格及盈亏，保留数字与符号；止损线使用涨红 token 的色值与虚线形态。
- **价格浅灰白与成交量灰**（price / volume）：V3 深底图表的价格点线与成交量柱。用户已确认浅灰白替代旧浅底深价格线。

### Tertiary

- **六角色点缀色**（a1 / a2 / pm / rk / tr / rv-accent）：依次对应基本面、题材、组合、风控、交易、复盘，用于工位头像边线及 3D 服装、道具的小面积点缀。

### Neutral

- **锌黑画布、卡面、主字、次级字与细线**（bg / card / fg / muted-fg / secondary-fg / border）：单一深色主题中的阅读层级。
- **二维工位底与三维背景**（stage / stage-3d / stage-light）：二维工作室有细点阵；展开后的 3D 保留蓝灰背景与柔光。

**The Color Separation Rule.** 品牌、角色点缀和涨跌色各自表达既定语义，不借角色色暗示成交、风险或收益。

## Typography

**Display Font / Body Font:** Inter、Noto Sans SC、PingFang SC、Microsoft YaHei、sans-serif。HTML 请求 Inter（400 / 500 / 600 / 700）与 Noto Sans SC（400 / 500 / 600），源码声明不代表远程字体已加载。

首页结论使用 display，窄屏降为（26px），字距（-.4px）。六项指标使用 metric，窄屏降为（22px）。面板标题使用 title；表格与记录主要为（13px），来源、时间与标签主要为 label。抽屉标题（18px），次级标题（15px）。全页数字继承 `tabular-nums`。旧资产栏 DOM 保留但被隐藏，其（40px）净值不再是首页视觉规范。

**The Evidence Text Rule.** 缺源、来源、只读与演示说明保留可读文字，不以颜色或姿态代替。

## Layout

页头与正文最大宽（1424px，含左右各 32px 内边距），正文内容最大宽（1360px）。桌面页头高（64px），导航是紧凑分段容器，当前项使用透明白填充；没有旧金色底线。正文主间距（14px），结论区域标题下接六列指标；旧资产栏隐藏。

运行时把交易室与团队记录移入同一网格，列宽为（2fr / minmax(300px,1fr)），即约 8:4。今日计划独占全宽，底部风控额度与持仓账本两列，观测条件与任务证据默认折叠。团队记录最多渲染最近（25）条，新记录在前；任务列表内部滚动。

（1100px）以下指标变三列、左右内边距（24px），记录最小列宽（260px）。（760px）以下指标为两列、主区域与底部单列、左右内边距（16px），导航换行。二维工位桌面高（268px）、手机高（248px），六列工位内容最小宽（720px）；展开 3D 高（290px），窄容器六人同场维持（960px）画布。工位与计划、持仓滚动区具有 `role="region"`、`tabindex="0"` 与可读标签；手机显示横向滑动提示，边缘渐隐只提示内部仍有内容。

计划表桌面最小宽（920px）、手机最小宽（600px），默认显示前（10）条，可展开全部；筛选为全部、在区间内、已作废。表头吸顶，滚动容器最大高（520px）。顶部两层径向光的宽度受页面约束，不应撑大页面横向滚动范围。

## Elevation & Depth

常驻面板通过透明白渐变、细边线与不同底色分层。二维工位屏幕有局部暗投影（0 8px 24px rgba(0,0,0,.35)），活跃头像有琥珀光晕；侧边详情抽屉以左侧暗投影及半透明黑遮罩与背景分离。顶部光只构成气氛，不表示行情、收益或系统健康。

展开的 3D 使用哑光 MeshStandardMaterial（roughness 0.92）、柔和投影、半球光和方向光；Three.js r149 的 LinearEncoding、ACESFilmicToneMapping、曝光（0.9）只属于舞台。浅弧工位与地面接力共用 centripetal CatmullRom 曲线；接力（720ms，cubic ease-out），happy 最长（1500ms）返回 work，均不表示盈利。暂停、隐藏页、离开视区与减少动态偏好限制持续动画；idle 为静态姿态。二维活跃头像呼吸周期（2.4s），减少动态时关闭。

**The Material Boundary Rule.** 页面以透明表面和局部微光分层，立体材质与空间投影留在展开的角色工作室中。

## Shapes

面板、指标与工作室内框分别采用 panel、stat、stage 圆角；导航与筛选容器采用 navigation，按钮采用 control，状态标签与模式标记采用 pill。工位头像为圆形；二维屏幕采用 screen 圆角。计划行以水平细线区分，不给每行套独立卡片。

3D 六人穿专业深色服装，以发型、眼镜、道具及点缀辨认；模型仍是基元几何。无标签剪影、灰阶与实际尺寸属于角色评审台模式，不意味着首页采用浅色主题。

## Components

### Buttons / Navigation / Selects

通用按钮和评审下拉框继承最小高（44px）、细边线与 control 圆角，部分紧凑控件按源码降为（28–36px）。悬停使用 muted，键盘焦点为（3px solid brand），偏移（3px）；禁用透明度（0.6）。交易室展开按钮为胶囊形（32px 最小高），计划筛选最小高（34px）并用 `aria-pressed` 标记选中。导航五项中仅首页与独立角色评审台有完整页面，其余入口仍为只读占位。当前没有文本输入框，也没有主题切换入口。

### Six Metrics / Risk Limits

六项指标为模拟盘净值、仓位、今日信号、形态确认、系统健康、模型调用。净值来自账本；仓位和完整信号流水未齐时保留“—”；形态确认只表示当前焦点条件；系统健康默认“待核验”；模型调用只统计带 `model_calls` 来源 ID 与 SHA 的已载入记录。历史区显示“走势待有序原件”，不画模拟趋势。额度缺源显示“待原件”，不填零；有值才画额度条。

### Two-dimensional / Three-dimensional Room

默认二维工位显示六人简称、角色色边线、工作说明与状态；没有工作原件时显示等待文字。只有已收到 work / talk 角色事件的工位显示活跃光。展开按钮切换二维与 3D；3D 首次展开才初始化。基本面 → 题材 → 组合 → 风控 → 交易 → 复盘构成阅读顺序。

3D 名称由世界坐标投影到 HTML 层，与画布共用内部滚动；未知状态、动作行隐藏，只保留姓名。WebGL 依赖、初始化或 context loss 失败时调用文字工作卡降级，源码处理不等于所有 GPU 与浏览器都已验收。模型 stop 护罩与急停只表达对应角色事件；缺源屏幕不得画虚构行情。

### Plans / Chart / Detail Drawer

表格包含股票、形态、今日走势、现价、距区间、盈亏比、确认进度、状态及详情入口。ACTIVE_TODAY 标签用透明琥珀底，作废状态弱化；逐项确认缺源时显示待原件，不能由价格位置推断满足。计划与状态更新各自保留记录 ID、SHA 与时刻。

紧凑图与抽屉共用 SVG 模型：只纳入 `closed === true` 且 close 有限的 5 分钟样本；按观测时刻排序，相邻时间差不是（300000ms）时断线。价格为浅灰白点线（线宽 1.8），计划区间为琥珀填充（14%），止损为红色虚线（4 / 3），成交量为灰柱（40%）。缺样本显示“5分钟原件待接入”；每点标题携带时刻、收盘与 SHA；可读图表标签说明局部覆盖。

详情使用原生 `dialog.showModal()`，右侧宽度（min(560px,100%)）、全高（100dvh），具有标题关联、关闭按钮与原生模态键盘行为。手机为全宽抽屉。逐项条件显示满足、未满足、证据不足，并提供阈值、观测、时刻与原始来源折叠区。

### Evidence / Local Data

10-20 冻结回放和本机 10-21 局部观测均由用户手动载入，明确标识非实时、非完整全天。今日已采（9）份计划原件与（32）个已闭合 5 分钟样本只代表该局部证据范围，不代表全天总量或所有计划都有曲线；以实际有序样本、原始来源及时间缺口为准。手动历史回放停止演示与既有轮询，10-20 缺少 relay 原件，不补造历史接力。

源码具有经过 session 契约后启动同源只读轮询的路径，不能据此宣称当前本机或 VM 已接通。账本未知不写空仓；只有来源明确的空列表才显示“还没有持仓”。截图、文档与本机测试不替代真实业务路径验收。

## Do's and Don'ts

### Do:

- **Do** 延续已锁定的深色 V3、六项指标、约 8:4 工作室与记录、全宽计划表。
- **Do** 默认二维工位，按用户操作展开 3D；窄屏内部滚动并保留键盘与文字提示。
- **Do** 让价格线、条件状态、额度与指标回到有 ID、SHA、时刻的原始记录。
- **Do** 区分缺源、已证实空列表、局部原件、演示及完整业务验收。

### Don't:

- **Don't** 用零、装饰曲线、角色姿态或页面连通暗示缺源的收益、成交或系统健康。
- **Don't** 把 9 份计划与 32 个闭合 5 分钟样本写成全天完整覆盖，或跨时间缺口连线。
- **Don't** 把本机修复、截图、独立评审或文档完成写成 Claude V3 ACCEPT、F5/F6/F7 通过或 VM 已发布。
- **Don't** 把 /team 的设计系统覆盖到根 DESIGN.md 所属旧控制台。
