---
name: tableau-twb-compiler-hefang
description: "Use when compiling, generating, editing, repairing, or learning Tableau .twb workbooks for HEFANG-DW. Trigger phrases: 编译 twb, 生成 twb, 直接改 twb, 自动搭建 Tableau dashboard, Tableau 看板开发, 批量改字段别名, dashboard XML, worksheet XML, Tableau 工作簿修复."
argument-hint: "[目标 twb 或看板模块，例如：门店首页_KPI总览 | 生成渠道达成概览 | 批量改字段 caption]"
---

# tableau-twb-compiler-hefang

## 作用

把用户的 Tableau 看板需求转成可维护的 `.twb` XML 修改 / 生成流程，优先服务 HEFANG 销售看板、门店日报看板和后续经营驾驶舱。

## 触发场景

- 用户要求“编译 `.twb`”“直接改 `.twb`”“自动生成 dashboard / worksheet”。
- 用户要求批量修改 Tableau 字段中文别名、默认格式、隐藏字段、计算字段。
- 用户提供 `.twb` / `.twbx`，希望 AI 生成页面骨架、KPI 卡、筛选器或图表模块。
- 用户反馈 Tableau 打不开、页签不显示、字段引用异常，需要修复 XML。

## 必读上下文

执行前按需读取：

1. `docs/Tableau_TWB编译知识库/README.md`：知识库入口、证据和项目衔接。
2. `docs/Tableau_TWB编译知识库/官方Schema吸收指南_20260514.md`：官方 Tableau XSD 的版本映射、适用边界、旧版 workbook 跳过规则和 MCP 校验入口。
3. `docs/Tableau_TWB编译知识库/14份样板学习笔记_20260508.md`：14 份样板沉淀出的编译规则。
4. `docs/Tableau_TWB编译知识库/视觉效果图联合学习_20260508.md`：`.twb` 与同名 `.png` 效果图联合学习出的视觉母版和 XML 编译启示。
5. `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md`：重开工作簿渲染测试阶段的真实报错、根因和修复经验。
6. `.github/skills/tableau-twb-compiler-hefang/assets/twb_compilation_patterns.md`：快速模式表与检查清单。
7. 若任务涉及 HEFANG 销售看板，读取 `docs/销售部数据治理-子项目/销售看板Tableau实施与追踪.md` 的当前阶段与 `docs/销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md` 的字段 / 模块边界。

## 工作流

### 1. 明确输入和边界

- 确认目标是 `.twb` 还是 `.twbx`；若是 `.twbx`，先分离主 `.twb`，不要直接在压缩包内盲改。
- 确认输出目标：字段别名、计算字段、worksheet、dashboard、筛选器、action、样式修复中的哪一类。
- 若用户同时提供效果图，先建立 `.twb` 与 `.png` 的同名映射；缺少效果图的样板只能提炼 XML 结构，不写视觉结论。
- 先选择视觉母版：左侧导航、顶部 hero、卡片网格、漏斗专题、经营驾驶舱；不要在未选母版时直接堆 worksheet。
- 不单方面修改业务口径；销售公式、目标口径、过滤范围以项目文档和用户确认事实为准。
- 不写入真实数据库凭据、连接串、Webhook 或本机私密路径到 git 追踪文件。

### 2. 建立备份和画像

- 修改目标 `.twb` 前，先复制一个带时间戳的备份。
- 解析 XML，记录 `workbook` 的 `version`、`source-build`、datasource 名称、worksheet 名称、dashboard 名称。
- 若 `workbook version` 可映射到官方公开 schema（当前已验证 `26.1` → `schemas/2026_1/twb_2026.1.0.xsd`），调用 `tableau_worksheet_mcp.validate_workbook_schema` 建立结构基线；若存量 workbook 为 `18.1` 等旧版本并返回 `skipped`，不要为通过 XSD 擅自升版本。
- 检测 dashboard zone 使用的是 `type` 还是 `type-v2`，后续新增节点必须沿用目标工作簿风格；如果是新起一张现代工作簿而不是修补旧样板，默认优先 `type-v2`。
- 检查现有 `id` 最大值、worksheet / dashboard / window 命名，避免冲突。

### 3. 编译 datasource 与字段语义层

- 优先保留用户已在 Tableau 中建好的 datasource 连接。
- 字段中文化优先修改 datasource 下 `<column caption="..." name="[...]" />`，不要改真实字段名。
- 新增计算字段时，用 `<column><calculation formula="..." /></column>` 方式补语义层；公式必须引用真实存在字段。
- 参数字段按 Tableau 格式写值：字符串带双引号、日期用 `#YYYY-MM-DD#`、boolean 用 `true` / `false`、real 可保留尾部小数点。
- 默认策略是“物理字段名保留英文，展示 caption 写中文”；系统控制字段、批次字段、技术字段可按需设为隐藏，但只能隐藏真实存在且当前页面不直接展示的字段。
- 候选默认隐藏字段可优先检查 `id`、`etl_*`、`data_version`、`created_at`、`updated_at`、`*_dt` 这类技术字段；写入前先核对目标 `.twb` 和 datasource 中确有该字段。

### 4. 编译 worksheet

- 每个业务模块先拆成独立 worksheet，再拼 dashboard。
- HEFANG 首版优先使用 Text、Bar、Line、GanttBar；Shape、bitmap、Pie、复杂地图后置。
- KPI 比例必须汇总后计算；禁止对行级比例字段直接 `AVG()` / `SUM()`。
- 新增 worksheet 时同步检查 window 元数据，否则 Tableau 可能能解析 XML 但不显示页签。
- KPI 卡优先分三类实现：纯数值卡、数值 + 对比卡、数值 + 对比 + sparkline 三层卡；稳定做法是“文本 worksheet 承担标题/主数值/同比环比，趋势 worksheet 单独承载迷你趋势”。
- 若要复刻漏斗或 Sankey，首版先用单 worksheet 的 Bar / GanttBar 做轻量漏斗，等基础页可打开后再升级到 polygon / path 类复杂图。

### 5. 编译 dashboard

- 默认固定尺寸，不优先使用自动尺寸；销售首页优先 1400 × 1000、1500 × 900 或 1600 × 1000。
- 根 zone 使用 0 到 100000 的比例坐标体系，再用 `layout-flow` 做横向 / 纵向容器。
- worksheet zone 可能没有 `type` / `type-v2`，只通过 `name` 挂载工作表；不要误删。
- 视觉层按“母版 → 固定尺寸 → 粗网格 → worksheet 占位 → 颜色 / 图标 / action”的顺序推进。
- 首版只生成稳定容器、标题、KPI 卡、基础筛选器；复杂 bitmap 背景、圆角 / 阴影模拟、浮动图标、toggle 和参数动作在基础渲染通过后再加。
- 左侧 sidebar 可优先按“品牌区 → 导航区 → 筛选区 → 导出区 → 署名 / 链接区”五段式搭骨架；首版至少落品牌区、导航区和筛选区，其余可先用 `empty` 或 `text` 占位。
- 顶部 tabs / pills 先区分“假 tab”和“真 tab”：首版优先用文本或 worksheet 做静态高亮导航；确认结构稳定后，再升级到 `paramctrl` + `edit-parameter-action` 的真切换控件。
- 多 dashboard 工作簿优先采用“三段式”：Overview 总览页、Detail 下钻页、Records 行级记录 / 导出页；不要一开始把所有阅读层级塞进单页 dashboard。
- 首版推荐 5 类筛选器骨架：日期范围、组织层级、渠道、品类、状态 / 标签；若当前主题不需要其中某类，可删减，但不要随意混用多套筛选口径。
- 页面应保留最基本元信息区：标题、`report_date` / 数据日期、`data_version` / 批次、Last Refresh 或数据来源说明。若品牌色尚未得到用户确认，先沿用所选母版主色做临时视觉稿，不把临时配色写成项目事实。

### 6. 验证

最低验证：

1. XML 可被 Python `ElementTree.parse()` 正常解析。
2. 官方 schema 覆盖的 workbook 必须跑 `validate_workbook_schema`；旧版不覆盖时记录 `skipped`，继续执行后续校验，不把跳过当作通过。
3. 新增 worksheet / dashboard / window 名称唯一且数量符合预期。
4. 新增字段、公式、筛选器引用真实 datasource 字段；字段引用优先用 `validate_field_refs` 在 worksheet + datasource 作用域校验。
5. 目标 `.twb` 能被 Tableau 打开；若用户尚未打开验证，必须标注“未实测 Tableau 渲染”。
6. 如果按 PNG 复刻视觉，必须说明是否已完成同名映射、尺寸对齐、视觉母版选择和缺图边界。
7. 首版 dashboard 即使遇到空数据，也应能显示默认标题、筛选器和“暂无数据”兜底文本，而不是整页空白。
8. PNG 截图尺寸如果比 dashboard 固定尺寸刚好小 1px，默认视为 Tableau 截图边界的正常现象，不单独判定为尺寸错误。
9. 若修改 HEFANG 文档、Skill 或可复用规则，完成后写入 `docs/AGENT_HANDOFF.md`；形成可复用经验时写入 `docs/AGENT_LESSONS.md`。

### 7. 重开测试与错误台帐

- 当 `.twb` 编译完成后，只要用户进入“关闭并重开工作簿做渲染测试”阶段，后续出现的报错、空白、字段失效、sheet 不显示、dashboard 布局错乱等阻塞问题，都默认属于当前任务范围，Agent 不应把它们视为任务外问题直接停下。
- 默认流程是：先读取报错文本或截图证据 -> 定位最小可疑 XML 片段 -> 尝试修复 -> 再让用户重开验证。
- 每次真实遇到并处理这类问题后，必须把“触发场景、报错 / 现象、根因判断、修复动作、验证状态、预防动作”写入 `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md`。
- 若问题尚未解决，也要登记为“进行中”，避免后续重复排查同一错误方向。
- 若 datasource 导入、重命名、连接切换等步骤更适合由用户手工完成，仍需在台帐中记录“交由用户完成的边界”和 Agent 后续接手条件。

## 输出格式

完成后按以下结构汇报：

1. 选择的基准样板或编译策略。
2. 实际修改 / 生成的 `.twb` 文件和备份位置。
3. 新增 / 修改的 datasource、worksheet、dashboard、计算字段、筛选器。
4. 已执行验证与结果。
5. 若发生渲染报错，已写入 / 更新的错误修复台帐条目。
6. 未实测风险和下一步人工打开 Tableau 验证项。

## 禁止事项

- 不复制外部样板完整 XML 作为项目模板；只提炼结构和编译经验。
- 不把 PNG 效果图理解为像素级复刻要求；除非用户明确要求，否则优先复刻信息层级和模块布局。
- 不把样板中的连接串、Hyper、Excel、图片路径当作 HEFANG 事实。
- 不在未备份目标 `.twb` 的情况下直接覆盖。
- 不宣称“Tableau 可正常渲染”，除非用户已打开确认或已有实际渲染证据。
- 不通过 Tableau 侧计算改写已冻结的 ETL / ADS 业务口径。

## 版本记录

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v0.3 | 2026-05-14 | 吸收官方 Tableau document schemas，新增 schema-first 画像 / 校验流程，并明确旧版 `18.1` 存量 workbook 不因 XSD 擅自升版本 |
| v0.2 | 2026-05-08 | 吸纳第二轮 TWB+PNG 联合学习的稳定规则，补充 KPI 卡分类、sidebar 五段式、假 tab/真 tab、三段式 dashboard、候选隐藏字段与空数据兜底 |
| v0.1 | 2026-05-08 | 初版 Tableau `.twb` 编译 Skill，覆盖输入边界、编译流程、验证与禁止事项 |
