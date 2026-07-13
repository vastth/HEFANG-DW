# TWB 编译模式速查

> 来源：2026-05-08 解析 14 份 Tableau `.twb` 样板，并联合 [docs/Tableau_TWB编译知识库/example](../../../../docs/Tableau_TWB编译知识库/example) 中 10 张同名 `.png` 效果图；2026-05-14 吸收 `tableau/tableau-document-schemas` 官方 XSD。详细笔记见 `docs/Tableau_TWB编译知识库/14份样板学习笔记_20260508.md`、`docs/Tableau_TWB编译知识库/视觉效果图联合学习_20260508.md` 与 `docs/Tableau_TWB编译知识库/官方Schema吸收指南_20260514.md`。

## 1. 样板结构事实

| 项目 | 结果 |
|---|---:|
| 工作簿 | 14 |
| worksheet | 409 |
| dashboard | 29 |
| window | 436 |
| action 相关节点 | 234 |
| story | 0 |
| 同名 PNG 效果图 | 10 |
| 缺少 PNG 的 `.twb` | 4 |

注意：datasource 节点计数包含 XML 引用与重复节点，不等同独立物理数据源数量。

## 1.1 官方 Schema 速查

| 项目 | 规则 |
|---|---|
| 官方仓库 | `https://github.com/tableau/tableau-document-schemas` |
| XSD 路径 | `schemas/YYYY_R/twb_YYYY.R.0.xsd` |
| 已验证映射 | TWB `version="26.1"` → `schemas/2026_1/twb_2026.1.0.xsd` |
| Manifest | 新建 2026.1+ TWB 可用 `<ManifestByVersion />` 简化 feature 清单 |
| 本地工具 | `tableau_worksheet_mcp.validate_workbook_schema` |
| 旧版边界 | HEFANG 当前大量存量 workbook 为 `version="18.1"`，官方公开 XSD 不直接覆盖；返回 `skipped` 时继续旧版校验链路，不擅自升版本 |
| 校验含义 | XSD 只代表结构 / 语法校验，不代表 Tableau 能打开或字段语义正确 |

## 2. Dashboard 编译模式

- 29 个 dashboard 全部为固定尺寸 `sizing-mode="fixed"`。
- 坐标体系通常是根 zone `x=0 y=0 w=100000 h=100000`，内部按比例切分。
- 老工作簿使用 `type`，新工作簿多用 `type-v2`；新增 zone 必须沿用目标工作簿风格。若是新起现代工作簿，默认优先 `type-v2`。
- worksheet zone 常见 `type` 为空、只保留 `name="worksheet_name"`。
- 常见 zone 类型：`layout-basic`、`layout-flow`、`text`、`bitmap`、`paramctrl`、`filter`、`empty`、`dashboard-object`。
- `layout-flow` 的 `param="horz"` / `param="vert"` 决定横向 / 纵向容器。

## 3. HEFANG 推荐默认值

| 场景 | 推荐 |
|---|---|
| 销售首页 dashboard | 1400 × 1000 或 1500 × 900 固定尺寸 |
| 多页工作簿结构 | Overview + Detail + Records 三段式 |
| 门店明细较多 | 1600 × 1000 或更高固定尺寸 |
| KPI 卡 | Text mark + 统一字体 / 边框 / 浅背景 |
| 排名 / 对比 | Bar mark |
| 趋势 / 累计进度 | Line 或 Area mark |
| 达成率进度条 | GanttBar mark |
| 图标 / 装饰 | Shape 或 bitmap，首版后置 |
| 复杂交互 | 先筛选器，再参数动作，最后 toggle |
| 首版筛选器骨架 | 日期范围、组织层级、渠道、品类、状态 / 标签 |

## 4. 视觉母版速查

| 母版 | 代表样板 | 适用场景 | 编译提示 |
|---|---|---|---|
| 左侧导航 + 主画布 | Advanced Superstore、Sales Analysis、E-Commerce | 销售 / 门店日报首页 | 根 `layout-flow` 横切 sidebar/content；sidebar 先用文本按钮近似 |
| 顶部 hero + 卡片网格 | Sales Dashboard #VizOfTheDay、Golf | 品牌色强、信息层级清晰的经营页 | 顶部固定高度 header，下面按 KPI strip / main grid / right rail 拆分 |
| 深色导航 + 广告投放分析 | Digital Ads、Marketing Campaign | 达播、广告、营销活动专题 | 日期 pill 和 metric selector 首版可静态，action 后置 |
| 暗色流程页 | Marketing Funnel、Email Marketing | 转化漏斗、会员路径、邮件流程 | 先条形漏斗 / GanttBar，Sankey / polygon 放增强阶段 |
| 巨型白色圆角容器 | Merchandise | 高级品牌视觉页 | Tableau 原生圆角阴影不稳定，首版用浅背景 + 边框 + 留白近似 |

视觉编译顺序：先母版和固定尺寸，再粗网格，再 worksheet 占位，最后做颜色、图标、bitmap、toggle、参数动作。

补充稳定规则：

- sidebar 优先按“品牌区 → 导航区 → 筛选区 → 导出区 → 署名 / 链接区”五段式落骨架。
- 顶部 tabs / pills 首版优先做静态高亮的“假 tab”，结构稳定后再升级到参数驱动的“真 tab”。
- 若品牌色未确认，先沿用所选母版主色做临时视觉稿，不把临时配色写成项目事实。

## 5. 参数与计算字段格式

| 类型 | XML 值格式 |
|---|---|
| string | `"Revenue"` |
| integer | `2024` |
| real | `2025.` 或 `0.56928464232116049` |
| date | `#2020-01-01#` |
| boolean | `true` / `false` |

常用公式模式：

- 指标切换：`CASE [Parameters].[Metric] WHEN 1 THEN ... END`
- 当前 / 去年同期：`IF YEAR([date]) = [Parameters].[Year] THEN [metric] END`
- 固定最大值：`{ FIXED : MAX(...) }`
- 聚合后比例：`SUM([actual]) / SUM([target])`
- 状态标签：`IF [value] >= 0 THEN '▲' ELSE '▼' END`

字段展示策略：

- 物理字段名保留英文，展示层优先改 `caption`。
- `id`、`etl_*`、`data_version`、`created_at`、`updated_at`、`*_dt` 等技术字段可作为候选默认隐藏项，但必须先确认字段真实存在且当前页不直接展示。

## 6. 验证清单

| 检查 | 必须结果 |
|---|---|
| XML 解析 | Python XML parser 通过 |
| 官方 schema | 2026.1+ workbook 跑 `validate_workbook_schema`；旧版返回 `skipped` 时记录原因 |
| 备份 | 修改前已保留目标 `.twb` 备份 |
| datasource | 不丢连接、不写凭据、不伪造字段 |
| worksheet | 名称唯一，有对应 window |
| dashboard | 固定 size、root zone、window、zone id 不冲突 |
| zone 风格 | `type` / `type-v2` 与原工作簿一致 |
| HEFANG 筛选 | `report_date`、`data_version` 不丢 |
| KPI 比例 | 汇总后计算，不平均行级比例 |
| Tableau 实测 | 用户打开验证后才能宣称渲染通过 |
| 视觉映射 | `.twb` 与 `.png` 同名匹配；缺图样板不写视觉结论 |
| PNG 尺寸 | 与 dashboard 固定尺寸一致；若恰好小 1px，默认视为 Tableau 截图边界正常现象 |
| 元信息 | 标题、数据日期 / `report_date`、`data_version`、Last Refresh 或来源说明不缺失 |
| 空数据兜底 | 空数据时仍能显示标题、筛选器和“暂无数据”提示 |
| 重开报错闭环 | 用户重开工作簿后若出现报错 / 空白 / 字段失效，默认先尝试修复，并把经验写入 `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md` |
