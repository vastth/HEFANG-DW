# Tableau Worksheet MCP 设计基线

> 版本：v0.6
> 日期：2026-05-14
> 状态：已实现 profiling + validate_field_refs + patch_chart_bindings + validate_workbook_schema，并完成真实 workbook 字段校验、单 pane patch、多 pane dual-axis 骨架 patch、累计趋势图完整 tooltip/style 子树 patch 验证、日趋势图 final dual-axis spec 验证，以及官方 Tableau XSD 适配层冒烟

## 1. 目标与范围

本子项目用于解决旧 Tableau workbook 在多 datasource 场景下的三个核心问题：

1. 不能准确识别 worksheet 当前实际绑定了哪些 datasource。
2. 不能按 datasource 正确输出字段目录，导致多数据源 workbook 被误判为“Unknown field”。
3. 后续做最小 XML 修补时，缺少稳定的 worksheet 画像层作为前置检查。
4. 直接创建 / 修改新版本 TWB 时，缺少官方 XSD 结构基线，容易只凭样板经验误写元素顺序、必填属性或枚举值。

本阶段目标不是做一个通用 Tableau authoring 平台，而是先做一个窄而稳的 HEFANG 定向 MCP。

## 2. 为什么不另起仓库

当前更适合放在 hefang_dw 仓内，作为独立子项目：

- 它服务的是 HEFANG 当前真实的 Tableau 旧 workbook 修复链路，不是脱离业务语境的通用库。
- 当前事实源、错误台账、设计边界和后续验证材料都在本仓库内。
- VS Code 侧已有 MCP 接入习惯，后续只需增加一个新的 server 入口，不必维护第二个独立工作区。

结论：放在仓内，但目录隔离为独立 Python 子项目。

## 3. 首版边界

### 3.1 本阶段实现

- 解析 `.twb` / `.twbx`
- 输出 workbook 级概览
- 输出 datasource 级字段目录
- 输出 worksheet 级 datasource 绑定、shelf 文本和依赖字段
- 通过 MCP 暴露 profiling 工具
- 通过 `validate_field_refs` 显式校验 worksheet + datasource 作用域下的字段合法性
- 通过 `patch_chart_bindings` 对单张 worksheet 做最小 XML patch
- 通过 `validate_workbook_schema` 在官方 schema 覆盖版本内做 TWB XSD 结构校验，并对旧版存量 workbook 明确返回跳过原因

### 3.2 本阶段不实现

- 不覆盖现有 cwtwb 全部工具面
- 不直接重建 worksheet
- 不自动推断跨 datasource 字段归属
- 不在未校验前直接写回 workbook
- 不自动修改 `<workbook version>` / `<workbook original-version>` 以迎合官方 XSD

## 4. 架构决策

### 4.1 状态模型

MCP server 保持单个 active workbook session：

- `open_workbook_profile` 打开并缓存当前 workbook profile
- 其余工具基于当前 active profile 查询
- 当前阶段不实现 close，重开新 workbook 即覆盖旧 session

### 4.2 字段注册策略

不能再沿用“一个 editor 只有一个全局 field registry”的模型。

首版策略：

- `datasource_name -> DatasourceProfile`
- 每个 datasource 自己持有字段列表
- worksheet profile 只声明自己依赖了哪些 datasource
- `list_fields` 默认必须显式带 `datasource_name`，或按 `worksheet_name` 先求出依赖 datasource 再返回分组结果

### 4.3 写入策略

后续如进入写入阶段，原则是“最小 patch，而不是整张 worksheet 重建”：

- 优先改 `rows` / `cols`
- 优先改 `pane/encodings`
- 按 worksheet 当前 datasource-dependencies 补最小依赖
- 如需新增绑定字段，同时补 `column` 与 `column-instance`
- 若要从单 pane 升级到 dual-axis / 多 pane，优先用 `replace_panes + pane_specs` 明确声明目标 pane 结构，并对齐到 `table > panes > pane` 的真实层级，而不是在原 pane 上做隐式猜测
- 若需要补 tooltip、label、axis style 等展示层节点，优先按最小 XML 子树写入 `table_view_spec`、`table_style_spec` 和 pane 级子树 spec，而不是回退到手工编辑
- 尽量不动 window、dashboard zone、无关 style 节点

### 4.4 官方 Schema 策略

- 官方 `tableau/tableau-document-schemas` 当前按 `schemas/YYYY_R/twb_YYYY.R.0.xsd` 发布 TWB XSD。
- `validate_workbook_schema` 先读取 `<workbook version>`，再把 `26.1` 这类版本映射为 `2026_1/twb_2026.1.0.xsd`。
- 若 workbook 版本小于 `26.1`，返回 `status="skipped"`，并提醒不要为 XSD 校验而升级旧文件版本。
- 官方主 XSD 没有随仓库提供 `http://www.tableausoftware.com/xml/user` 的 companion schema；本项目在缓存目录生成最小 `UserAttributes-AG` adapter，让 `xmlschema` 能解析 `user:UserAttributes-AG` 引用。
- `.twbx` 只提取包内第一个 `.twb` 做 XML 校验；不宣称官方 schema 已校验整个包。
- XSD 校验只作为结构基线，后续仍必须执行字段画像、字段引用校验和 Tableau 重开渲染测试。

## 5. 当前 MCP 工具设计

### 5.1 `open_workbook_profile(file_path)`

用途：打开 `.twb` / `.twbx`，建立 active profile。

返回内容：

- workbook 路径
- workbook 内 datasource 数量
- worksheet 数量
- dashboard 数量
- 每个 worksheet 当前绑定的 datasource 列表

### 5.2 `list_datasources()`

用途：列出当前 workbook 的 datasource 基本信息。

返回内容：

- datasource name
- caption
- 是否 `hasconnection=false`
- 字段数

### 5.3 `get_worksheet_profile(worksheet_name)`

用途：解释单张 worksheet 的结构和依赖范围。

返回内容：

- worksheet name
- datasource-dependencies 列表
- `table/rows` 文本
- `table/cols` 文本
- mark classes
- encodings 标签
- 每个 datasource 在 dependencies 中出现的字段

### 5.4 `list_fields(datasource_name=None, worksheet_name=None)`

用途：按 datasource 输出字段，而不是误导性地输出全局混合字段集。

规则：

- 传 `datasource_name` 时，只返回该 datasource 的字段目录
- 传 `worksheet_name` 时，返回该 worksheet 依赖 datasource 的分组字段目录
- 两者都不传时，只返回 datasource 摘要，不返回全局 Known fields 清单

### 5.5 `validate_field_refs(...)`

用途：在一个显式的 `worksheet + datasource` 作用域下，校验字段引用是否合法。

当前支持输入：

- `field_refs`
- `rows_text`
- `cols_text`
- `encodings`
- `dependency_fields`

返回内容：

- 解析后的 datasource name / caption
- worksheet 当前实际绑定的 datasource 列表
- 有效字段列表
- 无效字段列表
- 是否整体校验通过

### 5.6 `patch_chart_bindings(...)`

用途：不重建 worksheet，只对目标 worksheet 做最小 XML patch。

当前支持 patch：

- `rows_text`
- `cols_text`
- `mark_class`
- `encodings`
- `remove_encodings`
- `dependency_fields`
- `pane_index`
- `replace_panes`
- `pane_specs`
- `table_view_spec`
- `table_style_spec`
- `pane_specs[*].child_specs`
- `pane_specs[*].view_spec` / `mark_spec` / `encodings_spec` / `customized_tooltip_spec` / `customized_label_spec` / `style_spec`
- 增强后的 `updated_panes` 摘要（`child_tags`、`has_customized_tooltip`、`has_customized_label`）

写入原则：

- 先调用 `validate_field_refs`
- 只改目标 worksheet 的 `table/rows`、`table/cols`、目标 pane 的 `mark` / `encodings`
- 只对指定 datasource 的 `datasource-dependencies` 做最小补齐
- 若启用 `replace_panes`，则优先定位或创建 `table > panes` 容器，再按 `pane_specs` 重建 pane attrs / mark / encodings
- 若提供 table 或 pane 子树 spec，则只替换该节点本身，不扩散到无关 worksheet 或 dashboard
- 自动刷新 active workbook profile

### 5.7 `validate_workbook_schema(...)`

用途：在官方 schema 覆盖范围内验证 `.twb` 结构；对 `.twbx` 提取包内主 `.twb` 验证。

当前支持输入：

- `file_path`：可选；不传时使用 active workbook 的路径
- `schema_path`：可选；传入时强制使用本地 XSD，适合调试官方 schema adapter
- `allow_download`：默认允许从官方 raw URL 下载缺失 XSD 到本地缓存
- `max_errors`：最多返回的 XSD 错误数量

返回内容：

- `status`：`passed` / `failed` / `skipped`
- `valid`：`True` / `False` / `None`
- workbook 根节点版本、`source-build`、`source-platform`
- 选用的 schema 路径与 `user` namespace adapter 路径
- `errors[*].path` / `reason` / `message`
- 结构校验与语义渲染边界提示

## 6. 目录结构

```text
mcp_servers/tableau_worksheet_mcp/
├── DESIGN.md
├── README.md
├── pyproject.toml
├── src/
│   └── tableau_worksheet_mcp/
│       ├── __init__.py
│       ├── models.py
│       ├── official_schema.py
│       ├── profiler.py
│       └── server.py
└── tests/
    └── .gitkeep
```

## 7. 与现有 cwtwb 的关系

- 当前不是替换 cwtwb。
- 当前是补一层更可靠的 worksheet / datasource profiling 能力。
- 后续若这套 profiling + patch 链路稳定，再决定是否继续补 authoring 或把能力回灌到 cwtwb 分支。

## 8. 当前验收标准

满足以下条件即可认为 v0.5 首轮可用：

1. 能打开 `.twb` / `.twbx`
2. 能列出多个 datasource，而不是只识别一个 primary datasource
3. `get_worksheet_profile` 能看出某张 worksheet 当前挂的是哪几个 datasource
4. `list_fields` 不再输出误导性的单一全局字段集
5. `validate_field_refs` 能在显式 worksheet + datasource 范围内正确判定字段合法性
6. `patch_chart_bindings` 能对目标 worksheet 副本做最小写入，并在回读后体现变更
7. `patch_chart_bindings` 能通过 `replace_panes + pane_specs` 把单 pane worksheet 提升为多 pane 骨架
8. `patch_chart_bindings` 能对单 pane worksheet 一次性补写 tooltip encodings、自定义 tooltip 与 axis style，并在回读后保留正确 `mark class`
9. `patch_chart_bindings` 能把日趋势图 dual-axis 骨架推进到包含 table 级 axis rule、pane style、`mark-sizing` 与 `customized-tooltip` 的 final spec
10. `validate_workbook_schema` 能加载官方 `twb_2026.1.0.xsd` 与本地 `user` namespace adapter
11. `validate_workbook_schema` 对旧版 `version="18.1"` workbook 返回 `skipped`，不误导 agent 擅自升版本
12. Python 代码语法校验通过

## 9. 下一阶段建议

1. 获取或生成真实 `version="26.1"` workbook 后，为 `validate_workbook_schema` 补正式 passed/failed 样例。
2. 核对日趋势图是否还存在 inspect 未覆盖的 UI 层属性，再决定是否补 `customized-label` 或其它 pane 元数据。
3. 继续完善 patch 回执摘要，让多 pane / 子树 patch 的输出更接近结构化 diff。
4. 若最小 patch 链路稳定，再考虑做更高层的 recipe，而不是直接回到整张 worksheet authoring。

## 版本记录

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v0.6 | 2026-05-14 | 吸收 Tableau 官方 document schemas，新增 `validate_workbook_schema`、官方 XSD 缓存和 `user` namespace adapter，并明确旧版 `18.1` workbook 跳过边界 |
| v0.5 | 2026-05-09 | 新增日趋势图 final dual-axis patch spec，并增强 `updated_panes` 摘要为 child_tags / customized tooltip/label presence |
| v0.4 | 2026-05-09 | 新增 table/pane 级完整子树 patch 能力，并在累计趋势图副本上验证 `mark`、多 tooltip encodings、自定义 tooltip 与 axis style 可一次性落盘 |
| v0.3 | 2026-05-09 | 新增 `replace_panes + pane_specs` 多 pane patch 能力，并完成真实备份副本的日趋势图 dual-axis 骨架验证与 `table > panes > pane` 层级对齐 |
| v0.2 | 2026-05-09 | 新增 validate_field_refs 与 patch_chart_bindings，完成真实 workbook 的字段校验与最小 patch 首轮验证 |
| v0.1 | 2026-05-09 | 创建仓内独立 Tableau Worksheet MCP 子项目设计基线与首版骨架 |
