# Tableau Worksheet MCP

这是放在 hefang_dw 仓内的独立 Python MCP 子项目，用来解决旧 Tableau workbook 在多 datasource 场景下的 worksheet 画像、字段域识别和后续最小变更写入问题。

当前阶段已经进入第四阶段：在 worksheet + datasource profiling / patch 基础上，新增 Tableau 官方 document schemas 的结构校验能力。`validate_workbook_schema` 会按 workbook 版本判断是否可用官方 XSD，自动缓存官方 schema，并在官方主 XSD 缺少 `user` 命名空间 schema 时补本地兼容 adapter。

## 为什么放在仓内

- 直接复用 HEFANG 当前的 Tableau 场景文档、样板 workbook 和 VS Code MCP 接入方式。
- 与 ETL 主链隔离，避免把 `etl_*.py`、调度脚本和工具脚本混在一起。
- 后续验证时可以直接引用当前仓库里的业务事实、交接记录和错误修复台账。

## 当前范围

- 读取 `.twb` / `.twbx`
- 画像 workbook / datasource / worksheet
- 按 datasource 输出字段目录
- 按 worksheet 输出 datasource 绑定和依赖字段
- 按 workbook version 对官方 TWB XSD 做结构校验；旧版不覆盖时返回 `skipped` 而不是误判失败
- 按 worksheet + datasource 显式校验字段合法性
- 对单张 worksheet 做最小 XML patch：更新 `rows` / `cols` / `pane/encodings` / `datasource-dependencies`
- 按 spec 最小替换 table 级 `view` / `style` 与 pane 级完整子树，避免为 tooltip / axis style 回退到手工 XML
- 可选替换现有 pane 结构，并按 spec 在 `table > panes > pane` 容器下重建目标 worksheet 的多 pane 骨架

## 暂不做

- 不做全量 `configure_chart` 替代
- 不做 dashboard 写入
- 不做跨 datasource 自动猜测字段绑定
- 不做整张 worksheet 重建
- 不为通过官方 XSD 而自动升级旧版 workbook 根节点版本

## 目录

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

## 当前工具面

- `open_workbook_profile(file_path)`
- `list_datasources()`
- `get_worksheet_profile(worksheet_name)`
- `list_fields(datasource_name=None, worksheet_name=None)`
- `validate_workbook_schema(file_path=None, schema_path=None, allow_download=True, max_errors=20)`
- `validate_field_refs(worksheet_name, datasource_name, ...)`
- `patch_chart_bindings(worksheet_name, datasource_name, ...)`

当前 `patch_chart_bindings` 额外支持：

- `replace_panes`
- `pane_specs`
- `table_view_spec`
- `table_style_spec`
- `pane_specs[*].child_specs` / `view_spec` / `mark_spec` / `encodings_spec` / `customized_tooltip_spec` / `customized_label_spec` / `style_spec`

当前 `patch_chart_bindings` 的返回摘要也已补强：

- `updated_panes[*].child_tags`
- `updated_panes[*].has_customized_tooltip`
- `updated_panes[*].has_customized_label`

## 已验证状态

- 已通过 `uv run --project` 导入并调用 server。
- 已用仓内样本 workbook 验证 profiling 链路。
- 已用真实外部 workbook `销售部自动化日报.backup_20260509_110820.twb` 验证两张趋势图 worksheet 都只绑定 `ds_ads_daily_sales`。
- 已在 workspace 副本上通过 `patch_chart_bindings` 成功为 `销售趋势分析_日销售趋势` 补写一个 `label` encoding，确认最小 XML patch 可落盘。
- 已在 workspace 副本上通过 `patch_chart_bindings + replace_panes + pane_specs` 将单 pane 日趋势图提升为 3 pane 双轴骨架，且已确认 pane 层级对齐到 `table > panes > pane` 的真实 Tableau 结构。
- 已在 workspace 副本上通过 `patch_chart_bindings + table_style_spec + pane child_specs` 为 `销售趋势分析_累计达成趋势` 一次性补写 `mark`、多 tooltip encodings、自定义 tooltip 和 axis style，确认完整子树 patch 可落盘。
- 已在 workspace 副本上通过 `daily_trend_dual_axis_final_patch.json` 将 `销售趋势分析_日销售趋势` 推进到接近主文件当前形态，确认 3 个 pane 的完整子树、table 级 axis rule、`mark-sizing` 和 `customized-tooltip` 都可按 spec 落盘。
- 已验证官方 `twb_2026.1.0.xsd` 需要本地 `user:UserAttributes-AG` adapter 才能被 `xmlschema` 正常加载。
- 已用真实外部 workbook `销售部自动化日报.backup_20260509_110820.twb` 验证 `validate_workbook_schema` 对 `version="18.1"` 返回 `skipped`，符合存量旧版不强行套 2026.1 XSD 的边界。

## 运行方式

安装依赖后可通过 entry point 启动：

```powershell
python -m pip install -e .
tableau-worksheet-mcp
```

或直接：

```powershell
python -m tableau_worksheet_mcp.server
```

## 下一步

1. 后续拿到 `version="26.1"` 的真实 workbook 后，补一条 `validate_workbook_schema` 的 passed/failed 实例记录。
2. 继续核对日趋势图是否还存在未被 inspect 捕获的 UI 层属性，再决定是否需要补 `customized-label` 或其它 pane 元数据。
3. 在当前摘要基础上继续补更细的 diff 视图，减少手工 XML 比对。
4. 验证稳定后，再考虑是否补更高层的 chart recipe，而不是回到整张 worksheet 重建。
