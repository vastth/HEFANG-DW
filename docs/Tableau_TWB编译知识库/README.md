# Tableau TWB 编译知识库

> 版本：v0.4  
> 日期：2026-05-14  
> 适用范围：通过直接编辑 / 生成 Tableau `.twb` XML 来辅助 HEFANG 销售看板、门店日报看板、后续经营驾驶舱开发。

## 1. 文档定位

- 本目录用于沉淀“从真实 Tableau 工作簿样板中学习到的 `.twb` 编译经验”，服务后续自动生成、批量修改、修复 Tableau 工作簿。
- 本目录正文不内嵌 14 份样板 `.twb` 的完整 XML；[example/](example/) 是用户本轮放入的学习素材目录，文档只引用结构画像、抽象模式、编译规则和验证清单。
- 后续用户说“编译 twb”“直接改 twb”“自动生成 Tableau 看板”“批量改字段别名 / dashboard XML”时，优先读取项目 Skill：[../../.github/skills/tableau-twb-compiler-hefang/SKILL.md](../../.github/skills/tableau-twb-compiler-hefang/SKILL.md)。

## 2. 本轮学习材料与证据

| 证据 | 位置 | 说明 |
|---|---|---|
| 样板提取清单 | [../../reports/tableau_twb_extraction_manifest_20260508.csv](../../reports/tableau_twb_extraction_manifest_20260508.csv) | 14 个 `.twbx` 均已成功提取为同名 `.twb` |
| 结构画像 JSON | [../../reports/context_cache/tableau_twb_corpus_profile_20260508.json](../../reports/context_cache/tableau_twb_corpus_profile_20260508.json) | 解析 14 份 `.twb` 的 datasource、worksheet、dashboard、action、format、parameter、calculation 摘要 |
| 工作簿摘要 CSV | [../../reports/context_cache/tableau_twb_corpus_workbook_summary_20260508.csv](../../reports/context_cache/tableau_twb_corpus_workbook_summary_20260508.csv) | 每个样板的 worksheet / dashboard 数、dashboard 名称、尺寸、mark 类型 |
| 分析脚本 | [../../reports/context_cache/analyze_tableau_twb_corpus_20260508.py](../../reports/context_cache/analyze_tableau_twb_corpus_20260508.py) | 本轮只读解析脚本，用于复跑或扩展画像 |
| 详细学习笔记 | [14份样板学习笔记_20260508.md](14份样板学习笔记_20260508.md) | 本轮从 14 份样板中提炼出的编译规则 |
| 示例素材目录 | [example/](example/) | 用户放入的 14 份 `.twb` 与 10 张同名 `.png` 效果图；4 份 `.twb` 当前缺少同名 PNG |
| 视觉联合画像 JSON | [../../reports/context_cache/tableau_twb_visual_corpus_profile_20260508.json](../../reports/context_cache/tableau_twb_visual_corpus_profile_20260508.json) | 解析 [example/](example/) 中 `.twb` / `.png` 的同名匹配、尺寸、调色板、dashboard zone 摘要 |
| 视觉联合摘要 CSV | [../../reports/context_cache/tableau_twb_visual_corpus_summary_20260508.csv](../../reports/context_cache/tableau_twb_visual_corpus_summary_20260508.csv) | 每个样板的 PNG 尺寸、主色、dashboard 名称、固定尺寸、mark 类型 |
| 视觉联合学习笔记 | [视觉效果图联合学习_20260508.md](视觉效果图联合学习_20260508.md) | 基于 PNG 效果图与 TWB XML 的视觉母版、卡片、导航、漏斗、暗色专题页编译规则 |
| 视觉联合分析脚本 | [../../reports/context_cache/analyze_tableau_twb_visual_corpus_20260508.py](../../reports/context_cache/analyze_tableau_twb_visual_corpus_20260508.py) | 本轮只读解析脚本，用于复跑 [example/](example/) 的 TWB + PNG 画像 |
| 官方 Schema 指南 | [官方Schema吸收指南_20260514.md](官方Schema吸收指南_20260514.md) | 吸收 `tableau/tableau-document-schemas` 官方 XSD，明确 2026.1+ 结构校验、18.1 存量边界和 MCP 工具使用方式 |
| Skill 资产 | [../../.github/skills/tableau-twb-compiler-hefang/assets/twb_compilation_patterns.md](../../.github/skills/tableau-twb-compiler-hefang/assets/twb_compilation_patterns.md) | 供 Skill 快速加载的模式表与检查清单 |
| 错误修复台帐 | [Tableau_TWB错误修复台帐.md](Tableau_TWB错误修复台帐.md) | 沉淀 `.twb` 编译后在用户重开工作簿渲染测试阶段暴露出的报错、根因、修复动作与预防规则 |

原始外部样板目录：`D:/tianhao/Documents/我的 Tableau 存储库/工作簿_twb`。该目录在用户本机，不作为仓库追踪内容；当前 [example/](example/) 是用户另行放入本知识库的学习素材快照。

## 3. 与现有 Tableau 项目的衔接

当前 HEFANG 销售看板已有以下硬边界，后续直接编译 `.twb` 时必须继续遵守：

| 现有约束 | 来源 | 对 `.twb` 编译的影响 |
|---|---|---|
| 单工作簿、多数据源、按主题分工作表 | [../销售部数据治理-子项目/销售看板Tableau实施与追踪.md](../销售部数据治理-子项目/销售看板Tableau实施与追踪.md) | 不在 Tableau 数据源层强行做 ADS 之间联表 |
| MySQL 开发阶段默认实时连接 | [../销售部数据治理-子项目/销售看板Tableau实施与追踪.md](../销售部数据治理-子项目/销售看板Tableau实施与追踪.md) | 编译 `.twb` 时优先保留用户已建好的连接，不写入凭据 |
| 计算字段优先放 Tableau 语义层 | [../销售部数据治理-子项目/销售看板Tableau实施与追踪.md](../销售部数据治理-子项目/销售看板Tableau实施与追踪.md) | 只补展示层计算，不倒逼新增 ETL / 视图 |
| `report_date`、`data_version` 必须保留 | [../销售部数据治理-子项目/销售看板Tableau实施与追踪.md](../销售部数据治理-子项目/销售看板Tableau实施与追踪.md) | 生成 worksheet / dashboard 时必须统一过滤批次 |
| 首页模板先对齐结构和信息层级，视觉细节后置 | [../销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md](../销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md) | 自动编译优先保证模块、字段、阅读顺序，不追求像素级复刻 |
| KPI 比例必须“汇总后再计算” | [../销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md](../销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md) | `.twb` 中生成 KPI 计算字段时禁止对行级比例做 `AVG()` / `SUM()` |

## 4. 后续使用方式

1. 用户提出 Tableau 编译类任务时，先加载 [../../.github/skills/tableau-twb-compiler-hefang/SKILL.md](../../.github/skills/tableau-twb-compiler-hefang/SKILL.md)。
2. 若任务涉及页面视觉母版，读取 [视觉效果图联合学习_20260508.md](视觉效果图联合学习_20260508.md)，先确定左侧导航、顶部 hero、卡片网格、漏斗专题、经营驾驶舱中的哪一种基准。
3. 若任务涉及 HEFANG 销售看板，继续读取 [../销售部数据治理-子项目/销售看板Tableau实施与追踪.md](../销售部数据治理-子项目/销售看板Tableau实施与追踪.md) 和 [../销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md](../销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md) 的当前状态。
4. 编译前复制目标 `.twb` 备份，再修改 XML。
5. 若目标 workbook 版本在官方 schema 覆盖范围内，调用 `tableau_worksheet_mcp.validate_workbook_schema` 做 XSD 结构校验；若返回 `skipped`，按 [官方Schema吸收指南_20260514.md](官方Schema吸收指南_20260514.md) 继续走旧版存量工作簿校验链路。
6. 编译后至少完成 XML 语法解析、关键节点计数、目标工作表 / dashboard 名称检查；如果用户打开 Tableau 验证，再记录实际渲染反馈。
7. 若用户重开工作簿后出现报错、空白、字段失效或其它阻塞，Agent 默认先尝试修复，再把“现象 / 根因 / 修复 / 验证 / 预防”写入 [Tableau_TWB错误修复台帐.md](Tableau_TWB错误修复台帐.md)。

## 5. 版本记录

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v0.4 | 2026-05-14 | 吸收 Tableau 官方 document schemas 仓库，新增官方 Schema 指南，并把 `validate_workbook_schema` 纳入后续使用方式 |
| v0.3 | 2026-05-08 | 新增 Tableau TWB 错误修复台帐入口，并固定“重开工作簿渲染报错后先修复再登记”的知识库流程 |
| v0.2 | 2026-05-08 | 纳入 [example/](example/) 中 14 份 `.twb` 与 10 张同名 `.png` 的联合学习证据，补充视觉母版与后续使用方式 |
| v0.1 | 2026-05-08 | 新增 Tableau TWB 编译知识库入口，登记 14 份样板画像证据、项目级 Skill 与后续使用流程 |
