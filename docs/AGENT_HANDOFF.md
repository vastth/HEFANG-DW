# AGENT_HANDOFF.md — Agent 协作交接日志

> **这是 Claude Code 与 GitHub Copilot 之间的共享状态文件。**
>
> - **Claude Code**：每次完成一组有意义的代码/文档变更后，必须在本文件顶部追加一条记录。
> - **GitHub Copilot**：接手任何任务（审计、续写、重构）前，必须先读本文件最新一条记录，了解当前项目状态。
>
> **格式约定**：新记录追加在"交接日志"节的顶部（最新在最前）。保留最近 10 条，更早的归档到 `docs/AGENT_HANDOFF_archive.md`。
>
> **写入方式**：
> ```bash
> # 推荐：使用辅助脚本（自动格式化 + 追加）
> python scripts/log_agent_action.py \
>   --agent "Claude Code" \
>   --action "新增文件" \
>   --summary "一句话描述" \
>   --files "路径1:新增:说明" "路径2:修改:说明" \
>   --notes "Copilot 接棒须知1" "接棒须知2" \
>   --todos "未完成项1" "未完成项2"
>
> # 或直接手动在本文件顶部追加（见下方模板）
> ```

---

## 交接日志

---

### [2026-03-18 15:19] · GitHub Copilot · 修复 run_etl 静态报错

**摘要**：将 stdout/stderr 的 UTF-8 重配置改为类型检查友好的封装写法

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `run_etl.py` | 修改 | 封装 reconfigure 调用，消除 TextIO 属性报错 |

**Copilot 接棒须知**：
- 本次仅修复 `run_etl.py` 中 `sys.stdout` / `sys.stderr` 的静态检查报错，未改动 ETL 业务逻辑。
- `run_etl.py` 在本轮之前已存在其他未提交改动，本次交接记录不覆盖那些历史变更。

**未完成项**：
- [x] 已完成

### [2026-03-18 15:05] · GitHub Copilot · 执行 doc-sync 对齐文档

**摘要**：修正 RUNBOOK 示例输出名并为文档审计脚本补降噪词，清理本轮高风险与伪中风险项

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/RUNBOOK.md` | 修改 | 将查数与导出示例输出名改为通用占位 |
| `scripts/check_doc_sync.py` | 修改 | 为本轮确认的伪中风险项增加降噪词 |
| `reports/docs_code_alignment.json` | 修改 | 复跑文档审计输出最新结果 |

**Copilot 接棒须知**：
- 本轮 doc-sync 主要处理 RUNBOOK 中写死的示例输出名，以及 check_doc_sync.py 对 query_data/export_ads/索引名的词法误报。
- 该轮记录写入时实际仍残留 1 个 docs-only 高风险词 `ads_inventory_health_export`；后续已继续修正 RUNBOOK 示例输出名并需再次复扫确认。

**未完成项**：
- [ ] 如需进一步降低 low risk 噪音，可继续扩充 scripts/check_doc_sync.py 的 STOPWORDS，但不影响当前交付

---

### [2026-03-18 14:55] · GitHub Copilot · 验证 MCP 启动前提并修正示例配置

**摘要**：确认 .mcp.json、npx、uvx 与关键环境变量均可用，但当前聊天会话仍未暴露 MCP 工具；同步修正 RUNBOOK 中的 MCP 示例为 mcpServers 格式

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/RUNBOOK.md` | 修改 | 将 MCP 配置示例对齐为当前实际使用的 mcpServers / MYSQL_PASS / ORACLE_CONNECTION_STRING 格式 |
| `docs/AGENT_LESSONS.md` | 修改 | 追加 MCP 会话可见性经验 |

**Copilot 接棒须知**：
- 当前已验证 `.mcp.json` 配置文件存在，且 `npx -y @benborla29/mcp-server-mysql`、`uvx mcp-server-oracle` 手动启动无立即错误。
- 当前会话仍未出现 `mcp__mysql__...` / `mcp__oracle__...` 工具，说明“server 可启动”与“当前聊天工具面已挂载”是两个不同层次。

**未完成项**：
- [ ] 使用全新聊天会话再次验证 MCP 工具是否已暴露给代理。
- [ ] 若新会话仍无 MCP 工具，进一步检查宿主是否读取了当前仓库的 `.mcp.json`。


---

### [2026-03-18 14:48] · GitHub Copilot · 新增经验台帐与复盘机制

**摘要**：解释 MCP 可见性边界，新增 Agent 经验台帐、记录脚本、OpenCode lesson 命令与复盘提醒机制

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `scripts/log_agent_lesson.py` | 新增 | 新增经验台帐结构化写入脚本 |
| `docs/AGENT_LESSONS.md` | 新增 | 新增共享经验台帐并写入首条 Oracle 字段映射经验 |
| `.claude/settings.json` | 修改 | 增加经验复盘提示型 Hook |
| `.github/copilot-instructions.md` | 修改 | 增加经验台帐强制落盘规则与检查项 |
| `AGENTS.md` | 修改 | 增加经验台帐原则与 `/lesson` 命令 |
| `opencode.json` | 修改 | 注册 `/lesson` 命令 |
| `.opencode/commands/lesson.md` | 新增 | 新增 OpenCode 经验记录命令模板 |
| `README.md` | 修改 | 补充经验台帐入口 |
| `docs/RUNBOOK.md` | 修改 | 补充经验台帐写入命令与 Hook 边界 |
| `docs/ARCHITECTURE.md` | 修改 | 补充经验台帐与复盘执行面 |
| `CHANGELOG.md` | 修改 | 记录 v0.7.5 经验台帐机制 |

**Copilot 接棒须知**：
- 当前仓库已具备“台帐文档 + 写入脚本 + OpenCode 命令 + Claude 提示型 Hook”的第一版经验复盘机制。
- GitHub Copilot 当前仍未确认存在可由仓库本地强制注入的“会话结束自动写台帐”钩子，因此收尾时仍需主动判断是否要记账。

**未完成项**：
- [ ] 如需真正验证 MCP 是否能挂成可调用工具，需在本地重载编辑器会话并检查工具面板是否出现 mysql/oracle MCP 工具。
- [ ] 如需把经验台帐进一步自动同步到 repo memory，可在后续迭代补一条专用工作流或脚本说明。


---

### [2026-03-18 14:14] · GitHub Copilot · 修复 tools 直跑导入并新增只读查数工作流

**摘要**：修复 tools 目录脚本任意 cwd 直跑导入问题，新增通用只读查数工具、data-query skill/agent，并同步 README、RUNBOOK、ARCHITECTURE 与 CHANGELOG

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `tools/snapshot_mysql_hefangdw_schema.py` | 修改 | 改为基于 REPO_ROOT 导入 config 并解析输出路径 |
| `tools/snapshot_oracle_bosnds3_schema.py` | 修改 | 改为基于 REPO_ROOT 导入 config、读取 docs 并解析输出路径 |
| `tools/test_connection.py` | 修改 | 补齐 REPO_ROOT 导入逻辑，支持从 tools 目录直接运行 |
| `tools/export_ads.py` | 修改 | 新增 argparse 与稳定输出路径，保持 ads_inventory_health 只读导出 |
| `tools/query_data.py` | 新增 | 新增 MySQL/Oracle 通用只读查询与导出工具 |
| `.claude/skills/data-query/SKILL.md` | 新增 | 新增 data-query 查询路由技能 |
| `.claude/agents/data-query-agent.md` | 新增 | 新增数据查询与对账专家 agent 定义 |
| `README.md` | 修改 | 补充只读查数与结构快照入口 |
| `docs/RUNBOOK.md` | 修改 | 补充 MCP 与只读查数说明及版本记录 |
| `docs/ARCHITECTURE.md` | 修改 | 补充 data-query skill/agent、query_data 工具与查询执行面说明 |
| `CHANGELOG.md` | 修改 | 记录 v0.7.4 只读查数工具与路径修复 |

**Copilot 接棒须知**：
- tools 目录下的快照、导出、连接测试脚本现在都可以从非仓库根目录直接启动。
- 自由查数工作流已落到 tools/query_data.py，推荐顺序是 MCP 只读优先，失败时回退到 Python 查询工具。

**未完成项**：
- [ ] 如需真正启用 MCP，仍需本地创建 .mcp.json 并验证只读权限。
- [ ] 如需让自然语言直接自动生成更复杂业务 SQL，后续还可继续沉淀模板。


---

### [2026-03-18 14:05] · GitHub Copilot · 全量复核 MYSQL数据字典 并复跑审计

**摘要**：按最新 MySQL 快照对 docs/MYSQL数据字典.md 全表复核，16/16 张 MySQL 表确认无高置信字段漂移；复跑 scripts/check_doc_sync.py 审计

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/AGENT_HANDOFF.md` | 修改 | 追加本轮全量复核与审计复跑记录 |
| `reports/docs_code_alignment.json` | 修改 | 复跑文档审计输出最新结果 |

**Copilot 接棒须知**：
- MYSQL数据字典 与最新 MySQL 快照当前已对齐，可优先把后续关注点转到真正的结构/口径漂移，而不是继续逐表核字典。
- 本轮审计 high 仍为 0；medium 从 2 变为 3，但新增项是 idx_channel_code / idx_store_code / idx_wing_code 这类索引名词法噪音，不是结构漂移。
- docs_code_alignment 的 medium 增量来自 dim_channel 相关索引名，不属于高风险结构差异。

**未完成项**：
- [x] 已完成



---

### [2026-03-18 13:54] · GitHub Copilot · 修正 ads_inventory_health 数据字典

**摘要**：按最新 MySQL 快照修正 docs/MYSQL数据字典.md 中 ads_inventory_health 的字段顺序、可空性与默认值

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/MYSQL数据字典.md` | 修改 | 同步 ads_inventory_health 与最新快照一致 |

**Copilot 接棒须知**：
- 本次仅修改文档，不涉及 ETL 逻辑或表结构变更。
- 修正依据为 reports/snapshot_mysql_hefangdw_schema.json（2026-03-18 13:49:40）。
- dim_channel 的 WING_CODE 字段在快照与文档中已一致，无需继续修改。

**未完成项**：
- [ ] 如需进一步消除文档漂移，可继续核对其他表在 docs/MYSQL数据字典.md 中的可空性与默认值



---

### [2026-03-18 13:51] · GitHub Copilot · 执行 schema-snap 快照审计

**摘要**：更新 MySQL/Oracle 结构快照并完成 MySQL 数据字典字段漂移扫描

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `reports/snapshot_mysql_hefangdw_schema.json` | 修改 | 更新 MySQL 结构快照 |
| `reports/snapshot_oracle_bosnds3_schema.json` | 修改 | 更新 Oracle 结构快照 |

**Copilot 接棒须知**：
- MySQL 快照覆盖 16 张表，Oracle 快照覆盖 10 张表。
- dim_channel 的 WING_CODE 字段在快照与文档中一致。
- 发现 ads_inventory_health 与文档存在高置信可空性和默认值差异，尚未改文档。

**未完成项**：
- [ ] 如需消除漂移，更新 docs/MYSQL数据字典.md 中 ads_inventory_health 的字段可空性与默认值说明




---

### [2026-03-18 13:46] · GitHub Copilot · 重命名 dim_channel 字段

**摘要**：将 dim_channel 的 store_code 目标字段更名为 WING_CODE，并同步 ETL、DDL、测试与文档

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `etl_dim_channel.py` | 修改 | 目标列改为 WING_CODE |
| `test_etl_automation.py` | 修改 | 校验改为查询 WING_CODE |
| `SQL/create_dim_channel.sql` | 修改 | 字段名改为 WING_CODE |
| `SQL/alter_dim_channel_rename_store_code_to_wing_code.sql` | 新增 | 现网字段改名迁移脚本 |
| `README.md` | 修改 | 同步 dim_channel 字段名 |
| `docs/DATA_CONTRACTS.md` | 修改 | 同步契约字段名 |
| `docs/ETL业务逻辑说明.md` | 修改 | 同步映射字段名 |
| `docs/MYSQL数据字典.md` | 修改 | 同步数据字典字段名 |
| `docs/数据结构与映射手册.md` | 修改 | 同步字段映射说明 |
| `docs/数据仓库与ETL手册.md` | 修改 | 同步建表结构 |
| `CHANGELOG.md` | 修改 | 记录 v0.7.3 字段更名 |

**Copilot 接棒须知**：
- dim_channel 目标字段已由 store_code 更名为 WING_CODE。
- 已新增 SQL/alter_dim_channel_rename_store_code_to_wing_code.sql 用于现网迁移。
- Python 静态错误检查已通过。

**未完成项**：
- [ ] 执行 SQL/alter_dim_channel_rename_store_code_to_wing_code.sql 完成现网字段改名
- [ ] 执行 etl_dim_channel.py 或 run_etl.py 验证 dim_channel.WING_CODE 已按 Oracle WING_CODE 回填





---

### [2026-03-18 13:43] · GitHub Copilot · 修正 dim_channel 店仓映射

**摘要**：将 dim_channel.store_code 从回退口径改为直接映射 O2O_RETAIL_CHANNEL.WING_CODE，并同步测试与文档

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `etl_dim_channel.py` | 修改 | store_code 改为直接抽取 WING_CODE |
| `test_etl_automation.py` | 修改 | dim_channel 校验改为检查 store_code=DS001 |
| `SQL/create_dim_channel.sql` | 修改 | 修正 store_code 字段注释来源 |
| `README.md` | 修改 | 修正 dim_channel 字段说明 |
| `docs/DATA_CONTRACTS.md` | 修改 | 修正 dim_channel 契约与DQ规则 |
| `docs/ETL业务逻辑说明.md` | 修改 | 修正 dim_channel 映射逻辑说明 |
| `docs/MYSQL数据字典.md` | 修改 | 修正 dim_channel 字段说明 |
| `docs/数据结构与映射手册.md` | 修改 | 修正 WING_CODE 语义与直连映射说明 |
| `CHANGELOG.md` | 修改 | 记录 v0.7.3 店仓映射修正 |

**Copilot 接棒须知**：
- 本次已改动 etl_dim_channel.py，不再使用 CODE 作为 store_code 回退值。
- 已核实 Oracle BOSNDS3.O2O_RETAIL_CHANNEL 共 87 条记录且 WING_CODE 全部非空，因此直连映射不会减少记录数。
- 目标库是否已完成真实回填仍需执行 etl_dim_channel.py 或 run_etl.py 验证。

**未完成项**：
- [ ] 在目标环境执行 etl_dim_channel.py 或 run_etl.py，确认 dim_channel.store_code 已按 WING_CODE 回填
- [ ] 回填后复核 docs/TODO_ISSUES.md 的 P1-001 是否可关闭






---

### [2026-03-18 13:28] · GitHub Copilot · 修正文档中的 dim_channel 结论

**摘要**：将 P1-001 从已解决改为待验证，并澄清 O2O_RETAIL_CHANNEL 字段语义

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/TODO_ISSUES.md` | 修改 | 将 P1-001 调整为链路已补齐但目标库待回填验证 |
| `docs/数据结构与映射手册.md` | 修改 | 补充 CODE/WING_CODE/NAME 语义与 DS 店仓编码说明 |
| `docs/DATA_CONTRACTS.md` | 修改 | 修正 dim_channel 契约为目标设计已具备但实库待验证 |
| `docs/ETL业务逻辑说明.md` | 修改 | 补充 dim_channel 链路已补齐但未验证写库 |
| `docs/MYSQL数据字典.md` | 修改 | 标注 dim_channel 目标库现存数据待验证 |

**Copilot 接棒须知**：
- 本次仅修正文档结论，未改动 etl_dim_channel.py 与 run_etl.py 等 ETL 代码。
- 当前高置信结论是 WING_CODE 更符合 DS001 这类店仓编码语义，CODE 应保留为渠道档案编码。
- 若目标库 dim_channel.store_code 仍为纯数字，需要先执行 etl_dim_channel.py 回填，再决定是否关闭 P1-001。

**未完成项**：
- [ ] 在目标环境执行 etl_dim_channel.py 或 run_etl.py，验证 dim_channel.store_code 已回填为 DS 编码
- [ ] 回填完成后重新评估并更新 docs/TODO_ISSUES.md 的 P1-001 状态








## 模板（新记录请按此格式）

```markdown
---

### [YYYY-MM-DD HH:MM] · <Claude Code | GitHub Copilot> · <操作类型>

**摘要**：<一句话描述做了什么>

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `path/to/file` | 新增/修改/删除 | 具体说明 |

**影响范围**：<受影响的功能/表/ETL步骤/文档>

**Copilot 接棒须知**：
- <注意事项，例如：某文件与某代码需保持同步>
- <风险点或需要人工确认的口径>

**未完成项**：
- [ ] <TODO>
```
