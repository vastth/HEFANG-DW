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

### [2026-03-26 10:34] · GitHub Copilot · 创建综合 PR

**摘要**：整理 ODS 主链接入、MCP 入口说明与 GitHub 协作模板改动，准备提交综合 PR

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `config.py` | 修改 | 新增 ods_sync 展示名并补 Oracle 对账 SQL |
| `run_etl.py` | 修改 | 主链接入 ods_sync 并在上游失败时跳过 ads_health |
| `etl_ods_fa_storage.py` | 修改 | 补抽 qtypurchaserem 字段 |
| `etl_dws_sales.py` | 修改 | 改为消费 ODS 并增加命名锁重试 |
| `etl_dws_inventory.py` | 修改 | 改为消费 ODS 并增加命名锁重试 |
| `etl_ads_health.py` | 修改 | 增加命名锁与单事务覆盖 |
| `test_etl_automation.py` | 修改 | 补充 Oracle 对账阈值与 dim_channel 校验调整 |
| `SQL/create_ods_tables.sql` | 修改 | 为 ods_fa_storage 补 qtypurchaserem 列 |
| `SQL/alter_ods_fa_storage_add_qtypurchaserem.sql` | 新增 | 提供现网 ODS 补列 SQL |
| `.github/ISSUE_TEMPLATE/config.yml` | 新增 | 新增 Issue 模板统一入口配置 |
| `.github/pull_request_template.md` | 新增 | 新增项目 PR 模板 |
| `README.md` | 修改 | 同步主链 9 步与 ODS 接入说明 |
| `docs/ARCHITECTURE.md` | 修改 | 同步主链含 ODS 与 MCP 入口现状 |
| `docs/DATA_CONTRACTS.md` | 修改 | 同步 ODS 与 dim_channel 契约结论 |
| `docs/ETL业务逻辑说明.md` | 修改 | 同步 ODS 消费链路与锁重试逻辑 |
| `docs/RUNBOOK.md` | 修改 | 补充 MCP 主入口和锁冲突排查说明 |
| `docs/数据仓库与ETL手册.md` | 修改 | 同步 ODS 已纳入主链且 DWS 已消费 ODS |
| `docs/数据结构与映射手册.md` | 修改 | 修正 dim_channel 与库存映射说明 |
| `docs/业务逻辑与指标规范.md` | 修改 | 补充 WING_CODE 与 C_STORE.CODE 字段边界 |
| `docs/MYSQL数据字典.md` | 修改 | 同步 dim_channel 现网结论与 ODS 新字段 |
| `docs/SQL开发手册.md` | 修改 | 标注渠道店仓映射仅适用于 C_STORE.CODE |
| `docs/TODO_ISSUES.md` | 修改 | 关闭 dim_channel 待验证项 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀 MCP 路径与字段语义经验 |
| `AGENTS.md` | 修改 | 补充当前 MCP 可用性事实 |

**Copilot 接棒须知**：
- 当前 PR 范围同时包含 ETL 主链改造、文档同步、MCP 入口说明和 GitHub 协作模板，审阅时建议按模块阅读。
- 已执行 py_compile 与编辑器错误检查；未执行完整 test_etl_automation.py 和真实 run_etl.py 主链复跑。

**未完成项**：
- [ ] 如需进一步收口，执行真实数据库回归：test_etl_automation.py 与 run_etl.py。
- [ ] 如需进一步收口，确认 scripts/check_doc_sync.py 当前是否存在长耗时或挂起场景。

---

### [2026-03-24 13:28] · GitHub Copilot · 补全 GitHub Issue 模板入口配置

**摘要**：为 hefang_dw 的 Issue 模板新增统一入口配置并关闭空白 Issue

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/ISSUE_TEMPLATE/config.yml` | 新增 | 新增 Issue 新建入口配置与常用文档链接 |

**Copilot 接棒须知**：
- Issue 模板体系现已包含 3 个模板和统一入口配置，适合单人项目下的结构化记录
- 本轮未涉及 ETL 逻辑或业务口径，无需额外文档同步

**未完成项**：
- [ ] 当前模板体系已收口；如后续需要更强约束，可再升级为 GitHub Issue Forms


---

### [2026-03-24 13:18] · GitHub Copilot · 新增 GitHub Issue/PR 模板

**摘要**：为 hefang_dw 补充单人可追溯的 Issue 与 PR 模板

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/ISSUE_TEMPLATE/01_data_bug.md` | 新增 | 新增数据异常与 Bug 模板 |
| `.github/ISSUE_TEMPLATE/02_change_request.md` | 新增 | 新增 ETL/SQL/文档变更申请模板 |
| `.github/ISSUE_TEMPLATE/03_investigation_task.md` | 新增 | 新增待确认与调研任务模板 |
| `.github/pull_request_template.md` | 新增 | 新增适配 hefang_dw 的 PR 模板 |

**Copilot 接棒须知**：
- 模板已按单人项目场景设计，仍保留 Issue->PR->handoff 的可追溯链路
- 如后续启用 GitHub labels 或 Issue Forms，可在此基础上继续细化

**未完成项**：
- [ ] 如需进一步收口，可补 .github/ISSUE_TEMPLATE/config.yml 统一新建入口



---

### [2026-03-24 11:15] · GitHub Copilot · 清理用户级旧 DBHub MCP 配置

**摘要**：移除用户级 mcp.json 中已废弃的 io.github.bytebase/dbhub 及其专属 inputs，避免与工作区级 DBHub 配置混淆

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `d` | /tianhao/AppData/Roaming/Code/User/mcp.json | 修改:删除旧 io.github.bytebase/dbhub 与相关输入项 |

**Copilot 接棒须知**：
- 已按用户要求保留仓库根 .mcp.json，供未来 OpenCode / Claude 兼容使用。
- 当前 DBHub 与 Oracle 的实际主入口仍是工作区 .vscode/mcp.json；用户级 mcp.json 仅保留其他非数据库 MCP 配置。

**未完成项**：
- [ ] 建议重载 VS Code 窗口或新开聊天，使用户级 MCP 配置变更生效。




---

### [2026-03-24 11:12] · GitHub Copilot · 收口 .mcp.json 旧引用

**摘要**：不删除仓库根 .mcp.json，但将其统一标记为兼容/参考配置，并把 VS Code 会话主入口收口到 .vscode/mcp.json

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.claude/agents/data-query-agent.md` | 修改 | 将 .mcp.json 调整为兼容参考，不再当作当前会话主入口 |
| `.claude/agents/db-inspector.md` | 修改 | 补充 VS Code 会话优先检查 .vscode/mcp.json 与用户级 mcp.json |
| `.claude/skills/data-query/SKILL.md` | 修改 | 更新 MCP 不可用时的排查入口 |
| `docs/ARCHITECTURE.md` | 修改 | 将 .mcp.json 标记为兼容配置 |
| `docs/RUNBOOK.md` | 修改 | 明确 .vscode/mcp.json 是当前 VS Code/Copilot 主入口 |

**Copilot 接棒须知**：
- 仓库根 .mcp.json 对当前 Copilot 会话冗余，但对 Claude/OpenCode 仍有兼容价值，因此本轮未删除。
- 当前更值得清理的冗余项是用户级 mcp.json 里旧的 io.github.bytebase/dbhub 配置，但该文件在工作区外，本轮未自动修改。

**未完成项**：
- [ ] 如确认后续只保留 VS Code/Copilot 路线且不再使用 Claude/OpenCode，可再单独删除仓库根 .mcp.json，并同步清理 CLAUDE.md、CHANGELOG.md 等历史引用。





---

### [2026-03-24 11:06] · GitHub Copilot · 同步 MCP 可用性到各 agent

**摘要**：将当前 MySQL/Oracle MCP 实测可用性、配置入口与 Oracle 工具稳定性边界同步到 AGENTS 与经验台帐

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `AGENTS.md` | 修改 | 补充 2026-03-24 已验证的 MCP 现状与会话级入口说明 |
| `docs/AGENT_LESSONS.md` | 修改 | 记录 Oracle MCP 需接入工作区 .vscode/mcp.json 才会暴露为会话工具的经验 |

**Copilot 接棒须知**：
- 当前会话已验证：MySQL 走 DBHub 可执行结构查询与只读 SQL；Oracle 已可直接查询 BOSNDS3。
- Oracle 专用工具中 mcp_oracle_reqd_query 最稳定，mcp_oracle_list_tables 与 mcp_oracle_describe_table 仍应视为不稳定接口。
- 后续如 agent 看不到新 MCP 工具，优先检查 .vscode/mcp.json，并重载窗口后新开聊天。

**未完成项**：
- [ ] 如后续继续固化文档，可考虑把同样的 MCP 现状同步到 docs/RUNBOOK.md。






---

### [2026-03-24 10:41] · GitHub Copilot · 修复 Oracle MCP 挂载路径

**摘要**：将 Oracle MCP 从仓库根 .mcp.json 正式接入工作区 .vscode/mcp.json，并新增启动脚本读取本机 Oracle 环境变量

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.vscode/mcp.json` | 修改 | 新增 oracle MCP server 到工作区级 VS Code 配置 |
| `.vscode/start_oracle_mcp.ps1` | 新增 | 读取 ORACLE_CONNECTION_STRING 并启动 mcp-server-oracle |

**Copilot 接棒须知**：
- 根因是当前会话的 MCP 工具注册只看工作区 .vscode/mcp.json 和用户级 mcp.json，不会自动把仓库根 .mcp.json 暴露为 Copilot 工具。
- 已验证 Oracle 查询链路本身可用，BOSNDS3 表清单可通过仓库只读工具成功查出 2704 张表。
- 新的 Oracle MCP 启动脚本已消除 powershell.exe 的编码解析问题。

**未完成项**：
- [ ] 重载 VS Code 窗口并新开聊天，让 oracle server 在会话工具面重新注册。







---

### [2026-03-24 10:23] · GitHub Copilot · 校正归档中的 dim_channel 旧结论

**摘要**：为 AGENT_HANDOFF_archive 中早期 dim_channel 误判补充后续校正说明，避免历史归档干扰当前事实

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/AGENT_HANDOFF_archive.md` | 修改 | 为早期 dim_channel 记录补充 2026-03-23 后续校正说明 |
| `reports/docs_code_alignment.json` | 修改 | 刷新文档审计产物 |

**Copilot 接棒须知**：
- 本轮未改代码，也未改现行业务文档，只为归档旧结论补充后续校正，保留历史上下文同时避免检索误导。
- 当前现行事实仍以 docs/业务逻辑与指标规范.md、docs/DATA_CONTRACTS.md、docs/ETL业务逻辑说明.md 为准。

**未完成项**：
- [ ] 当前收口已完成；若继续，可再统一检查 archive 中其他历史业务口径是否需要类似后续校正标记








---

### [2026-03-24 10:16] · GitHub Copilot · 修正 DBHub MySQL host

**摘要**：将工作区 DBHub DSN 从 localhost 改为 127.0.0.1，并验证 MySQL 与 DBHub 均可成功连接

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.vscode/mcp.json` | 修改 | 将 DBHub MySQL DSN 的 host 从 localhost 改为 127.0.0.1 |

**Copilot 接棒须知**：
- dbhub_ro 账号实际创建在 127.0.0.1 上，使用 localhost 会命中不同的 MySQL 用户 host 规则。
- 已验证 mysql.exe 可登录 hefang_dw，DBHub 也可进入 MCP server running on stdio。

**未完成项**：
- [ ] 在 VS Code 中重载窗口或重开聊天，让更新后的工作区级 mcp.json 重新注册。









---

### [2026-03-24 10:12] · GitHub Copilot · 清理 DBHub 启动日志噪声

**摘要**：抑制 PowerShell 对 npx stderr 的 NativeCommandError 包装，保留真实 MySQL 1045 错误

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.vscode/start_dbhub.ps1` | 修改 | 关闭原生命令 stderr 到 PowerShell ErrorRecord 的包装 |

**Copilot 接棒须知**：
- 当前 DBHub 启动链路正常，剩余阻塞点仅为 MySQL 账号 dbhub_ro 在 localhost 上认证失败。
- 已用 mysql.exe 与 DBHub 两条链路复现同一 1045 错误，问题不在 MCP。

**未完成项**：
- [ ] 修正 dbhub_ro 密码，或在 MySQL 中重新创建并授权该账号后再重载 VS Code。











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
