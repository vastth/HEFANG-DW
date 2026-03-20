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

### [2026-03-20 10:35] · GitHub Copilot · 新增 superpowers 内化会议纪要

**摘要**：将 GitHub Copilot 能力内化讨论沉淀为持续更新的会议纪要文档，确认采用三阶段推进方案

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/superpowers内化会议纪要.md` | 新增 | 记录 superpowers 内化目标、三阶段方案、能力映射与后续更新规则 |
| `docs/AGENT_HANDOFF.md` | 修改 | 追加本轮会议纪要建档交接记录 |

**Copilot 接棒须知**：
- 后续凡涉及 Copilot 自定义能力、superpowers 方法论迁移、skills / agents / hooks 分层设计的讨论，优先更新 `docs/misc/superpowers内化会议纪要.md`。
- 当前仍处于方案讨论阶段，尚未创建 `.github/instructions/`、`.github/prompts/`、`.github/agents/` 或 `.github/skills/` 的新能力文件。

**未完成项**：
- [ ] 细化第一阶段 5 个能力的详细规格（名称、触发语、输入、输出、边界、是否调用脚本）
- [ ] 设计 `.github` 下未来 Copilot 自定义能力的目录分层

---

### [2026-03-20 10:51] · GitHub Copilot · 补充 hfsy 数据字典与实表审计产物

**摘要**：新增 HFSY 数据字典与 hfsy 结构快照，并把它们纳入数云 CRM 实施计划的主证据链。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `reports/snapshot_mysql_hfsy_schema.json` | 新增 | hfsy 实库结构快照，记录表、字段、键和行数 |
| `docs/HFSY数据字典.md` | 新增 | 基于 hfsy 实库快照生成源侧表字段数据字典 |
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 补充 hfsy 快照与 HFSY 数据字典为第 2 轮实表校正证据 |
| `CHANGELOG.md` | 修改 | 记录 v0.7.8 新增 HFSY 数据字典 |
| `.github/copilot-instructions.md` | 修改 | 将 docs/HFSY数据字典.md 纳入文档同步检查清单 |

**Copilot 接棒须知**：
- 后续 CRM 设计应优先引用 reports/snapshot_mysql_hfsy_schema.json 与 docs/HFSY数据字典.md；当前仍需补充 t_member_bind_info 的 *1 列覆盖率统计，以及确认 t_order_copy / t_order_copy1 是否仅为备份表。

**未完成项**：
- [ ] 继续做 hfsy 行级抽样与字段覆盖率探查
- [ ] 确认 t_order_copy 与 t_order_copy1 的正式链路角色
- [ ] 若继续实现 CRM ETL，按 hfsy.t_member_info / t_member_bind_info / t_pin_xid_rel 作为第一阶段输入

---

### [2026-03-20 09:50] · GitHub Copilot · 校正数云CRM实表依据

**摘要**：纳入 hfsy 实表与 xlsx 证据，修正 CRM 实施计划对标准方案和 MySQL 8.0 的过度假设

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 纳入 hfsy 实表与 xlsx 证据，切换到第 2 轮实表校正 |
| `CHANGELOG.md` | 修改 | 记录 v0.7.7 数云 CRM 实表证据校正 |
| `docs/AGENT_LESSONS.md` | 修改 | 记录标准方案不能替代真实实表的经验 |

**Copilot 接棒须知**：
- 后续 CRM 开发起点应从 `hfsy.t_member_info`、`hfsy.t_member_bind_info`、`t_pin_xid_rel` 出发，不再以 `fdi_*` JSON 表作为当前唯一事实源。
- 下一步优先做样例行级探查与 modified 字段质量检查，确认 *1 解密列覆盖率和 order_copy 表是否为备份。

**未完成项**：
- [ ] 对 hfsy 核心表抽样 5~10 行，验证 modified 时间串格式、platCode 分布和 *1 字段覆盖率
- [ ] 确认 t_order_copy 与 t_order_copy1 是否只是备份表，正式链路是否只消费 t_order


---

### [2026-03-19 18:11] · GitHub Copilot · 补充环境现实约束并生成数云方索取模板

**摘要**：将单人负责数据库的环境边界写入项目硬约束，并为数云方准备可直接发送的资料索取模板

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/copilot-instructions.md` | 修改 | 增加单人负责数据库与外部取证优先级硬约束 |
| `AGENTS.md` | 修改 | 增加环境现实约束与CRM取证路径 |
| `.claude/CLAUDE.md` | 修改 | 为Claude侧补充单人数据库环境硬约束 |
| `docs/ARCHITECTURE.md` | 修改 | 补充Oracle/VM部署边界与CRM实证来源限制 |
| `CHANGELOG.md` | 修改 | 记录v0.7.6环境约束更新 |

**Copilot 接棒须知**：
- 后续涉及CRM实证时，不再默认存在内部DBA或同事；优先向用户索取本地可导出材料，若环境无对象再转向数云方。

**未完成项**：
- [ ] 如进入CRM第2轮审计，先向数云方索取真实建表SQL、关键表样本与xid/商品类目表确认。



---

### [2026-03-19 18:35] · GitHub Copilot · 完成CRM第1轮字段级仲裁

**摘要**：完成 12 张数云 ODS 表的字段级仲裁矩阵，区分已可设计、待实表验证与标准方案文档自身缺口三类对象

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 新增第 1 轮 12 表字段级仲裁矩阵、发现清单与待确认项 |
| `docs/AGENT_LESSONS.md` | 修改 | 记录外部接入表不应一律视为可直接设计的经验 |

**Copilot 接棒须知**：
- 第一阶段真正可直接进入实现设计的核心对象仍是 `fdi_member_info` 与 `fdi_jos_pin_xid`，订单链路属于第二阶段扩展。
- 若继续第 2 轮，应优先索取真实 `shuyun_ods` 建表 SQL、`SHOW CREATE TABLE` 或脱敏样本，验证 `fdi_refund`、`fdi_rate`、`fdi_member_point_his`、`fdi_member_grade_his` 和商品类目表。

**未完成项**：
- [ ] 进入第 2 轮时，用真实 `shuyun_ods` 实表或样本验证 5 类残留问题：`member_id` 映射、`refund` 账号字段、`xid` 真实形态、包裹密文覆盖范围、商品类目表真实表名。




---

### [2026-03-19 18:18] · GitHub Copilot · 修正CRM计划版本漂移

**摘要**：在继续细审前修正实施计划文首版本号与版本记录不一致的问题

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 将文首当前版本从 v2.1 修正为 v2.2，与版本记录一致 |

**Copilot 接棒须知**：
- 当前实施计划正文与版本表已按 v2.2 审计结果对齐。
- 后续如继续细审，应重点处理“真实 ODS 实表/样本是否与仲裁文档一致”这一层，而不是再做文案级修词。

**未完成项**：
- [ ] 若需宣称与仲裁文档 100% 对齐，下一步必须引入真实 `shuyun_ods` 实表或样本数据做字段级核验。




---

### [2026-03-19 18:10] · GitHub Copilot · 再审计数云CRM实施计划

**摘要**：依据三个仲裁文档、当前代码库与数据库快照，再次修正数云CRM实施计划中的过期事实、无效证据链与配置过度设计问题

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 修正 `.env.example` 现状、移除不存在的 R10 证据、增加仲裁优先级与固定协议约束 |
| `docs/AGENT_LESSONS.md` | 修改 | 记录外部接入方案审计时的证据优先级与配置设计经验 |

**Copilot 接棒须知**：
- 当前 CRM 仍未落地任何代码或表结构，实施计划仍属于“待实施”文档，不应被当成已实现现状。
- 后续若进入实现阶段，`xid` 是否解密、`.env.example` 扩展方式和 AES 协议固定性均应按本轮再审计后的 v2.2 执行。

**未完成项**：
- [ ] 如进入实施阶段，先按 v2.2 计划扩展 `.env.example` 与 `config.py`，不要新增第二份环境模板，也不要把固定加密协议做成运行时开关。




---

### [2026-03-19 17:31] · GitHub Copilot · 补充数云CRM计划交叉审计结论

**摘要**：将敏感数据加密规则与数云沟通确认单的仲裁结论落入实施计划，并补充加密兼容、同步频率与京东pin→xid约束

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 补充交叉审计结论与仲裁材料约束 |
| `docs/AGENT_LESSONS.md` | 修改 | 记录数云CRM字段语义与加密兼容经验 |

**Copilot 接棒须知**：
- 本轮仅更新文档与经验台帐，未变更CRM代码实现。
- 实施计划已明确每小时同步、MySQL 8.0+、包裹格式未决与京东业务表plat_account=pinid。

**未完成项**：
- [ ] 如继续实施，先按文档中的 v2.1 约束落地 crypto/account_match/member ETL。




---

### [2026-03-19 17:23] · GitHub Copilot · 校正数云CRM实施计划

**摘要**：将数云CRM实施计划改写为与当前代码库一致的校正版，修正主键、目录、水位与调度边界

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/misc/数云CRM数据接入实施计划.md` | 修改 | 按当前仓库结构重写实施计划并补充校正依据与版本记录 |

**Copilot 接棒须知**：
- 本轮仅修改实施计划文档，未创建任何CRM代码或DDL文件。
- 计划已明确 dwd_member 主键改为稳定原值键，后续落地应避免使用 account_match_key 作为主键。

**未完成项**：
- [ ] 如进入实施阶段，先按计划落地 config.py、create_dwd_crm_tables.sql、utils/crypto.py、utils/account_match.py、etl_dwd_member.py、run_crm_etl.py。





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
