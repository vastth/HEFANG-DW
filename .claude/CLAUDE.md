# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

# CLAUDE.md — 何方珠宝数据仓库（HEFANG-DW）Agent 协作规范

> 本文件是 Claude Code / AI Copilot 的**项目级硬约束**。
> 修改本文件需同步更新 docs/ARCHITECTURE.md，并在 CHANGELOG.md 记录变更。

---

## 1. 语言与格式约束

| 类型 | 语言 | 说明 |
|------|------|------|
| 文档（.md、注释、提交信息）| **简体中文** | 技术术语保留英文 |
| 代码、命令、路径、变量名 | **英文** | 不翻译、不替换 |
| SQL 关键字 | **大写** | 对齐 SQL 开发手册 |
| Python 字符串/日志 | 中英均可 | 错误消息保持与 Oracle/MySQL 原文一致 |

**输出格式**：
- 解释类回复：先结论，再原因，最后代码示例
- 代码变更：先给出变更摘要（受影响文件、行号），再给出 diff 或完整片段
- 多文件变更：必须列出完整的文件清单，注明「新增 / 修改 / 删除」

---

## 2. 项目结构速查

```
hefang_dw/                     ← 仓库根目录
├── config.py                  ← 配置中心（唯一真值源）
├── alerts.py                  ← 企业微信告警模块
├── run_etl.py                 ← 主调度入口（8步流水线）
├── run_ods.py                 ← ODS 专项调度
├── scheduled_etl.py           ← Windows 任务计划包装
├── run_scheduled_etl.bat      ← 任务计划触发脚本
├── etl_ods_*.py               ← ODS 层（fa_storage / m_retail / m_retailitem）
├── etl_dim_*.py               ← DIM 层（product / sku / store / channel）
├── etl_dws_*.py               ← DWS 层（sales / inventory）
├── etl_ads_health.py          ← ADS 层（库存健康度）
├── test_etl_automation.py     ← 验收测试
├── SQL/                       ← DDL & DML 脚本
├── tools/                     ← 连通测试、质检、快照工具
├── scripts/                   ← 运维脚本（check_doc_sync.py、doctor.ps1、log_agent_action.py、log_agent_lesson.py）
├── docs/                      ← 技术文档（见下方文档地图）
├── .env.example               ← 环境变量模板（不含真实凭据）
└── .claude/                   ← Agent 配置（本文件）
```

**文档地图**：

| 文件 | 用途 |
|------|------|
| docs/AGENT_HANDOFF.md | **⭐ Agent 交接日志（Claude Code↔Copilot 共享状态）** |
| docs/ARCHITECTURE.md | 项目地图、数据流、调度依赖 |
| docs/RUNBOOK.md | 环境准备、常用命令、常见报错 |
| docs/DATA_CONTRACTS.md | ODS/DIM/DWS/ADS 数据契约 |
| docs/数据仓库与ETL手册.md | ETL 架构与调度详解 |
| docs/数据结构与映射手册.md | 源表→目标表字段映射 |
| docs/业务逻辑与指标规范.md | 指标口径与业务规则 |
| docs/ETL业务逻辑说明.md | 各 ETL 模块逻辑说明 |
| docs/MYSQL数据字典.md | MySQL 数仓表结构字典 |
| docs/SQL开发手册.md | SQL 开发规范与模板 |
| docs/AGENT_LESSONS.md | Agent 经验台帐（踩坑记录，供后续 Agent 避坑）|
| docs/AGENT_HANDOFF_archive.md | 交接日志归档 |
| docs/TODO_ISSUES.md | 待办与已知问题追踪 |
| docs/达播数据运营上传指南.md | 达播业务数据上传操作指南 |
| docs/何方珠宝_会员体系梳理.md | 会员体系业务梳理 |

---

## 3. 引用要求（防臆造）

在生成任何文档、注释或代码时，必须遵守以下原则：

1. **引用脚本/路径前，必须确认文件在仓库中真实存在**。
2. **引用环境变量前，必须确认其在 `.env.example` 或 `config.py` 中有定义**。
3. **引用数据库表/字段前，必须对照 docs/MYSQL数据字典.md 或 docs/数据结构与映射手册.md**。
4. **引用业务指标前，必须对照 docs/业务逻辑与指标规范.md**。
5. **不得臆造不存在的函数、模块、命令或配置项**。

---

## 3.1 开发环境现实约束（硬约束）

1. 当前公司开发环境下，用户是唯一负责数据库的人；**不得默认假设存在内部 DBA、内部运维或其他数据库开发同事**。
2. Oracle 源库位于阿里云；MySQL 目标库与 `hefang_dw` 项目运行在公司服务器虚拟机，并由用户一手搭建。
3. 当需要真实 `shuyun_ods` 结构、样本或推送事实时，先向用户索取其可直接导出的材料；若当前环境不存在该对象，再建议向数云方等外部对接方索取。
4. 若用户已说明当前 MySQL 未落 CRM 表，则不得继续把本地 MySQL 当作 CRM 实证来源。

---

## 4. 改动流程（Change Workflow）

### 4.1 修改 ETL 逻辑前

```
1. 阅读目标模块（etl_*.py）与 config.py
2. 对照 docs/数据结构与映射手册.md 确认源表/目标表字段
3. 对照 docs/业务逻辑与指标规范.md 确认口径
4. 确认 test_etl_automation.py 中已有的验收断言
5. 修改代码
6. 同步更新受影响的 docs/ 文档
7. 在 CHANGELOG.md 新增条目
```

### 4.2 修改数据库结构前

```
1. 阅读 SQL/create_*.sql 确认现有结构
2. 编写 SQL/alter_<表名>_<描述>.sql（命名规范：小写 + 下划线）
3. 同步更新 docs/MYSQL数据字典.md 与 docs/数据结构与映射手册.md
4. 同步更新 DATA_CONTRACTS.md 中对应契约
5. 在 CHANGELOG.md 记录"Database / SQL"条目
```

### 4.3 完成变更后：写入 Agent 交接日志（⭐ 强制）

每次完成一组有意义的代码/文档变更后，**必须**向 `docs/AGENT_HANDOFF.md` 追加一条交接记录，供 Copilot 接棒时感知上下文。

```bash
# 推荐：使用辅助脚本（自动格式化 + 防止遗漏）
python scripts/log_agent_action.py \
  --agent "Claude Code" \
  --action "修复 ETL 口径" \
  --summary "一句话描述" \
  --files "etl_dws_sales.py:修改:return_amt 计算逻辑修正" \
  --notes "Copilot 审计时注意 test_etl_automation.py 断言是否需更新" \
  --todos "验证回填后近30天数据无异常"
```

**记录必须包含**：
- 变更了哪些文件（路径 + 变更类型 + 一句话说明）
- Copilot 接棒须知（哪些地方需要同步、有哪些风险点）
- 未完成项（TODO，供下一个 Agent 或人工续接）

**例外（无需写入）**：纯注释调整、格式化、临时调试代码。

### 4.4 禁止操作

- **禁止**将真实密钥、连接串、Webhook URL 写入任何被 git 追踪的文件
- **禁止**修改 `config.py` 中的 `MAIN_CATEGORY_IDS`、`PROPERTY_*` 常量（需业务确认）
- **禁止**删除 `test_etl_automation.py` 中的现有断言（只能新增）
- **禁止**在未阅读文件的情况下建议修改
- **禁止**完成一组变更后跳过写入 `docs/AGENT_HANDOFF.md`（Copilot 依赖此文件感知上下文）

---

## 5. 环境变量约定

所有凭据通过环境变量注入：

**`.env.example` 中定义（数据库连接）**：
```bash
# Oracle（伯俊 ERP，数据源）
ORACLE_USER, ORACLE_PASSWORD, ORACLE_HOST, ORACLE_PORT, ORACLE_SERVICE

# MySQL（何方数仓，数据目标）
MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, MYSQL_CHARSET
```

**`config.py` 中通过 `os.getenv()` 读取（告警与调度）**：
```bash
WECHAT_WEBHOOK      # 企业微信机器人 Webhook（格式见 config.py 注释）
ETL_MAX_RETRIES     # 重试次数，默认 3
ETL_RETRY_SLEEP     # 重试间隔秒数，默认 60
ETL_CONN_TEST       # 设为 1 时跳过真实连接（用于 CI 测试）
```

**设置方式（Windows PowerShell，User 级永久生效）**：
```powershell
[Environment]::SetEnvironmentVariable('ORACLE_USER', 'your_user', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_PASSWORD', 'your_pass', 'User')
```

---

## 6. 验收标准

每次改动必须满足：

| 检查项 | 方法 |
|--------|------|
| ETL 逻辑正确 | `python run_etl.py --conn-test`（ETL_CONN_TEST=1）无异常 |
| ODS 增量正确 | `python run_ods.py --mode incremental` 日志无 ERROR |
| 数据质量 | `python tools/check_data.py` 无红色警告 |
| 文档同步 | `python scripts/check_doc_sync.py` 无 MISSING 报告 |
| 环境自检 | `pwsh scripts/doctor.ps1` 全部 PASS |
| 连通性 | `python tools/test_connection.py` Oracle + MySQL 均 OK |
| **Agent 交接** | `docs/AGENT_HANDOFF.md` 已追加本次变更记录 |

---

## 7. 提交信息规范

```
<类型>: <简短描述>（中文）

<可选正文>

参考：CHANGELOG.md v<版本号>
```

类型：`ETL` / `SQL` / `Docs` / `Fix` / `Refactor` / `Test` / `Config`

示例：
```
ETL: dws_sales 新增双水位回填逻辑

- 支持 MODIFIEDDATE（线上）与 SETTIME（线下）双通道
- 回填窗口默认 7 天，可通过参数覆盖

参考：CHANGELOG.md v0.6.3
```

---

## 8. Agent 与 Skill 快速索引

### Subagents（`.claude/agents/`）

| Agent | 激活时机 | 工具范围 |
|-------|---------|---------|
| `etl-auditor` | 「审计ETL」「检查口径」「核实字段映射」 | 只读（Read/Grep/Glob）|
| `doc-syncer` | 「同步文档」「更新字典」「文档对齐」 | 读写（含 Write/Edit）|
| `db-inspector` | 「检查表结构」「快照对比」 | 只读 + MySQL MCP |
| `data-query-agent` | 「帮我查数据」「导出样本」「核对单据」 | 只读 MCP + Python 回退 |
| `data-reconciler` | 「对账」「比对行数」「漏数了吗」 | 只读 MySQL + Oracle MCP |

### Skills（斜杠命令）

| 命令 | 功能 | 典型使用时机 |
|------|------|------------|
| `/handoff [摘要]` | 写入 AGENT_HANDOFF.md 交接记录 | 完成一组变更后（强制）|
| `/quality-check` | 运行连通性+ETL空跑+数据质量+文档同步全套检查 | 提交前、例行巡检 |
| `/doc-sync` | 检查并修复文档与代码的同步差异 | 修改 ETL/SQL 后 |
| `/etl-audit [模块]` | ETL 完整审计，输出发现清单 | 上线前、口径评审 |
| `/schema-snap` | 数据库结构快照 + 字典漂移检测 | 每周例行、DDL 变更后 |
| `/data-query` | 路由数据查询（结构查询/固定对账/自由查数）| 需要查看数仓数据时 |
| `/backfill` | 双水位历史回填工作流 | 补数、重跑历史数据 |

### Hooks（自动触发）

**PreToolUse（编辑前拦截）**：
- 修改 `config.py` 业务常量（`MAIN_CATEGORY_IDS`、`PROPERTY_*`）时发出警告
- 修改 `test_etl_automation.py` 断言时发出警告

**PostToolUse（编辑后提醒）**：
- 修改 `etl_*.py` 或 `SQL/*.sql` 后，提醒运行 `/doc-sync` 和 `/handoff`
- 修改任意文件后，触发经验复盘（自动检查是否需写入 `docs/AGENT_LESSONS.md`）

### MCP（数据库直连）
配置文件：`.mcp.json`（不提交，env var 引用）
- `mysql`：MySQL 数仓，只读，通过 `@benborla29/mcp-server-mysql`
- `oracle`：Oracle ERP，只读，通过 `mcp-server-oracle`（需 `uv` 和 `ORACLE_CONNECTION_STRING` 环境变量）

验证 MCP 状态：在 Claude Code 中输入 `/mcp`
