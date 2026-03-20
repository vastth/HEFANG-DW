# 更新日志（CHANGELOG）

> 说明：按日期与版本整理，条目按“Added / Changed / Fixed / Database / Docs”分类。


## 2026-03-18

### v0.7.8 — 新增 HFSY 数据字典（2026-03-20）

#### Docs
- 新增 `docs/HFSY数据字典.md`，基于 `reports/snapshot_mysql_hfsy_schema.json` 记录数云 `hfsy` 实库的表、字段、注释、键与当前行数。
- `docs/misc/数云CRM数据接入实施计划.md` 补充 `hfsy` 快照与数据字典产物，明确后续字段映射和实施设计应直接引用这两份审计产物。

### v0.7.7 — 数云 CRM 实表证据校正（2026-03-20）

#### Docs
- `docs/misc/数云CRM数据接入实施计划.md` 纳入数云 xlsx 与 `hfsy` 实表证据，确认当前真实源表为 `t_member_info`、`t_member_bind_info`、`t_trade`、`t_order`、`t_pin_xid_rel`、`sys_area`。
- 将数云侧 MySQL 版本前提从“建议 8.0+”纠偏为“当前实表运行在 5.7.42，实施必须保持 5.7 兼容”。

### v0.7.6 — 单人数据库环境约束（2026-03-19）

#### Changed
- `.github/copilot-instructions.md`、`AGENTS.md`、`.claude/CLAUDE.md` 增加当前开发环境的现实约束：用户为唯一数据库负责人，禁止默认假设存在内部 DBA / 运维协同。

#### Docs
- `docs/ARCHITECTURE.md` 补充 Oracle 位于阿里云、MySQL 与 `hefang_dw` 运行在公司服务器虚拟机的部署边界，以及 CRM 实证材料索取路径。

### v0.7.5 — 经验台帐与复盘机制（2026-03-18）

#### Added
- 新增 `scripts/log_agent_lesson.py`，用于将排障结论、业务纠错与字段语义修正写入经验台帐。
- 新增 `docs/AGENT_LESSONS.md`，作为共享的 Agent 经验台帐。
- 新增 `.opencode/commands/lesson.md`，为 OpenCode 提供手动经验落盘入口。

#### Changed
- `.claude/settings.json` 增加经验复盘提示型 Hook，要求在形成可复用经验后落盘台帐。
- `.github/copilot-instructions.md`、`AGENTS.md`、`README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md` 同步经验台帐机制、命令入口与限制说明。

### v0.7.4 — 只读查数工具与 tools 直跑修复（2026-03-18）

#### Added
- 新增 `tools/query_data.py`，支持 MySQL / Oracle 只读查询、模板查数、参数注入与导出。
- 新增 `.claude/skills/data-query/SKILL.md`，统一“结构查询 / 固定对账 / 自由查数”路由规则。
- 新增 `.claude/agents/data-query-agent.md`，补充 MCP 优先、Python 工具兜底的数据查询工作流。

#### Changed
- `tools/export_ads.py` 增加 `argparse`，支持 `--snapshot-date` 与 `--output`，仍保持 `ads_inventory_health` 只读导出。
- `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md` 补充只读查数、结构快照与 MCP 降级说明。

#### Fixed
- `tools/snapshot_mysql_hefangdw_schema.py`、`tools/snapshot_oracle_bosnds3_schema.py`、`tools/test_connection.py`、`tools/export_ads.py` 统一改为基于 `REPO_ROOT` 解析 `config.py`、`docs/` 与 `reports/` 路径，支持任意工作目录直接运行。

### v0.7.3 — dim_channel 店仓字段更名（2026-03-18）

#### Changed
- `etl_dim_channel.py` 将 `dim_channel` 目标字段更名为 `WING_CODE`，并直接映射 Oracle `O2O_RETAIL_CHANNEL.WING_CODE`。
- `test_etl_automation.py` 将 `dim_channel` 自动化校验改为核对 `WING_CODE='DS001'`。
- `SQL/create_dim_channel.sql` 修正目标字段名为 `WING_CODE`，并新增现网迁移脚本 `SQL/alter_dim_channel_rename_store_code_to_wing_code.sql`。

#### Docs
- 更新 `README.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`，统一 `dim_channel` 目标字段名为 `WING_CODE`。

### v0.7.2 — dim_channel 血缘补齐（2026-03-18）

#### Added
- 新增 `etl_dim_channel.py`，将 Oracle `O2O_RETAIL_CHANNEL` 全量同步到 MySQL `dim_channel`。
- 新增 `SQL/create_dim_channel.sql`，补齐 `dim_channel` 建表脚本。

#### Changed
- `run_etl.py` 主流水线由 7 步扩展为 8 步，新增 `dim_channel` 同步步骤。
- `config.py` 新增 `dim_channel` 任务显示名。
- `test_etl_automation.py` 新增 `dim_channel` 自动化校验。

#### Docs
- 更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/TODO_ISSUES.md`，关闭 P1-001。


## 2026-03-04

### v0.7.0 — everything-claude-code 四层架构扩展（2026-03-04）

#### Added
- 新增 3 个 ETL/数仓专属 Subagent（`.claude/agents/`）：
  - `etl-auditor`：ETL代码审计专家（只读，sonnet）
  - `doc-syncer`：文档同步执行者（读写，haiku）
  - `db-inspector`：数据库结构探查专家（只读 + MySQL MCP，haiku）
- 新增 5 个项目专属 Skill（`.claude/skills/`）：
  - `/handoff`：一键写入 AGENT_HANDOFF.md 交接记录
  - `/quality-check`：全套质检（连通性+ETL空跑+数据质量+文档同步）
  - `/doc-sync`：文档同步检查与自动修复
  - `/etl-audit`：ETL 完整审计，输出带优先级的发现清单
  - `/schema-snap`：数据库结构快照 + 字典漂移检测
- 新增本地 MCP 配置约定（`.mcp.json`，已忽略不提交）：MySQL + Oracle 双向 MCP（只读，env var 引用）
  - MySQL：`@benborla29/mcp-server-mysql`，直连何方数仓
  - Oracle：`mcp-server-oracle`，直连伯俊 ERP
- 新增 PostToolUse Hook：修改 `etl_*.py` 或 `SQL/*.sql` 后自动提醒同步文档

#### Changed
- `.claude/settings.json`：新增 `hooks.PostToolUse` 节点
- `.claude/CLAUDE.md`：新增第 8 章「Agent 与 Skill 快速索引」
- `.gitignore`：追加 `.mcp.json`（含 env var 引用，不提交）

#### Docs
- `CLAUDE.md` 第 8 章新增完整的 Subagents/Skills/Hooks/MCP 索引表

参考：架构模式来源 [affaan-m/everything-claude-code](https://github.com/affaan-m/everything-claude-code)


## 2026-03-03

### v0.6.4 — 文档审计与交接工具补齐（2026-03-03）

#### Added
- 新增文档审计脚本与术语过滤规则。来源：[scripts/check_doc_sync.py](scripts/check_doc_sync.py#L1-L80)
- 新增环境自检脚本（Doctor）。来源：[scripts/doctor.ps1](scripts/doctor.ps1#L1-L72)
- 新增交接日志写入脚本。来源：[scripts/log_agent_action.py](scripts/log_agent_action.py#L1-L76)
- 新增 MySQL/Oracle 结构快照导出脚本。来源：[tools/snapshot_mysql_hefangdw_schema.py](tools/snapshot_mysql_hefangdw_schema.py#L1-L86)、[tools/snapshot_oracle_bosnds3_schema.py](tools/snapshot_oracle_bosnds3_schema.py#L1-L78)

#### Changed
- dws_sales 抽取与清洗补充门店编码、云仓标识与别名字段处理。来源：[etl_dws_sales.py](etl_dws_sales.py#L28-L100)

#### Docs
- 新增 Agent 交接日志与待办追踪文档。来源：[docs/AGENT_HANDOFF.md](docs/AGENT_HANDOFF.md#L1-L44)、[docs/TODO_ISSUES.md](docs/TODO_ISSUES.md#L1-L45)


## 2026-02-26

### v0.6.3 — ODS双水位与质检链路（2026-02-26）

#### Added
- 新增 ODS 抽取链路：`etl_ods_fa_storage.py`、`etl_ods_m_retail.py`、`etl_ods_m_retailitem.py`、`run_ods.py`。
- 新增 ODS 质量校验工具：`tools/check_ods_incremental.py`、`tools/check_ods_retailitem_quality.py`。
- 明细双通道增量：`MODIFIEDDATE`（线上）与 `SETTIME`（线下）双水位对账与拆分校验。

#### Changed
- `run_ods.py` 集成质量校验并输出日志 `logs/ods_qc_*.log`。
- 明细增量逻辑改为双水位并记录 `ods_m_retailitem_settime` 水位。

#### Fixed
- 对账工具 `--as-of` 截止时间过滤兼容 `M_RETAIL` 查询别名。
- 全量完成后同步写入 `settime` 水位，避免后续增量再次全量回刷。

#### Database / SQL
- 新增 ODS 建表与增量迁移脚本：`SQL/create_ods_tables.sql`、`SQL/alter_ods_incremental.sql`。
- `ods_m_retailitem` 增加 `settime` 字段及索引。

#### Docs
- 更新 `README.md`：ODS 双水位说明、质量校验入口与日志说明。
- 更新 `docs/数据仓库与ETL手册.md`、`docs/数据结构与映射手册.md`、`docs/mysql_data_dictionary.md` 同步 ODS 口径与字段。


## 2026-02-24

### v0.6.2 — ETL摘要通知与调度入口统一（2026-02-24）

#### Changed
- `run_etl.py`：将 7 步 ETL 输出统一为结构化步骤报告（状态/详情/耗时），并在成功或失败场景都发送企业微信摘要。
- `run_etl.py`：统一摘要模板包含执行时间、总耗时、成功/警告/失败计数、步骤明细；失败时附加重试信息与失败原因。
- `run_etl.py`：重试等待参数改为读取 `ETL_RETRY_SLEEP`（默认回落到 `ETL_DEFAULT_RETRY_SLEEP`）。
- `scheduled_etl.py`：改为调用 `run_etl.py` 统一入口，避免多入口行为漂移；仅在 ETL 成功后继续执行 `test_etl_automation.py`。
- `run_scheduled_etl.bat`：同步说明当前链路为统一入口（含重试与摘要发送）。

#### Docs
- 更新 `README.md`：补充“成功/失败都发送企业微信统一摘要”策略与调度入口说明。
- 更新 `docs/数据仓库与ETL手册.md`：同步调度方式、异常处理与日常检查项。

#### Verified
- 本地连接测试模式（`ETL_CONN_TEST=1`、`ETL_MAX_RETRIES=1`）验证通过，企业微信成功收到摘要消息。


## 2026-02-06

### v0.6.1 — 告警与重试逻辑重构（2026-02-06）

#### Changed
- 将企业微信告警发送逻辑抽离为独立模块 `alerts.py`，便于替换或扩展告警渠道（例如支持邮件/钉钉等）。
- 将任务友好名称映射 `TASK_DISPLAY_NAME` 移至配置 `config.py`，便于运维调整与国际化。
- 在 `config.py` 中新增重试相关配置：`ETL_NON_RETRYABLE_ERROR_KEYWORDS`、`ETL_RETRYABLE_ERROR_KEYWORDS`、`ETL_MAX_RETRIES`（可通过环境变量覆盖）、`ETL_RETRY_SLEEP`。
- 改进 `run_etl.py` 的错误摘要提取逻辑（`_extract_error_summary`）：过滤 Help/URL 行并优先返回 ORA- 错误行，使告警内容更具可操作性。
- 新增判断逻辑 `_should_retry_based_on_details`：遇到确定性不可重试错误（例如认证/权限失败）会立即告警并放弃重试，避免无意义重复尝试。

#### Docs
- 更新 `README.md`：新增告警与测试相关环境变量说明（`WECHAT_WEBHOOK`、`ETL_CONN_TEST`、`ETL_MAX_RETRIES`、`ETL_RETRY_SLEEP`）以及 `--conn-test` 测试说明。

#### Verified
- 在本地以 `--conn-test`（故意使用错误凭据）运行验证：脚本在检测到认证失败后发出立即告警，且企业微信 webhook 返回成功。

### v0.6.0 — 达播纳入 ETL 可观测链路（2026-02-04）

#### Added
- 将外部达播（Dabo）CSV 纳入 ETL 可观测链路：新增 `dabo_ready` 就绪检查步骤，满足条件后触发回填
- ADS 宽表新增达播相关字段与“自然销量 / 自然销售额”字段：
  - `dabo_latest_date`
  - 达播 7 / 30 天销量与销售额
  - 自然销量 / 自然销售额（剔除达播影响）

#### Changed
- `etl_ads_health.py`：新增达播字段、回填逻辑，并补充自然口径计算
- `run_etl.py`：加入 `dabo_ready` 步骤与回填条件控制

#### Fixed
- 修复 ETL 中字段引用：`p.m_dim4_id` → `p.category_id`
- 解决 MySQL JOIN 字符集 / 排序规则冲突：关联字段显式使用 `COLLATE utf8mb4_unicode_ci`

#### Database / SQL
- 新增达播相关建表脚本：`ads_dabo_daily_sales`、`log_dabo_import`
- 为避免 MySQL `ADD COLUMN IF NOT EXISTS` 兼容问题，`ads_inventory_health` 改为分步 ALTER：
  - `alter_ads_inventory_health_add_dabo_latest_date.sql`
  - `alter_ads_inventory_health_add_dabo_revenue_fields.sql`
  - `alter_ads_inventory_health_add_dabo_natural_fields.sql`

#### Docs
- 同步更新达播 ETL、字段定义、回填与口径说明：
  - `docs/达播数据运营上传指南.md`
  - `docs/数据仓库与ETL手册.md`
  - `docs/数据结构与映射手册.md`
  - `docs/业务逻辑与指标规范.md`
  - `docs/SQL开发手册.md`
  - `docs/mysql_data_dictionary.md`
  - `README.md` 增加 ETL 步骤说明（含 `dabo_ready`）与 CHANGELOG 链接

