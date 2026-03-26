# ARCHITECTURE.md — 何方珠宝数据仓库项目地图

> 本文档是仓库架构的**唯一权威来源**。修改数据层结构、调度顺序或关键配置后，必须同步更新本文件。
>
> 最后更新：2026-03-19（v0.7.6 对齐）

---

## 1. 系统全景

```
┌─────────────────────────────────────────────────────────────────┐
│                        数据源（Source）                          │
│  Oracle 19c — 伯俊 ERP (BOSNDS3)                                │
│  表：FA_STORAGE / M_RETAIL / M_RETAILITEM / M_PRODUCT /         │
│       M_PRODUCTALIAS / C_STORE / M_PURCHASEITEM / ...           │
└────────────────────────┬────────────────────────────────────────┘
                         │ python-oracledb (thin mode)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ODS 层（Operational Data Store）              │
│  MySQL: ods_fa_storage / ods_m_retail / ods_m_retailitem        │
│  策略：增量（双水位）或全量覆盖                                  │
│  水位字段：MODIFIEDDATE（线上）/ SETTIME（线下）                  │
└────────────────────────┬────────────────────────────────────────┘
                         │ Pandas + SQLAlchemy
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    DIM 层（维度层，每日全刷）                      │
│  MySQL: dim_product / dim_sku / dim_store / dim_channel          │
│  来源：直接读 Oracle，每日全量覆盖                                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    DWS 层（明细汇总层）                           │
│  MySQL: dws_sales_daily / dws_inventory_daily                    │
│  销售：增量（按日期窗口从 ODS 聚合）                              │
│  库存：每日快照（全量覆盖当日数据）                               │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    ADS 层（应用层）                               │
│  MySQL: ads_inventory_health（库存健康度，SKU 粒度）              │
│         ads_dabo_daily_sales（达播数据，外部 CSV 导入）           │
│  每日全量重算，含 SABC 分级与健康度评分                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. 目录结构

```
hefang_dw/
│
├── 【调度入口】
│   ├── run_etl.py              主调度：9步流水线（dim→ods→dws→dabo→ads）
│   ├── run_ods.py              ODS 专项调度（增量/全量 + 自动质检）
│   ├── scheduled_etl.py        任务计划包装（调 run_etl.py → test_etl_automation.py）
│   └── run_scheduled_etl.bat   Windows 任务计划触发脚本
│
├── 【ETL 模块】
│   ├── etl_ods_fa_storage.py   ODS: 库存主档（FA_STORAGE）
│   ├── etl_ods_m_retail.py     ODS: 零售单据头（M_RETAIL，增量）
│   ├── etl_ods_m_retailitem.py ODS: 零售单据明细（M_RETAILITEM，双水位）
│   ├── etl_dim_product.py      DIM: 商品维度（全刷）
│   ├── etl_dim_sku.py          DIM: SKU 维度（全刷）
│   ├── etl_dim_store.py        DIM: 店仓维度（全刷）
│   ├── etl_dim_channel.py      DIM: 渠道维度（全刷）
│   ├── etl_dws_sales.py        DWS: 销售日报（增量）
│   ├── etl_dws_inventory.py    DWS: 库存快照（每日）
│   └── etl_ads_health.py       ADS: 库存健康度（全量重算）
│
├── 【核心配置】
│   ├── config.py               唯一配置中心（连接参数 + 业务常量）
│   └── alerts.py               企业微信告警模块
│
├── 【测试与质检】
│   ├── test_etl_automation.py  自动化验收测试
│   ├── tools/test_connection.py        连通性测试
│   ├── tools/check_data.py             通用数据质检
│   ├── tools/check_dws_inventory.py    库存质检
│   ├── tools/check_ods_incremental.py  ODS 增量对账
│   └── tools/check_ods_retailitem_quality.py  ODS 明细质检
│
├── 【数据库脚本】
│   └── SQL/
│       ├── create_ods_tables.sql       ODS 建表
│       ├── alter_ods_incremental.sql   双水位字段迁移
│       ├── alter_*.sql                 其他结构变更（13个）
│       ├── 库存健康度_SKU粒度_v5.0.sql  健康度计算 SQL（参考口径）
│       ├── ==日报数据SQL.sql            日报模板
│       └── ==线上销售月报SQL 2.0.sql    月报模板
│
├── 【工具】
│   ├── tools/export_ads.py                   导出 ads_inventory_health 快照
│   ├── tools/query_data.py                   通用只读查数与导出工具
│   ├── tools/snapshot_mysql_hefangdw_schema.py  MySQL 结构快照
│   ├── tools/snapshot_oracle_bosnds3_schema.py  Oracle 结构快照
│   ├── scripts/check_doc_sync.py              文档代码同步审计
│   └── scripts/log_agent_lesson.py            Agent 经验台帐写入
│
├── 【文档】
│   └── docs/（含 AGENT_HANDOFF / AGENT_LESSONS 等协作文档）
│
├── 【配置与模板】
│   ├── .env.example            环境变量模板
│   ├── .claude/settings.json   Agent 默认设置（可提交）
│   └── .claude/CLAUDE.md       Agent 协作规范（本项目）
│   ├── .claude/agents/          Agent 子代理定义（ETL/文档/结构）
│   ├── .claude/agents/data-query-agent.md  数据查询与对账专家
│   ├── .claude/skills/          Skills 定义（/handoff 等）
│   ├── .claude/skills/data-query/SKILL.md  data-query 查询路由工作流
│   └── .mcp.json                本地 MCP 兼容配置（主要供 Claude/OpenCode 参考，不作为 VS Code 会话主入口）
│
└── 【数据与输出】
    ├── data/                   测试参考数据（不提交）
    ├── logs/                   ETL 运行日志（不提交）
    ├── reports/                导出报表（不提交）
    └── notebooks/              Jupyter 探索（不提交规则变更）
```

---

## 3. ETL 执行流水线

### 3.1 主流水线（run_etl.py）

执行顺序固定（`STEP_ORDER`，见 `run_etl.py:43`）：

```
步骤  模块                   说明                        失败策略
─────────────────────────────────────────────────────────────────
1    etl_dim_product        商品维度全刷                 重试3次→告警继续
2    etl_dim_sku            SKU 维度全刷                 重试3次→告警继续
3    etl_dim_store          店仓维度全刷                 重试3次→告警继续
4    etl_dim_channel        渠道维度全刷                 重试3次→告警继续
5    ods_sync               ODS 增量同步 + 自动质检       重试3次→告警继续
6    etl_dws_sales          销售增量（已消费ODS）        重试3次→告警停止
7    etl_dws_inventory      库存快照（已消费ODS）        重试3次→告警停止
8    dabo_ready             达播 CSV 就绪检查            检查通过→触发回填
9    etl_ads_health         库存健康度全量重算           重试3次→告警继续
─────────────────────────────────────────────────────────────────
```

触发方式：
```bash
# 手动触发（每日正常）
python run_etl.py

# 连通性测试（不执行真实 ETL）
python run_etl.py --conn-test

# 任务计划（通过 scheduled_etl.py 包装）
python scheduled_etl.py
```

### 3.2 ODS 专项流水线（run_ods.py）

```bash
python run_ods.py                # 增量（默认，使用双水位）
python run_ods.py --full         # 全量覆盖
python run_ods.py --skip-qc      # 跳过自动质检
```

如仅执行质检，请直接运行 `tools/check_ods_incremental.py` 与 `tools/check_ods_retailitem_quality.py`；
ODS 质检日志输出到 `logs/ods_qc_<日期时间>.log`。

### 3.3 查询与审计执行面

- 结构探查：优先使用 MySQL / Oracle MCP 或 `db-inspector`，仅查看表、字段、索引与注释。
- 固定对账：优先使用 `tools/check_ods_incremental.py` 与 `tools/check_ods_retailitem_quality.py`，避免重复实现既有口径。
- 自由查数：通过 `tools/query_data.py` 统一承接 MySQL / Oracle 只读查询，并支持导出 `table`、`json`、`csv`、`excel`。
- ADS 固定导出：通过 `tools/export_ads.py` 导出 `ads_inventory_health`；结构快照由 `tools/snapshot_mysql_hefangdw_schema.py` 与 `tools/snapshot_oracle_bosnds3_schema.py` 生成。
- 经验复盘：通用经验沉淀到 `docs/AGENT_LESSONS.md`，由 `scripts/log_agent_lesson.py` 负责结构化写入；用户明确纠错的业务结论也必须进入该台帐。
- Hook 边界：当前仓库内已确认可用的是 `.claude/settings.json` 的 `PostToolUse` 提示型 Hook；GitHub Copilot 当前未暴露可在仓库本地强制执行的“会话结束自动写台帐”钩子，因此需要保留收尾自检与命令兜底。

---

## 4. 数据库连接

### 4.0 环境边界

- 当前公司开发环境由用户单人维护数据库与数仓工程，不存在可默认协同的内部 DBA / 运维角色。
- Oracle 源库运行在阿里云；MySQL 目标库与 `hefang_dw` 项目运行在公司服务器虚拟机。
- 因此，涉及真实 CRM 落库结构时，不应默认从当前 `hefang_dw` MySQL 中取得 `shuyun_ods` 实证；若本地未落表，需改为索取外部对接材料或未来联调环境证据。

### 4.1 数据源（Oracle）

| 参数 | 环境变量 | 默认值（`.env.example`）|
|------|----------|------------------------|
| 主机 | `ORACLE_HOST` | localhost |
| 端口 | `ORACLE_PORT` | 1521 |
| 服务名 | `ORACLE_SERVICE` | orcl |
| 用户名 | `ORACLE_USER` | change_me |
| 密码 | `ORACLE_PASSWORD` | change_me |

驱动：`python-oracledb`（thin 模式，**无需安装 Oracle Instant Client**）

### 4.2 数据目标（MySQL）

| 参数 | 环境变量 | 默认值 |
|------|----------|--------|
| 主机 | `MYSQL_HOST` | localhost |
| 端口 | `MYSQL_PORT` | 3306 |
| 数据库 | `MYSQL_DB` | hefang_dw |
| 用户名 | `MYSQL_USER` | change_me |
| 密码 | `MYSQL_PASSWORD` | change_me |

驱动：`SQLAlchemy + PyMySQL`

---

## 5. 关键业务常量（config.py）

> 以下常量由业务确认，**修改前必须获得业务确认**：

| 常量 | 值（ID列表）| 含义 |
|------|------------|------|
| `MAIN_CATEGORY_IDS` | (134,142,139,138,141,143,133,136,140,137,144,145) | 主销品类别（12个）|
| `PROPERTY_ONSALE` | (224,296,297) | 在售款性质 ID |
| `PROPERTY_NEW` | (225,298,299) | 新品性质 ID |
| `PROPERTY_DISCONTINUED` | (127,126,152) | 绝版款性质 ID |

库存状态判断逻辑（见 `etl_ads_health.py`）：

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.7.6 | 2026-03-19 | 补充单人负责数据库的环境现实约束，明确 Oracle/VM 部署边界与 CRM 实证取证路径 |
| v0.7.5 | 2026-03-18 | 补充经验台帐、查询执行面与 Hook 边界说明 |
- 总仓：`C_STORE.CODE = '001'`
- 云仓：`C_STORE.IS_ALLO2OSTORAGE = 'Y'`

---

## 6. 调度依赖图

```
Windows 任务计划（每日 xx:xx）
    └─▶ run_scheduled_etl.bat
            └─▶ scheduled_etl.py
                    ├─▶ run_etl.py（8步流水线）
                    │       ├─▶ etl_dim_product.run()
                    │       ├─▶ etl_dim_sku.run()
                    │       ├─▶ etl_dim_store.run()
                    │       ├─▶ etl_dim_channel.run()
                    │       ├─▶ etl_dws_sales.run()
                    │       ├─▶ etl_dws_inventory.run()
                    │       ├─▶ dabo_ready（CSV 就绪检查）
                    │       └─▶ etl_ads_health.run()
                    └─▶ test_etl_automation.py（仅在 ETL 成功后执行）
```

ODS 流水线当前已纳入主流水线，也保留独立手动执行入口：
```
Windows 任务计划 / 手动触发
    └─▶ run_etl.py
            ├─▶ ods_sync（内部调用 run_ods.run）
            │       ├─▶ etl_ods_fa_storage.run()
            │       ├─▶ etl_ods_m_retail.run()
            │       └─▶ etl_ods_m_retailitem.run()
            └─▶ 后续 DWS / ADS 主链

手动独立执行：
    └─▶ python run_ods.py
            ├─▶ etl_ods_fa_storage.run()
            ├─▶ etl_ods_m_retail.run()
            └─▶ etl_ods_m_retailitem.run()
```

---

## 7. 告警机制

- 模块：`alerts.py`，通过 `WECHAT_WEBHOOK` 环境变量配置
- 触发时机：每步 ETL 失败且重试耗尽后，以及整体流水线完成后
- 摘要格式：含执行时间、总耗时、成功/警告/失败计数、步骤明细
- 不可重试错误（立即告警，不等待）：ORA-01017 / invalid username / access denied 等（见 `config.py:ETL_NON_RETRYABLE_ERROR_KEYWORDS`）

---

## 8. 技术栈

| 组件 | 版本/说明 |
|------|----------|
| Python | 3.13.x |
| python-oracledb | thin 模式（无需 Instant Client）|
| SQLAlchemy | ORM + 原生 SQL |
| PyMySQL | MySQL 驱动 |
| Pandas | 数据转换 |
| Oracle DB | 19c EE（伯俊 ERP 数据源）|
| MySQL | 8.0.x（何方数仓目标）|
| Windows 任务计划 | 生产调度 |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.6.3 | 2026-03-01 | 对齐 v0.6.3 目录与调度描述 |
| v0.7.0 | 2026-03-04 | 补充 Agent/Skills 目录与 MCP 本地配置 |
| v0.7.1 | 2026-03-16 | 同步 run_ods 参数与 ODS 质检说明 |
| v0.7.2 | 2026-03-18 | 增加 dim_channel 维度实现并将主流水线更新为 8 步 |
| v0.7.4 | 2026-03-18 | 新增只读查数工具、data-query skill/agent 与 MCP 降级说明 |
| v0.7.5 | 2026-03-18 | 新增经验台帐、复盘脚本与 Hook 边界说明 |
