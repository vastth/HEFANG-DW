# ARCHITECTURE.md — 何方珠宝数据仓库项目地图

> 本文档是仓库架构的**唯一权威来源**。修改数据层结构、调度顺序或关键配置后，必须同步更新本文件。
>
> 最后更新：2026-03-01（v0.6.3 对齐）

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
│  MySQL: dim_product / dim_sku / dim_store                        │
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
│   ├── run_etl.py              主调度：7步流水线（dim→dws→dabo→ads）
│   ├── run_ods.py              ODS 专项调度（全量/增量/质检）
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
│   ├── tools/export_ads.py                   导出 ADS 到 Excel
│   ├── tools/snapshot_mysql_hefangdw_schema.py  MySQL 结构快照
│   ├── tools/snapshot_oracle_bosnds3_schema.py  Oracle 结构快照
│   └── scripts/check_doc_sync.py              文档代码同步审计
│
├── 【文档】
│   └── docs/（8个中文 markdown + 3个英文 markdown）
│
├── 【配置与模板】
│   ├── .env.example            环境变量模板
│   ├── .claude/settings.json   Agent 默认设置（可提交）
│   └── .claude/CLAUDE.md       Agent 协作规范（本项目）
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
4    etl_dws_sales          销售增量（T-1 默认）         重试3次→告警停止
5    etl_dws_inventory      库存快照                     重试3次→告警停止
6    dabo_ready             达播 CSV 就绪检查            检查通过→触发回填
7    etl_ads_health         库存健康度全量重算           重试3次→告警继续
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
python run_ods.py --mode incremental  # 增量（默认，使用双水位）
python run_ods.py --mode full          # 全量覆盖
python run_ods.py --mode qc            # 仅执行质量校验
```

ODS 质检日志输出到 `logs/ods_qc_<日期>.log`。

---

## 4. 数据库连接

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
- 总仓：`C_STORE.CODE = '001'`
- 云仓：`C_STORE.IS_ALLO2OSTORAGE = 'Y'`

---

## 6. 调度依赖图

```
Windows 任务计划（每日 xx:xx）
    └─▶ run_scheduled_etl.bat
            └─▶ scheduled_etl.py
                    ├─▶ run_etl.py（7步流水线）
                    │       ├─▶ etl_dim_product.run()
                    │       ├─▶ etl_dim_sku.run()
                    │       ├─▶ etl_dim_store.run()
                    │       ├─▶ etl_dws_sales.run()
                    │       ├─▶ etl_dws_inventory.run()
                    │       ├─▶ dabo_ready（CSV 就绪检查）
                    │       └─▶ etl_ads_health.run()
                    └─▶ test_etl_automation.py（仅在 ETL 成功后执行）
```

ODS 流水线独立调度（通常早于主流水线）：
```
Windows 任务计划（每日更早）
    └─▶ python run_ods.py --mode incremental
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
