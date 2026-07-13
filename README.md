# 何方珠宝数据仓库（HEFANG DW）

<div align="center">

**基于Oracle到MySQL的珠宝电商数据仓库项目**

[![Python](https://img.shields.io/badge/Python-3.13.9-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Oracle](https://img.shields.io/badge/Oracle-19c_EE-F80000?logo=oracle&logoColor=white)](https://www.oracle.com/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0.44-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)

</div>

---

## 📖 项目简介

**何方珠宝数据仓库**是一个为广东何方珠宝有限公司打造的企业级数据仓库解决方案，旨在整合线上线下全渠道销售与库存数据，支撑库存优化、补货决策、销售分析等业务需求。

**更新日志**：详见 [CHANGELOG.md](CHANGELOG.md)

### 业务背景
- **企业**：广东何方珠宝有限公司（HEFANG Jewelry）
- **主营**：时尚珠宝首饰（925银饰为主）
- **业务模式**：电商（天猫、抖音、京东、小红书等）+ 线下门店（直营+加盟）
- **ERP系统**：伯俊ERP（Oracle 11g数据库）
- **数据规模**：15,000+ SKU / 500,000+ 订单 / 150+ 店仓

### 核心价值
✅ **库存优化**：精准计算库存周转天数与建议补货数量  
✅ **销售洞察**：全渠道销售趋势分析与SABC分级  
✅ **云仓管理**：支持门店云仓机制，扩大电商可售库存池  
✅ **自动化**：定时ETL任务，保障数据时效性  
✅ **可视化**：对接Tableau，支持高管驾驶舱与运营报表

---

## 🏗️ 架构设计

### 技术架构

```
┌─────────────────────────────────────────────────────────────┐
│                      数据消费层                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Tableau Dashboard │ Excel导出     │   API接口     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                           ▲
                           │
┌─────────────────────────────────────────────────────────────┐
│                   MySQL数据仓库 (hefang_dw)                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ ADS应用层: ads_inventory_health | ads_store_daily_report | ads_store_daily_subject_report │   │
│  │            | ads_daily_sales                                                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ DWS汇总层: dws_sales_daily | dws_inventory_daily     │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ DIM维度层: dim_product | dim_store | dim_channel | dim_sku │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                           ▲
                           │ Python ETL Scripts
                           │
┌─────────────────────────────────────────────────────────────┐
│                   Oracle源数据库 (伯俊ERP)                   │
│  M_RETAIL | M_RETAILITEM | FA_STORAGE | M_PRODUCT          │
│  C_STORE | M_PURCHASE | M_PURCHASEITEM | M_DIM             │
└─────────────────────────────────────────────────────────────┘
```

### 数据分层模型

| 数据层 | 名称 | 表前缀 | 说明 | 更新频率 |
|--------|------|--------|------|----------|
| **ODS** | 原始层 | `ods_*` | 1:1复制源表（可选） | 视需要全量 |
| **DIM** | 维度层 | `dim_*` | 商品、店仓等主数据 | 每日全量 |
| **DWS** | 汇总层 | `dws_*` | 日粒度销售、库存明细 | 销售增量/库存快照 |
| **ADS** | 应用层 | `ads_*` | 业务主题宽表（库存健康度、门店经营日报等） | 每日全量 |

注：零售明细存在两条写入链路（线上/线下），增量同步采用“双水位”策略：
- `MODIFIEDDATE`：线上通道
- `SETTIME`：线下门店通道（`DOCNO/ORDERNO` 前缀含 `RT`）
- `OMS_SOURCECODE`：外部来源订单号，可用于达播主订单与 `ods_m_retail` 的 MySQL 内桥接
- `ads_dabo_order_retail_bridge`：达播订单到零售单头的 MySQL 桥接缓存；当 `ods_m_retail.oms_sourcecode` 尚未完成历史回填时，日报模板会自动回退到该缓存表
- `重复装载治理`：`ods_m_retail` / `ods_m_retailitem` 当前已改为“窗口清理 + 分块按源 `id` 替换写入”，并在模块级 `run()` 上增加 MySQL 命名锁，降低重复装载事故再次发生的概率
- `唯一键治理`：新建环境执行 `SQL/create_ods_tables.sql` 时，两个 ODS 零售表都会直接带 `UNIQUE KEY (id)`；现网历史库仍需由用户手工执行 `SQL/alter_ods_m_retail_enforce_unique_id.sql` 与 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql`

---

## 📁 目录结构

```
hefang_dw/
├── config.py                    # 数据库连接配置
├── db_connections.py            # 统一数据库连接工厂（连接池与超时）
│
├── run_etl.py                   # ETL总控脚本（全量执行）
├── scheduled_etl.py             # 定时任务调度脚本
├── run_scheduled_etl.bat        # Windows计划任务脚本
├── scheduled_total_control.py   # 主链成功后再触发销售专题链的总控调度脚本
├── run_scheduled_total_control.bat # 主链+销售专题总控 Windows 计划任务脚本
├── scheduled_store_daily_report.py # 门店日报专题调度脚本
├── run_scheduled_store_daily_report.bat # 门店日报专题Windows计划任务脚本
│
├── etl_dim_product.py           # 商品维度ETL
├── etl_dim_sku.py               # SKU维度ETL
├── etl_dim_store.py             # 店仓维度ETL
├── etl_dim_channel.py           # 渠道维度ETL
├── etl_dws_sales.py             # 销售明细ETL（SKU粒度）
├── etl_dws_inventory.py         # 库存明细ETL（SKU粒度）
├── etl_ads_health.py            # 库存健康度ETL
├── etl_ads_store_daily_report.py # 门店经营日报ETL（最终经营实体层）
├── etl_ads_store_daily_subject_report.py # 门店经营日报ETL（统计主体兼容层）
├── etl_ads_daily_sales.py       # 销售看板月度战役ETL（仓库样板，可由专题调度消费）
├── etl_ods_fa_storage.py         # ODS库存同步（默认全量，可选执行）
├── etl_ods_m_retail.py           # ODS零售主表增量同步（默认回刷7天，可切全量）
├── etl_ods_m_retailitem.py       # ODS零售明细双水位增量（MODIFIEDDATE+SETTIME，可切全量）
├── run_ods.py                    # ODS入口（默认增量，可选全量/调整回刷）
├── test_etl_automation.py       # ETL自动化测试
├── test_scheduled_store_daily_report.py # 门店日报专题调度最小单元测试
├── test_store_operation_owner_import.py # 门店经营负责人导入最小单元测试
│
├── tools/                       # 辅助工具脚本（非运行链路）
│   ├── test_connection.py       # 数据库连接测试工具
│   ├── check_data.py            # 数据质量检查脚本
│   ├── check_dws_inventory.py   # 库存专项检查
│   ├── check_ods_incremental.py # ODS对账（主表/明细）
│   ├── check_ods_retailitem_quality.py # ODS明细质量对账（双通道拆分）
│   ├── export_ads.py            # ADS数据导出
│   ├── query_data.py            # 通用只读查数与导出
│   ├── import_store_operation_owner_from_nas.py # 门店经营负责人快照 dry-run / 导入工具
│   ├── load_dabo_order_labels_from_nas.py # 达播订单标签 dry-run / 导入工具
│   └── snapshot_*_schema.py     # MySQL / Oracle 结构快照
├── SQL/create_store_operation_owner_tables.sql # 门店经营负责人快照与SCD2建表脚本
├── scripts/                     # 运维与协作脚本
│   ├── check_doc_sync.py        # 文档同步审计
│   ├── doctor.ps1               # 环境自检
│   ├── log_agent_action.py      # Agent交接记录写入
│   └── log_agent_lesson.py      # Agent经验台帐写入
│
│
├── notebooks/                   # 数据探索Jupyter笔记本（非运行链路）
│   ├── explore_M_IN_OUT_.ipynb
│   ├── explore_M_PURCHASE.ipynb
│   ├── explore_M_TRANSFER.ipynb
│   └── explore_RP_SIMPLESTORAGE.ipynb
│
├── docs/                        # 项目文档（⭐推荐阅读）
│   ├── 数据仓库与ETL手册.md     # 数仓架构与ETL流程
│   ├── 数据结构与映射手册.md     # 源表与目标表映射
│   ├── 业务逻辑与指标规范.md     # 指标定义与计算公式
│   ├── SQL开发手册.md           # SQL模板与开发规范
│   ├── ETL业务逻辑说明.md     # 每个ETL脚本的人话版逻辑说明
│   ├── 问题排查手册.md          # 常见问题与解决方案（待创建）
│   ├── MYSQL数据字典.md # MySQL数据字典（主）
│   ├── 子项目资料/              # 子项目上下文、权威资料与续接资料
│   │   └── ODS打通自动化链路计划与续接入口.md # ODS 打通工作续接主文件
│   └── ...
│
├── SQL/                         # SQL脚本
│   ├── create_ods_tables.sql     # ODS建表SQL（可选）
│   ├── create_ads_daily_sales.sql # 销售看板月度战役建表SQL（手工执行）
│   ├── check_ads_daily_sales_min.sql # 销售看板月度战役最小对账SQL
│   └── ...
│
├── README.md                    # 本文档
├── logs/                        # 日志输出目录
└── __pycache__/                 # Python缓存目录
```

---

## ⚡ 快速开始

### 1. 环境准备

**依赖安装**
```bash
# 推荐使用 python-oracledb（thin 模式）或当需要时安装 Oracle Instant Client
pip install python-oracledb pymysql pandas openpyxl
```

**Oracle 连接说明（thin vs Instant Client）**
- `python-oracledb` 支持两种模式：
   - thin 模式（纯 Python，通常无需安装 Oracle Instant Client，适合大多数场景）。
   - thick/OCI 模式（依赖 Oracle Instant Client），当需要使用某些 Oracle 客户端特性或更高性能时才需要安装。详见官方文档。
- 如果你确实需要安装 Instant Client（Windows），请参考：
   - 下载：https://www.oracle.com/database/technologies/instant-client/downloads.html
   - 解压并配置环境变量：将 instantclient 路径加入 `PATH`。

### 2. 配置数据库连接

优先通过环境变量配置数据库连接；`config.py` 默认读取以下变量：

```powershell
# Oracle源数据库（伯俊ERP）
$env:ORACLE_USER = 'your_username'
$env:ORACLE_PASSWORD = 'your_password'
$env:ORACLE_HOST = 'your_host'
$env:ORACLE_PORT = '1521'
$env:ORACLE_SERVICE = 'orcl'

# MySQL目标数仓
$env:MYSQL_HOST = 'localhost'
$env:MYSQL_PORT = '3306'
$env:MYSQL_USER = 'root'
$env:MYSQL_PASSWORD = 'your_password'
$env:MYSQL_DB = 'hefang_dw'

# 连接池与超时（可选；未设置时使用 db_connections.py 默认值）
$env:MYSQL_POOL_SIZE = '5'
$env:MYSQL_MAX_OVERFLOW = '5'
$env:MYSQL_POOL_TIMEOUT = '30'
$env:MYSQL_POOL_RECYCLE = '1800'
$env:MYSQL_CONNECT_TIMEOUT = '10'
$env:MYSQL_READ_TIMEOUT = '60'
$env:MYSQL_WRITE_TIMEOUT = '60'
$env:MYSQL_ETL_READ_TIMEOUT = '300'
$env:MYSQL_ETL_WRITE_TIMEOUT = '300'
$env:MYSQL_LONG_RUNNING_READ_TIMEOUT = '600'
$env:MYSQL_LONG_RUNNING_WRITE_TIMEOUT = '600'
$env:ORACLE_POOL_SIZE = '3'
$env:ORACLE_MAX_OVERFLOW = '2'
$env:ORACLE_POOL_TIMEOUT = '30'
$env:ORACLE_POOL_RECYCLE = '1800'

# NAS 自动鉴权（门店日报目标目录、达播云雀目录）
$env:HEFANG_NAS_USERNAME = 'your_nas_user'
$env:HEFANG_NAS_PASSWORD = 'your_nas_password'
```

如需查看基础连接配置键名，可参考 [config.py](config.py)。当前 hefang_dw 内部数据库连接统一通过 [db_connections.py](db_connections.py) 创建：SQLAlchemy Engine 默认启用 `pool_pre_ping`、`pool_recycle`、`pool_timeout`、`pool_size` 与 `max_overflow`，PyMySQL 直连默认注入 `connect_timeout`、`read_timeout` 与 `write_timeout`。连接工厂现支持三档 MySQL 超时：`default` 使用 `MYSQL_READ_TIMEOUT` / `MYSQL_WRITE_TIMEOUT`，`etl` 使用 `MYSQL_ETL_READ_TIMEOUT` / `MYSQL_ETL_WRITE_TIMEOUT`，`long_running` 使用 `MYSQL_LONG_RUNNING_READ_TIMEOUT` / `MYSQL_LONG_RUNNING_WRITE_TIMEOUT`；新增或修改任何数据库读写链路前，都必须按任务耗时与事务范围显式选择匹配的 `timeout_profile`，并保留超时验证证据。

### 3. 测试连接

```bash
python tools/test_connection.py
```

预期输出：
```
✅ Oracle连接成功！
✅ MySQL连接成功！
```

### 3.2 只读查数与导出

以下工具都支持从任意工作目录直接运行，默认按仓库根目录解析输出路径：

```bash
# 查看内置查数模板
python tools/query_data.py --list-templates

# MySQL：最近 7 天销售排行
python tools/query_data.py --template mysql_sales_rank_7d

# Oracle：最近 7 天零售单据统计
python tools/query_data.py --source oracle --template oracle_retail_docs_7d

# ODS 重复装载治理自检（会额外输出 duplicate_id_count）
python tools/check_ods_incremental.py

# MySQL：按 ODS BILLDATE 统计某个达播样本文件的每日实收/退款
python tools/query_data.py --template mysql_dabo_actual_daily_by_billdate --param source_file=dabo_20260204.csv
python tools/sync_dabo_order_retail_bridge.py --source-file dabo_20260204.csv

# 达播统一 Excel：从 NAS 最新订单管理文件提取候选集（默认不写库）
python tools/extract_dabo_order_candidates_from_nas.py --preview-limit 5
python tools/extract_dabo_order_candidates_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx" --export-csv reports/dabo_yunque_candidates_selected.csv
python tools/extract_dabo_order_candidates_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx" --export-order-label-csv reports/dabo_order_labels.csv

# 达播订单标签：默认 dry-run，只生成内部标识表摘要
python tools/load_dabo_order_labels_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx" --report-json reports/dabo_order_labels_dry_run.json

# 达播订单标签：用户授权后再执行正式写库
python tools/load_dabo_order_labels_from_nas.py --apply --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx"

# 达播订单标签：dry-run / apply 摘要会输出 normalization_status_distribution，用于检查 canonical 归一结果

# MySQL：手工执行 ods_m_retail.oms_sourcecode 历史回填（先装载暂存，再分批 apply）
python tools/backfill_ods_m_retail_oms_sourcecode.py --apply-batch-size 10000

# 若暂存表已准备完成，可仅继续分批 apply
python tools/backfill_ods_m_retail_oms_sourcecode.py --apply-only --apply-batch-size 10000

# 导出最新库存健康度快照
python tools/export_ads.py
```

说明：
- `tools/query_data.py` 只支持只读查询，适合临时查数、样本导出与自由分析。
- `tools/check_ods_incremental.py` 现会额外输出 `ods_m_retail` 与 `ods_m_retailitem` 的 `duplicate_id_count`，并对 `ods_m_retail.oms_sourcecode` 做 Oracle/MySQL 覆盖对照，可用于识别重复装载与桥接字段回退。
- `tools/export_ads.py` 仅导出 `ads_inventory_health`，不扩展其他业务逻辑。
- `tools/extract_dabo_order_candidates_from_nas.py` 是 hefang_dw 内部落地统一 Excel 主线的第一层工具：既可导出行级候选集，也可导出去重后的订单标签 CSV，当前不会直接改写旧达播兼容表。
- `tools/load_dabo_order_labels_from_nas.py` 默认只做 dry-run；在用户授权后可将统一 Excel 结果写入 `ads_dabo_order_label`，并为异常组合单生成 `canonical_system_order_id`，在保留原始 `system_order_id` 的前提下提升 ODS 桥接命中率。
- `tools/query_data.py` 已新增 `mysql_dabo_tagged_daily_by_billdate` 模板，当前会优先使用 `COALESCE(canonical_system_order_id, system_order_id)` 与 `ods_m_retail.oms_sourcecode` 做桥接，再按渠道汇总达播日实收/退款。
- `tools/snapshot_mysql_hefangdw_schema.py` 与 `tools/snapshot_oracle_bosnds3_schema.py` 生成的是结构快照，只反映表、字段、类型和注释，不查看实际数据值。
- `tools/backfill_ods_m_retail_oms_sourcecode.py` 会更新 MySQL ODS，请仅在需要补齐历史桥接字段时手工执行；全量 apply 已改为分批处理，降低长事务锁表风险。
- `SQL/alter_ods_m_retail_enforce_unique_id.sql` 与 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql` 也属于现网手工治理脚本：用于先清理历史重复装载，再补唯一键；默认仍应由用户人工执行。
- 若本次排障形成可复用结论，或你明确指出业务逻辑/字段语义错误，可用 `python scripts/log_agent_lesson.py ...` 将经验写入 `docs/AGENT_LESSONS.md`。

### 3.1 告警与快速测试（新增）

项目支持通过企业微信机器人发送 ETL 执行摘要，并提供安全的连接测试模式以验证消息发送与重试策略：

- 环境变量：
   - `WECHAT_WEBHOOK`：企业微信机器人完整 webhook URL（建议通过环境变量注入，不要写入代码仓库）。
   - `ETL_CONN_TEST`：设置为 `1` 或在命令行添加 `--conn-test` 启用“仅连接测试”模式（不会写入数据）。
   - `ETL_MAX_RETRIES`：可选，覆盖默认最大重试次数（默认 3）。
   - `ETL_RETRY_SLEEP`：可选，覆盖重试间隔秒数（默认 60）。

- 消息发送策略：
   - 成功：8 个 ETL 步骤全部完成后，发送“成功摘要”。
   - 失败：重试结束或命中不可重试错误后，发送“失败摘要”（同一模板）。
   - 统一模板字段：执行时间、总耗时、成功/警告/失败计数，以及按入口展开的关键摘要；其中总控入口的企微消息只保留高层链路状态、关键摘要与异常提示，完整步骤明细继续写入本地日志。
   - 总控模式：通过 `scheduled_total_control.py` 触发时，会抑制主链与门店销售专题链各自的企微成功/失败摘要，改由总控在两条链路完成后发送一条统一摘要；当前统一出口已覆盖“主链 + 门店销售专题”，后续新增专题可复用同一结构化摘要协议接入。总控企微正文默认不再展开 `detail_lines`，完整明细以 `logs/scheduled_total_control_YYYYMMDD.log` 为准。

- 使用示例（临时设置并运行连接测试）：

```powershell
$env:WECHAT_WEBHOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY'
$env:ETL_CONN_TEST = '1'
$env:ETL_MAX_RETRIES = '1'  # 仅尝试一次，便于快速验证告警
python run_etl.py --conn-test
```

- 说明：当脚本检测到不可重试的确定性错误（例如认证失败 ORA-01017、MySQL 1045），会立即停止重试并发送摘要，以避免无意义的重复尝试。摘要文本会自动使用 `config.py` 中的 `TASK_DISPLAY_NAME` 将任务 ID 映射为友好中文描述。

- 调度入口说明：
   - `run_etl.py` 是统一执行入口（包含重试 + 企微摘要发送）。
   - `scheduled_etl.py` 为调度包装脚本，内部调用 `run_etl.py`；`run_scheduled_etl.bat` 调用 `scheduled_etl.py`。
   - `scheduled_total_control.py` 为更外层的总控包装：先跑 `scheduled_etl.py`，仅当主链返回 `0` 时再跑 `scheduled_store_daily_report.py`；`run_scheduled_total_control.bat` 调用 `scheduled_total_control.py`。总控会收集各子链结构化摘要并统一发送一条企业微信消息，避免主链和专题链各发一条造成出口分散。
   - 若 Windows 计划任务希望把“0:05 / 12:30 主链 + 主链成功后自动触发销售专题”收口为一个入口，优先调 `run_scheduled_total_control.bat`，而不是把销售专题直接并入 `run_etl.py`。

 - 模块与配置位置说明：
    - 告警实现：`alerts.py`（项目根目录），替换告警渠道时可直接修改或替换此模块。
    - 告警显示名称：`config.py` 中的 `TASK_DISPLAY_NAME`，可直接在配置中修改友好名称或做国际化处理。

 - Oracle 校验 SQL（可选）：
    - 为避免测试中使用硬编码常量，可在 `config.py` 中配置 `ORACLE_VERIFY_QUERIES` 字典，示例：

```python
# config.py
ORACLE_VERIFY_QUERIES = {
      'dws_inventory_main_products': """
            SELECT COUNT(DISTINCT p.ID)
            FROM FA_STORAGE fs
            LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
            LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
            WHERE fs.ISACTIVE = 'Y'
               AND fs.M_PRODUCTALIAS_ID IS NOT NULL
               AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
               AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
      """,
      'ads_health_total': """
            SELECT COUNT(DISTINCT fs.M_PRODUCTALIAS_ID)
            FROM FA_STORAGE fs
            LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
            LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
            WHERE fs.ISACTIVE = 'Y'
               AND fs.M_PRODUCTALIAS_ID IS NOT NULL
               AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
               AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
      """,
}
```

    - `test_etl_automation.py` 会优先使用上述 SQL 从 Oracle 拉取对比计数，若未配置则回退到测试常量（便于快速运行）。

 - 关于销售负数：脚本中将销售表中的负数视为退货并计入统计（属于正常业务）；仅在负数数量异常增多时才会触发告警。


### 4. 首次全量ETL

```bash
python run_etl.py
```

执行流程：
```
[1/9] dim_product (商品维度) ✅
[2/9] dim_sku (SKU维度) ✅
[3/9] dim_store (店仓维度) ✅
[4/9] dim_channel (渠道维度) ✅
[5/9] ods_sync (ODS同步与质检) ✅
[6/9] dws_sales_daily (销售明细，主链近7天回带，已消费ODS) ✅
[7/9] dws_inventory_daily (库存明细，已消费ODS) ✅
[8/9] dabo_ready (达播主线就绪检查) ✅
[9/9] ads_inventory_health (库存健康度) ✅
```

说明：`dabo_ready` 现优先检查 `ads_dabo_order_label` 最新批次是否存在且最近 1 天有更新，并附带输出 `ads_dabo_daily_sales` 当日状态；`ads_health` 会优先基于最新标签批次在 ODS 内按订单标签汇总达播 SKU 指标，并用 `ads_dabo_order_retail_bridge` 做缓存兜底；仅当标签批次不可用时，才回退 `ads_dabo_daily_sales`，否则达播字段按 0 处理。

### 4.1 ODS同步（默认增量，可切全量）

```bash
# 默认：增量模式（回刷7天，自动分窗口；零售明细按 MODIFIEDDATE + SETTIME 双水位增量，并对双空明细按头单 MODIFIEDDATE 兜底）
python run_ods.py

# 强制全量（完成后会对 retail / retailitem 再做一轮固定 as-of 的 recent catch-up）
python run_ods.py --full

# 调整或关闭 full 后补追窗口
python run_ods.py --full --full-catchup-days 2
python run_ods.py --full --full-catchup-days 0

# 调整回刷天数或窗口大小
python run_ods.py --backfill-days 14 --window-days 1
```

质检与可选参数：
- 跳过质检：`--skip-qc`
- 质检全量：`--qc-all`
- 质检回看天数：`--qc-days 7`

说明：`run_ods.py` 会在抽取完成后自动执行 ODS 质量校验，并将结果写入 `logs/ods_qc_*.log`；增量模式下质检默认使用抽取完成时刻作为 `--as-of` 截止时间，避免时间漂移。
自 2026-04-07 起，`run_ods.py --full` 默认会在 `ods_m_retail` / `ods_m_retailitem` 全量完成后，再按同一个固定 `as-of` 自动补一轮最近 1 天的增量 catch-up，用来覆盖长时间全量期间新增但未落入原始 full 上界的数据；`--full-catchup-days 0` 可显式关闭。来源：[run_ods.py](run_ods.py#L72-L125)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L91-L151)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L134-L206)
自 2026-05-19 起，`ods_m_retailitem` 在 `modifieddate` 主通道内，额外回刷 `modifieddate/settime` 双空但头单 `M_RETAIL.MODIFIEDDATE` 落窗的明细，避免 `ods_m_retailitem_unknown_nulls` 长期残留差异。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L137-L158)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L330-L346)
同时，自 2026-03-23 起，`run_etl.py` 主链也会自动调用 ODS 同步；`run_ods.py` 仍保留为可独立手动执行的入口。
自 2026-04-23 起，`run_etl.py` 会把 ODS 默认 7 天回刷窗口同时用于 `dws_sales_daily` 主链，先执行 `run_ods_sync(backfill_days=7)`，再执行 `etl_dws_sales.run(days_back=7, include_today=True)`；这样即使 ODS 补齐了晚到的 `billdate`，DWS 也会在同轮主链内自动回带消费，不再只刷新当天。来源：[run_etl.py](run_etl.py#L59)；[run_etl.py](run_etl.py#L526)；[run_etl.py](run_etl.py#L544)；[run_etl.py](run_etl.py#L570)

### 4.2 门店经营日报专项生成（独立入口）

```bash
# 只检查依赖表、脚本内置 SQL 和配置是否齐备，不写入数据
python etl_ads_store_daily_report.py --conn-test

# 只检查门店日报统计主体层依赖，不写入数据
python etl_ads_store_daily_subject_report.py --conn-test

# 门店日报目标导入：按月份分文件时，显式指定目标月份
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5

# 门店日报目标导入：连同门店属性一起预演
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5 --sync-store-report-attr

# 门店属性差异清单：只读比对 April 权威快照与当前有效门店属性
python tools/diff_store_report_attr_snapshot.py --target-month 2026-04 --preview-limit 10 --output-json reports/store_attr_snapshot_diff_202604.json

# 门店属性快照登记：登记最新 NAS 快照、diff 摘要与待落地状态
python tools/register_store_attr_snapshot.py --target-month 2026-04 --diff-output reports/store_attr_snapshot_diff_202604_registered.json

# 门店日报专题调度：只做文件解析、日志表检查和 dry-run
python scheduled_store_daily_report.py --conn-test

# 门店日报专题调度：自动检查 NAS 最新目标文件；若目标/负责人链路命中幂等且 ADS 日期已覆盖，会继续比较 dws_sales_daily 与专题 ADS 的 etl_time，源 DWS 更新更晚时触发近7天 freshness 重跑
python scheduled_store_daily_report.py

# 门店日报专题调度：显式指定目标月份
python scheduled_store_daily_report.py --target-month 2026-04

# 门店日报专题调度：显式指定负责人快照日期与工作表
python scheduled_store_daily_report.py --target-month 2026-04 --owner-snapshot-date 2026-04-22 --owner-sheet-name 门店负责人映射模板

# 门店日报专题调度：只跑目标链路，不执行负责人快照导入
python scheduled_store_daily_report.py --target-month 2026-04 --no-run-owner-import

# 门店日报专题调度：导入成功后仅记录受影响日期，不自动批量重跑门店层/主体层/销售看板 ADS
python scheduled_store_daily_report.py --target-month 2026-04 --no-run-affected-ads

# 门店日报专题调度：显式按日期列表批量重跑门店层/主体层/销售看板 ADS
python scheduled_store_daily_report.py --rerun-report-date 2026-04-01 --rerun-report-date 2026-04-02 --rerun-data-version v1

# Windows 包装脚本同样支持透传显式重跑参数
run_scheduled_store_daily_report.bat --rerun-report-date 2026-04-01 --rerun-data-version v1

# 专题调度入口会先申请 hefang_dw:scheduled_store_daily_report 单实例锁；若已有另一条调度在跑，本次会立即退出，不再与 ads_* 子任务互相抢锁

# 门店日报专题调度：最小单元测试（不写库）
python -m unittest test_scheduled_store_daily_report.py

# 门店日报目标导入：正式写库
# 现网已于 2026-04-03 完成日志表建表；新环境首次使用前先执行 SQL/create_log_store_target_import.sql
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --created-by your_name

# 门店日报目标导入：多月份文件下，显式写入指定月份并同步门店属性
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --sync-store-report-attr --created-by your_name

# 门店日报目标导入：正式扩正式范围时，同步刷新 dim_store_report_attr
python tools/import_cfg_store_target_daily_from_nas.py --apply --sync-store-report-attr --created-by your_name

# 门店经营负责人快照：dry-run
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --preview-limit 10

# 门店经营负责人快照：显式指定文件与工作表
python tools/import_store_operation_owner_from_nas.py --file-path "\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx" --sheet-name 门店负责人映射模板 --snapshot-date 2026-04-21 --preview-limit 10

# 门店经营负责人快照：正式写库
# 首次使用前先执行 SQL/create_store_operation_owner_tables.sql
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --apply --created-by your_name

# 门店经营负责人导入：最小单元测试
python -m unittest test_store_operation_owner_import.py

# 生成指定日期、指定版本的门店经营日报
# 若目标库尚未补齐负责人字段，先执行 SQL/alter_ads_store_daily_report_add_owner_name.sql
python etl_ads_store_daily_report.py --report-date 2026-03-23 --data-version v1

# 生成指定日期、指定版本的门店经营日报统计主体层
python etl_ads_store_daily_subject_report.py --report-date 2026-03-23 --data-version v1

# 销售看板月度战役：只检查源依赖，不写入数据
python etl_ads_daily_sales.py --conn-test

# 销售看板月度战役：当前环境已建表；新环境正式执行前先由用户手工建表
python etl_ads_daily_sales.py --report-date 2026-04-14 --data-version v1
```

说明：`etl_ads_store_daily_report.py` 当前保持独立运行，尚未并入 `run_etl.py` 主链。
`etl_ads_store_daily_report.py` 当前按“最终经营实体”出数：未配置共同考核时保持一店一行，已配置共同考核时会在本表直接合并为经营体行；对应依赖也扩展为 `dim_store_report_attr`、`cfg_store_target_daily`、`cfg_store_assessment_subject_target_daily`、`cfg_store_assessment_assignment`。商品范围当前固定排除 `147=辅料`、`149=办公用品`、`150=道具`，其余 `dim_product.category_id` 默认纳入，因此新增品类不再依赖 `dim_report_product_rule` 补配置。`etl_ads_store_daily_subject_report.py` 以上述最终经营实体层 `ads_store_daily_report` 为输入，再叠加共同考核配置回填完整主体编码、主店锚点与成员数；若当前月份未配置共同考核，主体层会按 `STORE_<store_code>` 自动回退为“每店一个统计主体”。来源：[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L8)；[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L109)；[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L148)；[etl_ads_store_daily_subject_report.py](etl_ads_store_daily_subject_report.py#L95)；[etl_ads_store_daily_subject_report.py](etl_ads_store_daily_subject_report.py#L154)。正式计算 SQL 已内置在两个 ETL 脚本中，`docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql` 仅保留为设计参考，不再作为运行时依赖。
当前脚本还会按最终经营实体粒度左联 `dim_store_operation_owner_assignment` 的生效负责人切片，把 `owner_name` 下沉到 `ads_store_daily_report`；若目标库尚未执行 `SQL/alter_ads_store_daily_report_add_owner_name.sql`，或当前 `report_date` 命中负责人切片重叠 / 缺切片，脚本会在写数前直接失败。负责人字段允许为空，但“缺切片”和“切片重叠”不允许静默通过。来源：[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L149)；[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L370)；[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L526)；[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L629)；[etl_ads_store_daily_report.py](etl_ads_store_daily_report.py#L822)。
`etl_ads_daily_sales.py` 当前已补齐建表 SQL、独立 ETL 与最小对账 SQL，作为销售看板“月度战役”主题的第二张仓库样板表。本轮已统一到门店日报权威口径：共同考核经营体按 `sales_date` 优先取 `cfg_store_assessment_subject_target_daily.day_target`，未命中时才回退经营实体内门店日目标求和；当日实际和去年同期实际改为在 `ods_m_retail + ods_m_retailitem` 上按门店日报同口径门店范围、商品范围汇总净额；`cum_target_amt`、`cum_actual_amt` 与 `last_year_cum_actual_amt` 当前都按 `area_name + report_channel_type` 的日序列累计，不再按单店窗口累加。当前代码已接入 `scheduled_store_daily_report.py` 的受影响日期批量重跑，但仍未接入 `run_etl.py`。历史 `2026-04-15 / v1` 与 `2026-04 / v2` 的样本验证结论对应的是旧版销售主题逻辑，本轮统一门店日报口径后尚未追加新的正式写库验证；后续如需落库，应由用户手工执行 ETL，再用 `SQL/check_ads_daily_sales_min.sql` 重新做最小对账。来源：[etl_ads_daily_sales.py](etl_ads_daily_sales.py#L122)；[etl_ads_daily_sales.py](etl_ads_daily_sales.py#L141)；[etl_ads_daily_sales.py](etl_ads_daily_sales.py#L175)；[etl_ads_daily_sales.py](etl_ads_daily_sales.py#L189)；[etl_ads_daily_sales.py](etl_ads_daily_sales.py#L231)；[etl_ads_daily_sales.py](etl_ads_daily_sales.py#L311)；[SQL/create_ads_daily_sales.sql](SQL/create_ads_daily_sales.sql#L1)；[SQL/check_ads_daily_sales_min.sql](SQL/check_ads_daily_sales_min.sql#L1)
`cfg_store_target_daily` 的正式交付路径已确认采用“业务投递 Excel 到 NAS 指定目录，由 Python 定时扫描导入”；当前已冻结 NAS 目录为 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，并按月份分文件管理，当前推荐命名规则已切换为 `YYYYMM考核数据配置表.xlsx`，例如 `202604考核数据配置表.xlsx`；导入脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。当前仓库已提供导入脚本 `tools/import_cfg_store_target_daily_from_nas.py`；现网已于 2026-04-03 完成 `log_store_target_import` 建表、首轮 `--apply` 写库与专项消费验证，新环境首次使用前仍需先执行 `SQL/create_log_store_target_import.sql`，并在 dry-run 通过后显式使用 `--apply`。当 NAS 目录下同时存在多个月份文件时，脚本要求显式传入 `--target-month YYYY-MM` 后再执行 dry-run / apply，避免误选月份；若同一目标月份同时存在多个版本文件，则需改用 `--file-path` 显式指定具体文件。若模板已新增 `门店类型` 列，可追加 `--sync-store-report-attr` 按 `store_id` 将当前有效 `dim_store_report_attr` 记录分类为未变化 / 变更 / 新增 / 退出；未变化不动，变更执行关旧开新，新增只开新，退出只关旧。若工作簿同时提供 `统计主体目标` 与 `门店考核归属` 两张 sheet，脚本会在同一事务中按目标月份 + 目标版本删旧重灌 `cfg_store_assessment_subject_target_daily` 与 `cfg_store_assessment_assignment`；两张 sheet 必须同时存在，且两张都为空时表示清空当前月份共同考核配置。当前 `门店考核归属` sheet 已新增必填列 `门店ID`；列名沿用 `门店ID`，但业务真值应填写 RT 门店编码，如 `RT050`。脚本会优先按 `dim_store.store_code` 命中，若填写纯数字则继续兼容 `dim_store.store_id`，`门店名称` 仅作为展示与名称漂移提示，不再承担唯一匹配职责。脚本会把 `门店类型` 原值直接写入 `report_channel_type` 作为日报渠道最终真值，并在 dry-run / diff 输出中同步派生 `report_channel_type_group` 粗分类。`SQL/alter_dim_store_report_attr_add_channel_type_group.sql` 已于 2026-04-08 执行到现网，当前 `dim_store_report_attr` 已包含 `report_channel_type_group` 生成列。脚本默认沿用目标月内现有最新 `effective_start_date`，目标月无现存版本时回退到月首，并在写库前校验是否存在其他生效区间重叠。若 Windows 因 DNS 调整、凭证清理或计划任务上下文变化导致 UNC 会话失效，脚本会先读取 `HEFANG_NAS_USERNAME` / `HEFANG_NAS_PASSWORD` 自动重建 `\\192.168.0.151\hefang总部` 连接；未配置或配置错误时会直接报错，不再盲目重试。
`tools/import_store_operation_owner_from_nas.py` 负责把业务维护的“当前门店经营负责人快照”落到 MySQL，并在库内维护 SCD2 历史。它默认读取 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx`，兼容工作表 `门店负责人映射表 / 门店负责人映射模板`，首行必须包含 `门店编码 / 门店名称 / 负责人`，`备注` 可选。当前 NAS 正式文件已内置 `填写说明` sheet，并在数据表头追加批注，用于把“当前真值快照、共同考核推荐维护 `SUBJECT`、共同考核同月过渡允许 `STORE + SUBJECT` 并存且仅告警、负责人可为空、不维护 Excel 历史区间、同一实体只保留一行”的业务录入口径冻结在文件内；导入脚本会忽略说明 sheet，只读取数据 sheet。脚本会按 `snapshot_date` 先读取 `dim_store_report_attr` 当前有效且纳入口径的门店，再结合 `cfg_store_assessment_assignment` 与 `cfg_store_assessment_subject_target_daily` 推导当日应维护的经营实体清单：独立门店维护 `STORE`，共同考核经营体维护 `SUBJECT`。若同一目标月内已配置共同考核，负责人快照推荐只保留经营体行；但在生效切换过渡期内，若被吸收的 RT 成员门店行与对应 `SUBJECT` 行并存，或在生效日前已提前维护 `SUBJECT` 且成员 `STORE` 仍保留，脚本会把这些行降级为 warning，不再因 `unexpected_entities` 阻断 `--apply`。真正缺少当前应维护实体，或仅提前维护 `SUBJECT` 但未同时保留成员 `STORE` 时，仍会失败。正式写库前需先执行 `SQL/create_store_operation_owner_tables.sql` 创建 `cfg_store_operation_owner_snapshot`、`dim_store_operation_owner_assignment` 与 `log_store_operation_owner_import`；脚本默认 dry-run，只有 `--apply` 才会按快照日覆盖写入当前快照、维护历史切片，并写入导入日志。最小单元测试已覆盖“RT007 被 `SUBJ_SZ_WXTD` 吸收后 `STORE + SUBJECT` 并存仅告警”“共同考核生效日前提前维护 `SUBJECT` 且 `STORE` 仍在时仅告警”和“历史切片按 changed/new/exited 正确拆分”三类场景。来源：[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L27)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L29)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L39)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L152)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L223)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L256)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L320)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L364)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L403)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L437)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L584)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L728)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L775)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L830)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L876)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L917)；[tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L997)；[SQL/create_store_operation_owner_tables.sql](SQL/create_store_operation_owner_tables.sql#L1)；[SQL/create_store_operation_owner_tables.sql](SQL/create_store_operation_owner_tables.sql#L23)；[SQL/create_store_operation_owner_tables.sql](SQL/create_store_operation_owner_tables.sql#L48)；[test_store_operation_owner_import.py](test_store_operation_owner_import.py#L16)；[test_store_operation_owner_import.py](test_store_operation_owner_import.py#L66)
当前负责人快照链路已接入 `scheduled_store_daily_report.py`：自动模式会在目标导入之后执行负责人导入，并按 `file_md5 + snapshot_date` 做独立幂等判重；只有 `history_diff_counts.changed/new/exited > 0` 时，才会把负责人链路新增到受影响日期集合，且日期起点会被截断到当前目标月月初。来源：[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L975)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L991)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L1223)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L426)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L506)；[test_scheduled_store_daily_report.py](test_scheduled_store_daily_report.py#L114)；[test_scheduled_store_daily_report.py](test_scheduled_store_daily_report.py#L129)。
`scheduled_store_daily_report.py` 是当前门店日报的正式专题调度入口。自动模式会先检查 NAS 目录中最后修改的目标文件，只有当工作簿解析出的 `target_month` 等于当前月份时才继续执行；若最新文件属于历史或未来月份，则本轮记录跳过。通过当前月份门禁后，脚本再以 `log_store_target_import` 中最近一次 `SUCCESS` 记录的 `file_md5 + target_month + target_version` 做幂等判重；若同一文件已成功导入，本次会直接跳过，不重复写库。当前专题调度除 NAS 目标导入、门店属性同步与共同考核配置同步外，已按冻结规则产出当前 `target_version` 的受影响日期集合；当本次为正式 IMPORTED 且受影响日期非空时，会按同一日期列表顺序依次触发 `ads_store_daily_report`、`ads_store_daily_subject_report` 与 `ads_daily_sales` 批量重跑。为避免多条包装层实例重复触发同一批日期，当前脚本在进入主循环前会先申请顶层命名锁 `hefang_dw:scheduled_store_daily_report`；若已有另一条专题调度在跑，本次会立即退出，不再进入外层重试等待。来源：[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L49)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L93)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L984)
当前专题调度的实际顺序已扩展为“目标导入/幂等跳过 -> 负责人导入/幂等跳过或禁用 -> 合并两条链路的受影响日期 -> 自然日推进兜底判定 -> DWS freshness 判定 -> 三张保留 ADS 批量重跑”；新增 CLI 参数 `--owner-file-path`、`--owner-sheet-name`、`--owner-snapshot-date` 与 `--no-run-owner-import`，用于覆盖默认负责人文件、快照日或临时关闭负责人链路。若目标链路命中 `file_md5 + target_month + target_version` 幂等跳过、负责人链路也没有新增 `changed/new/exited` 受影响日期，但当前 `data_version` 下三张保留 ADS 的 `report_date` 仍未补到统一上界，专题调度会自动按缺口日期补跑；若日期已覆盖到统一上界，则继续比较近 7 天 `dws_sales_daily.etl_time` 与三张保留 ADS 的 `etl_time`，只要源 DWS 更新晚于专题 ADS，就按 freshness 命中的日期重跑，支持同一天第 2/第 3 次总控后刷新专题 ADS。来源：[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L101)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L450)；[scheduled_store_daily_report.py](scheduled_store_daily_report.py#L511)
正式关口开口前，可先使用 `tools/diff_store_report_attr_snapshot.py` 只读比对 April 权威快照与当前有效 `dim_store_report_attr`，按 `store_id` 输出未变化 / 变更 / 新增 / 退出四类清单，作为后续执行 SQL 与回滚 SQL 的输入证据。
若需要把快照审计结果沉淀成可追踪台账，可再执行 `tools/register_store_attr_snapshot.py`；该工具会同时落盘完整 diff JSON，并把 `file_md5 / compare_date / diff_counts / status` 追加到 `reports/store_attr_snapshot_registry.json`。即使最新 NAS 细分类门店类型尚未正式 apply 到现网，只要候选快照本身可解析且比对通过，也会登记为 `pending_apply`，不阻断第 2 步“快照登记机制”。

### 5. 验证数据

```sql
-- 在MySQL中执行
SELECT 'dim_product' AS 表名, COUNT(*) AS 记录数 FROM dim_product
UNION ALL SELECT 'dim_sku', COUNT(*) FROM dim_sku
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_channel', COUNT(*) FROM dim_channel
UNION ALL SELECT 'dws_sales_daily', COUNT(*) FROM dws_sales_daily
UNION ALL SELECT 'dws_inventory_daily', COUNT(*) FROM dws_inventory_daily
UNION ALL SELECT 'ads_store_daily_report', COUNT(*) FROM ads_store_daily_report
UNION ALL SELECT 'ads_inventory_health', COUNT(*) FROM ads_inventory_health;
```

---

## 📊 数据仓库表说明

### 维度表 (DIM)

#### `dim_product` - 商品维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| product_id | 商品ID | 主键 |
| product_code | 商品款号 | M_PRODUCT.NAME |
| product_name | 商品名称 | - |
| brand_id/brand_name | 品牌 | 维度映射 |
| category_id/category_name | 类别 | 耳饰/项链/戒指等 |
| series_id/series_name | 系列 | - |
| property_id/property_name | 性质 | 常规款/限定款等 |
| material | 材质成分 | 如"925银、合成立方氧化锆" |
| price_list | 吊牌价 | - |
| price_cost | 成本价 | - |
| is_main_product | 是否主销品 | Y/N |
| is_active | 是否有效 | Y/N |
| created_at | 创建时间 | 来自 Oracle CREATIONDATE |

**源表**：Oracle `M_PRODUCT`, `M_DIM`  
**更新策略**：每日全量覆盖

#### `dim_sku` - SKU维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| sku_id | SKU ID | 主键/最小库存单位 |
| product_id | 商品ID | 对应款号 |
| sku_barcode | SKU条码 | - |
| sku_color | 颜色 | - |
| sku_size | 尺寸 | - |
| is_active | 是否有效 | Y/N |
| created_at | 创建时间 | 来自 Oracle CREATIONDATE |
| updated_at | 更新时间 | ETL运行时间 |

**源表**：Oracle `M_PRODUCT_ALIAS`, `M_ATTRIBUTESETINSTANCE`  
**更新策略**：每日全量覆盖

#### `dim_store` - 店仓维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| store_id | 店仓ID | 主键 |
| store_code | 店仓编码 | 001=总仓, DS%=电商, RT%=门店 |
| store_name | 店仓名称 | - |
| area_id/area_name | 区域 | 维度映射 |
| is_warehouse | 是否仓库 | 1/0 |
| is_store | 是否门店 | 1/0 |
| is_cloud_store | 是否云仓 | Y/N |
| is_center | 是否物流中心 | Y/N |
| store_type | 类型 | 总仓/电商/门店/测试/功能仓 |
| is_active | 是否有效 | Y/N |
| created_at | 创建时间 | ETL运行时间 |

**源表**：Oracle `C_STORE`  
**更新策略**：每日全量覆盖

#### `dim_channel` - 渠道维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| channel_id | 渠道ID | 主键 |
| channel_name | 渠道名称 | Oracle O2O_RETAIL_CHANNEL.NAME |
| channel_code | 渠道编码 | Oracle O2O_RETAIL_CHANNEL.CODE |
| WING_CODE | 对应店仓编码 | Oracle O2O_RETAIL_CHANNEL.WING_CODE |
| is_main | 是否主要渠道 | 1/0 |
| platform_type | 平台类型 | 天猫/京东/抖音/小红书/视频号/唯品会/得物/其他 |
| is_active | 是否有效 | Y/N |
| created_at | 创建时间 | ETL运行时间 |

**源表**：Oracle `O2O_RETAIL_CHANNEL`  
**更新策略**：每日全量覆盖

### 明细层 (DWS)

#### `dws_sales_daily` - 销售明细表
按日期+店仓+SKU粒度统计销售数据

| 字段 | 说明 | 计算逻辑 |
|------|------|----------|
| date_id | 日期 | YYYYMMDD格式 |
| store_id | 店仓ID | - |
| store_code | 店仓编码 | - |
| is_cloud_store | 云仓标识 | Y/N |
| product_id | 商品ID | - |
| m_productalias_id | SKU ID | - |
| sales_qty | 销售数量 | 正单数量 |
| sales_amount | 销售金额 | 正单金额 |
| sales_amount_list | 吊牌金额 | 吊牌金额 |
| return_qty | 退货数量 | 负单数量（绝对值）|
| return_amount | 退货金额 | 负单金额（绝对值）|
| order_count | 订单数 | 仅统计正单 |
| 净销量 | 净销量 | 字段存在但当前ETL不填充，未在代码实现写入（默认0） |
| 净销售额 | 净销售额 | 字段存在但当前ETL不填充，未在代码实现写入（默认0） |
| etl_time | ETL时间 | 写入时间戳 |

说明：净销量/净销售额字段名以 MYSQL 数据字典为准，当前未在代码实现写入。

**源表**：MySQL `ods_m_retail`, `ods_m_retailitem`, `dim_store`  
**更新策略**：`run_etl.py` 主链默认近 7 天回带（与 ODS 默认回刷 7 天对齐）；独立调用 `etl_dws_sales.run()` 时仍保留“凌晨查昨天、白天查今天”的智能模式。来源：[run_etl.py](run_etl.py#L59)；[run_etl.py](run_etl.py#L544)；[etl_dws_sales.py](etl_dws_sales.py#L178)

**代码字段命名对照（审计用）**：

| 字段名 | 含义 | 说明 |
|--------|------|------|
| c_area_id | 门店区域ID | 对应 C_STORE.C_AREA_ID |
| m_dim1_id | 品牌维度ID | 对应 M_PRODUCT.M_DIM1_ID |
| m_attributesetinstance_id | 属性实例ID | 对应 M_PRODUCT_ALIAS.M_ATTRIBUTESETINSTANCE_ID |
| start_time | 任务开始时间 | 脚本内变量，用于计算耗时 |
| end_time | 任务结束时间 | 脚本内变量，用于计算耗时 |

#### `dws_inventory_daily` - 库存明细表
按日期+店仓+SKU粒度记录库存快照

| 字段 | 说明 | 备注 |
|------|------|------|
| date_id | 快照日期 | YYYYMMDD格式 |
| store_id | 店仓ID | - |
| store_code | 店仓编码 | - |
| is_cloud_store | 云仓标识 | Y/N |
| product_id | 商品ID | - |
| m_productalias_id | SKU ID | - |
| qty | 库存数量 | - |
| qty_valid | 可用库存 | 取自 QTY（QTYVALID 未维护） |
| qty_occupy | 占用数量 | 固定填0 |
| qtypurchaserem | 采购欠数 | 在途库存（已下单未入库）|
| etl_time | ETL时间 | 写入时间戳 |

**源表**：Oracle `FA_STORAGE`, `C_STORE`, `M_PRODUCT`  
**更新策略**：每日全量快照

### 应用层 (ADS)

#### `ads_inventory_health` - 库存健康度应用表
每个SKU的库存健康度全方位分析

| 字段分类 | 字段名 | 说明 |
|----------|--------|------|
| **基础信息** | product_id, product_code, product_name | 商品信息 |
| | sku_id, sku_barcode, color, size | SKU信息 |
| | category_id/category_name | 分类属性 |
| | series_id/series_name, property_id/property_name | 分类属性 |
| **库存指标** | total_qty / warehouse_qty / cloud_qty | 总库存/总仓/云仓 |
| | purchase_rem_qty | 采购欠数（在途库存）|
| **销售指标** | sales_qty_7d / sales_qty_30d | 近7天/30天销量 |
| | sales_amt_30d | 近30天销售额 |
| | return_qty_30d / return_amount_30d | 近30天退货量/退货额 |
| | daily_avg_sales / daily_avg_sales_7d | 30天/7天日均销量 |
| | dabo_sales_qty_30d / dabo_sales_qty_7d | 近30天/7天达播销量 |
| | dabo_revenue_30d / dabo_revenue_7d | 近30天/7天达播销售额 |
| | dabo_latest_date | 达播最新日期 |
| | natural_sales_qty_30d / natural_sales_qty_7d | 近30天/7天自然销量 |
| | natural_revenue_30d / natural_revenue_7d | 近30天/7天自然销售额 |
| | natural_daily_avg_sales / natural_daily_avg_sales_7d | 自然日均销量 |
| **周转指标** | turnover_days | 库存周转天数 |
| | suggest_qty | 建议补货数量（可为负）|
| **分级指标** | sku_grade | SABC分级 |
| | sales_rank / sales_ratio / cumulative_ratio | 销售排名/占比/累计占比 |
| | inventory_status / status_priority | 库存状态/优先级 |
| **趋势指标** | sales_velocity / sales_trend | 销售加速度/趋势 |
| | natural_sales_velocity | 自然销售加速度 |
| **时间字段** | snapshot_date / etl_time / created_at | 快照/ETL时间 |

**核心算法**：
```
建议补货数量 = (90天 - 当前周转天数) × 日均销量 - 近30天退货 - 采购欠数
库存周转天数 = 当前库存 / (近30天销售 / 30)
销售加速度 = (近7天日均销量) / (近30天日均销量)
```

**库存状态分级**：
| 状态 | 条件 | 补货优先级 |
|------|------|------------|
| 紧急缺货 | 有销售 且 周转<30天 | 🔴 1级 |
| 需补货 | 有销售 且 30≤周转<70天 | 🟠 2级 |
| 正常 | 有销售 且 70≤周转≤90天 | 🟢 3级 |
| 库存过高 | 有销售 且 周转>90天 | 🔵 4级 |
| 滞销 | 有库存 但 无销售 | ⚪ 5级 |
| 停售 | 无库存 且 无销售 | ⚫ 6级 |

**SABC分级**（基于销售金额累计占比）：
- **S类**（前30%）：超级爆款，最高优先级
- **A类**（30%-70%）：核心款，重点监控
- **B类**（70%-90%）：常规款，正常补货
- **C类**（90%-100%+无销售）：长尾/滞销款

**源表**：MySQL `dws_sales_daily`, `dws_inventory_daily`, `dim_product`, `dim_store`, `dim_sku`  
**更新策略**：每日全量重算

---

## 🔄 定时任务配置

### 方案一：Windows计划任务

```
任务：每日凌晨3点执行
程序：python
参数：C:\Users\tianhao\PycharmProjects\hefang_dw\run_etl.py
起始于：C:\Users\tianhao\PycharmProjects\hefang_dw
```

或使用批处理脚本：
```bash
# 运行 run_scheduled_etl.bat
# 运行 run_scheduled_store_daily_report.bat
```

### 方案二：Linux Crontab

```bash
# 每天凌晨3点执行
0 3 * * * cd /opt/hefang_dw && python run_etl.py >> /var/log/hefang_etl.log 2>&1
```

---

## 🛠️ 数据维护

### 回补历史数据

```bash
# 回补近90天销售数据（示例）
python -c "from etl_dws_sales import backfill; backfill(20251102, 20260130)"

# 重跑指定日期门店经营日报
python etl_ads_store_daily_report.py --report-date 2026-03-23 --data-version v1

# 重算库存健康度
python etl_ads_health.py
```

### 数据质量检查

```bash
# 全面数据质量检查
python tools/check_data.py

# 库存专项检查
python tools/check_dws_inventory.py

# ODS对账（主表/明细）
python tools/check_ods_incremental.py --days 7

# ODS明细质量对账（双通道拆分）
python tools/check_ods_retailitem_quality.py --days 7

# 输出分组（脚本打印标签）
# ods_m_retailitem_all / ods_m_retailitem_online_modifieddate / ods_m_retailitem_offline_settime / ods_m_retailitem_unknown_nulls

# 使用截止时间避免时间漂移
python tools/check_ods_incremental.py --days 7 --as-of "2026-02-26 17:11:52"
python tools/check_ods_retailitem_quality.py --days 7 --as-of "2026-02-26 17:11:52"
```

### 导出应用层数据

```bash
# 导出库存健康度到Excel
python tools/export_ads.py

# 导出文件名前缀（与脚本一致）
# ads_inventory_health_

# 导出文件名示例
# ads_inventory_health-20260120.csv
```

---

## 📚 文档导航

### 核心文档（推荐阅读）

| 文档 | 内容 | 适用人群 |
|------|------|----------|
| [数据仓库与ETL手册](docs/数据仓库与ETL手册.md) | 数仓架构、ETL流程、任务调度 | 数据工程师 |
| [数据结构与映射手册](docs/数据结构与映射手册.md) | 源表结构、字段映射、取数逻辑 | 开发人员 |
| [业务逻辑与指标规范](docs/业务逻辑与指标规范.md) | 指标定义、计算公式、业务规则 | 业务分析师、产品经理 |
| [SQL开发手册](docs/SQL开发手册.md) | SQL模板、开发规范、最佳实践 | SQL开发者 |
| [ETL业务逻辑说明](docs/ETL业务逻辑说明.md) | 每个ETL脚本的人话版逻辑说明 | 所有人员 |

### 扩展文档

- [docs/MYSQL数据字典.md](docs/MYSQL数据字典.md) - MySQL数据字典
- [docs/ETL业务逻辑说明.md](docs/ETL业务逻辑说明.md) - 每个ETL脚本的人话版业务逻辑说明

---

## 🔒 数据治理

### 数据质量规则

| 规则类型 | 检查项 | 阈值 |
|----------|--------|------|
| 完整性 | 主键非空率 | 100% |
| 一致性 | 销售金额 vs 行金额合计差异 | <0.1% |
| 及时性 | 最新数据日期 | T-1日 |
| 准确性 | 库存周转天数异常值 | <0或>1000标记 |

### 字段命名规范

```
1. 表名：{层级}_{主题}_{粒度}
   示例：dws_sales_daily, ads_inventory_health

2. 字段名：小写+下划线
   示例：product_id, sales_qty, turnover_days

3. 日期字段：snapshot_date (YYYYMMDD格式)

4. 数量字段：qty / amount / count
   示例：sales_qty, total_qty, store_count

5. 标识字段：is_xxx / has_xxx
   示例：is_cloud_store, has_sales
```

### 文档同步闭环

- 事实源：`*.py`、`*.sql`、配置为准，文档仅解释事实。
- 先审计再修订：`python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`
- 当要求审计对齐文档时，先询问是否调用快照脚本生成数据库快照（可选执行）。来源：[tools/snapshot_mysql_hefangdw_schema.py](tools/snapshot_mysql_hefangdw_schema.py#L1-L8)；[tools/snapshot_oracle_bosnds3_schema.py](tools/snapshot_oracle_bosnds3_schema.py#L1-L9)
- 以差异清单分批修订，高风险项清零后再合并。
- 高风险定义：表名（ods_/dwd_/dws_/ads_/dim_ 前缀）、入口脚本（run_etl.py / run_ods.py / scheduled_etl.py）、任务键名（如 dws_sales / dws_inventory / ads_health）。
- 三阶段闭环：阶段A仅扫描不改文档；阶段B只改高风险项；阶段C复跑审计并对比差异数量。
- 合并前必须更新 reports/docs_code_alignment.json（与文档修订同步）。
- 证据引用格式示例：来源：[run_etl.py](run_etl.py#L43-L51)
- 详细规范见 [docs/数据仓库与ETL手册.md](docs/数据仓库与ETL手册.md)。

---

## 🤝 团队协作

### 角色分工

| 角色 | 职责 | 联系方式 |
|------|------|----------|
| 数据工程师 | ETL开发、数据仓库维护 | tianxiaoyu911@gmail.com |
| 业务分析师 | 指标定义、报表需求 | tianxiaoyu911@gmail.com |
| 数据库管理员 | 数据库优化、权限管理 | tianxiaoyu911@gmail.com |
| 项目负责人 | 需求评审、优先级排序 | tianxiaoyu911@gmail.com |

### 开发流程

```
1. 需求评审 → 2. 技术方案设计 → 3. 开发测试 → 4. 上线部署 → 5. 监控维护
```

### 代码管理

```bash
# 分支策略
main    - 生产环境
dev     - 开发环境
feature/* - 功能分支
hotfix/* - 紧急修复
```

---

## 📈 版本记录

| 版本 | 日期 | 更新内容 | 维护人 |
|------|------|----------|--------|
| v2.42 | 2026-06-18 | 将负责人共同考核说明更新为“推荐维护 SUBJECT，但同月过渡允许 STORE + SUBJECT 并存且仅告警”，并补记 2026-06-18/19 生效切换验证结论 | GitHub Copilot |
| v2.41 | 2026-06-18 | 纠正 `门店考核归属` 的 `门店ID` 字段语义：业务填写 RT 门店编码，脚本优先按 `store_code` 命中并兼容纯数字 `store_id` | GitHub Copilot |
| v2.40 | 2026-06-18 | 将 `门店考核归属` sheet 更新为必填 `门店ID`，共同考核导入改为优先按 `store_id` 匹配、门店名称仅作辅助校验 | GitHub Copilot |
| v2.39 | 2026-06-08 | 将门店日报与 ads_daily_sales 的商品范围改为固定排除 `147/149/150`，避免新增品类因漏配被排除 | tianxiaoyu911@gmail.com |
| v2.38 | 2026-06-06 | 退役 3 张销售专题 ADS，并将门店销售专题说明收口到当前保留链路 | tianxiaoyu911@gmail.com |
| v2.37 | 2026-04-27 | 将销售专题 ADS 的业务口径统一到 ads_store_daily_report 权威事实，并补记历史验证不覆盖本轮新逻辑 | tianxiaoyu911@gmail.com |
| v2.36 | 2026-04-27 | 新增 hefang_dw 统一数据库连接工厂说明，并补充 MySQL / Oracle 连接池与超时环境变量 | tianxiaoyu911@gmail.com |
| v2.35 | 2026-04-27 | 将门店日报专题调度扩展为完整销售专题 ADS 链，并补充 DWS freshness 规则，支持同日多次总控后刷新专题 ADS | tianxiaoyu911@gmail.com |
| v2.34 | 2026-04-27 | 为门店日报专题调度补充自然日推进兜底，避免目标与负责人都无新增变更时五层 ADS 停在旧 report_date | tianxiaoyu911@gmail.com |
| v2.33 | 2026-04-23 | 将 run_etl.py 的 dws_sales 主链窗口对齐到 ODS 7 天回刷，并补记 2026-04-21/22 的五层重跑与 Oracle→DWS→ADS 复对账结论 | tianxiaoyu911@gmail.com |
| v2.32 | 2026-04-23 | 补记销售专题 SKU 层连带贡献精度要求提升到 DECIMAL(14,2)，并同步 2026-04-22/v2 五层调度实跑结果 | tianxiaoyu911@gmail.com |
| v2.31 | 2026-04-22 | 将负责人快照接入门店日报专题调度，并补记 ads_store_daily_report 的负责人字段与待执行 alter 约束 | tianxiaoyu911@gmail.com |
| v2.30 | 2026-04-22 | 补记负责人映射 NAS 文件内置填写说明页与表头批注，并冻结业务录入口径 | tianxiaoyu911@gmail.com |
| v2.29 | 2026-04-21 | 新增门店经营负责人快照导入链路说明、命令示例与 SCD2 历史维护约束 | tianxiaoyu911@gmail.com |
| v2.28 | 2026-04-17 | 补记销售专题 SKU 层已完成专题调度第五层显式重跑验证，并统一更新五层写库状态 | tianxiaoyu911@gmail.com |
| v2.27 | 2026-04-17 | 将销售专题 SKU 层接入 scheduled_store_daily_report 第五层批量重跑，并同步更新其已正式写库状态与验证边界 | tianxiaoyu911@gmail.com |
| v2.26 | 2026-04-17 | 将销售专题 SKU 层扩展为含 attach_contribution 的二期样板，并补充 ODS 订单级口径与增量 alter 脚本 | tianxiaoyu911@gmail.com |
| v2.25 | 2026-04-17 | 将销售专题 SKU 层扩展为含 sales_mix_pct、rank_no、trend_tag 的二期样板，并补充现网 alter 脚本 | tianxiaoyu911@gmail.com |
| v2.24 | 2026-04-16 | 将销售专题组织日层接入 scheduled_store_daily_report 第四层批量重跑，并补记四层实跑验证结果 | tianxiaoyu911@gmail.com |
| v2.23 | 2026-04-16 | 同步专题调度实跑结论：自动模式命中幂等跳过、显式 rerun 已完成三层 ADS 写库验证，并补记销售专题组织日层的 v2 复验与接链建议 | tianxiaoyu911@gmail.com |
| v2.22 | 2026-04-16 | 将 ads_daily_sales 接入 scheduled_store_daily_report 专题调度代码，并补充三层 ADS 批量重跑与最小单元测试说明 | tianxiaoyu911@gmail.com |
| v2.21 | 2026-04-10 | 将门店经营日报更新为最终经营实体层，并同步主体层改为基于最终结果补主体编码 | tianxiaoyu911@gmail.com |
| v2.22 | 2026-04-23 | 同步销售主题 ADS 改为 report_channel_type 明细口径，并移除 全国/全部 物理汇总行描述 | tianxiaoyu911@gmail.com |
| v2.20 | 2026-04-10 | 新增门店日报统计主体层、共同考核两张配置表与四 sheet 模板说明，并明确专题调度会顺序重跑门店层和主体层 ADS | tianxiaoyu911@gmail.com |
| v2.19 | 2026-04-09 | 将 ads_inventory_health 的达播字段切换为标签主线优先、legacy 回退兜底，并同步主调度说明 | tianxiaoyu911@gmail.com |
| v2.18 | 2026-04-09 | 将 run_etl.py 的 dabo_ready 切换为达播标签主线优先检查，并说明 legacy CSV 仅作为库存健康兼容回填开关 | tianxiaoyu911@gmail.com |
| v2.17 | 2026-04-09 | 为 ads_dabo_order_label 增加 canonical_system_order_id 归一桥接字段，并更新标签装载与查询模板说明 | tianxiaoyu911@gmail.com |
| v2.18 | 2026-04-14 | 补充 NAS 自动鉴权环境变量，并更新门店日报/达播 NAS 工具的自动恢复说明 | tianxiaoyu911@gmail.com |
| v2.15 | 2026-04-08 | 新增门店属性快照登记工具与 pending_apply 台账说明 | tianxiaoyu911@gmail.com |
| v2.14 | 2026-04-08 | 执行门店日报渠道粗分类生成列 DDL，并同步更新现网表结构说明 | tianxiaoyu911@gmail.com |
| v2.16 | 2026-04-08 | 新增门店日报专题调度入口与 Windows 计划任务包装脚本说明 | tianxiaoyu911@gmail.com |
| v2.13 | 2026-04-08 | 调整门店日报渠道模型为细分类真值，并补充 report_channel_type_group 生成列说明 | tianxiaoyu911@gmail.com |
| v2.12 | 2026-04-08 | 新增门店属性只读差异清单工具，用于 April 关口开口前识别四类差异 | tianxiaoyu911@gmail.com |
| v2.11 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充自动选档规则 | tianxiaoyu911@gmail.com |
| v2.10 | 2026-04-08 | 门店日报目标 NAS 导入脚本新增多月份文件下的 --target-month 用法说明 | tianxiaoyu911@gmail.com |
| v2.9 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本与日志表 DDL 的运行说明 | tianxiaoyu911@gmail.com |
| v2.8 | 2026-04-03 | 冻结门店日报目标 NAS 目录与固定文件命名约定 | tianxiaoyu911@gmail.com |
| v2.7 | 2026-04-03 | 明确门店日报目标配置采用 NAS 投递目录加 Python 定时扫描导入的正式路径 | tianxiaoyu911@gmail.com |
| v1.0 | 2026-01-15 | 初始版本，包含DIM/DWS/ADS层 | tianxiaoyu911@gmail.com |
| v1.1 | 2026-01-19 | 新增采购欠数字段，优化建议补货算法 | tianxiaoyu911@gmail.com |
| v1.2 | 2026-01-20 | 文档重构，新增架构说明与使用指南 | tianxiaoyu911@gmail.com |
| v1.3 | 2026-01-30 | SKU维度与SKU粒度同步，销售智能判断与口径统一 | tianxiaoyu911@gmail.com |
| v1.4 | 2026-02-28 | 更新MySQL数据字典文件名引用 | tianxiaoyu911@gmail.com |
| v1.5 | 2026-02-28 | 增加文档同步闭环与审计命令 | tianxiaoyu911@gmail.com |
| v1.6 | 2026-02-28 | 补充审计输出标签与导出文件名 | tianxiaoyu911@gmail.com |
| v1.7 | 2026-02-28 | 补充高风险定义与入口脚本范围 | tianxiaoyu911@gmail.com |
| v1.8 | 2026-02-28 | 同步阶段A/B/C闭环描述 | tianxiaoyu911@gmail.com |
| v1.9 | 2026-02-28 | 增加审计JSON合并门禁 | tianxiaoyu911@gmail.com |
| v2.0 | 2026-02-28 | 补充证据引用格式示例 | tianxiaoyu911@gmail.com |
| v2.1 | 2026-02-28 | 调整导出文件示例与前缀说明 | tianxiaoyu911@gmail.com |
| v2.2 | 2026-02-28 | 标注net字段未在代码实现写入 | tianxiaoyu911@gmail.com |
| v2.3 | 2026-02-28 | 调整净销量/净销售额字段展示说明 | tianxiaoyu911@gmail.com |
| v2.4 | 2026-02-28 | 补充代码字段命名对照表 | tianxiaoyu911@gmail.com |
| v2.5 | 2026-02-28 | 增加审计前询问是否执行快照脚本 | tianxiaoyu911@gmail.com |
| v2.6 | 2026-03-18 | 将 dim_channel 店仓字段重命名为 WING_CODE 并对齐 Oracle 来源 | tianxiaoyu911@gmail.com |

---

## 📞 技术支持

### 常见问题

**Q1: Oracle连接失败？**
- 默认使用 `python-oracledb` thin 模式，无需安装 Instant Client
- 检查 `ORACLE_USER` / `ORACLE_PASSWORD` / `ORACLE_HOST` / `ORACLE_SERVICE` 环境变量
- 确认网络防火墙设置

**Q2: ETL执行失败？**
- 查看`logs/`目录下的日志文件
- 检查源数据库表是否正常
- 验证MySQL数据库权限

**Q3: 数据不一致？**
- 运行`tools/check_data.py`进行质量检查
- 对比源表与目标表记录数
- 查看[ETL业务逻辑说明](docs/ETL业务逻辑说明.md)了解各脚本逻辑

### 联系方式

- **项目仓库**：（内部Git地址）
- **技术文档**：[docs/](docs/)目录
- **问题反馈**：提交Issue或联系维护人员

---

## 📄 License

本项目为广东何方珠宝有限公司内部数据仓库项目，仅供内部使用。

---

<div align="center">

**⭐ 建议优先阅读 [业务逻辑与指标规范](docs/业务逻辑与指标规范.md) 了解核心业务逻辑**

**Made with ❤️ by HEFANG Data Team**

</div>

