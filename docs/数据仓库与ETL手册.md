# 何方珠宝 - 数据仓库与ETL手册

> 数仓建设方案 | ETL同步逻辑 | 调度说明

---

## 📋 目录

1. [数仓架构设计](#一数仓架构设计)
2. [分层与表结构](#二分层与表结构)
3. [ETL同步逻辑](#三etl同步逻辑)
4. [调度与监控](#四调度与监控)

---

## 一、数仓架构设计

### 1.1 整体架构

```
┌──────────────────────────────────────────────────────────┐
│                     数据仓库架构                         │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Oracle生产库（伯俊ERP）                                  │
│       ↓                                                  │
│  Python ETL（每日凌晨执行）                               │
│       ↓                                                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │              MySQL本地数据仓库                     │  │
│  │                                                    │  │
│  │  ┌────┐  ┌────┐  ┌────┐  ┌────┐  ┌────┐          │  │
│  │  │ODS │→ │DIM │→ │DWD │→ │DWS │→ │ADS │          │  │
│  │  │原始│  │维度│  │明细│  │汇总│  │应用│          │  │
│  │  └────┘  └────┘  └────┘  └────┘  └────┘          │  │
│  └────────────────────────────────────────────────────┘  │
│       ↓                                                  │
│  Tableau / FineBI / Excel                                │
│       ↓                                                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │  电商日报 | 库存健康度 | 月度报告 | 进销存看板     │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

### 1.2 分层说明

| 层级 | 名称 | 说明 | 示例表 |
|------|------|------|--------|
| ODS | 原始数据层 | 1:1复制源表（保留） | ods_m_retail, ods_fa_storage |
| DIM | 维度层 | 维度表 | dim_product, dim_product_attr, dim_store, dim_channel, dim_sku, dim_date |
| DWD | 明细事实层 | 清洗后的明细（保留） | 旁路验证：dwd_sales_retail_item, dwd_inventory_storage_snapshot（由 `scheduled_dws_v2_shadow.py` 并行消费，未进 `run_etl.py` 主链） |
| DWS | 汇总事实层 | 按主题汇总 | 生产：dws_sales_daily, dws_inventory_daily；并行验证：dws_sales_daily_v2, dws_inventory_daily_v2（已接独立 shadow 调度与 `scheduled_total_control.py` 非阻断子链，仍不切 ADS） |
| ADS | 应用层 | 面向应用 | ads_inventory_health, ads_dabo_order_label, ads_dabo_daily_sales, ads_dabo_order_retail_bridge（运行层桥接缓存）；ads_daily_report/ads_sales_summary（规划，未在代码实现） |

---

### 1.3 星型模型设计

```
                         ┌───────────────┐
                         │  dim_date     │
                         │  日期维度      │
                         └───────┬───────┘
                                 │
┌───────────────┐       ┌───────┴───────┐       ┌───────────────┐
│  dim_store    │       │ fact_sales    │       │ dim_product   │
│  店仓维度      │◄──────│ 销售事实表    │──────►│ 商品维度       │
└───────────────┘       │               │       └───────────────┘
                        │ • date_id     │
                        │ • store_id    │
                        │ • product_id  │
                        │ • 销量        │
                        │ • 销售额      │
                        │ • 退货量      │
                        │ • 退货额      │
                        └───────────────┘
```

> 注：SKU维度来自 dim_sku（sku_id），用于SKU粒度分析。

---

## 二、分层与表结构

### 2.1 维度表设计

**dim_product（商品维度）**
```sql
CREATE TABLE dim_product (
    product_id      BIGINT PRIMARY KEY,
    product_code    VARCHAR(80),
    product_name    VARCHAR(200),
    category_id     INT,
    category_name   VARCHAR(50),
    property_id     INT,
    property_name   VARCHAR(50),
    series_id       INT,
    series_name     VARCHAR(100),
    brand_id        INT,
    brand_name      VARCHAR(50),
    year_id         INT,
    year_name       VARCHAR(20),
    price_list      DECIMAL(12,2),
    price_cost      DECIMAL(12,2),
    material        TEXT,
    is_main_product CHAR(1),
    is_active       CHAR(1),
    created_at      DATETIME,
    updated_at      DATETIME
);
```

> 注：`material` 来自 Oracle `M_PRODUCT.FABELEMENT`（材质成分）。`year_id`/`year_name` 对应 Oracle `M_DIM2_ID` 维度，当前ETL未填充。

**dim_product_attr（商品属性表）**
```sql
CREATE TABLE dim_product_attr (
    product_id      BIGINT,
    color           TEXT,
    size            TEXT
);
```

**dim_store（店仓维度）**
```sql
CREATE TABLE dim_store (
    store_id        BIGINT PRIMARY KEY,
    store_code      VARCHAR(40),
    store_name      VARCHAR(255),
    area_id         INT,
    area_name       VARCHAR(100),
    is_warehouse    TINYINT,
    is_store        TINYINT,
    is_cloud_store  CHAR(1),
    is_center       CHAR(1),
    store_type      VARCHAR(20),
    is_active       CHAR(1),
    created_at      DATETIME,
    updated_at      DATETIME
);
```

**dim_channel（渠道维度）**
```sql
CREATE TABLE dim_channel (
    channel_id      INT PRIMARY KEY,
    channel_name    VARCHAR(50),
    channel_code    VARCHAR(20),
    WING_CODE       VARCHAR(40),
    is_main         TINYINT,
    platform_type   VARCHAR(20),
    is_active       CHAR(1),
    created_at      DATETIME
);
```

**dim_sku（SKU维度）**
```sql
CREATE TABLE dim_sku (
    sku_id            BIGINT PRIMARY KEY,
    product_id        BIGINT,
    sku_barcode       VARCHAR(80),
    sku_color         VARCHAR(60),
    sku_size          VARCHAR(60),
    is_active         CHAR(1),
    created_at        DATETIME,
    updated_at        DATETIME
);
```

**dim_date（日期维度）**
说明：dim_date 为静态维度表，当前未在代码实现自动生成。
```sql
CREATE TABLE dim_date (
    date_id         INT PRIMARY KEY,
    date_value      DATE,
    date_year       INT,
    date_month      INT,
    date_day        INT,
    date_quarter    INT,
    week_of_year    INT,
    day_of_week     INT,
    day_name_cn     VARCHAR(10),
    month_name_cn   VARCHAR(10),
    is_weekend      TINYINT,
    is_holiday      TINYINT,
    holiday_name    VARCHAR(50),
    year_month      VARCHAR(7),
    created_at      DATETIME
);
```

---

### 2.2 事实表设计

**dws_sales_daily（日销售汇总）**
说明：表结构与索引为SQL/人工建表，当前未在代码实现自动建表/建索引；`net_qty`/`net_amount` 也未在代码实现写入。
```sql
CREATE TABLE dws_sales_daily (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_id         INT,
    store_id        BIGINT,
    product_id      BIGINT,
    m_productalias_id BIGINT,
    sales_qty       INT,
    sales_amount    DECIMAL(14,2),
    sales_amount_list DECIMAL(14,2),
    return_qty      INT,
    return_amount   DECIMAL(14,2),
    net_qty         INT,          -- ⚠️ 当前ETL未填充，MySQL默认0
    net_amount      DECIMAL(14,2),-- ⚠️ 当前ETL未填充，MySQL默认0
    order_count     INT,          -- 仅统计正单(TOT_AMT_ACTUAL>0)的不重复零售单数
    store_code      VARCHAR(32),
    is_cloud_store  CHAR(1),
    created_at      DATETIME,
    updated_at      DATETIME,
    etl_time        DATETIME,
    
    INDEX idx_date (date_id),
    INDEX idx_store (store_id),
    INDEX idx_product (product_id)
);
```

**dws_inventory_daily（日库存快照）**
```sql
CREATE TABLE dws_inventory_daily (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_id         INT,
    store_id        BIGINT,
    store_code      VARCHAR(32),
    is_cloud_store  CHAR(1),
    product_id      BIGINT,
    m_productalias_id BIGINT,
    qty             INT,
    qty_valid       INT,
    qty_occupy      INT,
    qtypurchaserem  BIGINT,
    created_at      DATETIME,
    etl_time        DATETIME,
    
    INDEX idx_date (date_id),
    INDEX idx_store (store_id),
    INDEX idx_product (product_id)
);
```

---

### 2.3 应用表设计

**ads_inventory_health（库存健康度）**

此表在MySQL内基于dws层计算，不从Oracle直接抽取：

```sql
-- 数据来源
库存数据 ← dws_inventory_daily (当天)
销售数据 ← dws_sales_daily (近30天)
达播销量 ← ads_dabo_order_label 最新批次 + ODS/缓存兜底（近30天/近7天）
商品信息 ← dim_product
SKU信息 ← dim_sku
仓库信息 ← dim_store

- 周转天数 = 库存 / (30天销量 / 30)
- 库存状态（滞销/停售/紧急缺货/需补货/正常/库存过高）
- 状态优先级（紧急缺货=1…停售=6）
- SABC分级（按销售额累计占比）
- 销售排名/占比/累计占比（sales_rank/sales_ratio/cumulative_ratio）
- 建议补货 = (90-周转天数)*日均销量 - 退货 - 采购欠数
- 自然销量 = 电商+云仓销量 - 达播销量（用于自然销售加速度与趋势判断）
```

补充说明：`etl_ads_health.py` 现优先使用最新 `ads_dabo_order_label` 批次，在 ODS 内按订单标签识别达播订单并对 `ods_m_retailitem` 汇总 SKU 30/7 天指标；若 ODS 尚无对应订单，则回退 `ads_dabo_order_retail_bridge` 缓存；仅当标签批次不可用时，才回退 `ads_dabo_daily_sales`。来源：[etl_ads_health.py](../etl_ads_health.py#L195)；[etl_ads_health.py](../etl_ads_health.py#L270)；[etl_ads_health.py](../etl_ads_health.py#L672)

**ads_store_daily_report（门店经营日报）**

此表在 MySQL 内基于 ODS + 配置表生成，当前由独立脚本手工触发：

销售部当前正式冻结口径详见 [docs/销售部数据治理-子项目/store_daily_report_sales_rule_freeze.md](销售部数据治理-子项目/store_daily_report_sales_rule_freeze.md)；若与历史设计稿或 Excel 快照冲突，以该冻结稿和当前 ETL 实现为准。

```
-- 数据来源
零售单头/明细 ← ods_m_retail + ods_m_retailitem
商品过滤 ← dim_product（固定排除 147=辅料、149=办公用品、150=道具）
门店范围 ← dim_store + dim_store_report_attr
目标值 ← cfg_store_target_daily
共同考核配置 ← cfg_store_assessment_subject_target_daily + cfg_store_assessment_assignment
负责人切片 ← dim_store_operation_owner_assignment

-- 核心特点
- 1行 = 1个最终经营实体在1天、1个目标版本下的日报宽表
- 命中共同考核配置时，物理门店会在本表直接合并为经营体行
- 按最终经营实体编码 + 实体类型 + 报告日命中负责人历史切片，下沉 `owner_name`
- 按 report_date + data_version 先删后插
- 日/月销售额与销量当前都采用净额/净量口径
- 绝对金额小于 1 的明细整体排除（ABS(ri.tot_amt_actual) >= 1）
```

说明：仓库已提供 `SQL/alter_ads_store_daily_report_add_owner_name.sql`；目标库执行前，`owner_name` 仍属于未实现物理字段，更新后的 `etl_ads_store_daily_report.py` 会在运行前直接提示缺列，不会静默写数。

来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L149)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L370)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L526)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L629)

**ads_store_daily_subject_report（门店经营日报统计主体层）**

此表在 MySQL 内基于最终经营实体层日报 + 共同考核配置生成，当前由独立脚本或专题调度触发：

```
-- 数据来源
最终经营实体层 ← ads_store_daily_report
主体目标 ← cfg_store_assessment_subject_target_daily
主体归属 ← cfg_store_assessment_assignment

-- 核心特点
- 1行 = 1个统计主体在1天、1个目标版本下的日报宽表
- 共同考核只认显式配置，不按商场或城市自动推断
- 不再重复汇总物理门店事实，而是基于最终经营实体结果补主体编码、主店锚点与成员数
```

来源：[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L95)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L139)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L154)

---

## 三、ETL同步逻辑

### 3.1 各表同步策略

| 目标表 | 源表 | 策略 | 说明 |
|--------|------|------|------|
| ods_fa_storage | FA_STORAGE | 全量覆盖（可选） | ODS原始层，独立执行，不影响现有DWS/ADS |
| ods_m_retail | M_RETAIL | 增量为主（默认回刷7天，可全量） | ODS原始层，独立执行，不影响现有DWS/ADS |
| ods_m_retailitem | M_RETAILITEM | 双水位增量（MODIFIEDDATE + SETTIME，可全量） | ODS原始层，独立执行，不影响现有DWS/ADS |
| dim_product | M_PRODUCT + M_DIM + M_ATTRIBUTESETINSTANCE | 全量覆盖 | 商品信息可能改，含材质字段 |
| dim_product_attr | M_ATTRIBUTESETINSTANCE（通过M_PRODUCT_ALIAS关联） | 全量覆盖(replace) | 每个商品取第一个SKU的颜色/尺寸 |
| dim_sku | M_PRODUCT_ALIAS + M_ATTRIBUTESETINSTANCE | 全量覆盖 | SKU信息可能改 |
| dim_store | C_STORE + C_AREA | 全量覆盖 | 门店可能新增 |
| dim_channel | O2O_RETAIL_CHANNEL | 全量覆盖 | 电商渠道与店仓映射 |
| dws_sales_daily | ods_m_retail + ods_m_retailitem + dim_store | 增量（按日期） | `run_etl.py` 主链默认近7天回带，与 ODS 默认回刷7天对齐；独立运行仍保留“凌晨查昨天、白天查今天”的智能模式（全渠道、全品类；业务筛选下沉ADS） |
| dws_inventory_daily | ods_fa_storage + dim_store | 全量快照 | 每日记录当天库存（总仓+云仓，不做主销品类过滤） |
| dws_sales_daily_v2 | dwd_sales_retail_item | dry-run / conn-test / S3 手工写入 + shadow 调度 | 已人工建表；`etl_dws_sales_v2.py` 默认输出源摘要 SQL、候选 `INSERT ... SELECT` SQL、写后摘要和 DWD-v2 对账 SQL；`--conn-test` 只读校验结构；已在用户明确授权下完成一次 S3 实跑验收，`--execute` 需 `WRITE_DWS_SALES_V2`；当前可由 `scheduled_dws_v2_shadow.py` 独立运行，也可作为 `scheduled_total_control.py` 的非阻断 `dws_v2_shadow` 子链观察运行，仍不进入 `run_etl.py` 主链 |
| dws_inventory_daily_v2 | dwd_inventory_storage_snapshot | dry-run / conn-test / S3 手工写入 + shadow 调度 | 已人工建表；`etl_dws_inventory_v2.py` 默认输出源摘要 SQL、候选 `INSERT ... SELECT` SQL、写后摘要和 DWD-v2 对账 SQL；`--conn-test` 只读校验结构；已在用户明确授权下完成一次 S3 实跑验收，`--execute` 需 `WRITE_DWS_INVENTORY_V2`；当前可由 `scheduled_dws_v2_shadow.py` 独立运行，也可作为 `scheduled_total_control.py` 的非阻断 `dws_v2_shadow` 子链观察运行，仍不进入 `run_etl.py` 主链 |
| ads_inventory_health | MySQL内计算 | 重新计算 | 基于dws层，达播字段优先走标签主线 |
| ads_store_daily_report | ODS + 配置表 + 共同考核配置 | 按日期版本覆盖 | 最终经营实体层，独立入口，当前不在 run_etl.py 主链 |
| ads_store_daily_subject_report | 最终经营实体层 + 共同考核配置 | 按日期版本覆盖 | 统计主体层，基于门店层最终结果补主体编码 |
| ads_dabo_order_label | 统一 Excel / NAS | 按 source_file 覆盖 | 默认 dry-run；用户授权后由工具脚本写入 |
| ads_dabo_daily_sales | CSV文件 | 文件驱动 | 监听例行/紧急目录 |
| log_dabo_import | ETL日志 | 追加写入 | 每次导入记录 |

**ODS增量同步规则（阶段二）**
1. 增量条件：主通道按 `MODIFIEDDATE` 回刷窗口，默认回刷 7 天。
2. 双水位策略：
    - 线上通道（`MODIFIEDDATE IS NOT NULL`）按 `MODIFIEDDATE` 增量。
    - 线下通道（`MODIFIEDDATE IS NULL` 且 `SETTIME IS NOT NULL`）按 `SETTIME` 增量。
    - 双空兜底（`MODIFIEDDATE IS NULL` 且 `SETTIME IS NULL`）按头单 `M_RETAIL.MODIFIEDDATE` 落窗回刷，仍归属 `modifieddate` 主通道。
3. 窗口边界：使用半开区间 `>= start_ts` 且 `< end_ts`，避免重叠/漏数。
4. 稳定排序：
    - `MODIFIEDDATE` 通道按 `MODIFIEDDATE, ID` 排序。
    - `SETTIME` 通道按 `SETTIME, ID` 排序。
    - 双空兜底按 `M_RETAIL.MODIFIEDDATE, ID` 排序。
5. 断点续跑：ODS 使用 `ods_sync_state` 记录窗口起止与状态，支持双水位。
    - `ods_m_retailitem` 记录 `MODIFIEDDATE` 主通道水位，并承载双空兜底窗口。
    - `ods_m_retailitem_settime` 记录 `SETTIME` 水位。
6. 性能前提：Oracle 侧需有 `M_RETAIL(MODIFIEDDATE, ID)`、
    `M_RETAILITEM(MODIFIEDDATE, ID)`、`M_RETAILITEM(SETTIME, ID)`、`M_RETAILITEM(M_RETAIL_ID)` 索引。 (DBA无法判断加索引的风险，所以暂时没有实施 建立该索引)

> 运行入口 `run_ods.py` 默认走增量模式并回刷7天，可通过 `--full` 强制全量；自 2026-04-07 起，`--full` 默认会在 `ods_m_retail` / `ods_m_retailitem` 全量结束后按同一个固定 `as-of` 自动补一轮最近 1 天 catch-up，可用 `--full-catchup-days` 调整或设为 `0` 关闭。来源：[run_ods.py](../run_ods.py#L72-L125)；[etl_ods_m_retail.py](../etl_ods_m_retail.py#L91-L151)；[etl_ods_m_retailitem.py](../etl_ods_m_retailitem.py#L134-L206)

### 3.1.1 ODS落表治理提醒

2026-04 对 `ods_m_retail` / `ods_m_retailitem` 的只读诊断确认，ODS 表只完成字段落地和增量水位，不等于治理闭环完成：

1. 源端 `id` 不能直接假设为 MySQL 已落实主键；若历史存在跨 `etl_batch_id` 的重复装载，应先区分业务键与物理候选唯一键，再讨论 `PRIMARY KEY` / `UNIQUE`。
2. 对会被 DWS 或 MCP 直接联表消费的 ODS，必须同步评估查询路径索引，而不是只保留 `modifieddate` / `settime` 这类同步索引。
3. `ods_m_retail` / `ods_m_retailitem` 的经验表明，缺少头表过滤索引和明细连接索引时，约 309 万行明细也会退化成服务端 300+ 秒慢 SQL；补齐路径索引后，同一路径可回到小结果集联表与排序。
4. 新表上线、历史补数或结构调整后，若未完成“主键/唯一键可行性 + 高频查询索引 + EXPLAIN ANALYZE 基线”三项检查，不应视为治理完成。
5. 当前代码层已经落地两层防护：`etl_ods_m_retail.py` / `etl_ods_m_retailitem.py` 会在窗口清理后对当前源 chunk 再按 `id` 替换写入，并在 `run()` 上加 MySQL 命名锁，优先避免重复装载再次发生。
6. 当前数据库层分两种场景：新建环境执行 `SQL/create_ods_tables.sql` 时会直接带 `UNIQUE KEY (id)`；现网历史库则仍需由用户手工执行 `SQL/alter_ods_m_retail_enforce_unique_id.sql` 与 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql`，再用 `tools/check_ods_incremental.py` 复核 `duplicate_id_count`。

来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L46-L64)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L243-L331)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L47-L65)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L293-L423)；[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

---

### 3.2 同步时序

**ODS（已纳入主链，也可独立执行）**
```
任意时间  run_ods.py（默认增量，--full 为全量 + retail/retailitem recent catch-up）
```

```
03:00  ETL开始
03:05  同步dim_product（约3分钟）
03:08  同步dim_sku（约1分钟）
03:10  同步dim_store（约1分钟）
03:11  同步dim_channel（约1分钟）
03:12  同步ODS原始层并执行质检（约5分钟）
03:17  同步dws_sales_daily（主链近7天回带）（约5分钟，已消费ODS）
03:22  同步dws_inventory_daily（约10分钟，已消费ODS）
03:32  达播主线就绪检查（标签主线优先，附带 legacy CSV 状态）
03:34  计算ads_inventory_health（约5分钟）
03:39  ETL结束
06:00  Tableau数据源刷新
实时  达播CSV监听（例行/紧急目录）
```

说明：自 2026-03-23 起，`run_etl.py` 已纳入 ODS 同步步骤；`dws_sales_daily` 与 `dws_inventory_daily` 均已切换为消费 ODS，当前主链剩余的 Oracle 直连主要在 DIM 层。自 2026-04-23 起，主链进一步把 `dws_sales_daily` 的窗口与 ODS 默认回刷 7 天对齐：先执行 `run_ods_sync(backfill_days=7, qc_days=7)`，再执行 `etl_dws_sales.run(days_back=7, include_today=True)`，用于承接 ODS 晚到补数。来源：[run_etl.py](../run_etl.py#L59)；[run_etl.py](../run_etl.py#L526)；[run_etl.py](../run_etl.py#L544)；[run_etl.py](../run_etl.py#L570)
当前 `dabo_ready` 会优先检查 `ads_dabo_order_label` 最新批次是否存在且最近 1 天有更新，并继续上报 `ads_dabo_daily_sales` 的 legacy 状态；`ads_inventory_health` 在主调度中会优先使用标签主线，若标签批次未就绪但 legacy 当日可用则回退 legacy，否则达播字段按 0 处理。来源：[run_etl.py](../run_etl.py#L583)；[run_etl.py](../run_etl.py#L649)
`ads_store_daily_report` 当前保持独立入口，按配置表确认完成后手工执行 `python etl_ads_store_daily_report.py --report-date YYYY-MM-DD --data-version v1`；若要产出共同考核统计主体层，再追加执行 `python etl_ads_store_daily_subject_report.py --report-date YYYY-MM-DD --data-version v1`。

---

### 3.3 增量同步逻辑

**销售数据增量：**
```python
# 智能判断：凌晨查昨天，白天查今天（可通过 days_back 回溯）
current_time = datetime.now()
if include_today:
    end_dt = current_time - timedelta(days=1) if current_time.hour < 6 else current_time
else:
    end_dt = current_time - timedelta(days=1)

start_dt = end_dt - timedelta(days=days_back-1)
start_date = int(start_dt.strftime('%Y%m%d'))
end_date = int(end_dt.strftime('%Y%m%d'))

# 先删后插
mysql.execute(f"DELETE FROM dws_sales_daily WHERE date_id >= {start_date} AND date_id <= {end_date}")
df = oracle.query(sales_sql.format(start_date=start_date, end_date=end_date))
# 关键过滤：
# - 只取SKU（M_PRODUCTALIAS_ID IS NOT NULL）
# - 业务筛选在ADS层完成（如电商/云仓、主销品类）
mysql.to_sql(df, 'dws_sales_daily', if_exists='append')
```

**补数逻辑：**
```python
# 如果需要补历史数据
def backfill(start_date, end_date):
    mysql.execute(f"DELETE FROM dws_sales_daily WHERE date_id >= {start_date} AND date_id <= {end_date}")
    df = oracle.query(sales_sql.format(start=start_date, end=end_date))
    mysql.to_sql(df, 'dws_sales_daily', if_exists='append')
```

---

### 3.4 库存快照逻辑

```python
# 每天记录当天库存状态
today = datetime.now().strftime('%Y%m%d')

# 删除今天旧数据（如果重跑）
mysql.execute(f"DELETE FROM dws_inventory_daily WHERE date_id = {today}")

# 抽取当前库存
df = mysql.query("""
    SELECT 
        fs.c_store_id AS store_id,
        COALESCE(s.store_code, '') AS store_code,
        COALESCE(s.is_cloud_store, 'N') AS is_cloud_store,
        fs.m_product_id AS product_id,
        fs.m_productalias_id AS m_productalias_id,
        fs.qty AS qty,
        fs.qty AS qty_valid,
        COALESCE(fs.qtypurchaserem, 0) AS qtypurchaserem
    FROM ods_fa_storage fs
        LEFT JOIN dim_store s ON fs.c_store_id = s.store_id
        WHERE fs.isactive = 'Y'
            AND fs.m_productalias_id IS NOT NULL
            AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
        -- 库存快照不做主销品过滤，口径在ADS层统一控制
""")
df['date_id'] = int(today)
df['qty_occupy'] = 0

# 写入
mysql.to_sql(df, 'dws_inventory_daily', if_exists='append')
```

---

### 3.5 达播标签 / CSV 同步逻辑

> 说明：旧达播聚合 ETL 仍独立在外部项目维护；本仓库当前已新增统一 Excel 内部主线，优先解决“订单打标”，不把 Excel 金额字段兼容当成第一优先级。

```
内部标签主线：读取 订单管理*.xlsx → 行级筛选 → 生成订单标签 → dry-run / apply 到 ads_dabo_order_label
旧兼容主线：监听例行/紧急目录 → 读取CSV → 字段校验 → 清洗 → 按发货日期+SKU聚合 → 写入 ads_dabo_daily_sales
```

当前内部标签主线的约束：
- 主桥接键固定为 `system_order_id`
- `platform_order_id` 仅作辅助追溯
- `COALESCE(ads_dabo_order_label.canonical_system_order_id, ads_dabo_order_label.system_order_id) = ods_m_retail.oms_sourcecode` 是 ODS 内部优先桥接路径
- `system_order_id` 永远保留 Excel 原值；只有精确未命中且同文件内存在唯一已命中 superset 候选的异常组合单，才会补 `canonical_system_order_id`
- 生意额、退款等指标统一在 ODS / SQL 层按标签筛选计算

**为什么要每日快照**：
- 库存是状态数据，今天的库存明天就变了
- 快照可以分析库存趋势
- 可以追溯历史某天的库存

---

### 3.6 应用表计算逻辑

ads_inventory_health不从Oracle抽，而是在MySQL内计算：

```sql
-- 数据来源
库存数据 ← dws_inventory_daily (当天)
销售数据 ← dws_sales_daily (近30天)
达播销量 ← ads_dabo_daily_sales (近30天/近7天)
商品信息 ← dim_product
SKU信息 ← dim_sku
仓库信息 ← dim_store

-- 计算步骤
1. 汇总库存（总仓+云仓）
2. 汇总销售（近30天/近7天）
3. 计算周转天数
4. 判断库存状态
5. 计算ABC分级
6. 生成建议补货
7. 计算自然销量与自然加速度（电商+云仓销量 - 达播销量）
8. 标签主线就绪时记录 dabo_ready 状态；仅在 legacy CSV 当日可用时回填当日达播/自然字段
```

门店经营日报 `ads_store_daily_report` 也不从 Oracle 直接出最终宽表，而是先落 ODS，再在 MySQL 内按配置表收口：

```sql
-- 数据来源
ods_m_retail / ods_m_retailitem
dim_product / dim_store
dim_store_report_attr / cfg_store_target_daily

-- 计算步骤
1. 先校验当日门店配置是否存在生效区间重叠，并校验负责人切片是否唯一有效
2. 按 report_date + data_version 删除旧结果
3. 汇总日销售 / 月累计 / 上月同期 / 去年同期
4. 精确匹配日目标与月目标
5. 计算达成率、同比、排名和时间进度
6. 校验输出行数与有效门店数一致
```

说明：正式交付时，目标 Excel 将先投递到 NAS 目录 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，并按月份分文件管理，当前推荐命名规则已切换为 `YYYYMM考核数据配置表.xlsx`；导入脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`，再由 `tools/import_cfg_store_target_daily_from_nas.py` 扫描并导入 `cfg_store_target_daily`；`etl_ads_store_daily_report.py` 只消费已入库结果，不直接读取 NAS 文件。当前仓库已落盘该导入脚本；现网已于 2026-04-03 完成 `log_store_target_import` 建表、首轮 `--apply` 写库与专项消费验证。若本轮还要同步门店属性，新环境/现网首次启用前还需由用户人工执行 `SQL/create_store_report_attr_snapshot.sql` 建立 `cfg_store_report_attr_snapshot`。若 NAS 目录同时存在多个月份文件，需显式追加 `--target-month YYYY-MM` 选择本次导入月份；若同月同时存在多个版本文件，则需改用 `--file-path` 显式指定。若模板显式提供 `门店类型` 列，可追加 `--sync-store-report-attr`；脚本会先覆盖 `cfg_store_report_attr_snapshot`，再同步刷新 `dim_store_report_attr`；当前 `report_channel_type_group` 已通过 2026-04-08 执行的生成列 DDL 生效到现网。若工作簿同时提供 `统计主体目标` 与 `门店考核归属` 两张可选 sheet，导入脚本会在同一事务中同步刷新 `cfg_store_assessment_subject_target_daily` 与 `cfg_store_assessment_assignment`。若 Windows 因 DNS 调整、凭证清理或任务上下文变化导致 UNC 会话失效，导入链路会先读取 `HEFANG_NAS_USERNAME` / `HEFANG_NAS_PASSWORD` 自动重建 `\\192.168.0.151\hefang总部` 连接；未配置或配置错误时直接失败。

目标导入工具约束：
- 默认工作表：`导入模板`
- 默认模式：dry-run，不写库
- 共同考核 sheet：`统计主体目标` 与 `门店考核归属` 必须同时提供；若两张都存在但均无有效数据，表示清空当月共同考核配置
- 版本一致性：若 `统计主体目标` 或 `门店考核归属` 的 `目标月份`、`目标版本` 与 `导入模板` 不一致，脚本直接失败，不做静默兼容
- 多月份文件：若同一文件同时存在多个 `目标月份`，必须传 `--target-month YYYY-MM`，未传时脚本直接失败并提示可选月份
- 正式写库：按目标月份 + 目标版本先删后插 `cfg_store_target_daily`
- 共同考核写库：若可选 sheet 存在，则同事务刷新 `cfg_store_assessment_subject_target_daily` 与 `cfg_store_assessment_assignment`
- 门店属性同步：追加 `--sync-store-report-attr` 后，脚本会先按 `target_month + target_version` 覆盖 `cfg_store_report_attr_snapshot`，再按 `store_id` 将当前有效 `dim_store_report_attr` 记录分类为未变化 / 变更 / 新增 / 退出；未变化不动，变更执行关旧开新，新增只开新，退出只关旧
- 门店类型约束：启用门店属性同步时，模板必须显式提供 `门店类型` 列，当前支持 `小程序 / 线上小程序 / 直营 / 直营-奥莱 / 联营 / 联营-免税 / 联营-奥莱`
- 生效开始日：默认沿用目标月内现有最新 `effective_start_date`，目标月无现存版本时回退到月首，也可用 `--attr-effective-start-date` 显式覆盖
- 重叠保护：当前只会在 `cfg_store_report_attr_snapshot` 同月同版本出现重复 `store_id`，或 `dim_store_report_attr` 在所选生效日对同一门店命中多条当前有效记录时直接失败；仅因上一版历史仍有效不再单独拦截
- 导入日志：仅 `--apply` 写入 `log_store_target_import`
- 匹配失败：若 `store_name` 未命中 `dim_store`，脚本直接失败并输出候选门店建议

负责人快照导入约束：
- 默认文件：`\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx`
- 默认工作表：`门店负责人映射表`；兼容 `门店负责人映射模板`
- 模板说明：正式 NAS 文件内置 `填写说明` sheet 与数据表头批注，用于冻结业务录入口径；导入脚本忽略说明 sheet，只读取数据 sheet
- 必填表头：`门店编码`、`门店名称`、`负责人`；`备注` 可选
- 快照定位：业务侧只维护“当前真值”，不维护 Excel 生效区间；脚本按 `snapshot_date` 从 `dim_store_report_attr` 当前有效门店叠加共同考核配置，推导当日应维护的经营实体清单
- 实体粒度：独立门店维护 `STORE`；共同考核经营体维护 `SUBJECT`
- 共同考核约束：若经营体已存在，负责人快照中只允许保留 `SUBJECT` 行；被吸收成员店若仍出现在 Excel 中，会被识别为 `unexpected_entities` 并阻断 `--apply`
- 正式写库：`--apply` 时按快照日先删后插 `cfg_store_operation_owner_snapshot`，再维护 `dim_store_operation_owner_assignment` 的 SCD2 历史切片
- 历史去重：若新快照与前一版历史切片完全一致，则直接重开旧版本，不新增重复切片
- 导入日志：仅 `--apply` 写入 `log_store_operation_owner_import`
- 建表前置：若快照表、历史表或日志表缺失，脚本直接失败并提示先执行 `SQL/create_store_operation_owner_tables.sql`
- 调度接入：当前负责人快照链路已接入 `scheduled_store_daily_report.py`；自动模式会在目标导入之后执行负责人导入，并按 `file_md5 + snapshot_date` 做独立幂等判重
- 受影响日期：只有 `history_diff_counts.changed/new/exited > 0` 时，负责人链路才会新增受影响日期；日期起点按 `max(owner_snapshot_date, target_month_start)` 截断，避免用当前目标版本回刷目标月之外的数据
- 调度开关：若只想跑目标链路，可在专题调度追加 `--no-run-owner-import`

来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L27)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L29)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L39)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L223)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L256)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L320)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L364)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L403)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L437)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L584)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L775)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L830)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L876)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L917)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L997)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L1)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L23)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L48)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L16)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L66)

门店日报专题调度约束：
- 当前已新增 `scheduled_store_daily_report.py` 作为门店日报正式专题调度包装层，对应 Windows 触发脚本为 `run_scheduled_store_daily_report.bat`
- 自动模式会先选择 NAS 目录中最后修改的目标文件，并在解析后校验 `target_month` 是否等于当前月份；若不是当前月份，则本轮记录跳过。若需处理历史或未来月份，必须显式传入 `--target-month` 或 `--file-path`
- 默认会同步刷新 `dim_store_report_attr`；若只需导入 `cfg_store_target_daily`，可在手工执行时追加 `--no-sync-store-report-attr`
- 调度前会读取 `log_store_target_import`，若相同 `file_md5 + target_month + target_version` 已存在 `SUCCESS` 记录，则本次直接跳过，不重复写库
- 当前专题调度已负责 NAS 目标导入、负责人快照导入、门店属性同步、共同考核配置同步、受影响日期判断，以及按日期列表顺序批量重跑 `ads_store_daily_report`、`ads_store_daily_subject_report` 与 `ads_daily_sales`
- 自动模式新增 `--auto-report-date-mode previous-day|current-day`：默认 `previous-day` 按前一天生成最终版；若需在 22:30 之类的当日批次产出同日临时快照，可改用 `current-day`，把受影响日期统一上界扩到运行当天。显式 `--rerun-report-date` 模式不受影响。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L345)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2652)
- 自 2026-04-27 起，`ads_daily_sales` 的门店范围、商品范围、共同考核目标与净单口径统一复用 `ads_store_daily_report` 权威事实；专题调度只负责按该统一口径重跑，不再保留旧版销售主题分叉规则。来源：[../etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L118)
- 当前专题调度在进入主循环前会先申请顶层命名锁 `hefang_dw:scheduled_store_daily_report`；若已有另一条专题调度在跑，本次立即退出，不再重复触发同一批日期
- 负责人导入发生在目标导入之后；若负责人日志表中已存在相同 `file_md5 + snapshot_date` 的最近一次 `SUCCESS` 记录，则本次负责人链路直接跳过，不重复写库
- 若命中 `--conn-test` 或当前月份门禁跳过，则不产生新的受影响日期集合，也不触发批量重跑
- 若目标链路命中 `file_md5 + target_month + target_version` 幂等跳过，且负责人链路也没有 `changed/new/exited` 变更，专题调度会继续按当前 `data_version` 检查三张保留 ADS 的 `report_date` 覆盖是否已补到统一上界；若仍存在缺口，则自动按缺口日期触发批量重跑。若有任一 ADS 在当月完全缺失，则直接按“目标月首日到统一上界”整段补跑；若日期已覆盖，则比较近 7 天 `dws_sales_daily.etl_time` 与专题 ADS `etl_time`，源 DWS 更新更晚时按 freshness 日期重跑，支持同一天多次总控刷新专题 ADS。代码常量为 `DWS_SALES_FRESHNESS_LOOKBACK_DAYS = 7`，结构化分支规则键为 `dws_sales_daily_etl_time_newer_than_ads`，摘要字段为 `source_freshness_branch`
- 若目标文件仅包含“部分门店尚未在 `dim_store` 建立”的问题，导入工具现会把这些门店记录为 `WARNING` 并跳过对应的目标/门店属性/共同考核归属配置；专题调度与总控会同步输出 `WARNING` 摘要并继续重跑其他门店的三张保留 ADS。若全部门店都未命中，或共同考核归属会因未命中而被整体清空，则仍立即失败，避免空覆盖当月配置
- 若失败属于模板校验类问题，例如共同考核 sheet 缺失配对、目标月份/目标版本不一致、缺少 `门店类型` 列，专题调度会直接输出原始错误并停止，不进入重试等待
- 若正式 IMPORTED 后受影响日期非空，默认自动按“门店层 -> 主体层 -> 销售看板月度战役”顺序触发批量重跑；若只想保留日期判断结果，可追加 `--no-run-affected-ads`
- 若只想临时关闭负责人链路，可追加 `--no-run-owner-import`；若需覆盖默认负责人文件或快照日，可追加 `--owner-file-path`、`--owner-sheet-name`、`--owner-snapshot-date`
- 当前三张保留 ADS 的批量重跑、显式日期续跑与 DWS freshness 分支已在 `test_scheduled_store_daily_report.py` 中覆盖。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L101)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L450)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L984)；[../test_scheduled_store_daily_report.py](../test_scheduled_store_daily_report.py#L81)；[../test_scheduled_store_daily_report.py](../test_scheduled_store_daily_report.py#L127)
- 若需要手工补跑指定日期，可使用 `python scheduled_store_daily_report.py --rerun-report-date YYYY-MM-DD --rerun-report-date YYYY-MM-DD --rerun-data-version v1`
- 达播侧 `tools/extract_dabo_order_candidates_from_nas.py` 与 `tools/load_dabo_order_labels_from_nas.py` 复用同一套 NAS 自动鉴权逻辑，统一读取 `HEFANG_NAS_USERNAME` / `HEFANG_NAS_PASSWORD`
---

## 四、调度与监控

### 4.1 调度方式

当前生产调度建议保留三层生产入口，并额外提供一个 DWS v2 shadow 观察入口，而不是把销售专题或 v2 直接并入 `run_etl.py`：

1. `run_etl.py`：通用数仓主链，负责 dim / ods / dws / dabo_ready / ads_health。
2. `scheduled_store_daily_report.py`：销售专题链，负责 NAS 目标导入、门店属性同步、负责人快照、受影响日期判断、DWS freshness 判定和三张保留 ADS 重跑。
3. `scheduled_total_control.py`：总控包装。默认 / `shadow_compare` 仍先跑 `scheduled_etl.py`，主链返回 `0` 后才继续触发 `scheduled_store_daily_report.py` 与后置 `scheduled_dws_v2_shadow.py`；有效模式为 `v2` 时，会先执行阻断型 `DWS v2 读源预刷新`，再跑主链和销售专题，避免生产 `ads_inventory_health` 在新日期读到空 `_v2` 源。若额外传入 `--topic-report-date-mode current-day`，只会向销售专题链透传同名 report_date 上界模式，用于同日临时快照；主链与 shadow 仍保持原有日期语义。来源：[../scheduled_total_control.py](../scheduled_total_control.py#L135)；[../scheduled_total_control.py](../scheduled_total_control.py#L716)
4. `scheduled_dws_v2_shadow.py`：DWS v2 独立 shadow / 前置刷新入口，串联 raw ODS → DWD → `_v2`；在总控 V2 前置刷新场景会追加 `--skip-ads-shadow-validation`，仅刷新读源，不在主链 ADS 重算前做持久化 ADS compare。

这样做的目的，是让“Windows 计划任务入口合一”和“主链 / 专题链业务边界独立”同时成立：

- 对计划任务运维来说，日常生产仍可只配置 `run_scheduled_total_control.bat` 一个入口；若需单独验证 v2，再补 `run_scheduled_dws_v2_shadow.bat`。
- 对代码职责来说，不需要把 NAS 文件依赖、模板校验失败、负责人快照异常等专题问题，硬塞进 `run_etl.py` 的通用主链失败域。
- 对告警出口来说，主链、专题链与 shadow 子链可以在总控模式下输出结构化摘要，再由 `scheduled_total_control.py` 统一汇总发送一条企业微信摘要，后续新增专题也能按同一协议接入。

连接层同样保持统一工厂，不回退到各脚本分散直连：`db_connections.py` 负责统一连接池与超时策略，调用方按任务时长选择 `default`、`etl`、`long_running` 三档 MySQL 超时。当前保留专题任务均按自身负载选择对应档位，避免为了少数长 SQL 把全局读写超时一并抬高。

**方式一：Windows任务计划程序**
```batch
@echo off
cd /d C:\Users\tianhao\PycharmProjects\hefang_dw
call run_scheduled_etl.bat
```

推荐在当前 `00:05` 与 `12:30` 这两个时点，把计划任务入口从 `run_scheduled_etl.bat` 调整为 `run_scheduled_total_control.bat`；若业务需要在 `22:30` 额外产出“当天临时快照”，可继续复用同一入口并透传 `--cutover-mode v2 --topic-report-date-mode current-day`：

```batch
@echo off
cd /d C:\Users\tianhao\PycharmProjects\hefang_dw
call run_scheduled_total_control.bat
```

```batch
@echo off
cd /d C:\Users\tianhao\PycharmProjects\hefang_dw
call run_scheduled_total_control.bat --cutover-mode v2 --topic-report-date-mode current-day
```

**方式二：run_scheduled_etl.bat**
- 时间：每天凌晨3:00
- 日志：logs/etl_YYYYMMDD.log
- 入口链路：`run_scheduled_etl.bat` -> `scheduled_etl.py` -> `run_etl.py`
- 说明：`run_etl.py` 为统一执行入口，负责重试与执行摘要输出；直接单独运行时仍会发送主链企业微信摘要，但在总控模式下会改为把结构化摘要交给 `scheduled_total_control.py` 统一发送。

**方式二点五：run_scheduled_total_control.bat**
- 时间：建议直接复用当前主链既有时点，例如 `00:05` 与 `12:30`；若要补同日临时快照，可额外增加 `22:30`
- 日志：`logs/scheduled_total_control_YYYYMMDD.log`
- 入口链路：`run_scheduled_total_control.bat` -> `scheduled_total_control.py`；默认 / `shadow_compare` 为主链成功后再串销售专题与后置 shadow，`v2` 模式为先刷新 DWS v2 读源，再跑主链与销售专题
- 说明：总控包装负责串联链路、注入子链摘要输出路径，并统一汇总主链、销售专题链与 DWS v2 读源刷新 / shadow 的执行摘要；若主链失败，则直接短路，不继续触发销售专题链与 shadow 子链；若 V2 前置刷新失败，则主链和专题链都会跳过，避免 ADS 读空 `_v2` 源；若销售专题失败，非 V2 后置 shadow 仍继续执行；若后置 shadow 失败，仅记 `WARNING`，不影响旧 DWS / ADS。若需要同日临时快照，可透传 `--topic-report-date-mode current-day`；不传则沿用专题默认 `previous-day`，继续生成前一天最终版。来源：[../run_scheduled_total_control.bat](../run_scheduled_total_control.bat#L12)；[../scheduled_total_control.py](../scheduled_total_control.py#L135)；[../scheduled_total_control.py](../scheduled_total_control.py#L716)

**方式二点六：run_scheduled_dws_v2_shadow.bat**
- 时间：仅在需要独立 shadow 验证时单独配置
- 入口链路：`run_scheduled_dws_v2_shadow.bat` -> `scheduled_dws_v2_shadow.py`
- 说明：适合单独验证 raw ODS → DWD → DWS v2 shadow；仍不修改 `run_etl.py` 主链，也不切 ADS 读源。可先用 `python scheduled_dws_v2_shadow.py --conn-test` 做只读预检。

**方式三：run_scheduled_store_daily_report.bat**
- 时间：建议与业务投递 NAS 文件的时间错开，按月目标文件更新频率独立调度
- 日志：`logs/store_daily_report_schedule_YYYYMMDD.log`
- 入口链路：`run_scheduled_store_daily_report.bat` -> `scheduled_store_daily_report.py` -> `tools/import_cfg_store_target_daily_from_nas.py`
- 说明：专题调度会先做日志表检查与目标文件解析，再按 `file_md5 + target_month + target_version` 判重；只有检测到新文件内容时才执行正式写库。直接单独运行时沿用自身企业微信告警，若由总控触发，则会抑制子链单独告警并把结构化摘要回传给总控统一发送。若需在 SQL 修复后显式重跑指定日期，可直接通过包装脚本透传参数，例如：`run_scheduled_store_daily_report.bat --rerun-report-date 2026-06-07 --rerun-data-version v1`。

---

### 4.2 异常处理

| 异常 | 处理 |
|------|------|
| Oracle连接失败 | 按重试策略执行，达到上限或不可重试时发送企业微信摘要 |
| MySQL写入失败 | 回滚，记录日志 |
| 数据量异常（差>10%） | 在验证环节提示，人工确认 |
| ETL执行完成（成功/失败） | 发送统一摘要（步骤状态、耗时、关键指标） |

---

### 4.3 监控检查

**每日检查项：**
- [ ] ETL是否成功完成
- [ ] 企业微信是否收到当日执行摘要（成功或失败）
- [ ] 各表记录数是否正常
- [ ] 昨日销售额是否有数据
- [ ] 库存快照是否生成

**快速检查SQL：**
```sql
-- 查看最新数据日期
SELECT 'dws_sales_daily' AS tbl, MAX(date_id) AS latest 
FROM dws_sales_daily
UNION ALL
SELECT 'dws_inventory_daily', MAX(date_id) 
FROM dws_inventory_daily
UNION ALL
SELECT 'ads_inventory_health', MAX(snapshot_date) 
FROM ads_inventory_health;

-- 查看昨日销售额
SELECT date_id, SUM(sales_amount) AS 销售额
FROM dws_sales_daily
WHERE date_id = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), '%Y%m%d')
GROUP BY date_id;
```

---

### 4.4 数据质量检查

**验证方法：**

1. **数据源验证**
   - 总库存 vs ERP系统（差异<1%）
   - 销售额 vs 各渠道后台（差异<3%）

2. **抽样验证**
   - 仓库抽查10个SKU实物库存
   - 商品部确认滞销商品清单
   - 运营确认TOP10热销排名

3. **签字确认**
   - 商品部：类别筛选、滞销判断
   - 仓库：库存数量
   - 运营：销售数据、周转阈值

---

### 4.5 常见问题排查

**问题1：ETL执行失败**
```bash
# 查看日志
tail -f logs/etl_20260120.log

# 手动重跑
python run_etl.py
```

**问题2：数据量异常**
```sql
-- 检查记录数变化
SELECT 
    DATE(etl_time) AS 日期,
    COUNT(*) AS 记录数
FROM dws_inventory_daily
GROUP BY DATE(etl_time)
ORDER BY 日期 DESC
LIMIT 7;
```

**问题3：数据对不上**
- 检查筛选条件（ISACTIVE='Y', STATUS=2）
- 检查仓库口径（总仓+云仓）
- 检查类别筛选（主销品ID列表）
- 检查日期范围

---

### 4.6 ETL配置文件

**config.py示例：**
```python
# Oracle配置
ORACLE_CONFIG = {
    'user': 'username',
    'password': 'password',
    'host': '192.168.1.100',
    'port': 1521,
    'service_name': 'ORCL'
}

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'dba',
    'password': 'password',
    'database': 'hefang_dw',
    'charset': 'utf8mb4'
}

# ETL重试配置（环境变量）
# 说明：run_etl.py 会优先读取环境变量，未设置时回落到 config.py 默认值
ETL_MAX_RETRIES = 3   # 最大重试次数
ETL_RETRY_SLEEP = 60  # 重试间隔秒数
```

---

### 4.7 文档同步闭环（SOP）

**定义：文档是代码的派生物（Single Source of Truth）**
规则：Python/SQL/配置是事实源；md 只能解释事实，不能自创事实。

**目标：** 避免“看起来完整”而编造字段、流程、表关系。

**闭环流程（6步）：**
1. **事实源约束**：以 `*.py`、`*.sql`、配置为准，文档只能解释代码中的事实。
2. **重构前审计**：生成“差异清单”（机器可读 JSON）。
    - 产出字段：`docs_only`、`code_only`、`intersection`、`risk_level`。
    - 输出路径：`reports/docs_code_alignment.json`。
    - 审计命令：`python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。
    - 降噪规则：审计脚本会自过滤审计元术语（如 advisories/non_blocking 标记词），避免将脚本自身新增输出词计入 `code_only` 噪音。
3. **任务拆分为三阶段**：
    - 阶段A：仅扫描，不改文档。输出差异清单 + 风险分级（高/中/低）。
    - 阶段B：只改高风险项（表名、字段名、核心计算逻辑、任务入口）。
    - 阶段C：回归复扫，确认差异数量下降；否则继续下一轮。
4. **Agent 改文档约束**（每轮固定提示）：
    - 仅允许修改 `docs/*.md` 与 `README.md`。
    - 每条修改必须附“来源代码文件 + 行号”。
    - 禁止补充无法从代码证明的描述。
    - 若 MySQL 表结构存在字段，则 `DATA_CONTRACTS` 的“关键字段”必须包含；且当 ETL 未写入时，含义中必须注明“字段存在但当前ETL不填充”。
    - 输出“未能确认项”（不得猜测补全）。
    - 每轮最多改 N 条（例如 20 条）。
5. **文档验收门禁（可验收条件）**：
    - 高风险差异项为 0。
    - 标记为 `reason=field_exists_but_not_filled` 的项仅作提醒，不计入阶段B/阶段C阻断条件。
    - 关键对象（任务名、表名、入口脚本）与代码一致。
    - 本轮新增代码若无文档映射，阻断合并。
6. **标准化 SOP**：
    - 开发改代码 → 运行审计 → 按清单分批改文档 → 人工复核高风险 → 复跑审计 → 合并。

**任务模板（中文，可直接用）：**
你现在是文档同步审计员。
第一步只读取 `reports/docs_code_alignment.json`，不要改文档。
输出：高/中/低风险差异清单（含原因）。

第二步仅修复高风险项，且只允许修改 `README.md` 和 `docs/*.md`。
每条修改必须给出“来源代码文件+行号”。
禁止补充无法从代码证明的信息。

第三步给出本轮修改摘要 + 未确认项列表。
最后提示复跑审计命令并比较前后差异数量。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v3.53 | 2026-06-08 | 补记 `run_scheduled_store_daily_report.bat` 已支持参数透传，可直接转发显式重跑日期给 `scheduled_store_daily_report.py` |
| v3.52 | 2026-06-08 | 将门店日报与 ads_daily_sales 的商品范围改为固定排除 `147/149/150`，不再依赖 `dim_report_product_rule` 维护 active 集合 |
| v3.51 | 2026-05-19 | 补记 `ods_m_retailitem` 为 `modifieddate/settime` 双空明细新增“按头单 `M_RETAIL.MODIFIEDDATE` 落窗回刷”的增量兜底，并补充 `M_RETAILITEM(M_RETAIL_ID)` 连接索引前提 |
| v3.50 | 2026-05-18 | 补记销售专题 `auto-report-date-mode` 与总控 `topic-report-date-mode`，支持 22:30 同日临时快照且不影响 00:05 默认最终版 |
| v3.49 | 2026-05-13 | 补记总控 V2 模式的 DWS v2 读源前置刷新顺序，明确前置刷新失败时跳过主链以保护 ADS 写表 |
| v3.48 | 2026-05-07 | 补记 `scheduled_dws_v2_shadow.py` / `run_scheduled_dws_v2_shadow.bat` 与总控第三子链已落地；shadow 仅作非阻断观察，不进入 `run_etl.py` 主链 |
| v3.48 | 2026-06-06 | 退役 3 张销售专题 ADS，并将门店销售专题调度手册收口到当前保留链路 |
| v3.47 | 2026-05-07 | 补记 DWS v2 已完成一次 S3 实跑验收，仍保持并行验证资产状态，不接总控 |
| v3.46 | 2026-05-07 | 补记 DWS v2 S3 手工写入分支已新增：默认 dry-run，写入需确认令牌、命名锁、事务和 DWD-v2 对账；本轮未接总控 |
| v3.45 | 2026-05-07 | 补记 DWS v2 并行表已进入 dry-run / conn-test 脚本阶段但无写库入口，仍未接总控 |
| v3.44 | 2026-05-06 | 将门店未命中 dim_store 的专题调度行为更新为 WARNING + 跳过坏门店，并补记全量未命中时的安全失败阀 |
| v3.43 | 2026-04-27 | 补记 ads_daily_sales、ads_sku_daily、ads_sales_org_daily 已统一复用 ads_store_daily_report 权威口径 |
| v3.42 | 2026-04-27 | 将门店日报专题调度更新为六层 ADS，并补充 DWS freshness 规则，支持同日多次总控后刷新专题 ADS |
| v3.41 | 2026-04-27 | 补记 scheduled_total_control.py 已统一汇总主链与门店销售专题链的企业微信摘要，并预留结构化摘要协议供后续专题接入 |
| v3.40 | 2026-04-27 | 为门店日报专题调度补充自然日推进兜底，避免目标与负责人都无新增变更时五层 ADS 停在旧 report_date |
| v3.39 | 2026-04-24 | 新增 scheduled_total_control.py 与 run_scheduled_total_control.bat，统一承接 00:05 与 12:30 的主链后置销售专题调度 |
| v3.38 | 2026-04-23 | 补记门店日报专题调度新增单实例锁，并明确销售主题 ADS 命名锁现为事务后显式释放 |
| v3.37 | 2026-04-23 | 补记 run_etl.py 已将 dws_sales 主链窗口固定为近7天回带，并同步主链与专题复跑结论 |
| v3.36 | 2026-04-23 | 补记 ads_sku_daily 连带贡献精度要求提升到 DECIMAL(14,2)，并同步 2026-04-22/v2 五层调度结果 |
| v3.35 | 2026-04-22 | 补记负责人快照已接入专题调度，并新增 ads_store_daily_report 负责人字段与待执行 alter 说明 |
| v3.34 | 2026-04-22 | 补记负责人映射 NAS 文件内置填写说明页与表头批注，明确说明页不参与导入 |
| v3.33 | 2026-04-21 | 新增门店经营负责人快照导入约束，并注明当前真值快照与库内 SCD2 维护边界 |
| v3.32 | 2026-04-17 | 补记 ads_sku_daily 已完成专题调度第五层显式重跑验证，并更新五层写库结果 |
| v3.31 | 2026-04-17 | 将 ads_sku_daily 接入门店日报专题调度第五层，并补记当前仅完成代码接链与单元测试验证 |
| v3.30 | 2026-04-16 | 补记 ads_sales_org_monthly 与 ads_sku_daily 已完成仓库样板落包并保持独立入口 |
| v3.29 | 2026-04-16 | 将 ads_sales_org_daily 接入门店日报专题调度第四层，并补记四层实跑验证结果 |
| v3.28 | 2026-04-16 | 修正门店日报目标 NAS 根目录，并同步专题调度自动跳过与显式 rerun 写库验证状态 |
| v3.27 | 2026-04-16 | 将 ads_daily_sales 纳入门店日报专题调度三层批量重跑，并注明当前仅完成单元测试验证 |
| v3.26 | 2026-04-15 | 新增销售部门店经营日报正式冻结稿入口，明确现网解释优先级 |
| v3.25 | 2026-04-15 | 补充专题调度对共同考核 sheet 月份/版本不一致等模板校验失败的立即失败与不重试说明 |
| v3.24 | 2026-04-10 | 将 ads_store_daily_report 更新为最终经营实体层，并同步主体层改为基于最终结果补主体编码 |
| v3.23 | 2026-04-10 | 更新门店日报目标 NAS 命名约定为 YYYYMM考核数据配置表.xlsx，并注明导入脚本兼容历史旧文件名 |
| v3.22 | 2026-04-10 | 新增门店日报统计主体层与共同考核多 sheet 导入说明，并同步专题调度双层重跑语义 |
| v3.21 | 2026-04-09 | 将 ads_inventory_health 的达播来源更新为标签主线优先、legacy 回退兜底，并同步主调度行为 |
| v3.21 | 2026-05-06 | 将门店属性同步改为先写 cfg_store_report_attr_snapshot 再承接 dim_store_report_attr，并补充首次启用建表前置 |
| v3.20 | 2026-04-09 | 更新 run_etl.py 的 dabo_ready 为达播标签主线优先检查，并明确 legacy CSV 仅用于 ads_inventory_health 兼容回填 |
| v3.19 | 2026-04-09 | 为 ads_dabo_order_label 增加 canonical_system_order_id 归一桥接说明，并将 ODS 优先桥接路径更新为 canonical 优先 |
| v3.18 | 2026-04-09 | 明确专题调度只自动处理当前月份快照，并将门店属性同步语义更新为未变化/变更/新增/退出分类 |
| v3.17 | 2026-04-08 | 新增门店日报专题调度入口与 Windows 包装脚本说明，并补充按 MD5 判重规则 |
| v3.16 | 2026-04-08 | 更新门店日报渠道粗分类生成列为现网已执行状态，并补充细分类支持范围 |
| v3.17 | 2026-04-14 | 补充 NAS 自动鉴权环境变量与门店日报/达播 NAS 链路自动恢复说明 |
| v3.15 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充目录选档规则 |
| v3.14 | 2026-04-08 | 补充门店日报目标导入在多月份文件下需显式传入 --target-month 的运行约束 |
| v3.13 | 2026-04-07 | 补充 run_ods --full 默认追加固定 as-of recent catch-up 的运行行为 |
| v3.12 | 2026-04-03 | 补充门店日报目标导入支持同步 dim_store_report_attr 的约束与默认生效日策略 |
| v3.11 | 2026-04-03 | 更新门店日报目标 NAS 导入说明为现网已建表、已首轮 apply、已完成专项消费验证 |
| v3.10 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本与日志表 DDL 说明 |
| v3.9 | 2026-04-03 | 冻结门店日报目标 NAS 目录与固定文件命名约定 |
| v3.8 | 2026-04-03 | 明确门店日报目标配置采用 NAS 投递目录加 Python 定时扫描导入的正式路径 |
| v2.3 | 2026-02-27 | 更新ETL配置说明与排查建议 |
| v2.4 | 2026-02-28 | 新增文档同步闭环（SOP） |
| v2.5 | 2026-02-28 | 补充文档审计命令与统一输出路径 |
| v2.6 | 2026-02-28 | 明确DWD/ADS示例实现范围 |
| v3.8 | 2026-04-08 | 新增 ads_dabo_order_label 与统一 Excel 订单标签主线说明 |
| v2.7 | 2026-02-28 | 标注dim_date未在代码实现自动生成 |
| v2.8 | 2026-02-28 | 标注dws_sales_daily未在代码实现自动建表与net字段写入 |
| v2.9 | 2026-03-02 | 补充审计规则：结构字段入契约与未填充标注 |
| v3.0 | 2026-03-02 | 增加未填充字段提醒项 non-blocking 规则 |
| v3.1 | 2026-03-02 | 增加审计元术语自过滤降噪说明 |
| v3.2 | 2026-03-02 | 增加仅过滤审计脚本内部函数名策略 |
| v3.3 | 2026-03-18 | 新增 dim_channel 维度设计与主流水线时序 |
| v3.4 | 2026-03-31 | 补充 ads_dabo_order_retail_bridge 运行层桥接缓存说明 |
| v3.5 | 2026-04-02 | 补充 ODS 落表治理提醒，明确主键可行性与查询路径索引必须同步评审 |
| v3.6 | 2026-04-02 | 补充 ODS 重复装载代码治理、fresh install 唯一键与现网手工治理步骤 |
| v3.7 | 2026-04-03 | 新增门店经营日报独立入口、应用表设计与运行说明 |
