# DATA_CONTRACTS.md — 何方珠宝数据仓库数据契约

> **定义**：数据契约描述每张表的数据生产者与消费者之间的约定，包括粒度、主键、增量水位、质量点（DQ）和关键指标口径。
>
> 当任意表结构、粒度、水位逻辑发生变更时，必须同步更新本文件。
>
> 最后更新：2026-06-06（专题链路收口对齐）

---

## 契约总则

1. ADS 层 MySQL 表已被 Tableau 和其他下游直接消费，既有字段名属于外部消费契约。
2. 若后续由影子链替代旧链，允许新增 ADS 字段，但不得改名或删除既有字段。
3. 若未来确需调整既有 ADS 字段名，必须先完成消费层迁移、文档同步，并由用户明确确认后再执行。
4. 当前生产默认仍是 `legacy`；`run_etl.py` 仅对 `ads_inventory_health` 暴露显式 `legacy / shadow_compare / v2` cutover 契约，其中 `shadow_compare` 不改变生产写数表，只补 `_v2` 报告型对账证据。来源：[cutover_controls.py](../cutover_controls.py#L29)；[run_etl.py](../run_etl.py#L767)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)

---

## 目录

- [ODS 层](#ods-层)
  - [ods_fa_storage](#ods_fa_storage)
  - [ods_m_retail](#ods_m_retail)
  - [ods_m_retailitem](#ods_m_retailitem)
- [DIM 层](#dim-层)
  - [dim_product](#dim_product)
  - [dim_sku](#dim_sku)
  - [dim_store](#dim_store)
  - [dim_channel](#dim_channel)
- [CFG / 配置层](#cfg--配置层)
  - [dim_store_report_attr](#dim_store_report_attr)
  - [dim_report_product_rule](#dim_report_product_rule)
  - [cfg_store_target_daily](#cfg_store_target_daily)
  - [cfg_store_assessment_subject_target_daily](#cfg_store_assessment_subject_target_daily)
  - [cfg_store_assessment_assignment](#cfg_store_assessment_assignment)
- [DWS 层](#dws-层)
  - [dws_sales_daily](#dws_sales_daily)
  - [dws_inventory_daily](#dws_inventory_daily)
- [ADS 层](#ads-层)
  - [ads_daily_sales](#ads_daily_sales)
  - [ads_store_daily_report](#ads_store_daily_report)
  - [ads_store_daily_subject_report](#ads_store_daily_subject_report)
  - [ads_inventory_health](#ads_inventory_health)
  - [ads_dabo_daily_sales](#ads_dabo_daily_sales)
  - [ads_dabo_order_label](#ads_dabo_order_label)
  - [ads_dabo_order_retail_bridge](#ads_dabo_order_retail_bridge)
- [指标口径速查](#指标口径速查)

---

## ODS 层

ODS（Operational Data Store）是 Oracle ERP 到 MySQL 数仓的原始镜像层。字段名与 Oracle 源表保持一致（小写化）。

---

### ods_fa_storage

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `FA_STORAGE`（库存主档）|
| **生产脚本** | `etl_ods_fa_storage.py` |
| **粒度** | 1行 = 1个 SKU（`M_PRODUCTALIAS_ID`）在1个店仓（`C_STORE_ID`）的库存记录 |
| **更新策略** | 全量覆盖（每日或按需触发）|
| **主键** | `(m_productalias_id, c_store_id)` |
| **水位字段** | 无（全量覆盖，不使用增量水位）|
| **ETL时间戳** | `etl_loaded_at`（每次写入时更新）|

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `id` | BIGINT | 库存记录 ID | PK 组成 |
| `m_productalias_id` | BIGINT | SKU ID（条码维度）| NOT NULL |
| `m_product_id` | BIGINT | 商品 ID（SPU 维度）| NOT NULL |
| `c_store_id` | BIGINT | 店仓 ID | NOT NULL |
| `qty` | DECIMAL | 当前库存数量 | ≥ 0 |
| `qtyvalid` | DECIMAL | Oracle 原始可用库存字段 | 可为空 |
| `qtypurchaserem` | DECIMAL | 采购欠数/在途 | ≥ 0 |
| `isactive` | VARCHAR(1) | 是否有效（'Y'/'N'）| 仅 'Y' 参与计算 |
| `etl_loaded_at` | DATETIME | ETL 写入时间 | NOT NULL |

**DQ 规则**：
- `isactive = 'Y'` 且 `qty IS NOT NULL` 的记录才参与下游计算
- 总仓：`c_store_id` 对应 `C_STORE.CODE = '001'`
- 云仓：`c_store_id` 对应 `C_STORE.IS_ALLO2OSTORAGE = 'Y'`

---

### ods_m_retail

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `M_RETAIL`（零售单据头）|
| **生产脚本** | `etl_ods_m_retail.py` |
| **粒度** | 1行 = 1张零售单（单据头）|
| **更新策略** | 增量（基于 `modifieddate`，回刷 `backfill_days` 默认7天，窗口 `window_days` 默认1天；窗口内先按时间范围清理，再对当前源分块按 `id` 替换写入，可切全量）来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L46-L64)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L243-L270) |
| **主键/业务键** | `id`（Oracle 源业务键；新建环境 DDL 已定义 `UNIQUE KEY uk_ods_m_retail_id (id)`，现网历史库需先执行去重治理 SQL 后再视为已落实唯一约束）来源：[SQL/create_ods_tables.sql](SQL/create_ods_tables.sql#L30)；[SQL/alter_ods_m_retail_enforce_unique_id.sql](SQL/alter_ods_m_retail_enforce_unique_id.sql#L4-L40) |
| **增量水位** | `modifieddate`（源字段，UPDATE 时更新）|
| **水位存储** | MySQL 元数据表 `ods_sync_state`（`table_name=ods_m_retail`，含窗口断点）来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L71-L135) |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `id` | BIGINT | 单据 ID（Oracle 主键）| 业务键映射，NOT NULL |
| `docno` | VARCHAR | 单据编号 |  |
| `billdate` | INT | 单据日期（业务日期）| 用于日报日期分区 |
| `c_store_id` | BIGINT | 所属店仓 | NOT NULL |
| `oms_sourcecode` | VARCHAR(512) | 外部来源订单号 | 达播主订单桥接键 |
| `tot_amt_actual` | DECIMAL | 实际成交金额 |  |
| `tot_amt_list` | DECIMAL | 吊牌金额 |  |
| `tot_qty` | DECIMAL | 单据总数量 |  |
| `status` | INT | 单据状态 |  |
| `isactive` | CHAR(1) | 是否有效（'Y'/'N'）|  |
| `modifieddate` | DATETIME | 最后修改时间 | 增量依赖字段 |
| `etl_batch_id` | BIGINT | ETL 批次号 | NOT NULL |
| `etl_loaded_at` | DATETIME | ETL 写入时间 | NOT NULL |

说明：字段以 MySQL 结构快照为准，并与抽取 SQL 保持一致。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L147-L280)

**DQ/处理规则**：
- 全量模式先写入 `modifieddate` 为空记录（按 `id` 排序）。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L229-L236)
- 增量模式先按 `modifieddate` 窗口清理，再对当前 chunk 按源 `id` 删除旧行并写入，避免同一业务 `id` 因时间窗漂移残留旧副本。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L243-L270)
- `run()` 使用 MySQL `GET_LOCK` 串行化单表同步，并对可重试锁冲突做最多 3 次重试。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L293-L331)
- `oms_sourcecode` 可与 `ads_dabo_order_bridge.main_order_id` 直接桥接，在 MySQL ODS 内按 `billdate` 复用销售/退货口径汇总达播日实收。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py)；[tools/query_data.py](tools/query_data.py)
- `tools/check_ods_incremental.py` 会额外输出 `duplicate_id_count`，可用于唯一键治理前后的重复行复核。来源：[tools/check_ods_incremental.py](tools/check_ods_incremental.py#L58-L62)；[tools/check_ods_incremental.py](tools/check_ods_incremental.py#L141-L159)
- 治理提醒：ODS 落表不能只校验字段与水位；若后续直接承接 MCP / 联表查询，必须同步评估主键/唯一键可行性和头表过滤索引。`dws_sales` 当前直接依赖 `billdate`、`c_store_id`、`status`、`isactive` 过滤 `ods_m_retail`。来源：[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

---

### ods_m_retailitem

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `M_RETAILITEM`（零售单据明细）|
| **生产脚本** | `etl_ods_m_retailitem.py` |
| **粒度** | 1行 = 1个单据明细行（1个 SKU 在1张单中的销售记录）|
| **更新策略** | 双水位增量（`modifieddate` + `settime`），回刷 `backfill_days` 默认7天，窗口 `window_days` 默认1天；两个通道都会在窗口清理后对当前源分块按 `id` 替换写入，可切全量。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L47-L65)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L293-L354) |
| **主键/业务键** | `id`（Oracle 源业务键；新建环境 DDL 已定义 `UNIQUE KEY uk_ods_m_retailitem_id (id)`，现网历史库需先执行去重治理 SQL 后再视为已落实唯一约束）来源：[SQL/create_ods_tables.sql](SQL/create_ods_tables.sql#L48)；[SQL/alter_ods_m_retailitem_enforce_unique_id.sql](SQL/alter_ods_m_retailitem_enforce_unique_id.sql#L4-L39) |
| **增量水位（线上）** | `modifieddate`（联合单据头 `M_RETAIL.MODIFIEDDATE`）|
| **增量水位（线下）** | `settime`（单据明细中的 `SETTIME` 字段）|
| **水位存储** | MySQL 元数据表 `ods_sync_state`，`table_name` 使用 `ods_m_retailitem`（modifieddate）与 `ods_m_retailitem_settime`（settime），含窗口断点。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L72-L136) |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `id` | BIGINT | 明细行 ID | 业务键映射，NOT NULL |
| `m_retail_id` | BIGINT | 关联单据头 ID | FK → ods_m_retail.id |
| `m_productalias_id` | BIGINT | SKU ID | NOT NULL |
| `m_product_id` | BIGINT | 商品 ID | NOT NULL |
| `qty` | DECIMAL | 销售数量（负数为退货）| 负数表退货 |
| `pricelist` | DECIMAL | 吊牌价 |  |
| `priceactual` | DECIMAL | 实际成交单价 |  |
| `tot_amt_actual` | DECIMAL | 实际成交金额 |  |
| `tot_amt_list` | DECIMAL | 吊牌金额 |  |
| `modifieddate` | DATETIME | 线上水位字段 | 线上增量依赖 |
| `settime` | DATETIME | 线下水位字段 | 线下增量依赖 |
| `etl_batch_id` | VARCHAR | ETL 批次号 | NOT NULL |
| `etl_loaded_at` | DATETIME | ETL 写入时间 | NOT NULL |

说明：字段以 MySQL 结构快照为准，并与抽取 SQL 保持一致。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L159-L371)

**DQ/处理规则**：
- 正向销售：`qty > 0`，退货：`qty < 0`（在 DWS 层分别聚合）
- 全量模式先写入 `modifieddate` 为空记录（按 `id` 排序）。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L273-L280)
- 增量模式会在 `modifieddate` 与 `settime` 两条通道中分别先按窗口清理，再对当前 chunk 按源 `id` 删除旧行并写入，避免跨窗口重复装载。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L293-L354)
- `run()` 使用 MySQL `GET_LOCK` 串行化单表同步，并对可重试锁冲突做最多 3 次重试。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L385-L423)
- `tools/check_ods_incremental.py` 会额外输出 `duplicate_id_count`，可用于唯一键治理前后的重复行复核。来源：[tools/check_ods_incremental.py](tools/check_ods_incremental.py#L58-L62)；[tools/check_ods_incremental.py](tools/check_ods_incremental.py#L141-L159)
- 治理提醒：`ods_m_retailitem` 不能只保留 `modifieddate` / `settime` 这类同步索引；若用于销售联表或 MCP 高频查询，还必须同步评估 `m_retail_id` 连接索引与 `m_productalias_id` 过滤能力。来源：[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

---

## DIM 层

DIM（Dimension）层是每日全量刷新的维度表。直接从 Oracle 拉取，不依赖 ODS。

---

### dim_product

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `M_PRODUCT`（商品主档）|
| **生产脚本** | `etl_dim_product.py` |
| **粒度** | 1行 = 1个商品（SPU 粒度，`m_product_id`）|
| **更新策略** | 每日全量覆盖（TRUNCATE + INSERT）|
| **主键** | `m_product_id` |
| **水位字段** | 无（全量）|

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `product_id` | BIGINT | 商品 ID（SPU）|
| `product_code` | VARCHAR | 商品编号 |
| `product_name` | VARCHAR | 商品名称 |
| `category_id` | INT | 品类 ID |
| `category_name` | VARCHAR | 品类名称 |
| `property_id` | INT | 款性质 ID |
| `property_name` | VARCHAR | 款性质名称 |
| `series_id` | INT | 系列 ID |
| `series_name` | VARCHAR | 系列名称 |
| `brand_id` | INT | 品牌 ID |
| `brand_name` | VARCHAR | 品牌名称 |
| `year_id` | INT | 年份 ID（字段存在但当前ETL不填充） | 
| `year_name` | VARCHAR | 年份名称（字段存在但当前ETL不填充） |
| `price_list` | DECIMAL | 吊牌价 |
| `price_cost` | DECIMAL | 成本价 |
| `material` | TEXT | 材质 |
| `is_main_product` | CHAR(1) | 是否主销品（Y/N） |
| `is_active` | CHAR(1) | 是否有效（Y/N） |
| `created_at` | DATETIME | 创建时间（源端） |
| `updated_at` | DATETIME | 更新时间 |

说明：字段以 MySQL 结构快照为准。`is_main_product` 由 `MAIN_CATEGORY_IDS` 计算，`is_active` 取自 Oracle。年份相关字段与 `updated_at` 字段存在于表结构，但当前 ETL 未写入。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_dim_product.py](etl_dim_product.py#L27-L82)

**DQ/处理规则**：
- 仅抽取 `p.ISACTIVE = 'Y'` 的商品。来源：[etl_dim_product.py](etl_dim_product.py#L73-L82)
- `is_main_product = 'Y'` 当 `category_id IN MAIN_CATEGORY_IDS`。来源：[etl_dim_product.py](etl_dim_product.py#L27-L47)

---

### dim_sku

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `M_PRODUCTALIAS`（SKU/条码档）|
| **生产脚本** | `etl_dim_sku.py` |
| **粒度** | 1行 = 1个 SKU（`m_productalias_id`，对应一个条码）|
| **更新策略** | 每日全量覆盖 |
| **主键** | `m_productalias_id` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `sku_id` | BIGINT | SKU ID（条码维度）|
| `product_id` | BIGINT | 所属商品 ID（SPU）|
| `sku_barcode` | VARCHAR | 条码（EAN/内部码）|
| `sku_color` | VARCHAR | 颜色 |
| `sku_size` | VARCHAR | 尺码/规格 |
| `is_active` | CHAR(1) | 是否有效（Y/N）|
| `created_at` | DATETIME | 记录创建时间（源端）|
| `updated_at` | DATETIME | ETL 更新时间 |

说明：dim_sku 字段以 MySQL 结构快照为准，并与 ETL 输出字段保持一致。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_dim_sku.py](etl_dim_sku.py#L32-L45)

---

### dim_store

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `C_STORE`（店仓档案）|
| **生产脚本** | `etl_dim_store.py` |
| **粒度** | 1行 = 1个店铺或仓库 |
| **更新策略** | 每日全量覆盖 |
| **主键** | `store_id` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `store_id` | BIGINT | 店仓 ID |
| `store_code` | VARCHAR | 店仓编号（总仓='001'） |
| `store_name` | VARCHAR | 店仓名称 |
| `area_name` | VARCHAR | 区域名称 |
| `is_cloud_store` | CHAR(1) | 是否云仓（Oracle `IS_ALLO2OSTORAGE`） |
| `store_type` | VARCHAR | 按店仓编码派生的类型 |
| `is_active` | CHAR(1) | 是否有效（Oracle `ISACTIVE`） |
| `open_date` | DATE | 门店开业日期；Oracle `C_STORE.OPENDATE` 经安全日期转换，源值为空或非法时统一为 NULL |
| `created_at` | DATETIME | ETL 写入时间 |

说明：`dim_store` 当前已调整为全量抽取 Oracle `C_STORE`，不再在维表层过滤 `ISACTIVE='N'` 的停用/闭店门店；`is_active` 仅保留状态标识，是否纳入具体业务口径由下游 ETL 或配置表自行判断。`open_date` 只落维表，不冗余写入 ADS；目标表缺列时 ETL 在全量清空前失败，要求用户先人工执行 `SQL/alter_dim_store_add_open_date.sql`。来源：[etl_dim_store.py](../etl_dim_store.py#L23-L119)

---

### dim_channel

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `O2O_RETAIL_CHANNEL`（电商渠道档案） |
| **生产脚本** | `etl_dim_channel.py` |
| **粒度** | 1行 = 1个电商渠道 |
| **更新策略** | 每日全量覆盖 |
| **主键** | `channel_id` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `channel_id` | INT | 渠道ID（Oracle `ID`） |
| `channel_name` | VARCHAR | 渠道名称（Oracle `NAME`） |
| `channel_code` | VARCHAR | 渠道档案编码（Oracle `CODE`） |
| `WING_CODE` | VARCHAR | 渠道挂接码（Oracle `WING_CODE`） |
| `is_main` | TINYINT | 是否主要渠道（文档定义主渠道ID集映射） |
| `platform_type` | VARCHAR | 平台类型（按渠道名称派生） |
| `is_active` | CHAR(1) | 是否有效（Oracle `ISACTIVE`） |
| `created_at` | DATETIME | ETL 写入时间 |

说明：`dim_channel` 的目标结构已在 MySQL 快照中存在，且已于 2026-03-23 实查 Oracle `O2O_RETAIL_CHANNEL` 与 MySQL `dim_channel`，确认两边均为 87 条记录、`WING_CODE` 全部非空，说明目标库现存数据已完成真实回填。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json#L720-L790)；[reports/snapshot_oracle_bosnds3_schema.json](reports/snapshot_oracle_bosnds3_schema.json#L9870-L9925)；[etl_dim_channel.py](etl_dim_channel.py#L1-L131)

**DQ/处理规则**：
- `is_main = 1` 当 `channel_id` 在文档定义的主要渠道 ID 集 `{11, 19, 28, 57, 60, 85, 300}`。来源：[etl_dim_channel.py](etl_dim_channel.py#L22-L51)
- `WING_CODE` 直接映射 Oracle `WING_CODE`；按 2026-03-23 实查结果，当前源表与目标表中的该字段均为非空，但现网值以 Oracle 原始短码为准，不应在测试中再硬编码假设 `DS001` 必然存在。来源：[etl_dim_channel.py](etl_dim_channel.py#L27-L45)
- `platform_type` 按渠道名称归类为天猫/京东/抖音/小红书/视频号/唯品会/得物/其他。来源：[etl_dim_channel.py](etl_dim_channel.py#L33-L43)

---

## CFG / 配置层

CFG / 配置层用于承接门店日报主题的门店纳入范围、商品纳入范围和目标版本配置。当前由 MySQL 侧人工维护，ETL 只读消费。

---

### dim_store_report_attr

| 属性 | 值 |
|------|-----|
| **来源** | MySQL 业务配置表（人工维护） |
| **生产方式** | 手工维护；正式扩范围时可通过 `tools/import_cfg_store_target_daily_from_nas.py --sync-store-report-attr` 基于 NAS 模板的 `门店类型` 列同步导入 |
| **粒度** | 1行 = 1个店仓在 1 个生效起始日下的日报业务属性版本 |
| **更新策略** | 按生效日期增删改 |
| **主键** | `id` |
| **唯一键** | `(store_id, effective_start_date)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `store_id` | BIGINT | 店仓 ID，对应 `dim_store.store_id` | NOT NULL |
| `store_code` | VARCHAR(40) | 店仓编码 | 应与 `dim_store` 保持一致 |
| `store_name` | VARCHAR(255) | 店仓名称 |  |
| `report_channel_type` | VARCHAR(20) | 日报渠道最终真值（如 `线上小程序`、`直营-奥莱`、`联营-免税` 等） | NOT NULL |
| `report_channel_type_group` | VARCHAR(20) | 日报渠道粗分类（由 `report_channel_type` 派生） | STORED 生成列；已于 2026-04-08 执行 DDL |
| `store_grade` | VARCHAR(20) | 店铺等级 | 可为空 |
| `is_duty_free` | CHAR(1) | 是否免税（Y/N） | 默认 N |
| `is_include_in_daily_report` | CHAR(1) | 是否纳入日报（Y/N） | 仅 Y 参与计算 |
| `effective_start_date` | DATE | 生效开始日 | NOT NULL |
| `effective_end_date` | DATE | 生效结束日 | 默认 `9999-12-31` |

**DQ 规则**：
- 同一 `report_date` 下，不允许同一 `store_id` 命中多条有效配置；`etl_ads_store_daily_report.py` 运行前会做重叠校验。
- `is_include_in_daily_report = 'Y'` 的有效店仓数决定当日 `ads_store_daily_report` 的期望输出行数。
- 若使用 `tools/import_cfg_store_target_daily_from_nas.py --sync-store-report-attr`，模板必须显式提供 `门店类型` 列；脚本会把该列原值直接写入 `report_channel_type`，并同步派生 `report_channel_type_group`；`门店类型` 为免税时 `is_duty_free` 判为 `Y`，其余门店一律判为 `N`，不再沿用历史有效属性中的旧值，也不再按门店名称兜底；脚本默认沿用目标月内现有最新 `effective_start_date`，目标月无现存版本时回退到月份首日，并在写库前检查是否存在其他不同起始日的有效配置重叠。来源：[../tools/import_cfg_store_target_daily_from_nas.py](../tools/import_cfg_store_target_daily_from_nas.py#L1711-L1719)
- `report_channel_type_group` 的表内生成列 DDL 已通过 `SQL/alter_dim_store_report_attr_add_channel_type_group.sql` 执行到现网；当前 `dim_store_report_attr` 已同时包含细分类真值列 `report_channel_type` 与派生粗分类列 `report_channel_type_group`。
- 结构证据：`reports/snapshot_mysql_hefangdw_schema.json` 已于 2026-04-08 14:21:42 刷新快照，确认 `dim_store_report_attr.report_channel_type_group` 为 `STORED GENERATED`。

---

### dim_report_product_rule

| 属性 | 值 |
|------|-----|
| **来源** | MySQL 业务配置表（人工维护） |
| **生产方式** | 手工维护 / Excel 导入 |
| **粒度** | 1行 = 1个商品类别在 1 个生效起始日下的日报纳入口径版本 |
| **更新策略** | 按生效日期增删改 |
| **主键** | `id` |
| **唯一键** | `(category_id, effective_start_date)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `category_id` | INT | 商品类别 ID，对应 `dim_product.category_id` | NOT NULL |
| `category_name` | VARCHAR(100) | 商品类别名称 |  |
| `include_in_store_daily_report` | CHAR(1) | 是否纳入门店日报（Y/N） | 仅 Y 参与计算 |
| `rule_note` | VARCHAR(500) | 规则说明 | 可为空 |
| `effective_start_date` | DATE | 生效开始日 | NOT NULL |
| `effective_end_date` | DATE | 生效结束日 | 默认 `9999-12-31` |

**DQ 规则**：
- 当前 `ads_store_daily_report` 与 `ads_daily_sales` 已不再直接消费该表；它保留为历史配置与人工分析参考。
- 若后续业务需要调整“固定排除类目”集合，必须同步修改 `etl_ads_store_daily_report.py`、`etl_ads_daily_sales.py` 与 `SQL/check_ads_daily_sales_min.sql`，并按影响日期人工重跑结果表。

---

### cfg_store_target_daily

| 属性 | 值 |
|------|-----|
| **来源** | MySQL 业务配置表（以入库结果为准） |
| **生产方式** | 正式交付：业务按月份投递 `YYYYMM考核数据配置表.xlsx` 到 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，再由 `tools/import_cfg_store_target_daily_from_nas.py` 扫描导入；脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。现网已于 2026-04-03 完成日志表建表、首轮 `--apply` 写库与 `2026-03-23 / v1` 专项消费验证，新环境首次写库前仍需先执行 `SQL/create_log_store_target_import.sql` |
| **粒度** | 1行 = 1个店仓在 1 天、1 个目标版本下的目标值 |
| **更新策略** | 按 `target_date + store_id + target_version` 覆盖维护 |
| **主键** | `id` |
| **唯一键** | `(target_date, store_id, target_version)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `target_date` | DATE | 目标日期 | NOT NULL |
| `store_id` | BIGINT | 店仓 ID | NOT NULL |
| `month_target` | DECIMAL(18,2) | 月固定目标 | 默认 0.00；不按日目标合计回算 |
| `day_target` | DECIMAL(18,2) | 当日冻结目标 | 默认 0.00；允许月内动态调整 |
| `target_version` | VARCHAR(32) | 目标版本号 | 默认 `v1` |

**DQ 规则**：
- `etl_ads_store_daily_report.py` 按 `target_date = report_date AND target_version = data_version` 精确匹配目标版本。
- 正式交付路径已确认采用“业务投递 Excel 到 NAS 指定目录，由 Python 定时扫描并导入 `cfg_store_target_daily`”；当前已冻结 NAS 目录为 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，并按月份分文件管理，当前推荐命名规则已切换为 `YYYYMM考核数据配置表.xlsx`；导入脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。当前仓库已提供导入脚本，默认 dry-run，只有 `--apply` 才会按目标月份与版本删旧重灌；若 NAS 目录内同时存在多个目标月份文件，需显式传入 `--target-month` 选择月份；若同月同时存在多个版本文件，则需改用 `--file-path` 显式指定。若 `store_name` 未命中 `dim_store`，脚本会直接失败并返回候选建议。
- 若同一份模板显式提供 `门店类型` 列，导入脚本可在 `--sync-store-report-attr` 模式下按 `store_id` 对当前有效 `dim_store_report_attr` 记录执行未变化 / 变更 / 新增 / 退出分类；未变化不动，变更执行关旧开新，新增只开新，退出只关旧。默认不会改写无关门店的历史版本。
- 若工作簿同时提供 `统计主体目标` 与 `门店考核归属` 两张 sheet，导入脚本会在同一事务中同步刷新共同考核配置；两张 sheet 必须同时存在。若两张 sheet 均存在但无有效数据，则表示清空当月共同考核配置。
- `month_target` 表示当月固定目标，`day_target` 表示该日冻结值；业务允许在月内动态调整日目标，因此同一自然月内 `day_target` 合计允许不等于 `month_target`，不作为 DQ 错误。
- 目标配置行数少于有效门店数时当前只告警、不阻断运行；该规则已由业务确认，原因是未来门店数量可能收缩，允许部分门店暂时无目标但保留日报行；缺失门店会以 0 目标写入并导致达成率为空。

---

### cfg_store_assessment_subject_target_daily

| 属性 | 值 |
|------|-----|
| **来源** | MySQL 业务配置表（同一份 NAS 工作簿 `统计主体目标` sheet 导入） |
| **生产方式** | `tools/import_cfg_store_target_daily_from_nas.py` 在共同考核模式下按目标月份与版本重载 |
| **粒度** | 1行 = 1个统计主体在 1 天、1 个目标版本下的目标值 |
| **更新策略** | 按 `target_date + subject_code + target_version` 覆盖维护 |
| **主键** | `id` |
| **唯一键** | `(target_date, subject_code, target_version)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `target_date` | DATE | 目标日期 | NOT NULL |
| `target_month` | DATE | 目标月份首日 | NOT NULL |
| `subject_code` | VARCHAR(64) | 统计主体编码 | NOT NULL |
| `subject_name` | VARCHAR(255) | 统计主体名称 | NOT NULL |
| `assessment_mode` | VARCHAR(20) | 考核模式（独立/合并） | 仅允许 `独立`、`合并` |
| `month_target` | DECIMAL(18,2) | 主体月目标 | 默认 0.00 |
| `day_target` | DECIMAL(18,2) | 主体日目标 | 默认 0.00 |
| `target_version` | VARCHAR(32) | 目标版本号 | 默认 `v1` |

**DQ 规则**：
- `门店考核归属` 引用的 `subject_code` 必须在这里存在；否则导入脚本直接失败。
- 主体目标沿用门店目标的“月目标固定、日目标冻结”规则；允许月内 `day_target` 合计不等于 `month_target`。
- 若工作簿进入共同考核模式但该表当月留空，表示清空当月主体目标配置；主体层会回退到门店目标汇总。

---

### cfg_store_assessment_assignment

| 属性 | 值 |
|------|-----|
| **来源** | MySQL 业务配置表（同一份 NAS 工作簿 `门店考核归属` sheet 导入） |
| **生产方式** | `tools/import_cfg_store_target_daily_from_nas.py` 在共同考核模式下按目标月份与版本重载 |
| **粒度** | 1行 = 1个门店在 1 个目标月份、1 个目标版本下的一段主体归属生效区间 |
| **更新策略** | 按 `target_month + target_version` 删除当月旧配置后整月重灌 |
| **主键** | `id` |
| **唯一键** | `(target_month, target_version, store_id, effective_start_date)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `store_id` | BIGINT | 门店 ID | NOT NULL |
| `subject_code` | VARCHAR(64) | 统计主体编码 | NOT NULL |
| `assignment_role` | VARCHAR(20) | 归属角色（主店/快闪/独立） | 仅允许 `主店`、`快闪`、`独立` |
| `is_joint_assessment` | CHAR(1) | 是否共同考核（Y/N） | 仅允许 `Y`、`N` |
| `anchor_store_id` | BIGINT | 挂靠主店 ID | 共同考核成员行可为空，由唯一主店自动回填 |
| `effective_start_date` | DATE | 生效开始日 | 必须落在 `target_month` 自然月内 |
| `effective_end_date` | DATE | 生效结束日 | 必须落在 `target_month` 自然月内且不早于开始日 |

**DQ 规则**：
- `门店考核归属` sheet 新增 `门店ID` 必填列；列名沿用 `门店ID`，但业务填写值以 `dim_store.store_code` 为准，如 `RT050`。导入脚本优先按 `store_code` 命中 `dim_store`，若填写纯数字则兼容 `store_id`；Excel `门店名称` 仅作为业务展示与名称漂移提示，不再作为唯一匹配键。
- 同一个门店在同一个 `target_month + target_version` 下不允许出现生效区间重叠；导入脚本在写库前做校验。
- 同商场、同城市、同 RT 编码都**不**自动推断共同考核；是否共同考核只以这张表的显式配置为准。
- 共同考核成员若未显式填主店且同主体下恰好存在一条 `assignment_role='主店'`，导入脚本会自动回填；否则直接失败。
- 未配置任何归属时，主体层默认回退为“每店一个统计主体”，不会阻断主体层 ETL。

---

### log_store_target_import

| 属性 | 值 |
|------|-----|
| **来源** | `tools/import_cfg_store_target_daily_from_nas.py` 执行日志 |
| **建表 SQL** | `SQL/create_log_store_target_import.sql` |
| **状态** | 已建表并完成首轮 SUCCESS 日志验证 |
| **粒度** | 1行 = 1次门店日报目标导入执行 |
| **更新策略** | 追加写入 |
| **主键** | `id` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `file_name` | VARCHAR(255) | 源文件名 | NOT NULL |
| `file_md5` | CHAR(32) | 源文件 MD5 | 可空 |
| `source_sheet` | VARCHAR(100) | 来源工作表 | 默认 `导入模板` |
| `target_month` | DATE | 目标月份首日 | 可空 |
| `target_version` | VARCHAR(32) | 目标版本号 | 可空 |
| `store_count` | INT | 命中的门店数 | ≥ 0 |
| `records_total` | INT | 源门店行数 | ≥ 0 |
| `records_after_filter` | INT | 展开后的日粒度行数 | ≥ 0 |
| `records_inserted` | INT | 实际写入行数 | ≥ 0 |
| `status` | VARCHAR(20) | 执行状态 | NOT NULL |
| `message` | VARCHAR(1000) | 执行摘要或错误信息 | 可空 |

**DQ 规则**：
- 当前日志表只在 `--apply` 写库模式下记录；dry-run 结果由脚本标准输出或 `--output-json` 文件承接。
- `status='FAILED'` 时，`message` 应包含未命中门店、候选建议或数据库写入异常等可追溯信息。

---

### cfg_duty_free_store_mtd_sales

| 属性 | 值 |
|------|-----|
| **来源** | NAS `免税门店月累计销售.xlsx` / `免税月累计` sheet + MySQL 门店真值校验 |
| **生产方式** | `tools/import_duty_free_store_mtd_sales_from_nas.py` 读取免税月累计快照；默认 dry-run，`--apply` 写库 |
| **粒度** | 1行 = 1个 `target_month + data_version + store_id` 的免税门店月累计快照 |
| **更新策略** | 按 `target_month + data_version` 删除旧快照后整批重灌 |
| **主键** | `id` |
| **唯一键** | `(target_month, data_version, store_id)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `target_month` | DATE | 目标月份首日 | NOT NULL |
| `data_version` | VARCHAR(32) | 目标版本号 | NOT NULL |
| `store_id` | BIGINT | 免税门店 ID | Excel `门店ID` 可填 `dim_store.store_id` 或 `dim_store.store_code`，入库前统一解析为 `store_id` |
| `store_name` | VARCHAR(255) | 免税门店名称 | 必须与当前有效 `dim_store_report_attr` 真值一致 |
| `report_channel_type` | VARCHAR(32) | 经营渠道细分类 | 必须与当前有效 `dim_store_report_attr.report_channel_type` 一致 |
| `external_mtd_sales_amt` | DECIMAL(18,2) | 业务维护的月累计销售额 | 默认 0.00 |
| `source_file_name` | VARCHAR(255) | 源 Excel 文件名 | 可空 |
| `source_file_md5` | CHAR(32) | 源 Excel MD5 | 可空 |

**DQ 规则**：
- 模板必填表头固定为 `目标月份 / 数据版本 / 门店ID / 门店名称 / 渠道类型 / 月累计`；同一份文件内只允许 1 个 `目标月份` 和 1 个 `数据版本`；`月累计` 空白按 `0.00` 解析。来源：[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L34)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L196)
- `门店ID` 列可填 `dim_store.store_id` 或 `dim_store.store_code`；脚本会先解析到 `dim_store.store_id`，且当前有效 `dim_store_report_attr.is_duty_free='Y'`，否则导入脚本直接失败，不允许把普通门店误写到免税快照。来源：[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L88)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L231)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L351)
- 文件内 `门店名称`、`渠道类型` 必须与维表真值一致；脚本不会按 Excel 文本反向改写维度。
- 当前链路只承接 `月累计销售额` 单指标，不生成日销、销量、订单数、连带率、客单价或折扣率等派生事实。

---

### log_duty_free_store_mtd_sales_import

| 属性 | 值 |
|------|-----|
| **来源** | `tools/import_duty_free_store_mtd_sales_from_nas.py` 执行日志 |
| **建表 SQL** | `SQL/create_log_duty_free_store_mtd_sales_import.sql` |
| **状态** | 仓库已提供建表 SQL；目标环境首次启用前需人工执行 |
| **粒度** | 1行 = 1次免税月累计快照导入执行 |
| **更新策略** | 追加写入 |
| **主键** | `id` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `file_name` | VARCHAR(255) | 源文件名 | NOT NULL |
| `file_md5` | CHAR(32) | 源文件 MD5 | 可空 |
| `target_month` | DATE | 目标月份首日 | 可空 |
| `data_version` | VARCHAR(32) | 目标版本号 | 可空 |
| `changed_store_count` | INT | 变更门店数 | ≥ 0 |
| `new_store_count` | INT | 新增门店数 | ≥ 0 |
| `exited_store_count` | INT | 退出门店数 | ≥ 0 |
| `records_inserted` | INT | 实际写入行数 | ≥ 0 |
| `status` | VARCHAR(20) | 执行状态 | NOT NULL |

**DQ 规则**：
- 正式调度用 `file_md5 + target_month + data_version` 做免税链路独立幂等判重；命中最近 `SUCCESS` 时直接跳过。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2026)；[../SQL/create_log_duty_free_store_mtd_sales_import.sql](../SQL/create_log_duty_free_store_mtd_sales_import.sql#L7)
- `changed_store_count / new_store_count / exited_store_count` 只用于判断是否新增免税受影响日期，不作为其他业务指标事实来源。

---

### cfg_store_operation_owner_snapshot

| 属性 | 值 |
|------|-----|
| **来源** | NAS `门店负责人映射表.xlsx` 当前快照 / 显式生效区间 + MySQL 当前经营实体清单校验 |
| **生产方式** | `tools/import_store_operation_owner_from_nas.py` 读取负责人映射表；默认 dry-run，`--apply` 写库 |
| **粒度** | 1行 = 1个 `snapshot_date` 下 1 个经营实体 |
| **更新策略** | 按 `snapshot_date` 删除同日旧快照后整日重灌 |
| **主键** | `id` |
| **唯一键** | `(snapshot_date, entity_type, entity_code)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `snapshot_date` | DATE | 快照日期 | NOT NULL |
| `entity_type` | VARCHAR(20) | 经营实体类型：`STORE` / `SUBJECT` | 仅允许 `STORE`、`SUBJECT` |
| `entity_id` | BIGINT | 经营实体 ID；共同考核时取挂靠主店 ID | 可空 |
| `entity_code` | VARCHAR(64) | 经营实体编码；普通门店=`store_code`，共同考核=`subject_code` | NOT NULL |
| `entity_name` | VARCHAR(255) | 经营实体名称 | NOT NULL |
| `owner_name` | VARCHAR(100) | 负责人名称 | 允许为空 |
| `remark` | VARCHAR(500) | 业务备注 | 可空 |
| `source_file_name` | VARCHAR(255) | 来源文件名 | 可空 |
| `source_file_md5` | CHAR(32) | 来源文件 MD5 | 可空 |

**DQ 规则**：
- Excel 必填表头为 `门店编码 / 门店名称 / 负责人`；`门店负责人映射表 / 门店负责人映射模板` 两个 sheet 名均可识别。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L27)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L29)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L39)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L142)
- `备注`、`生效日期`、`失效日期` 为可选列；未填写日期时默认按 `snapshot_date ~ 9999-12-31` 解释，若显式填写区间则必须覆盖 `snapshot_date`，否则进入 `invalid_effective_date_rows` 并阻断 `--apply`。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L40)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L204)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L561)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L878)
- 未填写日期且负责人/实体信息未变的行，分类阶段仍视为 `unchanged`，不会因为默认 `snapshot_date` 每天重开一段新历史；只有显式区间变化或负责人实际变化时，才会进入 `changed`。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L59)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L276)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L683)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L237)
- 预期经营实体来自 `dim_store_report_attr` 当前有效且纳日报口径的门店，再叠加 `cfg_store_assessment_assignment` 与 `cfg_store_assessment_subject_target_daily` 推导；独立门店维护 `STORE`，共同考核经营体维护 `SUBJECT`。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L256)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L299)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L320)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L364)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L403)
- 若共同考核经营体已存在，负责人快照推荐保留 `SUBJECT` 行作为最终真值；但在同一目标月的生效切换过渡期内，若被吸收成员门店 `STORE` 行与对应 `SUBJECT` 行并存，或在正式生效日前已提前维护 `SUBJECT` 且成员 `STORE` 仍保留，脚本会将这些实体记入 `tolerated_transition_entities` 并降级为 warning，不阻断 `--apply`。若仅提前维护 `SUBJECT`、却未同时保留成员 `STORE`，或确实缺少当前应维护实体，则仍会失败。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L437)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L16)
- `owner_name` 允许为空，表示当前未分配负责人，不视为导入错误。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L178)

---

### dim_store_operation_owner_assignment

| 属性 | 值 |
|------|-----|
| **来源** | `cfg_store_operation_owner_snapshot` + 当前有效历史切片对比结果 |
| **生产方式** | `tools/import_store_operation_owner_from_nas.py --apply` 在同次导入中维护 SCD2 |
| **粒度** | 1行 = 1个经营实体的 1 段负责人生效区间 |
| **更新策略** | `unchanged` 不动；`changed/new` 按快照行显式起止日期开新；`changed/exited` 按切换起点关旧或删除冲突切片 |
| **主键** | `id` |
| **唯一键** | `(entity_type, entity_code, effective_start_date)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `entity_type` | VARCHAR(20) | 经营实体类型 | 仅允许 `STORE`、`SUBJECT` |
| `entity_id` | BIGINT | 经营实体 ID | 可空 |
| `entity_code` | VARCHAR(64) | 经营实体编码 | NOT NULL |
| `entity_name` | VARCHAR(255) | 经营实体名称 | NOT NULL |
| `owner_name` | VARCHAR(100) | 负责人名称 | 允许为空 |
| `source_snapshot_date` | DATE | 触发当前版本生效的快照日期 | NOT NULL |
| `source_file_name` | VARCHAR(255) | 来源文件名 | 可空 |
| `source_file_md5` | CHAR(32) | 来源文件 MD5 | 可空 |
| `effective_start_date` | DATE | 生效开始日 | NOT NULL |
| `effective_end_date` | DATE | 生效结束日 | NOT NULL |
| `is_current` | CHAR(1) | 是否当前有效 | 仅允许 `Y`、`N` |

**DQ 规则**：
- 同一 `snapshot_date` 下若同一实体命中多条历史有效切片，脚本直接失败，不继续写库。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L512)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)
- `unchanged` 不新增历史切片；`changed/exited` 先按 `snapshot.effective_start_date` 作为切换起点关旧，若旧切片起始日已大于等于切换起点则直接删除；`changed/new` 再按快照行的 `effective_start_date / effective_end_date` 开新。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L794)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L967)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1104)
- 对未显式填写日期的当前真值行，`unchanged` 判等只看实体与负责人 payload，不因默认日期值变化而转成 `changed`。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L683)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L733)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L237)
- 若新快照与紧邻上一版历史切片完全一致，脚本直接重开上一版，不新增重复切片。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L876)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L917)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L66)

---

### log_store_operation_owner_import

| 属性 | 值 |
|------|-----|
| **来源** | `tools/import_store_operation_owner_from_nas.py --apply` 执行日志 |
| **建表 SQL** | `SQL/create_store_operation_owner_tables.sql` |
| **状态** | 已建本地 DDL；是否写库取决于用户是否执行 `--apply` |
| **粒度** | 1行 = 1次负责人快照导入执行 |
| **更新策略** | 追加写入 |
| **主键** | `id` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `file_name` | VARCHAR(255) | 源文件名 | NOT NULL |
| `file_md5` | CHAR(32) | 源文件 MD5 | 可空 |
| `source_sheet` | VARCHAR(100) | 来源工作表 | NOT NULL |
| `snapshot_date` | DATE | 快照日期 | NOT NULL |
| `records_total` | INT | 源数据行数 | ≥ 0 |
| `expected_entity_count` | INT | 预期经营实体数 | ≥ 0 |
| `matched_entity_count` | INT | 成功匹配的经营实体数 | ≥ 0 |
| `missing_entity_count` | INT | 缺失经营实体数 | ≥ 0 |
| `unexpected_entity_count` | INT | 异常经营实体数 | ≥ 0 |
| `snapshot_rows_inserted` | INT | 快照表写入行数 | ≥ 0 |
| `history_rows_opened` | INT | 历史表开新/重开行数 | ≥ 0 |
| `history_rows_closed` | INT | 历史表关旧/同日替换行数 | ≥ 0 |
| `status` | VARCHAR(20) | 执行状态 | NOT NULL |
| `message` | VARCHAR(1000) | 执行摘要或错误信息 | 可空 |

**DQ 规则**：
- 当前日志表只在 `--apply` 写库模式下记录；dry-run 结果由标准输出或 `--output-json` 文件承接。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L775)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1164)
- 当 `--apply` 命中 `missing_entities`、未被过渡规则吸收的 `unexpected_entities`、实体名称不一致或历史重叠时，脚本写入 `FAILED` 日志并保留对应计数与错误摘要；若仅命中过渡期 `tolerated_transition_entities`，状态会降为 `WARNING` 并附带 warning 摘要，不阻断写库。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L775)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1164)

---

## DWS 层

DWS（Data Warehouse Summary）层是面向主题的汇总明细层。

---

### dws_sales_daily

| 属性 | 值 |
|------|-----|
| **来源** | ODS: `ods_m_retail` + `ods_m_retailitem` + DIM: `dim_store` |
| **生产脚本** | `etl_dws_sales.py` |
| **粒度** | 1行 = 1个 SKU（`m_productalias_id`）在1天（`date_id`）的销售汇总 |
| **更新策略** | 增量（按日期窗口 DELETE + INSERT）|
| **主键** | `id` |
| **唯一键** | `(date_id, store_id, product_id, m_productalias_id)`。来源：[SQL/alter_dws_sales_unique_key.sql](SQL/alter_dws_sales_unique_key.sql#L1-L8) |
| **增量水位** | `date_id`（业务日期，T-1 默认）|
| **回填接口** | `etl_dws_sales.backfill(start_date, end_date)` |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `date_id` | INT | 销售日期（业务日期，YYYYMMDD）| PK 组成 |
| `store_id` | BIGINT | 店仓 ID | NOT NULL |
| `product_id` | BIGINT | 商品 ID（SPU）| NOT NULL |
| `m_productalias_id` | BIGINT | SKU ID | PK 组成 |
| `sales_qty` | INT | 销售数量（正向，qty>0）| ≥ 0 |
| `sales_amount` | DECIMAL | 销售金额（正向）| ≥ 0 |
| `sales_amount_list` | DECIMAL | 吊牌金额 | ≥ 0 |
| `return_qty` | INT | 退货数量（来自 qty<0 的记录）| ≥ 0 |
| `return_amount` | DECIMAL | 退货金额 | ≥ 0 |
| `net_qty` | INT | 净销量（字段存在但当前ETL不填充，默认0） | 可为负 |
| `net_amount` | DECIMAL | 净销售额（字段存在但当前ETL不填充，默认0） | 可为负 |
| `order_count` | INT | 订单数 | ≥ 0 |
| `store_code` | VARCHAR | 源店仓编码 |  |
| `is_cloud_store` | CHAR(1) | 是否云仓（Y/N） |  |
| `etl_time` | DATETIME | ETL 时间戳 | NOT NULL |
| `created_at` | DATETIME | 创建时间 |  |
| `updated_at` | DATETIME | 更新时间 |  |

说明：字段以 MySQL 结构快照为准，ETL 输出包含 `sales_qty`/`sales_amount`/`sales_amount_list`/`return_qty`/`return_amount` 等，净销量/净销售额字段存在但未由 ETL 写入；`store_code` 与 `is_cloud_store` 当前由 `dim_store` 回补。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_dws_sales.py](etl_dws_sales.py)

**DQ 规则**：
- `sales_qty = SUM(ods_m_retailitem.qty) WHERE ods_m_retail.tot_amt_actual > 0`
- `return_qty = SUM(ABS(ods_m_retailitem.qty)) WHERE ods_m_retail.tot_amt_actual < 0`
- 同一 `(date_id, store_id, product_id, m_productalias_id)` 只有1行（聚合后唯一）。来源：[SQL/alter_dws_sales_unique_key.sql](SQL/alter_dws_sales_unique_key.sql#L1-L8)
- 业务日期窗口：按 `date_id` 做 DELETE + INSERT 回刷，不直接使用 ODS 的双水位作为 DWS 水位。

---

### dws_inventory_daily

| 属性 | 值 |
|------|-----|
| **来源** | ODS: `ods_fa_storage` + DIM: `dim_store` |
| **生产脚本** | `etl_dws_inventory.py` |
| **粒度** | 1行 = 1个 SKU 在1天（`date_id`）的库存快照 |
| **更新策略** | 每日快照（覆盖当日数据）|
| **主键** | `id` |
| **唯一键** | `(date_id, store_id, product_id, m_productalias_id)`。来源：[SQL/alter_dws_inventory_unique_key.sql](SQL/alter_dws_inventory_unique_key.sql#L1-L29) |
| **增量水位** | `date_id`（当日）|

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `date_id` | INT | 快照日期（YYYYMMDD） | PK 组成 |
| `store_id` | BIGINT | 店仓 ID | NOT NULL |
| `store_code` | VARCHAR | 店仓编码 |  |
| `is_cloud_store` | CHAR(1) | 是否云仓（Y/N） |  |
| `product_id` | BIGINT | 商品 ID | NOT NULL |
| `m_productalias_id` | BIGINT | SKU ID | PK 组成 |
| `qty` | INT | 库存数量 | ≥ 0（来自 FA_STORAGE）|
| `qty_valid` | INT | 可用库存 | ≥ 0 |
| `qty_occupy` | INT | 占用数量 | ≥ 0 |
| `qtypurchaserem` | BIGINT | 采购欠数/在途 | ≥ 0 |
| `etl_time` | DATETIME | ETL 时间戳 | NOT NULL |
| `created_at` | DATETIME | 创建时间 |  |

说明：字段以 MySQL 结构快照为准，并与 ETL 输出字段保持一致；当前 `store_code` 与 `is_cloud_store` 由 `dim_store` 回补，`qty_valid` 继续沿用 `qty` 口径。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_dws_inventory.py](etl_dws_inventory.py)

**DQ 规则**：
- 仅包含 `isactive = 'Y'` 且参与计算的店仓（总仓或云仓）
- 同一 `(date_id, store_id, product_id, m_productalias_id)` 只有1行（聚合后唯一）。来源：[SQL/alter_dws_inventory_unique_key.sql](SQL/alter_dws_inventory_unique_key.sql#L1-L29)
- 与 `tools/check_dws_inventory.py` 的 Oracle 对账行数偏差 < 1%

---

## ADS 层

ADS（Application Data Store）层是面向业务消费的宽表，每日全量重算。

---

### ads_daily_sales

| 属性 | 值 |
|------|-----|
| **来源** | `ods_m_retail` + `ods_m_retailitem` + `dim_store` + `dim_store_report_attr` + `dim_product` + `cfg_store_target_daily` + `cfg_store_assessment_subject_target_daily` + `cfg_store_assessment_assignment` |
| **生产脚本** | `etl_ads_daily_sales.py` |
| **粒度** | 1行 = 1个报告日、1个 `battle_month`、1个 `sales_date`、1个战区、1个经营渠道细分类 |
| **更新策略** | 按 `(report_date, data_version)` 先删后插 |
| **主键** | `id` |
| **唯一键** | `(report_date, data_version, battle_month, sales_date, area_name, report_channel_type)` |

**关键字段**：

| 字段名 | 类型 | 含义 | 计算 / 规则 |
|--------|------|------|-------------|
| `report_date` | DATE | 报告日期 | ETL 参数传入 |
| `battle_month` | DATE | 战役月份首日 | 固定取 `report_date` 所在自然月月初 |
| `sales_date` | DATE | 销售日期 | 只覆盖 `battle_month` 月初到 `report_date` |
| `area_name` | VARCHAR(50) | 战区 | `dim_store.area_name`；物理层不再生成 `全国` 汇总行 |
| `report_channel_type` | VARCHAR(32) | 经营渠道细分类 | `dim_store_report_attr.report_channel_type`；物理层不再生成 `全部` 汇总行 |
| `day_target_amt` | DECIMAL(18,2) | 当日节奏目标 | 共同考核时优先取主体日目标，否则回退经营实体内门店日目标求和。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L118)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L169)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L175) |
| `day_actual_amt` | DECIMAL(18,2) | 当日实际 | 基于 `ods_m_retail + ods_m_retailitem` 汇总净额，并只保留门店日报商品范围。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L186)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L223) |
| `cum_target_amt` | DECIMAL(18,2) | 月累计目标 | 按 `area_name + report_channel_type` 的 `sales_date` 序列累加。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L302)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L311) |
| `cum_actual_amt` | DECIMAL(18,2) | 月累计实际 | 按 `area_name + report_channel_type` 的 `sales_date` 序列累加净额。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L302)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L312) |
| `last_year_cum_actual_amt` | DECIMAL(18,2) | 去年同期累计实际 | 按当前切片与 `sales_date` 对齐后累加去年同期净额。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L302)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L313) |
| `data_version` | VARCHAR(32) | 数据版本号 | ETL 参数传入；用于精确匹配 `cfg_store_target_daily.target_version` |
| `etl_time` | DATETIME | ETL 时间戳 | ETL 运行时间 |

**DQ 规则**：
- `--conn-test` 只检查源依赖；若目标表尚未执行 `SQL/create_ads_daily_sales.sql`，脚本只告警、不直接失败。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L1)；[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)
- 同一 `report_date` 下，不允许同一 `store_id` 命中多条有效 `dim_store_report_attr`；否则 ETL 直接失败。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L1)
- 同一 `battle_month` 范围内，不允许 `cfg_store_target_daily` 出现同店同日重复目标记录；否则 ETL 直接失败。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L1)
- 输出行数必须等于 `天数 * 明细组合数`，且每个 `sales_date` 都必须完整产出一套明细切片。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L1)
- 仓库已提供 `SQL/check_ads_daily_sales_min.sql`，最小对账覆盖行数、唯一键，以及按全部明细切片聚合后的整段日序列核对；核对 SQL 已同步切到共同考核主体日目标优先、日报有效门店、固定排除 `147/149/150` 三类商品与 ODS 净额事实。来源：[SQL/check_ads_daily_sales_min.sql](../SQL/check_ads_daily_sales_min.sql#L1)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L118)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L302)

说明：当前 `ads_daily_sales` 已完成仓库内 DDL、ETL 与最小对账 SQL 样板，并已接入 `scheduled_store_daily_report.py` 的受影响日期批量重跑，但仍未接入 `run_etl.py` 主调度。历史 `2026-04-15 / v1` 与 `2026-04 / v2` 的写库与最小对账记录形成于旧版销售主题逻辑；本轮统一到 `ads_store_daily_report` 权威口径后，不能直接视为新逻辑验证结论，后续需按新口径重新复核。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L456)；[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)；[AGENT_HANDOFF_archive.md](AGENT_HANDOFF_archive.md#L460)

---

### ads_store_daily_report

| 属性 | 值 |
|------|-----|
| **来源** | `ods_m_retail` + `ods_m_retailitem` + `dim_product` + `dim_store` + `dim_store_report_attr` + `cfg_store_target_daily` + `cfg_store_assessment_subject_target_daily` + `cfg_store_assessment_assignment` + `dim_store_operation_owner_assignment` + `cfg_duty_free_store_mtd_sales` |
| **生产脚本** | `etl_ads_store_daily_report.py` |
| **粒度** | 1行 = 1个最终经营实体在 1 个报告日、1 个目标版本下的日报宽表 |
| **更新策略** | 按 `(report_date, data_version)` 先删后插 |
| **主键** | `id` |
| **唯一键** | `(report_date, store_id, data_version)` |

**关键字段**：

| 字段名 | 类型 | 含义 | 计算 / 规则 |
|--------|------|------|-------------|
| `report_date` | DATE | 报告日期 | ETL 参数传入 |
| `store_id` | BIGINT | 经营实体 ID | 未配置共同考核时等于原门店 `store_id`；已配置时取挂靠主店 `store_id` |
| `store_code` | VARCHAR(40) | 经营实体编码 | 未配置时等于 `dim_store.store_code`；已配置共同考核时写入 `subject_code` 前 40 位 |
| `store_name` | VARCHAR(255) | 经营实体名称 | 未配置时等于门店名称；已配置共同考核时取 `subject_name` |
| `owner_name` | VARCHAR(100) | 负责人名称 | 来自 `dim_store_operation_owner_assignment.owner_name`；按实体类型 + 实体编码 + 报告日命中有效切片，允许为空 |
| `report_channel_type` | VARCHAR(20) | 日报渠道最终真值 | 来自 `dim_store_report_attr` |
| `day_sales_amt` | DECIMAL(18,2) | 日销售额 | 当日净额；退货负值直接冲减 |
| `day_sales_qty` | INT | 日销量 | 当日净量；退货负数直接冲减 |
| `day_order_cnt` | INT | 日订单数 | 基于日报有效交易集先按零售单去重，再按过滤后商品范围的单号净额 `>0=1 / <0=-1` 汇总；`ABS(金额) < 0.0001` 视为 0 |
| `day_target` | DECIMAL(18,2) | 日目标 | 已配置共同考核时优先取主体日目标；否则回退经营实体内门店日目标求和 |
| `day_ach_rate` | DECIMAL(18,4) | 日达成率 | `day_sales_amt / day_target`；目标为 0 时返回 NULL |
| `mtd_sales_amt` | DECIMAL(18,2) | 月累计销售额 | 非免税实体取当月起始至 `report_date` 的净额；免税实体若命中 `cfg_duty_free_store_mtd_sales`，则改用外部快照月累计覆盖 |
| `mtd_list_amt` | DECIMAL(18,2) | 月累计吊牌金额 | 当月起始至 `report_date` 的日报有效交易集吊牌金额；用于月累计折扣率与 Tableau 明细总计的聚合分母 |
| `mtd_sales_qty` | INT | 月累计销量 | 当月起始至 `report_date` 的净量 |
| `mtd_order_cnt` | INT | 月累计订单数 | 基于月内日报有效交易集先按零售单去重，再按过滤后商品范围的单号净额 `>0=1 / <0=-1` 汇总；`ABS(金额) < 0.0001` 视为 0 |
| `month_target` | DECIMAL(18,2) | 月目标 | 已配置共同考核时优先取主体月目标；否则回退经营实体内门店月目标求和 |
| `month_ach_rate` | DECIMAL(18,4) | 月达成率 | `mtd_sales_amt / month_target`；免税实体命中外部快照时随覆盖后的 `mtd_sales_amt` 一起重算；目标为 0 时返回 NULL |
| `last_month_mtd_sales_amt` | DECIMAL(18,2) | 上月同期累计销售额 | 同日口径 |
| `last_year_mtd_sales_amt` | DECIMAL(18,2) | 去年同期累计销售额 | 同期口径 |
| `same_store_mtd_sales_amt` | DECIMAL(18,2) | 同店本期累计销售额 | 从完整源物理门店范围出发，纳入 `open_date <= 去年同期月份第一天`、且 `assignment_role` 不为 `快闪` 的门店本期月累计销售额；本期无交易时保留为 0，再按最终经营实体汇总 |
| `same_store_last_year_mtd_sales_amt` | DECIMAL(18,2) | 同店去年同期累计销售额 | 同一开业日期资格集合的去年同期月累计销售额；去年同期为 0 仍保留为 0。若当前经营实体命中当月中途生效的 `快闪` 合并，则去年同期累计上界截到“最早快闪生效日前一天”的去年同日 |
| `yoy_rate` | DECIMAL(18,4) | 销售额同比率 | `(same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt) - 1` |
| `yoy_amt_diff` | DECIMAL(18,2) | 销售额同比差额 | `same_store_mtd_sales_amt - same_store_last_year_mtd_sales_amt` |
| `day_rank` | INT | 日销排名 | `RANK() OVER (ORDER BY day_sales_amt DESC, store_id)` |
| `mtd_rank` | INT | 月销排名 | `RANK() OVER (ORDER BY mtd_sales_amt DESC, store_id)` |
| `time_progress` | DECIMAL(18,4) | 时间进度 | `DAY(report_date) / DAY(LAST_DAY(report_date))` |
| `data_version` | VARCHAR(32) | 数据版本号 | 默认 `v1` |

**DQ / 处理规则**：
- 原始门店范围来自 `dim_store_report_attr` 当前生效且 `is_include_in_daily_report = 'Y'` 的记录，但最终输出行数按“经营实体数”校验，不再强制等于物理门店数。
- 商品范围固定排除 `147=辅料`、`149=办公用品`、`150=道具`，其余有 `dim_product.category_id` 的商品默认纳入。
- 交易明细按行级过滤 `ABS(ri.tot_amt_actual) >= 1`，当前冻结为“绝对金额小于 1 的明细整体排除”。
- 若当前 `report_date` 命中的共同考核归属缺少对应主体目标，门店日报 ETL 直接失败，避免最终表落出半配置经营体。
- 若负责人历史表已开始维护，当前 `report_date` 命中的经营实体必须各自命中唯一一条有效负责人切片；切片重叠或缺切片时，门店日报 ETL 直接失败。
- 销售额同比以 `dim_store.open_date <= 去年同期月份第一天` 判定源物理门店同店资格；`open_date IS NULL` 统一表示开业日期不可用，判为非同店、记录 DQ 告警且不回退旧销售额资格。完整源门店集合以经营实体映射驱动，两侧销售事实均左连接，避免丢失“本期为 0 / 去年同期为正数”的 `-100%`，或“本期为正数 / 去年同期为 0”的零分母 NULL 场景。
- 若当前经营实体在报告月内中途吸收 `快闪` 成员，则 `same_store_last_year_mtd_sales_amt` 的去年同期累计上界截到最早 `快闪` 生效日前一天，避免主体门店在合并后已无 ERP 销售时仍继续累计去年后续天数。
- 免税外部快照当前只覆盖 `mtd_sales_amt`、`month_ach_rate` 与 `mtd_rank`；不据此反推日销、销量、订单数、连带率、客单价或折扣率。
- 若 `day_target > 0` 或 `month_target > 0`，对应达成率字段不得为空；脚本运行后会做校验。
- 目标配置条数与有效门店数不一致时当前只告警，不自动阻断；其中“少于有效门店数”的场景已由业务确认允许。

说明：当前 `ads_store_daily_report` 已直接承接共同考核合并语义，最终业务输出不再保留被合并的物理门店行。仓库已新增 `SQL/alter_ads_store_daily_report_add_owner_name.sql`、`SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql` 与 `SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql`，目标库执行前 `owner_name`、`mtd_list_amt`、`same_store_mtd_sales_amt`、`same_store_last_year_mtd_sales_amt` 仍属于待落地物理列；更新后的 ETL 会先检查缺列状态，再决定是否写数。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L8)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L149)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L526)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L608)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L711)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L822)；[../SQL/alter_ads_store_daily_report_add_owner_name.sql](../SQL/alter_ads_store_daily_report_add_owner_name.sql#L1)；[../SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql](../SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql#L1)；[../SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql](../SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql#L1)

---

### ads_store_daily_subject_report

| 属性 | 值 |
|------|-----|
| **来源** | `ads_store_daily_report` + `cfg_store_assessment_subject_target_daily` + `cfg_store_assessment_assignment` |
| **生产脚本** | `etl_ads_store_daily_subject_report.py` |
| **粒度** | 1行 = 1个统计主体在 1 个报告日、1 个目标版本下的日报宽表 |
| **更新策略** | 按 `(report_date, data_version)` 先删后插 |
| **主键** | `id` |
| **唯一键** | `(report_date, subject_code, data_version)` |

**关键字段**：

| 字段名 | 类型 | 含义 | 计算 / 规则 |
|--------|------|------|-------------|
| `report_date` | DATE | 报告日期 | ETL 参数传入 |
| `subject_code` | VARCHAR(64) | 统计主体编码 | 共同考核时来自配置；未配置时回退 `STORE_<store_code>` |
| `subject_name` | VARCHAR(255) | 统计主体名称 | 优先取主体目标表；默认独立时回退门店名称 |
| `subject_source` | VARCHAR(32) | 主体来源 | `configured_subject` / `default_independent` |
| `assessment_mode` | VARCHAR(20) | 考核模式 | 优先取主体目标表；否则按 `is_joint_assessment='Y'` 或成员数 > 1 推断 `合并` |
| `anchor_store_id` | BIGINT | 挂靠主店 ID | 主店或唯一主店自动回填 |
| `report_channel_type` | VARCHAR(32) | 经营渠道细分类 | 直接承接 `ads_store_daily_report.report_channel_type`；不得为空 |
| `member_store_count` | INT | 主体内门店数 | 来自 `cfg_store_assessment_assignment` 聚合；未配置时固定为 1 |
| `day_sales_amt` | DECIMAL(18,2) | 主体日销售额 | 直接复用 `ads_store_daily_report.day_sales_amt` |
| `day_order_cnt` | INT | 主体日订单数 | 直接复用 `ads_store_daily_report.day_order_cnt`；自动继承门店层“按过滤后商品范围单号净额判 `1 / 0 / -1`，近零值按 0 处理”的口径 |
| `day_target` | DECIMAL(18,2) | 主体日目标 | 直接复用 `ads_store_daily_report.day_target` |
| `mtd_sales_amt` | DECIMAL(18,2) | 主体月累计销售额 | 直接复用 `ads_store_daily_report.mtd_sales_amt` |
| `mtd_order_cnt` | INT | 主体月累计订单数 | 直接复用 `ads_store_daily_report.mtd_order_cnt`；自动继承门店层“按过滤后商品范围单号净额判 `1 / 0 / -1`，近零值按 0 处理”的口径 |
| `month_target` | DECIMAL(18,2) | 主体月目标 | 直接复用 `ads_store_daily_report.month_target` |
| `day_rank` / `mtd_rank` | INT | 主体排名 | `RANK()` 按主体粒度重算 |
| `data_version` | VARCHAR(32) | 数据版本号 | 与门店层保持一致 |

**DQ / 处理规则**：
- 主体层 ETL 依赖同日同版本的 `ads_store_daily_report` 先行成功；专题调度固定按“门店层 -> 主体层”顺序重跑。
- 若当前 `report_date` 命中的归属配置在同一门店上出现多条有效记录，主体层 ETL 直接失败，避免重复放大。
- 若当前 `report_date` 已配置共同考核归属，但缺少对应主体日目标，门店层与主体层都会直接失败；未配置共同考核则允许整体回退为独立主体。
- `report_channel_type` 不允许为空；主体层写库前会统计空值数并在发现空值时直接失败。来源：[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L465)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L491)
- 若 `day_target > 0` 或 `month_target > 0`，对应达成率字段不得为空；脚本运行后会做校验。

说明：主体层当前不再重新汇总物理门店销售事实，而是以最终经营实体结果为底稿，直接承接门店层 `report_channel_type`，再补齐主体编码、主店锚点与成员数。来源：[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L95)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L139)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L154)

---

### ads_inventory_health

| 属性 | 值 |
|------|-----|
| **来源** | 默认 `dws_inventory_daily` + `dws_sales_daily` + `dim_*` + `ads_dabo_order_label` 最新批次（ODS/缓存兜底；缺失时回退 `ads_dabo_daily_sales`）；若主链显式 `cutover_mode=v2`，则切换为 `dws_inventory_daily_v2 + dws_sales_daily_v2` |
| **生产脚本** | `etl_ads_health.py` |
| **粒度** | 1行 = 1个 SKU 的当日库存健康评估（SKU 粒度）|
| **更新策略** | 每日全量重算（TRUNCATE + INSERT）|
| **主键** | `m_productalias_id`（当日唯一）|
| **计算口径** | `SQL/库存健康度_SKU粒度_v5.0.sql`（参考 SQL）|

**关键指标字段**：

| 字段名 | 类型 | 含义 | 计算公式 |
|--------|------|------|---------|
| `snapshot_date` | DATE | 快照日期 | 当天 |
| `sku_id` | BIGINT | SKU 主键 |  |
| `sku_barcode` | VARCHAR | SKU 条码 |  |
| `color` | VARCHAR | SKU 颜色 |  |
| `size` | VARCHAR | SKU 尺寸 |  |
| `total_qty` | INT | 当前总库存 | `warehouse_qty + cloud_qty` |
| `warehouse_qty` | INT | 总仓库存 |  |
| `cloud_qty` | INT | 云仓库存 |  |
| `sales_qty_30d` | INT | 近30天销量（全量） | SUM(dws_sales_daily.sales_qty) |
| `sales_qty_7d` | INT | 近7天销量（全量） | SUM(dws_sales_daily.sales_qty) |
| `return_qty_30d` | INT | 近30天退货数量 |  |
| `sales_amt_30d` | DECIMAL | 近30天销售额 |  |
| `dabo_sales_qty_30d` | INT | 近30天达播销量 | 优先按最新标签批次桥接 ODS 明细汇总；缺失时回退 ads_dabo_daily_sales |
| `dabo_sales_qty_7d` | INT | 近7天达播销量 | 优先按最新标签批次桥接 ODS 明细汇总；缺失时回退 ads_dabo_daily_sales |
| `dabo_latest_date` | DATE | 达播最新日期 | 达播来源窗口内最新业务日期 |
| `dabo_revenue_30d` | DECIMAL | 近30天达播销售额 | 优先按最新标签批次桥接 ODS 明细汇总；缺失时回退 ads_dabo_daily_sales |
| `dabo_revenue_7d` | DECIMAL | 近7天达播销售额 | 优先按最新标签批次桥接 ODS 明细汇总；缺失时回退 ads_dabo_daily_sales |
| `natural_sales_qty_30d` | INT | 近30天自然销量 | `sales_qty_30d - dabo_sales_qty_30d` |
| `turnover_days` | DECIMAL | 库存周转天数 | `total_qty / (sales_qty_30d / 30)`，sales_qty_30d=0时为 NULL |
| `suggest_qty` | INT | 建议补货量 | `(90 - turnover_days) × 日均销 - 30天退货 - 采购在途` |
| `sales_velocity` | DECIMAL | 销售加速度 | `(sales_qty_7d / 7) / (sales_qty_30d / 30)`，分母=0时为 NULL |
| `inventory_status` | VARCHAR(20) | 库存状态 | 见下方分级规则 |
| `sku_grade` | CHAR(1) | SABC 分级 | 见下方分级规则 |
| `etl_time` | DATETIME | ETL 时间戳 | NOT NULL |

说明：字段以 MySQL 结构快照为准，完整字段见快照。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_ads_health.py](../etl_ads_health.py#L270)

说明补充：自 2026-04-09 起，`ads_inventory_health` 的达播字段优先使用最新 `ads_dabo_order_label` 批次，在 ODS 内按 `COALESCE(canonical_system_order_id, system_order_id) = ods_m_retail.oms_sourcecode` 识别达播订单，并对 `ods_m_retailitem` 汇总 SKU 近30天/近7天销量与销售额；若 ODS 尚无对应订单，则回退 `ads_dabo_order_retail_bridge` 缓存；仅当标签批次不可用时，才回退 `ads_dabo_daily_sales`。来源：[etl_ads_health.py](../etl_ads_health.py#L195)；[etl_ads_health.py](../etl_ads_health.py#L270)；[etl_ads_health.py](../etl_ads_health.py#L672)；[run_etl.py](../run_etl.py#L583)；[run_etl.py](../run_etl.py#L649)

说明补充：自 2026-05-12 起，`run_etl.py` 对 `ads_inventory_health` 新增显式 cutover 契约：默认 `legacy` 读取旧 DWS；`shadow_compare` 仍按旧 DWS 写生产表，但会额外对 `dws_inventory_daily_v2 + dws_sales_daily_v2` 生成报告型对账；`v2` 才会显式改读 `_v2`。`--rollback-to-legacy` 优先回退到旧链。来源：[cutover_controls.py](../cutover_controls.py#L29)；[cutover_controls.py](../cutover_controls.py#L55)；[run_etl.py](../run_etl.py#L767)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)；[run_etl.py](../run_etl.py#L998)；[run_etl.py](../run_etl.py#L1013)；[etl_ads_health.py](../etl_ads_health.py#L523)

**库存状态（health_grade）分级规则**：

| 状态 | 条件 | 含义 |
|------|------|------|
| `RED` | `turnover_days < 30` | 严重短货，需立即补货 |
| `ORANGE` | `30 ≤ turnover_days < 70` | 需关注，建议安排补货 |
| `GREEN` | `70 ≤ turnover_days ≤ 90` | 库存健康，无需操作 |
| `BLUE` | `turnover_days > 90` | 积压，关注去化 |
| `GRAY` | `sale_30d = 0 AND qty_total > 0` | 无销售但有库存（滞销）|
| `BLACK` | `sale_30d = 0 AND qty_total = 0` | 无库存无销售（死档）|

**SABC 分级规则**（按近30天累计销售额排名）：

| 分级 | 累计占比 | 含义 |
|------|---------|------|
| `S` | 前 30% | 超级畅销款（核心品）|
| `A` | 30% ~ 70% | 主力款（核心+次核心）|
| `B` | 70% ~ 90% | 普通款 |
| `C` | 后 10% | 长尾/滞销款 |

**DQ 规则**：
- 仅包含 `is_main_category = 1` 的 SKU（主销品类，见 `config.MAIN_CATEGORY_IDS`）
- `total_qty IS NOT NULL`，否则该 SKU 健康度设为 NULL 并记录日志
- 与 Oracle 对账：总 SKU 数差异 < 5%（见 `config.ORACLE_VERIFY_QUERIES`）

---

### ads_dabo_daily_sales

| 属性 | 值 |
|------|-----|
| **来源** | 另一个项目产出后导入（达播数据运营手动上传）|
| **生产方式** | 手动导入（见 `docs/达播数据运营上传指南.md`）|
| **粒度** | 1行 = 1个 SKU 在1天的达播销售记录 |
| **更新策略** | 手动/外部文件更新；`run_etl.py` 继续上报其当日状态，但 `ads_health` 仅在标签批次不可用时才回退到本表 |
| **主键** | `(sale_date, product_alias_code)` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `sale_date` | DATE | 销售日期 |
| `product_alias_code` | VARCHAR | SKU 条码 |
| `dabo_sales_qty` | INT | 达播销量 |
| `dabo_order_count` | INT | 达播订单数 |
| `dabo_revenue` | DECIMAL | 达播销售额 |
| `created_at` | DATETIME | 创建时间 |
| `updated_at` | DATETIME | 更新时间 |

说明：`dabo_latest_date` 为 `etl_ads_health.py` 汇总计算出的衍生字段，不是 `ads_dabo_daily_sales` 物理字段。来源：[etl_ads_health.py](../etl_ads_health.py#L672)；[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)

说明补充：`ads_dabo_daily_sales` 仍保留为兼容聚合表，但在 `ads_inventory_health` 中已降级为 fallback 来源；主线口径改为先将订单级标签落到 `ads_dabo_order_label`，再由 ODS / SQL 按订单打标计算达播指标。来源：[etl_ads_health.py](../etl_ads_health.py#L195)；[etl_ads_health.py](../etl_ads_health.py#L270)；[tools/load_dabo_order_labels_from_nas.py](tools/load_dabo_order_labels_from_nas.py#L1-L430)

---

### ads_dabo_order_label

| 属性 | 值 |
|------|-----|
| **来源** | NAS `订单管理*.xlsx`（`T_V_OMSONLINEORDER`） |
| **生产方式** | `tools/load_dabo_order_labels_from_nas.py` |
| **粒度** | 1行 = 1个 `source_file` 下的 1 个达播 `system_order_id` 标签快照 |
| **更新策略** | 按 `source_file` 先删后插；默认 dry-run，需显式 `--apply` 才写库 |
| **主键** | `(source_file, system_order_id)` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `source_file` | VARCHAR | 来源 Excel 文件名 |
| `source_sheet` | VARCHAR | 来源工作表名 |
| `source_file_mtime` | DATETIME | 来源文件修改时间 |
| `first_source_row_number` | INT | 首个来源行号 |
| `source_row_count` | INT | 当前 `system_order_id` 在候选集中的行数 |
| `system_order_id` | VARCHAR(512) | 原始系统单号，保留 Excel 原值用于追溯 |
| `canonical_system_order_id` | VARCHAR(512) | 归一后的优先桥接键；默认等于原值，仅在唯一候选异常组合单上改写 |
| `normalization_status` | VARCHAR(32) | 归一状态，如 `exact_hit` / `auto_alias` |
| `normalization_rule` | VARCHAR(64) | 归一规则名，如 `same_file_unique_token_superset` |
| `normalization_evidence` | TEXT | 归一证据 JSON，记录 token、候选数与命中候选 |
| `platform_order_id` | VARCHAR(128) | 平台单号，仅作辅助追溯 |
| `is_dabo_order` | TINYINT | 是否达播订单 |
| `dabo_source` | VARCHAR(64) | 标签来源，当前固定 `yunque_order_management` |
| `dabo_channel_code` | VARCHAR(32) | 达播渠道代码 |
| `dabo_channel_name` | VARCHAR(64) | 达播渠道名称 |
| `influencer_id` | VARCHAR(64) | 主播 ID |
| `influencer_name` | VARCHAR(128) | 主播名称 |
| `order_status` | VARCHAR(64) | 订单状态 |
| `platform_ship_time` | VARCHAR(32) | 平台发货时间原始文本 |
| `created_at` | DATETIME | 创建时间 |
| `updated_at` | DATETIME | 更新时间 |

说明：当前业务目标不是先兼容 Excel 金额字段，而是先构建一张内部订单标签表，为 `ods_m_retail.oms_sourcecode` 打上“是否达播 / 达播渠道”标签；后续生意额、退款等指标统一在 ODS / SQL 层按标签筛选计算。自 2026-04-09 起，表内新增 `canonical_system_order_id` 归一桥接层：保留原始 `system_order_id` 不动，仅对精确未命中且在同一 `source_file` 内存在唯一已命中候选的组合单生成 canonical 值，供下游优先桥接。来源：[tools/extract_dabo_order_candidates_from_nas.py](tools/extract_dabo_order_candidates_from_nas.py#L19-L334)；[tools/load_dabo_order_labels_from_nas.py](tools/load_dabo_order_labels_from_nas.py#L1-L430)；[SQL/create_ads_dabo_order_label.sql](SQL/create_ads_dabo_order_label.sql#L1-L30)

说明补充：自 2026-04-09 起，`run_etl.py` 中的 `dabo_ready` 会优先检查 `ads_dabo_order_label` 最新批次是否存在且最近 1 天有更新，并同时上报 `ads_dabo_daily_sales` 的 legacy 状态；`ads_health` 在主调度中会优先使用标签主线，若标签批次未就绪但 legacy 当日可用则回退 legacy，否则达播字段按 0 处理。来源：[run_etl.py](../run_etl.py#L210)；[run_etl.py](../run_etl.py#L289)；[run_etl.py](../run_etl.py#L583)；[run_etl.py](../run_etl.py#L649)；[etl_ads_health.py](../etl_ads_health.py#L742)

---

### ads_dabo_order_retail_bridge

| 属性 | 值 |
|------|-----|
| **来源** | `ads_dabo_order_bridge.main_order_id` + Oracle `M_RETAIL` |
| **生产方式** | `tools/sync_dabo_order_retail_bridge.py --source-file ...` |
| **粒度** | 1行 = 1个达播样本文件中的主订单命中的 1 张零售单头 |
| **更新策略** | 按 `source_file` 覆盖重建 |
| **主键** | `(source_file, retail_id)` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `source_file` | VARCHAR | 达播样本文件名 |
| `main_order_id` | VARCHAR(512) | 达播主订单编号 |
| `retail_id` | BIGINT | 零售单头 ID |
| `billdate` | INT | 单据日期 |
| `retail_tot_amt_actual` | DECIMAL | 零售单头实收金额 |
| `retail_status` | INT | 零售单状态 |
| `retail_isactive` | CHAR(1) | 是否有效 |
| `synced_at` | DATETIME | 最近同步时间 |

说明：该表是运行层桥接缓存，不替代 `ods_m_retail.oms_sourcecode` 的正式同步；当历史回填未完成时，`tools/query_data.py` 的 `mysql_dabo_actual_daily_by_billdate` 模板会自动回退到本表。来源：[tools/sync_dabo_order_retail_bridge.py](tools/sync_dabo_order_retail_bridge.py)；[tools/query_data.py](tools/query_data.py)

---

## 指标口径速查

### 库存周转天数

```
周转天数 = 当前库存 / 近30天日均销量
         = total_qty / (sales_qty_30d / 30)
```

### 建议补货量

```
建议补货量 = (90 - 周转天数) × 近30天日均销量
           - 近30天退货量
           - 采购在途量（M_PURCHASEITEM 未到货）
```

### 自然销量口径（剔除达播）

```
natural_sales_qty_7d = sales_qty_7d - dabo_sales_qty_7d
natural_sales_qty_30d = sales_qty_30d - dabo_sales_qty_30d
```

### 销售速度比

```
销售速度比 = (近7天日均销量) / (近30天日均销量)
           = (sales_qty_7d / 7) / (sales_qty_30d / 30)

> 1：近期加速（趋热）
< 1：近期减速（趋冷）
```

### 主销品口径

```sql
-- 主销品 = 主销品类 AND ISACTIVE = 'Y' AND 有条码
WHERE p.M_DIM4_ID IN (134, 142, 139, 138, 141, 143, 133, 136, 140, 137, 144, 145)
  AND fs.ISACTIVE = 'Y'
  AND fs.M_PRODUCTALIAS_ID IS NOT NULL
  AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
```

> 来源：`config.py:MAIN_CATEGORY_IDS`，修改前需业务确认。

---

## 审计附注

- 结构快照 JSON 会包含 `schema_name` 与 `column_id` 字段，用于记录库名与列序号（审计辅助信息）。来源：[tools/snapshot_mysql_hefangdw_schema.py](tools/snapshot_mysql_hefangdw_schema.py#L1-L40)；[tools/snapshot_oracle_bosnds3_schema.py](tools/snapshot_oracle_bosnds3_schema.py#L1-L40)
- M3 架构完善子项目已确认优先旁路 `ods_*_raw` 方案；`ods_m_retail_raw`、`ods_m_retailitem_raw`、`ods_fa_storage_raw`、`dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` 已由用户人工建表并按授权完成旁路验证装载。当前销售 DWD 在 20260428-20260430 已与 `dws_sales_daily` 日级汇总对齐；库存 DWD 在 20260507 与本次 full raw 自洽，但与 `dws_inventory_daily` 的 `qty` 差 337，原因是生产 ODS/DWS 快照时间点早于本次 Oracle full raw 初始化。上述对象仍未接入 `run_etl.py` / 总控，当前 DWS / ADS 不消费，不代表生产契约已切换。来源：[ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md](ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md)；[../reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json](../reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json)
- M4 架构完善子项目已输出 `dws_sales_daily_v2`、`dws_inventory_daily_v2` 并行表草案；用户已人工建表，Copilot 先只读核验显示销售 v2 33 列、库存 v2 31 列，均具备 `date_id + store_id + product_id + m_productalias_id` 唯一键、`validation_status` 与 `etl_time`。当前 `etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py` 与 `dws_v2_write_utils.py` 仍保留 dry-run / conn-test / S3 手工写入能力；默认 dry-run 只打印源摘要 SQL、候选 `INSERT ... SELECT` SQL、写后目标摘要 SQL和 DWD-v2 对账 SQL；`--conn-test` 只读校验连接、字段与唯一键；S3 写入必须显式传入 `--execute --confirm-write WRITE_DWS_SALES_V2` 或 `WRITE_DWS_INVENTORY_V2`，并使用命名锁、显式事务、失败回滚、JSON 运行证据和写后 DWD-v2 对账。其中库存脚本已新增 `--source-loaded-at-cutoff` / `--align-with-old-dws`，可把旧 `dws_inventory_daily` 当日 `MAX(etl_time)` 固定为 aligned cutoff，并在执行时先删除 `date_id` 同日切片再重灌，避免更晚快照残留 key 污染 old DWS 对比。当前已在用户明确授权下完成一次 S3 实跑验收：销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0；销售与旧 DWS 0 差异，库存与旧 DWS 的 200 条同 key `qty` 差异当前按快照时点不同记录。当前下游生产契约默认仍以旧 DWS / ADS 为准，但主链已支持显式 `legacy / shadow_compare / v2` 三种 `ads_inventory_health` 读源边界；其中 `shadow_compare` 仅补报告型对账，不改变生产写数表。来源：[ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md](ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md)；[../dws_v2_write_utils.py](../dws_v2_write_utils.py#L1)；[../etl_dws_sales_v2.py](../etl_dws_sales_v2.py#L1)；[../etl_dws_inventory_v2.py](../etl_dws_inventory_v2.py#L1)；[../run_etl.py](../run_etl.py#L767)；[../run_etl.py](../run_etl.py#L782)；[../run_etl.py](../run_etl.py#L803)；[../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json](../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json)；[../reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json](../reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json)；[../reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json](../reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json)

---

## 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| v2.75 | 2026-07-13 | 将门店日报同店资格切换为 `dim_store.open_date` 截止日判定，明确空日期 DQ、零销售门店保留、快闪排除与月中分母截断边界 |
| v2.74 | 2026-06-18 | 纠正 `门店考核归属` 的 `门店ID` 字段语义：业务填写 RT 门店编码，脚本优先按 `store_code` 命中并兼容纯数字 `store_id` |
| v2.73 | 2026-06-18 | 将 `门店考核归属` 契约更新为必填 `门店ID`，并明确共同考核导入优先按 `store_id` 匹配、门店名称仅作辅助校验 |
| v2.72 | 2026-06-08 | 将门店日报与 ads_daily_sales 的商品范围契约切换为固定排除 `147/149/150`，其余 category_id 默认纳入 |
| v2.71 | 2026-05-26 | 补充 `dim_store_report_attr.is_duty_free` 同步规则：`门店类型` 包含 `免税` 时优先判为 `Y` |
| v2.70 | 2026-05-26 | 免税月累计 `门店ID` 列兼容 `store_code`，并记录空白 `月累计` 按 `0.00` 解析与 `is_duty_free='Y'` 前置校验 |
| v2.69 | 2026-05-26 | 将免税月累计外部快照契约从 `report_date` 纠正为 `target_month`，并同步调度幂等键与日志字段说明 |
| v2.68 | 2026-05-25 | 新增免税月累计快照与导入日志契约，并明确 `ads_store_daily_report` 仅对免税实体覆盖 `mtd_sales_amt / month_ach_rate / mtd_rank` |
| v2.67 | 2026-05-22 | 明确 `ads_store_daily_report` 的同店同比辅助金额需排除 `assignment_role=快闪` 的源门店，避免 RT014 误入同店口径 |
| v2.66 | 2026-05-22 | 将 `ads_store_daily_report.yoy_rate / yoy_amt_diff` 契约切换为同店同比，并新增两列同店辅助金额字段 |
| v2.65 | 2026-05-20 | 新增 `ads_store_daily_report.mtd_list_amt` 契约，作为月累计折扣率与 Tableau 明细总计的吊牌金额分母 |
| v2.64 | 2026-05-12 | 补记 `ads_inventory_health` 已新增显式 cutover 契约：默认 legacy、`shadow_compare` 只做 v2 报告型对账、`v2` 才改读 `_v2` |
| v2.63 | 2026-05-12 | 补记负责人快照默认日期行保持 unchanged 的规则，避免每天误切一段新历史 |
| v2.62 | 2026-05-12 | 新增 ADS 外部消费契约总则，明确影子链替代旧链时允许新增字段，但不得改名或删除既有 ADS 字段 |
| v2.61 | 2026-05-12 | 将负责人快照契约更新为兼容 Excel 显式生效/失效日期，并同步历史切片按 effective_start_date 切换的规则 |
| v2.60 | 2026-05-07 | 补记库存 DWS v2 已支持 `source_loaded_at cutoff` 自动/显式对齐、同日切片删后重灌和 old DWS aligned 对账，作为 S4 shadow compare 前置口径 |
| v2.59 | 2026-05-07 | 补记 DWS v2 已完成一次 S3 实跑验收：销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0；由于仍未接总控 / 未切 ADS，v2 表仍未形成生产契约 |
| v2.59 | 2026-06-06 | 退役 3 张销售专题 ADS，并将销售专题契约收口到当前保留链路 |
| v2.58 | 2026-05-07 | 补记 DWS v2 S3 手工写入分支已新增：默认 dry-run，写入需确认令牌、命名锁、事务和 DWD-v2 对账；本轮未执行真实写入，v2 表仍未形成生产契约 |
| v2.57 | 2026-05-07 | 补记 DWS v2 dry-run / conn-test 脚本已新增但无写库入口，v2 表仍未形成生产契约 |
| v2.56 | 2026-05-07 | 补记 DWS v2 两张并行表已由用户人工建表并完成空表核验，但仍未写数据、未接生产契约 |
| v2.55 | 2026-05-07 | 同步 M3 raw / DWD 旁路表已完成销售完整业务日期和库存 full raw 初始化验证，但仍未接生产契约 |
| v2.54 | 2026-04-29 | 补记 M3 raw ODS / DWD 草案对象与只读对账 SQL 尚未落库、尚未接入生产契约 |
| v2.53 | 2026-04-29 | 将 ads_sales_org_monthly.month_order_cnt 改为汇总 ads_store_daily_report.day_order_cnt，并将 ads_sku_daily.mtd_order_cnt 校准为按 SKU 过滤后净额与近零容差判单 |
| v2.52 | 2026-04-29 | 补记 ads_store_daily_subject_report 的 day_order_cnt / mtd_order_cnt 直接承接门店层订单数，并自动继承过滤后金额与近零容差口径 |
| v2.51 | 2026-04-29 | 校准 ads_sales_org_monthly 当前来源：销售事实来自 ODS 明细，DWD 仍未实现，目标口径对齐共同考核主体优先 |
| v2.50 | 2026-04-27 | 补记销售主题 ADS 的最小对账 SQL 已统一到门店日报权威口径同源验证 |
| v2.49 | 2026-04-27 | 将 ads_sales_org_daily、ads_daily_sales、ads_sku_daily 契约统一到 ads_store_daily_report 权威口径，并补记旧验证记录不覆盖新逻辑 |
| v2.48 | 2026-04-27 | 补记 ads_sales_org_monthly 已接入门店销售专题调度第六层，并更新专题 freshness 调度边界 |
| v2.47 | 2026-04-23 | 同步销售主题 ADS 改为 report_channel_type 明细口径，并补记主体层承接渠道字段与去除 全国/全部 物理汇总行契约 |
| v2.46 | 2026-04-23 | 将 ads_sku_daily.attach_contribution 契约精度提升到 DECIMAL(14,2)，并补记 2026-04-22/v2 实跑结果 |
| v2.45 | 2026-04-22 | 补记 ads_store_daily_report 负责人字段契约、负责人切片校验与待执行 alter 说明 |
| v2.44 | 2026-04-21 | 新增门店经营负责人快照、SCD2 历史与导入日志三张表的数据契约 |
| v2.43 | 2026-04-17 | 补记 ads_sku_daily 已完成专题调度第五层显式重跑验证，并更新五层写库状态 |
| v2.42 | 2026-04-17 | 将 ads_sku_daily 接入专题调度第五层，并补记其已正式写库、当前仅完成代码接链与单测验证 |
| v2.41 | 2026-04-17 | 将 ads_sku_daily 更新为含 attach_contribution 的二期样板，并补记 ODS 订单级口径与旧结构告警 |
| v2.40 | 2026-04-17 | 将 ads_sku_daily 更新为已补 sales_mix_pct、rank_no、trend_tag 的二期样板，并补记 alter 脚本与派生字段校验 |
| v2.39 | 2026-04-16 | 新增 ads_sales_org_monthly 与 ads_sku_daily 契约，并注明当前仅完成 conn-test 验证 |
| v2.38 | 2026-04-16 | 将 ads_sales_org_daily 接入专题调度第四层，并补记四层实跑验证结果 |
| v2.37 | 2026-04-16 | 同步专题调度自动跳过与显式 rerun 写库验证状态 |
| v2.36 | 2026-04-16 | 将 ads_daily_sales 接入专题调度代码，并补充三层批量重跑与单元测试验证边界 |
| v2.35 | 2026-04-16 | 更新 ads_daily_sales 为已完成 2026-04-15/v1 首轮样本与最小对账验证状态 |
| v2.34 | 2026-04-16 | 更新 ads_daily_sales 为当前库已建表但空表待样本验证状态 |
| v2.33 | 2026-04-15 | 新增 ads_daily_sales 契约，并将 ads_sales_org_daily 状态更新为已完成单日验证 |
| v2.32 | 2026-04-15 | 新增 ads_sales_org_daily 仓库样板契约，并明确净额/YTD 目标首版规则 |
| v2.31 | 2026-04-15 | 将门店日报目标导入 NAS 根目录从 月度日目标配置表 更新为 目标配置表 |
| v2.30 | 2026-04-10 | 将 ads_store_daily_report 更新为最终经营实体粒度，并同步主体层改为基于最终结果补主体编码 |
| v2.29 | 2026-04-10 | 新增门店日报共同考核配置契约与统计主体层 ADS 契约，并更新四 sheet 导入与双层重跑规则 |
| v2.28 | 2026-04-09 | 将 ads_inventory_health 的达播字段更新为标签主线优先、legacy 回退兜底，并同步 ads_dabo_daily_sales 契约定位 |
| v2.27 | 2026-04-09 | 更新 dabo_ready 为达播标签主线优先检查，并明确 ads_dabo_daily_sales 仅作为 ads_health 兼容回填开关 |
| v2.26 | 2026-04-09 | 为 ads_dabo_order_label 增加 canonical_system_order_id 与归一审计字段，明确下游优先桥接 canonical 值 |
| v2.25 | 2026-04-09 | 明确专题调度只自动处理当前月份快照，并将门店属性同步契约更新为未变化/变更/新增/退出分类 |
| v2.24 | 2026-04-08 | 同步门店日报渠道粗分类生成列已执行到现网的契约状态 |
| v2.23 | 2026-04-08 | 调整门店日报渠道契约为细分类真值，并补充 report_channel_type_group 生成列方案 |
| v2.22 | 2026-04-08 | 新增 ads_dabo_order_label 数据契约，明确统一 Excel 主线先做订单打标再按 ODS/SQL 计算指标 |
| v2.21 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充目录选档契约 |
| v2.20 | 2026-04-03 | 补充门店类型驱动的 dim_store_report_attr 同步规则与默认生效日策略 |
| v2.19 | 2026-04-03 | 更新门店日报目标导入契约为已建表、已首轮 apply、已完成专项消费验证 |
| v2.18 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本与日志表契约说明 |
| v2.17 | 2026-04-03 | 冻结门店日报目标 NAS 目录与固定文件命名约定 |
| v2.16 | 2026-04-03 | 明确门店日报月目标固定、日目标动态调整且月内日目标合计可不等于月目标 |
| v2.15 | 2026-04-03 | 明确门店日报目标配置采用 NAS 投递目录加 Python 定时扫描导入的正式路径 |
| v2.14 | 2026-04-03 | 补充门店日报目标配置少于有效门店数时只告警的业务确认规则 |
| v1.0 | 2026-03-01 | 初版，对齐 v0.6.3 ODS 双水位与达播字段 |
| v1.1 | 2026-03-01 | 修正 ods_m_retailitem 水位存储键名 | 
| v2.13 | 2026-04-03 | 新增门店日报配置层与 ads_store_daily_report 数据契约 |
| v1.2 | 2026-03-01 | 标注未在代码实现的字段名与达播口径字段 |
| v1.3 | 2026-03-01 | 修正 dim_sku 字段名并对齐快照证据 |
| v1.4 | 2026-03-01 | 修正 dws_sales_daily 字段名并对齐快照证据 |
| v1.5 | 2026-03-01 | 修正 dws_sales_daily DQ 规则字段命名 |
| v1.6 | 2026-03-01 | 修正 dws_sales_daily 主键与唯一键描述 |
| v1.7 | 2026-03-01 | 补充 ads_dabo_daily_sales 数据来源说明 |
| v1.8 | 2026-03-01 | 修正 dws_inventory_daily 字段名与唯一键描述 |
| v1.9 | 2026-03-01 | 修正 ads_inventory_health 字段名并对齐快照证据 |
| v2.0 | 2026-03-01 | 修正 ods_m_retail 字段、水位存储与增量逻辑描述 |
| v2.1 | 2026-03-01 | 修正 ods_m_retailitem 字段与双水位处理描述 |
| v2.2 | 2026-03-01 | 修正 dim_product 字段清单与抽取逻辑描述 |
| v2.3 | 2026-03-02 | 调整审计术语与补充快照字段说明 |
| v2.4 | 2026-03-02 | 补回结构字段并标注未填充说明 |
| v2.5 | 2026-03-18 | 新增 dim_channel 数据契约与 Oracle 来源说明 |
| v2.6 | 2026-03-18 | 将 dim_channel 店仓字段重命名为 WING_CODE 并对齐 Oracle 来源 |
| v2.7 | 2026-03-23 | 修正 dim_channel 现网核对结论，确认目标库数据已与 Oracle 对齐 |
| v2.8 | 2026-03-31 | 为 ods_m_retail 补充 oms_sourcecode 契约并记录达播 MySQL 内桥接用途 |
| v2.9 | 2026-03-31 | 将 ods_m_retail.oms_sourcecode 契约扩容为 VARCHAR(512)，兼容 Oracle 超长来源订单号 |
| v2.10 | 2026-03-31 | 更正 ods_m_retail.id 为逻辑主键映射，记录当前通过普通索引支撑同步与桥接 |
| v2.11 | 2026-03-31 | 新增 ads_dabo_order_retail_bridge 运行层缓存契约，供达播日报桥接兜底 |
| v2.14 | 2026-04-10 | 更新门店日报目标 NAS 命名约定为 YYYYMM考核数据配置表.xlsx，并注明脚本兼容历史旧文件名 |
| v2.13 | 2026-04-02 | 同步 ODS 重复装载治理已改为按源 id 替换写入，并补充现网唯一键治理脚本与 duplicate_id_count 复核方式 |
| v2.12 | 2026-04-02 | 补充 ods_m_retail 与 ods_m_retailitem 的主键治理提醒，并明确 ODS 高频查询必须同步评估连接/过滤索引 |
