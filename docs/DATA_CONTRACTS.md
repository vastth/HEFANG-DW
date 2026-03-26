# DATA_CONTRACTS.md — 何方珠宝数据仓库数据契约

> **定义**：数据契约描述每张表的数据生产者与消费者之间的约定，包括粒度、主键、增量水位、质量点（DQ）和关键指标口径。
>
> 当任意表结构、粒度、水位逻辑发生变更时，必须同步更新本文件。
>
> 最后更新：2026-03-18（v2.5 对齐）

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
- [DWS 层](#dws-层)
  - [dws_sales_daily](#dws_sales_daily)
  - [dws_inventory_daily](#dws_inventory_daily)
- [ADS 层](#ads-层)
  - [ads_inventory_health](#ads_inventory_health)
  - [ads_dabo_daily_sales](#ads_dabo_daily_sales)
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
| **更新策略** | 增量（基于 `modifieddate`，回刷 `backfill_days` 默认7天，窗口 `window_days` 默认1天，可切全量）来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L101-L169) |
| **主键** | `id`（Oracle 源主键）|
| **增量水位** | `modifieddate`（源字段，UPDATE 时更新）|
| **水位存储** | MySQL 元数据表 `ods_sync_state`（`table_name=ods_m_retail`，含窗口断点）来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L25-L87) |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `id` | BIGINT | 单据 ID（Oracle 主键）| PK，NOT NULL |
| `docno` | VARCHAR | 单据编号 |  |
| `billdate` | INT | 单据日期（业务日期）| 用于日报日期分区 |
| `c_store_id` | BIGINT | 所属店仓 | NOT NULL |
| `tot_amt_actual` | DECIMAL | 实际成交金额 |  |
| `tot_amt_list` | DECIMAL | 吊牌金额 |  |
| `tot_qty` | DECIMAL | 单据总数量 |  |
| `status` | INT | 单据状态 |  |
| `isactive` | CHAR(1) | 是否有效（'Y'/'N'）|  |
| `modifieddate` | DATETIME | 最后修改时间 | 增量依赖字段 |
| `etl_batch_id` | BIGINT | ETL 批次号 | NOT NULL |
| `etl_loaded_at` | DATETIME | ETL 写入时间 | NOT NULL |

说明：字段以 MySQL 结构快照为准，并与抽取 SQL 保持一致。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L101-L219)

**DQ/处理规则**：
- 全量模式先写入 `modifieddate` 为空记录（按 `id` 排序）。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L173-L189)
- 增量模式按 `modifieddate` 窗口先删后写。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L199-L221)

---

### ods_m_retailitem

| 属性 | 值 |
|------|-----|
| **来源** | Oracle: `M_RETAILITEM`（零售单据明细）|
| **生产脚本** | `etl_ods_m_retailitem.py` |
| **粒度** | 1行 = 1个单据明细行（1个 SKU 在1张单中的销售记录）|
| **更新策略** | 双水位增量（`modifieddate` + `settime`），回刷 `backfill_days` 默认7天，窗口 `window_days` 默认1天，可切全量。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L69-L162) |
| **主键** | `id`（Oracle 源主键）|
| **增量水位（线上）** | `modifieddate`（联合单据头 `M_RETAIL.MODIFIEDDATE`）|
| **增量水位（线下）** | `settime`（单据明细中的 `SETTIME` 字段）|
| **水位存储** | MySQL 元数据表 `ods_sync_state`，`table_name` 使用 `ods_m_retailitem`（modifieddate）与 `ods_m_retailitem_settime`（settime），含窗口断点。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L25-L87) |

**关键字段**：

| 字段名 | 类型 | 含义 | DQ 点 |
|--------|------|------|-------|
| `id` | BIGINT | 明细行 ID | PK，NOT NULL |
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

说明：字段以 MySQL 结构快照为准，并与抽取 SQL 保持一致。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L73-L214)

**DQ/处理规则**：
- 正向销售：`qty > 0`，退货：`qty < 0`（在 DWS 层分别聚合）
- 全量模式先写入 `modifieddate` 为空记录（按 `id` 排序）。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L184-L204)
- 增量模式按 `modifieddate` 窗口先删后写。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L214-L253)

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
| **主键** | `c_store_id` |

**关键字段**：

| 字段名 | 类型 | 含义 |
|--------|------|------|
| `c_store_id` | BIGINT | 店仓 ID |
| `store_code` | VARCHAR | 店仓编号（总仓='001'）|
| `store_name` | VARCHAR | 店仓名称 |
| `is_cloud_warehouse` | TINYINT | 是否云仓（`IS_ALLO2OSTORAGE='Y'` → 1）|
| `etl_updated_at` | DATETIME | ETL 写入时间 |

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

### ads_inventory_health

| 属性 | 值 |
|------|-----|
| **来源** | `dws_inventory_daily` + `dws_sales_daily` + `dim_*` + `ads_dabo_daily_sales`|
| **生产脚本** | `etl_ads_health.py`（572行）|
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
| `dabo_sales_qty_30d` | INT | 近30天达播销量 | 来自 ads_dabo_daily_sales |
| `natural_sales_qty_30d` | INT | 近30天自然销量 | `sales_qty_30d - dabo_sales_qty_30d` |
| `turnover_days` | DECIMAL | 库存周转天数 | `total_qty / (sales_qty_30d / 30)`，sales_qty_30d=0时为 NULL |
| `suggest_qty` | INT | 建议补货量 | `(90 - turnover_days) × 日均销 - 30天退货 - 采购在途` |
| `sales_velocity` | DECIMAL | 销售加速度 | `(sales_qty_7d / 7) / (sales_qty_30d / 30)`，分母=0时为 NULL |
| `inventory_status` | VARCHAR(20) | 库存状态 | 见下方分级规则 |
| `sku_grade` | CHAR(1) | SABC 分级 | 见下方分级规则 |
| `etl_time` | DATETIME | ETL 时间戳 | NOT NULL |

说明：字段以 MySQL 结构快照为准，完整字段见快照。来源：[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)；[etl_ads_health.py](etl_ads_health.py#L98-L150)

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
| **更新策略** | 手动触发回填（由 `run_etl.py` 中 `dabo_ready` 步骤检测）|
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

说明：`dabo_latest_date` 为 `etl_ads_health.py` 汇总计算出的衍生字段，不是 `ads_dabo_daily_sales` 物理字段。来源：[etl_ads_health.py](etl_ads_health.py#L156-L161)；[reports/snapshot_mysql_hefangdw_schema.json](reports/snapshot_mysql_hefangdw_schema.json)

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

---

## 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-03-01 | 初版，对齐 v0.6.3 ODS 双水位与达播字段 |
| v1.1 | 2026-03-01 | 修正 ods_m_retailitem 水位存储键名 | 
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
