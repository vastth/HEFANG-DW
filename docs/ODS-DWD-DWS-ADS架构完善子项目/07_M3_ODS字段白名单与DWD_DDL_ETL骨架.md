# M3 ODS 扩展字段白名单与 DWD DDL / ETL 骨架草案

---

## 1. 文档状态与边界

| 项 | 说明 |
|----|------|
| 文档状态 | M3 raw 旁路方案已获用户确认；raw ODS DDL、raw ODS 装载脚本与 DWD 小窗口对账 SQL 已输出；2026-04-30 已按 ERP AD_COLUMN 字典、FA_STORAGE 平台截图、Oracle 全量非零值扫描和用户确认的真实字段原则完成字段语义与字段筛选校准；用户已人工完成 5 张 raw / DWD 表建表并修正表注释；Copilot 已按授权完成 raw / DWD 小窗口真实装载，并于 2026-05-07 补齐销售完整业务日期 raw 与库存 full raw 初始化 |
| 本轮目标 | 冻结 `M_RETAIL`、`M_RETAILITEM`、`FA_STORAGE` 三张源表的 ODS 扩展字段白名单；比较兼容扩字段与旁路 `ods_*_raw` 方案；输出 raw ODS DDL、旁路 ODS / DWD ETL 和 DWD 小窗口对账 SQL |
| 实现状态 | 5 张 raw / DWD 表已由用户人工建表；已完成近 1 天小窗口验证、20260428-20260430 销售完整业务日期 raw / DWD 对账、20260507 库存 full raw / DWD 快照初始化；未改生产 ODS、未接入调度，当前 DWS / ADS 不消费 |
| 产物 | `SQL/draft_create_ods_m_retail_raw.sql`、`SQL/draft_create_ods_m_retailitem_raw.sql`、`SQL/draft_create_ods_fa_storage_raw.sql`、`etl_ods_m_retail_raw.py`、`etl_ods_m_retailitem_raw.py`、`etl_ods_fa_storage_raw.py`、`SQL/draft_create_dwd_sales_retail_item.sql`、`SQL/draft_create_dwd_inventory_storage_snapshot.sql`、`etl_dwd_sales_retail_item.py`、`etl_dwd_inventory_storage_snapshot.py`、`SQL/check_dwd_sales_retail_item_min.sql`、`SQL/check_dwd_inventory_storage_snapshot_min.sql` |
| 执行边界 | 已建表 DDL 与表注释修正由用户人工完成；raw / DWD 小窗口、销售完整业务日期 raw、库存 full raw 初始化均由用户授权后 Copilot 执行；后续 ALTER、索引调整、生产回填、调度接入仍需另行授权 |

本文件承接 `06_M2_5_ORACLE源库画像与ODS_DWD规划.md`。所有字段白名单基于本轮 Oracle 结构快照、字段启用率画像、现有 ETL 字段引用、M2 长期决策和 2026-04-30 全量非零值补证；2026-04-30 已用 `data/AD_COLUMN04301009.xlsx` 对 `M_RETAIL` / `M_RETAILITEM` 已选字段做显示名校准，并用用户提供的 ERP 开发平台截图对 `FA_STORAGE` 可见字段做语义校准。用户已明确确认：Oracle 源库存在模板化冗余字段，新架构只取可用、真实有数据且语义明确的字段；全量为 0 或全量为空的字段不进入 raw ODS / DWD 草案 DDL。2026-04-29 用户已确认 M3 优先采用旁路 `ods_*_raw` 方案；2026-04-30 用户已人工完成 5 张表建表并修正表注释，随后授权 Copilot 执行 raw ODS 近 1 天小窗口真实装载、DWD 小窗口写入与最小对账。2026-05-07 用户继续授权补完整业务日期销售 raw 和库存 full raw 初始化，销售 DWD 已与 `dws_sales_daily` 在 20260428-20260430 日级汇总对齐；库存 DWD 已与本次 full raw 自洽，但因现有 `dws_inventory_daily` 来自更早的生产 ODS 快照，与本次 Oracle full raw 初始化存在 337 件 `qty` 时间点差异。这些表仍未接入总控，当前 DWS / ADS 不消费。

---

## 2. 证据基线

| 证据 | 用途 |
|------|------|
| `data/AD_COLUMN04301009.xlsx` | ERP AD_COLUMN 字典；本轮用于校准 `M_RETAIL` / `M_RETAILITEM` 字段显示名与基础语义 |
| `reports/context_cache/ad_column_retail_raw_semantics_20260430.csv` | 从 AD_COLUMN 字典筛出的 raw ODS 已选字段语义缓存，便于后续定向复核 |
| 用户提供 ERP 开发平台 `FA_STORAGE` 字段截图（2026-04-30） | 校准库存源表 `QTY`、`QTYPREOUT`、`QTYPREIN`、`QTYVALID`、`QTY_FREEZE`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTY_OMS` 等显示名；是否入选草案 DDL 仍以全量非零值和语义价值共同判断 |
| `reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json` | 记录全量为 0 / 全量为空字段剔除依据，以及低覆盖但有真实非零值字段的保留边界 |
| `reports/snapshot_oracle_bosnds3_schema.json` | 确认三张 Oracle 源表字段存在、类型、顺序、可空性 |
| `reports/oracle_bosnds3_core_field_profile_202604.json` | 确认字段在统计窗口内是否有数据；`M_RETAIL` / `M_RETAILITEM` 为 2026-04 单据窗口，`FA_STORAGE` 为全表 |
| `reports/context_cache/m3_manual_ddl_verification_20260430.json` | 记录用户人工建表后 MySQL 只读核验结果：5 张表均存在、建表核验时 0 行、剔除字段未残留 |
| `reports/context_cache/m3_raw_dwd_small_window_load_20260430.json` | 记录本轮 raw ODS 近 1 天小窗口装载、DWD 小窗口 upsert、主键重复检查和 DWS 差异边界 |
| `reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json` | 记录销售完整业务日期 raw 补齐、库存 full raw 初始化、DWD 重算、raw→DWD 行数与 DWS 对账结果 |
| `SQL/create_ods_tables.sql#L1-L49` | 确认当前三张 ODS 表已落字段 |
| `etl_ods_m_retail.py#L107-L127`、`etl_ods_m_retailitem.py#L119-L139`、`etl_ods_fa_storage.py#L24-L36` | 确认当前 ODS 抽取 SQL |
| `etl_dws_sales.py#L34-L64`、`etl_dws_inventory.py#L35-L58` | 确认当前 DWS 消费字段与过滤范围 |
| `06_M2_5_ORACLE源库画像与ODS_DWD规划.md` | 确认 M2.5 对 ODS / DWD 的长期规划边界 |
| `SQL/draft_create_ods_m_retail_raw.sql`、`SQL/draft_create_ods_m_retailitem_raw.sql`、`SQL/draft_create_ods_fa_storage_raw.sql` | 确认 raw ODS 表结构；用户已人工执行建表，本轮已完成小窗口装载 |
| `etl_ods_m_retail_raw.py`、`etl_ods_m_retailitem_raw.py`、`etl_ods_fa_storage_raw.py` | 确认 raw ODS 旁路装载脚本，默认 dry-run，显式 `--execute` 后 upsert，不接总控 |
| `SQL/check_dwd_sales_retail_item_min.sql`、`SQL/check_dwd_inventory_storage_snapshot_min.sql` | 确认 DWD 小窗口只读对账 SQL；本轮已执行等效最小对账并记录差异边界 |

---

## 3. 三张表 ODS 扩展字段白名单冻结

### 3.1 字段分级约定

| 分级 | 含义 | M3 处理建议 |
|------|------|-------------|
| `MUST` | 当前链路已用或 DWD 必备事实字段 | ODS 扩展方案必须覆盖 |
| `SHOULD` | 源侧有数据，支撑 M2 已确认的长期业务上下文 | 第一批 ODS 扩展建议覆盖；若兼容扩字段成本高，可先进入 `ods_*_raw` |
| `WATCH` | 源侧有数据但本轮字典 / 截图未命中中文显示名，或当前统计窗口低覆盖 / 空字段但可能服务退货、闭环、追溯 | 进入旁路 raw 或后续补证，不进入第一批 DWD 强依赖 |
| `EXCLUDE` | 全量为 0 / 全量为空，且用户确认属于 Oracle 模板化冗余字段 | 不进入 raw ODS / DWD 草案 DDL；只在证据文件和文档中保留剔除记录 |

### 3.2 `M_RETAIL` 单头字段白名单

| 分级 | Oracle 字段 | 建议 MySQL 字段 | Oracle 类型 | 覆盖率证据 | 用途 |
|------|-------------|-----------------|-------------|------------|------|
| MUST | `ID` | `id` | `NUMBER(10,0)` | 100% | 单头主键 |
| MUST | `DOCNO` | `docno` | `VARCHAR2(80)` | 100% | 单据号 |
| MUST | `BILLDATE` | `billdate` | `NUMBER(8,0)` | 100% | 业务日期 / DWD 分区候选 |
| MUST | `C_STORE_ID` | `c_store_id` | `NUMBER(10,0)` | 100% | 店仓关联 |
| MUST | `OMS_SOURCECODE` | `oms_sourcecode` | `VARCHAR2(4000)` | 65.11% | WING平台单号；达播 / OMS 桥接候选 |
| MUST | `TOT_AMT_ACTUAL` | `tot_amt_actual` | `NUMBER(18,4)` | 99.99% | 总成交金额 |
| MUST | `TOT_AMT_LIST` | `tot_amt_list` | `NUMBER(18,4)` | 99.99% | 总零售金额 |
| MUST | `TOT_QTY` | `tot_qty` | `NUMBER(18,0)` | 99.99% | 总数量 |
| MUST | `STATUS` | `status` | `NUMBER(1,0)` | 100% | 提交状态；现有销售 DWS 有效单据过滤仍按代码口径处理 |
| MUST | `ISACTIVE` | `isactive` | `CHAR(1)` | 100% | 可用；现有销售 DWS 有效单据过滤仍按代码口径处理 |
| MUST | `MODIFIEDDATE` | `modifieddate` | `DATE` | 100% | 单头增量水位 |
| SHOULD | `CREATIONDATE` | `creationdate` | `DATE` | 100% | 源创建时间 / 追溯 |
| SHOULD | `DOCTYPE` | `doctype` | `CHAR(3)` | 100% | 单据类型 |
| SHOULD | `DESCRIPTION` | `description` | `VARCHAR2(765)` | 74.38% | 单头备注 / 业务核对 |
| SHOULD | `AVG_DISCOUNT` | `avg_discount` | `NUMBER(10,2)` | 99.99% | 折扣分析 |
| SHOULD | `C_VIP_ID` | `c_vip_id` | `NUMBER(10,0)` | 27.57% | VIP |
| SHOULD | `SALESREP_ID` | `salesrep_id` | `NUMBER(10,0)` | 27.25% | 零售员 |
| SHOULD | `PAY_STATUS` | `pay_status` | `NUMBER(1,0)` | 100% | 支付状态上下文 |
| SHOULD | `PAYERID` | `payerid` | `NUMBER(10,0)` | 99.87% | 支付操作人 |
| SHOULD | `PAYTIME` | `paytime` | `DATE` | 99.87% | 支付时间 |
| SHOULD | `CLOSE_STATUS` | `close_status` | `NUMBER(1,0)` | 100% | 关闭状态 |
| SHOULD | `REFNO` | `refno` | `VARCHAR2(255)` | 93.36% | POS零售单号 |
| SHOULD | `ISRETURNED` | `isreturned` | `VARCHAR2(255)` | 100% | 是否已退货，默认 N |
| SHOULD | `RETAILBILLTYPE` | `retailbilltype` | `VARCHAR2(3)` | 100% | 零售单类型 |
| WATCH | `DATEOUT` | `dateout` | `NUMBER(8,0)` | 99.98% | 出库日期，通常等于单据日期；少量历史调整单可能不同 |
| WATCH | `DATEIN` | `datein` | `NUMBER(8,0)` | 99.98% | 入库日期，通常等于单据日期；少量历史调整单可能不同 |
| WATCH | `CLOSERID` | `closerid` | `NUMBER(10,0)` | 0.01% | 关闭人，低覆盖，保留追溯 |
| WATCH | `CLOSETIME` | `closetime` | `DATE` | 0.01% | 关闭时间，低覆盖，保留追溯 |

冻结结论：`M_RETAIL` 第一批白名单覆盖当前 ODS 11 个已用字段，并新增 17 个上下文 / 状态 / 支付 / 追溯字段；不把 2026-04 空字段纳入第一批强依赖。

### 3.3 `M_RETAILITEM` 明细字段白名单

| 分级 | Oracle 字段 | 建议 MySQL 字段 | Oracle 类型 | 覆盖率证据 | 用途 |
|------|-------------|-----------------|-------------|------------|------|
| MUST | `ID` | `id` | `NUMBER(10,0)` | 100% | 明细主键 / DWD 主键 |
| MUST | `M_RETAIL_ID` | `m_retail_id` | `NUMBER(10,0)` | 100% | 零售单关联 |
| MUST | `M_PRODUCT_ID` | `m_product_id` | `NUMBER(10,0)` | 100% | 商品 |
| MUST | `M_PRODUCTALIAS_ID` | `m_productalias_id` | `NUMBER(10,0)` | 100% | 条码 |
| MUST | `QTY` | `qty` | `NUMBER(10,0)` | 100% | 数量 |
| MUST | `PRICELIST` | `pricelist` | `NUMBER(14,2)` | 100% | 零售价 |
| MUST | `PRICEACTUAL` | `priceactual` | `NUMBER(14,2)` | 100% | 成交价 |
| MUST | `TOT_AMT_ACTUAL` | `tot_amt_actual` | `NUMBER(18,4)` | 100% | 成交金额 |
| MUST | `TOT_AMT_LIST` | `tot_amt_list` | `NUMBER(18,4)` | 100% | 零售金额 |
| MUST | `MODIFIEDDATE` | `modifieddate` | `DATE` | 63.43% | 明细主水位 |
| MUST | `SETTIME` | `settime` | `DATE` | 36.59% | 设置时间；明细补充水位，覆盖 `MODIFIEDDATE` 为空的增量场景 |
| SHOULD | `M_ATTRIBUTESETINSTANCE_ID` | `m_attributesetinstance_id` | `NUMBER(10,0)` | 100% | ASI，颜色 / 尺码属性关联 |
| SHOULD | `ORDERNO` | `orderno` | `NUMBER(10,0)` | 45.89% | 序号 |
| SHOULD | `C_VIP_ID` | `c_vip_id` | `NUMBER(10,0)` | 0.19% | 明细会员候选；低覆盖但与业务销售底表相关 |
| SHOULD | `SALESREP_ID` | `salesrep_id` | `NUMBER(10,0)` | 0.19% | 营业员；低覆盖但与业务销售底表相关 |
| SHOULD | `DISCOUNT` | `discount` | `NUMBER(14,2)` | 100% | 明细折扣 |
| SHOULD | `DESCRIPTION` | `description` | `VARCHAR2(1530)` | 1.93% | 明细备注 |
| SHOULD | `STATUS` | `status` | `NUMBER(1,0)` | 100% | 状态 |
| SHOULD | `TYPE` | `type` | `NUMBER(1,0)` | 100% | 零售类型 |
| SHOULD | `RQTY` | `rqty` | `NUMBER(10,0)` | 100% | 已退数量，默认 0 |
| SHOULD | `SALESREPS_ID` | `salesreps_id` | `VARCHAR2(200)` | 36.58% | 多营业员 ID；同组字段 `SALESREPS_NAME` 显示名为“营业员(多选)” |
| SHOULD | `SALESREPS_NAME` | `salesreps_name` | `VARCHAR2(200)` | 36.58% | 营业员(多选) |
| SHOULD | `RCANQTY` | `rcanqty` | `NUMBER(10,0)` | 100% | 可退数量 |
| WATCH | `M_RETAILITEM_ID` | `m_retailitem_id` | `NUMBER(10,0)` | 16.73% | 原零售单明细ID |

剔除记录：

| 分级 | Oracle 字段 | 原建议 MySQL 字段 | 剔除依据 |
|------|-------------|--------------------|----------|
| EXCLUDE | `RETURNQTY` | `returnqty` | 全表 3,137,134 行均为 0；真实退货数量口径使用 `RQTY` / `RCANQTY` 与正负数量标识 |
| EXCLUDE | `ORG_M_RETAILITEM_ID` | `org_m_retailitem_id` | 全表 3,137,134 行均为空；`M_RETAILITEM_ID` 已保留为有真实数据的原零售单明细ID |

冻结结论：`M_RETAILITEM` 第一批白名单覆盖当前 ODS 11 个已用字段，并新增属性实例、行序号、折扣、明细状态 / 类型、真实退货相关字段和多营业员字段；`RETURNQTY`、`ORG_M_RETAILITEM_ID` 已按全量非零值证据从 raw ODS / DWD 草案剔除。

### 3.4 `FA_STORAGE` 库存字段白名单

`FA_STORAGE` 当前结构只有 24 个字段，但 M2.5 的“全部有数据”只说明字段非空，不等同于字段有真实业务值。2026-04-30 全量非零值扫描确认 `QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 全量为 0；用户确认这类 Oracle 模板化冗余字段不进入新架构。因此 ODS raw 层不再保留全字段，而是只保留真实有数据、语义明确且对库存 DWD 有价值的字段。

| 分级 | Oracle 字段 | 建议 MySQL 字段 | Oracle 类型 | 覆盖率证据 | 用途 |
|------|-------------|-----------------|-------------|------------|------|
| MUST | `ID` | `id` | `NUMBER(10,0)` | 100% | 库存源行主键 |
| MUST | `C_STORE_ID` | `c_store_id` | `NUMBER(10,0)` | 100% | 店仓关联 |
| MUST | `M_PRODUCT_ID` | `m_product_id` | `NUMBER(10,0)` | 100% | 商品关联 |
| MUST | `M_PRODUCTALIAS_ID` | `m_productalias_id` | `NUMBER(10,0)` | 100% | SKU / 条码关联 |
| MUST | `M_ATTRIBUTESETINSTANCE_ID` | `m_attributesetinstance_id` | `NUMBER(10,0)` | 100% | 属性实例关联 |
| MUST | `QTY` | `qty` | `NUMBER(18,0)` | 100% | 库存数量 |
| MUST | `QTYPURCHASEREM` | `qtypurchaserem` | `NUMBER(10,0)` | 非零 766 行 | 采购未入剩余数量 / 采购欠数 |
| MUST | `ISACTIVE` | `isactive` | `CHAR(1)` | 100% | 有效标识 |
| SHOULD | `CREATIONDATE` | `creationdate` | `DATE` | 100% | 源创建时间 |
| SHOULD | `MODIFIEDDATE` | `modifieddate` | `DATE` | 100% | 源修改时间 / 水位候选 |
| SHOULD | `QTYPREOUT` | `qtypreout` | `NUMBER(18,0)` | 100% | 在单数量 |
| SHOULD | `QTYPREIN` | `qtyprein` | `NUMBER(18,0)` | 100% | 在途数量 |
| SHOULD | `QTY_FREEZE` | `qty_freeze` | `NUMBER(18,0)` | 100% | 已冻结量 |
| SHOULD | `QTY_OMS` | `qty_oms` | `NUMBER(10,0)` | 非零 162 行 | OMS冻结量 |
| WATCH | `QTYOMSTRANSLATE` | `qtyomstranslate` | `NUMBER(10,0)` | 非零 15 行 | OMS转换占用 / 调整数量，低覆盖 |
| WATCH | `QTYPREOUT1` | `qtypreout1` | `NUMBER(18,0)` | 非零 20 行 | 备用预出调整数量，极低覆盖且当前全为负数 |
| WATCH | `AD_CLIENT_ID`、`AD_ORG_ID`、`OWNERID`、`MODIFIERID` | 同名小写 | 100% | 源系统审计 / 组织字段，DWD 暂不强依赖 |

剔除记录：

| 分级 | Oracle 字段 | 原建议 MySQL 字段 | 剔除依据 |
|------|-------------|--------------------|----------|
| EXCLUDE | `QTYVALID` | `qtyvalid` | 全表 201,607 行均为 0；当前 DWS 的 `qty_valid` 实际等价 `QTY`，DWD 对账中用 `qty` 生成等价值 |
| EXCLUDE | `QTY_BAS` | `qty_bas` | 全表 201,607 行均为 0 |
| EXCLUDE | `QTY_BAS_PREOUT` | `qty_bas_preout` | 全表 201,607 行均为 0 |
| EXCLUDE | `QTYDIRTY` | `qtydirty` | 全表 201,607 行均为 0 |

冻结结论：库存 ODS 仍适合先做 raw 旁路，但不再盲目保留 `FA_STORAGE` 全字段；`dwd_inventory_storage_snapshot` 第一阶段消费库存数量、水位、范围标识和真实有非零值的库存信号，审计字段可留在 raw，全量为 0 的模板字段已剔除。

---

## 4. ODS 扩展方案比较

| 维度 | 兼容扩字段：直接扩 `ods_m_retail` / `ods_m_retailitem` / `ods_fa_storage` | 旁路 raw 表：新增 `ods_m_retail_raw` / `ods_m_retailitem_raw` / `ods_fa_storage_raw` |
|------|--------------------------------------------------|--------------------------------------------------------|
| 对现有生产链路影响 | 中等。新增 nullable 字段通常不破坏当前 `to_sql`，但修改 ODS ETL 填充字段会影响现有主链 | 低。现有 ODS / DWS / ADS 不动，raw 只服务 DWD 旁路验证 |
| 字段扩展灵活度 | 中。宽表扩多后可能使当前 ODS 语义混杂 | 高。raw 可按源表保留更多字段，DWD 再选择消费 |
| 回滚难度 | 中。ALTER 后回滚字段较麻烦，且可能影响历史数据 | 低。可直接停用旁路脚本和 raw 表，不影响当前链路 |
| 存储成本 | 较低。只扩现有表 | 较高。会重复落部分源数据 |
| 增量 / 水位治理 | 复用现有 `ods_sync_state` 较方便，但需防止当前水位语义被改坏 | 可独立使用 `ods_sync_state` 新表名或独立状态，隔离风险 |
| DWD 验证效率 | 中。DWD 可直接读扩展后 ODS，但需要先完成 ALTER 和 ODS ETL 改造 | 高。可先按 raw 小窗口落地，再逐步验证 DWD，不影响主链 |
| 适合阶段 | 当前 ODS 字段少、扩展少、确定性强时 | M3 / M4 旁路验证、源字段仍需复核时 |

### 4.1 本轮推荐

短期 M3 推荐优先采用“旁路 `ods_*_raw` 表”方案；2026-04-29 用户已确认采用该方案。原因：

1. 当前 `ods_m_retail`、`ods_m_retailitem`、`ods_fa_storage` 已服务现有主链 DWS / ADS，不应在字段语义尚未全部复核前直接改生产 ODS。
2. `M_RETAIL` / `M_RETAILITEM` 源字段很宽，且仍有少量字段本轮字典未命中中文显示名；raw 层更适合承接追溯和试错。
3. `FA_STORAGE` 字段少但存在模板化冗余，raw 层应快速保留真实有数据且语义明确的库存源事实，同时剔除全量为 0 / 全量为空字段，保护后续库存 DWD 的长期质量。
4. 旁路方案天然满足“不影响总控”的要求；后续若验证稳定，再决定是否把 raw 收敛回生产 ODS 或保留为长期 ODS raw 层。

兼容扩字段方案不废弃，适合作为 M4 之后的收敛方案：当字段语义、DWD 对账、历史回填和超时边界都验证后，再把稳定字段回灌到正式 ODS。

---

## 5. DWD DDL 草案产物

| 目标表 | 草案文件 | 状态 | 设计说明 |
|--------|----------|------|----------|
| `dwd_sales_retail_item` | `SQL/draft_create_dwd_sales_retail_item.sql` | 已由用户人工建表；20260428-20260430 已完成 5103 行完整业务日期 upsert；未接调度 | 一行一条 `M_RETAILITEM` 明细，带 `M_RETAIL` 单头上下文、会员 / 营业员 / 支付 / 退货候选字段和 DWS 过滤标识 |
| `dwd_inventory_storage_snapshot` | `SQL/draft_create_dwd_inventory_storage_snapshot.sql` | 已由用户人工建表；20260507 已完成 201946 行 full raw 快照 upsert；未接调度 | 一行一个 `snapshot_date + FA_STORAGE.ID` 库存源行，保留全店仓库存信号和当前 DWS 消费范围标识 |

上述 DWD 表已由用户人工完成建表，且已按授权完成旁路装载验证，但仍未形成生产数据契约。线上表注释旧字样已由用户人工执行 `SQL/alter_m3_raw_dwd_update_table_comments_after_create.sql` 修正；后续任何 ALTER / 索引 / 生产回填 / 调度接入仍需另行授权并注意 metadata lock。

---

## 6. 旁路 ETL 与对账产物

### 6.1 raw ODS DDL 草案

| 目标 raw 表 | 草案文件 | 状态 | 设计说明 |
|-------------|----------|------|----------|
| `ods_m_retail_raw` | `SQL/draft_create_ods_m_retail_raw.sql` | 已由用户人工建表；20260428-20260430 已补齐 2861 行完整业务日期单头 raw；未接调度 | 保留 `M_RETAIL` 单头主键、单号、日期、店仓、金额、状态、水位、支付、退货和追溯字段 |
| `ods_m_retailitem_raw` | `SQL/draft_create_ods_m_retailitem_raw.sql` | 已由用户人工建表；20260428-20260430 已补齐 5103 行完整业务日期明细 raw；未接调度 | 保留 `M_RETAILITEM` 明细主键、单头关联、商品 / SKU / 属性、数量、金额、双水位、退货和多营业员字段 |
| `ods_fa_storage_raw` | `SQL/draft_create_ods_fa_storage_raw.sql` | 已由用户人工建表；20260507 已完成 201946 行 full raw 初始化；未接调度 | 保留 `FA_STORAGE` 中真实有数据且语义明确的库存源字段及 ODS 批次字段，剔除全量为 0 的模板字段，服务全店仓库存 DWD 验证 |

### 6.2 raw ODS 装载脚本

| 脚本 | 默认行为 | 目标 |
|------|----------|------|
| `etl_ods_m_retail_raw.py` | 默认 dry-run 打印 `M_RETAIL` 候选抽取 SQL 与写入边界；`--conn-test` 只做 Oracle / MySQL `SELECT 1`；显式 `--execute` 后按主键 upsert；支持 `incremental`、`business-date`、`full` 模式 | 为 `ods_m_retail_raw` 小窗口与完整业务日期补齐提供受控入口 |
| `etl_ods_m_retailitem_raw.py` | 默认 dry-run 打印 `M_RETAILITEM` 候选抽取 SQL 与写入边界；`--conn-test` 只做 Oracle / MySQL `SELECT 1`；显式 `--execute` 后按主键 upsert；支持双水位 `incremental`、按 `M_RETAIL.BILLDATE` 补齐的 `business-date` 与 `full` 模式 | 为 `ods_m_retailitem_raw` 双水位小窗口与完整业务日期补齐提供受控入口 |
| `etl_ods_fa_storage_raw.py` | 默认 dry-run 打印 `FA_STORAGE` modified-window / full 候选抽取 SQL 与写入边界；`--conn-test` 只做 Oracle / MySQL `SELECT 1`；显式 `--execute --confirm-full-load` 后可按主键执行 full raw upsert | 为 `ods_fa_storage_raw` 小窗口验证与 full raw 初始化提供受控入口；modified-window 不等同库存全量快照 |

### 6.3 DWD ETL 小窗口装载

| 脚本 | 默认行为 | 目标 |
|------|----------|------|
| `etl_dwd_sales_retail_item.py` | 默认只打印候选 `INSERT ... SELECT` SQL；`--conn-test` 只做 MySQL `SELECT 1`；显式 `--execute` 后先检查 raw 非空再 upsert | 从 `ods_m_retailitem_raw` + `ods_m_retail_raw` + `dim_store` 生成 `dwd_sales_retail_item` 小窗口或完整业务日期数据 |
| `etl_dwd_inventory_storage_snapshot.py` | 默认只打印候选 `INSERT ... SELECT` SQL；`--conn-test` 只做 MySQL `SELECT 1`；显式 `--execute` 后先检查 raw 非空再 upsert | 从 `ods_fa_storage_raw` + `dim_store` 生成 `dwd_inventory_storage_snapshot` 小窗口或 full raw 快照数据 |

### 6.4 DWD 小窗口对账 SQL

| 对账文件 | 状态 | 校验目标 |
|----------|------|----------|
| `SQL/check_dwd_sales_retail_item_min.sql` | 只读 SQL；已执行等效对账 | 校验 `dwd_sales_retail_item` 主键重复、DWS scope 行数、缺头 / 缺 SKU 行、销售数量 / 金额 / 退货 / 订单数与 `dws_sales_daily` 的小窗口差异 |
| `SQL/check_dwd_inventory_storage_snapshot_min.sql` | 只读 SQL；已执行等效对账 | 校验 `dwd_inventory_storage_snapshot` 主键重复、库存范围标识、库存数量、`qty_valid` 等价值、采购欠数与 `dws_inventory_daily` 的快照差异，并概览 DWS 暂不消费的库存信号 |

骨架约束：

1. 不导入 `run_etl.py`，不接入 `STEP_ORDER`。
2. 销售 raw / DWD 小窗口默认 `timeout_profile='etl'`；库存 raw 全量候选默认 `timeout_profile='long_running'`；历史大窗口 / 全量回填必须保留耗时证据。
3. 默认 dry-run 不写库；只有显式 `--execute` 才写入，且不接调度。
4. 近 1 天小窗口验证与 2026-05-07 补完整业务日期 / full raw 初始化结果均已落盘；库存与 `dws_inventory_daily` 的 `qty` 差异来自生产 ODS/DWS 快照时间点早于本次 Oracle full raw 初始化。

---

## 7. 超时、锁与验证设计

| 项 | 建议 |
|----|------|
| 数据量 | 当前 MySQL `ods_m_retail` 约 188 万行、`ods_m_retailitem` 约 313 万行、`ods_fa_storage` 约 20 万行；DWD 历史回填不能用默认短超时 |
| `timeout_profile` | 小窗口旁路验证使用 `etl`；历史回填 / 全量重算使用 `long_running` |
| 写入策略 | 本轮 raw ODS 与 DWD 采用主键 upsert，不做窗口级 DELETE；后续生产化可再评估受影响 `date_id` 删除重写、staging + upsert 或库存 `snapshot_date` 重算 |
| 锁名 | 销售建议 `hefang_dw:dwd_sales_retail_item`；库存建议 `hefang_dw:dwd_inventory_storage_snapshot` |
| 最小验证 | 主键重复、行数、金额 / 数量、DWS scope 标识行数、当前 DWS 聚合差异、字段非空覆盖率；草案见 `SQL/check_dwd_sales_retail_item_min.sql` 与 `SQL/check_dwd_inventory_storage_snapshot_min.sql` |
| 回滚 | 旁路表与 DWD 表未接总控，回滚优先停脚本；若已人工建表，可由用户决定 DROP / RENAME / 保留 |

---

## 8. 下一步待办

| 顺序 | 待办 | 产物 |
|------|------|------|
| 1 | 已完成：由用户人工执行表注释修正 SQL，清理线上表注释中的“草案 / 未执行”旧字样 | `SQL/alter_m3_raw_dwd_update_table_comments_after_create.sql` 已作为留档；information_schema 已核验 |
| 2 | 已完成：Copilot 在用户授权后补 raw ODS 真实写入实现，并完成近 1 天窗口装载 | `reports/context_cache/m3_raw_dwd_small_window_load_20260430.json`；销售用 `timeout_profile='etl'`，库存 modified-window 用 `long_running` |
| 3 | 已完成：Copilot 在用户授权后补 DWD 真实写入实现，并做最小只读对账 | DWD 对 DWS 差异已记录；差异来自 modified-window 非全量验证边界，不能解释为完整日级 / 全量库存对账 |
| 4 | 已完成：补完整业务日期销售 raw 与库存 full raw 初始化，并重算旁路 DWD 对账 | `reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json` |
| 5 | 已完成：基于已验证的销售日级对账和库存 raw/DWD 自洽结果，输出 DWS v2 / 调度接入方案，完成用户人工建表后的空表核验、dry-run / conn-test / S3 手工写入分支，并完成 S3 实跑验收 | `08_M4_DWS_v2并行表_调度接入与回滚方案.md`、`SQL/draft_create_dws_sales_daily_v2.sql`、`SQL/draft_create_dws_inventory_daily_v2.sql`、`SQL/check_dws_v2_parallel_reconciliation.sql`、`dws_v2_write_utils.py`、`etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py`、`test_dws_v2_dry_run.py`、`reports/context_cache/dws_v2_manual_ddl_verification_20260507.json`、`reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json`、`reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json`；用户已人工建表，脚本默认不写库，但已在用户明确授权下完成一次受控 S3 写入验收 / 未调度修改 |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.2 | 2026-05-07 | 补记 DWS v2 已完成 S3 实跑验收，作为 M3→M4 续接证据：销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0 |
| v1.1 | 2026-05-07 | 补记 DWS v2 S3 手工写入分支已新增：默认 dry-run，写入需确认令牌、命名锁、事务和 DWD-v2 对账；本轮未执行真实写入 / 未接总控 |
| v1.0 | 2026-05-07 | 补记 DWS v2 dry-run / conn-test 脚本已新增且无写库入口，当前仍未写 v2 数据 / 未接总控 |
| v0.9 | 2026-05-07 | 补记 DWS v2 两张并行表已由用户人工建表并完成空表核验，当前仍未写 v2 数据 / 未接总控 |
| v0.8 | 2026-05-07 | 补记 M4 DWS v2 并行表、调度接入与回滚方案已输出；M3 仍未接总控 |
| v0.7 | 2026-05-07 | 记录销售完整业务日期 raw 补齐、库存 full raw 初始化、DWD 重算与对账结论；仍未接调度 |
| v0.6 | 2026-04-30 | 记录用户已人工修正线上表注释，并完成 raw ODS 近 1 天小窗口装载、DWD upsert 与最小对账；仍未接调度 |
| v0.5 | 2026-04-30 | 记录用户已人工完成 5 张 M3 raw / DWD 表建表，并将状态校准为已建空表、未装载、未接调度 |
| v0.4 | 2026-04-30 | 按 Oracle 全量非零值扫描和用户确认的真实字段原则，剔除 `RETURNQTY`、`ORG_M_RETAILITEM_ID`、`QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 等模板冗余字段，并更新库存对账边界 |
| v0.3 | 2026-04-30 | 按 ERP AD_COLUMN 字典和 FA_STORAGE 平台截图校准 raw ODS / DWD 草案字段语义，减少“语义待确认”项 |
| v0.2 | 2026-04-29 | 写入用户已确认旁路 `ods_*_raw` 方案；补充 raw ODS DDL、raw ODS 抽取骨架和 DWD 小窗口对账 SQL 草案产物 |
| v0.1 | 2026-04-29 | 新增三张 Oracle 源表 ODS 扩展字段白名单、ODS 扩展方案比较、DWD DDL 草案与旁路 ETL 骨架说明 |