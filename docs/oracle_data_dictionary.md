# Oracle 数据域 — 精简数据字典（hefang_dw 项目相关）

说明：本文件为仓库内代码引用证据支持的精简数据字典，仅列出在 `hefang_dw` 项目中出现且常用的 Oracle 源表（及相关衍生表）。每条记录附带引用文件以便追溯。

---

## M_RETAIL — 销售单（单头）
- 描述：销售/零售单头（订单级别信息）。
- 典型字段：ID, DOCNO, BILLDATE, C_STORE_ID, OMS_SOURCECODE, TOT_AMT_ACTUAL, TOT_AMT_LIST, TOT_QTY, STATUS, ISACTIVE, MODIFIEDDATE
- 证据代码：[etl_ods_m_retail.py](etl_ods_m_retail.py#L90-L120)

## M_RETAILITEM — 销售明细（单明细）
- 描述：销售明细行（商品层面数量、单价、明细金额等）。
- 典型字段：ID, M_RETAIL_ID, M_PRODUCT_ID, M_PRODUCTALIAS_ID, QTY, PRICELIST, PRICEACTUAL, TOT_AMT_ACTUAL, TOT_AMT_LIST, MODIFIEDDATE, SETTIME
- 证据代码：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L1-L80)

## M_PRODUCT — 商品主表
- 描述：商品维度（产品主键、编码、名称、品类、品牌、成本等）。
- 典型字段：ID, NAME / VALUE (编码/名称), M_DIM4_ID (类目), M_DIM1_ID (品牌), PRICELIST, PRECOST, ISACTIVE, CREATIONDATE
- 证据代码：[etl_dim_product.py](etl_dim_product.py#L20-L60)

## M_PRODUCT_ALIAS (M_PRODUCTALIAS) — SKU / 条码表
- 描述：SKU/条码维度（条码、关联 product、颜色/尺寸属性）。
- 典型字段：ID (sku_id), NO (sku_barcode), M_PRODUCT_ID, M_ATTRIBUTESETINSTANCE_ID, ISACTIVE
- 证据代码：[etl_dim_sku.py](etl_dim_sku.py#L1-L60)

## FA_STORAGE — 库存原表（M3 源）
- 描述：库存 / 店仓级可用量与在途信息（用于生成 DWD/DWS 库存快照）。
- 典型字段：ID, C_STORE_ID, M_PRODUCT_ID, M_PRODUCTALIAS_ID, M_ATTRIBUTESETINSTANCE_ID, QTY, QTYPREOUT, QTYPREIN, QTY_FREEZE, QTY_OMS, QTYPURCHASEREM, CREATIONDATE, MODIFIEDDATE
- 证据代码：[etl_ods_fa_storage_raw.py](etl_ods_fa_storage_raw.py#L1-L80)

## dwd_inventory_storage_snapshot — DWD 库存快照（衍生）
- 描述：按 `snapshot_date + storage_id` 聚合的 DWD 库存快照，包含快照元信息与库存量字段（由 `ods_fa_storage`/`ods_fa_storage_raw` 生成）。
- 典型字段：snapshot_date, storage_id, store_id, product_id, m_productalias_id, qty, etl_time 等
- 证据代码：[etl_dwd_inventory_storage_snapshot.py](etl_dwd_inventory_storage_snapshot.py#L1-L120)

---

备注：如果你需要我把每个表的完整字段（按代码中出现的列名）展开到 CSV 的每一列，我可以继续扫描并生成完整字段清单并追加到 `docs/oracle_data_dictionary.csv`。