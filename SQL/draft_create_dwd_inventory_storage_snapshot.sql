-- DWD inventory storage snapshot draft DDL.
-- Status: DDL 已由用户人工执行建表；当前空表；未装载数据；未接入调度。
-- Execution boundary: 后续 ALTER / 写库 / 回填 / 调度接入仍由用户人工执行或另行授权；Agent 不代执行。
-- Evidence:
--   docs/ODS-DWD-DWS-ADS架构完善子项目/06_M2_5_ORACLE源库画像与ODS_DWD规划.md
--   用户提供 ERP 开发平台 FA_STORAGE 字段截图（2026-04-30）
--   reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json
--   reports/context_cache/m3_manual_ddl_verification_20260430.json
--   reports/oracle_bosnds3_core_field_profile_202604.json

CREATE TABLE IF NOT EXISTS dwd_inventory_storage_snapshot (
    snapshot_date INT NOT NULL COMMENT '快照日期，YYYYMMDD',
    storage_id BIGINT NOT NULL COMMENT '库存源行 ID，来源 FA_STORAGE.ID',
    store_id BIGINT NOT NULL COMMENT '店仓 ID，来源 FA_STORAGE.C_STORE_ID',
    store_code VARCHAR(40) NULL COMMENT '店仓编码，来源 dim_store.store_code',
    is_cloud_store CHAR(1) NULL COMMENT '是否云仓门店，来源 dim_store.is_cloud_store',
    product_id BIGINT NOT NULL COMMENT '商品 ID，来源 FA_STORAGE.M_PRODUCT_ID',
    m_productalias_id BIGINT NULL COMMENT 'SKU / 条码 ID，来源 FA_STORAGE.M_PRODUCTALIAS_ID',
    attribute_set_instance_id BIGINT NULL COMMENT '属性实例 ID，来源 FA_STORAGE.M_ATTRIBUTESETINSTANCE_ID',

    qty DECIMAL(18,4) NULL COMMENT '库存数量，来源 FA_STORAGE.QTY',
    qty_preout DECIMAL(18,4) NULL COMMENT '在单数量，来源 FA_STORAGE.QTYPREOUT',
    qty_prein DECIMAL(18,4) NULL COMMENT '在途数量，来源 FA_STORAGE.QTYPREIN',
    qty_freeze DECIMAL(18,4) NULL COMMENT '已冻结量，来源 FA_STORAGE.QTY_FREEZE',
    qty_oms DECIMAL(18,4) NULL COMMENT 'OMS冻结量，来源 FA_STORAGE.QTY_OMS',
    qty_purchase_rem DECIMAL(18,4) NULL COMMENT '采购未入剩余数量 / 采购欠数，来源 FA_STORAGE.QTYPURCHASEREM',
    qty_oms_translate DECIMAL(18,4) NULL COMMENT 'OMS转换占用 / 调整数量，来源 FA_STORAGE.QTYOMSTRANSLATE，低覆盖',
    qty_preout1 DECIMAL(18,4) NULL COMMENT '备用预出调整数量，来源 FA_STORAGE.QTYPREOUT1，极低覆盖且当前全为负数',

    storage_isactive CHAR(1) NOT NULL DEFAULT 'Y' COMMENT '库存源行有效标识，来源 FA_STORAGE.ISACTIVE',
    is_active_storage_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否有效库存源行，Y/N',
    has_sku_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否有 SKU，Y/N',
    is_total_warehouse_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否总仓，当前以 dim_store.store_code = 001 判断',
    is_cloud_store_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否云仓门店，Y/N',
    dws_inventory_scope_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否进入当前库存 DWS 消费范围，Y/N',
    zero_qty_kept_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否保留 0 库存行，Y/N',
    negative_qty_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否负库存行，Y/N',

    storage_created_at DATETIME NULL COMMENT '库存源行创建时间，来源 FA_STORAGE.CREATIONDATE',
    storage_modified_at DATETIME NULL COMMENT '库存源行修改时间，来源 FA_STORAGE.MODIFIEDDATE',
    source_loaded_at DATETIME NULL COMMENT 'ODS 装载时间',
    source_batch_id VARCHAR(64) NULL COMMENT 'ODS 批次 ID',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DWD 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (snapshot_date, storage_id),
    KEY idx_dwd_inventory_storage_snapshot_store_sku (snapshot_date, store_id, m_productalias_id),
    KEY idx_dwd_inventory_storage_snapshot_product (snapshot_date, product_id, m_productalias_id),
    KEY idx_dwd_inventory_storage_snapshot_scope (snapshot_date, dws_inventory_scope_flag, is_active_storage_flag),
    KEY idx_dwd_inventory_storage_snapshot_watermark (storage_modified_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD 库存店仓快照事实，M3 已人工建空表，未接调度';