-- ODS raw draft DDL for Oracle BOSNDS3.FA_STORAGE.
-- Status: DDL 已由用户人工执行建表；当前空表；未装载数据；未接入调度。
-- Execution boundary: 后续 ALTER / 写库 / 回填 / 调度接入仍由用户人工执行或另行授权；Agent 不代执行。
-- Evidence:
--   docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md
--   用户提供 ERP 开发平台 FA_STORAGE 字段截图（2026-04-30）
--   reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json
--   reports/context_cache/m3_manual_ddl_verification_20260430.json
--   reports/snapshot_oracle_bosnds3_schema.json
--   reports/oracle_bosnds3_core_field_profile_202604.json

CREATE TABLE IF NOT EXISTS ods_fa_storage_raw (
    id BIGINT NOT NULL COMMENT 'Oracle FA_STORAGE.ID，库存源行主键',
    ad_client_id BIGINT NULL COMMENT 'Oracle FA_STORAGE.AD_CLIENT_ID，源系统租户字段',
    ad_org_id BIGINT NULL COMMENT 'Oracle FA_STORAGE.AD_ORG_ID，源系统组织字段',
    ownerid BIGINT NULL COMMENT 'Oracle FA_STORAGE.OWNERID，创建人',
    modifierid BIGINT NULL COMMENT 'Oracle FA_STORAGE.MODIFIERID，修改人',
    creationdate DATETIME NULL COMMENT 'Oracle FA_STORAGE.CREATIONDATE，源创建时间',
    modifieddate DATETIME NULL COMMENT 'Oracle FA_STORAGE.MODIFIEDDATE，源修改时间 / 水位候选',
    isactive CHAR(1) NOT NULL COMMENT 'Oracle FA_STORAGE.ISACTIVE，源有效标识',
    c_store_id BIGINT NOT NULL COMMENT 'Oracle FA_STORAGE.C_STORE_ID，店仓 ID',
    m_product_id BIGINT NOT NULL COMMENT 'Oracle FA_STORAGE.M_PRODUCT_ID，商品 ID',
    m_attributesetinstance_id BIGINT NULL COMMENT 'Oracle FA_STORAGE.M_ATTRIBUTESETINSTANCE_ID，属性实例 ID',
    qty DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTY，库存数量',
    qtypreout DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTYPREOUT，在单数量',
    qtyprein DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTYPREIN，在途数量',
    m_productalias_id BIGINT NULL COMMENT 'Oracle FA_STORAGE.M_PRODUCTALIAS_ID，SKU / 条码 ID',
    qty_freeze DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTY_FREEZE，已冻结量',
    qty_oms DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTY_OMS，OMS冻结量',
    qtypurchaserem DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTYPURCHASEREM，采购未入剩余数量 / 采购欠数',
    qtyomstranslate DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTYOMSTRANSLATE，OMS转换占用 / 调整数量，低覆盖',
    qtypreout1 DECIMAL(18,4) NULL COMMENT 'Oracle FA_STORAGE.QTYPREOUT1，备用预出调整数量，极低覆盖且当前全为负数',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ODS raw 装载批次 ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS raw 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_ods_fa_storage_raw_store_sku (c_store_id, m_productalias_id),
    KEY idx_ods_fa_storage_raw_product_sku (m_product_id, m_productalias_id),
    KEY idx_ods_fa_storage_raw_modifieddate (modifieddate),
    KEY idx_ods_fa_storage_raw_isactive (isactive)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS raw 库存余额，来源 Oracle FA_STORAGE，M3 已人工建空表';
