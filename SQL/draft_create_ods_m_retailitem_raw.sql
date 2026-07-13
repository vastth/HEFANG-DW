-- ODS raw draft DDL for Oracle BOSNDS3.M_RETAILITEM.
-- Status: DDL 已由用户人工执行建表；当前空表；未装载数据；未接入调度。
-- Execution boundary: 后续 ALTER / 写库 / 回填 / 调度接入仍由用户人工执行或另行授权；Agent 不代执行。
-- Evidence:
--   docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md
--   data/AD_COLUMN04301009.xlsx
--   reports/context_cache/ad_column_retail_raw_semantics_20260430.csv
--   reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json
--   reports/context_cache/m3_manual_ddl_verification_20260430.json
--   reports/snapshot_oracle_bosnds3_schema.json
--   reports/oracle_bosnds3_core_field_profile_202604.json

CREATE TABLE IF NOT EXISTS ods_m_retailitem_raw (
    id BIGINT NOT NULL COMMENT 'Oracle M_RETAILITEM.ID，明细主键',
    m_retail_id BIGINT NOT NULL COMMENT 'Oracle M_RETAILITEM.M_RETAIL_ID，零售单',
    m_product_id BIGINT NULL COMMENT 'Oracle M_RETAILITEM.M_PRODUCT_ID，商品',
    m_productalias_id BIGINT NULL COMMENT 'Oracle M_RETAILITEM.M_PRODUCTALIAS_ID，条码',
    m_attributesetinstance_id BIGINT NULL COMMENT 'Oracle M_RETAILITEM.M_ATTRIBUTESETINSTANCE_ID，ASI',
    qty DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.QTY，数量',
    pricelist DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.PRICELIST，零售价',
    priceactual DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.PRICEACTUAL，成交价',
    tot_amt_actual DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.TOT_AMT_ACTUAL，成交金额',
    tot_amt_list DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.TOT_AMT_LIST，零售金额',
    modifieddate DATETIME NULL COMMENT 'Oracle M_RETAILITEM.MODIFIEDDATE，明细修改时间 / 主水位',
    settime DATETIME NULL COMMENT 'Oracle M_RETAILITEM.SETTIME，设置时间 / 补充水位',
    orderno BIGINT NULL COMMENT 'Oracle M_RETAILITEM.ORDERNO，序号',
    c_vip_id BIGINT NULL COMMENT 'Oracle M_RETAILITEM.C_VIP_ID，明细会员 ID 候选',
    salesrep_id BIGINT NULL COMMENT 'Oracle M_RETAILITEM.SALESREP_ID，营业员',
    discount DECIMAL(18,6) NULL COMMENT 'Oracle M_RETAILITEM.DISCOUNT，折扣',
    description VARCHAR(1530) NULL COMMENT 'Oracle M_RETAILITEM.DESCRIPTION，明细备注',
    status INT NULL COMMENT 'Oracle M_RETAILITEM.STATUS，状态',
    type INT NULL COMMENT 'Oracle M_RETAILITEM.TYPE，零售类型',
    rqty DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.RQTY，已退数量，默认 0',
    salesreps_id VARCHAR(200) NULL COMMENT 'Oracle M_RETAILITEM.SALESREPS_ID，多营业员 ID；AD_COLUMN 本次命中同组 SALESREPS_NAME=营业员(多选)',
    salesreps_name VARCHAR(200) NULL COMMENT 'Oracle M_RETAILITEM.SALESREPS_NAME，多营业员名称',
    rcanqty DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAILITEM.RCANQTY，可退数量',
    m_retailitem_id BIGINT NULL COMMENT 'Oracle M_RETAILITEM.M_RETAILITEM_ID，原零售单明细ID',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ODS raw 装载批次 ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS raw 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_ods_m_retailitem_raw_retail_id (m_retail_id),
    KEY idx_ods_m_retailitem_raw_product_sku (m_product_id, m_productalias_id),
    KEY idx_ods_m_retailitem_raw_modifieddate (modifieddate),
    KEY idx_ods_m_retailitem_raw_settime (settime),
    KEY idx_ods_m_retailitem_raw_watermark (modifieddate, settime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS raw 零售明细，来源 Oracle M_RETAILITEM，M3 已人工建空表';
