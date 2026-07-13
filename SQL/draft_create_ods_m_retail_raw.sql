-- ODS raw draft DDL for Oracle BOSNDS3.M_RETAIL.
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

CREATE TABLE IF NOT EXISTS ods_m_retail_raw (
    id BIGINT NOT NULL COMMENT 'Oracle M_RETAIL.ID，单头主键',
    docno VARCHAR(80) NULL COMMENT 'Oracle M_RETAIL.DOCNO，单据编号',
    billdate INT NULL COMMENT 'Oracle M_RETAIL.BILLDATE，单据日期，YYYYMMDD',
    c_store_id BIGINT NULL COMMENT 'Oracle M_RETAIL.C_STORE_ID，店仓',
    oms_sourcecode TEXT NULL COMMENT 'Oracle M_RETAIL.OMS_SOURCECODE，WING平台单号，源字段 VARCHAR2(4000)',
    tot_amt_actual DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAIL.TOT_AMT_ACTUAL，总成交金额',
    tot_amt_list DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAIL.TOT_AMT_LIST，总零售金额',
    tot_qty DECIMAL(18,4) NULL COMMENT 'Oracle M_RETAIL.TOT_QTY，总数量',
    status INT NULL COMMENT 'Oracle M_RETAIL.STATUS，提交状态',
    isactive CHAR(1) NOT NULL COMMENT 'Oracle M_RETAIL.ISACTIVE，可用',
    modifieddate DATETIME NULL COMMENT 'Oracle M_RETAIL.MODIFIEDDATE，单头修改时间 / 水位',
    creationdate DATETIME NULL COMMENT 'Oracle M_RETAIL.CREATIONDATE，源创建时间',
    doctype CHAR(3) NULL COMMENT 'Oracle M_RETAIL.DOCTYPE，单据类型',
    description VARCHAR(765) NULL COMMENT 'Oracle M_RETAIL.DESCRIPTION，单头备注',
    avg_discount DECIMAL(18,6) NULL COMMENT 'Oracle M_RETAIL.AVG_DISCOUNT，单头平均折扣',
    c_vip_id BIGINT NULL COMMENT 'Oracle M_RETAIL.C_VIP_ID，VIP',
    salesrep_id BIGINT NULL COMMENT 'Oracle M_RETAIL.SALESREP_ID，零售员',
    pay_status INT NULL COMMENT 'Oracle M_RETAIL.PAY_STATUS，支付状态',
    payerid BIGINT NULL COMMENT 'Oracle M_RETAIL.PAYERID，支付操作人',
    paytime DATETIME NULL COMMENT 'Oracle M_RETAIL.PAYTIME，支付时间',
    close_status INT NULL COMMENT 'Oracle M_RETAIL.CLOSE_STATUS，关闭状态',
    closerid BIGINT NULL COMMENT 'Oracle M_RETAIL.CLOSERID，关闭操作人，低覆盖，追溯字段',
    closetime DATETIME NULL COMMENT 'Oracle M_RETAIL.CLOSETIME，关闭时间，低覆盖，追溯字段',
    refno VARCHAR(255) NULL COMMENT 'Oracle M_RETAIL.REFNO，POS零售单号',
    isreturned VARCHAR(255) NULL COMMENT 'Oracle M_RETAIL.ISRETURNED，是否已退货，默认 N',
    retailbilltype VARCHAR(3) NULL COMMENT 'Oracle M_RETAIL.RETAILBILLTYPE，零售单类型',
    dateout INT NULL COMMENT 'Oracle M_RETAIL.DATEOUT，出库日期，通常等于单据日期',
    datein INT NULL COMMENT 'Oracle M_RETAIL.DATEIN，入库日期，通常等于单据日期',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ODS raw 装载批次 ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS raw 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY idx_ods_m_retail_raw_billdate (billdate),
    KEY idx_ods_m_retail_raw_modifieddate (modifieddate),
    KEY idx_ods_m_retail_raw_store_date (c_store_id, billdate),
    KEY idx_ods_m_retail_raw_docno (docno),
    KEY idx_ods_m_retail_raw_oms_sourcecode (oms_sourcecode(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS raw 零售单头，来源 Oracle M_RETAIL，M3 已人工建空表';
