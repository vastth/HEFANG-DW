-- DWD sales retail item draft DDL.
-- Status: DDL 已由用户人工执行建表；当前空表；未装载数据；未接入调度。
-- Execution boundary: 后续 ALTER / 写库 / 回填 / 调度接入仍由用户人工执行或另行授权；Agent 不代执行。
-- Evidence:
--   docs/ODS-DWD-DWS-ADS架构完善子项目/06_M2_5_ORACLE源库画像与ODS_DWD规划.md
--   data/AD_COLUMN04301009.xlsx
--   reports/context_cache/ad_column_retail_raw_semantics_20260430.csv
--   reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json
--   reports/context_cache/m3_manual_ddl_verification_20260430.json
--   reports/oracle_bosnds3_core_field_profile_202604.json

CREATE TABLE IF NOT EXISTS dwd_sales_retail_item (
    retail_item_id BIGINT NOT NULL COMMENT '零售明细主键，来源 M_RETAILITEM.ID',
    retail_id BIGINT NOT NULL COMMENT '零售单头主键，来源 M_RETAILITEM.M_RETAIL_ID / M_RETAIL.ID',
    docno VARCHAR(80) NULL COMMENT '零售单号，来源 M_RETAIL.DOCNO',
    date_id INT NULL COMMENT '业务日期，YYYYMMDD，来源 M_RETAIL.BILLDATE',
    store_id BIGINT NULL COMMENT '店仓 ID，来源 M_RETAIL.C_STORE_ID',
    store_code VARCHAR(40) NULL COMMENT '店仓编码，来源 dim_store.store_code',
    is_cloud_store CHAR(1) NULL COMMENT '是否云仓门店，来源 dim_store.is_cloud_store',
    oms_sourcecode VARCHAR(512) NULL COMMENT 'OMS 来源编码，来源 M_RETAIL.OMS_SOURCECODE',
    retail_refno VARCHAR(255) NULL COMMENT '外部参考号，来源 M_RETAIL.REFNO',
    retail_doctype CHAR(3) NULL COMMENT '单据类型，来源 M_RETAIL.DOCTYPE',
    retail_bill_type VARCHAR(3) NULL COMMENT '零售单类型，来源 M_RETAIL.RETAILBILLTYPE',
    retail_description VARCHAR(765) NULL COMMENT '单头备注，来源 M_RETAIL.DESCRIPTION',

    product_id BIGINT NULL COMMENT '商品 ID，来源 M_RETAILITEM.M_PRODUCT_ID',
    m_productalias_id BIGINT NULL COMMENT 'SKU / 条码 ID，来源 M_RETAILITEM.M_PRODUCTALIAS_ID',
    attribute_set_instance_id BIGINT NULL COMMENT '属性实例 ID，来源 M_RETAILITEM.M_ATTRIBUTESETINSTANCE_ID',
    item_order_no BIGINT NULL COMMENT '明细行序号，来源 M_RETAILITEM.ORDERNO',

    qty DECIMAL(18,4) NULL COMMENT '明细数量，来源 M_RETAILITEM.QTY',
    price_list DECIMAL(18,4) NULL COMMENT '明细吊牌单价，来源 M_RETAILITEM.PRICELIST',
    price_actual DECIMAL(18,4) NULL COMMENT '明细成交单价，来源 M_RETAILITEM.PRICEACTUAL',
    discount_rate DECIMAL(18,6) NULL COMMENT '明细折扣，来源 M_RETAILITEM.DISCOUNT',
    line_actual_amt DECIMAL(18,4) NULL COMMENT '明细成交金额，来源 M_RETAILITEM.TOT_AMT_ACTUAL',
    line_list_amt DECIMAL(18,4) NULL COMMENT '明细吊牌金额，来源 M_RETAILITEM.TOT_AMT_LIST',
    r_qty DECIMAL(18,4) NULL COMMENT '已退数量，来源 M_RETAILITEM.RQTY，默认 0',
    r_can_qty DECIMAL(18,4) NULL COMMENT '可退数量，来源 M_RETAILITEM.RCANQTY',

    retail_actual_amt DECIMAL(18,4) NULL COMMENT '单头成交金额，来源 M_RETAIL.TOT_AMT_ACTUAL',
    retail_list_amt DECIMAL(18,4) NULL COMMENT '单头吊牌金额，来源 M_RETAIL.TOT_AMT_LIST',
    retail_total_qty DECIMAL(18,4) NULL COMMENT '单头总数量，来源 M_RETAIL.TOT_QTY',
    retail_avg_discount DECIMAL(18,6) NULL COMMENT '单头平均折扣，来源 M_RETAIL.AVG_DISCOUNT',

    retail_status INT NULL COMMENT '单头状态，来源 M_RETAIL.STATUS',
    retail_isactive CHAR(1) NULL COMMENT '单头有效标识，来源 M_RETAIL.ISACTIVE',
    item_status INT NULL COMMENT '明细状态，来源 M_RETAILITEM.STATUS',
    item_type INT NULL COMMENT '明细类型，来源 M_RETAILITEM.TYPE',
    is_returned VARCHAR(255) NULL COMMENT '是否已退货，来源 M_RETAIL.ISRETURNED，默认 N',
    related_retail_item_id BIGINT NULL COMMENT '原零售单明细ID，来源 M_RETAILITEM.M_RETAILITEM_ID',

    retail_vip_id BIGINT NULL COMMENT '单头会员 ID，来源 M_RETAIL.C_VIP_ID',
    item_vip_id BIGINT NULL COMMENT '明细会员 ID，来源 M_RETAILITEM.C_VIP_ID',
    retail_salesrep_id BIGINT NULL COMMENT '单头营业员 ID，来源 M_RETAIL.SALESREP_ID',
    item_salesrep_id BIGINT NULL COMMENT '明细营业员 ID，来源 M_RETAILITEM.SALESREP_ID',
    item_salesreps_id VARCHAR(200) NULL COMMENT '明细多营业员 ID，来源 M_RETAILITEM.SALESREPS_ID',
    item_salesreps_name VARCHAR(200) NULL COMMENT '明细多营业员名称，来源 M_RETAILITEM.SALESREPS_NAME',

    pay_status INT NULL COMMENT '支付状态，来源 M_RETAIL.PAY_STATUS',
    payer_id BIGINT NULL COMMENT '付款操作人，来源 M_RETAIL.PAYERID',
    pay_time DATETIME NULL COMMENT '付款时间，来源 M_RETAIL.PAYTIME',
    close_status INT NULL COMMENT '关闭状态，来源 M_RETAIL.CLOSE_STATUS',
    closer_id BIGINT NULL COMMENT '关闭操作人，来源 M_RETAIL.CLOSERID',
    close_time DATETIME NULL COMMENT '关闭时间，来源 M_RETAIL.CLOSETIME',

    has_retail_header_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否命中单头，Y/N',
    is_valid_retail_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否满足现有销售 DWS 有效单据口径，Y/N',
    has_sku_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否有 SKU，Y/N',
    is_positive_sale_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否正向销售行，Y/N',
    is_return_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否退货行，Y/N',
    dws_sales_scope_flag CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否进入当前销售 DWS 聚合范围，Y/N',

    retail_created_at DATETIME NULL COMMENT '单头创建时间，来源 M_RETAIL.CREATIONDATE',
    retail_modified_at DATETIME NULL COMMENT '单头修改时间，来源 M_RETAIL.MODIFIEDDATE',
    item_modified_at DATETIME NULL COMMENT '明细修改时间，来源 M_RETAILITEM.MODIFIEDDATE',
    item_set_time DATETIME NULL COMMENT '明细 SETTIME，来源 M_RETAILITEM.SETTIME',
    retail_source_loaded_at DATETIME NULL COMMENT '单头 ODS 装载时间',
    item_source_loaded_at DATETIME NULL COMMENT '明细 ODS 装载时间',
    retail_source_batch_id VARCHAR(64) NULL COMMENT '单头 ODS 批次 ID',
    item_source_batch_id VARCHAR(64) NULL COMMENT '明细 ODS 批次 ID',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DWD 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (retail_item_id),
    KEY idx_dwd_sales_retail_item_date_store_sku (date_id, store_id, m_productalias_id),
    KEY idx_dwd_sales_retail_item_retail_id (retail_id),
    KEY idx_dwd_sales_retail_item_product (product_id, m_productalias_id),
    KEY idx_dwd_sales_retail_item_flags (dws_sales_scope_flag, is_valid_retail_flag, has_sku_flag),
    KEY idx_dwd_sales_retail_item_watermark (retail_modified_at, item_modified_at, item_set_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD 销售零售明细事实，M3 已人工建空表，未接调度';