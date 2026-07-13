-- DWS sales daily v2 parallel table draft DDL.
-- Status: DDL 草案已由用户人工执行；Copilot 已补 dry-run / conn-test / S3 手工写入分支，并在用户明确授权下完成一次 S3 实跑验收：20260428-20260430 共写入 3417 行，DWD-v2 mismatch 为 0。当前仍未接入 run_etl.py / scheduled_etl.py / scheduled_total_control.py。
-- Execution boundary: CREATE / ALTER / INDEX / INSERT / DELETE / 回填默认仍由用户人工执行；S3 脚本写入分支仅供显式授权下的受控执行，并通过确认令牌、MySQL 命名锁、显式事务和写后 DWD-v2 对账输出运行证据。
-- Evidence:
--   reports/context_cache/dws_v2_manual_ddl_verification_20260507.json
--   reports/context_cache/dws_v2_parallel_design_evidence_20260507.json
--   reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json
--   reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json
--   dws_v2_write_utils.py
--   etl_dws_sales_v2.py
--   etl_dws_sales.py
--   etl_dwd_sales_retail_item.py

CREATE TABLE IF NOT EXISTS dws_sales_daily_v2 (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    date_id INT NOT NULL COMMENT '业务日期，YYYYMMDD，来源 dwd_sales_retail_item.date_id',
    store_id BIGINT NOT NULL COMMENT '店仓 ID，来源 dwd_sales_retail_item.store_id',
    store_code VARCHAR(40) NOT NULL DEFAULT '' COMMENT '店仓编码，来源 dwd_sales_retail_item.store_code',
    is_cloud_store CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否云仓门店，来源 dwd_sales_retail_item.is_cloud_store',
    product_id BIGINT NOT NULL COMMENT '商品 ID，来源 dwd_sales_retail_item.product_id',
    m_productalias_id BIGINT NOT NULL COMMENT 'SKU / 条码 ID，来源 dwd_sales_retail_item.m_productalias_id',

    sales_qty DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '销售数量，按当前 DWS 正向销售行口径汇总',
    sales_amount DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '销售金额，按当前 DWS 正向销售行口径汇总',
    sales_amount_list DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '吊牌金额，按当前 DWS 正向销售行口径汇总',
    return_qty DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '退货数量，按当前 DWS 退货行口径取绝对值汇总',
    return_amount DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '退货金额，按当前 DWS 退货行口径取绝对值汇总',
    net_qty DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '净销量候选值，建议由 sales_qty - return_qty 生成；切换前需确认是否暴露给下游',
    net_amount DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '净销售额候选值，建议由 sales_amount - return_amount 生成；切换前需确认是否暴露给下游',
    order_count BIGINT NOT NULL DEFAULT 0 COMMENT '订单数，COUNT(DISTINCT 正向销售 retail_id)',

    source_dwd_row_count BIGINT NOT NULL DEFAULT 0 COMMENT '参与聚合的 DWD 明细行数',
    positive_line_count BIGINT NOT NULL DEFAULT 0 COMMENT '正向销售明细行数',
    return_line_count BIGINT NOT NULL DEFAULT 0 COMMENT '退货明细行数',
    min_retail_modified_at DATETIME NULL COMMENT '参与聚合单头最小修改时间',
    max_retail_modified_at DATETIME NULL COMMENT '参与聚合单头最大修改时间',
    min_item_modified_at DATETIME NULL COMMENT '参与聚合明细最小修改时间',
    max_item_modified_at DATETIME NULL COMMENT '参与聚合明细最大修改时间',
    min_item_set_time DATETIME NULL COMMENT '参与聚合明细最小 SETTIME',
    max_item_set_time DATETIME NULL COMMENT '参与聚合明细最大 SETTIME',
    source_min_loaded_at DATETIME NULL COMMENT '参与聚合 raw ODS 最早装载时间',
    source_max_loaded_at DATETIME NULL COMMENT '参与聚合 raw ODS 最晚装载时间',
    load_batch_id VARCHAR(64) NULL COMMENT 'DWS v2 装载批次 ID',
    source_layer_version VARCHAR(32) NOT NULL DEFAULT 'M3_DWD_V1' COMMENT '来源层版本标识',
    validation_status VARCHAR(20) NOT NULL DEFAULT 'PENDING' COMMENT '并行对账状态：PENDING / PASSED / FAILED / WAIVED',
    validation_note VARCHAR(512) NULL COMMENT '并行对账说明或豁免原因',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DWS v2 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_dws_sales_daily_v2_date_store_product_sku (date_id, store_id, product_id, m_productalias_id),
    KEY idx_dws_sales_daily_v2_date (date_id),
    KEY idx_dws_sales_daily_v2_store_sku (date_id, store_id, m_productalias_id),
    KEY idx_dws_sales_daily_v2_product (date_id, product_id, m_productalias_id),
    KEY idx_dws_sales_daily_v2_watermark (max_retail_modified_at, max_item_modified_at, max_item_set_time),
    KEY idx_dws_sales_daily_v2_validation (validation_status, date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS v2 销售日汇总并行表，来源 dwd_sales_retail_item，未接生产调度';
