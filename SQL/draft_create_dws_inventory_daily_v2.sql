-- DWS inventory daily v2 parallel table draft DDL.
-- Status: DDL 草案已由用户人工执行；Copilot 已补 dry-run / conn-test / S3 手工写入分支，并在用户明确授权下完成一次 S3 实跑验收：20260507 共写入 75104 行，DWD-v2 mismatch 为 0。当前仍未接入 run_etl.py / scheduled_etl.py / scheduled_total_control.py。
-- Execution boundary: CREATE / ALTER / INDEX / INSERT / DELETE / 回填默认仍由用户人工执行；S3 脚本写入分支仅供显式授权下的受控执行，并通过确认令牌、MySQL 命名锁、显式事务和写后 DWD-v2 对账输出运行证据。
-- Evidence:
--   reports/context_cache/dws_v2_manual_ddl_verification_20260507.json
--   reports/context_cache/dws_v2_parallel_design_evidence_20260507.json
--   reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json
--   reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json
--   dws_v2_write_utils.py
--   etl_dws_inventory_v2.py
--   etl_dws_inventory.py
--   etl_dwd_inventory_storage_snapshot.py

CREATE TABLE IF NOT EXISTS dws_inventory_daily_v2 (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    date_id INT NOT NULL COMMENT '库存快照日期，YYYYMMDD，来源 dwd_inventory_storage_snapshot.snapshot_date',
    store_id BIGINT NOT NULL COMMENT '店仓 ID，来源 dwd_inventory_storage_snapshot.store_id',
    store_code VARCHAR(40) NOT NULL DEFAULT '' COMMENT '店仓编码，来源 dwd_inventory_storage_snapshot.store_code',
    is_cloud_store CHAR(1) NOT NULL DEFAULT 'N' COMMENT '是否云仓门店，来源 dwd_inventory_storage_snapshot.is_cloud_store',
    product_id BIGINT NOT NULL COMMENT '商品 ID，来源 dwd_inventory_storage_snapshot.product_id',
    m_productalias_id BIGINT NOT NULL COMMENT 'SKU / 条码 ID，来源 dwd_inventory_storage_snapshot.m_productalias_id',

    qty DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '库存数量，按当前 DWS 范围标识汇总 dwd_inventory_storage_snapshot.qty',
    qty_valid DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '可用库存等价候选值；M3 已剔除源侧全量无业务值 QTYVALID，第一阶段沿用 qty',
    qty_occupy DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '占用数量候选值；当前生产 DWS 无源字段，第一阶段保持 0',
    qtypurchaserem DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '采购欠数 / 在途，来源 qty_purchase_rem 汇总',
    qty_preout DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '在单数量候选汇总，来源 qty_preout',
    qty_prein DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '在途数量候选汇总，来源 qty_prein',
    qty_freeze DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '冻结数量候选汇总，来源 qty_freeze',
    qty_oms DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT 'OMS 冻结量候选汇总，来源 qty_oms',
    qty_oms_translate DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT 'OMS 转换占用 / 调整候选汇总，来源 qty_oms_translate',
    qty_preout1 DECIMAL(18,4) NOT NULL DEFAULT 0 COMMENT '备用预出调整候选汇总，来源 qty_preout1',

    source_dwd_row_count BIGINT NOT NULL DEFAULT 0 COMMENT '参与聚合的 DWD 库存源行数',
    zero_qty_row_count BIGINT NOT NULL DEFAULT 0 COMMENT '参与聚合的 0 库存源行数',
    negative_qty_row_count BIGINT NOT NULL DEFAULT 0 COMMENT '参与聚合的负库存源行数',
    min_storage_modified_at DATETIME NULL COMMENT '参与聚合库存源行最小修改时间',
    max_storage_modified_at DATETIME NULL COMMENT '参与聚合库存源行最大修改时间',
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
    UNIQUE KEY uk_dws_inventory_daily_v2_date_store_product_sku (date_id, store_id, product_id, m_productalias_id),
    KEY idx_dws_inventory_daily_v2_date (date_id),
    KEY idx_dws_inventory_daily_v2_store_sku (date_id, store_id, m_productalias_id),
    KEY idx_dws_inventory_daily_v2_product (date_id, product_id, m_productalias_id),
    KEY idx_dws_inventory_daily_v2_watermark (max_storage_modified_at),
    KEY idx_dws_inventory_daily_v2_validation (validation_status, date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS v2 库存日汇总并行表，来源 dwd_inventory_storage_snapshot，未接生产调度';
