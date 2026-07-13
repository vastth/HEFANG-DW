CREATE TABLE IF NOT EXISTS ads_dabo_order_retail_bridge (
    source_file VARCHAR(255) NOT NULL COMMENT '达播样本文件名',
    main_order_id VARCHAR(512) NOT NULL COMMENT '达播主订单编号',
    retail_id BIGINT NOT NULL COMMENT '零售单ID(M_RETAIL.ID)',
    billdate INT NOT NULL COMMENT '单据日期(YYYYMMDD)',
    retail_tot_amt_actual DECIMAL(18,2) NULL COMMENT '零售单头实收金额',
    retail_status INT NULL COMMENT '零售单状态(M_RETAIL.STATUS)',
    retail_isactive CHAR(1) NULL COMMENT '是否有效(M_RETAIL.ISACTIVE)',
    synced_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最近同步时间',
    PRIMARY KEY (source_file, retail_id),
    KEY idx_ads_dabo_order_retail_bridge_main_order (main_order_id),
    KEY idx_ads_dabo_order_retail_bridge_billdate (billdate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='达播订单到零售单头桥接缓存表';