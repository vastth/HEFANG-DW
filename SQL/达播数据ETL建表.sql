CREATE TABLE IF NOT EXISTS ads_dabo_daily_sales (
  sale_date DATE NOT NULL COMMENT '发货日期',
  product_alias_code VARCHAR(80) NOT NULL COMMENT 'SKU条码',
  dabo_sales_qty INT NOT NULL DEFAULT 0 COMMENT '达播销量',
  dabo_order_count INT NOT NULL DEFAULT 0 COMMENT '达播订单数',
  dabo_revenue DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '达播实收金额',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (sale_date, product_alias_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='达播日销售兼容汇总表（标签批次不可用时供 ads_inventory_health 回退）';

CREATE TABLE IF NOT EXISTS log_dabo_import (
  id BIGINT NOT NULL AUTO_INCREMENT,
  file_name VARCHAR(255) NOT NULL,
  file_path VARCHAR(500) NULL,
  records_total INT NOT NULL DEFAULT 0,
  records_after_filter INT NOT NULL DEFAULT 0,
  records_inserted INT NOT NULL DEFAULT 0,
  sku_match_rate DECIMAL(5,4) NULL,
  status VARCHAR(20) NOT NULL,
  message VARCHAR(1000) NULL,
  started_at DATETIME NULL,
  finished_at DATETIME NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_log_dabo_import_created_at (created_at),
  KEY idx_log_dabo_import_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
