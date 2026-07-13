CREATE TABLE IF NOT EXISTS cfg_duty_free_store_mtd_sales (
  id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  target_month DATE NOT NULL COMMENT '目标月份首日',
  data_version VARCHAR(32) NOT NULL DEFAULT 'v1' COMMENT '数据版本',
  store_id BIGINT NOT NULL COMMENT '门店ID(dim_store.store_id)',
  store_code VARCHAR(40) NOT NULL COMMENT '门店编码(dim_store.store_code)',
  store_name VARCHAR(255) NOT NULL COMMENT '门店名称',
  report_channel_type VARCHAR(20) NOT NULL COMMENT '渠道类型',
  external_mtd_sales_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '外部维护月累计销售额',
  source_file_name VARCHAR(255) NULL COMMENT '来源文件名',
  source_file_md5 CHAR(32) NULL COMMENT '来源文件MD5',
  created_by VARCHAR(64) NULL COMMENT '创建人',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY uk_cfg_duty_free_store_mtd_sales (target_month, data_version, store_id),
  KEY idx_cfg_duty_free_store_mtd_sales_lookup (target_month, data_version),
  KEY idx_cfg_duty_free_store_mtd_sales_store (store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='免税门店外部月累计销售额快照表';