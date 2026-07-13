ALTER TABLE ads_store_daily_report
  ADD COLUMN owner_name VARCHAR(100) NULL COMMENT '负责人名称，可为空' AFTER store_name;