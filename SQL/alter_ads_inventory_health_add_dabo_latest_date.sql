ALTER TABLE ads_inventory_health
  ADD COLUMN dabo_latest_date DATE NULL COMMENT '达播最新日期(按SKU)' AFTER dabo_sales_qty_7d;
