-- Step 1: 添加达播销售额字段
ALTER TABLE ads_inventory_health
  ADD COLUMN dabo_revenue_30d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近30天达播销售额' AFTER dabo_latest_date;

ALTER TABLE ads_inventory_health
  ADD COLUMN dabo_revenue_7d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近7天达播销售额' AFTER dabo_revenue_30d;

-- Step 2: 添加自然销售额字段
ALTER TABLE ads_inventory_health
  ADD COLUMN natural_revenue_30d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近30天自然销售额(全量-达播)' AFTER natural_sales_qty_7d;

ALTER TABLE ads_inventory_health
  ADD COLUMN natural_revenue_7d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近7天自然销售额(全量-达播)' AFTER natural_revenue_30d;
