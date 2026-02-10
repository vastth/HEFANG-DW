-- Step 1: 添加达播销量字段
ALTER TABLE ads_inventory_health
  ADD COLUMN dabo_sales_qty_30d INT NOT NULL DEFAULT 0 COMMENT '近30天达播销量' AFTER sales_qty_7d;

ALTER TABLE ads_inventory_health
  ADD COLUMN dabo_sales_qty_7d INT NOT NULL DEFAULT 0 COMMENT '近7天达播销量' AFTER dabo_sales_qty_30d;

-- Step 2: 添加自然销量字段
ALTER TABLE ads_inventory_health
  ADD COLUMN natural_sales_qty_30d INT NOT NULL DEFAULT 0 COMMENT '近30天自然销量(全量-达播)' AFTER dabo_sales_qty_7d;

ALTER TABLE ads_inventory_health
  ADD COLUMN natural_sales_qty_7d INT NOT NULL DEFAULT 0 COMMENT '近7天自然销量(全量-达播)' AFTER natural_sales_qty_30d;

-- Step 3: 添加自然日均销量字段
ALTER TABLE ads_inventory_health
  ADD COLUMN natural_daily_avg_sales DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '近30天自然日均销量' AFTER daily_avg_sales_7d;

ALTER TABLE ads_inventory_health
  ADD COLUMN natural_daily_avg_sales_7d DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '近7天自然日均销量' AFTER natural_daily_avg_sales;

-- Step 4: 添加自然销售加速度字段
ALTER TABLE ads_inventory_health
  ADD COLUMN natural_sales_velocity DECIMAL(5,2) NULL COMMENT '自然销售加速度' AFTER sales_velocity;