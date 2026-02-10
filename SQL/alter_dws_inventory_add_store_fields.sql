-- 为dws_inventory_daily新增店仓属性字段
ALTER TABLE dws_inventory_daily
    ADD COLUMN store_code VARCHAR(40) NULL COMMENT '店仓编码' AFTER store_id,
    ADD COLUMN is_cloud_store CHAR(1) NULL DEFAULT 'N' COMMENT '是否云仓(Y/N)' AFTER store_code;

-- 可选索引
-- CREATE INDEX idx_inv_store_code ON dws_inventory_daily (store_code);
