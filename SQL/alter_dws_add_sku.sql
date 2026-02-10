-- 为dws表新增SKU字段（m_productalias_id）
ALTER TABLE dws_inventory_daily
    ADD COLUMN m_productalias_id BIGINT NULL COMMENT 'SKU ID（条码）' AFTER product_id;

ALTER TABLE dws_sales_daily
    ADD COLUMN m_productalias_id BIGINT NULL COMMENT 'SKU ID（条码）' AFTER product_id;

-- 可选：根据查询需求建立索引
-- CREATE INDEX idx_dws_inventory_daily_sku ON dws_inventory_daily (date_id, m_productalias_id);
-- CREATE INDEX idx_dws_sales_daily_sku ON dws_sales_daily (date_id, m_productalias_id);
