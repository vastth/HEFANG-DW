-- 修复dws_sales_daily唯一键以支持SKU粒度
-- 当前唯一键：uk_date_store_product (date_id, store_id, product_id)
-- 需调整为： (date_id, store_id, product_id, m_productalias_id)

ALTER TABLE dws_sales_daily
    DROP INDEX uk_date_store_product;

ALTER TABLE dws_sales_daily
    ADD UNIQUE KEY uk_date_store_product_sku (date_id, store_id, product_id, m_productalias_id);
