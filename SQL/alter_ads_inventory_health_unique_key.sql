-- 修复ads_inventory_health唯一键以支持SKU粒度
-- 当前唯一键：uk_date_product (snapshot_date, product_id)
-- 需调整为： (snapshot_date, product_id, sku_id)

ALTER TABLE ads_inventory_health
    DROP INDEX uk_date_product;

ALTER TABLE ads_inventory_health
    ADD UNIQUE KEY uk_date_product_sku (snapshot_date, product_id, sku_id);
