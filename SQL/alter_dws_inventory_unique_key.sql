-- 修复dws_inventory_daily唯一键/索引以支持SKU粒度
-- 目标唯一键： (date_id, store_id, product_id, m_productalias_id)

-- 1) 删除旧唯一键（若存在）
SET @idx := (
    SELECT INDEX_NAME
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'dws_inventory_daily'
      AND index_name = 'uk_date_store_product'
    LIMIT 1
);
SET @sql := IF(@idx IS NULL,
    'SELECT 1',
    'ALTER TABLE dws_inventory_daily DROP INDEX uk_date_store_product'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2) 新增SKU粒度唯一键（若不存在）
SET @idx2 := (
    SELECT INDEX_NAME
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'dws_inventory_daily'
      AND index_name = 'uk_date_store_product_sku'
    LIMIT 1
);
SET @sql2 := IF(@idx2 IS NULL,
    'ALTER TABLE dws_inventory_daily ADD UNIQUE KEY uk_date_store_product_sku (date_id, store_id, product_id, m_productalias_id)',
    'SELECT 1'
);
PREPARE stmt2 FROM @sql2;
EXECUTE stmt2;
DEALLOCATE PREPARE stmt2;

-- 3) 可选：增加SKU索引（若不存在）
SET @idx3 := (
    SELECT INDEX_NAME
    FROM information_schema.statistics
    WHERE table_schema = DATABASE()
      AND table_name = 'dws_inventory_daily'
      AND index_name = 'idx_productalias'
    LIMIT 1
);
SET @sql3 := IF(@idx3 IS NULL,
    'ALTER TABLE dws_inventory_daily ADD INDEX idx_productalias (m_productalias_id)',
    'SELECT 1'
);
PREPARE stmt3 FROM @sql3;
EXECUTE stmt3;
DEALLOCATE PREPARE stmt3;
