-- 用途：人工执行 DDL 前后检查 dim_store.open_date 状态；本文件仅包含只读查询。

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    COLUMN_COMMENT,
    ORDINAL_POSITION
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME = 'dim_store'
  AND COLUMN_NAME = 'open_date';

SELECT
    COUNT(*) AS total_store_count,
    SUM(CASE WHEN open_date IS NULL THEN 1 ELSE 0 END) AS open_date_null_count,
    MIN(open_date) AS min_open_date,
    MAX(open_date) AS max_open_date
FROM dim_store;