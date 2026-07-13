-- 免税月累计快照/日志表字段从 report_date 迁移为 target_month
-- 背景：用户已确认 Excel 业务字段为“目标月份”，代码与建表脚本已切到 target_month；现网表仍为旧 report_date 字段。
-- 执行边界：本文件仅提供人工执行 SQL；Agent 不直接执行 ALTER。
-- 风险评估：2026-05-26 只读探查显示 cfg_duty_free_store_mtd_sales 与 log_duty_free_store_mtd_sales_import 均为 0 行；本脚本仅改字段名/注释和日志索引名，数据搬迁量为 0，预计仅产生短暂 metadata lock。

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_KEY
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('cfg_duty_free_store_mtd_sales', 'log_duty_free_store_mtd_sales_import')
  AND COLUMN_NAME IN ('report_date', 'target_month')
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT 'cfg_duty_free_store_mtd_sales' AS table_name, COUNT(*) AS row_count
FROM cfg_duty_free_store_mtd_sales
UNION ALL
SELECT 'log_duty_free_store_mtd_sales_import' AS table_name, COUNT(*) AS row_count
FROM log_duty_free_store_mtd_sales_import;

ALTER TABLE cfg_duty_free_store_mtd_sales
    CHANGE COLUMN report_date target_month DATE NOT NULL COMMENT '目标月份首日';

ALTER TABLE log_duty_free_store_mtd_sales_import
    CHANGE COLUMN report_date target_month DATE NULL COMMENT '目标月份首日';

ALTER TABLE log_duty_free_store_mtd_sales_import
    RENAME INDEX idx_log_duty_free_store_mtd_sales_import_report_date
    TO idx_log_duty_free_store_mtd_sales_import_target_month;

SELECT
    TABLE_NAME,
    COLUMN_NAME,
    COLUMN_TYPE,
    IS_NULLABLE,
    COLUMN_KEY
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('cfg_duty_free_store_mtd_sales', 'log_duty_free_store_mtd_sales_import')
  AND COLUMN_NAME IN ('report_date', 'target_month')
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT
    TABLE_NAME,
    INDEX_NAME,
    NON_UNIQUE,
    SEQ_IN_INDEX,
    COLUMN_NAME
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME IN ('cfg_duty_free_store_mtd_sales', 'log_duty_free_store_mtd_sales_import')
ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;