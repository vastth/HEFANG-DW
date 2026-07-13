SET @column_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'ads_store_daily_subject_report'
      AND column_name = 'report_channel_type'
);

SET @ddl := IF(
    @column_exists = 0,
    'ALTER TABLE ads_store_daily_subject_report ADD COLUMN report_channel_type VARCHAR(32) NOT NULL DEFAULT ''未配置'' COMMENT ''经营渠道细分类'' AFTER anchor_store_name',
    'SELECT ''SKIP ads_store_daily_subject_report report_channel_type exists'''
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;