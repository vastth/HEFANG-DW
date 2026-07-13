SET @old_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'ads_daily_sales'
      AND column_name = 'report_channel_type_group'
);

SET @new_exists := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'ads_daily_sales'
      AND column_name = 'report_channel_type'
);

SET @ddl := IF(
    @old_exists = 1 AND @new_exists = 0,
    'ALTER TABLE ads_daily_sales CHANGE COLUMN report_channel_type_group report_channel_type VARCHAR(32) NOT NULL COMMENT ''经营渠道细分类；不再生成全部汇总行''',
    'SELECT ''SKIP ads_daily_sales channel column migration'''
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;