-- April 门店属性差异正式执行 SQL
-- 证据来源：reports/store_attr_snapshot_diff_202604.json
-- 差异结论：未变化 71，变更 0，新增 2，退出 0
-- 本次仅需新增以下 2 家门店自 2026-04-01 起的有效记录：
--   1) store_id = 738, store_code = RT114, store_name = 武汉武商梦时代店, report_channel_type = 联营
--   2) store_id = 740, store_code = RT115, store_name = 惠州华贸天地店, report_channel_type = 联营

-- 执行前核对：当前库内不应已存在这 2 家门店在 2026-04-01 当天的有效记录
SELECT
    store_id,
    store_code,
    store_name,
    report_channel_type,
    effective_start_date,
    effective_end_date
FROM dim_store_report_attr
WHERE store_id IN (738, 740)
ORDER BY store_id, effective_start_date;

START TRANSACTION;

INSERT INTO dim_store_report_attr (
    store_id,
    store_code,
    store_name,
    report_channel_type,
    store_grade,
    is_duty_free,
    is_include_in_daily_report,
    remark,
    effective_start_date,
    effective_end_date,
    updated_by
)
SELECT
    738,
    'RT114',
    '武汉武商梦时代店',
    '联营',
    NULL,
    'N',
    'Y',
    'NAS导入:2026-04/v1/门店类型=联营',
    '2026-04-01',
    '9999-12-31',
    'store_attr_april_delta_sql'
FROM dual
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_store_report_attr
    WHERE store_id = 738
      AND effective_start_date = '2026-04-01'
);

INSERT INTO dim_store_report_attr (
    store_id,
    store_code,
    store_name,
    report_channel_type,
    store_grade,
    is_duty_free,
    is_include_in_daily_report,
    remark,
    effective_start_date,
    effective_end_date,
    updated_by
)
SELECT
    740,
    'RT115',
    '惠州华贸天地店',
    '联营',
    NULL,
    'N',
    'Y',
    'NAS导入:2026-04/v1/门店类型=联营',
    '2026-04-01',
    '9999-12-31',
    'store_attr_april_delta_sql'
FROM dual
WHERE NOT EXISTS (
    SELECT 1
    FROM dim_store_report_attr
    WHERE store_id = 740
      AND effective_start_date = '2026-04-01'
);

COMMIT;

-- 执行后核对：
-- 1) 这 2 家门店应成功出现在 2026-04-01 生效集内
SELECT
    store_id,
    store_code,
    store_name,
    report_channel_type,
    effective_start_date,
    effective_end_date,
    updated_by
FROM dim_store_report_attr
WHERE store_id IN (738, 740)
ORDER BY store_id, effective_start_date;

-- 2) 2026-04-01 当天不应存在同店多条有效配置
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT store_id) AS distinct_store_count
FROM dim_store_report_attr
WHERE is_include_in_daily_report = 'Y'
  AND '2026-04-01' BETWEEN effective_start_date AND effective_end_date;
