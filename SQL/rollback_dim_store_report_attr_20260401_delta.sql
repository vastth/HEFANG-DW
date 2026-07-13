-- April 门店属性差异回滚 SQL
-- 仅回滚 apply_dim_store_report_attr_20260401_delta.sql 新增的 2 家门店记录

-- 回滚前核对：确认当前仅存在本次新增的目标记录
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

START TRANSACTION;

DELETE FROM dim_store_report_attr
WHERE store_id IN (738, 740)
  AND effective_start_date = '2026-04-01'
  AND effective_end_date = '9999-12-31'
  AND is_include_in_daily_report = 'Y'
  AND report_channel_type = '联营'
  AND remark = 'NAS导入:2026-04/v1/门店类型=联营';

COMMIT;

-- 回滚后核对：这 2 家门店不应再存在 2026-04-01 起的新切片
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