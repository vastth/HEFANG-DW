-- 修正 RT110 / 杭州萧山国际机场店免税标记
-- 背景：用户 2026-05-26 确认 RT110 / 杭州萧山国际机场店确实属于免税门店。
-- 执行边界：本文件仅提供人工执行 SQL；Agent 不直接执行 UPDATE。
-- 风险评估：按 dim_store_report_attr.id=219 精确更新单行，过滤条件包含 store_id/store_code/当前值，事务范围与锁持有时间极小。

SELECT
    id,
    store_id,
    store_code,
    store_name,
    report_channel_type,
    is_duty_free,
    is_include_in_daily_report,
    effective_start_date,
    effective_end_date,
    updated_by,
    updated_at
FROM dim_store_report_attr
WHERE store_code = 'RT110'
  AND DATE('2026-05-01') BETWEEN effective_start_date AND effective_end_date
ORDER BY effective_start_date DESC, id DESC;

START TRANSACTION;

UPDATE dim_store_report_attr
SET
    is_duty_free = 'Y',
    updated_by = 'manual_duty_free_truth_20260526',
    updated_at = NOW()
WHERE id = 219
  AND store_id = 708
  AND store_code = 'RT110'
  AND is_duty_free = 'N';

SELECT ROW_COUNT() AS updated_rows;

SELECT
    id,
    store_id,
    store_code,
    store_name,
    report_channel_type,
    is_duty_free,
    is_include_in_daily_report,
    effective_start_date,
    effective_end_date,
    updated_by,
    updated_at
FROM dim_store_report_attr
WHERE id = 219;

-- 本脚本按单行保护条件提交；执行后请确认 updated_rows = 1 且 is_duty_free = 'Y'。
COMMIT;