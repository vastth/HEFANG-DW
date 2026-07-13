-- 门店日报商品范围补纳 459=餐具
-- 说明：
-- 1. 当前业务已确认：门店日报专题所有 ADS 均应纳入餐具；订单数继续按净单口径计算，即按单号去重后，销售金额 >0 记 1、<0 记 -1、=0 记 0。
-- 2. 本脚本只调整 dim_report_product_rule 配置，不直接写 ads_store_daily_report 等结果表；执行后仍需由用户人工重跑受影响日期的门店日报专题链路。
-- 3. ads_store_daily_report 是门店日报专题权威事实表，ads_store_daily_subject_report 与 ads_daily_sales 当前都统一复用该商品范围口径。
-- 4. 为保持当前日报配置切片一致，本脚本沿用现网同批规则的生效起始日 2026-03-23。

START TRANSACTION;

INSERT INTO dim_report_product_rule (
    category_id,
    category_name,
    include_in_store_daily_report,
    rule_note,
    effective_start_date,
    effective_end_date,
    updated_by
) VALUES (
    459,
    '餐具',
    'Y',
    '2026-04-29 业务确认纳入门店日报专题所有 ADS；餐具为近期新增类别，执行后需人工重跑受影响日期的门店日报专题链路',
    DATE '2026-03-23',
    DATE '9999-12-31',
    'github_copilot_20260429'
)
ON DUPLICATE KEY UPDATE
    category_name = VALUES(category_name),
    include_in_store_daily_report = VALUES(include_in_store_daily_report),
    rule_note = VALUES(rule_note),
    effective_end_date = VALUES(effective_end_date),
    updated_by = VALUES(updated_by);

COMMIT;

-- 回读校验
SELECT
    category_id,
    category_name,
    include_in_store_daily_report,
    effective_start_date,
    effective_end_date,
    updated_by,
    updated_at
FROM dim_report_product_rule
WHERE category_id = 459
ORDER BY effective_start_date;