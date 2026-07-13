-- 门店日报商品范围补纳 148=辅销品、394=配饰
-- 说明：
-- 1. 仅影响门店日报 ads_store_daily_report 的商品范围，不改变库存健康等链路沿用的主销品口径。
-- 2. 当前业务已确认：辅销品、配饰应纳入门店日报计算；订单数仍按净单口径计算，整单净额为 0 仍记 0 单。
-- 3. 为保持当前日报配置切片一致，本脚本沿用现网同批规则的生效起始日 2026-03-23。

START TRANSACTION;

INSERT INTO dim_report_product_rule (
    category_id,
    category_name,
    include_in_store_daily_report,
    rule_note,
    effective_start_date,
    effective_end_date,
    updated_by
) VALUES
    (
        148,
        '辅销品',
        'Y',
        '2026-04-10 业务确认纳入门店日报；仅作用于门店日报商品范围，不改变净单口径与库存健康主销品范围',
        DATE '2026-03-23',
        DATE '9999-12-31',
        'github_copilot_20260410'
    ),
    (
        394,
        '配饰',
        'Y',
        '2026-04-10 业务确认纳入门店日报；仅作用于门店日报商品范围，不改变净单口径与库存健康主销品范围',
        DATE '2026-03-23',
        DATE '9999-12-31',
        'github_copilot_20260410'
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
WHERE category_id IN (148, 394)
ORDER BY category_id, effective_start_date;