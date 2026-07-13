-- 为 dim_store_report_attr 新增日报渠道粗分类生成列
-- 设计原则：
-- 1) report_channel_type 继续直接存业务最终真值（细分类）
-- 2) report_channel_type_group 由 report_channel_type 自动派生，不手工写入

ALTER TABLE dim_store_report_attr
ADD COLUMN report_channel_type_group VARCHAR(20)
GENERATED ALWAYS AS (
    CASE
        WHEN report_channel_type IN ('小程序', '线上小程序') THEN '小程序'
        WHEN report_channel_type IN ('直营', '直营-奥莱') THEN '直营'
        WHEN report_channel_type IN ('联营', '联营-免税', '联营-奥莱') THEN '联营'
        ELSE NULL
    END
) STORED COMMENT '日报渠道粗分类（由 report_channel_type 派生）'
AFTER report_channel_type;

-- 执行后核对：
SELECT
    report_channel_type,
    report_channel_type_group,
    COUNT(*) AS row_count
FROM dim_store_report_attr
GROUP BY report_channel_type, report_channel_type_group
ORDER BY report_channel_type;