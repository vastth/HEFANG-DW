SET @report_date = '2026-04-15';
SET @data_version = 'v1';

-- 1) 行数与唯一键检查：应等于 天数 * 当前日报有效明细组织组合数
WITH params AS (
    SELECT
        CAST(@report_date AS DATE) AS report_date,
        CAST(DATE_FORMAT(@report_date, '%Y-%m-01') AS DATE) AS battle_month
),
target_store_scope AS (
    SELECT DISTINCT
        t.store_id
    FROM cfg_store_target_daily t
    CROSS JOIN params p
    WHERE t.target_version = @data_version
      AND t.target_date BETWEEN p.battle_month AND p.report_date
),
store_attr_candidates AS (
    SELECT
        sra.store_id,
        sra.report_channel_type,
        ROW_NUMBER() OVER (
            PARTITION BY sra.store_id
            ORDER BY
                CASE
                    WHEN p.report_date BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0
                    ELSE 1
                END,
                sra.effective_end_date DESC,
                sra.effective_start_date DESC
        ) AS attr_recency_rank
    FROM dim_store_report_attr sra
    INNER JOIN target_store_scope tss
        ON sra.store_id = tss.store_id
    CROSS JOIN params p
    WHERE sra.is_include_in_daily_report = 'Y'
      AND sra.effective_start_date <= p.report_date
      AND sra.effective_end_date >= p.battle_month
),
store_scope AS (
    SELECT
        sac.store_id,
        COALESCE(NULLIF(TRIM(s.area_name), ''), '未分区') AS area_name,
        COALESCE(NULLIF(TRIM(sac.report_channel_type), ''), '未配置') AS report_channel_type
    FROM store_attr_candidates sac
    INNER JOIN dim_store s
        ON sac.store_id = s.store_id
    WHERE sac.attr_recency_rank = 1
),
assignment_candidates AS (
    SELECT
        sa.store_id,
        sa.subject_code,
        sa.assignment_role,
        COALESCE(
            sa.anchor_store_id,
            CASE
                WHEN sa.assignment_role = '主店' THEN sa.store_id
                ELSE NULL
            END
        ) AS anchor_store_id,
        ROW_NUMBER() OVER (
            PARTITION BY sa.store_id
            ORDER BY
                CASE
                    WHEN p.report_date BETWEEN sa.effective_start_date AND sa.effective_end_date THEN 0
                    ELSE 1
                END,
                sa.effective_end_date DESC,
                sa.effective_start_date DESC
        ) AS assignment_recency_rank
    FROM cfg_store_assessment_assignment sa
    INNER JOIN target_store_scope tss
        ON sa.store_id = tss.store_id
    CROSS JOIN params p
    WHERE sa.target_month = p.battle_month
      AND sa.target_version = @data_version
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.battle_month
),
assignment_scope AS (
    SELECT
        ac.store_id,
        ac.subject_code,
        ac.assignment_role,
        ac.anchor_store_id
    FROM assignment_candidates ac
    WHERE ac.assignment_recency_rank = 1
),
store_entity_map AS (
    SELECT
        ss.store_id,
        COALESCE(anchor_ss.area_name, ss.area_name) AS area_name,
        COALESCE(anchor_ss.report_channel_type, ss.report_channel_type) AS report_channel_type
    FROM store_scope ss
    LEFT JOIN assignment_scope ass
        ON ss.store_id = ass.store_id
    LEFT JOIN store_scope anchor_ss
        ON COALESCE(ass.anchor_store_id, ss.store_id) = anchor_ss.store_id
),
group_scope AS (
    SELECT DISTINCT area_name, report_channel_type
    FROM store_entity_map
),
expected_rows AS (
    SELECT
        COUNT(*) AS expected_rows_per_day,
        DATEDIFF(p.report_date, p.battle_month) + 1 AS sales_day_count
    FROM group_scope
    CROSS JOIN params p
),
ads_scope AS (
    SELECT
        COUNT(*) AS output_row_count,
        COUNT(DISTINCT sales_date) AS output_sales_day_count,
        COUNT(*) - COUNT(DISTINCT CONCAT(
            DATE_FORMAT(report_date, '%Y-%m-%d'),
            '||',
            data_version,
            '||',
            DATE_FORMAT(battle_month, '%Y-%m-%d'),
            '||',
            DATE_FORMAT(sales_date, '%Y-%m-%d'),
            '||',
            area_name,
            '||',
            report_channel_type
        )) AS duplicate_key_count
    FROM ads_daily_sales
    WHERE report_date = @report_date
      AND data_version = @data_version
),
ads_day_scope AS (
    SELECT
        COALESCE(SUM(CASE WHEN day_row_count <> er.expected_rows_per_day THEN 1 ELSE 0 END), 0) AS abnormal_sales_date_count
    FROM (
        SELECT sales_date, COUNT(*) AS day_row_count
        FROM ads_daily_sales
        WHERE report_date = @report_date
          AND data_version = @data_version
        GROUP BY sales_date
    ) daily_rows
    CROSS JOIN expected_rows er
)
SELECT
    'row_count_and_unique_key' AS check_name,
    er.expected_rows_per_day * er.sales_day_count AS expected_row_count,
    ads.output_row_count AS actual_row_count,
    er.sales_day_count AS expected_sales_day_count,
    ads.output_sales_day_count AS actual_sales_day_count,
    ads.duplicate_key_count,
    ads_day.abnormal_sales_date_count,
    CASE
        WHEN ads.output_row_count = er.expected_rows_per_day * er.sales_day_count
         AND ads.output_sales_day_count = er.sales_day_count
         AND ads.duplicate_key_count = 0
         AND ads_day.abnormal_sales_date_count = 0 THEN 'OK'
        ELSE 'CHECK'
    END AS status
FROM expected_rows er
CROSS JOIN ads_scope ads
CROSS JOIN ads_day_scope ads_day;

-- 2) 明细聚合全序列核对：主体日目标优先 + ODS 净额 + 门店日报商品范围
WITH RECURSIVE
params AS (
    SELECT
        CAST(@report_date AS DATE) AS report_date,
        CAST(DATE_FORMAT(@report_date, '%Y-%m-01') AS DATE) AS battle_month,
        CAST(DATE_FORMAT(@report_date, '%Y%m%d') AS UNSIGNED) AS report_date_id,
        CAST(DATE_FORMAT(DATE_FORMAT(@report_date, '%Y-%m-01'), '%Y%m%d') AS UNSIGNED) AS battle_month_start_id,
        CAST(DATE_SUB(@report_date, INTERVAL 1 YEAR) AS DATE) AS last_year_report_date,
        CAST(DATE_FORMAT(DATE_SUB(@report_date, INTERVAL 1 YEAR), '%Y%m%d') AS UNSIGNED) AS last_year_report_date_id,
        CAST(DATE_FORMAT(DATE_SUB(DATE_FORMAT(@report_date, '%Y-%m-01'), INTERVAL 1 YEAR), '%Y%m%d') AS UNSIGNED) AS last_year_battle_month_start_id,
        @data_version AS data_version
),
date_scope AS (
    SELECT p.battle_month AS sales_date
    FROM params p
    UNION ALL
    SELECT DATE_ADD(ds.sales_date, INTERVAL 1 DAY)
    FROM date_scope ds
    CROSS JOIN params p
    WHERE ds.sales_date < p.report_date
),
target_store_scope AS (
    SELECT DISTINCT
        t.store_id
    FROM cfg_store_target_daily t
    CROSS JOIN params p
    WHERE t.target_version = p.data_version
      AND t.target_date BETWEEN p.battle_month AND p.report_date
),
store_attr_candidates AS (
    SELECT
        sra.store_id,
        sra.report_channel_type,
        ROW_NUMBER() OVER (
            PARTITION BY sra.store_id
            ORDER BY
                CASE
                    WHEN p.report_date BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0
                    ELSE 1
                END,
                sra.effective_end_date DESC,
                sra.effective_start_date DESC
        ) AS attr_recency_rank
    FROM dim_store_report_attr sra
    INNER JOIN target_store_scope tss
        ON sra.store_id = tss.store_id
    CROSS JOIN params p
    WHERE sra.is_include_in_daily_report = 'Y'
      AND sra.effective_start_date <= p.report_date
      AND sra.effective_end_date >= p.battle_month
),
store_scope AS (
    SELECT
        sac.store_id,
        COALESCE(NULLIF(TRIM(s.area_name), ''), '未分区') AS area_name,
        COALESCE(NULLIF(TRIM(sac.report_channel_type), ''), '未配置') AS report_channel_type
    FROM store_attr_candidates sac
    INNER JOIN dim_store s
        ON sac.store_id = s.store_id
    WHERE sac.attr_recency_rank = 1
),
target_daily_by_store_date AS (
    SELECT
        t.store_id,
        t.target_date AS sales_date,
        MAX(COALESCE(t.day_target, 0)) AS day_target_amt
    FROM cfg_store_target_daily t
    INNER JOIN store_scope ss
        ON t.store_id = ss.store_id
    CROSS JOIN params p
    WHERE t.target_version = p.data_version
      AND t.target_date BETWEEN p.battle_month AND p.report_date
    GROUP BY t.store_id, t.target_date
),
assignment_candidates AS (
    SELECT
        sa.store_id,
        sa.subject_code,
        sa.assignment_role,
        COALESCE(
            sa.anchor_store_id,
            CASE
                WHEN sa.assignment_role = '主店' THEN sa.store_id
                ELSE NULL
            END
        ) AS anchor_store_id,
        ROW_NUMBER() OVER (
            PARTITION BY sa.store_id
            ORDER BY
                CASE
                    WHEN p.report_date BETWEEN sa.effective_start_date AND sa.effective_end_date THEN 0
                    ELSE 1
                END,
                sa.effective_end_date DESC,
                sa.effective_start_date DESC
        ) AS assignment_recency_rank
    FROM cfg_store_assessment_assignment sa
    INNER JOIN target_store_scope tss
        ON sa.store_id = tss.store_id
    CROSS JOIN params p
    WHERE sa.target_month = p.battle_month
      AND sa.target_version = p.data_version
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.battle_month
),
assignment_scope AS (
    SELECT
        ac.store_id,
        ac.subject_code,
        ac.assignment_role,
        ac.anchor_store_id
    FROM assignment_candidates ac
    WHERE ac.assignment_recency_rank = 1
),
subject_target_by_date AS (
    SELECT
        st.subject_code,
        st.target_date AS sales_date,
        MAX(COALESCE(st.day_target, 0)) AS day_target_amt
    FROM cfg_store_assessment_subject_target_daily st
    CROSS JOIN params p
    WHERE st.target_version = p.data_version
      AND st.target_date BETWEEN p.battle_month AND p.report_date
    GROUP BY st.subject_code, st.target_date
),
store_entity_map AS (
    SELECT
        ss.store_id,
        COALESCE(
            CASE
                WHEN ass.subject_code IS NOT NULL THEN COALESCE(ass.anchor_store_id, ss.store_id)
                ELSE NULL
            END,
            ss.store_id
        ) AS report_entity_id,
        COALESCE(anchor_ss.area_name, ss.area_name) AS area_name,
        COALESCE(anchor_ss.report_channel_type, ss.report_channel_type) AS report_channel_type,
        ass.subject_code
    FROM store_scope ss
    LEFT JOIN assignment_scope ass
        ON ss.store_id = ass.store_id
    LEFT JOIN store_scope anchor_ss
        ON COALESCE(ass.anchor_store_id, ss.store_id) = anchor_ss.store_id
),
group_scope AS (
    SELECT DISTINCT area_name, report_channel_type
    FROM store_entity_map
),
entity_day_base AS (
    SELECT
        ds.sales_date,
        sem.store_id,
        sem.report_entity_id,
        sem.area_name,
        sem.report_channel_type,
        sem.subject_code
    FROM store_entity_map sem
    CROSS JOIN date_scope ds
),
entity_target_daily AS (
    SELECT
        sales_date,
        report_entity_id,
        area_name,
        report_channel_type,
        COALESCE(MAX(std.day_target_amt), SUM(tdsd.day_target_amt), 0) AS day_target_amt
    FROM entity_day_base edb
    LEFT JOIN target_daily_by_store_date tdsd
        ON edb.store_id = tdsd.store_id
       AND edb.sales_date = tdsd.sales_date
    LEFT JOIN subject_target_by_date std
        ON edb.subject_code = std.subject_code
       AND edb.sales_date = std.sales_date
    GROUP BY sales_date, report_entity_id, area_name, report_channel_type
),
excluded_category_scope AS (
    SELECT 147 AS category_id
    UNION ALL SELECT 149
    UNION ALL SELECT 150
),
detail_base AS (
    SELECT
        r.billdate AS date_id,
        r.c_store_id AS store_id,
        ri.m_product_id AS product_id,
        ri.tot_amt_actual
    FROM ods_m_retail r
    INNER JOIN ods_m_retailitem ri
        ON r.id = ri.m_retail_id
    CROSS JOIN params p
    WHERE r.isactive = 'Y'
      AND r.status = 2
      AND ri.m_productalias_id IS NOT NULL
      AND ABS(ri.tot_amt_actual) >= 1
      AND r.billdate BETWEEN p.last_year_battle_month_start_id AND p.report_date_id
),
filtered_detail AS (
    SELECT
        db.date_id,
        sem.area_name,
        sem.report_channel_type,
        db.tot_amt_actual
    FROM detail_base db
    INNER JOIN store_entity_map sem
        ON db.store_id = sem.store_id
    INNER JOIN dim_product dp
        ON db.product_id = dp.product_id
    LEFT JOIN excluded_category_scope ecs
        ON dp.category_id = ecs.category_id
    WHERE dp.category_id IS NOT NULL
      AND ecs.category_id IS NULL
),
actual_daily_by_group AS (
    SELECT
        STR_TO_DATE(CAST(fd.date_id AS CHAR(8)), '%Y%m%d') AS sales_date,
        fd.area_name,
        fd.report_channel_type,
        SUM(fd.tot_amt_actual) AS day_actual_amt
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.battle_month_start_id AND p.report_date_id
    GROUP BY STR_TO_DATE(CAST(fd.date_id AS CHAR(8)), '%Y%m%d'), fd.area_name, fd.report_channel_type
),
last_year_daily_by_group AS (
    SELECT
        DATE_ADD(STR_TO_DATE(CAST(fd.date_id AS CHAR(8)), '%Y%m%d'), INTERVAL 1 YEAR) AS sales_date,
        fd.area_name,
        fd.report_channel_type,
        SUM(fd.tot_amt_actual) AS last_year_day_actual_amt
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.last_year_battle_month_start_id AND p.last_year_report_date_id
    GROUP BY DATE_ADD(STR_TO_DATE(CAST(fd.date_id AS CHAR(8)), '%Y%m%d'), INTERVAL 1 YEAR), fd.area_name, fd.report_channel_type
),
group_day_base AS (
    SELECT
        p.report_date,
        p.battle_month,
        ds.sales_date,
        gs.area_name,
        gs.report_channel_type,
        p.data_version
    FROM group_scope gs
    CROSS JOIN date_scope ds
    CROSS JOIN params p
),
group_day_metrics AS (
    SELECT
        gdb.report_date,
        gdb.battle_month,
        gdb.sales_date,
        gdb.area_name,
        gdb.report_channel_type,
        COALESCE(SUM(etd.day_target_amt), 0) AS day_target_amt,
        COALESCE(ad.day_actual_amt, 0) AS day_actual_amt,
        COALESCE(ly.last_year_day_actual_amt, 0) AS last_year_day_actual_amt,
        gdb.data_version
    FROM group_day_base gdb
    LEFT JOIN entity_target_daily etd
        ON gdb.sales_date = etd.sales_date
       AND gdb.area_name = etd.area_name
       AND gdb.report_channel_type = etd.report_channel_type
    LEFT JOIN actual_daily_by_group ad
        ON gdb.sales_date = ad.sales_date
       AND gdb.area_name = ad.area_name
       AND gdb.report_channel_type = ad.report_channel_type
    LEFT JOIN last_year_daily_by_group ly
        ON gdb.sales_date = ly.sales_date
       AND gdb.area_name = ly.area_name
       AND gdb.report_channel_type = ly.report_channel_type
    GROUP BY
        gdb.report_date,
        gdb.battle_month,
        gdb.sales_date,
        gdb.area_name,
        gdb.report_channel_type,
        ad.day_actual_amt,
        ly.last_year_day_actual_amt,
        gdb.data_version
),
group_day_cumulative AS (
    SELECT
        report_date,
        battle_month,
        sales_date,
        area_name,
        report_channel_type,
        day_target_amt,
        day_actual_amt,
        SUM(day_target_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS cum_target_amt,
        SUM(day_actual_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS cum_actual_amt,
        SUM(last_year_day_actual_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS last_year_cum_actual_amt,
        data_version
    FROM group_day_metrics
),
source_total AS (
    SELECT
        sales_date,
        SUM(day_target_amt) AS day_target_amt,
        SUM(day_actual_amt) AS day_actual_amt,
        SUM(cum_target_amt) AS cum_target_amt,
        SUM(cum_actual_amt) AS cum_actual_amt,
        SUM(last_year_cum_actual_amt) AS last_year_cum_actual_amt
    FROM group_day_cumulative
    GROUP BY sales_date
),
ads_aggregated AS (
    SELECT
        sales_date,
        SUM(day_target_amt) AS day_target_amt,
        SUM(day_actual_amt) AS day_actual_amt,
        SUM(cum_target_amt) AS cum_target_amt,
        SUM(cum_actual_amt) AS cum_actual_amt,
        SUM(last_year_cum_actual_amt) AS last_year_cum_actual_amt
    FROM ads_daily_sales
    WHERE report_date = @report_date
      AND data_version = @data_version
    GROUP BY sales_date
),
compare_result AS (
    SELECT
        s.sales_date,
        s.day_target_amt AS source_day_target_amt,
        a.day_target_amt AS ads_day_target_amt,
        s.day_actual_amt AS source_day_actual_amt,
        a.day_actual_amt AS ads_day_actual_amt,
        s.cum_target_amt AS source_cum_target_amt,
        a.cum_target_amt AS ads_cum_target_amt,
        s.cum_actual_amt AS source_cum_actual_amt,
        a.cum_actual_amt AS ads_cum_actual_amt,
        s.last_year_cum_actual_amt AS source_last_year_cum_actual_amt,
        a.last_year_cum_actual_amt AS ads_last_year_cum_actual_amt,
        CASE
            WHEN ROUND(s.day_target_amt, 2) = ROUND(COALESCE(a.day_target_amt, 0.00), 2)
             AND ROUND(s.day_actual_amt, 2) = ROUND(COALESCE(a.day_actual_amt, 0.00), 2)
             AND ROUND(s.cum_target_amt, 2) = ROUND(COALESCE(a.cum_target_amt, 0.00), 2)
             AND ROUND(s.cum_actual_amt, 2) = ROUND(COALESCE(a.cum_actual_amt, 0.00), 2)
             AND ROUND(s.last_year_cum_actual_amt, 2) = ROUND(COALESCE(a.last_year_cum_actual_amt, 0.00), 2)
            THEN 0 ELSE 1
        END AS mismatch_flag
    FROM source_total s
    LEFT JOIN ads_aggregated a
        ON s.sales_date = a.sales_date
)
SELECT
    'detail_aggregate_series_compare' AS check_name,
    COUNT(*) AS compared_day_count,
    SUM(mismatch_flag) AS mismatch_day_count,
    GROUP_CONCAT(
        CASE WHEN mismatch_flag = 1 THEN DATE_FORMAT(sales_date, '%Y-%m-%d') END
        ORDER BY sales_date SEPARATOR ','
    ) AS mismatch_dates,
    CASE WHEN SUM(mismatch_flag) = 0 THEN 'OK' ELSE 'CHECK' END AS status
FROM compare_result;