# -*- coding: utf-8 -*-
"""
何方珠宝 - 门店经营日报 ETL

当前脚本将已完成样本对账的 SQL 正式封装为独立 ETL 入口，
用于产出 ads_store_daily_report。

当前口径按最终经营实体出数：
1. 未配置共同考核时，保持一店一行。
2. 命中共同考核配置时，主店与快闪店直接在本表合并为同一经营实体行。
3. 月中闭店或退场门店在当月剩余日期继续保留经营实体与月目标，日目标仍按当天配置精确匹配。

注意：
1. 当前仍为独立入口，尚未接入 run_etl.py 主调度。
2. 计算 SQL 已内置在当前脚本中，不依赖外部 .sql 文件。
3. 运行前会做只读前置检查，避免在配置重叠或依赖对象缺失时直接写数。
"""

import argparse
import logging
import time
from datetime import date, datetime, timedelta

from pymysql.cursors import DictCursor

from config import ETL_DEFAULT_MAX_RETRIES, ETL_DEFAULT_RETRY_SLEEP
from db_connections import connect_mysql


SQL_SOURCE_LABEL = 'embedded:etl_ads_store_daily_report.py'
DELETE_SQL_TEMPLATE = """
DELETE FROM ads_store_daily_report
WHERE report_date = @report_date
  AND data_version = @data_version
""".strip()

INSERT_SQL_TEMPLATE = """
INSERT INTO ads_store_daily_report (
    report_date,
    store_id,
    store_code,
    store_name,
    owner_name,
    area_name,
    report_channel_type,
    store_grade,
    is_duty_free,
    day_sales_amt,
    day_sales_qty,
    day_order_cnt,
    day_attach_rate,
    day_avg_ticket,
    day_discount_rate,
    day_target,
    day_ach_rate,
    mtd_sales_amt,
    mtd_list_amt,
    mtd_sales_qty,
    mtd_order_cnt,
    mtd_attach_rate,
    mtd_avg_ticket,
    mtd_discount_rate,
    month_target,
    month_ach_rate,
    last_month_mtd_sales_amt,
    last_month_mtd_sales_qty,
    last_year_mtd_sales_amt,
    same_store_mtd_sales_amt,
    same_store_last_year_mtd_sales_amt,
    yoy_rate,
    yoy_amt_diff,
    last_year_mtd_sales_qty,
    yoy_qty_rate,
    yoy_qty_diff,
    day_rank,
    mtd_rank,
    time_progress,
    data_version,
    etl_time
)
WITH
params AS (
    SELECT
        CAST(@report_date AS DATE) AS report_date,
        CAST(DATE_FORMAT(DATE_FORMAT(@report_date, '%Y-%m-01'), '%Y-%m-%d') AS DATE) AS target_month,
        CAST(DATE_FORMAT(@report_date, '%Y%m%d') AS UNSIGNED) AS report_date_id,
        CAST(DATE_FORMAT(DATE_FORMAT(@report_date, '%Y-%m-01'), '%Y%m%d') AS UNSIGNED) AS month_start_id,
        CAST(DATE_FORMAT(DATE_SUB(@report_date, INTERVAL 1 MONTH), '%Y%m%d') AS UNSIGNED) AS last_month_same_day_id,
        CAST(DATE_FORMAT(DATE_SUB(@report_date, INTERVAL 1 YEAR), '%Y%m%d') AS UNSIGNED) AS last_year_same_day_id,
        CAST(DATE_FORMAT(DATE_FORMAT(DATE_SUB(@report_date, INTERVAL 1 MONTH), '%Y-%m-01'), '%Y%m%d') AS UNSIGNED) AS last_month_start_id,
        CAST(DATE_FORMAT(DATE_FORMAT(DATE_SUB(@report_date, INTERVAL 1 YEAR), '%Y-%m-01'), '%Y%m%d') AS UNSIGNED) AS last_year_start_id,
        DATE_SUB(CAST(DATE_FORMAT(@report_date, '%Y-%m-01') AS DATE), INTERVAL 1 YEAR) AS same_store_open_cutoff,
        DAY(@report_date) / DAY(LAST_DAY(@report_date)) AS time_progress,
        @data_version AS data_version,
        @etl_time AS etl_time
),

target_store_scope AS (
    SELECT DISTINCT
        t.store_id
    FROM cfg_store_target_daily t
    CROSS JOIN params p
    WHERE t.target_version = p.data_version
      AND t.target_date BETWEEN p.target_month AND p.report_date
),

joint_assessment_member_scope AS (
    SELECT DISTINCT
        sa.store_id
    FROM cfg_store_assessment_assignment sa
    CROSS JOIN params p
    WHERE sa.target_month = p.target_month
      AND sa.target_version = p.data_version
      AND sa.subject_code IS NOT NULL
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.target_month
),

joint_assessment_anchor_scope AS (
    SELECT DISTINCT
        sa.anchor_store_id AS store_id
    FROM cfg_store_assessment_assignment sa
    CROSS JOIN params p
    WHERE sa.target_month = p.target_month
      AND sa.target_version = p.data_version
      AND sa.subject_code IS NOT NULL
      AND sa.anchor_store_id IS NOT NULL
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.target_month
),

store_attr_scope AS (
    SELECT store_id FROM target_store_scope
    UNION
    SELECT store_id FROM joint_assessment_member_scope
    UNION
    SELECT store_id FROM joint_assessment_anchor_scope
),

store_attr_candidates AS (
    SELECT
        sra.store_id,
        sra.store_code,
        sra.store_name,
        sra.report_channel_type,
        sra.store_grade,
        sra.is_duty_free,
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
    INNER JOIN store_attr_scope sas
        ON sra.store_id = sas.store_id
    CROSS JOIN params p
    WHERE sra.is_include_in_daily_report = 'Y'
      AND sra.effective_start_date <= p.report_date
),

store_scope AS (
    SELECT
        sac.store_id,
        sac.store_code,
        sac.store_name,
        s.area_name,
        sac.report_channel_type,
        sac.store_grade,
        sac.is_duty_free
    FROM store_attr_candidates sac
    INNER JOIN dim_store s
        ON sac.store_id = s.store_id
    WHERE sac.attr_recency_rank = 1
),

source_store_scope AS (
    SELECT
        s.store_id,
        s.store_code,
        s.store_name,
        s.open_date
    FROM dim_store s
    INNER JOIN (
        SELECT store_id FROM target_store_scope
        UNION
        SELECT store_id FROM joint_assessment_member_scope
    ) scoped_source_store
        ON s.store_id = scoped_source_store.store_id
),

assignment_candidates AS (
    SELECT
        sa.store_id,
        sa.subject_code,
        sa.assignment_role,
        sa.is_joint_assessment,
        sa.effective_start_date,
        COALESCE(
            sa.anchor_store_id,
            CASE
                WHEN sa.assignment_role = '主店' THEN sa.store_id
                ELSE NULL
            END
        ) AS anchor_store_id,
        COALESCE(
            sa.anchor_store_name,
            CASE
                WHEN sa.assignment_role = '主店' THEN sa.store_name
                ELSE NULL
            END
        ) AS anchor_store_name,
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
    CROSS JOIN params p
    WHERE sa.target_month = p.target_month
      AND sa.target_version = p.data_version
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.target_month
),

assignment_scope AS (
    SELECT
        ac.store_id,
        ac.subject_code,
        ac.assignment_role,
        ac.is_joint_assessment,
        ac.effective_start_date,
        ac.anchor_store_id,
        ac.anchor_store_name
    FROM assignment_candidates ac
    WHERE ac.assignment_recency_rank = 1
),

flash_merge_cutoff_scope AS (
    SELECT
        ass.subject_code,
        DATE_SUB(MIN(ass.effective_start_date), INTERVAL 1 DAY) AS merge_before_date
    FROM assignment_scope ass
    CROSS JOIN params p
    WHERE ass.assignment_role = '快闪'
      AND ass.effective_start_date > p.target_month
    GROUP BY ass.subject_code
),

subject_target AS (
    SELECT
        st.subject_code,
        st.subject_name,
        st.day_target,
        st.month_target
    FROM cfg_store_assessment_subject_target_daily st
    CROSS JOIN params p
    WHERE st.target_date = p.report_date
      AND st.target_version = p.data_version
),

subject_target_mtd_latest AS (
    SELECT
        ranked_subject_target.subject_code,
        ranked_subject_target.subject_name,
        ranked_subject_target.month_target
    FROM (
        SELECT
            st.subject_code,
            st.subject_name,
            st.month_target,
            ROW_NUMBER() OVER (
                PARTITION BY st.subject_code
                ORDER BY st.target_date DESC
            ) AS subject_target_recency_rank
        FROM cfg_store_assessment_subject_target_daily st
        CROSS JOIN params p
        WHERE st.target_date BETWEEN p.target_month AND p.report_date
          AND st.target_version = p.data_version
    ) ranked_subject_target
    WHERE ranked_subject_target.subject_target_recency_rank = 1
),

store_entity_map AS (
    SELECT
        sss.store_id AS source_store_id,
        sss.store_code AS source_store_code,
        sss.store_name AS source_store_name,
        sss.open_date AS source_store_open_date,
        COALESCE(
            CASE
                WHEN ass.subject_code IS NOT NULL THEN COALESCE(ass.anchor_store_id, sss.store_id)
                ELSE NULL
            END,
            sss.store_id
        ) AS report_entity_id,
        CONVERT(
            CASE
                WHEN ass.subject_code IS NOT NULL THEN LEFT(ass.subject_code, 40)
                ELSE sss.store_code
            END USING utf8mb4
        ) COLLATE utf8mb4_0900_ai_ci AS report_entity_code,
        CONVERT(
            CASE
                WHEN ass.subject_code IS NOT NULL THEN 'SUBJECT'
                ELSE 'STORE'
            END USING utf8mb4
        ) COLLATE utf8mb4_0900_ai_ci AS report_entity_type,
            CONVERT(
                CASE
                    WHEN ass.subject_code IS NOT NULL THEN COALESCE(st.subject_name, stml.subject_name, ass.subject_code)
                    ELSE sss.store_name
                END USING utf8mb4
            ) COLLATE utf8mb4_0900_ai_ci AS report_entity_name,
            CONVERT(COALESCE(anchor_ss.area_name, ss.area_name) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS area_name,
            CONVERT(COALESCE(anchor_ss.report_channel_type, ss.report_channel_type) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS report_channel_type,
            CONVERT(COALESCE(anchor_ss.store_grade, ss.store_grade) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS store_grade,
            CONVERT(COALESCE(anchor_ss.is_duty_free, ss.is_duty_free) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS is_duty_free,
        ass.assignment_role,
        ass.subject_code,
        st.day_target AS subject_day_target,
        COALESCE(st.month_target, stml.month_target) AS subject_month_target
    FROM source_store_scope sss
    LEFT JOIN assignment_scope ass
        ON sss.store_id = ass.store_id
    LEFT JOIN subject_target st
        ON ass.subject_code = st.subject_code
    LEFT JOIN subject_target_mtd_latest stml
        ON ass.subject_code = stml.subject_code
    LEFT JOIN store_scope ss
        ON sss.store_id = ss.store_id
    LEFT JOIN store_scope anchor_ss
        ON COALESCE(ass.anchor_store_id, sss.store_id) = anchor_ss.store_id
),

report_entity_scope AS (
    SELECT
        sem.report_entity_id AS store_id,
        CONVERT(MAX(sem.report_entity_code) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS store_code,
        CONVERT(MAX(sem.report_entity_type) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS report_entity_type,
        CONVERT(MAX(sem.report_entity_name) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS store_name,
        CONVERT(MAX(sem.area_name) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS area_name,
        CONVERT(MAX(sem.report_channel_type) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS report_channel_type,
        CONVERT(MAX(sem.store_grade) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS store_grade,
        CONVERT(MAX(sem.is_duty_free) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS is_duty_free
    FROM store_entity_map sem
    GROUP BY sem.report_entity_id
),

excluded_category_scope AS (
    SELECT 147 AS category_id
    UNION ALL SELECT 149
    UNION ALL SELECT 150
),

detail_base AS (
    SELECT
        r.billdate AS date_id,
        r.id AS retail_id,
        r.docno,
        r.c_store_id AS store_id,
        r.tot_amt_actual AS retail_amt,
        ri.id AS retail_item_id,
        ri.m_product_id AS product_id,
        ri.m_productalias_id AS sku_id,
        ri.qty,
        ri.tot_amt_actual,
        ri.tot_amt_list
    FROM ods_m_retail r
    INNER JOIN ods_m_retailitem ri
        ON r.id = ri.m_retail_id
    CROSS JOIN params p
    WHERE r.isactive = 'Y'
      AND r.status = 2
      AND ri.m_productalias_id IS NOT NULL
      AND r.billdate BETWEEN p.last_year_start_id AND p.report_date_id
            AND ABS(ri.tot_amt_actual) >= 1
),

filtered_detail AS (
    SELECT
        db.date_id,
        db.retail_id,
        db.docno,
        sem.source_store_id,
        sem.report_entity_id AS store_id,
        db.retail_amt,
        db.product_id,
        db.sku_id,
        db.qty,
        db.tot_amt_actual,
        db.tot_amt_list
    FROM detail_base db
    INNER JOIN store_entity_map sem
        ON db.store_id = sem.source_store_id
    INNER JOIN dim_product dp
        ON db.product_id = dp.product_id
    LEFT JOIN excluded_category_scope ecs
        ON dp.category_id = ecs.category_id
    WHERE dp.category_id IS NOT NULL
      AND ecs.category_id IS NULL
),

day_fact AS (
    SELECT
        fd.store_id,
        SUM(fd.tot_amt_actual) AS day_sales_amt,
        SUM(fd.qty) AS day_sales_qty,
        SUM(fd.tot_amt_list) AS day_list_amt
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id = p.report_date_id
    GROUP BY fd.store_id
),

day_order_fact AS (
    SELECT
        fd.store_id,
        SUM(
            CASE
                WHEN ABS(fd.filtered_retail_amt) < 0.0001 THEN 0
                WHEN fd.filtered_retail_amt > 0 THEN 1
                WHEN fd.filtered_retail_amt < 0 THEN -1
                ELSE 0
            END
        ) AS day_order_cnt
    FROM (
        SELECT
            fd.store_id,
            fd.retail_id,
            ROUND(SUM(fd.tot_amt_actual), 4) AS filtered_retail_amt
        FROM filtered_detail fd
        CROSS JOIN params p
        WHERE fd.date_id = p.report_date_id
        GROUP BY fd.store_id, fd.retail_id
    ) fd
    GROUP BY fd.store_id
),

mtd_fact AS (
    SELECT
        fd.store_id,
        SUM(fd.tot_amt_actual) AS mtd_sales_amt,
        SUM(fd.qty) AS mtd_sales_qty,
        SUM(fd.tot_amt_list) AS mtd_list_amt
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.month_start_id AND p.report_date_id
    GROUP BY fd.store_id
),

mtd_order_fact AS (
    SELECT
        fd.store_id,
        SUM(
            CASE
                WHEN ABS(fd.filtered_retail_amt) < 0.0001 THEN 0
                WHEN fd.filtered_retail_amt > 0 THEN 1
                WHEN fd.filtered_retail_amt < 0 THEN -1
                ELSE 0
            END
        ) AS mtd_order_cnt
    FROM (
        SELECT
            fd.store_id,
            fd.retail_id,
            ROUND(SUM(fd.tot_amt_actual), 4) AS filtered_retail_amt
        FROM filtered_detail fd
        CROSS JOIN params p
        WHERE fd.date_id BETWEEN p.month_start_id AND p.report_date_id
        GROUP BY fd.store_id, fd.retail_id
    ) fd
    GROUP BY fd.store_id
),

last_month_mtd_fact AS (
    SELECT
        fd.store_id,
        SUM(fd.tot_amt_actual) AS last_month_mtd_sales_amt,
        SUM(fd.qty) AS last_month_mtd_sales_qty
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.last_month_start_id AND p.last_month_same_day_id
    GROUP BY fd.store_id
),

last_year_mtd_fact AS (
    SELECT
        fd.store_id,
        SUM(fd.tot_amt_actual) AS last_year_mtd_sales_amt,
        SUM(fd.qty) AS last_year_mtd_sales_qty
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.last_year_start_id AND p.last_year_same_day_id
    GROUP BY fd.store_id
),

same_store_current_fact AS (
    SELECT
        fd.source_store_id,
        SUM(fd.tot_amt_actual) AS same_store_mtd_sales_amt
    FROM filtered_detail fd
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.month_start_id AND p.report_date_id
    GROUP BY fd.source_store_id
),

same_store_last_year_fact AS (
    SELECT
        fd.source_store_id,
        SUM(fd.tot_amt_actual) AS same_store_last_year_mtd_sales_amt
    FROM filtered_detail fd
    LEFT JOIN store_entity_map sem
        ON fd.source_store_id = sem.source_store_id
    LEFT JOIN flash_merge_cutoff_scope fmcs
        ON sem.subject_code = fmcs.subject_code
    CROSS JOIN params p
    WHERE fd.date_id BETWEEN p.last_year_start_id AND p.last_year_same_day_id
      AND fd.date_id <= COALESCE(
          CAST(DATE_FORMAT(DATE_SUB(fmcs.merge_before_date, INTERVAL 1 YEAR), '%Y%m%d') AS UNSIGNED),
          p.last_year_same_day_id
      )
    GROUP BY fd.source_store_id
),

same_store_entity_fact AS (
    SELECT
        sem.report_entity_id AS store_id,
        SUM(COALESCE(sscf.same_store_mtd_sales_amt, 0.00)) AS same_store_mtd_sales_amt,
        SUM(COALESCE(sslyf.same_store_last_year_mtd_sales_amt, 0.00)) AS same_store_last_year_mtd_sales_amt
    FROM store_entity_map sem
    LEFT JOIN same_store_current_fact sscf
        ON sem.source_store_id = sscf.source_store_id
    LEFT JOIN same_store_last_year_fact sslyf
        ON sem.source_store_id = sslyf.source_store_id
        CROSS JOIN params p
        WHERE sem.source_store_open_date IS NOT NULL
            AND sem.source_store_open_date <= p.same_store_open_cutoff
            AND COALESCE(sem.assignment_role, '') <> '快闪'
    GROUP BY sem.report_entity_id
),

target_day AS (
    SELECT
        t.store_id,
        t.day_target,
        t.month_target
    FROM cfg_store_target_daily t
    CROSS JOIN params p
    WHERE t.target_date = p.report_date
      AND t.target_version = p.data_version
),

target_mtd_latest AS (
    SELECT
        ranked_target.store_id,
        ranked_target.month_target
    FROM (
        SELECT
            t.store_id,
            t.month_target,
            ROW_NUMBER() OVER (
                PARTITION BY t.store_id
                ORDER BY t.target_date DESC
            ) AS target_recency_rank
        FROM cfg_store_target_daily t
        CROSS JOIN params p
        WHERE t.target_date BETWEEN p.target_month AND p.report_date
          AND t.target_version = p.data_version
    ) ranked_target
    WHERE ranked_target.target_recency_rank = 1
),

entity_target AS (
    SELECT
        sem.report_entity_id AS store_id,
        COALESCE(MAX(sem.subject_day_target), SUM(COALESCE(td.day_target, 0.00))) AS day_target,
        COALESCE(MAX(sem.subject_month_target), SUM(COALESCE(tml.month_target, td.month_target, 0.00))) AS month_target
    FROM store_entity_map sem
    LEFT JOIN target_day td
        ON sem.source_store_id = td.store_id
    LEFT JOIN target_mtd_latest tml
        ON sem.source_store_id = tml.store_id
    GROUP BY sem.report_entity_id
),

owner_assignment_candidates AS (
    SELECT
        oa.entity_type,
        oa.entity_code,
        oa.owner_name,
        ROW_NUMBER() OVER (
            PARTITION BY oa.entity_type, oa.entity_code
            ORDER BY
                CASE
                    WHEN p.report_date BETWEEN oa.effective_start_date AND oa.effective_end_date THEN 0
                    ELSE 1
                END,
                oa.effective_end_date DESC,
                oa.effective_start_date DESC
        ) AS owner_recency_rank
    FROM dim_store_operation_owner_assignment oa
    CROSS JOIN params p
    WHERE oa.effective_start_date <= p.report_date
      AND oa.effective_end_date >= p.target_month
),

owner_assignment_scope AS (
    SELECT
        CONVERT(oac.entity_type USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_type,
        CONVERT(oac.entity_code USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_code,
        oac.owner_name
    FROM owner_assignment_candidates oac
    WHERE oac.owner_recency_rank = 1
),

duty_free_mtd_fact AS (
    SELECT
        sem.report_entity_id AS store_id,
        SUM(dfm.external_mtd_sales_amt) AS external_mtd_sales_amt
    FROM cfg_duty_free_store_mtd_sales dfm
    INNER JOIN store_entity_map sem
        ON dfm.store_id = sem.source_store_id
    CROSS JOIN params p
        WHERE dfm.target_month = p.target_month
      AND dfm.data_version = p.data_version
    GROUP BY sem.report_entity_id
),

assembled AS (
    SELECT
        p.report_date,
        res.store_id,
        res.store_code,
        res.store_name,
        oas.owner_name,
        res.area_name,
        res.report_channel_type,
        res.store_grade,
        res.is_duty_free,
        COALESCE(df.day_sales_amt, 0.00) AS day_sales_amt,
        COALESCE(df.day_sales_qty, 0) AS day_sales_qty,
        COALESCE(dof.day_order_cnt, 0) AS day_order_cnt,
        CASE
            WHEN COALESCE(dof.day_order_cnt, 0) = 0 THEN NULL
            ELSE ROUND(COALESCE(df.day_sales_qty, 0) / dof.day_order_cnt, 4)
        END AS day_attach_rate,
        CASE
            WHEN COALESCE(dof.day_order_cnt, 0) = 0 THEN NULL
            ELSE ROUND(COALESCE(df.day_sales_amt, 0.00) / dof.day_order_cnt, 2)
        END AS day_avg_ticket,
        CASE
            WHEN COALESCE(df.day_list_amt, 0) = 0 THEN NULL
            ELSE ROUND(df.day_sales_amt / df.day_list_amt, 4)
        END AS day_discount_rate,
        COALESCE(et.day_target, 0.00) AS day_target,
        CASE
            WHEN COALESCE(et.day_target, 0) = 0 THEN NULL
            ELSE ROUND(COALESCE(df.day_sales_amt, 0.00) / et.day_target, 4)
        END AS day_ach_rate,
        CASE
            WHEN res.is_duty_free = 'Y' AND dmf.external_mtd_sales_amt IS NOT NULL THEN dmf.external_mtd_sales_amt
            ELSE COALESCE(mf.mtd_sales_amt, 0.00)
        END AS mtd_sales_amt,
        COALESCE(mf.mtd_list_amt, 0.00) AS mtd_list_amt,
        COALESCE(mf.mtd_sales_qty, 0) AS mtd_sales_qty,
        COALESCE(mof.mtd_order_cnt, 0) AS mtd_order_cnt,
        CASE
            WHEN COALESCE(mof.mtd_order_cnt, 0) = 0 THEN NULL
            ELSE ROUND(COALESCE(mf.mtd_sales_qty, 0) / mof.mtd_order_cnt, 4)
        END AS mtd_attach_rate,
        CASE
            WHEN COALESCE(mof.mtd_order_cnt, 0) = 0 THEN NULL
            ELSE ROUND(COALESCE(mf.mtd_sales_amt, 0.00) / mof.mtd_order_cnt, 2)
        END AS mtd_avg_ticket,
        CASE
            WHEN COALESCE(mf.mtd_list_amt, 0) = 0 THEN NULL
            ELSE ROUND(mf.mtd_sales_amt / mf.mtd_list_amt, 4)
        END AS mtd_discount_rate,
        COALESCE(et.month_target, 0.00) AS month_target,
        CASE
            WHEN COALESCE(et.month_target, 0) = 0 THEN NULL
            ELSE ROUND(
                CASE
                    WHEN res.is_duty_free = 'Y' AND dmf.external_mtd_sales_amt IS NOT NULL THEN dmf.external_mtd_sales_amt
                    ELSE COALESCE(mf.mtd_sales_amt, 0.00)
                END / et.month_target,
                4
            )
        END AS month_ach_rate,
        COALESCE(lmmf.last_month_mtd_sales_amt, 0.00) AS last_month_mtd_sales_amt,
        COALESCE(lmmf.last_month_mtd_sales_qty, 0) AS last_month_mtd_sales_qty,
        COALESCE(lymf.last_year_mtd_sales_amt, 0.00) AS last_year_mtd_sales_amt,
        COALESCE(ssef.same_store_mtd_sales_amt, 0.00) AS same_store_mtd_sales_amt,
        COALESCE(ssef.same_store_last_year_mtd_sales_amt, 0.00) AS same_store_last_year_mtd_sales_amt,
        CASE
            WHEN COALESCE(ssef.same_store_last_year_mtd_sales_amt, 0) = 0 THEN NULL
            ELSE ROUND((ssef.same_store_mtd_sales_amt / ssef.same_store_last_year_mtd_sales_amt) - 1, 4)
        END AS yoy_rate,
        COALESCE(ssef.same_store_mtd_sales_amt, 0.00) - COALESCE(ssef.same_store_last_year_mtd_sales_amt, 0.00) AS yoy_amt_diff,
        COALESCE(lymf.last_year_mtd_sales_qty, 0) AS last_year_mtd_sales_qty,
        CASE
            WHEN COALESCE(lymf.last_year_mtd_sales_qty, 0) = 0 THEN NULL
            ELSE ROUND((mf.mtd_sales_qty / lymf.last_year_mtd_sales_qty) - 1, 4)
        END AS yoy_qty_rate,
        COALESCE(mf.mtd_sales_qty, 0) - COALESCE(lymf.last_year_mtd_sales_qty, 0) AS yoy_qty_diff,
        RANK() OVER (ORDER BY COALESCE(df.day_sales_amt, 0.00) DESC, res.store_id) AS day_rank,
        RANK() OVER (
            ORDER BY
                CASE
                    WHEN res.is_duty_free = 'Y' AND dmf.external_mtd_sales_amt IS NOT NULL THEN dmf.external_mtd_sales_amt
                    ELSE COALESCE(mf.mtd_sales_amt, 0.00)
                END DESC,
                res.store_id
        ) AS mtd_rank,
        p.time_progress,
        p.data_version,
        p.etl_time
    FROM report_entity_scope res
    CROSS JOIN params p
    LEFT JOIN day_fact df
        ON res.store_id = df.store_id
    LEFT JOIN day_order_fact dof
        ON res.store_id = dof.store_id
    LEFT JOIN mtd_fact mf
        ON res.store_id = mf.store_id
    LEFT JOIN duty_free_mtd_fact dmf
        ON res.store_id = dmf.store_id
    LEFT JOIN mtd_order_fact mof
        ON res.store_id = mof.store_id
    LEFT JOIN last_month_mtd_fact lmmf
        ON res.store_id = lmmf.store_id
    LEFT JOIN last_year_mtd_fact lymf
        ON res.store_id = lymf.store_id
    LEFT JOIN same_store_entity_fact ssef
        ON res.store_id = ssef.store_id
    LEFT JOIN entity_target et
        ON res.store_id = et.store_id
    LEFT JOIN owner_assignment_scope oas
        ON res.report_entity_type = oas.entity_type
       AND res.store_code = oas.entity_code
)

SELECT
    report_date,
    store_id,
    store_code,
    store_name,
    owner_name,
    area_name,
    report_channel_type,
    store_grade,
    is_duty_free,
    day_sales_amt,
    day_sales_qty,
    day_order_cnt,
    day_attach_rate,
    day_avg_ticket,
    day_discount_rate,
    day_target,
    day_ach_rate,
    mtd_sales_amt,
    mtd_list_amt,
    mtd_sales_qty,
    mtd_order_cnt,
    mtd_attach_rate,
    mtd_avg_ticket,
    mtd_discount_rate,
    month_target,
    month_ach_rate,
    last_month_mtd_sales_amt,
    last_month_mtd_sales_qty,
    last_year_mtd_sales_amt,
    same_store_mtd_sales_amt,
    same_store_last_year_mtd_sales_amt,
    yoy_rate,
    yoy_amt_diff,
    last_year_mtd_sales_qty,
    yoy_qty_rate,
    yoy_qty_diff,
    day_rank,
    mtd_rank,
    time_progress,
    data_version,
    etl_time
FROM assembled
""".strip()

REQUIRED_TABLES = (
    'dim_store_report_attr',
    'cfg_store_target_daily',
    'cfg_duty_free_store_mtd_sales',
    'cfg_store_assessment_subject_target_daily',
    'cfg_store_assessment_assignment',
    'dim_store_operation_owner_assignment',
    'ads_store_daily_report',
    'ods_m_retail',
    'ods_m_retailitem',
    'dim_product',
    'dim_store',
)
REQUIRED_COLUMNS = {
    'dim_store': (
        'open_date',
    ),
    'ads_store_daily_report': (
        'owner_name',
        'mtd_list_amt',
        'same_store_mtd_sales_amt',
        'same_store_last_year_mtd_sales_amt',
    ),
}
REQUIRED_SQL_SNIPPETS = (
    'DELETE FROM ads_store_daily_report',
    'INSERT INTO ads_store_daily_report (',
    'ABS(ri.tot_amt_actual) >= 1',
    'SELECT 147 AS category_id',
    'WHERE dp.category_id IS NOT NULL',
    'FROM cfg_store_assessment_assignment sa',
    'FROM cfg_store_assessment_subject_target_daily st',
    'FROM dim_store_operation_owner_assignment oa',
    't.target_date BETWEEN p.target_month AND p.report_date',
    'WHEN p.report_date BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0',
    'COALESCE(st.month_target, stml.month_target) AS subject_month_target',
    'COALESCE(MAX(sem.subject_month_target), SUM(COALESCE(tml.month_target, td.month_target, 0.00))) AS month_target',
    'COALESCE(mf.mtd_list_amt, 0.00) AS mtd_list_amt',
    'duty_free_mtd_fact AS (',
    "WHEN res.is_duty_free = 'Y' AND dmf.external_mtd_sales_amt IS NOT NULL THEN dmf.external_mtd_sales_amt",
    'same_store_entity_fact AS (',
    'sem.source_store_open_date <= p.same_store_open_cutoff',
    "COALESCE(sem.assignment_role, '') <> '快闪'",
    't.target_version = p.data_version',
)
RETRYABLE_MYSQL_LOCK_KEYWORDS = (
    '1213',
    '1205',
    'deadlock found',
    'lock wait timeout exceeded',
)


def _setup_logger():
    logger = logging.getLogger(__name__)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


logger = _setup_logger()


def _is_retryable_mysql_lock_error(exc):
    message = str(exc).lower()
    return any(keyword in message for keyword in RETRYABLE_MYSQL_LOCK_KEYWORDS)


def _connect():
    return connect_mysql(
        timeout_profile='long_running',
        cursorclass=DictCursor,
        autocommit=False,
    )


def _escape_sql_string(value):
    return value.replace("'", "''")


def _validate_sql_skeleton():
    sql_text = '\n'.join((DELETE_SQL_TEMPLATE, INSERT_SQL_TEMPLATE))
    missing_snippets = [snippet for snippet in REQUIRED_SQL_SNIPPETS if snippet not in sql_text]
    if missing_snippets:
        joined = '; '.join(missing_snippets)
        raise RuntimeError(f'SQL 骨架缺少关键片段: {joined}')


def _render_sql_template(sql_text, report_date, data_version, etl_time):
    report_date_literal = f"'{report_date.isoformat()}'"
    data_version_literal = f"'{_escape_sql_string(data_version)}'"
    etl_time_literal = f"'{etl_time.strftime('%Y-%m-%d %H:%M:%S')}'"

    return (
        sql_text
        .replace('@report_date', report_date_literal)
        .replace('@data_version', data_version_literal)
        .replace('@etl_time', etl_time_literal)
    )


def _build_sql_statements(report_date, data_version, etl_time):
    _validate_sql_skeleton()
    delete_sql = _render_sql_template(DELETE_SQL_TEMPLATE, report_date, data_version, etl_time).strip()
    insert_sql = _render_sql_template(INSERT_SQL_TEMPLATE, report_date, data_version, etl_time).strip()
    return delete_sql, insert_sql


def _fetch_required_table_state(conn):
    placeholders = ', '.join(['%s'] * len(REQUIRED_TABLES))
    sql = f"""
        SELECT table_name AS table_name_alias
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name IN ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, REQUIRED_TABLES)
        existing_tables = set()
        for row in cursor.fetchall():
            table_name = row.get('table_name_alias') or row.get('TABLE_NAME_ALIAS')
            if table_name:
                existing_tables.add(table_name)

    missing_tables = [table_name for table_name in REQUIRED_TABLES if table_name not in existing_tables]
    return missing_tables


def _fetch_required_column_state(conn):
    if not REQUIRED_COLUMNS:
        return []

    table_names = tuple(REQUIRED_COLUMNS.keys())
    placeholders = ', '.join(['%s'] * len(table_names))
    sql = f"""
        SELECT
            table_name AS table_name_alias,
            column_name AS column_name_alias
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name IN ({placeholders})
    """
    existing_columns = {table_name: set() for table_name in table_names}
    with conn.cursor() as cursor:
        cursor.execute(sql, table_names)
        for row in cursor.fetchall():
            table_name = row.get('table_name_alias') or row.get('TABLE_NAME_ALIAS')
            column_name = row.get('column_name_alias') or row.get('COLUMN_NAME_ALIAS')
            if table_name in existing_columns and column_name:
                existing_columns[table_name].add(column_name)

    missing_columns = []
    for table_name, required_columns in REQUIRED_COLUMNS.items():
        for column_name in required_columns:
            if column_name not in existing_columns.get(table_name, set()):
                missing_columns.append(f'{table_name}.{column_name}')
    return missing_columns


def _fetch_config_stats(conn, report_date, data_version):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            joint_assessment_anchor_scope AS (
                SELECT DISTINCT
                    sa.anchor_store_id AS store_id
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.subject_code IS NOT NULL
                  AND sa.anchor_store_id IS NOT NULL
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            store_attr_scope AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
                UNION
                SELECT store_id FROM joint_assessment_anchor_scope
            ),
            source_store_ids AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
            ),
            store_attr_candidates AS (
                SELECT
                    sra.store_id,
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
                INNER JOIN store_attr_scope sas
                    ON sra.store_id = sas.store_id
                CROSS JOIN scope_params p
                WHERE sra.is_include_in_daily_report = 'Y'
                  AND sra.effective_start_date <= p.report_date
            )
            SELECT
                COUNT(*) AS store_attr_row_count,
                (SELECT COUNT(*) FROM source_store_ids) AS distinct_store_count,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT sra.store_id
                        FROM dim_store_report_attr sra
                        INNER JOIN store_attr_scope sas
                            ON sra.store_id = sas.store_id
                        CROSS JOIN scope_params p
                        WHERE sra.is_include_in_daily_report = 'Y'
                          AND p.report_date BETWEEN sra.effective_start_date AND sra.effective_end_date
                        GROUP BY sra.store_id
                        HAVING COUNT(*) > 1
                    ) overlap_scope
                ) AS store_attr_overlap_store_count
            FROM store_attr_candidates sac
            WHERE sac.attr_recency_rank = 1
            """,
            (report_date, report_date, data_version),
        )
        store_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            source_store_ids AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
            )
            SELECT
                COUNT(*) AS missing_dim_store_count,
                GROUP_CONCAT(
                    CAST(missing_scope.store_id AS CHAR)
                    ORDER BY missing_scope.store_id
                    SEPARATOR ', '
                ) AS missing_dim_store_examples
            FROM (
                SELECT
                    ssi.store_id
                FROM source_store_ids ssi
                LEFT JOIN dim_store s
                    ON ssi.store_id = s.store_id
                WHERE s.store_id IS NULL
                ORDER BY ssi.store_id
                LIMIT 10
            ) missing_scope
            """,
            (report_date, report_date, data_version),
        )
        missing_dim_store_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT sa.store_id
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            source_store_ids AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
            ),
            unusable_open_date_samples AS (
                SELECT
                    s.store_id,
                    s.store_code,
                    s.store_name
                FROM source_store_ids ssi
                INNER JOIN dim_store s
                    ON ssi.store_id = s.store_id
                WHERE s.open_date IS NULL
                ORDER BY s.store_id
                LIMIT 10
            )
            SELECT
                (SELECT COUNT(*) FROM source_store_ids) AS same_store_scope_count,
                (
                    SELECT COUNT(*)
                    FROM source_store_ids ssi
                    INNER JOIN dim_store s
                        ON ssi.store_id = s.store_id
                    WHERE s.open_date IS NULL
                ) AS unusable_open_date_count,
                (
                    SELECT GROUP_CONCAT(
                        CONCAT(store_id, '/', COALESCE(store_code, ''), '/', COALESCE(store_name, ''))
                        ORDER BY store_id
                        SEPARATOR ', '
                    )
                    FROM unusable_open_date_samples
                ) AS unusable_open_date_examples
            """,
            (report_date, report_date, data_version),
        )
        open_date_quality_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            source_store_scope AS (
                SELECT
                    s.store_id,
                    s.store_code
                FROM dim_store s
                INNER JOIN (
                    SELECT store_id FROM target_store_scope
                    UNION
                    SELECT store_id FROM joint_assessment_member_scope
                ) scoped_source_store
                    ON s.store_id = scoped_source_store.store_id
            ),
            assignment_candidates AS (
                SELECT
                    sa.store_id,
                    sa.subject_code,
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
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            assignment_scope AS (
                SELECT
                    ac.store_id,
                    ac.subject_code
                FROM assignment_candidates ac
                WHERE ac.assignment_recency_rank = 1
            )
            SELECT
                COUNT(DISTINCT CASE
                    WHEN sa.subject_code IS NOT NULL THEN CONCAT('SUBJECT#', sa.subject_code)
                    ELSE CONCAT('STORE#', CAST(ss.store_id AS CHAR))
                END) AS distinct_entity_count,
                COUNT(DISTINCT CASE
                    WHEN sa.subject_code IS NOT NULL THEN sa.subject_code
                    ELSE NULL
                END) AS configured_subject_count
            FROM source_store_scope ss
            LEFT JOIN assignment_scope sa
              ON ss.store_id = sa.store_id
            """,
            (report_date, report_date, data_version),
        )
        entity_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            source_store_scope AS (
                SELECT
                    s.store_id,
                    s.store_code
                FROM dim_store s
                INNER JOIN (
                    SELECT store_id FROM target_store_scope
                    UNION
                    SELECT store_id FROM joint_assessment_member_scope
                ) scoped_source_store
                    ON s.store_id = scoped_source_store.store_id
            ),
            assignment_candidates AS (
                SELECT
                    sa.store_id,
                    sa.subject_code,
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
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            assignment_scope AS (
                SELECT
                    ac.store_id,
                    ac.subject_code
                FROM assignment_candidates ac
                WHERE ac.assignment_recency_rank = 1
            ),
            expected_entities AS (
                SELECT DISTINCT
                    CONVERT(
                        CASE
                            WHEN sa.subject_code IS NOT NULL THEN 'SUBJECT'
                            ELSE 'STORE'
                        END USING utf8mb4
                    ) COLLATE utf8mb4_0900_ai_ci AS entity_type,
                    CONVERT(
                        CASE
                            WHEN sa.subject_code IS NOT NULL THEN LEFT(sa.subject_code, 40)
                            ELSE ss.store_code
                        END USING utf8mb4
                    ) COLLATE utf8mb4_0900_ai_ci AS entity_code
                FROM source_store_scope ss
                LEFT JOIN assignment_scope sa
                  ON ss.store_id = sa.store_id
            ),
            owner_assignment_candidates AS (
                SELECT
                    oa.entity_type,
                    oa.entity_code,
                    oa.owner_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY oa.entity_type, oa.entity_code
                        ORDER BY
                            CASE
                                WHEN p.report_date BETWEEN oa.effective_start_date AND oa.effective_end_date THEN 0
                                ELSE 1
                            END,
                            oa.effective_end_date DESC,
                            oa.effective_start_date DESC
                    ) AS owner_recency_rank
                FROM dim_store_operation_owner_assignment oa
                CROSS JOIN scope_params p
                WHERE oa.effective_start_date <= p.report_date
                  AND oa.effective_end_date >= p.target_month
            ),
            owner_assignment_scope AS (
                SELECT
                    CONVERT(oac.entity_type USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_type,
                    CONVERT(oac.entity_code USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_code,
                    oac.owner_name
                FROM owner_assignment_candidates oac
                WHERE oac.owner_recency_rank = 1
            )
            SELECT
                COUNT(oas.entity_code) AS owner_assignment_row_count,
                COUNT(DISTINCT CASE
                    WHEN oas.entity_code IS NOT NULL THEN CONCAT(oas.entity_type, '#', oas.entity_code)
                    ELSE NULL
                END) AS owner_assignment_entity_count,
                SUM(CASE WHEN oas.entity_code IS NULL THEN 1 ELSE 0 END) AS missing_owner_entity_count,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT doa.entity_type, doa.entity_code
                        FROM dim_store_operation_owner_assignment doa
                        INNER JOIN expected_entities ee
                          ON ee.entity_type = doa.entity_type
                         AND ee.entity_code = doa.entity_code
                        CROSS JOIN scope_params p
                        WHERE p.report_date BETWEEN doa.effective_start_date AND doa.effective_end_date
                        GROUP BY doa.entity_type, doa.entity_code
                        HAVING COUNT(*) > 1
                    ) overlap_owner
                ) AS owner_overlap_entity_count
            FROM expected_entities ee
            LEFT JOIN owner_assignment_scope oas
              ON ee.entity_type = oas.entity_type
             AND ee.entity_code = oas.entity_code
            """,
            (report_date, report_date, data_version),
        )
        owner_assignment_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT MIN(effective_start_date) AS owner_history_start_date
            FROM dim_store_operation_owner_assignment
            """
        )
        owner_history_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            )
            SELECT
                SUM(CASE WHEN t.target_date = p.report_date THEN 1 ELSE 0 END) AS target_row_count,
                COUNT(DISTINCT t.store_id) AS target_store_count
            FROM cfg_store_target_daily t
            CROSS JOIN scope_params p
            WHERE t.target_version = p.data_version
              AND t.target_date BETWEEN p.target_month AND p.report_date
            """,
            (report_date, report_date, data_version),
        )
        target_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            active_assignment AS (
                SELECT
                    sa.store_id,
                    COUNT(*) AS active_row_count
                FROM cfg_store_assessment_assignment sa
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND p.report_date BETWEEN sa.effective_start_date AND sa.effective_end_date
                GROUP BY sa.store_id
            )
            SELECT
                COALESCE(SUM(active_row_count), 0) AS assignment_row_count,
                COUNT(store_id) AS assignment_store_count,
                COALESCE(SUM(CASE WHEN active_row_count > 1 THEN 1 ELSE 0 END), 0) AS assignment_overlap_store_count
            FROM active_assignment
            """,
            (report_date, report_date, data_version),
        )
        assignment_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH scope_params AS (
                SELECT
                    CAST(%s AS DATE) AS report_date,
                    CAST(DATE_FORMAT(%s, '%%Y-%%m-01') AS DATE) AS target_month,
                    %s AS data_version
            ),
            target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                CROSS JOIN scope_params p
                WHERE t.target_version = p.data_version
                  AND t.target_date BETWEEN p.target_month AND p.report_date
            ),
            assignment_candidates AS (
                SELECT
                    sa.store_id,
                    sa.subject_code,
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
                CROSS JOIN scope_params p
                WHERE sa.target_month = p.target_month
                  AND sa.target_version = p.data_version
                  AND sa.effective_start_date <= p.report_date
                  AND sa.effective_end_date >= p.target_month
            ),
            assignment_scope AS (
                SELECT DISTINCT
                    ac.subject_code
                FROM assignment_candidates ac
                WHERE ac.assignment_recency_rank = 1
                  AND ac.subject_code IS NOT NULL
            ),
            subject_target_scope AS (
                SELECT DISTINCT
                    st.subject_code
                FROM cfg_store_assessment_subject_target_daily st
                CROSS JOIN scope_params p
                WHERE st.target_version = p.data_version
                  AND st.target_date BETWEEN p.target_month AND p.report_date
            )
            SELECT COUNT(*) AS missing_subject_target_count
            FROM assignment_scope ass
            LEFT JOIN subject_target_scope sts
              ON ass.subject_code = sts.subject_code
            WHERE sts.subject_code IS NULL
            """,
            (report_date, report_date, data_version),
        )
        subject_target_stats = cursor.fetchone()

    return {
        'store_attr_row_count': int(store_stats['store_attr_row_count'] or 0),
        'distinct_store_count': int(store_stats['distinct_store_count'] or 0),
        'store_attr_overlap_store_count': int(store_stats['store_attr_overlap_store_count'] or 0),
        'missing_dim_store_count': int(missing_dim_store_stats['missing_dim_store_count'] or 0),
        'missing_dim_store_examples': missing_dim_store_stats['missing_dim_store_examples'] or '',
        'same_store_scope_count': int(open_date_quality_stats['same_store_scope_count'] or 0),
        'unusable_open_date_count': int(open_date_quality_stats['unusable_open_date_count'] or 0),
        'unusable_open_date_examples': open_date_quality_stats['unusable_open_date_examples'] or '',
        'distinct_entity_count': int(entity_stats['distinct_entity_count'] or 0),
        'configured_subject_count': int(entity_stats['configured_subject_count'] or 0),
        'owner_assignment_row_count': int(owner_assignment_stats['owner_assignment_row_count'] or 0),
        'owner_assignment_entity_count': int(owner_assignment_stats['owner_assignment_entity_count'] or 0),
        'missing_owner_entity_count': int(owner_assignment_stats['missing_owner_entity_count'] or 0),
        'owner_overlap_entity_count': int(owner_assignment_stats['owner_overlap_entity_count'] or 0),
        'owner_history_start_date': owner_history_stats['owner_history_start_date'],
        'target_row_count': int(target_stats['target_row_count'] or 0),
        'target_store_count': int(target_stats['target_store_count'] or 0),
        'assignment_row_count': int(assignment_stats['assignment_row_count'] or 0),
        'assignment_store_count': int(assignment_stats['assignment_store_count'] or 0),
        'assignment_overlap_store_count': int(assignment_stats['assignment_overlap_store_count'] or 0),
        'missing_subject_target_count': int(subject_target_stats['missing_subject_target_count'] or 0),
    }


def _validate_config_stats(config_stats, report_date):
    if config_stats['distinct_store_count'] == 0:
        raise RuntimeError('当前 target_month 截至 report_date 无可纳入日报的有效门店配置，无法生成日报')

    if config_stats['store_attr_overlap_store_count'] > 0:
        raise RuntimeError(
            'dim_store_report_attr 在当前 report_date 下存在门店生效区间重叠，'
            '需要先清理后再运行日报 ETL'
        )

    if config_stats['missing_dim_store_count'] > 0:
        examples = config_stats['missing_dim_store_examples']
        example_text = f'；示例: {examples}' if examples else ''
        raise RuntimeError(
            'dim_store 缺少 target_month 截至 report_date 的有效门店维度记录，'
            f"共 {config_stats['missing_dim_store_count']} 条{example_text}。"
            '请先确认是否需要恢复 Oracle/C_STORE 有效门店，'
            '或同步清理 dim_store_report_attr / cfg_store_target_daily 对应门店配置后再运行日报 ETL'
        )

    if config_stats['assignment_overlap_store_count'] > 0:
        raise RuntimeError(
            'cfg_store_assessment_assignment 在当前 report_date 下存在门店归属重叠，'
            '需要先清理后再运行日报 ETL'
        )

    if config_stats['owner_history_start_date'] is None:
        raise RuntimeError(
            'dim_store_operation_owner_assignment 当前无任何历史切片，'
            '请先执行负责人快照导入后再运行日报 ETL'
        )

    if config_stats['owner_overlap_entity_count'] > 0:
        raise RuntimeError(
            'dim_store_operation_owner_assignment 在当前 report_date 下存在经营实体负责人生效区间重叠，'
            '需要先清理后再运行日报 ETL'
        )

    if (
        report_date >= config_stats['owner_history_start_date']
        and config_stats['owner_assignment_entity_count'] != config_stats['distinct_entity_count']
    ):
        raise RuntimeError(
            'dim_store_operation_owner_assignment 在当前 report_date 下缺少经营实体负责人切片，'
            '请先补跑负责人快照导入后再运行日报 ETL'
        )

    if config_stats['missing_subject_target_count'] > 0:
        raise RuntimeError(
            '当前 report_date 存在已配置共同考核归属但缺少主体目标的记录，'
            '无法安全生成最终日报'
        )


def _log_same_store_open_date_quality(config_stats, report_date, data_version):
    unusable_count = config_stats['unusable_open_date_count']
    message = (
        '同店开业日期数据质量: '
        f'report_date={report_date.isoformat()}, data_version={data_version}, '
        f"total_scope={config_stats['same_store_scope_count']}, unusable_count={unusable_count}"
    )
    if unusable_count > 0:
        examples = config_stats['unusable_open_date_examples'] or '-'
        logger.warning('%s, samples=%s；这些源门店本次不纳入同店辅助金额，且不回退销售额资格', message, examples)
    else:
        logger.info('%s', message)


def _fetch_output_stats(conn, report_date, data_version):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS output_row_count,
                SUM(CASE WHEN owner_name IS NULL THEN 1 ELSE 0 END) AS null_owner_name_count,
                SUM(CASE WHEN day_target = 0 THEN 1 ELSE 0 END) AS zero_day_target_count,
                SUM(CASE WHEN month_target = 0 THEN 1 ELSE 0 END) AS zero_month_target_count,
                SUM(CASE WHEN day_target > 0 AND day_ach_rate IS NULL THEN 1 ELSE 0 END) AS null_day_ach_rate_count,
                SUM(CASE WHEN month_target > 0 AND month_ach_rate IS NULL THEN 1 ELSE 0 END) AS null_month_ach_rate_count
            FROM ads_store_daily_report
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        output_stats = cursor.fetchone()

    return {
        'output_row_count': int(output_stats['output_row_count'] or 0),
        'null_owner_name_count': int(output_stats['null_owner_name_count'] or 0),
        'zero_day_target_count': int(output_stats['zero_day_target_count'] or 0),
        'zero_month_target_count': int(output_stats['zero_month_target_count'] or 0),
        'null_day_ach_rate_count': int(output_stats['null_day_ach_rate_count'] or 0),
        'null_month_ach_rate_count': int(output_stats['null_month_ach_rate_count'] or 0),
    }


def _validate_output_stats(config_stats, output_stats):
    if output_stats['output_row_count'] != config_stats['distinct_entity_count']:
        raise RuntimeError(
            'ads_store_daily_report 输出行数与最终经营实体数不一致，'
            f"期望 {config_stats['distinct_entity_count']} 行，实际 {output_stats['output_row_count']} 行"
        )

    if output_stats['null_day_ach_rate_count'] > 0:
        raise RuntimeError('存在 day_target > 0 但 day_ach_rate 为空的记录，请检查日目标与达成率计算逻辑')

    if output_stats['null_month_ach_rate_count'] > 0:
        raise RuntimeError('存在 month_target > 0 但 month_ach_rate 为空的记录，请检查月目标与达成率计算逻辑')


def conn_test():
    logger.info('开始执行门店日报连接与依赖检查')
    _validate_sql_skeleton()

    db_info = None
    with _connect() as conn:
        missing_tables = _fetch_required_table_state(conn)
        if missing_tables:
            raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")

        missing_columns = _fetch_required_column_state(conn)
        if missing_columns:
            raise RuntimeError(f"缺少依赖字段: {', '.join(missing_columns)}")

        with conn.cursor() as cursor:
            cursor.execute('SELECT DATABASE() AS db_name, VERSION() AS mysql_version')
            db_info = cursor.fetchone()

    if db_info is None:
        raise RuntimeError('MySQL 连接检查未返回数据库元信息')

    logger.info(
        '连接检查通过: database=%s, version=%s, sql_source=%s',
        db_info['db_name'],
        db_info['mysql_version'],
        SQL_SOURCE_LABEL,
    )
    return True


def _execute_sql_statements(conn, delete_sql, insert_sql):
    with conn.cursor() as cursor:
        # 当前环境下 multi-statement 执行链不稳定，拆成显式两段执行更可控。
        cursor.execute(delete_sql)
        cursor.execute(insert_sql)


def run(report_date=None, data_version='v1', max_retries=ETL_DEFAULT_MAX_RETRIES,
        retry_sleep=ETL_DEFAULT_RETRY_SLEEP):
    report_date = report_date or (date.today() - timedelta(days=1))
    started_at = datetime.now()

    logger.info('开始生成门店经营日报: report_date=%s, data_version=%s', report_date, data_version)

    for attempt in range(1, max_retries + 1):
        conn = None
        try:
            delete_sql, insert_sql = _build_sql_statements(report_date, data_version, started_at)

            conn = _connect()
            missing_tables = _fetch_required_table_state(conn)
            if missing_tables:
                raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")

            missing_columns = _fetch_required_column_state(conn)
            if missing_columns:
                raise RuntimeError(f"缺少依赖字段: {', '.join(missing_columns)}")

            config_stats = _fetch_config_stats(conn, report_date, data_version)
            _validate_config_stats(config_stats, report_date)
            _log_same_store_open_date_quality(config_stats, report_date, data_version)

            _execute_sql_statements(conn, delete_sql, insert_sql)
            output_stats = _fetch_output_stats(conn, report_date, data_version)
            _validate_output_stats(config_stats, output_stats)
            conn.commit()
            conn.close()
            conn = None

            duration = (datetime.now() - started_at).seconds
            logger.info(
                '门店经营日报生成完成: 输出实体=%s, 原始门店=%s, 已配置主体=%s, 当日目标记录=%s, MTD目标门店=%s, 空负责人=%s, 零日目标=%s, 零月目标=%s, 耗时=%s秒',
                output_stats['output_row_count'],
                config_stats['distinct_store_count'],
                config_stats['configured_subject_count'],
                config_stats['target_row_count'],
                config_stats['target_store_count'],
                output_stats['null_owner_name_count'],
                output_stats['zero_day_target_count'],
                output_stats['zero_month_target_count'],
                duration,
            )

            if config_stats['target_row_count'] != config_stats['target_store_count']:
                logger.warning(
                    '当日目标行数与 MTD 门店目标范围不一致: current_target_rows=%s, mtd_target_stores=%s。'
                    '月中闭店或退场门店会命中该场景；当前仅告警，不阻断运行。',
                    config_stats['target_row_count'],
                    config_stats['target_store_count'],
                )

            return {
                'report_date': report_date.isoformat(),
                'data_version': data_version,
                **config_stats,
                **output_stats,
                'duration_seconds': duration,
            }

        except Exception as exc:
            if conn is not None:
                conn.rollback()
                conn.close()

            if attempt >= max_retries or not _is_retryable_mysql_lock_error(exc):
                logger.error('门店经营日报生成失败: %s', exc)
                raise

            sleep_seconds = retry_sleep * attempt
            logger.warning(
                '门店经营日报遇到可重试锁冲突，第 %s/%s 次重试前等待 %s 秒: %s',
                attempt,
                max_retries,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


def _parse_args():
    parser = argparse.ArgumentParser(description='Generate ads_store_daily_report')
    parser.add_argument('--report-date', type=date.fromisoformat, default=None,
                        help='报告日期，格式 YYYY-MM-DD；默认昨天')
    parser.add_argument('--data-version', default='v1', help='目标版本号，默认 v1')
    parser.add_argument('--conn-test', action='store_true', help='只做连接与依赖检查，不写入数据')
    parser.add_argument('--max-retries', type=int, default=ETL_DEFAULT_MAX_RETRIES,
                        help='可重试锁冲突的最大重试次数')
    parser.add_argument('--retry-sleep', type=int, default=ETL_DEFAULT_RETRY_SLEEP,
                        help='基础重试等待秒数，实际等待=retry_sleep*attempt')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    if args.conn_test:
        conn_test()
    else:
        run(
            report_date=args.report_date,
            data_version=args.data_version,
            max_retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )