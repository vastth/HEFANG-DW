# -*- coding: utf-8 -*-
"""
何方珠宝 - 销售看板月度战役日节奏 ETL

当前脚本用于产出 ads_daily_sales，作为销售看板“月度战役”主题的首轮样板表。

首版约束：
1. 当前保持独立入口，尚未接入 run_etl.py 主调度。
2. battle_month 固定为 report_date 所在自然月月初。
3. sales_date 只覆盖 battle_month 月初到 report_date，不预展开未来日期。
4. 只写实际、目标与累计物理字段，不下沉预测字段和诊断标签。
5. 日目标与销售额统一沿用门店日报经营体与商品范围口径。
"""

import argparse
import logging
import time
from datetime import date, datetime, timedelta

from pymysql.cursors import DictCursor

from config import ETL_DEFAULT_MAX_RETRIES, ETL_DEFAULT_RETRY_SLEEP
from db_connections import connect_mysql


SQL_SOURCE_LABEL = 'embedded:etl_ads_daily_sales.py'
LOCK_NAME = 'hefang_dw:ads_daily_sales'

DELETE_SQL_TEMPLATE = """
DELETE FROM ads_daily_sales
WHERE report_date = @report_date
  AND data_version = @data_version
""".strip()

INSERT_SQL_TEMPLATE = """
INSERT INTO ads_daily_sales (
    report_date,
    battle_month,
    sales_date,
    area_name,
    report_channel_type,
    day_target_amt,
    day_actual_amt,
    cum_target_amt,
    cum_actual_amt,
    last_year_cum_actual_amt,
    data_version,
    etl_time
)
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
        @data_version AS data_version,
        @etl_time AS etl_time
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
        WHERE 1 = 1
            AND t.target_version = p.data_version
            AND t.target_date BETWEEN p.battle_month AND p.report_date
),

joint_assessment_member_scope AS (
    SELECT DISTINCT
        sa.store_id
    FROM cfg_store_assessment_assignment sa
    CROSS JOIN params p
    WHERE sa.target_month = p.battle_month
      AND sa.target_version = p.data_version
      AND sa.subject_code IS NOT NULL
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.battle_month
),

joint_assessment_anchor_scope AS (
    SELECT DISTINCT
        sa.anchor_store_id AS store_id
    FROM cfg_store_assessment_assignment sa
    CROSS JOIN params p
    WHERE sa.target_month = p.battle_month
      AND sa.target_version = p.data_version
      AND sa.subject_code IS NOT NULL
      AND sa.anchor_store_id IS NOT NULL
      AND sa.effective_start_date <= p.report_date
      AND sa.effective_end_date >= p.battle_month
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
    INNER JOIN store_attr_scope sas
        ON sra.store_id = sas.store_id
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

source_store_scope AS (
    SELECT store_id FROM target_store_scope
    UNION
    SELECT store_id FROM joint_assessment_member_scope
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
        sss.store_id,
        COALESCE(
            CASE
                WHEN ass.subject_code IS NOT NULL THEN COALESCE(ass.anchor_store_id, sss.store_id)
                ELSE NULL
            END,
            sss.store_id
        ) AS report_entity_id,
        COALESCE(anchor_ss.area_name, ss.area_name) AS area_name,
        COALESCE(anchor_ss.report_channel_type, ss.report_channel_type) AS report_channel_type,
        ass.subject_code
    FROM source_store_scope sss
    LEFT JOIN assignment_scope ass
        ON sss.store_id = ass.store_id
    LEFT JOIN store_scope ss
        ON sss.store_id = ss.store_id
    LEFT JOIN store_scope anchor_ss
        ON COALESCE(ass.anchor_store_id, sss.store_id) = anchor_ss.store_id
),

group_scope AS (
    SELECT
        DISTINCT sem.area_name,
        sem.report_channel_type
    FROM store_entity_map sem
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
        edb.sales_date AS sales_date,
        edb.report_entity_id,
        edb.area_name,
        edb.report_channel_type,
        COALESCE(MAX(std.day_target_amt), SUM(tdsd.day_target_amt), 0) AS day_target_amt
    FROM entity_day_base edb
    LEFT JOIN target_daily_by_store_date tdsd
        ON edb.store_id = tdsd.store_id
       AND edb.sales_date = tdsd.sales_date
    LEFT JOIN subject_target_by_date std
        ON edb.subject_code = std.subject_code
       AND edb.sales_date = std.sales_date
    GROUP BY edb.sales_date, edb.report_entity_id, edb.area_name, edb.report_channel_type
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
        p.data_version,
        p.etl_time
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
        gdb.data_version,
        gdb.etl_time
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
        gdb.data_version,
        gdb.etl_time
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
        data_version,
        etl_time
    FROM group_day_metrics
),

org_detail AS (
    SELECT
        report_date,
        battle_month,
        sales_date,
        area_name,
        report_channel_type,
        MAX(day_target_amt) AS day_target_amt,
        MAX(day_actual_amt) AS day_actual_amt,
        MAX(cum_target_amt) AS cum_target_amt,
        MAX(cum_actual_amt) AS cum_actual_amt,
        MAX(last_year_cum_actual_amt) AS last_year_cum_actual_amt,
        MAX(data_version) AS data_version,
        MAX(etl_time) AS etl_time
    FROM group_day_cumulative
    GROUP BY report_date, battle_month, sales_date, area_name, report_channel_type
),

assembled AS (
    SELECT * FROM org_detail
)

SELECT
    report_date,
    battle_month,
    sales_date,
    area_name,
    report_channel_type,
    day_target_amt,
    day_actual_amt,
    cum_target_amt,
    cum_actual_amt,
    last_year_cum_actual_amt,
    data_version,
    etl_time
FROM assembled
""".strip()

SOURCE_TABLES = (
    'ods_m_retail',
    'ods_m_retailitem',
    'dim_store',
    'dim_store_report_attr',
    'dim_product',
    'cfg_store_target_daily',
    'cfg_store_assessment_subject_target_daily',
    'cfg_store_assessment_assignment',
)
TARGET_TABLE = 'ads_daily_sales'
TARGET_REQUIRED_COLUMNS = (
    'report_channel_type',
)
TARGET_SCHEMA_SQL_PATH = 'SQL/alter_ads_daily_sales_replace_report_channel_type_group.sql'
REQUIRED_SQL_SNIPPETS = (
    'DELETE FROM ads_daily_sales',
    'INSERT INTO ads_daily_sales (',
    'WITH RECURSIVE',
    'target_store_scope AS (',
    'joint_assessment_member_scope AS (',
    'joint_assessment_anchor_scope AS (',
    'store_attr_scope AS (',
    'source_store_scope AS (',
    "sra.is_include_in_daily_report = 'Y'",
    'SELECT 147 AS category_id',
    'WHERE dp.category_id IS NOT NULL',
    'AND t.target_date BETWEEN p.battle_month AND p.report_date',
    'AND t.target_version = p.data_version',
    'sa.target_version = p.data_version',
    'edb.sales_date AS sales_date',
    'COALESCE(MAX(std.day_target_amt), SUM(tdsd.day_target_amt), 0) AS day_target_amt',
    'SUM(day_target_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS cum_target_amt',
    'SUM(day_actual_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS cum_actual_amt',
    'SUM(last_year_day_actual_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS last_year_cum_actual_amt',
)
RETRYABLE_MYSQL_LOCK_KEYWORDS = (
    '1213',
    '1205',
    'deadlock found',
    'lock wait timeout exceeded',
    '未能获取命名锁',
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
        cursorclass=DictCursor,
        autocommit=False,
    )


def _escape_sql_string(value):
    return value.replace("'", "''")


def _validate_sql_skeleton():
    sql_text = '\n'.join((DELETE_SQL_TEMPLATE, INSERT_SQL_TEMPLATE))
    missing_snippets = [snippet for snippet in REQUIRED_SQL_SNIPPETS if snippet not in sql_text]
    if missing_snippets:
        raise RuntimeError(f"SQL 骨架缺少关键片段: {'; '.join(missing_snippets)}")


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


def _fetch_required_table_state(conn, required_tables):
    placeholders = ', '.join(['%s'] * len(required_tables))
    sql = f"""
        SELECT table_name AS table_name_alias
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name IN ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, required_tables)
        existing_tables = {
            (row.get('table_name_alias') or row.get('TABLE_NAME_ALIAS'))
            for row in cursor.fetchall()
        }

    return [table_name for table_name in required_tables if table_name not in existing_tables]


def _fetch_missing_columns(conn, table_name, required_columns):
    placeholders = ', '.join(['%s'] * len(required_columns))
    sql = f"""
        SELECT column_name AS column_name_alias
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = %s
          AND column_name IN ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (table_name, *required_columns))
        existing_columns = {
            (row.get('column_name_alias') or row.get('COLUMN_NAME_ALIAS'))
            for row in cursor.fetchall()
        }

    return [column_name for column_name in required_columns if column_name not in existing_columns]


def _fetch_scope_stats(conn, report_date, data_version):
    battle_month = report_date.replace(day=1)
    sales_day_count = (report_date - battle_month).days + 1
    with conn.cursor() as cursor:
        cursor.execute(
            """
            WITH target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                WHERE 1 = 1
                  AND t.target_version = %s
                  AND t.target_date BETWEEN %s AND %s
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            joint_assessment_anchor_scope AS (
                SELECT DISTINCT
                    sa.anchor_store_id AS store_id
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.subject_code IS NOT NULL
                  AND sa.anchor_store_id IS NOT NULL
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            source_store_scope AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
            ),
            store_attr_scope AS (
                SELECT store_id FROM source_store_scope
                UNION
                SELECT store_id FROM joint_assessment_anchor_scope
            ),
            store_attr_candidates AS (
                SELECT
                    sra.store_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY sra.store_id
                        ORDER BY
                            CASE
                                WHEN %s BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0
                                ELSE 1
                            END,
                            sra.effective_end_date DESC,
                            sra.effective_start_date DESC
                    ) AS attr_recency_rank
                FROM dim_store_report_attr sra
                INNER JOIN store_attr_scope sas
                    ON sra.store_id = sas.store_id
                WHERE sra.is_include_in_daily_report = 'Y'
                  AND sra.effective_start_date <= %s
                  AND sra.effective_end_date >= %s
            )
            SELECT
                COUNT(*) AS store_attr_row_count,
                (SELECT COUNT(*) FROM source_store_scope) AS distinct_store_count,
                (
                    SELECT COUNT(*)
                    FROM (
                        SELECT sra.store_id
                        FROM dim_store_report_attr sra
                        INNER JOIN store_attr_scope sas
                            ON sra.store_id = sas.store_id
                        WHERE sra.is_include_in_daily_report = 'Y'
                          AND %s BETWEEN sra.effective_start_date AND sra.effective_end_date
                        GROUP BY sra.store_id
                        HAVING COUNT(*) > 1
                    ) overlap_scope
                ) AS store_attr_overlap_store_count
            FROM store_attr_candidates sac
            WHERE sac.attr_recency_rank = 1
            """,
            (
                data_version,
                battle_month,
                report_date,
                battle_month,
                data_version,
                report_date,
                battle_month,
                battle_month,
                data_version,
                report_date,
                battle_month,
                report_date,
                report_date,
                battle_month,
                report_date,
            ),
        )
        store_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                WHERE 1 = 1
                  AND t.target_version = %s
                  AND t.target_date BETWEEN %s AND %s
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            joint_assessment_anchor_scope AS (
                SELECT DISTINCT
                    sa.anchor_store_id AS store_id
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.subject_code IS NOT NULL
                  AND sa.anchor_store_id IS NOT NULL
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            source_store_scope AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
            ),
            store_attr_scope AS (
                SELECT store_id FROM source_store_scope
                UNION
                SELECT store_id FROM joint_assessment_anchor_scope
            ),
            store_attr_candidates AS (
                SELECT
                    sra.store_id,
                    sra.report_channel_type,
                    ROW_NUMBER() OVER (
                        PARTITION BY sra.store_id
                        ORDER BY
                            CASE
                                WHEN %s BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0
                                ELSE 1
                            END,
                            sra.effective_end_date DESC,
                            sra.effective_start_date DESC
                    ) AS attr_recency_rank
                FROM dim_store_report_attr sra
                INNER JOIN store_attr_scope sas
                    ON sra.store_id = sas.store_id
                WHERE sra.is_include_in_daily_report = 'Y'
                  AND sra.effective_start_date <= %s
                  AND sra.effective_end_date >= %s
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
                                WHEN %s BETWEEN sa.effective_start_date AND sa.effective_end_date THEN 0
                                ELSE 1
                            END,
                            sa.effective_end_date DESC,
                            sa.effective_start_date DESC
                    ) AS assignment_recency_rank
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            assignment_scope AS (
                SELECT
                    ac.store_id,
                    ac.subject_code,
                    ac.anchor_store_id
                FROM assignment_candidates ac
                WHERE ac.assignment_recency_rank = 1
            ),
            store_entity_map AS (
                SELECT
                    sss.store_id,
                    COALESCE(anchor_ss.area_name, ss.area_name) AS area_name,
                    COALESCE(anchor_ss.report_channel_type, ss.report_channel_type) AS report_channel_type
                FROM source_store_scope sss
                LEFT JOIN assignment_scope ass
                    ON sss.store_id = ass.store_id
                LEFT JOIN store_scope ss
                    ON sss.store_id = ss.store_id
                LEFT JOIN store_scope anchor_ss
                    ON COALESCE(ass.anchor_store_id, sss.store_id) = anchor_ss.store_id
            )
            SELECT
                COUNT(DISTINCT CONCAT(
                    sem.area_name,
                    '||',
                    sem.report_channel_type
                )) AS detail_group_count
            FROM store_entity_map sem
            """,
            (
                data_version,
                battle_month,
                report_date,
                battle_month,
                data_version,
                report_date,
                battle_month,
                battle_month,
                data_version,
                report_date,
                battle_month,
                report_date,
                report_date,
                battle_month,
                report_date,
                battle_month,
                data_version,
                report_date,
                battle_month,
            ),
        )
        group_stats = cursor.fetchone()

        cursor.execute(
            """
            WITH target_store_scope AS (
                SELECT DISTINCT
                    t.store_id
                FROM cfg_store_target_daily t
                WHERE 1 = 1
                  AND t.target_version = %s
                  AND t.target_date BETWEEN %s AND %s
            ),
            joint_assessment_member_scope AS (
                SELECT DISTINCT
                    sa.store_id
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.subject_code IS NOT NULL
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            joint_assessment_anchor_scope AS (
                SELECT DISTINCT
                    sa.anchor_store_id AS store_id
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND sa.subject_code IS NOT NULL
                  AND sa.anchor_store_id IS NOT NULL
                  AND sa.effective_start_date <= %s
                  AND sa.effective_end_date >= %s
            ),
            source_store_scope AS (
                SELECT store_id FROM target_store_scope
                UNION
                SELECT store_id FROM joint_assessment_member_scope
            ),
            store_attr_scope AS (
                SELECT store_id FROM source_store_scope
                UNION
                SELECT store_id FROM joint_assessment_anchor_scope
            ),
            store_attr_candidates AS (
                SELECT
                    sra.store_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY sra.store_id
                        ORDER BY
                            CASE
                                WHEN %s BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0
                                ELSE 1
                            END,
                            sra.effective_end_date DESC,
                            sra.effective_start_date DESC
                    ) AS attr_recency_rank
                FROM dim_store_report_attr sra
                INNER JOIN store_attr_scope sas
                    ON sra.store_id = sas.store_id
                WHERE sra.is_include_in_daily_report = 'Y'
                  AND sra.effective_start_date <= %s
                  AND sra.effective_end_date >= %s
            )
            SELECT
                COUNT(*) AS missing_dim_store_count
            FROM store_attr_candidates sac
            LEFT JOIN dim_store s
              ON sac.store_id = s.store_id
            WHERE sac.attr_recency_rank = 1
              AND s.store_id IS NULL
            """,
            (
                data_version,
                battle_month,
                report_date,
                battle_month,
                data_version,
                report_date,
                battle_month,
                battle_month,
                data_version,
                report_date,
                battle_month,
                report_date,
                report_date,
                battle_month,
            ),
        )
        missing_dim_store_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                COUNT(*) AS target_row_count,
                COUNT(DISTINCT CONCAT(CAST(t.store_id AS CHAR), '||', DATE_FORMAT(t.target_date, '%%Y-%%m-%%d'))) AS distinct_target_day_count,
                COUNT(DISTINCT t.store_id) AS distinct_target_store_count
            FROM cfg_store_target_daily t
            INNER JOIN (
                SELECT DISTINCT
                    scoped_target.store_id
                FROM cfg_store_target_daily scoped_target
                                WHERE 1 = 1
                                    AND scoped_target.target_version = %s
                                    AND scoped_target.target_date BETWEEN %s AND %s
            ) current_scope
              ON t.store_id = current_scope.store_id
            WHERE t.target_version = %s
              AND t.target_date BETWEEN %s AND %s
            """,
                        (data_version, battle_month, report_date, data_version, battle_month, report_date),
        )
        target_stats = cursor.fetchone()

    expected_rows_per_day = int(group_stats['detail_group_count'] or 0)

    return {
        'battle_month': battle_month,
        'sales_day_count': sales_day_count,
        'store_attr_row_count': int(store_stats['store_attr_row_count'] or 0),
        'distinct_store_count': int(store_stats['distinct_store_count'] or 0),
        'store_attr_overlap_store_count': int(store_stats['store_attr_overlap_store_count'] or 0),
        'missing_dim_store_count': int(missing_dim_store_stats['missing_dim_store_count'] or 0),
        'detail_group_count': int(group_stats['detail_group_count'] or 0),
        'expected_rows_per_day': expected_rows_per_day,
        'target_row_count': int(target_stats['target_row_count'] or 0),
        'distinct_target_day_count': int(target_stats['distinct_target_day_count'] or 0),
        'distinct_target_store_count': int(target_stats['distinct_target_store_count'] or 0),
    }


def _validate_scope_stats(scope_stats):
    if scope_stats['distinct_store_count'] == 0:
        raise RuntimeError('dim_store_report_attr 在当前 report_date 下无有效门店配置，无法生成 ads_daily_sales')

    if scope_stats['store_attr_overlap_store_count'] > 0:
        raise RuntimeError(
            'dim_store_report_attr 在当前 report_date 下存在门店生效区间重叠，'
            '需要先清理后再运行 ads_daily_sales'
        )

    if scope_stats['missing_dim_store_count'] > 0:
        raise RuntimeError('dim_store_report_attr 存在未命中 dim_store 的有效 store_id，无法安全生成月战役表')

    if scope_stats['target_row_count'] != scope_stats['distinct_target_day_count']:
        raise RuntimeError(
            'cfg_store_target_daily 在当前 battle_month 范围内存在同店同日重复目标记录，'
            '需要先清理重复目标'
        )


def _fetch_output_stats(conn, report_date, data_version, expected_rows_per_day):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS output_row_count,
                COUNT(DISTINCT sales_date) AS output_sales_day_count,
                MIN(sales_date) AS min_sales_date,
                MAX(sales_date) AS max_sales_date,
                COUNT(*) - COUNT(DISTINCT CONCAT(
                    DATE_FORMAT(report_date, '%%Y-%%m-%%d'),
                    '||',
                    data_version,
                    '||',
                    DATE_FORMAT(battle_month, '%%Y-%%m-%%d'),
                    '||',
                    DATE_FORMAT(sales_date, '%%Y-%%m-%%d'),
                    '||',
                    area_name,
                    '||',
                                        report_channel_type
                )) AS duplicate_key_count
            FROM ads_daily_sales
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        output_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT COALESCE(SUM(CASE WHEN day_row_count <> %s THEN 1 ELSE 0 END), 0) AS abnormal_sales_date_count
            FROM (
                SELECT sales_date, COUNT(*) AS day_row_count
                FROM ads_daily_sales
                WHERE report_date = %s
                  AND data_version = %s
                GROUP BY sales_date
            ) daily_rows
            """,
            (expected_rows_per_day, report_date, data_version),
        )
        day_stats = cursor.fetchone()

    return {
        'output_row_count': int(output_stats['output_row_count'] or 0),
        'output_sales_day_count': int(output_stats['output_sales_day_count'] or 0),
        'min_sales_date': output_stats['min_sales_date'],
        'max_sales_date': output_stats['max_sales_date'],
        'duplicate_key_count': int(output_stats['duplicate_key_count'] or 0),
        'abnormal_sales_date_count': int(day_stats['abnormal_sales_date_count'] or 0),
    }


def _validate_output_stats(scope_stats, output_stats, report_date):
    expected_total_rows = scope_stats['expected_rows_per_day'] * scope_stats['sales_day_count']

    if output_stats['output_row_count'] != expected_total_rows:
        raise RuntimeError(
            'ads_daily_sales 输出行数异常，'
            f"期望 {expected_total_rows} 行，实际 {output_stats['output_row_count']} 行"
        )

    if output_stats['output_sales_day_count'] != scope_stats['sales_day_count']:
        raise RuntimeError(
            'ads_daily_sales 输出的 sales_date 天数异常，'
            f"期望 {scope_stats['sales_day_count']} 天，实际 {output_stats['output_sales_day_count']} 天"
        )

    if output_stats['min_sales_date'] != scope_stats['battle_month']:
        raise RuntimeError(
            'ads_daily_sales 最早 sales_date 异常，'
            f"期望 {scope_stats['battle_month']}，实际 {output_stats['min_sales_date']}"
        )

    if output_stats['max_sales_date'] != report_date:
        raise RuntimeError(
            'ads_daily_sales 最晚 sales_date 异常，'
            f"期望 {report_date}，实际 {output_stats['max_sales_date']}"
        )

    if output_stats['abnormal_sales_date_count'] > 0:
        raise RuntimeError('ads_daily_sales 存在 sales_date 粒度缺行或多行，请检查组织汇总 SQL')

    if output_stats['duplicate_key_count'] > 0:
        raise RuntimeError('ads_daily_sales 出现重复唯一键行，请检查组织汇总 SQL')


def _acquire_lock(conn):
    with conn.cursor() as cursor:
        cursor.execute('SELECT GET_LOCK(%s, %s) AS got_lock', (LOCK_NAME, 30))
        row = cursor.fetchone()
    got_lock = row.get('got_lock') if row else None
    if got_lock != 1:
        raise TimeoutError(f'未能获取命名锁: {LOCK_NAME}')


def _release_lock(conn):
    with conn.cursor() as cursor:
        cursor.execute('SELECT RELEASE_LOCK(%s) AS released_lock', (LOCK_NAME,))


def conn_test():
    logger.info('开始执行 ads_daily_sales 连接与依赖检查')
    _validate_sql_skeleton()

    with _connect() as conn:
        missing_tables = _fetch_required_table_state(conn, SOURCE_TABLES)
        if missing_tables:
            raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")

        missing_target_tables = _fetch_required_table_state(conn, (TARGET_TABLE,))
        missing_target_columns = []
        if not missing_target_tables:
            missing_target_columns = _fetch_missing_columns(conn, TARGET_TABLE, TARGET_REQUIRED_COLUMNS)

        with conn.cursor() as cursor:
            cursor.execute('SELECT DATABASE() AS db_name, VERSION() AS mysql_version')
            db_info = cursor.fetchone()

    logger.info(
        '连接检查通过: database=%s, version=%s, sql_source=%s',
        db_info['db_name'],
        db_info['mysql_version'],
        SQL_SOURCE_LABEL,
    )
    if missing_target_tables:
        logger.warning(
            '目标表尚未建表: %s。当前 conn-test 仅验证源依赖；正式 run 前请先执行 SQL/create_ads_daily_sales.sql。',
            ', '.join(missing_target_tables),
        )
    elif missing_target_columns:
        logger.warning(
            '目标表 ads_daily_sales 缺少字段: %s。若现网已建旧版表，请先执行 %s 或重建目标表。',
            ', '.join(missing_target_columns),
            TARGET_SCHEMA_SQL_PATH,
        )
    return True


def _execute_sql_statements(conn, delete_sql, insert_sql):
    with conn.cursor() as cursor:
        cursor.execute(delete_sql)
        cursor.execute(insert_sql)


def run(report_date=None, data_version='v1', max_retries=ETL_DEFAULT_MAX_RETRIES,
        retry_sleep=ETL_DEFAULT_RETRY_SLEEP):
    report_date = report_date or (date.today() - timedelta(days=1))
    started_at = datetime.now()

    logger.info('开始生成 ads_daily_sales: report_date=%s, data_version=%s', report_date, data_version)

    for attempt in range(1, max_retries + 1):
        conn = None
        lock_acquired = False
        try:
            delete_sql, insert_sql = _build_sql_statements(report_date, data_version, started_at)

            conn = _connect()
            missing_tables = _fetch_required_table_state(conn, SOURCE_TABLES + (TARGET_TABLE,))
            if missing_tables:
                raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")

            missing_target_columns = _fetch_missing_columns(conn, TARGET_TABLE, TARGET_REQUIRED_COLUMNS)
            if missing_target_columns:
                raise RuntimeError(
                    'ads_daily_sales 目标表结构过旧，缺少字段: '
                    f"{', '.join(missing_target_columns)}；请先执行 {TARGET_SCHEMA_SQL_PATH} 或重建目标表"
                )

            scope_stats = _fetch_scope_stats(conn, report_date, data_version)
            _validate_scope_stats(scope_stats)

            _acquire_lock(conn)
            lock_acquired = True
            try:
                _execute_sql_statements(conn, delete_sql, insert_sql)
                output_stats = _fetch_output_stats(
                    conn,
                    report_date,
                    data_version,
                    scope_stats['expected_rows_per_day'],
                )
                _validate_output_stats(scope_stats, output_stats, report_date)
                conn.commit()
            finally:
                if lock_acquired:
                    try:
                        _release_lock(conn)
                    except Exception as lock_exc:
                        logger.warning('释放 ads_daily_sales 命名锁失败: %s', lock_exc)
            conn.close()
            conn = None

            duration = int((datetime.now() - started_at).total_seconds())
            logger.info(
                'ads_daily_sales 生成完成: battle_month=%s, 天数=%s, 生效源门店=%s, 明细组合=%s, 覆盖目标门店=%s, 输出行数=%s, 耗时=%s秒',
                scope_stats['battle_month'],
                scope_stats['sales_day_count'],
                scope_stats['distinct_store_count'],
                scope_stats['detail_group_count'],
                scope_stats['distinct_target_store_count'],
                output_stats['output_row_count'],
                duration,
            )

            if scope_stats['target_row_count'] == 0:
                logger.warning('当前 battle_month 范围未命中任何日目标配置，本次 day_target/cum_target 将全部为 0。')

            return {
                'report_date': report_date.isoformat(),
                'data_version': data_version,
                'battle_month': scope_stats['battle_month'].isoformat(),
                **{k: v for k, v in scope_stats.items() if k != 'battle_month'},
                **output_stats,
                'duration_seconds': duration,
            }

        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                finally:
                    if lock_acquired:
                        try:
                            _release_lock(conn)
                        except Exception as lock_exc:
                            logger.warning('释放 ads_daily_sales 命名锁失败: %s', lock_exc)
                    conn.close()

            if attempt >= max_retries or not _is_retryable_mysql_lock_error(exc):
                logger.error('ads_daily_sales 生成失败: %s', exc)
                raise

            sleep_seconds = retry_sleep * attempt
            logger.warning(
                'ads_daily_sales 遇到可重试锁冲突，第 %s/%s 次重试前等待 %s 秒: %s',
                attempt,
                max_retries,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


def _parse_args():
    parser = argparse.ArgumentParser(description='Generate ads_daily_sales')
    parser.add_argument('--report-date', type=date.fromisoformat, default=None,
                        help='报告日期，格式 YYYY-MM-DD；默认昨天')
    parser.add_argument('--data-version', default='v1', help='数据版本号，默认 v1')
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