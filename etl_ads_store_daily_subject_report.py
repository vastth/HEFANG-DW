# -*- coding: utf-8 -*-
"""何方珠宝 - 门店经营日报统计主体层 ETL。"""

import argparse
import logging
import time
from datetime import date, datetime, timedelta

from pymysql.cursors import DictCursor

from config import ETL_DEFAULT_MAX_RETRIES, ETL_DEFAULT_RETRY_SLEEP
from db_connections import connect_mysql


SQL_SOURCE_LABEL = 'embedded:etl_ads_store_daily_subject_report.py'
DELETE_SQL_TEMPLATE = """
DELETE FROM ads_store_daily_subject_report
WHERE report_date = @report_date
  AND data_version = @data_version
""".strip()

INSERT_SQL_TEMPLATE = """
INSERT INTO ads_store_daily_subject_report (
    report_date,
    subject_code,
    subject_name,
    subject_source,
    assessment_mode,
    anchor_store_id,
    anchor_store_name,
    report_channel_type,
    member_store_count,
    day_sales_amt,
    day_sales_qty,
    day_order_cnt,
    day_attach_rate,
    day_avg_ticket,
    day_target,
    day_ach_rate,
    mtd_sales_amt,
    mtd_sales_qty,
    mtd_order_cnt,
    mtd_attach_rate,
    mtd_avg_ticket,
    month_target,
    month_ach_rate,
    last_month_mtd_sales_amt,
    last_month_mtd_sales_qty,
    last_year_mtd_sales_amt,
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
        DAY(@report_date) / DAY(LAST_DAY(@report_date)) AS time_progress,
        @data_version AS data_version,
        @etl_time AS etl_time
),

base_store AS (
    SELECT
        sr.report_date,
        sr.store_id,
        sr.store_code,
        sr.store_name,
        sr.report_channel_type,
        sr.day_sales_amt,
        sr.day_sales_qty,
        sr.day_order_cnt,
        sr.day_target,
        sr.mtd_sales_amt,
        sr.mtd_sales_qty,
        sr.mtd_order_cnt,
        sr.month_target,
        sr.last_month_mtd_sales_amt,
        sr.last_month_mtd_sales_qty,
        sr.last_year_mtd_sales_amt,
        sr.last_year_mtd_sales_qty,
        sr.time_progress
    FROM ads_store_daily_report sr
    CROSS JOIN params p
    WHERE sr.report_date = p.report_date
      AND sr.data_version = p.data_version
),

configured_subject_scope AS (
    SELECT
        sa.subject_code,
        MAX(
            COALESCE(
                sa.anchor_store_id,
                CASE
                    WHEN sa.assignment_role = '主店' THEN sa.store_id
                    ELSE NULL
                END
            )
        ) AS anchor_store_id,
        MAX(
            COALESCE(
                sa.anchor_store_name,
                CASE
                    WHEN sa.assignment_role = '主店' THEN sa.store_name
                    ELSE NULL
                END
            )
        ) AS anchor_store_name,
        COUNT(DISTINCT sa.store_id) AS member_store_count,
        MAX(CASE WHEN sa.is_joint_assessment = 'Y' THEN 1 ELSE 0 END) AS has_joint_assessment
    FROM cfg_store_assessment_assignment sa
    CROSS JOIN params p
    WHERE sa.target_month = p.target_month
      AND sa.target_version = p.data_version
      AND p.report_date BETWEEN sa.effective_start_date AND sa.effective_end_date
    GROUP BY sa.subject_code
),

subject_target AS (
    SELECT
        st.subject_code,
        st.subject_name,
        st.assessment_mode,
        st.day_target,
        st.month_target
    FROM cfg_store_assessment_subject_target_daily st
    CROSS JOIN params p
    WHERE st.target_date = p.report_date
      AND st.target_version = p.data_version
),

configured_subject AS (
    SELECT
        st.subject_code,
        st.subject_name,
        st.assessment_mode,
        LEFT(st.subject_code, 40) AS report_entity_code,
        css.anchor_store_id,
        css.anchor_store_name,
        css.member_store_count,
        css.has_joint_assessment
    FROM subject_target st
    INNER JOIN configured_subject_scope css
        ON st.subject_code = css.subject_code
),

resolved_subject AS (
    SELECT
        bs.report_date,
        bs.store_id,
        bs.store_code,
        bs.store_name,
        bs.report_channel_type,
        cs.subject_code,
        cs.subject_name,
        cs.assessment_mode AS configured_assessment_mode,
        cs.anchor_store_id,
        cs.anchor_store_name,
        cs.member_store_count,
        cs.has_joint_assessment,
        bs.day_sales_amt,
        bs.day_sales_qty,
        bs.day_order_cnt,
        bs.day_target,
        bs.mtd_sales_amt,
        bs.mtd_sales_qty,
        bs.mtd_order_cnt,
        bs.month_target,
        bs.last_month_mtd_sales_amt,
        bs.last_month_mtd_sales_qty,
        bs.last_year_mtd_sales_amt,
        bs.last_year_mtd_sales_qty,
        bs.time_progress
    FROM base_store bs
    LEFT JOIN configured_subject cs
        ON bs.store_code = cs.report_entity_code
       AND bs.store_name = cs.subject_name
)

SELECT
    rs.report_date,
    COALESCE(rs.subject_code, CONCAT('STORE_', rs.store_code)) AS subject_code,
    COALESCE(rs.subject_name, rs.store_name) AS subject_name,
    CASE
        WHEN rs.subject_code IS NULL THEN 'default_independent'
        ELSE 'configured_subject'
    END AS subject_source,
    COALESCE(
        rs.configured_assessment_mode,
        CASE
            WHEN COALESCE(rs.has_joint_assessment, 0) = 1 OR COALESCE(rs.member_store_count, 1) > 1 THEN '合并'
            ELSE '独立'
        END
    ) AS assessment_mode,
    COALESCE(rs.anchor_store_id, rs.store_id) AS anchor_store_id,
    COALESCE(rs.anchor_store_name, rs.store_name) AS anchor_store_name,
    rs.report_channel_type,
    COALESCE(rs.member_store_count, 1) AS member_store_count,
    rs.day_sales_amt,
    rs.day_sales_qty,
    rs.day_order_cnt,
    CASE
        WHEN rs.day_order_cnt = 0 THEN NULL
        ELSE ROUND(rs.day_sales_qty / rs.day_order_cnt, 4)
    END AS day_attach_rate,
    CASE
        WHEN rs.day_order_cnt = 0 THEN NULL
        ELSE ROUND(rs.day_sales_amt / rs.day_order_cnt, 2)
    END AS day_avg_ticket,
    rs.day_target,
    CASE
        WHEN rs.day_target = 0 THEN NULL
        ELSE ROUND(rs.day_sales_amt / rs.day_target, 4)
    END AS day_ach_rate,
    rs.mtd_sales_amt,
    rs.mtd_sales_qty,
    rs.mtd_order_cnt,
    CASE
        WHEN rs.mtd_order_cnt = 0 THEN NULL
        ELSE ROUND(rs.mtd_sales_qty / rs.mtd_order_cnt, 4)
    END AS mtd_attach_rate,
    CASE
        WHEN rs.mtd_order_cnt = 0 THEN NULL
        ELSE ROUND(rs.mtd_sales_amt / rs.mtd_order_cnt, 2)
    END AS mtd_avg_ticket,
    rs.month_target,
    CASE
        WHEN rs.month_target = 0 THEN NULL
        ELSE ROUND(rs.mtd_sales_amt / rs.month_target, 4)
    END AS month_ach_rate,
    rs.last_month_mtd_sales_amt,
    rs.last_month_mtd_sales_qty,
    rs.last_year_mtd_sales_amt,
    CASE
        WHEN rs.last_year_mtd_sales_amt = 0 THEN NULL
        ELSE ROUND((rs.mtd_sales_amt / rs.last_year_mtd_sales_amt) - 1, 4)
    END AS yoy_rate,
    rs.mtd_sales_amt - rs.last_year_mtd_sales_amt AS yoy_amt_diff,
    rs.last_year_mtd_sales_qty,
    CASE
        WHEN rs.last_year_mtd_sales_qty = 0 THEN NULL
        ELSE ROUND((rs.mtd_sales_qty / rs.last_year_mtd_sales_qty) - 1, 4)
    END AS yoy_qty_rate,
    rs.mtd_sales_qty - rs.last_year_mtd_sales_qty AS yoy_qty_diff,
    RANK() OVER (
        ORDER BY rs.day_sales_amt DESC,
        COALESCE(rs.subject_code, CONCAT('STORE_', rs.store_code))
    ) AS day_rank,
    RANK() OVER (
        ORDER BY rs.mtd_sales_amt DESC,
        COALESCE(rs.subject_code, CONCAT('STORE_', rs.store_code))
    ) AS mtd_rank,
    rs.time_progress,
    p.data_version,
    p.etl_time
FROM resolved_subject rs
CROSS JOIN params p
""".strip()

REQUIRED_TABLES = (
    'ads_store_daily_report',
    'cfg_store_assessment_subject_target_daily',
    'cfg_store_assessment_assignment',
    'ads_store_daily_subject_report',
)
TARGET_REQUIRED_COLUMNS = (
    'report_channel_type',
)
TARGET_SCHEMA_SQL_PATH = 'SQL/alter_ads_store_daily_subject_report_add_report_channel_type.sql'
REQUIRED_SQL_SNIPPETS = (
    'DELETE FROM ads_store_daily_subject_report',
    'INSERT INTO ads_store_daily_subject_report (',
    'FROM ads_store_daily_report sr',
    'COUNT(DISTINCT sa.store_id) AS member_store_count',
    'FROM cfg_store_assessment_subject_target_daily st',
    'LEFT(st.subject_code, 40) AS report_entity_code',
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
        existing_tables = {
            row.get('table_name_alias') or row.get('TABLE_NAME_ALIAS')
            for row in cursor.fetchall()
        }
    return [table_name for table_name in REQUIRED_TABLES if table_name not in existing_tables]


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


def _fetch_config_stats(conn, report_date, data_version):
    month_start = report_date.replace(day=1)
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS store_row_count,
                COUNT(DISTINCT store_id) AS distinct_store_count
            FROM ads_store_daily_report
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        store_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT
                COUNT(*) AS assignment_row_count,
                COUNT(DISTINCT store_id) AS assignment_store_count
            FROM cfg_store_assessment_assignment
            WHERE target_month = %s
              AND target_version = %s
              AND %s BETWEEN effective_start_date AND effective_end_date
            """,
            (month_start, data_version, report_date),
        )
        assignment_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(*) AS missing_subject_target_count
            FROM (
                SELECT DISTINCT sa.subject_code
                FROM cfg_store_assessment_assignment sa
                WHERE sa.target_month = %s
                  AND sa.target_version = %s
                  AND %s BETWEEN sa.effective_start_date AND sa.effective_end_date
            ) assigned
            LEFT JOIN cfg_store_assessment_subject_target_daily st
              ON st.subject_code = assigned.subject_code
             AND st.target_date = %s
             AND st.target_version = %s
            WHERE st.subject_code IS NULL
            """,
            (month_start, data_version, report_date, report_date, data_version),
        )
        subject_target_stats = cursor.fetchone()

    return {
        'store_row_count': int(store_stats['store_row_count'] or 0),
        'distinct_store_count': int(store_stats['distinct_store_count'] or 0),
        'assignment_row_count': int(assignment_stats['assignment_row_count'] or 0),
        'assignment_store_count': int(assignment_stats['assignment_store_count'] or 0),
        'missing_subject_target_count': int(subject_target_stats['missing_subject_target_count'] or 0),
    }


def _validate_config_stats(config_stats):
    if config_stats['distinct_store_count'] == 0:
        raise RuntimeError('ads_store_daily_report 在当前 report_date 下无可聚合门店数据，无法生成主体层日报')

    if config_stats['assignment_row_count'] != config_stats['assignment_store_count']:
        raise RuntimeError('cfg_store_assessment_assignment 在当前 report_date 下存在门店归属重叠，需先清理后再运行主体层 ETL')

    if config_stats['missing_subject_target_count'] > 0:
        raise RuntimeError('当前 report_date 存在已配置主体归属但缺少主体目标的记录，无法安全生成主体层日报')


def _fetch_output_stats(conn, report_date, data_version):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS output_row_count,
                COUNT(DISTINCT subject_code) AS distinct_subject_count,
                SUM(CASE WHEN report_channel_type IS NULL OR TRIM(report_channel_type) = '' THEN 1 ELSE 0 END) AS blank_report_channel_type_count,
                SUM(CASE WHEN day_target > 0 AND day_ach_rate IS NULL THEN 1 ELSE 0 END) AS null_day_ach_rate_count,
                SUM(CASE WHEN month_target > 0 AND month_ach_rate IS NULL THEN 1 ELSE 0 END) AS null_month_ach_rate_count
            FROM ads_store_daily_subject_report
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        output_stats = cursor.fetchone()

    return {
        'output_row_count': int(output_stats['output_row_count'] or 0),
        'distinct_subject_count': int(output_stats['distinct_subject_count'] or 0),
        'blank_report_channel_type_count': int(output_stats['blank_report_channel_type_count'] or 0),
        'null_day_ach_rate_count': int(output_stats['null_day_ach_rate_count'] or 0),
        'null_month_ach_rate_count': int(output_stats['null_month_ach_rate_count'] or 0),
    }


def _validate_output_stats(output_stats):
    if output_stats['output_row_count'] != output_stats['distinct_subject_count']:
        raise RuntimeError('ads_store_daily_subject_report 输出存在重复主体编码，请检查唯一键与聚合逻辑')

    if output_stats['blank_report_channel_type_count'] > 0:
        raise RuntimeError('ads_store_daily_subject_report 存在空 report_channel_type，请检查主体层渠道承接逻辑')

    if output_stats['null_day_ach_rate_count'] > 0:
        raise RuntimeError('存在 day_target > 0 但 day_ach_rate 为空的主体记录，请检查目标映射与达成率计算逻辑')

    if output_stats['null_month_ach_rate_count'] > 0:
        raise RuntimeError('存在 month_target > 0 但 month_ach_rate 为空的主体记录，请检查目标映射与达成率计算逻辑')


def conn_test():
    logger.info('开始执行门店日报统计主体层连接与依赖检查')
    _validate_sql_skeleton()
    with _connect() as conn:
        missing_tables = _fetch_required_table_state(conn)
        if missing_tables:
            raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")
        missing_target_columns = _fetch_missing_columns(conn, 'ads_store_daily_subject_report', TARGET_REQUIRED_COLUMNS)
        with conn.cursor() as cursor:
            cursor.execute('SELECT DATABASE() AS db_name, VERSION() AS mysql_version')
            db_info = cursor.fetchone() or {}

    logger.info(
        '连接检查通过: database=%s, version=%s, sql_source=%s',
        db_info.get('db_name'),
        db_info.get('mysql_version'),
        SQL_SOURCE_LABEL,
    )
    if missing_target_columns:
        logger.warning(
            '目标表 ads_store_daily_subject_report 缺少字段: %s。若现网已建旧版表，请先执行 %s 或重建目标表。',
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

    logger.info('开始生成门店经营日报统计主体层: report_date=%s, data_version=%s', report_date, data_version)

    for attempt in range(1, max_retries + 1):
        conn = None
        try:
            delete_sql, insert_sql = _build_sql_statements(report_date, data_version, started_at)

            conn = _connect()
            missing_tables = _fetch_required_table_state(conn)
            if missing_tables:
                raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")

            missing_target_columns = _fetch_missing_columns(conn, 'ads_store_daily_subject_report', TARGET_REQUIRED_COLUMNS)
            if missing_target_columns:
                raise RuntimeError(
                    'ads_store_daily_subject_report 目标表结构过旧，缺少字段: '
                    f"{', '.join(missing_target_columns)}；请先执行 {TARGET_SCHEMA_SQL_PATH} 或重建目标表"
                )

            config_stats = _fetch_config_stats(conn, report_date, data_version)
            _validate_config_stats(config_stats)

            _execute_sql_statements(conn, delete_sql, insert_sql)
            output_stats = _fetch_output_stats(conn, report_date, data_version)
            _validate_output_stats(output_stats)
            conn.commit()
            conn.close()
            conn = None

            duration = (datetime.now() - started_at).seconds
            logger.info(
                '门店经营日报统计主体层生成完成: 输出=%s, 基础门店=%s, 配置归属=%s, 耗时=%s秒',
                output_stats['output_row_count'],
                config_stats['distinct_store_count'],
                config_stats['assignment_store_count'],
                duration,
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
                logger.error('门店经营日报统计主体层生成失败: %s', exc)
                raise

            sleep_seconds = retry_sleep * attempt
            logger.warning(
                '门店经营日报统计主体层遇到可重试锁冲突，第 %s/%s 次重试前等待 %s 秒: %s',
                attempt,
                max_retries,
                sleep_seconds,
                exc,
            )
            time.sleep(sleep_seconds)


def _parse_args():
    parser = argparse.ArgumentParser(description='Generate ads_store_daily_subject_report')
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