# -*- coding: utf-8 -*-
"""DWS 万店掌客流聚合 ETL。"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta

from ovopark_etl_common import connect_mysql_dict, ensure_tables, month_start, setup_logger, to_date_id


DAILY_REQUIRED_TABLES = (
    'dwd_ovopark_passenger_flow_daily',
    'dws_ovopark_passenger_flow_daily',
)

MONTHLY_REQUIRED_TABLES = (
    'dws_ovopark_passenger_flow_daily',
    'dws_ovopark_passenger_flow_monthly',
)

DAILY_UPSERT_SQL = """
INSERT INTO dws_ovopark_passenger_flow_daily (
    date_id,
    store_id,
    store_code,
    store_name,
    area_name,
    all_day_passenger_flow,
    business_time_passenger_flow,
    all_day_outside_passenger_flow,
    all_day_pass_passenger_flow,
    all_day_out_flow_count,
    all_day_dressing_passenger_flow,
    all_day_in_shop_rate,
    all_day_dressing_rate,
    covered_dep_count,
    source_dwd_row_count,
    source_min_requested_at,
    source_max_requested_at,
    load_batch_id,
    validation_status,
    validation_note,
    etl_time
)
SELECT
    d.date_id,
    d.store_id,
    MAX(d.store_code) AS store_code,
    MAX(d.store_name) AS store_name,
    MAX(d.area_name) AS area_name,
    SUM(CASE WHEN d.is_on_business_time = 0 THEN d.passenger_flow ELSE 0 END) AS all_day_passenger_flow,
    SUM(CASE WHEN d.is_on_business_time = 1 THEN d.passenger_flow ELSE 0 END) AS business_time_passenger_flow,
    SUM(CASE WHEN d.is_on_business_time = 0 THEN d.outside_passenger_flow ELSE 0 END) AS all_day_outside_passenger_flow,
    SUM(CASE WHEN d.is_on_business_time = 0 THEN d.pass_passenger_flow ELSE 0 END) AS all_day_pass_passenger_flow,
    SUM(CASE WHEN d.is_on_business_time = 0 THEN d.out_flow_count ELSE 0 END) AS all_day_out_flow_count,
    SUM(CASE WHEN d.is_on_business_time = 0 THEN d.dressing_passenger_flow ELSE 0 END) AS all_day_dressing_passenger_flow,
    CASE
        WHEN SUM(CASE WHEN d.is_on_business_time = 0 THEN d.outside_passenger_flow ELSE 0 END) = 0 THEN NULL
        ELSE ROUND(
            SUM(CASE WHEN d.is_on_business_time = 0 THEN d.passenger_flow ELSE 0 END)
            / SUM(CASE WHEN d.is_on_business_time = 0 THEN d.outside_passenger_flow ELSE 0 END),
            4
        )
    END AS all_day_in_shop_rate,
    CASE
        WHEN SUM(CASE WHEN d.is_on_business_time = 0 THEN d.passenger_flow ELSE 0 END) = 0 THEN NULL
        ELSE ROUND(
            SUM(CASE WHEN d.is_on_business_time = 0 THEN d.dressing_passenger_flow ELSE 0 END)
            / SUM(CASE WHEN d.is_on_business_time = 0 THEN d.passenger_flow ELSE 0 END),
            4
        )
    END AS all_day_dressing_rate,
    COUNT(DISTINCT d.ovopark_dep_id) AS covered_dep_count,
    COUNT(*) AS source_dwd_row_count,
    MIN(d.source_requested_at) AS source_min_requested_at,
    MAX(d.source_requested_at) AS source_max_requested_at,
    %s AS load_batch_id,
    'PENDING' AS validation_status,
    NULL AS validation_note,
    %s AS etl_time
FROM dwd_ovopark_passenger_flow_daily d
WHERE d.date_id BETWEEN %s AND %s
GROUP BY d.date_id, d.store_id
ON DUPLICATE KEY UPDATE
    store_code = VALUES(store_code),
    store_name = VALUES(store_name),
    area_name = VALUES(area_name),
    all_day_passenger_flow = VALUES(all_day_passenger_flow),
    business_time_passenger_flow = VALUES(business_time_passenger_flow),
    all_day_outside_passenger_flow = VALUES(all_day_outside_passenger_flow),
    all_day_pass_passenger_flow = VALUES(all_day_pass_passenger_flow),
    all_day_out_flow_count = VALUES(all_day_out_flow_count),
    all_day_dressing_passenger_flow = VALUES(all_day_dressing_passenger_flow),
    all_day_in_shop_rate = VALUES(all_day_in_shop_rate),
    all_day_dressing_rate = VALUES(all_day_dressing_rate),
    covered_dep_count = VALUES(covered_dep_count),
    source_dwd_row_count = VALUES(source_dwd_row_count),
    source_min_requested_at = VALUES(source_min_requested_at),
    source_max_requested_at = VALUES(source_max_requested_at),
    load_batch_id = VALUES(load_batch_id),
    validation_status = VALUES(validation_status),
    validation_note = VALUES(validation_note),
    etl_time = VALUES(etl_time)
""".strip()

MONTHLY_UPSERT_SQL = """
INSERT INTO dws_ovopark_passenger_flow_monthly (
    report_date,
    target_year,
    target_month,
    store_id,
    store_code,
    store_name,
    area_name,
    month_passenger_flow,
    month_business_time_passenger_flow,
    month_outside_passenger_flow,
    month_pass_passenger_flow,
    month_out_flow_count,
    month_dressing_passenger_flow,
    month_avg_daily_passenger_flow,
    month_in_shop_rate,
    month_dressing_rate,
    days_with_data,
    calendar_day_count,
    data_coverage_rate,
    source_dws_day_row_count,
    data_version,
    etl_time
)
SELECT
    %s AS report_date,
    YEAR(STR_TO_DATE(CAST(d.date_id AS CHAR), '%Y%m%d')) AS target_year,
    MONTH(STR_TO_DATE(CAST(d.date_id AS CHAR), '%Y%m%d')) AS target_month,
    d.store_id,
    MAX(d.store_code) AS store_code,
    MAX(d.store_name) AS store_name,
    MAX(d.area_name) AS area_name,
    SUM(d.all_day_passenger_flow) AS month_passenger_flow,
    SUM(d.business_time_passenger_flow) AS month_business_time_passenger_flow,
    SUM(d.all_day_outside_passenger_flow) AS month_outside_passenger_flow,
    SUM(d.all_day_pass_passenger_flow) AS month_pass_passenger_flow,
    SUM(d.all_day_out_flow_count) AS month_out_flow_count,
    SUM(d.all_day_dressing_passenger_flow) AS month_dressing_passenger_flow,
    CASE
        WHEN COUNT(DISTINCT d.date_id) = 0 THEN NULL
        ELSE ROUND(SUM(d.all_day_passenger_flow) / COUNT(DISTINCT d.date_id), 2)
    END AS month_avg_daily_passenger_flow,
    CASE
        WHEN SUM(d.all_day_outside_passenger_flow) = 0 THEN NULL
        ELSE ROUND(SUM(d.all_day_passenger_flow) / SUM(d.all_day_outside_passenger_flow), 4)
    END AS month_in_shop_rate,
    CASE
        WHEN SUM(d.all_day_passenger_flow) = 0 THEN NULL
        ELSE ROUND(SUM(d.all_day_dressing_passenger_flow) / SUM(d.all_day_passenger_flow), 4)
    END AS month_dressing_rate,
    COUNT(DISTINCT d.date_id) AS days_with_data,
    DAY(LAST_DAY(%s)) AS calendar_day_count,
    CASE
        WHEN DAY(LAST_DAY(%s)) = 0 THEN NULL
        ELSE ROUND(COUNT(DISTINCT d.date_id) / DAY(LAST_DAY(%s)), 4)
    END AS data_coverage_rate,
    COUNT(*) AS source_dws_day_row_count,
    %s AS data_version,
    %s AS etl_time
FROM dws_ovopark_passenger_flow_daily d
WHERE d.date_id BETWEEN %s AND %s
GROUP BY YEAR(STR_TO_DATE(CAST(d.date_id AS CHAR), '%Y%m%d')), MONTH(STR_TO_DATE(CAST(d.date_id AS CHAR), '%Y%m%d')), d.store_id
ON DUPLICATE KEY UPDATE
    store_code = VALUES(store_code),
    store_name = VALUES(store_name),
    area_name = VALUES(area_name),
    month_passenger_flow = VALUES(month_passenger_flow),
    month_business_time_passenger_flow = VALUES(month_business_time_passenger_flow),
    month_outside_passenger_flow = VALUES(month_outside_passenger_flow),
    month_pass_passenger_flow = VALUES(month_pass_passenger_flow),
    month_out_flow_count = VALUES(month_out_flow_count),
    month_dressing_passenger_flow = VALUES(month_dressing_passenger_flow),
    month_avg_daily_passenger_flow = VALUES(month_avg_daily_passenger_flow),
    month_in_shop_rate = VALUES(month_in_shop_rate),
    month_dressing_rate = VALUES(month_dressing_rate),
    days_with_data = VALUES(days_with_data),
    calendar_day_count = VALUES(calendar_day_count),
    data_coverage_rate = VALUES(data_coverage_rate),
    source_dws_day_row_count = VALUES(source_dws_day_row_count),
    etl_time = VALUES(etl_time)
""".strip()


logger = setup_logger(__name__)


def _daily_scope_stats(conn, start_date_id: int, end_date_id: int):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS source_row_count, COUNT(DISTINCT store_id) AS store_count
            FROM dwd_ovopark_passenger_flow_daily
            WHERE date_id BETWEEN %s AND %s
            """,
            (start_date_id, end_date_id),
        )
        stats = cursor.fetchone()
    return {
        'source_row_count': int(stats['source_row_count'] or 0),
        'store_count': int(stats['store_count'] or 0),
    }


def _monthly_scope_stats(conn, month_start_id: int, report_date_id: int, report_date: date, data_version: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS source_day_row_count, COUNT(DISTINCT store_id) AS store_count
            FROM dws_ovopark_passenger_flow_daily
            WHERE date_id BETWEEN %s AND %s
            """,
            (month_start_id, report_date_id),
        )
        source_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(*) AS target_row_count
            FROM dws_ovopark_passenger_flow_monthly
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        target_stats = cursor.fetchone()

    return {
        'source_day_row_count': int(source_stats['source_day_row_count'] or 0),
        'store_count': int(source_stats['store_count'] or 0),
        'target_row_count': int(target_stats['target_row_count'] or 0),
    }


def conn_test(stage: str, start_date: date, end_date: date, report_date: date, data_version: str):
    with connect_mysql_dict(timeout_profile='long_running', autocommit=True) as conn:
        if stage == 'daily':
            ensure_tables(conn, DAILY_REQUIRED_TABLES)
            stats = _daily_scope_stats(conn, to_date_id(start_date), to_date_id(end_date))
            logger.info('dws_ovopark_passenger_flow daily conn-test 通过：source_rows=%s, stores=%s', stats['source_row_count'], stats['store_count'])
        else:
            ensure_tables(conn, MONTHLY_REQUIRED_TABLES)
            stats = _monthly_scope_stats(conn, to_date_id(month_start(report_date)), to_date_id(report_date), report_date, data_version)
            logger.info('dws_ovopark_passenger_flow monthly conn-test 通过：source_day_rows=%s, stores=%s', stats['source_day_row_count'], stats['store_count'])


def run(stage: str, start_date: date, end_date: date, report_date: date, data_version: str, execute: bool = False):
    with connect_mysql_dict(timeout_profile='long_running', autocommit=False) as conn:
        if stage == 'daily':
            ensure_tables(conn, DAILY_REQUIRED_TABLES)
            start_date_id = to_date_id(start_date)
            end_date_id = to_date_id(end_date)
            stats = _daily_scope_stats(conn, start_date_id, end_date_id)
            if not execute:
                logger.info('dry-run：未执行 DWS 日聚合；date_range=%s~%s, source_rows=%s', start_date_id, end_date_id, stats['source_row_count'])
                return {'execute': False, 'stage': stage, **stats}

            batch_id = f"dws_ovopark_daily_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            etl_time = datetime.now()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(DAILY_UPSERT_SQL, (batch_id, etl_time, start_date_id, end_date_id))
                    affected_rows = cursor.rowcount
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            logger.info('dws_ovopark_passenger_flow daily 装载完成：affected_rows=%s, batch_id=%s', affected_rows, batch_id)
            return {'execute': True, 'stage': stage, 'affected_rows': affected_rows, 'batch_id': batch_id, **stats}

        ensure_tables(conn, MONTHLY_REQUIRED_TABLES)
        month_start_id = to_date_id(month_start(report_date))
        report_date_id = to_date_id(report_date)
        stats = _monthly_scope_stats(conn, month_start_id, report_date_id, report_date, data_version)
        if not execute:
            logger.info('dry-run：未执行 DWS 月聚合；report_date=%s, source_day_rows=%s', report_date, stats['source_day_row_count'])
            return {'execute': False, 'stage': stage, **stats}

        etl_time = datetime.now()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    MONTHLY_UPSERT_SQL,
                    (report_date, report_date, report_date, report_date, data_version, etl_time, month_start_id, report_date_id),
                )
                affected_rows = cursor.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        logger.info('dws_ovopark_passenger_flow monthly 装载完成：affected_rows=%s, report_date=%s', affected_rows, report_date)
        return {'execute': True, 'stage': stage, 'affected_rows': affected_rows, **stats}


def _parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description='Build DWS Ovopark passenger flow daily/monthly aggregations')
    parser.add_argument('--stage', choices=('daily', 'monthly'), required=True, help='聚合阶段：daily 或 monthly')
    parser.add_argument('--start-date', type=date.fromisoformat, default=yesterday, help='日聚合开始日期')
    parser.add_argument('--end-date', type=date.fromisoformat, default=yesterday, help='日聚合结束日期')
    parser.add_argument('--report-date', type=date.fromisoformat, default=yesterday, help='月聚合观察日')
    parser.add_argument('--data-version', default='v1', help='月聚合版本号，默认 v1')
    parser.add_argument('--conn-test', action='store_true', help='只做 MySQL 依赖检查与范围统计')
    parser.add_argument('--execute', action='store_true', help='执行聚合写入')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    if args.start_date > args.end_date:
        raise ValueError('--start-date 不能晚于 --end-date')
    if args.conn_test:
        conn_test(args.stage, args.start_date, args.end_date, args.report_date, args.data_version)
    else:
        run(args.stage, args.start_date, args.end_date, args.report_date, args.data_version, execute=args.execute)
