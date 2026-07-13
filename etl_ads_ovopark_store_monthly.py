# -*- coding: utf-8 -*-
"""ADS 万店掌门店月客流宽表 ETL。"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from ovopark_etl_common import connect_mysql_dict, ensure_tables, setup_logger


REQUIRED_TABLES = (
    'dws_ovopark_passenger_flow_monthly',
    'ads_ovopark_store_monthly',
    'dim_store',
    'dim_store_report_attr',
    'dim_store_operation_owner_assignment',
    'dim_ovopark_shop_mapping',
)

DELETE_SQL = """
DELETE FROM ads_ovopark_store_monthly
WHERE report_date = %s
  AND data_version = %s
""".strip()

INSERT_SQL = """
INSERT INTO ads_ovopark_store_monthly (
    report_date,
    target_year,
    target_month,
    store_id,
    store_code,
    store_name,
    owner_name,
    area_name,
    report_channel_type,
    store_grade,
    is_duty_free,
    ovopark_dep_id,
    ovopark_dep_key,
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
    data_version,
    etl_time
)
SELECT
    m.report_date,
    m.target_year,
    m.target_month,
    m.store_id,
    m.store_code,
    m.store_name,
    owner_scope.owner_name,
    COALESCE(ds.area_name, m.area_name) AS area_name,
    sra.report_channel_type,
    sra.store_grade,
    sra.is_duty_free,
    mapping_scope.ovopark_dep_id,
    mapping_scope.ovopark_dep_key,
    m.month_passenger_flow,
    m.month_business_time_passenger_flow,
    m.month_outside_passenger_flow,
    m.month_pass_passenger_flow,
    m.month_out_flow_count,
    m.month_dressing_passenger_flow,
    m.month_avg_daily_passenger_flow,
    m.month_in_shop_rate,
    m.month_dressing_rate,
    m.days_with_data,
    m.calendar_day_count,
    m.data_coverage_rate,
    m.data_version,
    NOW() AS etl_time
FROM dws_ovopark_passenger_flow_monthly m
LEFT JOIN dim_store ds
    ON m.store_id = ds.store_id
LEFT JOIN dim_store_report_attr sra
    ON m.store_id = sra.store_id
   AND %s BETWEEN sra.effective_start_date AND sra.effective_end_date
LEFT JOIN dim_store_operation_owner_assignment owner_scope
    ON owner_scope.entity_type = 'STORE'
   AND owner_scope.entity_code = m.store_code
   AND %s BETWEEN owner_scope.effective_start_date AND owner_scope.effective_end_date
LEFT JOIN dim_ovopark_shop_mapping mapping_scope
    ON mapping_scope.hefang_store_id = m.store_id
   AND mapping_scope.is_current = 'Y'
   AND mapping_scope.mapping_status = 'MATCHED'
   AND %s BETWEEN mapping_scope.effective_start_date AND mapping_scope.effective_end_date
WHERE m.report_date = %s
  AND m.data_version = %s
""".strip()


logger = setup_logger(__name__)


def _fetch_scope_stats(conn, report_date: date, data_version: str):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*) AS source_row_count, COUNT(DISTINCT store_id) AS source_store_count
            FROM dws_ovopark_passenger_flow_monthly
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        source_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(*) AS target_row_count
            FROM ads_ovopark_store_monthly
            WHERE report_date = %s
              AND data_version = %s
            """,
            (report_date, data_version),
        )
        target_stats = cursor.fetchone()

    return {
        'source_row_count': int(source_stats['source_row_count'] or 0),
        'source_store_count': int(source_stats['source_store_count'] or 0),
        'target_row_count': int(target_stats['target_row_count'] or 0),
    }


def conn_test(report_date: date, data_version: str):
    with connect_mysql_dict(timeout_profile='long_running', autocommit=True) as conn:
        ensure_tables(conn, REQUIRED_TABLES)
        stats = _fetch_scope_stats(conn, report_date, data_version)
    logger.info('ads_ovopark_store_monthly conn-test 通过：source_rows=%s, source_stores=%s', stats['source_row_count'], stats['source_store_count'])


def run(report_date: date, data_version: str, execute: bool = False):
    with connect_mysql_dict(timeout_profile='long_running', autocommit=False) as conn:
        ensure_tables(conn, REQUIRED_TABLES)
        stats = _fetch_scope_stats(conn, report_date, data_version)

        if not execute:
            logger.info('dry-run：未执行 ADS 装载；report_date=%s, source_rows=%s', report_date, stats['source_row_count'])
            return {'execute': False, 'report_date': report_date.isoformat(), 'data_version': data_version, **stats}

        try:
            with conn.cursor() as cursor:
                cursor.execute(DELETE_SQL, (report_date, data_version))
                cursor.execute(INSERT_SQL, (report_date, report_date, report_date, report_date, data_version))
                inserted_rows = cursor.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        with connect_mysql_dict(timeout_profile='long_running', autocommit=True) as verify_conn:
            verified_stats = _fetch_scope_stats(verify_conn, report_date, data_version)

    logger.info('ads_ovopark_store_monthly 装载完成：inserted_rows=%s, final_rows=%s', inserted_rows, verified_stats['target_row_count'])
    return {
        'execute': True,
        'report_date': report_date.isoformat(),
        'data_version': data_version,
        'inserted_rows': inserted_rows,
        **verified_stats,
    }


def _parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description='Build ads_ovopark_store_monthly from DWS monthly flow')
    parser.add_argument('--report-date', type=date.fromisoformat, default=yesterday, help='报表观察日，格式 YYYY-MM-DD')
    parser.add_argument('--data-version', default='v1', help='版本号，默认 v1')
    parser.add_argument('--conn-test', action='store_true', help='只做 MySQL 依赖检查与范围统计')
    parser.add_argument('--execute', action='store_true', help='执行 ADS 装载')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    if args.conn_test:
        conn_test(args.report_date, args.data_version)
    else:
        run(args.report_date, args.data_version, execute=args.execute)