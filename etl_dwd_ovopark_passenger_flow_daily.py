# -*- coding: utf-8 -*-
"""DWD 万店掌门店日级客流事实 ETL。"""

from __future__ import annotations

import argparse
from datetime import date, timedelta

from ovopark_etl_common import connect_mysql_dict, ensure_tables, setup_logger, to_date_id


REQUIRED_TABLES = (
    'ods_ovopark_passenger_flow_daily',
    'ods_ovopark_shop',
    'dim_ovopark_shop_mapping',
    'dim_store',
    'dwd_ovopark_passenger_flow_daily',
)

UPSERT_SQL = """
INSERT INTO dwd_ovopark_passenger_flow_daily (
    date_id,
    store_id,
    store_code,
    store_name,
    area_name,
    ovopark_dep_id,
    ovopark_dep_key,
    ovopark_shop_name,
    ovopark_organize_id,
    ovopark_organize_name,
    mapping_status,
    match_source,
    is_on_business_time,
    passenger_flow,
    outside_passenger_flow,
    pass_passenger_flow,
    out_flow_count,
    dressing_passenger_flow,
    in_shop_rate,
    dressing_rate,
    source_request_window_start,
    source_request_window_end,
    source_requested_at,
    source_response_stat_code,
    source_ods_batch_id,
    etl_time
)
SELECT
    o.date_id,
    m.hefang_store_id AS store_id,
    COALESCE(ds.store_code, m.hefang_store_code, '') AS store_code,
    COALESCE(ds.store_name, m.hefang_store_name, '') AS store_name,
    ds.area_name,
    o.dep_id AS ovopark_dep_id,
    o.dep_key AS ovopark_dep_key,
    COALESCE(os.shop_name, o.shop_name, m.ovopark_shop_name) AS ovopark_shop_name,
    os.organize_id AS ovopark_organize_id,
    os.organize_name AS ovopark_organize_name,
    m.mapping_status,
    m.match_source,
    o.is_on_business_time,
    COALESCE(o.passenger_flow, 0) AS passenger_flow,
    COALESCE(o.outside_passenger_flow, 0) AS outside_passenger_flow,
    COALESCE(o.pass_passenger_flow, 0) AS pass_passenger_flow,
    COALESCE(o.out_flow_count, 0) AS out_flow_count,
    COALESCE(o.dressing_passenger_flow, 0) AS dressing_passenger_flow,
    o.in_shop_rate,
    o.dressing_rate,
    o.request_window_start AS source_request_window_start,
    o.request_window_end AS source_request_window_end,
    o.requested_at AS source_requested_at,
    o.response_stat_code AS source_response_stat_code,
    CAST(o.etl_batch_id AS CHAR) AS source_ods_batch_id,
    NOW() AS etl_time
FROM ods_ovopark_passenger_flow_daily o
INNER JOIN dim_ovopark_shop_mapping m
    ON o.dep_id = m.ovopark_dep_id
   AND m.is_current = 'Y'
   AND m.mapping_status = 'MATCHED'
   AND m.hefang_store_id IS NOT NULL
   AND STR_TO_DATE(CAST(o.date_id AS CHAR), '%Y%m%d') BETWEEN m.effective_start_date AND m.effective_end_date
LEFT JOIN ods_ovopark_shop os
    ON o.dep_id = os.dep_id
LEFT JOIN dim_store ds
    ON m.hefang_store_id = ds.store_id
WHERE o.date_id BETWEEN %s AND %s
ON DUPLICATE KEY UPDATE
    store_code = VALUES(store_code),
    store_name = VALUES(store_name),
    area_name = VALUES(area_name),
    ovopark_dep_key = VALUES(ovopark_dep_key),
    ovopark_shop_name = VALUES(ovopark_shop_name),
    ovopark_organize_id = VALUES(ovopark_organize_id),
    ovopark_organize_name = VALUES(ovopark_organize_name),
    mapping_status = VALUES(mapping_status),
    match_source = VALUES(match_source),
    passenger_flow = VALUES(passenger_flow),
    outside_passenger_flow = VALUES(outside_passenger_flow),
    pass_passenger_flow = VALUES(pass_passenger_flow),
    out_flow_count = VALUES(out_flow_count),
    dressing_passenger_flow = VALUES(dressing_passenger_flow),
    in_shop_rate = VALUES(in_shop_rate),
    dressing_rate = VALUES(dressing_rate),
    source_request_window_start = VALUES(source_request_window_start),
    source_request_window_end = VALUES(source_request_window_end),
    source_requested_at = VALUES(source_requested_at),
    source_response_stat_code = VALUES(source_response_stat_code),
    source_ods_batch_id = VALUES(source_ods_batch_id),
    etl_time = VALUES(etl_time)
""".strip()


logger = setup_logger(__name__)


def _fetch_scope_stats(conn, start_date_id: int, end_date_id: int):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS source_row_count,
                COUNT(DISTINCT dep_id) AS source_dep_count
            FROM ods_ovopark_passenger_flow_daily
            WHERE date_id BETWEEN %s AND %s
            """,
            (start_date_id, end_date_id),
        )
        source_stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT COUNT(DISTINCT ovopark_dep_id) AS mapped_dep_count
            FROM dim_ovopark_shop_mapping
            WHERE is_current = 'Y'
              AND mapping_status = 'MATCHED'
              AND hefang_store_id IS NOT NULL
            """
        )
        mapping_stats = cursor.fetchone()

    return {
        'source_row_count': int(source_stats['source_row_count'] or 0),
        'source_dep_count': int(source_stats['source_dep_count'] or 0),
        'mapped_dep_count': int(mapping_stats['mapped_dep_count'] or 0),
    }


def conn_test(start_date: date, end_date: date):
    start_date_id = to_date_id(start_date)
    end_date_id = to_date_id(end_date)
    with connect_mysql_dict(timeout_profile='long_running', autocommit=True) as conn:
        ensure_tables(conn, REQUIRED_TABLES)
        stats = _fetch_scope_stats(conn, start_date_id, end_date_id)
    logger.info(
        'dwd_ovopark_passenger_flow_daily conn-test 通过：source_rows=%s, source_deps=%s, mapped_deps=%s',
        stats['source_row_count'],
        stats['source_dep_count'],
        stats['mapped_dep_count'],
    )


def run(start_date: date, end_date: date, execute: bool = False):
    start_date_id = to_date_id(start_date)
    end_date_id = to_date_id(end_date)
    with connect_mysql_dict(timeout_profile='long_running', autocommit=False) as conn:
        ensure_tables(conn, REQUIRED_TABLES)
        stats = _fetch_scope_stats(conn, start_date_id, end_date_id)

        if not execute:
            logger.info(
                'dry-run：未执行 DWD 装载；date_range=%s~%s, source_rows=%s',
                start_date_id,
                end_date_id,
                stats['source_row_count'],
            )
            return {
                'execute': False,
                'start_date_id': start_date_id,
                'end_date_id': end_date_id,
                **stats,
            }

        try:
            with conn.cursor() as cursor:
                cursor.execute(UPSERT_SQL, (start_date_id, end_date_id))
                affected_rows = cursor.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    logger.info('dwd_ovopark_passenger_flow_daily 装载完成：affected_rows=%s', affected_rows)
    return {
        'execute': True,
        'affected_rows': affected_rows,
        'start_date_id': start_date_id,
        'end_date_id': end_date_id,
        **stats,
    }


def _parse_args():
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser(description='Build dwd_ovopark_passenger_flow_daily from ODS and DIM mapping')
    parser.add_argument('--start-date', type=date.fromisoformat, default=yesterday, help='开始日期，格式 YYYY-MM-DD')
    parser.add_argument('--end-date', type=date.fromisoformat, default=yesterday, help='结束日期，格式 YYYY-MM-DD')
    parser.add_argument('--conn-test', action='store_true', help='只做 MySQL 依赖检查与范围统计')
    parser.add_argument('--execute', action='store_true', help='执行 DWD 装载')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    if args.start_date > args.end_date:
        raise ValueError('--start-date 不能晚于 --end-date')
    if args.conn_test:
        conn_test(args.start_date, args.end_date)
    else:
        run(args.start_date, args.end_date, execute=args.execute)
