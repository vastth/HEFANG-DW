# -*- coding: utf-8 -*-
"""万店掌客流 ODS ETL。"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime

from ovopark_api_client import OvoparkApiClient, load_ovopark_credentials
from ovopark_etl_common import (
    as_decimal_value,
    as_int,
    connect_mysql_dict,
    ensure_tables,
    iter_dates,
    parse_datetime,
    setup_logger,
    to_date_id,
)


REQUIRED_TABLES = (
    'ods_ovopark_api_raw',
    'ods_ovopark_passenger_flow_daily',
    'ods_ovopark_passenger_flow_hourly',
    'dim_ovopark_shop_mapping',
)

RAW_INSERT_SQL = """
INSERT INTO ods_ovopark_api_raw (
    api_name,
    request_method,
    request_route,
    request_object_type,
    request_object_key,
    request_shop_id,
    request_page_number,
    request_page_size,
    request_time_type,
    request_start_hour,
    request_end_hour,
    request_is_on_business_time,
    request_window_start,
    request_window_end,
    request_param_json,
    response_stat_code,
    response_codename,
    response_result,
    response_total,
    response_row_count,
    gateway_request_id,
    response_json,
    requested_at,
    etl_batch_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

DAILY_UPSERT_SQL = """
INSERT INTO ods_ovopark_passenger_flow_daily (
    date_id,
    dep_id,
    dep_key,
    shop_id,
    shop_name,
    request_window_start,
    request_window_end,
    is_on_business_time,
    passenger_flow,
    outside_passenger_flow,
    in_shop_rate,
    out_flow_count,
    dressing_rate,
    pass_passenger_flow,
    dressing_passenger_flow,
    response_stat_code,
    requested_at,
    etl_batch_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    dep_key = VALUES(dep_key),
    shop_id = VALUES(shop_id),
    shop_name = VALUES(shop_name),
    request_window_start = VALUES(request_window_start),
    request_window_end = VALUES(request_window_end),
    passenger_flow = VALUES(passenger_flow),
    outside_passenger_flow = VALUES(outside_passenger_flow),
    in_shop_rate = VALUES(in_shop_rate),
    out_flow_count = VALUES(out_flow_count),
    dressing_rate = VALUES(dressing_rate),
    pass_passenger_flow = VALUES(pass_passenger_flow),
    dressing_passenger_flow = VALUES(dressing_passenger_flow),
    response_stat_code = VALUES(response_stat_code),
    requested_at = VALUES(requested_at),
    etl_batch_id = VALUES(etl_batch_id)
""".strip()

HOURLY_UPSERT_SQL = """
INSERT INTO ods_ovopark_passenger_flow_hourly (
    biz_date_id,
    stat_time,
    dep_id,
    dep_key,
    request_object_key,
    request_object_type,
    shop_id,
    shop_name,
    time_type,
    start_hour,
    end_hour,
    is_on_business_time,
    passenger_flow,
    pass_passenger_flow,
    in_count_having_pass_device,
    outside_passenger_flow,
    in_shop_rate,
    out_flow_count,
    dressing_rate,
    duplicated_flow,
    response_stat_code,
    requested_at,
    etl_batch_id
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    dep_key = VALUES(dep_key),
    shop_id = VALUES(shop_id),
    shop_name = VALUES(shop_name),
    passenger_flow = VALUES(passenger_flow),
    pass_passenger_flow = VALUES(pass_passenger_flow),
    in_count_having_pass_device = VALUES(in_count_having_pass_device),
    outside_passenger_flow = VALUES(outside_passenger_flow),
    in_shop_rate = VALUES(in_shop_rate),
    out_flow_count = VALUES(out_flow_count),
    dressing_rate = VALUES(dressing_rate),
    duplicated_flow = VALUES(duplicated_flow),
    response_stat_code = VALUES(response_stat_code),
    requested_at = VALUES(requested_at),
    etl_batch_id = VALUES(etl_batch_id)
""".strip()


logger = setup_logger(__name__)


def _insert_raw(
    cursor,
    response_json,
    request_meta,
    batch_id,
    requested_at,
    request_object_type,
    request_object_key,
    request_window_start,
    request_window_end,
    time_type,
    start_hour,
    end_hour,
    is_on_business_time,
):
    stat = response_json.get('stat') or {}
    data = response_json.get('data') or {}
    response_row_count = None
    if isinstance(data, list):
        response_row_count = sum(len(item.get('dataList') or []) for item in data if isinstance(item, dict))
    elif isinstance(data, dict):
        response_row_count = len(data.get('rows') or [])
        if response_row_count == 0 and data:
            response_row_count = 1
    gateway_request_id = request_meta['response_headers'].get('requestId') or request_meta['response_headers'].get('x-request-id')
    cursor.execute(
        RAW_INSERT_SQL,
        (
            request_meta['method_name'],
            'POST',
            load_ovopark_credentials().base_url,
            request_object_type,
            request_object_key,
            request_meta['request_params'].get('shopId'),
            None,
            None,
            time_type,
            start_hour,
            end_hour,
            is_on_business_time,
            request_window_start,
            request_window_end,
            json.dumps(request_meta['request_params'], ensure_ascii=False),
            stat.get('code'),
            stat.get('codename'),
            response_json.get('result'),
            data.get('total') if isinstance(data, dict) else None,
            response_row_count,
            gateway_request_id,
            json.dumps(response_json, ensure_ascii=False),
            requested_at,
            batch_id,
        ),
    )


def _load_dep_scope(conn, dep_ids):
    sql = """
        SELECT DISTINCT
            m.ovopark_dep_id AS dep_id,
            m.ovopark_dep_key AS dep_key,
            COALESCE(m.ovopark_shop_name, os.shop_name) AS ovopark_shop_name
        FROM dim_ovopark_shop_mapping m
        LEFT JOIN ods_ovopark_shop os
          ON m.ovopark_dep_id = os.dep_id
        WHERE m.is_current = 'Y'
          AND m.mapping_status = 'MATCHED'
    """
    params = []
    if dep_ids:
        placeholders = ', '.join(['%s'] * len(dep_ids))
        sql += f" AND m.ovopark_dep_id IN ({placeholders})"
        params.extend(dep_ids)
    sql += ' ORDER BY m.ovopark_dep_id'

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
    if not rows:
        raise RuntimeError('dim_ovopark_shop_mapping 当前无 MATCHED 且 is_current=Y 的门店映射，无法装载客流 ODS')
    return rows


def _build_daily_row(dep_scope, requested_at, request_window_start, request_window_end, response_json, batch_id, is_on_business_time):
    data = response_json.get('data') or {}
    stat = response_json.get('stat') or {}
    date_id = int(request_window_start.strftime('%Y%m%d'))
    return (
        date_id,
        dep_scope['dep_id'],
        dep_scope['dep_key'],
        data.get('shopId') or None,
        dep_scope.get('ovopark_shop_name'),
        request_window_start,
        request_window_end,
        is_on_business_time,
        as_int(data.get('passengerFlow')) or 0,
        as_int(data.get('outsidePassengerFlow')) or 0,
        as_decimal_value(data.get('inShopRate')),
        as_int(data.get('outFlowCount')) or 0,
        as_decimal_value(data.get('dressingRate')),
        as_int(data.get('passPassengerFlow')) or 0,
        as_int(data.get('dressingPassengerFlow')) or 0,
        stat.get('code'),
        requested_at,
        batch_id,
    )


def _build_hourly_rows(dep_scope, requested_at, response_json, batch_id, time_type, start_hour, end_hour, is_on_business_time):
    stat = response_json.get('stat') or {}
    root_data = response_json.get('data') or []
    if isinstance(root_data, dict):
        root_data = root_data.get('rows') or root_data.get('data') or []

    output_rows = []
    for shop_row in root_data:
        dep_id = as_int(shop_row.get('depId')) or dep_scope['dep_id']
        dep_key = f'S_{dep_id}'
        shop_name = shop_row.get('name') or dep_scope.get('ovopark_shop_name')
        shop_id = shop_row.get('shopId') or None
        for hour_row in shop_row.get('dataList') or []:
            stat_time = parse_datetime(hour_row.get('time'))
            if stat_time is None:
                continue
            output_rows.append(
                (
                    int(stat_time.strftime('%Y%m%d')),
                    stat_time,
                    dep_id,
                    dep_key,
                    dep_scope['dep_key'],
                    'STORE',
                    shop_id,
                    shop_name,
                    time_type,
                    start_hour,
                    end_hour,
                    is_on_business_time,
                    as_int(hour_row.get('passengerFlow')) or 0,
                    as_int(hour_row.get('passPassengerFlow')) or 0,
                    as_int(hour_row.get('inCountHavingPassDevice')) or 0,
                    as_int(hour_row.get('outSidePassengerFlow')) or 0,
                    as_decimal_value(hour_row.get('inShopRate')),
                    as_int(hour_row.get('outFlowCount')) or 0,
                    as_decimal_value(hour_row.get('dressingRate')),
                    as_int(hour_row.get('duplicatedFlow')) or 0,
                    stat.get('code'),
                    requested_at,
                    batch_id,
                )
            )
    return output_rows


def conn_test(sample_dep_id: int | None = None, include_hourly: bool = False, is_on_business_time: int = 0):
    credentials = load_ovopark_credentials(require_login=True)
    client = OvoparkApiClient(credentials)
    authenticator = client.resolve_authenticator()

    with connect_mysql_dict(timeout_profile='long_running', autocommit=True) as conn:
        ensure_tables(conn, REQUIRED_TABLES)
        dep_scope = _load_dep_scope(conn, [sample_dep_id] if sample_dep_id else None)[0]

    probe_date = date.today()
    start_time = datetime(probe_date.year, probe_date.month, probe_date.day, 0, 0, 0)
    end_time = datetime(probe_date.year, probe_date.month, probe_date.day, 23, 59, 59)

    response_json, _ = client.get_daily_passenger_indicator(
        dep_id=dep_scope['dep_id'],
        start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
        end_time=end_time.strftime('%Y-%m-%d %H:%M:%S'),
        authenticator=authenticator,
        is_on_business_time=is_on_business_time,
    )
    logger.info('Ovopark 客流日级 conn-test 通过：dep_id=%s, stat=%s', dep_scope['dep_id'], (response_json.get('stat') or {}).get('code'))

    if include_hourly:
        hourly_json, _ = client.get_hourly_passenger_indicator(
            dep_key=dep_scope['dep_key'],
            start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_time=end_time.strftime('%Y-%m-%d %H:%M:%S'),
            authenticator=authenticator,
            time_type=1,
            is_on_business_time=is_on_business_time,
        )
        logger.info('Ovopark 客流小时 conn-test 通过：dep_key=%s, stat=%s', dep_scope['dep_key'], (hourly_json.get('stat') or {}).get('code'))


def run(
    *,
    start_date: date,
    end_date: date,
    dep_ids: list[int] | None,
    include_hourly: bool,
    is_on_business_time: int,
    execute: bool,
):
    with connect_mysql_dict(timeout_profile='long_running', autocommit=False) as conn:
        ensure_tables(conn, REQUIRED_TABLES)
        dep_scope = _load_dep_scope(conn, dep_ids)

        if not execute:
            logger.info(
                'dry-run：未执行 API 抓取；dep_count=%s, start_date=%s, end_date=%s, include_hourly=%s',
                len(dep_scope),
                start_date,
                end_date,
                include_hourly,
            )
            return {
                'execute': False,
                'dep_count': len(dep_scope),
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'include_hourly': include_hourly,
            }

        credentials = load_ovopark_credentials(require_login=True)
        client = OvoparkApiClient(credentials)
        authenticator = client.resolve_authenticator()
        batch_id = f"ovopark_pf_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        daily_row_count = 0
        hourly_row_count = 0
        request_count = 0

        try:
            for shop_scope in dep_scope:
                for biz_date in iter_dates(start_date, end_date):
                    request_window_start = datetime(biz_date.year, biz_date.month, biz_date.day, 0, 0, 0)
                    request_window_end = datetime(biz_date.year, biz_date.month, biz_date.day, 23, 59, 59)

                    daily_requested_at = datetime.now()
                    daily_json, daily_meta = client.get_daily_passenger_indicator(
                        dep_id=shop_scope['dep_id'],
                        start_time=request_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                        end_time=request_window_end.strftime('%Y-%m-%d %H:%M:%S'),
                        authenticator=authenticator,
                        is_on_business_time=is_on_business_time,
                    )

                    with conn.cursor() as cursor:
                        _insert_raw(
                            cursor,
                            daily_json,
                            daily_meta,
                            batch_id,
                            daily_requested_at,
                            'DEP_ID',
                            str(shop_scope['dep_id']),
                            request_window_start,
                            request_window_end,
                            None,
                            None,
                            None,
                            is_on_business_time,
                        )
                        cursor.execute(
                            DAILY_UPSERT_SQL,
                            _build_daily_row(
                                shop_scope,
                                daily_requested_at,
                                request_window_start,
                                request_window_end,
                                daily_json,
                                batch_id,
                                is_on_business_time,
                            ),
                        )
                    conn.commit()

                    request_count += 1
                    daily_row_count += 1

                    if include_hourly:
                        hourly_requested_at = datetime.now()
                        hourly_json, hourly_meta = client.get_hourly_passenger_indicator(
                            dep_key=shop_scope['dep_key'],
                            start_time=request_window_start.strftime('%Y-%m-%d %H:%M:%S'),
                            end_time=request_window_end.strftime('%Y-%m-%d %H:%M:%S'),
                            authenticator=authenticator,
                            time_type=1,
                            is_on_business_time=is_on_business_time,
                        )
                        hourly_rows = _build_hourly_rows(
                            shop_scope,
                            hourly_requested_at,
                            hourly_json,
                            batch_id,
                            1,
                            None,
                            None,
                            is_on_business_time,
                        )
                        with conn.cursor() as cursor:
                            _insert_raw(
                                cursor,
                                hourly_json,
                                hourly_meta,
                                batch_id,
                                hourly_requested_at,
                                'DEP_KEY',
                                shop_scope['dep_key'],
                                request_window_start,
                                request_window_end,
                                1,
                                None,
                                None,
                                is_on_business_time,
                            )
                            if hourly_rows:
                                cursor.executemany(HOURLY_UPSERT_SQL, hourly_rows)
                        conn.commit()
                        request_count += 1
                        hourly_row_count += len(hourly_rows)

                logger.info('Ovopark 客流 ODS 已完成 dep_id=%s', shop_scope['dep_id'])
        except Exception:
            conn.rollback()
            raise

    logger.info(
        'Ovopark 客流 ODS 装载完成：dep_count=%s, request_count=%s, daily_rows=%s, hourly_rows=%s, batch_id=%s',
        len(dep_scope),
        request_count,
        daily_row_count,
        hourly_row_count,
        batch_id,
    )
    return {
        'execute': True,
        'dep_count': len(dep_scope),
        'request_count': request_count,
        'daily_rows': daily_row_count,
        'hourly_rows': hourly_row_count,
        'batch_id': batch_id,
    }


def _parse_args():
    parser = argparse.ArgumentParser(description='Load Ovopark passenger flow into ODS tables')
    parser.add_argument('--start-date', type=date.fromisoformat, default=date.today(), help='开始日期，格式 YYYY-MM-DD')
    parser.add_argument('--end-date', type=date.fromisoformat, default=date.today(), help='结束日期，格式 YYYY-MM-DD')
    parser.add_argument('--dep-id', dest='dep_ids', action='append', type=int, default=None, help='指定万店掌 dep_id；可重复传入')
    parser.add_argument('--include-hourly', action='store_true', help='同时抓取小时级客流')
    parser.add_argument('--is-on-business-time', type=int, default=0, help='是否只拉营业时间内数据，默认 0=全天')
    parser.add_argument('--conn-test', action='store_true', help='只执行 API 登录、维表范围与依赖检查')
    parser.add_argument('--execute', action='store_true', help='执行 API 抓取并写入 ODS')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    if args.start_date > args.end_date:
        raise ValueError('--start-date 不能晚于 --end-date')

    if args.conn_test:
        conn_test(
            sample_dep_id=args.dep_ids[0] if args.dep_ids else None,
            include_hourly=args.include_hourly,
            is_on_business_time=args.is_on_business_time,
        )
    else:
        run(
            start_date=args.start_date,
            end_date=args.end_date,
            dep_ids=args.dep_ids,
            include_hourly=args.include_hourly,
            is_on_business_time=args.is_on_business_time,
            execute=args.execute,
        )