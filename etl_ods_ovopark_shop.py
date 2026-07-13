# -*- coding: utf-8 -*-
"""万店掌门店快照 ODS ETL。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from ovopark_api_client import OvoparkApiClient, load_ovopark_credentials
from ovopark_etl_common import connect_mysql_dict, ensure_tables, parse_datetime, setup_logger


REQUIRED_TABLES = ('ods_ovopark_api_raw', 'ods_ovopark_shop')

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

SHOP_UPSERT_SQL = """
INSERT INTO ods_ovopark_shop (
    dep_id,
    dep_key,
    shop_name,
    address,
    organize_id,
    organize_name,
    dep_organize_id,
    group_id,
    shop_id,
    trilateral_id,
    country_code,
    location_code,
    longitude,
    latitude,
    open_status,
    validate_status,
    validate_date,
    close_time,
    create_time,
    device_register_time,
    ipc_current_count,
    ipc_count_limit,
    dev_count,
    has_pc,
    is_complete_config,
    service_permission,
    source_request_at,
    etl_batch_id
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    dep_key = VALUES(dep_key),
    shop_name = VALUES(shop_name),
    address = VALUES(address),
    organize_id = VALUES(organize_id),
    organize_name = VALUES(organize_name),
    dep_organize_id = VALUES(dep_organize_id),
    group_id = VALUES(group_id),
    shop_id = VALUES(shop_id),
    trilateral_id = VALUES(trilateral_id),
    country_code = VALUES(country_code),
    location_code = VALUES(location_code),
    longitude = VALUES(longitude),
    latitude = VALUES(latitude),
    open_status = VALUES(open_status),
    validate_status = VALUES(validate_status),
    validate_date = VALUES(validate_date),
    close_time = VALUES(close_time),
    create_time = VALUES(create_time),
    device_register_time = VALUES(device_register_time),
    ipc_current_count = VALUES(ipc_current_count),
    ipc_count_limit = VALUES(ipc_count_limit),
    dev_count = VALUES(dev_count),
    has_pc = VALUES(has_pc),
    is_complete_config = VALUES(is_complete_config),
    service_permission = VALUES(service_permission),
    source_request_at = VALUES(source_request_at),
    etl_batch_id = VALUES(etl_batch_id)
""".strip()


logger = setup_logger(__name__)


def _normalize_service_permission(value):
    if value in (None, ''):
        return None
    if isinstance(value, str):
        return value[:32]
    return json.dumps(value, ensure_ascii=False)[:32]


def _insert_raw(cursor, response_json, request_meta, batch_id, requested_at, page_number, page_size):
    stat = response_json.get('stat') or {}
    data = response_json.get('data') or {}
    rows = data.get('rows') if isinstance(data, dict) else None
    gateway_request_id = request_meta['response_headers'].get('requestId') or request_meta['response_headers'].get('x-request-id')
    cursor.execute(
        RAW_INSERT_SQL,
        (
            request_meta['method_name'],
            'POST',
            load_ovopark_credentials().base_url,
            'PAGE',
            f'page:{page_number}',
            None,
            page_number,
            page_size,
            None,
            None,
            None,
            None,
            None,
            None,
            json.dumps(request_meta['request_params'], ensure_ascii=False),
            stat.get('code'),
            stat.get('codename'),
            response_json.get('result'),
            data.get('total') if isinstance(data, dict) else None,
            len(rows) if isinstance(rows, list) else None,
            gateway_request_id,
            json.dumps(response_json, ensure_ascii=False),
            requested_at,
            batch_id,
        ),
    )


def _build_shop_rows(rows, batch_id, requested_at):
    output_rows = []
    for row in rows:
        dep_id = int(row['id'])
        output_rows.append(
            (
                dep_id,
                f'S_{dep_id}',
                row.get('name'),
                row.get('address'),
                row.get('organizeId'),
                row.get('organizeName'),
                row.get('depOrganizeId'),
                row.get('groupId'),
                row.get('shopId') or None,
                row.get('trilateralId') or None,
                row.get('countryCode'),
                row.get('location'),
                row.get('longitude'),
                row.get('latitude'),
                row.get('openStatus'),
                row.get('validateStatus'),
                parse_datetime(row.get('validateDate')),
                parse_datetime(row.get('closeTime')),
                parse_datetime(row.get('createTime')),
                parse_datetime(row.get('deviceRegisterTime')),
                row.get('ipcCurrentCount'),
                row.get('ipcCountLimit'),
                row.get('devCount'),
                row.get('hasPc'),
                row.get('isCompleteconfig'),
                _normalize_service_permission(row.get('servicePermission')),
                requested_at,
                batch_id,
            )
        )
    return output_rows


def conn_test(page_size: int = 1):
    credentials = load_ovopark_credentials(require_login=True)
    client = OvoparkApiClient(credentials)
    authenticator = client.resolve_authenticator()
    response_json, _ = client.get_departments(page_number=1, page_size=page_size, authenticator=authenticator)

    with connect_mysql_dict(timeout_profile='long_running', autocommit=True) as conn:
        ensure_tables(conn, REQUIRED_TABLES)

    stat = response_json.get('stat') or {}
    logger.info('Ovopark 门店快照 conn-test 通过：api_stat_code=%s, mysql_ready=Y', stat.get('code'))


def run(page_size: int = 100, max_pages: int | None = None, execute: bool = False):
    with connect_mysql_dict(timeout_profile='long_running', autocommit=False) as conn:
        ensure_tables(conn, REQUIRED_TABLES)

        if not execute:
            logger.info('dry-run：未执行 API 抓取；page_size=%s, max_pages=%s', page_size, max_pages)
            return {
                'execute': False,
                'page_size': page_size,
                'max_pages': max_pages,
            }

        credentials = load_ovopark_credentials(require_login=True)
        client = OvoparkApiClient(credentials)
        authenticator = client.resolve_authenticator()

        batch_id = f"ovopark_shop_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        page_number = 1
        total_rows = 0
        total_pages = 0

        try:
            while True:
                requested_at = datetime.now()
                response_json, request_meta = client.get_departments(
                    page_number=page_number,
                    page_size=page_size,
                    authenticator=authenticator,
                )
                data = response_json.get('data') or {}
                rows = data.get('rows') or []
                total = data.get('total') if isinstance(data, dict) else None

                with conn.cursor() as cursor:
                    _insert_raw(cursor, response_json, request_meta, batch_id, requested_at, page_number, page_size)
                    if rows:
                        cursor.executemany(SHOP_UPSERT_SQL, _build_shop_rows(rows, batch_id, requested_at))
                conn.commit()

                total_rows += len(rows)
                total_pages += 1
                logger.info('Ovopark 门店快照已处理 page=%s, rows=%s, total=%s', page_number, len(rows), total)

                if not rows:
                    break
                if max_pages is not None and page_number >= max_pages:
                    break
                if total is not None and total_rows >= int(total):
                    break
                page_number += 1
        except Exception:
            conn.rollback()
            raise

    logger.info('Ovopark 门店快照装载完成：pages=%s, rows=%s, batch_id=%s', total_pages, total_rows, batch_id)
    return {
        'execute': True,
        'pages': total_pages,
        'rows': total_rows,
        'batch_id': batch_id,
    }


def _parse_args():
    parser = argparse.ArgumentParser(description='Load Ovopark shop snapshots into ODS tables')
    parser.add_argument('--page-size', type=int, default=100, help='分页大小，默认 100')
    parser.add_argument('--max-pages', type=int, default=None, help='最多抓取页数，默认抓到结束')
    parser.add_argument('--conn-test', action='store_true', help='只执行 API 登录与 MySQL 依赖检查')
    parser.add_argument('--execute', action='store_true', help='执行 API 抓取并写入 ODS')
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()
    if args.conn_test:
        conn_test(page_size=min(args.page_size, 5))
    else:
        run(page_size=args.page_size, max_pages=args.max_pages, execute=args.execute)
