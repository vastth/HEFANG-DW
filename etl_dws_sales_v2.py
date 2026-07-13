# -*- coding: utf-8 -*-
"""DWS v2 销售日汇总并行表 dry-run / conn-test / S3 手工写入脚本。

默认模式仍只输出候选 SQL；``--conn-test`` 只做只读连接与结构检查。
进入 S3 后新增受控 ``--execute`` 分支，但必须由用户在本地手工传入精确
``--confirm-write`` 令牌才会写入 ``dws_sales_daily_v2``。脚本仍不接入
``run_etl.py``、``scheduled_etl.py``、``scheduled_total_control.py``，也不切换 ADS。
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text

from db_connections import create_mysql_engine
from dws_v2_write_utils import (
    acquire_named_lock,
    commit_if_open,
    ensure_write_confirmation,
    fetch_all_mappings,
    fetch_one_mapping,
    release_named_lock,
    validate_lock_settings,
    write_runtime_report,
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


DEFAULT_SOURCE_TABLE = 'dwd_sales_retail_item'
DEFAULT_TARGET_TABLE = 'dws_sales_daily_v2'
DEFAULT_START_DATE = 20260428
DEFAULT_END_DATE = 20260430
DEFAULT_SOURCE_LAYER_VERSION = 'M3_DWD_V1'
DEFAULT_VALIDATION_STATUS = 'PENDING'
DEFAULT_VALIDATION_NOTE = 'S3 manual branch generated; pending reconciliation'
DEFAULT_LOAD_BATCH_ID = 'DWS_SALES_V2_S3_MANUAL'
WRITE_CONFIRMATION_TOKEN = 'WRITE_DWS_SALES_V2'
DEFAULT_LOCK_NAME = 'hefang_dw:dws_sales_daily_v2:s3'
DEFAULT_LOCK_TIMEOUT_SECONDS = 10
DEFAULT_RECONCILIATION_LIMIT = 20

IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

SOURCE_REQUIRED_COLUMNS = {
    'date_id',
    'store_id',
    'store_code',
    'is_cloud_store',
    'product_id',
    'm_productalias_id',
    'qty',
    'line_actual_amt',
    'line_list_amt',
    'retail_id',
    'is_positive_sale_flag',
    'is_return_flag',
    'dws_sales_scope_flag',
    'retail_modified_at',
    'item_modified_at',
    'item_set_time',
    'retail_source_loaded_at',
    'item_source_loaded_at',
}

TARGET_REQUIRED_COLUMNS = {
    'date_id',
    'store_id',
    'store_code',
    'is_cloud_store',
    'product_id',
    'm_productalias_id',
    'sales_qty',
    'sales_amount',
    'sales_amount_list',
    'return_qty',
    'return_amount',
    'net_qty',
    'net_amount',
    'order_count',
    'source_dwd_row_count',
    'positive_line_count',
    'return_line_count',
    'min_retail_modified_at',
    'max_retail_modified_at',
    'min_item_modified_at',
    'max_item_modified_at',
    'min_item_set_time',
    'max_item_set_time',
    'source_min_loaded_at',
    'source_max_loaded_at',
    'load_batch_id',
    'source_layer_version',
    'validation_status',
    'validation_note',
    'etl_time',
}

TARGET_UNIQUE_KEY_COLUMNS = [
    'date_id',
    'store_id',
    'product_id',
    'm_productalias_id',
]

UPSERT_UPDATE_COLUMNS = (
    'store_code',
    'is_cloud_store',
    'sales_qty',
    'sales_amount',
    'sales_amount_list',
    'return_qty',
    'return_amount',
    'net_qty',
    'net_amount',
    'order_count',
    'source_dwd_row_count',
    'positive_line_count',
    'return_line_count',
    'min_retail_modified_at',
    'max_retail_modified_at',
    'min_item_modified_at',
    'max_item_modified_at',
    'min_item_set_time',
    'max_item_set_time',
    'source_min_loaded_at',
    'source_max_loaded_at',
    'load_batch_id',
    'source_layer_version',
    'validation_status',
    'validation_note',
    'etl_time',
)


@dataclass(frozen=True)
class DwsSalesV2DryRunConfig:
    start_date: int = DEFAULT_START_DATE
    end_date: int = DEFAULT_END_DATE
    source_table: str = DEFAULT_SOURCE_TABLE
    target_table: str = DEFAULT_TARGET_TABLE
    load_batch_id: str = DEFAULT_LOAD_BATCH_ID
    source_layer_version: str = DEFAULT_SOURCE_LAYER_VERSION
    validation_status: str = DEFAULT_VALIDATION_STATUS
    validation_note: str = DEFAULT_VALIDATION_NOTE
    timeout_profile: str = 'etl'
    lock_name: str = DEFAULT_LOCK_NAME
    lock_timeout_seconds: int = DEFAULT_LOCK_TIMEOUT_SECONDS


def _validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f'非法表名或标识符: {identifier}')
    return identifier


def _validate_config(config: DwsSalesV2DryRunConfig) -> None:
    if config.start_date > config.end_date:
        raise ValueError('start_date 不能大于 end_date')
    _validate_identifier(config.source_table)
    _validate_identifier(config.target_table)
    validate_lock_settings(config.lock_name, config.lock_timeout_seconds)


def build_params(config: DwsSalesV2DryRunConfig) -> dict[str, object]:
    return {
        'start_date': config.start_date,
        'end_date': config.end_date,
        'load_batch_id': config.load_batch_id,
        'source_layer_version': config.source_layer_version,
        'validation_status': config.validation_status,
        'validation_note': config.validation_note,
    }


def build_source_summary_sql(config: DwsSalesV2DryRunConfig) -> str:
    """生成只读源数据范围摘要 SQL，不写库。"""

    _validate_config(config)
    return f"""
SELECT
    COUNT(*) AS source_dwd_row_count,
    COUNT(DISTINCT CONCAT_WS('|', date_id, store_id, product_id, m_productalias_id)) AS candidate_group_count,
    SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN 1 ELSE 0 END) AS positive_line_count,
    SUM(CASE WHEN is_return_flag = 'Y' THEN 1 ELSE 0 END) AS return_line_count,
    MIN(date_id) AS min_date_id,
    MAX(date_id) AS max_date_id,
    MIN(retail_modified_at) AS min_retail_modified_at,
    MAX(retail_modified_at) AS max_retail_modified_at,
    MIN(item_modified_at) AS min_item_modified_at,
    MAX(item_modified_at) AS max_item_modified_at,
    MIN(item_set_time) AS min_item_set_time,
    MAX(item_set_time) AS max_item_set_time
FROM {config.source_table}
WHERE date_id >= :start_date
  AND date_id <= :end_date
  AND dws_sales_scope_flag = 'Y'
  AND date_id IS NOT NULL
  AND store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND m_productalias_id IS NOT NULL
""".strip()


def build_insert_sql(config: DwsSalesV2DryRunConfig) -> str:
    """生成候选 INSERT ... SELECT SQL；当前脚本只打印，不执行。"""

    _validate_config(config)
    update_clause = ',\n    '.join(
        f'{column} = VALUES({column})' for column in UPSERT_UPDATE_COLUMNS
    )
    return f"""
INSERT INTO {config.target_table} (
    date_id,
    store_id,
    store_code,
    is_cloud_store,
    product_id,
    m_productalias_id,
    sales_qty,
    sales_amount,
    sales_amount_list,
    return_qty,
    return_amount,
    net_qty,
    net_amount,
    order_count,
    source_dwd_row_count,
    positive_line_count,
    return_line_count,
    min_retail_modified_at,
    max_retail_modified_at,
    min_item_modified_at,
    max_item_modified_at,
    min_item_set_time,
    max_item_set_time,
    source_min_loaded_at,
    source_max_loaded_at,
    load_batch_id,
    source_layer_version,
    validation_status,
    validation_note,
    etl_time
)
SELECT
    agg.date_id,
    agg.store_id,
    agg.store_code,
    agg.is_cloud_store,
    agg.product_id,
    agg.m_productalias_id,
    agg.sales_qty,
    agg.sales_amount,
    agg.sales_amount_list,
    agg.return_qty,
    agg.return_amount,
    agg.sales_qty - agg.return_qty AS net_qty,
    agg.sales_amount - agg.return_amount AS net_amount,
    agg.order_count,
    agg.source_dwd_row_count,
    agg.positive_line_count,
    agg.return_line_count,
    agg.min_retail_modified_at,
    agg.max_retail_modified_at,
    agg.min_item_modified_at,
    agg.max_item_modified_at,
    agg.min_item_set_time,
    agg.max_item_set_time,
    agg.source_min_loaded_at,
    agg.source_max_loaded_at,
    :load_batch_id AS load_batch_id,
    :source_layer_version AS source_layer_version,
    :validation_status AS validation_status,
    :validation_note AS validation_note,
    NOW() AS etl_time
FROM (
    SELECT
        date_id,
        store_id,
        COALESCE(store_code, '') AS store_code,
        COALESCE(is_cloud_store, 'N') AS is_cloud_store,
        product_id,
        m_productalias_id,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(qty, 0) ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_actual_amt, 0) ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_list_amt, 0) ELSE 0 END) AS sales_amount_list,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(qty, 0)) ELSE 0 END) AS return_qty,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(line_actual_amt, 0)) ELSE 0 END) AS return_amount,
        COUNT(DISTINCT CASE WHEN is_positive_sale_flag = 'Y' THEN retail_id END) AS order_count,
        COUNT(*) AS source_dwd_row_count,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN 1 ELSE 0 END) AS positive_line_count,
        SUM(CASE WHEN is_return_flag = 'Y' THEN 1 ELSE 0 END) AS return_line_count,
        MIN(retail_modified_at) AS min_retail_modified_at,
        MAX(retail_modified_at) AS max_retail_modified_at,
        MIN(item_modified_at) AS min_item_modified_at,
        MAX(item_modified_at) AS max_item_modified_at,
        MIN(item_set_time) AS min_item_set_time,
        MAX(item_set_time) AS max_item_set_time,
        MIN(
            CASE
                WHEN retail_source_loaded_at IS NULL THEN item_source_loaded_at
                WHEN item_source_loaded_at IS NULL THEN retail_source_loaded_at
                WHEN retail_source_loaded_at <= item_source_loaded_at THEN retail_source_loaded_at
                ELSE item_source_loaded_at
            END
        ) AS source_min_loaded_at,
        MAX(
            CASE
                WHEN retail_source_loaded_at IS NULL THEN item_source_loaded_at
                WHEN item_source_loaded_at IS NULL THEN retail_source_loaded_at
                WHEN retail_source_loaded_at >= item_source_loaded_at THEN retail_source_loaded_at
                ELSE item_source_loaded_at
            END
        ) AS source_max_loaded_at
    FROM {config.source_table}
    WHERE date_id >= :start_date
      AND date_id <= :end_date
      AND dws_sales_scope_flag = 'Y'
      AND date_id IS NOT NULL
      AND store_id IS NOT NULL
      AND product_id IS NOT NULL
      AND m_productalias_id IS NOT NULL
    GROUP BY date_id, store_id, COALESCE(store_code, ''), COALESCE(is_cloud_store, 'N'), product_id, m_productalias_id
) agg
ON DUPLICATE KEY UPDATE
    {update_clause},
    updated_at = NOW()
""".strip()


def build_target_summary_sql(config: DwsSalesV2DryRunConfig) -> str:
    """生成目标 v2 表装载后摘要 SQL。"""

    _validate_config(config)
    return f"""
SELECT
    COUNT(*) AS target_row_count,
    SUM(COALESCE(source_dwd_row_count, 0)) AS source_dwd_row_count,
    SUM(COALESCE(sales_qty, 0)) AS sales_qty,
    SUM(COALESCE(sales_amount, 0)) AS sales_amount,
    SUM(COALESCE(return_qty, 0)) AS return_qty,
    SUM(COALESCE(return_amount, 0)) AS return_amount,
    SUM(COALESCE(order_count, 0)) AS order_count,
    MIN(etl_time) AS min_etl_time,
    MAX(etl_time) AS max_etl_time
FROM {config.target_table}
WHERE date_id >= :start_date
  AND date_id <= :end_date
""".strip()


def build_reconciliation_detail_sql(config: DwsSalesV2DryRunConfig, *, limit: int | None = None) -> str:
    """生成 DWD 聚合与目标 v2 的差异明细 SQL。"""

    _validate_config(config)
    limit_clause = f'\nLIMIT {int(limit)}' if limit is not None else ''
    return f"""
WITH dwd_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(qty, 0) ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_actual_amt, 0) ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_list_amt, 0) ELSE 0 END) AS sales_amount_list,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(qty, 0)) ELSE 0 END) AS return_qty,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(line_actual_amt, 0)) ELSE 0 END) AS return_amount,
        COUNT(DISTINCT CASE WHEN is_positive_sale_flag = 'Y' THEN retail_id END) AS order_count,
        COUNT(*) AS source_dwd_row_count
    FROM {config.source_table}
    WHERE date_id >= :start_date
      AND date_id <= :end_date
      AND dws_sales_scope_flag = 'Y'
      AND date_id IS NOT NULL
      AND store_id IS NOT NULL
      AND product_id IS NOT NULL
      AND m_productalias_id IS NOT NULL
    GROUP BY date_id, store_id, product_id, m_productalias_id
), v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        sales_qty,
        sales_amount,
        sales_amount_list,
        return_qty,
        return_amount,
        order_count,
        source_dwd_row_count
    FROM {config.target_table}
    WHERE date_id >= :start_date
      AND date_id <= :end_date
), combined_scope AS (
    SELECT 'DWD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count, source_dwd_row_count
    FROM dwd_scope
    UNION ALL
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count, source_dwd_row_count
    FROM v2_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_qty ELSE 0 END) AS dwd_sales_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_qty ELSE 0 END) AS v2_sales_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_amount ELSE 0 END) AS dwd_sales_amount,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_amount ELSE 0 END) AS v2_sales_amount,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_amount_list ELSE 0 END) AS dwd_sales_amount_list,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_amount_list ELSE 0 END) AS v2_sales_amount_list,
    SUM(CASE WHEN source_layer = 'DWD' THEN return_qty ELSE 0 END) AS dwd_return_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN return_qty ELSE 0 END) AS v2_return_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN return_amount ELSE 0 END) AS dwd_return_amount,
    SUM(CASE WHEN source_layer = 'V2' THEN return_amount ELSE 0 END) AS v2_return_amount,
    SUM(CASE WHEN source_layer = 'DWD' THEN order_count ELSE 0 END) AS dwd_order_count,
    SUM(CASE WHEN source_layer = 'V2' THEN order_count ELSE 0 END) AS v2_order_count,
    SUM(CASE WHEN source_layer = 'DWD' THEN source_dwd_row_count ELSE 0 END) AS dwd_source_rows,
    SUM(CASE WHEN source_layer = 'V2' THEN source_dwd_row_count ELSE 0 END) AS v2_source_rows
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_sales_qty - v2_sales_qty) > 0.0001
    OR ABS(dwd_sales_amount - v2_sales_amount) > 0.01
    OR ABS(dwd_sales_amount_list - v2_sales_amount_list) > 0.01
    OR ABS(dwd_return_qty - v2_return_qty) > 0.0001
    OR ABS(dwd_return_amount - v2_return_amount) > 0.01
    OR ABS(dwd_order_count - v2_order_count) > 0.0001
    OR ABS(dwd_source_rows - v2_source_rows) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id{limit_clause}
""".strip()


def build_reconciliation_count_sql(config: DwsSalesV2DryRunConfig) -> str:
    """生成差异行数统计 SQL。"""

    return f"""
SELECT COUNT(*) AS mismatch_count
FROM (
    {build_reconciliation_detail_sql(config)}
) mismatch_scope
""".strip()


def _fetch_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
            """
        ),
        {'table_name': table_name},
    ).fetchall()
    return {row[0] for row in rows}


def _fetch_unique_key_columns(conn, table_name: str, index_name: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
              AND INDEX_NAME = :index_name
              AND NON_UNIQUE = 0
            ORDER BY SEQ_IN_INDEX
            """
        ),
        {'table_name': table_name, 'index_name': index_name},
    ).fetchall()
    return [row[0] for row in rows]


def _assert_columns(table_name: str, actual_columns: set[str], required_columns: set[str]) -> None:
    if not actual_columns:
        raise RuntimeError(f'表不存在或无字段: {table_name}')
    missing_columns = sorted(required_columns - actual_columns)
    if missing_columns:
        raise RuntimeError(f'{table_name} 缺少字段: {missing_columns}')


def _assert_structure(conn, config: DwsSalesV2DryRunConfig) -> None:
    """校验源表、目标表字段与唯一键结构。"""

    source_columns = _fetch_columns(conn, config.source_table)
    target_columns = _fetch_columns(conn, config.target_table)
    _assert_columns(config.source_table, source_columns, SOURCE_REQUIRED_COLUMNS)
    _assert_columns(config.target_table, target_columns, TARGET_REQUIRED_COLUMNS)
    unique_key_columns = _fetch_unique_key_columns(
        conn,
        config.target_table,
        'uk_dws_sales_daily_v2_date_store_product_sku',
    )
    if unique_key_columns != TARGET_UNIQUE_KEY_COLUMNS:
        raise RuntimeError(f'{config.target_table} 唯一键不符合预期: {unique_key_columns}')


def conn_test(config: DwsSalesV2DryRunConfig) -> None:
    """只读连接和结构检查；不写入目标表。"""

    _validate_config(config)
    engine = create_mysql_engine(timeout_profile=config.timeout_profile)
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
            _assert_structure(conn, config)
    finally:
        engine.dispose()
    logger.info(
        'dws_sales_daily_v2 conn-test 通过：source=%s, target=%s, timeout_profile=%s',
        config.source_table,
        config.target_table,
        config.timeout_profile,
    )


def execute_load(
    config: DwsSalesV2DryRunConfig,
    *,
    confirm_write: str | None,
    output_json: str | None = None,
    reconciliation_limit: int = DEFAULT_RECONCILIATION_LIMIT,
) -> dict[str, Any]:
    """执行 S3 手工写入分支，并输出运行证据。"""

    _validate_config(config)
    ensure_write_confirmation(confirm_write, WRITE_CONFIRMATION_TOKEN, config.target_table)

    params = build_params(config)
    report: dict[str, Any] = {
        'script': 'etl_dws_sales_v2.py',
        'mode': 'execute',
        'target_table': config.target_table,
        'source_table': config.source_table,
        'date_window': {'start_date': config.start_date, 'end_date': config.end_date},
        'timeout_profile': config.timeout_profile,
        'lock_name': config.lock_name,
        'lock_timeout_seconds': config.lock_timeout_seconds,
        'started_at': datetime.now().isoformat(sep=' '),
        'status': 'STARTED',
    }

    engine = create_mysql_engine(timeout_profile=config.timeout_profile)
    lock_acquired = False
    output_path = None
    original_error: Exception | None = None
    try:
        with engine.connect() as conn:
            try:
                conn.execute(text('SELECT 1'))
                _assert_structure(conn, config)
                report['source_summary_before'] = fetch_one_mapping(conn, build_source_summary_sql(config), params)
                report['target_summary_before'] = fetch_one_mapping(conn, build_target_summary_sql(config), params)
                commit_if_open(conn)

                acquire_named_lock(conn, config.lock_name, config.lock_timeout_seconds)
                lock_acquired = True
                logger.info('已获取 MySQL 命名锁: %s', config.lock_name)

                started = time.perf_counter()
                transaction = conn.begin()
                try:
                    result = conn.execute(text(build_insert_sql(config)), params)
                    transaction.commit()
                except Exception as exc:
                    original_error = exc
                    if transaction.is_active:
                        transaction.rollback()
                    report['status'] = 'FAILED'
                    report['error'] = repr(exc)
                    report['cleanup'] = 'transaction_rollback_attempted; named_lock_release_pending'
                    raise
                duration_seconds = round(time.perf_counter() - started, 3)

                report['insert_rowcount'] = result.rowcount
                report['write_duration_seconds'] = duration_seconds
                report['target_summary_after'] = fetch_one_mapping(conn, build_target_summary_sql(config), params)
                report['reconciliation'] = {
                    'mismatch_count': fetch_one_mapping(conn, build_reconciliation_count_sql(config), params).get('mismatch_count', 0),
                    'sample_limit': reconciliation_limit,
                    'sample_mismatches': fetch_all_mappings(
                        conn,
                        build_reconciliation_detail_sql(config, limit=reconciliation_limit),
                        params,
                    ),
                }
                mismatch_count = int(report['reconciliation']['mismatch_count'] or 0)
                report['status'] = 'SUCCESS' if mismatch_count == 0 else 'WARNING'
                report['finished_at'] = datetime.now().isoformat(sep=' ')
                report['cleanup'] = 'write_transaction_committed; named_lock_release_pending'
            finally:
                if lock_acquired:
                    try:
                        release_named_lock(conn, config.lock_name)
                        report['lock_released'] = True
                        report['cleanup'] = f"{report.get('cleanup', '')}; named_lock_released"
                    except Exception as release_error:
                        report['lock_released'] = False
                        report['lock_release_error'] = repr(release_error)
                        if original_error is None:
                            raise
    except Exception as exc:
        original_error = original_error or exc
        report.setdefault('status', 'FAILED')
        report.setdefault('error', repr(original_error))
        report['finished_at'] = datetime.now().isoformat(sep=' ')
        raise
    finally:
        output_path = write_runtime_report(report, 'dws_sales_v2_s3_load', output_json)
        logger.info('DWS sales v2 S3 运行证据已写入: %s', output_path)
        engine.dispose()

    report['output_json'] = str(output_path) if output_path else None
    return report


def run(
    config: DwsSalesV2DryRunConfig,
    *,
    conn_test_only: bool,
    execute: bool = False,
    confirm_write: str | None = None,
    output_json: str | None = None,
    reconciliation_limit: int = DEFAULT_RECONCILIATION_LIMIT,
) -> None:
    if conn_test_only and execute:
        raise ValueError('--conn-test 不能与 --execute 同时使用')

    if conn_test_only:
        conn_test(config)
        return

    if execute:
        report = execute_load(
            config,
            confirm_write=confirm_write,
            output_json=output_json,
            reconciliation_limit=reconciliation_limit,
        )
        logger.info(
            'dws_sales_daily_v2 S3 写入分支完成：status=%s, mismatch_count=%s, output_json=%s',
            report.get('status'),
            (report.get('reconciliation') or {}).get('mismatch_count'),
            report.get('output_json'),
        )
        return

    logger.info(
        '生成 dws_sales_daily_v2 dry-run SQL，日期范围：%s - %s；不会连接数据库或写入 MySQL。',
        config.start_date,
        config.end_date,
    )
    print('-- DWS sales v2 source summary SQL (read-only)')
    print(build_source_summary_sql(config))
    print('\n-- DWS sales v2 candidate INSERT SQL (manual S3 execute requires explicit confirmation)')
    print(build_insert_sql(config))
    print('\n-- DWS sales v2 post-load target summary SQL (read-only)')
    print(build_target_summary_sql(config))
    print('\n-- DWS sales v2 DWD-v2 reconciliation SQL (read-only; sample)')
    print(build_reconciliation_detail_sql(config, limit=DEFAULT_RECONCILIATION_LIMIT))
    print('\n-- Parameters')
    print(build_params(config))
    logger.info(
        '默认 dry-run 不写库；若用户手工执行 S3 写入，需追加 --execute --confirm-write %s。',
        WRITE_CONFIRMATION_TOKEN,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='DWS v2 销售日汇总并行表 dry-run / conn-test / S3 手工写入。'
    )
    parser.add_argument('--start-date', type=int, default=DEFAULT_START_DATE, help='开始日期 YYYYMMDD，默认 M3 验证窗口 20260428。')
    parser.add_argument('--end-date', type=int, default=DEFAULT_END_DATE, help='结束日期 YYYYMMDD，默认 M3 验证窗口 20260430。')
    parser.add_argument('--source-table', default=DEFAULT_SOURCE_TABLE, help='DWD 源表，默认 dwd_sales_retail_item。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='DWS v2 目标表名，默认 dws_sales_daily_v2。')
    parser.add_argument('--load-batch-id', default=DEFAULT_LOAD_BATCH_ID, help='装载批次 ID，写入分支会落入目标表。')
    parser.add_argument('--source-layer-version', default=DEFAULT_SOURCE_LAYER_VERSION, help='来源层版本标识。')
    parser.add_argument('--validation-status', default=DEFAULT_VALIDATION_STATUS, help='候选并行对账状态，默认 PENDING。')
    parser.add_argument('--validation-note', default=DEFAULT_VALIDATION_NOTE, help='候选并行对账说明。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='etl')
    parser.add_argument('--conn-test', action='store_true', help='只执行 MySQL SELECT 1 与只读结构检查。')
    parser.add_argument('--execute', action='store_true', help='执行 S3 手工写入分支；必须同时传入 --confirm-write。')
    parser.add_argument('--confirm-write', default=None, help=f'写入确认令牌，必须精确等于 {WRITE_CONFIRMATION_TOKEN}。')
    parser.add_argument('--lock-name', default=DEFAULT_LOCK_NAME, help='MySQL 命名锁名称。')
    parser.add_argument('--lock-timeout-seconds', type=int, default=DEFAULT_LOCK_TIMEOUT_SECONDS, help='获取命名锁等待秒数。')
    parser.add_argument('--output-json', default=None, help='S3 写入运行证据 JSON 输出路径；未传则写入 reports/context_cache。')
    parser.add_argument('--reconciliation-limit', type=int, default=DEFAULT_RECONCILIATION_LIMIT, help='写入后差异样本输出行数。')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = DwsSalesV2DryRunConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        source_table=args.source_table,
        target_table=args.target_table,
        load_batch_id=args.load_batch_id,
        source_layer_version=args.source_layer_version,
        validation_status=args.validation_status,
        validation_note=args.validation_note,
        timeout_profile=args.timeout_profile,
        lock_name=args.lock_name,
        lock_timeout_seconds=args.lock_timeout_seconds,
    )
    run(
        config,
        conn_test_only=args.conn_test,
        execute=args.execute,
        confirm_write=args.confirm_write,
        output_json=args.output_json,
        reconciliation_limit=args.reconciliation_limit,
    )


if __name__ == '__main__':
    main()