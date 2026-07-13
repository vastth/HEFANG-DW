# -*- coding: utf-8 -*-
"""DWS v2 并行 shadow-run 调度入口。

S4 阶段通过独立调度链刷新 M3 raw / DWD 旁路对象，并把结果写入
`dws_sales_daily_v2` / `dws_inventory_daily_v2`。该脚本支持：
1. `--conn-test` 只读检查 Oracle / MySQL 连通性与 v2 结构。
2. 默认调度模式下执行 raw ODS -> DWD -> DWS v2 的 shadow-run。
3. 在总控模式下输出结构化摘要，并抑制子链企业微信，交由总控统一汇总。

设计边界：
- 只写 `_v2` 与旁路 raw / DWD 对象，不修改 `run_etl.py` 主链。
- 库存 old DWS 对齐改为独立的主链 `ods_fa_storage` 可比基线检查；post-refresh 的库存 v2 仅校验 DWD -> v2 自洽。
- shadow-run 自身失败只作为观测告警，不应阻断旧生产链的主退出码。
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Any

from pymysql.cursors import DictCursor

from alerts import send_wechat_alert
from config import (
    ETL_DEFAULT_MAX_RETRIES,
    ETL_DEFAULT_RETRY_SLEEP,
    ETL_NON_RETRYABLE_ERROR_KEYWORDS,
    ETL_RETRYABLE_ERROR_KEYWORDS,
    WECHAT_WEBHOOK,
)
from control_chain_summary import (
    should_suppress_child_wechat_alert,
    write_total_control_chain_summary,
)
from db_connections import connect_mysql
from dws_v2_write_utils import write_runtime_report
from etl_ads_health import validate_inventory_health_shadow_against_persisted
from etl_dwd_inventory_storage_snapshot import (
    DwdInventoryStorageSnapshotConfig,
    conn_test as dwd_inventory_conn_test,
    execute_load as dwd_inventory_execute_load,
)
from etl_dwd_sales_retail_item import (
    DwdSalesRetailItemConfig,
    conn_test as dwd_sales_conn_test,
    execute_load as dwd_sales_execute_load,
)
from etl_dws_inventory_v2 import (
    DwsInventoryV2DryRunConfig,
    WRITE_CONFIRMATION_TOKEN as DWS_INVENTORY_WRITE_CONFIRMATION_TOKEN,
    conn_test as dws_inventory_v2_conn_test,
    execute_load as dws_inventory_v2_execute_load,
)
from etl_dws_sales_v2 import (
    DwsSalesV2DryRunConfig,
    WRITE_CONFIRMATION_TOKEN as DWS_SALES_WRITE_CONFIRMATION_TOKEN,
    conn_test as dws_sales_v2_conn_test,
    execute_load as dws_sales_v2_execute_load,
)
from etl_ods_fa_storage_raw import (
    OdsFaStorageRawConfig,
    conn_test as ods_fa_storage_conn_test,
    execute_load as ods_fa_storage_execute_load,
)
from etl_ods_m_retail_raw import (
    OdsRetailRawConfig,
    conn_test as ods_retail_conn_test,
    execute_load as ods_retail_execute_load,
)
from etl_ods_m_retailitem_raw import (
    OdsRetailItemRawConfig,
    conn_test as ods_retailitem_conn_test,
    execute_load as ods_retailitem_execute_load,
)
from run_etl import DWS_SALES_MAINLINE_DAYS_BACK


PROJECT_DIR = Path(__file__).resolve().parent
os.chdir(PROJECT_DIR)


def _reconfigure_text_stream(stream: object) -> None:
    reconfigure = getattr(stream, 'reconfigure', None)
    if callable(reconfigure):
        reconfigure(encoding='utf-8')


_reconfigure_text_stream(sys.stdout)
_reconfigure_text_stream(sys.stderr)

LOG_DIR = PROJECT_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"dws_v2_shadow_schedule_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

CHAIN_KEY = 'dws_v2_shadow'
CHAIN_LABEL = 'DWS v2 Shadow'
ADS_INVENTORY_HEALTH_SALES_DAYS_BACK = 31
DEFAULT_SALES_SHADOW_DAYS_BACK = max(
    DWS_SALES_MAINLINE_DAYS_BACK,
    ADS_INVENTORY_HEALTH_SALES_DAYS_BACK,
)
DEFAULT_INVENTORY_RAW_DAYS_BACK = 1
DEFAULT_CHUNK_SIZE = 1000
DEFAULT_RECONCILIATION_LIMIT = 20
SCHEDULE_LOCK_NAME = 'hefang_dw:scheduled_dws_v2_shadow'
INVENTORY_OLD_ALIGNMENT_SOURCE_TABLE = 'ods_fa_storage'
INVENTORY_OLD_ALIGNMENT_TARGET_TABLE = 'dws_inventory_daily'
INVENTORY_ADS_GATE_VALIDATION_BASIS_KEY = 'current_ods_and_dwd_baseline'
INVENTORY_ADS_GATE_VALIDATION_BASIS_DESCRIPTION = (
    'ods_fa_storage + dwd_inventory_storage_snapshot -> dws_inventory_daily_v2'
)


class ShadowScheduleError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool, report: dict[str, Any] | None = None):
        super().__init__(message)
        self.retryable = retryable
        self.report = report or {}


def _format_datetime_value(value: object) -> str:
    if value is None:
        return '-'
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _parse_datetime_arg(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _build_sales_window(
    sales_start_date: int | None,
    sales_end_date: int | None,
    sales_days_back: int,
) -> tuple[int, int]:
    if (sales_start_date is None) != (sales_end_date is None):
        raise ValueError('销售窗口请同时传入 --sales-start-date 与 --sales-end-date，或只传 --sales-days-back。')
    if sales_start_date is not None and sales_end_date is not None:
        if sales_start_date > sales_end_date:
            raise ValueError('销售窗口 start_date 不能晚于 end_date。')
        return sales_start_date, sales_end_date

    if sales_days_back <= 0:
        raise ValueError('--sales-days-back 必须大于 0。')
    sales_end = date.today()
    sales_start = sales_end - timedelta(days=sales_days_back - 1)
    return int(sales_start.strftime('%Y%m%d')), int(sales_end.strftime('%Y%m%d'))


def _build_inventory_window(
    inventory_start_time: datetime | None,
    inventory_end_time: datetime | None,
    inventory_raw_days_back: int,
) -> tuple[datetime, datetime]:
    if (inventory_start_time is None) != (inventory_end_time is None):
        raise ValueError('库存 raw 窗口请同时传入 --inventory-start-time 与 --inventory-end-time，或只传 --inventory-raw-days-back。')
    if inventory_start_time is not None and inventory_end_time is not None:
        if inventory_start_time >= inventory_end_time:
            raise ValueError('库存 raw 窗口 start_time 必须早于 end_time。')
        return inventory_start_time, inventory_end_time

    if inventory_raw_days_back <= 0:
        raise ValueError('--inventory-raw-days-back 必须大于 0。')
    inventory_end = datetime.now()
    inventory_start = inventory_end - timedelta(days=inventory_raw_days_back)
    return inventory_start, inventory_end


def _count_inclusive_days(start_date: int, end_date: int) -> int:
    start = datetime.strptime(str(start_date), '%Y%m%d').date()
    end = datetime.strptime(str(end_date), '%Y%m%d').date()
    return (end - start).days + 1


def _resolve_sales_timeout_profile(sales_start_date: int, sales_end_date: int) -> str:
    sales_days_back = _count_inclusive_days(sales_start_date, sales_end_date)
    if sales_days_back > DWS_SALES_MAINLINE_DAYS_BACK:
        return 'long_running'
    return 'etl'


def _message_contains_any(message: str, keywords: list[str] | tuple[str, ...]) -> bool:
    lowered = message.lower()
    return any(keyword.lower() in lowered for keyword in keywords)


def _is_retryable_message(message: str) -> bool:
    if not message:
        return True
    if _message_contains_any(message, ETL_NON_RETRYABLE_ERROR_KEYWORDS):
        return False
    if ETL_RETRYABLE_ERROR_KEYWORDS and _message_contains_any(message, ETL_RETRYABLE_ERROR_KEYWORDS):
        return True
    return True


def _connect_lock_db():
    return connect_mysql(
        timeout_profile='etl',
        cursorclass=DictCursor,
        autocommit=True,
    )


def _acquire_singleton_lock(lock_name: str, timeout_seconds: int = 0) -> tuple[object, int | None]:
    lock_conn = _connect_lock_db()
    try:
        with lock_conn.cursor() as cursor:
            cursor.execute(
                'SELECT CONNECTION_ID() AS connection_id, GET_LOCK(%s, %s) AS got_lock',
                (lock_name, timeout_seconds),
            )
            row = cursor.fetchone() or {}
    except Exception:
        lock_conn.close()
        raise

    connection_id = row.get('connection_id')
    got_lock = row.get('got_lock')
    if got_lock != 1:
        lock_conn.close()
        raise ShadowScheduleError(
            f'已有其他 DWS v2 shadow-run 实例在运行，请勿并发重复触发: lock_name={lock_name}',
            retryable=False,
        )
    return lock_conn, connection_id


def _release_singleton_lock(lock_conn, lock_name: str) -> None:
    if lock_conn is None:
        return

    try:
        with lock_conn.cursor() as cursor:
            cursor.execute('SELECT RELEASE_LOCK(%s) AS released_lock', (lock_name,))
    except Exception as exc:
        logger.warning('释放 DWS v2 shadow-run 单实例锁失败: lock_name=%s, error=%s', lock_name, exc)
    finally:
        lock_conn.close()


def _append_step_result(
    report: dict[str, Any],
    *,
    key: str,
    label: str,
    started_at: datetime,
    ended_at: datetime,
    status: str,
    summary: dict[str, Any],
) -> None:
    report.setdefault('steps', []).append(
        {
            'key': key,
            'label': label,
            'status': status,
            'started_at': started_at,
            'ended_at': ended_at,
            'duration_seconds': round((ended_at - started_at).total_seconds(), 3),
            'summary': summary,
        }
    )


def _build_inventory_old_alignment_probe_sql(snapshot_date: int) -> str:
    return f"""
SELECT
    COUNT(*) AS old_dws_row_count,
    COUNT(DISTINCT etl_time) AS old_dws_distinct_etl_time_count,
    MIN(etl_time) AS old_dws_min_etl_time,
    MAX(etl_time) AS old_dws_max_etl_time,
    SUM(COALESCE(qty, 0)) AS old_dws_qty,
    SUM(COALESCE(qtypurchaserem, 0)) AS old_dws_qtypurchaserem
FROM {INVENTORY_OLD_ALIGNMENT_TARGET_TABLE}
WHERE date_id = {int(snapshot_date)}
""".strip()


def _build_inventory_old_alignment_source_summary_sql(snapshot_date: int) -> str:
    return f"""
WITH ods_scope AS (
    SELECT
        {int(snapshot_date)} AS date_id,
        fs.c_store_id AS store_id,
        fs.m_product_id AS product_id,
        fs.m_productalias_id AS m_productalias_id,
        SUM(COALESCE(fs.qty, 0)) AS qty,
        SUM(COALESCE(fs.qty, 0)) AS qty_valid,
        0 AS qty_occupy,
        SUM(COALESCE(fs.qtypurchaserem, 0)) AS qtypurchaserem
    FROM {INVENTORY_OLD_ALIGNMENT_SOURCE_TABLE} fs
    LEFT JOIN dim_store s
      ON fs.c_store_id = s.store_id
    WHERE fs.isactive = 'Y'
      AND fs.m_productalias_id IS NOT NULL
      AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
    GROUP BY fs.c_store_id, fs.m_product_id, fs.m_productalias_id
)
SELECT
    COUNT(*) AS ods_scope_row_count,
    SUM(COALESCE(qty, 0)) AS ods_qty,
    SUM(COALESCE(qtypurchaserem, 0)) AS ods_qtypurchaserem
FROM ods_scope
""".strip()


def _build_inventory_old_alignment_detail_sql(snapshot_date: int, *, limit: int | None = None) -> str:
    limit_clause = f'\nLIMIT {int(limit)}' if limit is not None else ''
    return f"""
WITH ods_scope AS (
    SELECT
        {int(snapshot_date)} AS date_id,
        fs.c_store_id AS store_id,
        fs.m_product_id AS product_id,
        fs.m_productalias_id AS m_productalias_id,
        SUM(COALESCE(fs.qty, 0)) AS qty,
        SUM(COALESCE(fs.qty, 0)) AS qty_valid,
        0 AS qty_occupy,
        SUM(COALESCE(fs.qtypurchaserem, 0)) AS qtypurchaserem
    FROM {INVENTORY_OLD_ALIGNMENT_SOURCE_TABLE} fs
    LEFT JOIN dim_store s
      ON fs.c_store_id = s.store_id
    WHERE fs.isactive = 'Y'
      AND fs.m_productalias_id IS NOT NULL
      AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
    GROUP BY fs.c_store_id, fs.m_product_id, fs.m_productalias_id
), old_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty_valid, 0)) AS qty_valid,
        SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
        SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem
    FROM {INVENTORY_OLD_ALIGNMENT_TARGET_TABLE}
    WHERE date_id = {int(snapshot_date)}
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'ODS' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM ods_scope
    UNION ALL
    SELECT 'OLD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM old_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'ODS' THEN qty ELSE 0 END) AS ods_qty,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty ELSE 0 END) AS old_qty,
    SUM(CASE WHEN source_layer = 'ODS' THEN qty_valid ELSE 0 END) AS ods_qty_valid,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_valid ELSE 0 END) AS old_qty_valid,
    SUM(CASE WHEN source_layer = 'ODS' THEN qty_occupy ELSE 0 END) AS ods_qty_occupy,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_occupy ELSE 0 END) AS old_qty_occupy,
    SUM(CASE WHEN source_layer = 'ODS' THEN qtypurchaserem ELSE 0 END) AS ods_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'OLD' THEN qtypurchaserem ELSE 0 END) AS old_qtypurchaserem
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(ods_qty - old_qty) > 0.0001
    OR ABS(ods_qty_valid - old_qty_valid) > 0.0001
    OR ABS(ods_qty_occupy - old_qty_occupy) > 0.0001
    OR ABS(ods_qtypurchaserem - old_qtypurchaserem) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id{limit_clause}
""".strip()


def _build_inventory_old_alignment_count_sql(snapshot_date: int) -> str:
    return f"""
SELECT COUNT(*) AS mismatch_count
FROM (
    {_build_inventory_old_alignment_detail_sql(snapshot_date)}
) mismatch_scope
""".strip()


def _run_inventory_old_alignment_baseline(
    snapshot_date: int,
    reconciliation_limit: int,
    *,
    allow_missing_old_dws_snapshot: bool = False,
) -> dict[str, Any]:
    conn = connect_mysql(
        timeout_profile='etl',
        cursorclass=DictCursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(_build_inventory_old_alignment_probe_sql(snapshot_date))
            old_dws_probe = cursor.fetchone() or {}

            cursor.execute(_build_inventory_old_alignment_source_summary_sql(snapshot_date))
            source_summary = cursor.fetchone() or {}

            cursor.execute(_build_inventory_old_alignment_count_sql(snapshot_date))
            mismatch_row = cursor.fetchone() or {}
            mismatch_count = int(mismatch_row.get('mismatch_count') or 0)

            sample_mismatches: list[dict[str, Any]] = []
            if mismatch_count > 0:
                cursor.execute(
                    _build_inventory_old_alignment_detail_sql(
                        snapshot_date,
                        limit=reconciliation_limit,
                    )
                )
                sample_mismatches = list(cursor.fetchall() or [])
    finally:
        conn.close()

    old_dws_row_count = int(old_dws_probe.get('old_dws_row_count') or 0)
    if old_dws_row_count == 0:
        status = 'SKIPPED' if allow_missing_old_dws_snapshot else 'WARNING'
        reason = (
            'old_dws_snapshot_not_ready_in_pre_refresh'
            if allow_missing_old_dws_snapshot
            else 'old_dws_snapshot_missing'
        )
    elif mismatch_count == 0:
        status = 'SUCCESS'
        reason = 'matched_current_ods'
    else:
        status = 'WARNING'
        reason = 'current_ods_baseline_mismatch'

    return {
        'status': status,
        'reason': reason,
        'snapshot_date': snapshot_date,
        'compare_source': INVENTORY_OLD_ALIGNMENT_SOURCE_TABLE,
        'old_dws_table': INVENTORY_OLD_ALIGNMENT_TARGET_TABLE,
        'mismatch_count': mismatch_count,
        'sample_limit': reconciliation_limit,
        'sample_mismatches': sample_mismatches,
        'old_dws_probe': old_dws_probe,
        'source_summary': source_summary,
    }


def _build_inventory_ads_gate_validation(report: dict[str, Any]) -> dict[str, Any]:
    validation = {
        'basis_key': INVENTORY_ADS_GATE_VALIDATION_BASIS_KEY,
        'basis_description': INVENTORY_ADS_GATE_VALIDATION_BASIS_DESCRIPTION,
        'status': 'PENDING',
        'gate_ready': False,
        'reason': 'execute_only',
    }
    if report.get('mode') != 'execute':
        return validation

    inventory_alignment = report.get('inventory_alignment') or {}
    skip_ads_shadow_validation = bool(report.get('skip_ads_shadow_validation'))
    same_snapshot_requested = bool(
        inventory_alignment.get('align_with_old_dws')
        or inventory_alignment.get('source_loaded_at_cutoff')
    )
    inventory_report = next(
        (step.get('summary') or {} for step in report.get('steps', []) if step.get('key') == 'dws_inventory_v2'),
        {},
    )
    inventory_old_alignment_report = next(
        (
            step.get('summary') or {}
            for step in report.get('steps', [])
            if step.get('key') == 'inventory_old_dws_comparable_alignment'
        ),
        {},
    )

    baseline_status = str(inventory_old_alignment_report.get('status', 'UNKNOWN')).upper()
    baseline_reason = str(inventory_old_alignment_report.get('reason') or '')
    inventory_status = str(inventory_report.get('status', 'UNKNOWN')).upper()
    baseline_mismatch_count = int(inventory_old_alignment_report.get('mismatch_count') or 0)
    dwd_to_v2_mismatch_count = int(
        ((inventory_report.get('reconciliation') or {}).get('mismatch_count')) or 0
    )
    inventory_error = str(inventory_report.get('error') or '')

    validation.update(
        {
            'same_snapshot_requested': same_snapshot_requested,
            'compare_source': inventory_old_alignment_report.get('compare_source', '-'),
            'current_ods_baseline_status': baseline_status,
            'current_ods_baseline_mismatch_count': baseline_mismatch_count,
            'current_ods_baseline_reason': baseline_reason,
            'dwd_to_v2_status': inventory_status,
            'dwd_to_v2_mismatch_count': dwd_to_v2_mismatch_count,
        }
    )

    if (
        skip_ads_shadow_validation
        and not same_snapshot_requested
        and baseline_status == 'SKIPPED'
        and baseline_reason == 'old_dws_snapshot_not_ready_in_pre_refresh'
    ):
        if inventory_status == 'SUCCESS' and dwd_to_v2_mismatch_count == 0:
            validation['status'] = 'READY'
            validation['gate_ready'] = True
            validation['reason'] = 'pre_refresh_dwd_to_v2_passed_without_old_dws_snapshot'
            return validation

        if inventory_status == 'SUCCESS':
            validation['status'] = 'BLOCKED'
            validation['reason'] = 'dwd_to_v2_mismatch'
            return validation

        validation['status'] = 'BLOCKED'
        validation['reason'] = 'dwd_to_v2_step_failed'
        return validation

    if baseline_status != 'SUCCESS':
        validation['status'] = 'BLOCKED'
        validation['reason'] = baseline_reason or 'current_ods_baseline_mismatch'
        return validation

    if inventory_status == 'SUCCESS' and dwd_to_v2_mismatch_count == 0:
        validation['status'] = 'READY'
        validation['gate_ready'] = True
        validation['reason'] = 'current_ods_baseline_and_dwd_to_v2_passed'
        return validation

    if inventory_status == 'SUCCESS':
        validation['status'] = 'BLOCKED'
        validation['reason'] = 'dwd_to_v2_mismatch'
        return validation

    if same_snapshot_requested and 'same-snapshot cutoff 无法复原历史快照' in inventory_error:
        validation['status'] = 'BLOCKED'
        validation['reason'] = 'same_snapshot_diagnostic_failed'
        return validation

    validation['status'] = 'BLOCKED'
    validation['reason'] = 'dwd_to_v2_step_failed'
    return validation


def _execute_step(
    report: dict[str, Any],
    *,
    key: str,
    label: str,
    runner,
    status_getter=None,
    fail_chain_on_error: bool = True,
) -> dict[str, Any]:
    started_at = datetime.now()
    logger.info('开始执行 shadow 步骤: %s', label)
    try:
        summary = runner() or {}
        status = status_getter(summary) if callable(status_getter) else 'SUCCESS'
        ended_at = datetime.now()
        _append_step_result(
            report,
            key=key,
            label=label,
            started_at=started_at,
            ended_at=ended_at,
            status=status,
            summary=summary,
        )
        logger.info('shadow 步骤完成: %s, status=%s', label, status)
        return summary
    except Exception as exc:
        ended_at = datetime.now()
        status = 'FAILED' if fail_chain_on_error else 'WARNING'
        _append_step_result(
            report,
            key=key,
            label=label,
            started_at=started_at,
            ended_at=ended_at,
            status=status,
            summary={'status': status, 'error': repr(exc)},
        )
        if fail_chain_on_error:
            logger.error('shadow 步骤失败: %s, error=%s', label, exc, exc_info=True)
            report['finished_at'] = ended_at
            raise ShadowScheduleError(
                f'{label} 失败: {exc}',
                retryable=_is_retryable_message(str(exc)),
                report=report,
            ) from exc

        logger.warning('shadow 观察步骤异常，已降级为 WARNING: %s, error=%s', label, exc, exc_info=True)
        return {'status': 'WARNING', 'error': repr(exc)}


def _get_dws_step_status(summary: dict[str, Any]) -> str:
    status = str(summary.get('status', 'SUCCESS')).upper()
    if status in {'SUCCESS', 'SKIPPED'}:
        return status
    return 'WARNING'


def _build_step_detail_line(step: dict[str, Any]) -> str:
    key = step.get('key')
    summary = step.get('summary') or {}
    duration_seconds = step.get('duration_seconds')
    suffix = '' if duration_seconds is None else f' [{duration_seconds}s]'
    if key == 'ods_m_retail_raw':
        return f"零售单头 raw：rows={summary.get('row_count', '-')}, mode={summary.get('mode', '-')}, timeout={summary.get('timeout_profile', '-')}{suffix}"
    if key == 'ods_m_retailitem_raw':
        return f"零售明细 raw：rows={summary.get('row_count', '-')}, mode={summary.get('mode', '-')}, timeout={summary.get('timeout_profile', '-')}{suffix}"
    if key == 'dwd_sales_retail_item':
        return (
            '销售 DWD：'
            f"source_rows={summary.get('source_rows', '-')}, "
            f"affected_rows={summary.get('affected_rows', '-')}, "
            f"target_window_rows={summary.get('target_window_rows', '-')}{suffix}"
        )
    if key == 'dws_sales_v2':
        reconciliation = summary.get('reconciliation') or {}
        return (
            '销售 DWS v2：'
            f"status={summary.get('status', '-')}, "
            f"insert_rowcount={summary.get('insert_rowcount', '-')}, "
            f"mismatch_count={reconciliation.get('mismatch_count', '-')}, "
            f"report={summary.get('output_json', '-')}{suffix}"
        )
    if key == 'ods_fa_storage_raw':
        return f"库存 raw：rows={summary.get('row_count', '-')}, mode={summary.get('mode', '-')}, timeout={summary.get('timeout_profile', '-')}{suffix}"
    if key == 'dwd_inventory_storage_snapshot':
        return (
            '库存 DWD：'
            f"source_rows={summary.get('source_rows', '-')}, "
            f"affected_rows={summary.get('affected_rows', '-')}, "
            f"target_snapshot_rows={summary.get('target_snapshot_rows', '-')}{suffix}"
        )
    if key == 'dws_inventory_v2':
        reconciliation = summary.get('reconciliation') or {}
        return (
            '库存 DWS v2：'
            f"status={summary.get('status', '-')}, "
            f"delete_rowcount={summary.get('delete_rowcount', '-')}, "
            f"insert_rowcount={summary.get('insert_rowcount', '-')}, "
            f"dwd_mismatch_count={reconciliation.get('mismatch_count', '-')}, "
            f"report={summary.get('output_json', '-')}{suffix}"
        )
    if key == 'inventory_old_dws_comparable_alignment':
        old_dws_probe = summary.get('old_dws_probe') or {}
        return (
            '库存当前 ODS 基线：'
            f"status={summary.get('status', '-')}, "
            f"mismatch_count={summary.get('mismatch_count', '-')}, "
            f"compare_source={summary.get('compare_source', '-')}, "
            f"old_dws_max_etl_time={old_dws_probe.get('old_dws_max_etl_time', '-')}{suffix}"
        )
    if key == 'ads_inventory_health_shadow_validation':
        return (
            'ADS 结果对账：'
            f"status={summary.get('status', '-')}, "
            f"mismatch_count={summary.get('mismatch_count', '-')}, "
            f"baseline_table={summary.get('baseline_table', '-')}, "
            f"shadow_inventory_table={summary.get('shadow_inventory_table', '-')}, "
            f"shadow_sales_table={summary.get('shadow_sales_table', '-')}{suffix}"
        )
    return f"{step.get('label', key)}：status={step.get('status', '-')}{suffix}"


def _build_chain_summary_payload(
    report: dict[str, Any],
    *,
    attempt: int,
    max_retries: int,
) -> dict[str, Any]:
    started_at = report.get('started_at') or datetime.now()
    ended_at = report.get('finished_at') or datetime.now()
    sales_window = report.get('sales_window') or {}
    inventory_window = report.get('inventory_raw_window') or {}
    summary_lines = [
        (
            '动作：M3 raw / DWD 与 DWS v2 只读连通检查通过'
            if report.get('mode') == 'conn_test'
            else '动作：已执行 M3 raw / DWD 刷新并写入 DWS v2 shadow 表'
        ),
        (
            '销售窗口：'
            f"{sales_window.get('start_date', '-')} ~ {sales_window.get('end_date', '-')}"
        ),
        (
            '库存窗口：'
            f"{inventory_window.get('start_time', '-')} ~ {inventory_window.get('end_time', '-')}"
        ),
        f"库存快照：{report.get('snapshot_date', '-')}",
        f'尝试：{attempt}/{max_retries}',
    ]
    if sales_window:
        sales_days_back = sales_window.get('days_back', '-')
        required_days_back = sales_window.get('ads_inventory_health_required_days_back')
        covers_ads_gate = sales_window.get('covers_ads_inventory_health')
        sales_timeout_profile = sales_window.get('timeout_profile', '-')
        summary_lines.insert(
            2,
            f'销售超时档位：{sales_timeout_profile}',
        )
        if required_days_back is not None and covers_ads_gate is not None:
            gate_status = '已覆盖' if covers_ads_gate else '未覆盖'
            summary_lines.insert(
                3,
                f'ADS 销售门：{gate_status}（当前 {sales_days_back} 天，目标 {required_days_back} 天）',
            )
    detail_lines = [_build_step_detail_line(step) for step in report.get('steps', [])]
    issue_lines: list[str] = []

    sales_report = next(
        (step.get('summary') for step in report.get('steps', []) if step.get('key') == 'dws_sales_v2'),
        {},
    )
    inventory_report = next(
        (step.get('summary') for step in report.get('steps', []) if step.get('key') == 'dws_inventory_v2'),
        {},
    )
    inventory_old_alignment_report = next(
        (
            step.get('summary')
            for step in report.get('steps', [])
            if step.get('key') == 'inventory_old_dws_comparable_alignment'
        ),
        {},
    )
    ads_inventory_health_validation_report = next(
        (
            step.get('summary')
            for step in report.get('steps', [])
            if step.get('key') == 'ads_inventory_health_shadow_validation'
        ),
        {},
    )
    inventory_ads_gate_validation = (
        report.get('inventory_ads_gate_validation')
        or _build_inventory_ads_gate_validation(report)
    )
    if report.get('mode') == 'execute':
        sales_mismatch_count = ((sales_report.get('reconciliation') or {}).get('mismatch_count'))
        inventory_mismatch_count = ((inventory_report.get('reconciliation') or {}).get('mismatch_count'))
        inventory_old_alignment_count = inventory_old_alignment_report.get('mismatch_count')
        summary_lines.insert(
            3,
            '销售对账：'
            f"status={sales_report.get('status', '-')}, mismatch_count={sales_mismatch_count if sales_mismatch_count is not None else '-'}",
        )
        summary_lines.insert(
            4,
            '库存 DWD→v2 对账：'
            f"status={inventory_report.get('status', '-')}, mismatch_count={inventory_mismatch_count if inventory_mismatch_count is not None else '-'}",
        )
        if inventory_old_alignment_report:
            old_dws_probe = inventory_old_alignment_report.get('old_dws_probe') or {}
            summary_lines.insert(
                5,
                '库存当前 ODS 基线：'
                f"status={inventory_old_alignment_report.get('status', '-')}, mismatch_count={inventory_old_alignment_count if inventory_old_alignment_count is not None else '-'}, "
                f"compare_source={inventory_old_alignment_report.get('compare_source', '-')}",
            )
            detail_lines.append(
                '库存当前 ODS 基线：'
                f"compare_source={inventory_old_alignment_report.get('compare_source', '-')}, "
                f"mismatch_count={inventory_old_alignment_count if inventory_old_alignment_count is not None else '-'}, "
                f"old_dws_max_etl_time={old_dws_probe.get('old_dws_max_etl_time', '-')}"
            )
        summary_lines.insert(
            6,
            '库存 ADS 门：'
            f"status={inventory_ads_gate_validation.get('status', '-')}, basis={inventory_ads_gate_validation.get('basis_key', '-')}",
        )
        detail_lines.append(
            '库存 ADS 门：'
            f"basis={inventory_ads_gate_validation.get('basis_description', '-')}, "
            f"compare_source={inventory_ads_gate_validation.get('compare_source', '-')}, "
            f"current_ods_baseline_status={inventory_ads_gate_validation.get('current_ods_baseline_status', '-')}, "
            f"dwd_to_v2_status={inventory_ads_gate_validation.get('dwd_to_v2_status', '-')}, "
            f"dwd_to_v2_mismatch_count={inventory_ads_gate_validation.get('dwd_to_v2_mismatch_count', '-')}, "
            f"reason={inventory_ads_gate_validation.get('reason', '-')}, "
            f"same_snapshot_requested={inventory_ads_gate_validation.get('same_snapshot_requested', False)}"
        )
        if ads_inventory_health_validation_report:
            summary_lines.insert(
                7,
                'ADS 结果对账：'
                f"status={ads_inventory_health_validation_report.get('status', '-')}, mismatch_count={ads_inventory_health_validation_report.get('mismatch_count', '-')}",
            )
            detail_lines.append(
                'ADS 结果对账：'
                f"baseline_table={ads_inventory_health_validation_report.get('baseline_table', '-')}, "
                f"shadow_inventory_table={ads_inventory_health_validation_report.get('shadow_inventory_table', '-')}, "
                f"shadow_sales_table={ads_inventory_health_validation_report.get('shadow_sales_table', '-')}, "
                f"baseline_row_count={ads_inventory_health_validation_report.get('baseline_row_count', '-')}, "
                f"shadow_row_count={ads_inventory_health_validation_report.get('shadow_row_count', '-')}, "
                f"mismatch_count={ads_inventory_health_validation_report.get('mismatch_count', '-')}, "
                f"reason={ads_inventory_health_validation_report.get('reason', '-')}, "
                f"dabo_source_mode={ads_inventory_health_validation_report.get('dabo_source_mode', '-')}",
            )
    else:
        detail_lines.append('库存当前 ODS 基线检查仅在 execute 模式下校验。')

    for step in report.get('steps', []):
        if step.get('status') == 'WARNING':
            issue_lines.append(f"- {step.get('label')} 返回 WARNING，请复核其运行证据与对账结果")
        if step.get('status') == 'FAILED':
            issue_lines.append(f"- {step.get('label')} 失败：{(step.get('summary') or {}).get('error', '-')}")
    if report.get('error'):
        issue_lines.append(f"- 本轮失败原因：{report.get('error')}")

    headline_map = {
        'SUCCESS': 'DWS v2 Shadow 调度完成',
        'WARNING': 'DWS v2 Shadow 调度告警',
        'FAILED': 'DWS v2 Shadow 调度失败',
        'ERROR': 'DWS v2 Shadow 调度异常',
    }
    return {
        'chain_key': CHAIN_KEY,
        'chain_label': CHAIN_LABEL,
        'status': report.get('status', 'UNKNOWN'),
        'headline': headline_map.get(report.get('status', 'UNKNOWN'), 'DWS v2 Shadow 调度摘要'),
        'started_at': started_at,
        'ended_at': ended_at,
        'duration_seconds': int((ended_at - started_at).total_seconds()),
        'summary_lines': summary_lines,
        'detail_lines': detail_lines,
        'issue_lines': issue_lines,
    }


def _compose_alert_message(payload: dict[str, Any]) -> str:
    lines = [f"何方珠宝 {payload.get('headline', 'DWS v2 Shadow 调度摘要')}"]
    lines.extend(str(line) for line in payload.get('summary_lines', []))
    detail_lines = payload.get('detail_lines') or []
    if detail_lines:
        lines.append('明细：')
        lines.extend(f'- {line}' for line in detail_lines)
    issue_lines = payload.get('issue_lines') or []
    if issue_lines:
        lines.append('异常/提示：')
        lines.extend(str(line) for line in issue_lines)
    return '\n'.join(lines)


def _send_alert_if_enabled(payload: dict[str, Any]) -> None:
    if should_suppress_child_wechat_alert():
        return
    send_wechat_alert(WECHAT_WEBHOOK, _compose_alert_message(payload))


def _build_execute_report(
    sales_start_date: int,
    sales_end_date: int,
    inventory_window_start: datetime,
    inventory_window_end: datetime,
    snapshot_date: int,
    *,
    inventory_source_loaded_at_cutoff: datetime | None,
    inventory_align_with_old_dws: bool,
    conn_test_only: bool,
    retail_chunk_size: int,
    retailitem_chunk_size: int,
    inventory_chunk_size: int,
    reconciliation_limit: int,
    skip_ads_shadow_validation: bool = False,
) -> dict[str, Any]:
    started_at = datetime.now()
    snapshot_dt = datetime.strptime(str(snapshot_date), '%Y%m%d')
    run_marker = started_at.strftime('%Y%m%d_%H%M%S')
    sales_days_back = _count_inclusive_days(sales_start_date, sales_end_date)
    sales_timeout_profile = _resolve_sales_timeout_profile(sales_start_date, sales_end_date)
    report: dict[str, Any] = {
        'script': 'scheduled_dws_v2_shadow.py',
        'mode': 'conn_test' if conn_test_only else 'execute',
        'sales_window': {
            'start_date': sales_start_date,
            'end_date': sales_end_date,
            'days_back': sales_days_back,
            'timeout_profile': sales_timeout_profile,
            'mainline_days_back': DWS_SALES_MAINLINE_DAYS_BACK,
            'ads_inventory_health_required_days_back': ADS_INVENTORY_HEALTH_SALES_DAYS_BACK,
            'covers_ads_inventory_health': sales_days_back >= ADS_INVENTORY_HEALTH_SALES_DAYS_BACK,
        },
        'inventory_raw_window': {
            'start_time': inventory_window_start,
            'end_time': inventory_window_end,
        },
        'inventory_alignment': {
            'align_with_old_dws': inventory_align_with_old_dws,
            'source_loaded_at_cutoff': inventory_source_loaded_at_cutoff,
        },
        'snapshot_date': snapshot_date,
        'skip_ads_shadow_validation': skip_ads_shadow_validation,
        'started_at': started_at,
        'steps': [],
        'status': 'STARTED',
    }

    sales_v2_output_json = f'reports/context_cache/dws_sales_v2_shadow_{run_marker}.json'
    inventory_v2_output_json = f'reports/context_cache/dws_inventory_v2_shadow_{run_marker}.json'

    if conn_test_only:
        _execute_step(
            report,
            key='ods_m_retail_raw',
            label='销售 raw 单头 conn-test',
            runner=lambda: (
                ods_retail_conn_test(sales_timeout_profile)
                or {'mode': 'business-date', 'timeout_profile': sales_timeout_profile}
            ),
        )
        _execute_step(
            report,
            key='ods_m_retailitem_raw',
            label='销售 raw 明细 conn-test',
            runner=lambda: (
                ods_retailitem_conn_test(sales_timeout_profile)
                or {'mode': 'business-date', 'timeout_profile': sales_timeout_profile}
            ),
        )
        _execute_step(
            report,
            key='dwd_sales_retail_item',
            label='销售 DWD conn-test',
            runner=lambda: (
                dwd_sales_conn_test(sales_timeout_profile)
                or {'timeout_profile': sales_timeout_profile}
            ),
        )
        _execute_step(
            report,
            key='dws_sales_v2',
            label='销售 DWS v2 conn-test',
            runner=lambda: (
                dws_sales_v2_conn_test(
                    DwsSalesV2DryRunConfig(
                        start_date=sales_start_date,
                        end_date=sales_end_date,
                        timeout_profile=sales_timeout_profile,
                    )
                )
                or {'status': 'SUCCESS', 'timeout_profile': sales_timeout_profile}
            ),
            status_getter=_get_dws_step_status,
        )
        _execute_step(
            report,
            key='ods_fa_storage_raw',
            label='库存 raw conn-test',
            runner=lambda: (
                ods_fa_storage_conn_test('long_running')
                or {'mode': 'modified-window', 'timeout_profile': 'long_running'}
            ),
        )
        _execute_step(
            report,
            key='dwd_inventory_storage_snapshot',
            label='库存 DWD conn-test',
            runner=lambda: (dwd_inventory_conn_test('long_running') or {'timeout_profile': 'long_running'}),
        )
        _execute_step(
            report,
            key='dws_inventory_v2',
            label='库存 DWS v2 conn-test',
            runner=lambda: (
                dws_inventory_v2_conn_test(
                    DwsInventoryV2DryRunConfig(
                        snapshot_date=snapshot_date,
                        source_loaded_at_cutoff=inventory_source_loaded_at_cutoff,
                        align_with_old_dws=inventory_align_with_old_dws,
                        timeout_profile='long_running',
                    )
                )
                or {
                    'status': 'SUCCESS',
                    'timeout_profile': 'long_running',
                    'align_with_old_dws': inventory_align_with_old_dws,
                    'source_loaded_at_cutoff': _format_datetime_value(inventory_source_loaded_at_cutoff),
                    'alignment_check': 'skipped_in_conn_test',
                }
            ),
            status_getter=_get_dws_step_status,
        )
    else:
        _execute_step(
            report,
            key='ods_m_retail_raw',
            label='销售 raw 单头 business-date 装载',
            runner=lambda: {
                'row_count': ods_retail_execute_load(
                    OdsRetailRawConfig(
                        mode='business-date',
                        start_date=sales_start_date,
                        end_date=sales_end_date,
                        timeout_profile=sales_timeout_profile,
                    ),
                    chunk_size=retail_chunk_size,
                ),
                'mode': 'business-date',
                'timeout_profile': sales_timeout_profile,
            },
        )
        _execute_step(
            report,
            key='ods_m_retailitem_raw',
            label='销售 raw 明细 business-date 装载',
            runner=lambda: {
                'row_count': ods_retailitem_execute_load(
                    OdsRetailItemRawConfig(
                        mode='business-date',
                        start_date=sales_start_date,
                        end_date=sales_end_date,
                        timeout_profile=sales_timeout_profile,
                    ),
                    chunk_size=retailitem_chunk_size,
                ),
                'mode': 'business-date',
                'timeout_profile': sales_timeout_profile,
            },
        )
        _execute_step(
            report,
            key='dwd_sales_retail_item',
            label='销售 DWD 近窗重算',
            runner=lambda: {
                **dwd_sales_execute_load(
                    DwdSalesRetailItemConfig(
                        start_date=sales_start_date,
                        end_date=sales_end_date,
                        timeout_profile=sales_timeout_profile,
                    )
                ),
                'timeout_profile': sales_timeout_profile,
            },
        )
        _execute_step(
            report,
            key='dws_sales_v2',
            label='销售 DWS v2 shadow 写入',
            runner=lambda: dws_sales_v2_execute_load(
                DwsSalesV2DryRunConfig(
                    start_date=sales_start_date,
                    end_date=sales_end_date,
                    timeout_profile=sales_timeout_profile,
                    load_batch_id=f'DWS_SALES_V2_S4_SHADOW_{run_marker}',
                    validation_note='S4 shadow run via scheduled_dws_v2_shadow',
                ),
                confirm_write=DWS_SALES_WRITE_CONFIRMATION_TOKEN,
                output_json=sales_v2_output_json,
                reconciliation_limit=reconciliation_limit,
            ),
            status_getter=_get_dws_step_status,
        )
        _execute_step(
            report,
            key='inventory_old_dws_comparable_alignment',
            label='库存 old DWS 可比基线检查',
            runner=lambda: _run_inventory_old_alignment_baseline(
                snapshot_date,
                reconciliation_limit,
                allow_missing_old_dws_snapshot=(
                    skip_ads_shadow_validation
                    and not inventory_align_with_old_dws
                    and inventory_source_loaded_at_cutoff is None
                ),
            ),
            status_getter=_get_dws_step_status,
            fail_chain_on_error=False,
        )
        _execute_step(
            report,
            key='ods_fa_storage_raw',
            label='库存 raw modified-window 装载',
            runner=lambda: {
                'row_count': ods_fa_storage_execute_load(
                    OdsFaStorageRawConfig(
                        mode='modified-window',
                        start_time=inventory_window_start,
                        end_time=inventory_window_end,
                        timeout_profile='long_running',
                    ),
                    chunk_size=inventory_chunk_size,
                ),
                'mode': 'modified-window',
                'timeout_profile': 'long_running',
            },
        )
        _execute_step(
            report,
            key='dwd_inventory_storage_snapshot',
            label='库存 DWD 快照重算',
            runner=lambda: {
                **dwd_inventory_execute_load(
                    DwdInventoryStorageSnapshotConfig(
                        snapshot_date=snapshot_date,
                        timeout_profile='long_running',
                    )
                ),
                'timeout_profile': 'long_running',
            },
        )
        _execute_step(
            report,
            key='dws_inventory_v2',
            label='库存 DWS v2 shadow 写入',
            runner=lambda: dws_inventory_v2_execute_load(
                DwsInventoryV2DryRunConfig(
                    snapshot_date=snapshot_date,
                    source_loaded_at_cutoff=inventory_source_loaded_at_cutoff,
                    align_with_old_dws=inventory_align_with_old_dws,
                    timeout_profile='long_running',
                    load_batch_id=f'DWS_INVENTORY_V2_S4_SHADOW_{run_marker}',
                    validation_note='S4 shadow run via scheduled_dws_v2_shadow',
                ),
                confirm_write=DWS_INVENTORY_WRITE_CONFIRMATION_TOKEN,
                output_json=inventory_v2_output_json,
                reconciliation_limit=reconciliation_limit,
            ),
            status_getter=_get_dws_step_status,
        )
        if skip_ads_shadow_validation:
            now = datetime.now()
            _append_step_result(
                report,
                key='ads_inventory_health_shadow_validation',
                label='ads_inventory_health 影子链结果对账',
                started_at=now,
                ended_at=now,
                status='SKIPPED',
                summary={
                    'status': 'SKIPPED',
                    'reason': 'skip_ads_shadow_validation',
                    'note': '本轮仅用于生产 V2 主链前置刷新 DWS v2 读源，ADS 持久化结果将在主链重算后再具备可比性。',
                },
            )
            logger.info('跳过 ads_inventory_health 影子链结果对账：本轮仅做 V2 读源预刷新')
        else:
            _execute_step(
                report,
                key='ads_inventory_health_shadow_validation',
                label='ads_inventory_health 影子链结果对账',
                runner=lambda: validate_inventory_health_shadow_against_persisted(
                    snapshot_dt=snapshot_dt,
                    inventory_table='dws_inventory_daily_v2',
                    sales_table='dws_sales_daily_v2',
                    sample_limit=reconciliation_limit,
                ),
                status_getter=_get_dws_step_status,
                fail_chain_on_error=False,
            )

    report['inventory_ads_gate_validation'] = _build_inventory_ads_gate_validation(report)
    warning_steps = [step for step in report.get('steps', []) if step.get('status') == 'WARNING']
    report['status'] = 'WARNING' if warning_steps else 'SUCCESS'
    report['finished_at'] = datetime.now()
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='DWS v2 并行 shadow-run 调度入口')
    parser.add_argument('--conn-test', action='store_true', help='只做 raw / DWD / DWS v2 的连通性与结构检查。')
    parser.add_argument(
        '--sales-days-back',
        type=int,
        default=DEFAULT_SALES_SHADOW_DAYS_BACK,
        help='销售 shadow-run 默认回算天数；默认取 max(主链7天回刷, ads_inventory_health 近30天窗口)=31 天。',
    )
    parser.add_argument('--sales-start-date', type=int, default=None, help='显式指定销售窗口开始日期 YYYYMMDD。')
    parser.add_argument('--sales-end-date', type=int, default=None, help='显式指定销售窗口结束日期 YYYYMMDD。')
    parser.add_argument('--inventory-raw-days-back', type=int, default=DEFAULT_INVENTORY_RAW_DAYS_BACK, help='库存 raw modified-window 回刷天数，默认 1 天。')
    parser.add_argument('--inventory-start-time', default=None, help='显式指定库存 raw 窗口开始时间，ISO 格式。')
    parser.add_argument('--inventory-end-time', default=None, help='显式指定库存 raw 窗口结束时间，ISO 格式。')
    inventory_alignment_group = parser.add_mutually_exclusive_group()
    inventory_alignment_group.add_argument(
        '--inventory-source-loaded-at-cutoff',
        default=None,
        help='库存 same snapshot 用的 source_loaded_at cutoff，ISO 格式；透传给 etl_dws_inventory_v2.py。',
    )
    inventory_alignment_group.add_argument(
        '--inventory-align-with-old-dws',
        action='store_true',
        help='库存 shadow 自动读取旧 dws_inventory_daily 当日 MAX(etl_time) 作为 same snapshot cutoff。',
    )
    parser.add_argument('--snapshot-date', type=int, default=None, help='库存 DWD / DWS v2 目标快照日期 YYYYMMDD，默认今天。')
    parser.add_argument('--retail-chunk-size', type=int, default=DEFAULT_CHUNK_SIZE, help='销售单头 raw 分批大小。')
    parser.add_argument('--retailitem-chunk-size', type=int, default=DEFAULT_CHUNK_SIZE, help='销售明细 raw 分批大小。')
    parser.add_argument('--inventory-chunk-size', type=int, default=DEFAULT_CHUNK_SIZE, help='库存 raw 分批大小。')
    parser.add_argument('--reconciliation-limit', type=int, default=DEFAULT_RECONCILIATION_LIMIT, help='DWS v2 对账样本输出行数。')
    parser.add_argument('--max-retries', type=int, default=ETL_DEFAULT_MAX_RETRIES, help='重试次数，默认取 config.py。')
    parser.add_argument('--retry-sleep', type=int, default=ETL_DEFAULT_RETRY_SLEEP, help='重试间隔秒数，默认取 config.py。')
    parser.add_argument('--output-json', default=None, help='整体 shadow-run 运行证据 JSON 输出路径；未传则写入 reports/context_cache。')
    parser.add_argument(
        '--skip-ads-shadow-validation',
        action='store_true',
        help='仅刷新 raw/DWD/DWS v2 读源，不对比已持久化 ads_inventory_health；用于总控 V2 主链前置刷新。',
    )
    return parser


def run_with_retries(args: argparse.Namespace) -> int:
    try:
        sales_start_date, sales_end_date = _build_sales_window(
            args.sales_start_date,
            args.sales_end_date,
            args.sales_days_back,
        )
        inventory_window_start, inventory_window_end = _build_inventory_window(
            _parse_datetime_arg(args.inventory_start_time),
            _parse_datetime_arg(args.inventory_end_time),
            args.inventory_raw_days_back,
        )
        inventory_source_loaded_at_cutoff = _parse_datetime_arg(args.inventory_source_loaded_at_cutoff)
        snapshot_date = args.snapshot_date or int(date.today().strftime('%Y%m%d'))
    except Exception as exc:
        logger.error('DWS v2 shadow-run 参数检查失败: %s', exc)
        failure_report = {
            'script': 'scheduled_dws_v2_shadow.py',
            'mode': 'conn_test' if args.conn_test else 'execute',
            'status': 'FAILED',
            'started_at': datetime.now(),
            'finished_at': datetime.now(),
            'steps': [],
            'error': repr(exc),
        }
        output_path = write_runtime_report(failure_report, 'scheduled_dws_v2_shadow', args.output_json)
        failure_report['output_json'] = str(output_path)
        payload = _build_chain_summary_payload(failure_report, attempt=1, max_retries=max(args.max_retries, 1))
        write_total_control_chain_summary(payload)
        _send_alert_if_enabled(payload)
        return 2

    try:
        lock_conn, lock_connection_id = _acquire_singleton_lock(SCHEDULE_LOCK_NAME)
    except ShadowScheduleError as exc:
        logger.error('获取 DWS v2 shadow-run 单实例锁失败: %s', exc)
        failure_report = exc.report or {
            'script': 'scheduled_dws_v2_shadow.py',
            'mode': 'conn_test' if args.conn_test else 'execute',
            'status': 'FAILED',
            'started_at': datetime.now(),
            'finished_at': datetime.now(),
            'steps': [],
            'error': repr(exc),
        }
        output_path = write_runtime_report(failure_report, 'scheduled_dws_v2_shadow', args.output_json)
        failure_report['output_json'] = str(output_path)
        payload = _build_chain_summary_payload(failure_report, attempt=1, max_retries=max(args.max_retries, 1))
        write_total_control_chain_summary(payload)
        _send_alert_if_enabled(payload)
        return 2

    logger.info(
        '已获取 DWS v2 shadow-run 单实例锁: lock_name=%s, connection_id=%s',
        SCHEDULE_LOCK_NAME,
        lock_connection_id,
    )

    try:
        for attempt in range(1, args.max_retries + 1):
            try:
                report = _build_execute_report(
                    sales_start_date,
                    sales_end_date,
                    inventory_window_start,
                    inventory_window_end,
                    snapshot_date,
                    inventory_source_loaded_at_cutoff=inventory_source_loaded_at_cutoff,
                    inventory_align_with_old_dws=args.inventory_align_with_old_dws,
                    conn_test_only=args.conn_test,
                    retail_chunk_size=args.retail_chunk_size,
                    retailitem_chunk_size=args.retailitem_chunk_size,
                    inventory_chunk_size=args.inventory_chunk_size,
                    reconciliation_limit=args.reconciliation_limit,
                    skip_ads_shadow_validation=args.skip_ads_shadow_validation,
                )
                output_path = write_runtime_report(report, 'scheduled_dws_v2_shadow', args.output_json)
                report['output_json'] = str(output_path)
                payload = _build_chain_summary_payload(report, attempt=attempt, max_retries=args.max_retries)
                write_total_control_chain_summary(payload)
                _send_alert_if_enabled(payload)
                logger.info('DWS v2 shadow-run 完成: status=%s, output_json=%s', report.get('status'), output_path)
                return 0 if report.get('status') == 'SUCCESS' else 1
            except ShadowScheduleError as exc:
                report = exc.report or {
                    'script': 'scheduled_dws_v2_shadow.py',
                    'mode': 'conn_test' if args.conn_test else 'execute',
                    'status': 'FAILED',
                    'started_at': datetime.now(),
                    'finished_at': datetime.now(),
                    'steps': [],
                    'error': repr(exc),
                }
                report['status'] = 'FAILED'
                report['error'] = repr(exc)
                report['finished_at'] = datetime.now()

                exhausted = attempt >= args.max_retries
                if not exc.retryable or exhausted:
                    output_path = write_runtime_report(report, 'scheduled_dws_v2_shadow', args.output_json)
                    report['output_json'] = str(output_path)
                    payload = _build_chain_summary_payload(report, attempt=attempt, max_retries=args.max_retries)
                    write_total_control_chain_summary(payload)
                    _send_alert_if_enabled(payload)
                    logger.error('DWS v2 shadow-run 失败并结束: retryable=%s, exhausted=%s', exc.retryable, exhausted)
                    return 2 if not exc.retryable else 1

                logger.warning(
                    'DWS v2 shadow-run 失败，等待 %s 秒后重试（%s/%s）: %s',
                    args.retry_sleep,
                    attempt,
                    args.max_retries,
                    exc,
                )
                time.sleep(args.retry_sleep)
            except Exception as exc:
                error_trace = traceback.format_exc()
                logger.error('DWS v2 shadow-run 出现未捕获异常: %s', error_trace)
                retryable = _is_retryable_message(str(exc))
                exhausted = attempt >= args.max_retries
                report = {
                    'script': 'scheduled_dws_v2_shadow.py',
                    'mode': 'conn_test' if args.conn_test else 'execute',
                    'status': 'FAILED',
                    'started_at': datetime.now(),
                    'finished_at': datetime.now(),
                    'steps': [],
                    'error': repr(exc),
                }
                if not retryable or exhausted:
                    output_path = write_runtime_report(report, 'scheduled_dws_v2_shadow', args.output_json)
                    report['output_json'] = str(output_path)
                    payload = _build_chain_summary_payload(report, attempt=attempt, max_retries=args.max_retries)
                    write_total_control_chain_summary(payload)
                    _send_alert_if_enabled(payload)
                    return 2 if not retryable else 1
                logger.warning('DWS v2 shadow-run 命中可重试异常，等待 %s 秒后重试...', args.retry_sleep)
                time.sleep(args.retry_sleep)
    finally:
        _release_singleton_lock(lock_conn, SCHEDULE_LOCK_NAME)

    return 1


if __name__ == '__main__':
    sys.exit(run_with_retries(build_parser().parse_args()))