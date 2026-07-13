# -*- coding: utf-8 -*-
"""DWS v2 库存日汇总并行表 dry-run / conn-test / S3 手工写入脚本。

默认模式仍只输出候选 SQL；``--conn-test`` 只做只读连接与结构检查。
进入 S3 后新增受控 ``--execute`` 分支，但必须由用户在本地手工传入精确
``--confirm-write`` 令牌才会写入 ``dws_inventory_daily_v2``。脚本仍不接入
``run_etl.py``、``scheduled_etl.py``、``scheduled_total_control.py``，也不切换 ADS。
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass, replace
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


DEFAULT_SOURCE_TABLE = 'dwd_inventory_storage_snapshot'
DEFAULT_TARGET_TABLE = 'dws_inventory_daily_v2'
DEFAULT_SNAPSHOT_DATE = 20260507
DEFAULT_SOURCE_LAYER_VERSION = 'M3_DWD_V1'
DEFAULT_VALIDATION_STATUS = 'PENDING'
DEFAULT_VALIDATION_NOTE = 'S3 manual branch generated; pending reconciliation'
DEFAULT_LOAD_BATCH_ID = 'DWS_INVENTORY_V2_S3_MANUAL'
DEFAULT_OLD_DWS_TABLE = 'dws_inventory_daily'
WRITE_CONFIRMATION_TOKEN = 'WRITE_DWS_INVENTORY_V2'
DEFAULT_LOCK_NAME = 'hefang_dw:dws_inventory_daily_v2:s3'
DEFAULT_LOCK_TIMEOUT_SECONDS = 10
DEFAULT_RECONCILIATION_LIMIT = 20

IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

SOURCE_REQUIRED_COLUMNS = {
    'snapshot_date',
    'store_id',
    'store_code',
    'is_cloud_store',
    'product_id',
    'm_productalias_id',
    'qty',
    'qty_preout',
    'qty_prein',
    'qty_freeze',
    'qty_oms',
    'qty_purchase_rem',
    'qty_oms_translate',
    'qty_preout1',
    'dws_inventory_scope_flag',
    'zero_qty_kept_flag',
    'negative_qty_flag',
    'storage_modified_at',
    'source_loaded_at',
}

TARGET_REQUIRED_COLUMNS = {
    'date_id',
    'store_id',
    'store_code',
    'is_cloud_store',
    'product_id',
    'm_productalias_id',
    'qty',
    'qty_valid',
    'qty_occupy',
    'qtypurchaserem',
    'qty_preout',
    'qty_prein',
    'qty_freeze',
    'qty_oms',
    'qty_oms_translate',
    'qty_preout1',
    'source_dwd_row_count',
    'zero_qty_row_count',
    'negative_qty_row_count',
    'min_storage_modified_at',
    'max_storage_modified_at',
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
    'qty',
    'qty_valid',
    'qty_occupy',
    'qtypurchaserem',
    'qty_preout',
    'qty_prein',
    'qty_freeze',
    'qty_oms',
    'qty_oms_translate',
    'qty_preout1',
    'source_dwd_row_count',
    'zero_qty_row_count',
    'negative_qty_row_count',
    'min_storage_modified_at',
    'max_storage_modified_at',
    'source_min_loaded_at',
    'source_max_loaded_at',
    'load_batch_id',
    'source_layer_version',
    'validation_status',
    'validation_note',
    'etl_time',
)


@dataclass(frozen=True)
class DwsInventoryV2DryRunConfig:
    snapshot_date: int = DEFAULT_SNAPSHOT_DATE
    source_table: str = DEFAULT_SOURCE_TABLE
    target_table: str = DEFAULT_TARGET_TABLE
    old_dws_table: str = DEFAULT_OLD_DWS_TABLE
    load_batch_id: str = DEFAULT_LOAD_BATCH_ID
    source_layer_version: str = DEFAULT_SOURCE_LAYER_VERSION
    validation_status: str = DEFAULT_VALIDATION_STATUS
    validation_note: str = DEFAULT_VALIDATION_NOTE
    source_loaded_at_cutoff: datetime | None = None
    align_with_old_dws: bool = False
    timeout_profile: str = 'long_running'
    lock_name: str = DEFAULT_LOCK_NAME
    lock_timeout_seconds: int = DEFAULT_LOCK_TIMEOUT_SECONDS


def _validate_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f'非法表名或标识符: {identifier}')
    return identifier


def _validate_config(config: DwsInventoryV2DryRunConfig) -> None:
    _validate_identifier(config.source_table)
    _validate_identifier(config.target_table)
    _validate_identifier(config.old_dws_table)
    validate_lock_settings(config.lock_name, config.lock_timeout_seconds)


def _parse_datetime_arg(value: str) -> datetime:
    normalized = value.strip().replace('T', ' ')
    try:
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f'非法 DATETIME 参数: {value}，应为 YYYY-MM-DD HH:MM:SS 或 ISO 格式。'
        ) from exc


def _format_datetime_value(value: datetime | None) -> str | None:
    return value.isoformat(sep=' ') if value is not None else None


def _build_source_scope_where_sql(config: DwsInventoryV2DryRunConfig) -> str:
    predicates = [
        'snapshot_date = :snapshot_date',
        "dws_inventory_scope_flag = 'Y'",
        'store_id IS NOT NULL',
        'product_id IS NOT NULL',
        'm_productalias_id IS NOT NULL',
    ]
    if config.source_loaded_at_cutoff is not None:
        predicates.append('source_loaded_at <= :source_loaded_at_cutoff')
    return 'WHERE ' + '\n  AND '.join(predicates)


def _build_alignment_note(config: DwsInventoryV2DryRunConfig, alignment_mode: str | None) -> str:
    base_note = config.validation_note.strip()
    if config.source_loaded_at_cutoff is None:
        return base_note

    suffix_parts = [
        f'source_loaded_at_cutoff={_format_datetime_value(config.source_loaded_at_cutoff)}',
        f'cutoff_mode={alignment_mode or "explicit"}',
        f'old_dws_table={config.old_dws_table}',
    ]
    if base_note:
        return f'{base_note}; ' + '; '.join(suffix_parts)
    return '; '.join(suffix_parts)


def build_delete_existing_slice_sql(config: DwsInventoryV2DryRunConfig) -> str:
    _validate_config(config)
    return f"""
DELETE FROM {config.target_table}
WHERE date_id = :snapshot_date
""".strip()


def build_old_dws_alignment_probe_sql(config: DwsInventoryV2DryRunConfig) -> str:
    _validate_config(config)
    return f"""
SELECT
    COUNT(*) AS old_dws_row_count,
    COUNT(DISTINCT etl_time) AS old_dws_distinct_etl_time_count,
    MIN(etl_time) AS old_dws_min_etl_time,
    MAX(etl_time) AS old_dws_max_etl_time,
    SUM(COALESCE(qty, 0)) AS old_dws_qty,
    SUM(COALESCE(qtypurchaserem, 0)) AS old_dws_qtypurchaserem
FROM {config.old_dws_table}
WHERE date_id = :snapshot_date
""".strip()


def build_old_dws_comparison_detail_sql(config: DwsInventoryV2DryRunConfig, *, limit: int | None = None) -> str:
    _validate_config(config)
    limit_clause = f'\nLIMIT {int(limit)}' if limit is not None else ''
    return f"""
WITH v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty_valid, 0)) AS qty_valid,
        SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
        SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem
    FROM {config.target_table}
    WHERE date_id = :snapshot_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
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
    FROM {config.old_dws_table}
    WHERE date_id = :snapshot_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM v2_scope
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
    SUM(CASE WHEN source_layer = 'V2' THEN qty ELSE 0 END) AS v2_qty,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty ELSE 0 END) AS old_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_valid ELSE 0 END) AS v2_qty_valid,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_valid ELSE 0 END) AS old_qty_valid,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_occupy ELSE 0 END) AS v2_qty_occupy,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_occupy ELSE 0 END) AS old_qty_occupy,
    SUM(CASE WHEN source_layer = 'V2' THEN qtypurchaserem ELSE 0 END) AS v2_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'OLD' THEN qtypurchaserem ELSE 0 END) AS old_qtypurchaserem
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(v2_qty - old_qty) > 0.0001
    OR ABS(v2_qty_valid - old_qty_valid) > 0.0001
    OR ABS(v2_qty_occupy - old_qty_occupy) > 0.0001
    OR ABS(v2_qtypurchaserem - old_qtypurchaserem) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id{limit_clause}
""".strip()


def build_old_dws_comparison_count_sql(config: DwsInventoryV2DryRunConfig) -> str:
    return f"""
SELECT COUNT(*) AS mismatch_count
FROM (
    {build_old_dws_comparison_detail_sql(config)}
) mismatch_scope
""".strip()


def build_late_loaded_scope_probe_sql(config: DwsInventoryV2DryRunConfig) -> str:
    _validate_config(config)
    if config.source_loaded_at_cutoff is None:
        raise ValueError('构造 late-loaded scope probe SQL 时必须提供 source_loaded_at_cutoff。')
    return f"""
SELECT
    COUNT(*) AS late_scope_row_count,
    SUM(COALESCE(qty, 0)) AS late_scope_qty,
    SUM(COALESCE(qty_purchase_rem, 0)) AS late_scope_qtypurchaserem,
    MIN(source_loaded_at) AS min_source_loaded_at_after_cutoff,
    MAX(source_loaded_at) AS max_source_loaded_at_after_cutoff
FROM {config.source_table}
WHERE snapshot_date = :snapshot_date
  AND dws_inventory_scope_flag = 'Y'
  AND store_id IS NOT NULL
  AND product_id IS NOT NULL
  AND m_productalias_id IS NOT NULL
  AND source_loaded_at > :source_loaded_at_cutoff
""".strip()


def _fetch_old_dws_probe(conn, config: DwsInventoryV2DryRunConfig, params: dict[str, object]) -> dict[str, Any]:
    return fetch_one_mapping(conn, build_old_dws_alignment_probe_sql(config), params)


def _assert_same_snapshot_cutoff_reproducible(
    probe: dict[str, Any],
    config: DwsInventoryV2DryRunConfig,
) -> None:
    late_scope_row_count = int(probe.get('late_scope_row_count') or 0)
    if late_scope_row_count <= 0:
        return

    cutoff_text = _format_datetime_value(config.source_loaded_at_cutoff)
    raise RuntimeError(
        'inventory same-snapshot cutoff 无法复原历史快照：'
        f'snapshot_date={config.snapshot_date}, source_loaded_at_cutoff={cutoff_text} 后仍有 '
        f'{late_scope_row_count} 条 scope 行，'
        f'late_scope_qty={probe.get("late_scope_qty") or 0}, '
        f'late_scope_qtypurchaserem={probe.get("late_scope_qtypurchaserem") or 0}, '
        f'min_source_loaded_at_after_cutoff={probe.get("min_source_loaded_at_after_cutoff")}, '
        f'max_source_loaded_at_after_cutoff={probe.get("max_source_loaded_at_after_cutoff")}。'
        '当前 raw/DWD 使用 source_loaded_at 记录 MySQL 装载时点；若 cutoff 之后仍有 scope 行，'
        '则不能把 old DWS 的 etl_time 直接当作可回放的 source_loaded_at 截止时点。'
    )


def _resolve_effective_config(
    conn,
    config: DwsInventoryV2DryRunConfig,
    params: dict[str, object],
) -> tuple[DwsInventoryV2DryRunConfig, dict[str, Any] | None]:
    alignment_context: dict[str, Any] | None = None
    resolved_config = config

    if config.align_with_old_dws or config.source_loaded_at_cutoff is not None:
        old_dws_probe = _fetch_old_dws_probe(conn, config, params)
        if not old_dws_probe.get('old_dws_row_count'):
            raise RuntimeError(
                f'{config.old_dws_table} 在 snapshot_date={config.snapshot_date} 无可用行，无法对齐库存时点。'
            )

        cutoff_mode = 'explicit'
        cutoff_value = config.source_loaded_at_cutoff
        if config.align_with_old_dws:
            cutoff_text = old_dws_probe.get('old_dws_max_etl_time')
            if not cutoff_text:
                raise RuntimeError(
                    f'{config.old_dws_table} 在 snapshot_date={config.snapshot_date} 缺少 max(etl_time)，无法自动对齐。'
                )
            cutoff_value = datetime.fromisoformat(str(cutoff_text).replace('T', ' '))
            cutoff_mode = 'old_dws_max_etl_time'

        resolved_config = replace(resolved_config, source_loaded_at_cutoff=cutoff_value)
        resolved_note = _build_alignment_note(resolved_config, cutoff_mode)
        resolved_config = replace(resolved_config, validation_note=resolved_note)

        alignment_context = {
            'cutoff_mode': cutoff_mode,
            'source_loaded_at_cutoff': _format_datetime_value(resolved_config.source_loaded_at_cutoff),
            'old_dws_table': config.old_dws_table,
            'old_dws_probe': old_dws_probe,
        }
        old_dws_max_etl_time = old_dws_probe.get('old_dws_max_etl_time')
        if old_dws_max_etl_time and resolved_config.source_loaded_at_cutoff is not None:
            alignment_context['cutoff_matches_old_dws_max_etl_time'] = (
                _format_datetime_value(resolved_config.source_loaded_at_cutoff)
                == str(old_dws_max_etl_time).replace('T', ' ')
            )
        late_loaded_scope_probe = fetch_one_mapping(
            conn,
            build_late_loaded_scope_probe_sql(resolved_config),
            build_params(resolved_config),
        )
        alignment_context['late_loaded_scope_after_cutoff'] = late_loaded_scope_probe
        alignment_context['same_snapshot_reproducible'] = int(
            late_loaded_scope_probe.get('late_scope_row_count') or 0
        ) == 0
        _assert_same_snapshot_cutoff_reproducible(late_loaded_scope_probe, resolved_config)

    return resolved_config, alignment_context


def build_params(config: DwsInventoryV2DryRunConfig) -> dict[str, object]:
    return {
        'snapshot_date': config.snapshot_date,
        'load_batch_id': config.load_batch_id,
        'source_layer_version': config.source_layer_version,
        'validation_status': config.validation_status,
        'validation_note': config.validation_note,
        'source_loaded_at_cutoff': config.source_loaded_at_cutoff,
    }


def build_source_summary_sql(config: DwsInventoryV2DryRunConfig) -> str:
    """生成只读源数据范围摘要 SQL，不写库。"""

    _validate_config(config)
    return f"""
SELECT
    COUNT(*) AS source_dwd_row_count,
    COUNT(DISTINCT CONCAT_WS('|', snapshot_date, store_id, product_id, m_productalias_id)) AS candidate_group_count,
    SUM(CASE WHEN zero_qty_kept_flag = 'Y' OR COALESCE(qty, 0) = 0 THEN 1 ELSE 0 END) AS zero_qty_row_count,
    SUM(CASE WHEN negative_qty_flag = 'Y' OR COALESCE(qty, 0) < 0 THEN 1 ELSE 0 END) AS negative_qty_row_count,
    MIN(snapshot_date) AS min_snapshot_date,
    MAX(snapshot_date) AS max_snapshot_date,
    MIN(storage_modified_at) AS min_storage_modified_at,
    MAX(storage_modified_at) AS max_storage_modified_at,
    MIN(source_loaded_at) AS source_min_loaded_at,
    MAX(source_loaded_at) AS source_max_loaded_at
FROM {config.source_table}
{_build_source_scope_where_sql(config)}
""".strip()


def build_insert_sql(config: DwsInventoryV2DryRunConfig) -> str:
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
    qty,
    qty_valid,
    qty_occupy,
    qtypurchaserem,
    qty_preout,
    qty_prein,
    qty_freeze,
    qty_oms,
    qty_oms_translate,
    qty_preout1,
    source_dwd_row_count,
    zero_qty_row_count,
    negative_qty_row_count,
    min_storage_modified_at,
    max_storage_modified_at,
    source_min_loaded_at,
    source_max_loaded_at,
    load_batch_id,
    source_layer_version,
    validation_status,
    validation_note,
    etl_time
)
SELECT
    snapshot_date AS date_id,
    store_id,
    COALESCE(store_code, '') AS store_code,
    COALESCE(is_cloud_store, 'N') AS is_cloud_store,
    product_id,
    m_productalias_id,
    SUM(COALESCE(qty, 0)) AS qty,
    SUM(COALESCE(qty, 0)) AS qty_valid,
    0 AS qty_occupy,
    SUM(COALESCE(qty_purchase_rem, 0)) AS qtypurchaserem,
    SUM(COALESCE(qty_preout, 0)) AS qty_preout,
    SUM(COALESCE(qty_prein, 0)) AS qty_prein,
    SUM(COALESCE(qty_freeze, 0)) AS qty_freeze,
    SUM(COALESCE(qty_oms, 0)) AS qty_oms,
    SUM(COALESCE(qty_oms_translate, 0)) AS qty_oms_translate,
    SUM(COALESCE(qty_preout1, 0)) AS qty_preout1,
    COUNT(*) AS source_dwd_row_count,
    SUM(CASE WHEN zero_qty_kept_flag = 'Y' OR COALESCE(qty, 0) = 0 THEN 1 ELSE 0 END) AS zero_qty_row_count,
    SUM(CASE WHEN negative_qty_flag = 'Y' OR COALESCE(qty, 0) < 0 THEN 1 ELSE 0 END) AS negative_qty_row_count,
    MIN(storage_modified_at) AS min_storage_modified_at,
    MAX(storage_modified_at) AS max_storage_modified_at,
    MIN(source_loaded_at) AS source_min_loaded_at,
    MAX(source_loaded_at) AS source_max_loaded_at,
    :load_batch_id AS load_batch_id,
    :source_layer_version AS source_layer_version,
    :validation_status AS validation_status,
    :validation_note AS validation_note,
    NOW() AS etl_time
FROM {config.source_table}
{_build_source_scope_where_sql(config)}
GROUP BY snapshot_date, store_id, COALESCE(store_code, ''), COALESCE(is_cloud_store, 'N'), product_id, m_productalias_id
ON DUPLICATE KEY UPDATE
    {update_clause},
    updated_at = NOW()
""".strip()


def build_target_summary_sql(config: DwsInventoryV2DryRunConfig) -> str:
    """生成目标 v2 表装载后摘要 SQL。"""

    _validate_config(config)
    return f"""
SELECT
    COUNT(*) AS target_row_count,
    SUM(COALESCE(source_dwd_row_count, 0)) AS source_dwd_row_count,
    SUM(COALESCE(qty, 0)) AS qty,
    SUM(COALESCE(qty_valid, 0)) AS qty_valid,
    SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
    SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem,
    SUM(COALESCE(qty_preout, 0)) AS qty_preout,
    SUM(COALESCE(qty_prein, 0)) AS qty_prein,
    SUM(COALESCE(qty_freeze, 0)) AS qty_freeze,
    SUM(COALESCE(qty_oms, 0)) AS qty_oms,
    SUM(COALESCE(qty_oms_translate, 0)) AS qty_oms_translate,
    SUM(COALESCE(qty_preout1, 0)) AS qty_preout1,
    MIN(etl_time) AS min_etl_time,
    MAX(etl_time) AS max_etl_time
FROM {config.target_table}
WHERE date_id = :snapshot_date
""".strip()


def build_reconciliation_detail_sql(config: DwsInventoryV2DryRunConfig, *, limit: int | None = None) -> str:
    """生成 DWD 聚合与目标 v2 的差异明细 SQL。"""

    _validate_config(config)
    limit_clause = f'\nLIMIT {int(limit)}' if limit is not None else ''
    return f"""
WITH dwd_scope AS (
    SELECT
        snapshot_date AS date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty, 0)) AS qty_valid,
        0 AS qty_occupy,
        SUM(COALESCE(qty_purchase_rem, 0)) AS qtypurchaserem,
        SUM(COALESCE(qty_preout, 0)) AS qty_preout,
        SUM(COALESCE(qty_prein, 0)) AS qty_prein,
        SUM(COALESCE(qty_freeze, 0)) AS qty_freeze,
        SUM(COALESCE(qty_oms, 0)) AS qty_oms,
        SUM(COALESCE(qty_oms_translate, 0)) AS qty_oms_translate,
        SUM(COALESCE(qty_preout1, 0)) AS qty_preout1,
        COUNT(*) AS source_dwd_row_count
    FROM {config.source_table}
        {_build_source_scope_where_sql(config)}
    GROUP BY snapshot_date, store_id, product_id, m_productalias_id
), v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        qty,
        qty_valid,
        qty_occupy,
        qtypurchaserem,
        qty_preout,
        qty_prein,
        qty_freeze,
        qty_oms,
        qty_oms_translate,
        qty_preout1,
        source_dwd_row_count
    FROM {config.target_table}
    WHERE date_id = :snapshot_date
), combined_scope AS (
    SELECT 'DWD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem, qty_preout, qty_prein, qty_freeze,
           qty_oms, qty_oms_translate, qty_preout1, source_dwd_row_count
    FROM dwd_scope
    UNION ALL
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem, qty_preout, qty_prein, qty_freeze,
           qty_oms, qty_oms_translate, qty_preout1, source_dwd_row_count
    FROM v2_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty ELSE 0 END) AS dwd_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN qty ELSE 0 END) AS v2_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_valid ELSE 0 END) AS dwd_qty_valid,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_valid ELSE 0 END) AS v2_qty_valid,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_occupy ELSE 0 END) AS dwd_qty_occupy,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_occupy ELSE 0 END) AS v2_qty_occupy,
    SUM(CASE WHEN source_layer = 'DWD' THEN qtypurchaserem ELSE 0 END) AS dwd_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'V2' THEN qtypurchaserem ELSE 0 END) AS v2_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_preout ELSE 0 END) AS dwd_qty_preout,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_preout ELSE 0 END) AS v2_qty_preout,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_prein ELSE 0 END) AS dwd_qty_prein,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_prein ELSE 0 END) AS v2_qty_prein,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_freeze ELSE 0 END) AS dwd_qty_freeze,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_freeze ELSE 0 END) AS v2_qty_freeze,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_oms ELSE 0 END) AS dwd_qty_oms,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_oms ELSE 0 END) AS v2_qty_oms,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_oms_translate ELSE 0 END) AS dwd_qty_oms_translate,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_oms_translate ELSE 0 END) AS v2_qty_oms_translate,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_preout1 ELSE 0 END) AS dwd_qty_preout1,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_preout1 ELSE 0 END) AS v2_qty_preout1,
    SUM(CASE WHEN source_layer = 'DWD' THEN source_dwd_row_count ELSE 0 END) AS dwd_source_rows,
    SUM(CASE WHEN source_layer = 'V2' THEN source_dwd_row_count ELSE 0 END) AS v2_source_rows
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_qty - v2_qty) > 0.0001
    OR ABS(dwd_qty_valid - v2_qty_valid) > 0.0001
    OR ABS(dwd_qty_occupy - v2_qty_occupy) > 0.0001
    OR ABS(dwd_qtypurchaserem - v2_qtypurchaserem) > 0.0001
    OR ABS(dwd_qty_preout - v2_qty_preout) > 0.0001
    OR ABS(dwd_qty_prein - v2_qty_prein) > 0.0001
    OR ABS(dwd_qty_freeze - v2_qty_freeze) > 0.0001
    OR ABS(dwd_qty_oms - v2_qty_oms) > 0.0001
    OR ABS(dwd_qty_oms_translate - v2_qty_oms_translate) > 0.0001
    OR ABS(dwd_qty_preout1 - v2_qty_preout1) > 0.0001
    OR ABS(dwd_source_rows - v2_source_rows) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id{limit_clause}
""".strip()


def build_reconciliation_count_sql(config: DwsInventoryV2DryRunConfig) -> str:
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


def _assert_structure(conn, config: DwsInventoryV2DryRunConfig) -> None:
    """校验源表、目标表字段与唯一键结构。"""

    source_columns = _fetch_columns(conn, config.source_table)
    target_columns = _fetch_columns(conn, config.target_table)
    _assert_columns(config.source_table, source_columns, SOURCE_REQUIRED_COLUMNS)
    _assert_columns(config.target_table, target_columns, TARGET_REQUIRED_COLUMNS)
    unique_key_columns = _fetch_unique_key_columns(
        conn,
        config.target_table,
        'uk_dws_inventory_daily_v2_date_store_product_sku',
    )
    if unique_key_columns != TARGET_UNIQUE_KEY_COLUMNS:
        raise RuntimeError(f'{config.target_table} 唯一键不符合预期: {unique_key_columns}')


def conn_test(config: DwsInventoryV2DryRunConfig) -> None:
    """只读连接和结构检查；不写入目标表。"""

    _validate_config(config)
    engine = create_mysql_engine(timeout_profile=config.timeout_profile)
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
            _assert_structure(conn, config)
            effective_config, alignment_context = _resolve_effective_config(conn, config, build_params(config))
    finally:
        engine.dispose()
    if alignment_context:
        logger.info(
            'dws_inventory_daily_v2 conn-test 通过：source=%s, target=%s, timeout_profile=%s, source_loaded_at_cutoff=%s, cutoff_mode=%s',
            effective_config.source_table,
            effective_config.target_table,
            effective_config.timeout_profile,
            alignment_context.get('source_loaded_at_cutoff'),
            alignment_context.get('cutoff_mode'),
        )
        return

    logger.info(
        'dws_inventory_daily_v2 conn-test 通过：source=%s, target=%s, timeout_profile=%s',
        config.source_table,
        config.target_table,
        config.timeout_profile,
    )


def execute_load(
    config: DwsInventoryV2DryRunConfig,
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
        'script': 'etl_dws_inventory_v2.py',
        'mode': 'execute',
        'target_table': config.target_table,
        'source_table': config.source_table,
        'old_dws_table': config.old_dws_table,
        'snapshot_date': config.snapshot_date,
        'align_with_old_dws': config.align_with_old_dws,
        'source_loaded_at_cutoff': _format_datetime_value(config.source_loaded_at_cutoff),
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
                effective_config, alignment_context = _resolve_effective_config(conn, config, params)
                effective_params = build_params(effective_config)
                if alignment_context:
                    report['alignment_context'] = alignment_context
                    report['source_loaded_at_cutoff'] = alignment_context.get('source_loaded_at_cutoff')
                report['source_summary_before'] = fetch_one_mapping(conn, build_source_summary_sql(effective_config), effective_params)
                report['target_summary_before'] = fetch_one_mapping(conn, build_target_summary_sql(effective_config), effective_params)
                commit_if_open(conn)

                acquire_named_lock(conn, effective_config.lock_name, effective_config.lock_timeout_seconds)
                lock_acquired = True
                logger.info('已获取 MySQL 命名锁: %s', effective_config.lock_name)

                started = time.perf_counter()
                transaction = conn.begin()
                try:
                    delete_result = conn.execute(text(build_delete_existing_slice_sql(effective_config)), {'snapshot_date': effective_config.snapshot_date})
                    result = conn.execute(text(build_insert_sql(effective_config)), effective_params)
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

                report['delete_rowcount'] = delete_result.rowcount
                report['insert_rowcount'] = result.rowcount
                report['write_duration_seconds'] = duration_seconds
                report['target_summary_after'] = fetch_one_mapping(conn, build_target_summary_sql(effective_config), effective_params)
                report['reconciliation'] = {
                    'mismatch_count': fetch_one_mapping(conn, build_reconciliation_count_sql(effective_config), effective_params).get('mismatch_count', 0),
                    'sample_limit': reconciliation_limit,
                    'sample_mismatches': fetch_all_mappings(
                        conn,
                        build_reconciliation_detail_sql(effective_config, limit=reconciliation_limit),
                        effective_params,
                    ),
                }
                mismatch_count = int(report['reconciliation']['mismatch_count'] or 0)
                old_dws_mismatch_count = 0
                if alignment_context:
                    report['old_dws_alignment'] = {
                        'mismatch_count': fetch_one_mapping(
                            conn,
                            build_old_dws_comparison_count_sql(effective_config),
                            effective_params,
                        ).get('mismatch_count', 0),
                        'sample_limit': reconciliation_limit,
                        'sample_mismatches': fetch_all_mappings(
                            conn,
                            build_old_dws_comparison_detail_sql(effective_config, limit=reconciliation_limit),
                            effective_params,
                        ),
                    }
                    old_dws_mismatch_count = int(report['old_dws_alignment']['mismatch_count'] or 0)
                report['status'] = 'SUCCESS' if mismatch_count == 0 and old_dws_mismatch_count == 0 else 'WARNING'
                report['finished_at'] = datetime.now().isoformat(sep=' ')
                report['cleanup'] = 'write_transaction_committed; named_lock_release_pending'
            finally:
                if lock_acquired:
                    try:
                        release_named_lock(conn, effective_config.lock_name)
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
        output_path = write_runtime_report(report, 'dws_inventory_v2_s3_load', output_json)
        logger.info('DWS inventory v2 S3 运行证据已写入: %s', output_path)
        engine.dispose()

    report['output_json'] = str(output_path) if output_path else None
    return report


def run(
    config: DwsInventoryV2DryRunConfig,
    *,
    conn_test_only: bool,
    execute: bool = False,
    confirm_write: str | None = None,
    output_json: str | None = None,
    reconciliation_limit: int = DEFAULT_RECONCILIATION_LIMIT,
) -> None:
    if conn_test_only and execute:
        raise ValueError('--conn-test 不能与 --execute 同时使用')
    if config.align_with_old_dws and not conn_test_only and not execute:
        raise ValueError('--align-with-old-dws 需要连接数据库，请改用 --conn-test / --execute，或显式传入 --source-loaded-at-cutoff。')

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
            'dws_inventory_daily_v2 S3 写入分支完成：status=%s, mismatch_count=%s, output_json=%s',
            report.get('status'),
            (report.get('reconciliation') or {}).get('mismatch_count'),
            report.get('output_json'),
        )
        return

    logger.info(
        '生成 dws_inventory_daily_v2 dry-run SQL，快照日期：%s；不会连接数据库或写入 MySQL。',
        config.snapshot_date,
    )
    print('-- DWS inventory v2 source summary SQL (read-only)')
    print(build_source_summary_sql(config))
    print('\n-- DWS inventory v2 candidate INSERT SQL (manual S3 execute requires explicit confirmation)')
    print(build_insert_sql(config))
    print('\n-- DWS inventory v2 post-load target summary SQL (read-only)')
    print(build_target_summary_sql(config))
    print('\n-- DWS inventory v2 DWD-v2 reconciliation SQL (read-only; sample)')
    print(build_reconciliation_detail_sql(config, limit=DEFAULT_RECONCILIATION_LIMIT))
    print('\n-- DWS inventory old DWS alignment probe SQL (read-only; fetch old_dws_max_etl_time before S4 compare)')
    print(build_old_dws_alignment_probe_sql(config))
    if config.source_loaded_at_cutoff is not None:
        print('\n-- DWS inventory v2 vs old DWS comparison SQL (read-only; only valid after this snapshot_date was fully reloaded with the same source_loaded_at_cutoff)')
        print(build_old_dws_comparison_detail_sql(config, limit=DEFAULT_RECONCILIATION_LIMIT))
    print('\n-- Parameters')
    print(build_params(config))
    logger.info(
        '默认 dry-run 不写库；若用户手工执行 S3 写入，需追加 --execute --confirm-write %s。',
        WRITE_CONFIRMATION_TOKEN,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='DWS v2 库存日汇总并行表 dry-run / conn-test / S3 手工写入。'
    )
    parser.add_argument('--snapshot-date', type=int, default=DEFAULT_SNAPSHOT_DATE, help='库存快照日期 YYYYMMDD，默认 M3 full raw 验证快照 20260507。')
    parser.add_argument('--source-table', default=DEFAULT_SOURCE_TABLE, help='DWD 源表，默认 dwd_inventory_storage_snapshot。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='DWS v2 目标表名，默认 dws_inventory_daily_v2。')
    parser.add_argument('--old-dws-table', default=DEFAULT_OLD_DWS_TABLE, help='旧 DWS 对比表，默认 dws_inventory_daily。')
    parser.add_argument('--load-batch-id', default=DEFAULT_LOAD_BATCH_ID, help='装载批次 ID，写入分支会落入目标表。')
    parser.add_argument('--source-layer-version', default=DEFAULT_SOURCE_LAYER_VERSION, help='来源层版本标识。')
    parser.add_argument('--validation-status', default=DEFAULT_VALIDATION_STATUS, help='候选并行对账状态，默认 PENDING。')
    parser.add_argument('--validation-note', default=DEFAULT_VALIDATION_NOTE, help='候选并行对账说明。')
    cutoff_group = parser.add_mutually_exclusive_group()
    cutoff_group.add_argument('--source-loaded-at-cutoff', type=_parse_datetime_arg, default=None, help='库存 source_loaded_at 截止时点，格式 YYYY-MM-DD HH:MM:SS；用于固定与旧 DWS 的同源快照截面。')
    cutoff_group.add_argument('--align-with-old-dws', action='store_true', help='自动读取旧 dws_inventory_daily 当日 MAX(etl_time) 作为 source_loaded_at cutoff，仅适用于 conn-test / execute。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='long_running')
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
    config = DwsInventoryV2DryRunConfig(
        snapshot_date=args.snapshot_date,
        source_table=args.source_table,
        target_table=args.target_table,
        old_dws_table=args.old_dws_table,
        load_batch_id=args.load_batch_id,
        source_layer_version=args.source_layer_version,
        validation_status=args.validation_status,
        validation_note=args.validation_note,
        source_loaded_at_cutoff=args.source_loaded_at_cutoff,
        align_with_old_dws=args.align_with_old_dws,
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