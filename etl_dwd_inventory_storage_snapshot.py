# -*- coding: utf-8 -*-
"""DWD 库存店仓快照事实旁路 ETL。

状态：M3 小窗口装载实现；默认只输出 SQL / 做连接测试，只有显式 ``--execute``
才按 ``snapshot_date + storage_id`` upsert 写入 ``dwd_inventory_storage_snapshot``，不接入 run_etl.py。

设计边界：
- 目标表草案见 SQL/draft_create_dwd_inventory_storage_snapshot.sql。
- 默认假设 M3 选择旁路 ods_*_raw 源表；若改为兼容扩字段方案，需把源表名改回现有 ODS。
- 所有真实 CREATE / INSERT / DELETE / 回填由用户人工授权执行。
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date

from sqlalchemy import text

from db_connections import create_mysql_engine


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


DEFAULT_STORAGE_TABLE = 'ods_fa_storage_raw'
DEFAULT_TARGET_TABLE = 'dwd_inventory_storage_snapshot'

UPSERT_UPDATE_COLUMNS = (
    'store_id',
    'store_code',
    'is_cloud_store',
    'product_id',
    'm_productalias_id',
    'attribute_set_instance_id',
    'qty',
    'qty_preout',
    'qty_prein',
    'qty_freeze',
    'qty_oms',
    'qty_purchase_rem',
    'qty_oms_translate',
    'qty_preout1',
    'storage_isactive',
    'is_active_storage_flag',
    'has_sku_flag',
    'is_total_warehouse_flag',
    'is_cloud_store_flag',
    'dws_inventory_scope_flag',
    'zero_qty_kept_flag',
    'negative_qty_flag',
    'storage_created_at',
    'storage_modified_at',
    'source_loaded_at',
    'source_batch_id',
    'etl_time',
)


@dataclass(frozen=True)
class DwdInventoryStorageSnapshotConfig:
    snapshot_date: int
    storage_table: str = DEFAULT_STORAGE_TABLE
    target_table: str = DEFAULT_TARGET_TABLE
    timeout_profile: str = 'etl'


def _default_snapshot_date() -> int:
    return int(date.today().strftime('%Y%m%d'))


def build_insert_sql(config: DwdInventoryStorageSnapshotConfig) -> str:
    """生成小窗口 INSERT ... SELECT ... ON DUPLICATE KEY UPDATE SQL。"""

    update_clause = ',\n    '.join(
        f"{column} = VALUES({column})" for column in UPSERT_UPDATE_COLUMNS
    )

    return f"""
INSERT INTO {config.target_table} (
    snapshot_date,
    storage_id,
    store_id,
    store_code,
    is_cloud_store,
    product_id,
    m_productalias_id,
    attribute_set_instance_id,
    qty,
    qty_preout,
    qty_prein,
    qty_freeze,
    qty_oms,
    qty_purchase_rem,
    qty_oms_translate,
    qty_preout1,
    storage_isactive,
    is_active_storage_flag,
    has_sku_flag,
    is_total_warehouse_flag,
    is_cloud_store_flag,
    dws_inventory_scope_flag,
    zero_qty_kept_flag,
    negative_qty_flag,
    storage_created_at,
    storage_modified_at,
    source_loaded_at,
    source_batch_id,
    etl_time
)
SELECT
    :snapshot_date AS snapshot_date,
    fs.id AS storage_id,
    fs.c_store_id AS store_id,
    COALESCE(s.store_code, '') AS store_code,
    COALESCE(s.is_cloud_store, 'N') AS is_cloud_store,
    fs.m_product_id AS product_id,
    fs.m_productalias_id,
    fs.m_attributesetinstance_id AS attribute_set_instance_id,
    fs.qty,
    fs.qtypreout AS qty_preout,
    fs.qtyprein AS qty_prein,
    fs.qty_freeze,
    fs.qty_oms,
    fs.qtypurchaserem AS qty_purchase_rem,
    fs.qtyomstranslate AS qty_oms_translate,
    fs.qtypreout1 AS qty_preout1,
    fs.isactive AS storage_isactive,
    CASE WHEN fs.isactive = 'Y' THEN 'Y' ELSE 'N' END AS is_active_storage_flag,
    CASE WHEN fs.m_productalias_id IS NOT NULL THEN 'Y' ELSE 'N' END AS has_sku_flag,
    CASE WHEN s.store_code = '001' THEN 'Y' ELSE 'N' END AS is_total_warehouse_flag,
    CASE WHEN COALESCE(s.is_cloud_store, 'N') = 'Y' THEN 'Y' ELSE 'N' END AS is_cloud_store_flag,
    CASE
        WHEN fs.isactive = 'Y'
         AND fs.m_productalias_id IS NOT NULL
         AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
        THEN 'Y' ELSE 'N'
    END AS dws_inventory_scope_flag,
    CASE WHEN COALESCE(fs.qty, 0) = 0 THEN 'Y' ELSE 'N' END AS zero_qty_kept_flag,
    CASE WHEN COALESCE(fs.qty, 0) < 0 THEN 'Y' ELSE 'N' END AS negative_qty_flag,
    fs.creationdate AS storage_created_at,
    fs.modifieddate AS storage_modified_at,
    fs.etl_loaded_at AS source_loaded_at,
    CAST(fs.etl_batch_id AS CHAR) AS source_batch_id,
    NOW() AS etl_time
FROM {config.storage_table} fs
LEFT JOIN dim_store s
    ON fs.c_store_id = s.store_id
ON DUPLICATE KEY UPDATE
    {update_clause},
    updated_at = NOW()
""".strip()


def build_source_count_sql(config: DwdInventoryStorageSnapshotConfig) -> str:
    return f"SELECT COUNT(*) FROM {config.storage_table}"


def execute_load(config: DwdInventoryStorageSnapshotConfig) -> dict[str, int]:
    """执行 DWD 库存快照小窗口 upsert。"""

    params = {'snapshot_date': config.snapshot_date}
    engine = create_mysql_engine(timeout_profile=config.timeout_profile)
    try:
        with engine.begin() as conn:
            source_rows = conn.execute(text(build_source_count_sql(config))).scalar() or 0
            if source_rows <= 0:
                raise RuntimeError('raw ODS 库存表无数据，已停止 DWD 写入')
            result = conn.execute(text(build_insert_sql(config)), params)
            target_rows = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {config.target_table}
                    WHERE snapshot_date = :snapshot_date
                    """
                ),
                params,
            ).scalar() or 0
    finally:
        engine.dispose()

    logger.info(
        'dwd_inventory_storage_snapshot 小窗口 upsert 完成：source_rows=%s, affected=%s, target_snapshot_rows=%s',
        source_rows,
        result.rowcount,
        target_rows,
    )
    return {
        'source_rows': int(source_rows),
        'affected_rows': int(result.rowcount or 0),
        'target_snapshot_rows': int(target_rows),
    }


def conn_test(timeout_profile: str) -> None:
    """只读连接测试。"""

    engine = create_mysql_engine(timeout_profile=timeout_profile)
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
    finally:
        engine.dispose()
    logger.info('MySQL conn-test 通过，timeout_profile=%s', timeout_profile)


def run(config: DwdInventoryStorageSnapshotConfig, *, conn_test_only: bool, execute: bool) -> None:
    if conn_test_only:
        conn_test(config.timeout_profile)
        return

    sql = build_insert_sql(config)
    logger.info('生成 dwd_inventory_storage_snapshot 候选 SQL，快照日期：%s', config.snapshot_date)
    print(sql)
    print({'snapshot_date': config.snapshot_date})

    if execute:
        summary = execute_load(config)
        print(summary)
    else:
        logger.info('未追加 --execute，仅完成 dry-run 输出；不会写入 MySQL。')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='DWD 库存店仓快照事实旁路小窗口装载。')
    parser.add_argument('--snapshot-date', type=int, default=_default_snapshot_date(), help='快照日期 YYYYMMDD，默认今天。')
    parser.add_argument('--storage-table', default=DEFAULT_STORAGE_TABLE, help='库存 ODS 源表，默认 ods_fa_storage_raw。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='DWD 目标表名。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='etl')
    parser.add_argument('--conn-test', action='store_true', help='只执行 MySQL SELECT 1 连接测试。')
    parser.add_argument('--execute', action='store_true', help='显式执行 MySQL upsert；未提供时仅 dry-run。')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = DwdInventoryStorageSnapshotConfig(
        snapshot_date=args.snapshot_date,
        storage_table=args.storage_table,
        target_table=args.target_table,
        timeout_profile=args.timeout_profile,
    )
    run(config, conn_test_only=args.conn_test, execute=args.execute)


if __name__ == '__main__':
    main()