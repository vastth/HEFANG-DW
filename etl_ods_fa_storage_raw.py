# -*- coding: utf-8 -*-
"""ODS raw 库存余额旁路抽取。

状态：M3 小窗口装载实现；默认 dry-run 输出 SQL，只有显式 ``--execute`` 才写入
``ods_fa_storage_raw``，且不接入 run_etl.py。

设计边界：
- 目标表草案见 SQL/draft_create_ods_fa_storage_raw.sql。
- 来源表为 Oracle BOSNDS3.FA_STORAGE，保留 M3 白名单字段。
- 库存 raw 第一阶段默认保留全店仓全量快照，不继承当前 DWS 范围过滤。
- 所有真实 CREATE / TRUNCATE / INSERT / 回填由用户人工授权执行。
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

import pandas as pd
from sqlalchemy import text

from db_connections import create_mysql_engine, create_oracle_engine
from etl_m3_load_utils import normalize_dataframe, upsert_dataframe


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


DEFAULT_SOURCE_TABLE = 'FA_STORAGE'
DEFAULT_TARGET_TABLE = 'ods_fa_storage_raw'

SOURCE_COLUMNS = (
    'id',
    'ad_client_id',
    'ad_org_id',
    'ownerid',
    'modifierid',
    'creationdate',
    'modifieddate',
    'isactive',
    'c_store_id',
    'm_product_id',
    'm_attributesetinstance_id',
    'qty',
    'qtypreout',
    'qtyprein',
    'm_productalias_id',
    'qty_freeze',
    'qty_oms',
    'qtypurchaserem',
    'qtyomstranslate',
    'qtypreout1',
)
LOAD_COLUMNS = SOURCE_COLUMNS + ('etl_batch_id', 'etl_loaded_at')


@dataclass(frozen=True)
class OdsFaStorageRawConfig:
    mode: str = 'modified-window'
    start_time: datetime | None = None
    end_time: datetime | None = None
    source_table: str = DEFAULT_SOURCE_TABLE
    target_table: str = DEFAULT_TARGET_TABLE
    timeout_profile: str = 'long_running'


def _default_window(days_back: int = 1) -> tuple[datetime, datetime]:
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_back)
    return start_time, end_time


def _coerce_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def build_source_sql(config: OdsFaStorageRawConfig) -> str:
    """生成 Oracle 抽取 SQL；草案阶段不自动执行。"""

    where_clause = ''
    if config.mode == 'modified-window':
        where_clause = """
WHERE fs.MODIFIEDDATE >= :start_time
  AND fs.MODIFIEDDATE < :end_time
""".rstrip()

    return f"""
SELECT
    fs.ID AS id,
    fs.AD_CLIENT_ID AS ad_client_id,
    fs.AD_ORG_ID AS ad_org_id,
    fs.OWNERID AS ownerid,
    fs.MODIFIERID AS modifierid,
    fs.CREATIONDATE AS creationdate,
    fs.MODIFIEDDATE AS modifieddate,
    fs.ISACTIVE AS isactive,
    fs.C_STORE_ID AS c_store_id,
    fs.M_PRODUCT_ID AS m_product_id,
    fs.M_ATTRIBUTESETINSTANCE_ID AS m_attributesetinstance_id,
    fs.QTY AS qty,
    fs.QTYPREOUT AS qtypreout,
    fs.QTYPREIN AS qtyprein,
    fs.M_PRODUCTALIAS_ID AS m_productalias_id,
    fs.QTY_FREEZE AS qty_freeze,
    fs.QTY_OMS AS qty_oms,
    fs.QTYPURCHASEREM AS qtypurchaserem,
    fs.QTYOMSTRANSLATE AS qtyomstranslate,
    fs.QTYPREOUT1 AS qtypreout1
FROM {config.source_table} fs
{where_clause}
ORDER BY fs.ID
""".strip()


def build_write_boundary(config: OdsFaStorageRawConfig) -> str:
    """输出后续人工授权时的写入边界；不返回可直接执行的批处理。"""

    if config.mode == 'modified-window':
        return f"""
    -- 写入边界（默认 dry-run）：
    -- 1. 显式追加 --execute 后，按 FA_STORAGE.MODIFIEDDATE 窗口从 Oracle 抽取并 upsert 到 {config.target_table}。
    -- 2. 写入主键为 id；重复执行同一窗口会按 id 更新，不做窗口级 DELETE，避免误删其他批次。
    -- 3. modified-window 只适合 M3 小窗口验证；不能直接等同库存全量快照。
    -- 4. MySQL timeout_profile={config.timeout_profile}；脚本不接入 run_etl.py / 总控。
""".strip()

    return f"""
-- 写入边界（默认 dry-run）：
-- 1. 全量初始化 {config.target_table} 属于长耗时操作，必须显式追加 --execute --confirm-full-load。
-- 2. 当前实现仍按 id upsert，不执行 TRUNCATE / DROP。
-- 3. FA_STORAGE 全量前需保留 timeout_profile=long_running 的耗时证据，并评估 Oracle 扫描与 MySQL 写入耗时。
""".strip()


def _source_params(config: OdsFaStorageRawConfig) -> dict[str, datetime]:
    if config.mode != 'modified-window':
        return {}
    if not config.start_time or not config.end_time:
        raise ValueError('modified-window 模式必须提供 start_time 与 end_time')
    if config.start_time >= config.end_time:
        raise ValueError('start_time 必须早于 end_time')
    return {'start_time': config.start_time, 'end_time': config.end_time}


def execute_load(config: OdsFaStorageRawConfig, *, chunk_size: int) -> int:
    """执行 raw ODS 小窗口 upsert。"""

    params = _source_params(config)
    sql = text(build_source_sql(config))
    batch_id = f"m3_fa_storage_raw_{uuid4().hex[:12]}"
    loaded_at = datetime.now()
    total_rows = 0

    oracle_engine = create_oracle_engine()
    mysql_engine = create_mysql_engine(timeout_profile=config.timeout_profile)
    try:
        with oracle_engine.connect() as oracle_conn:
            for chunk in pd.read_sql_query(sql, oracle_conn, params=params, chunksize=chunk_size):
                chunk = normalize_dataframe(chunk)
                if chunk.empty:
                    continue
                chunk['etl_batch_id'] = batch_id
                chunk['etl_loaded_at'] = loaded_at
                with mysql_engine.begin() as mysql_conn:
                    total_rows += upsert_dataframe(
                        mysql_conn,
                        config.target_table,
                        chunk,
                        LOAD_COLUMNS,
                        LOAD_COLUMNS,
                        extra_update_expressions=('`updated_at` = NOW()',),
                    )
                logger.info('ods_fa_storage_raw 已 upsert %s 行...', total_rows)
    finally:
        oracle_engine.dispose()
        mysql_engine.dispose()

    logger.info(
        'ods_fa_storage_raw 装载完成：rows=%s, batch_id=%s, timeout_profile=%s',
        total_rows,
        batch_id,
        config.timeout_profile,
    )
    return total_rows


def conn_test(timeout_profile: str) -> None:
    """只读连接测试。"""

    oracle_engine = create_oracle_engine()
    mysql_engine = create_mysql_engine(timeout_profile=timeout_profile)
    try:
        with oracle_engine.connect() as oracle_conn:
            oracle_conn.execute(text('SELECT 1 FROM DUAL'))
        with mysql_engine.connect() as mysql_conn:
            mysql_conn.execute(text('SELECT 1'))
    finally:
        oracle_engine.dispose()
        mysql_engine.dispose()
    logger.info('Oracle / MySQL conn-test 通过，MySQL timeout_profile=%s', timeout_profile)


def run(
    config: OdsFaStorageRawConfig,
    *,
    conn_test_only: bool,
    execute: bool,
    chunk_size: int,
    confirm_full_load: bool,
) -> int | None:
    if conn_test_only:
        conn_test(config.timeout_profile)
        return None

    params = _source_params(config)

    logger.info('生成 ods_fa_storage_raw 候选抽取 SQL，mode=%s', config.mode)
    print(build_source_sql(config))
    print(params)
    print(build_write_boundary(config))

    if execute:
        if config.mode == 'full' and not confirm_full_load:
            raise ValueError('full 写入必须显式追加 --confirm-full-load')
        return execute_load(config, chunk_size=chunk_size)

    logger.info('未追加 --execute，仅完成 dry-run 输出；不会写入 MySQL。')
    return None


def parse_args() -> argparse.Namespace:
    default_start, default_end = _default_window()
    parser = argparse.ArgumentParser(description='ODS raw 库存余额旁路小窗口装载。')
    parser.add_argument('--mode', choices=('full', 'modified-window'), default='modified-window')
    parser.add_argument('--start-time', default=default_start.isoformat(timespec='seconds'), help='modified-window 开始时间，ISO 格式。')
    parser.add_argument('--end-time', default=default_end.isoformat(timespec='seconds'), help='modified-window 结束时间，ISO 格式。')
    parser.add_argument('--source-table', default=DEFAULT_SOURCE_TABLE, help='Oracle 源表，默认 FA_STORAGE。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='MySQL raw 目标表名。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='long_running')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Oracle 读取与 MySQL upsert 分批大小。')
    parser.add_argument('--confirm-full-load', action='store_true', help='确认执行 full 模式写入；小窗口不需要。')
    parser.add_argument('--conn-test', action='store_true', help='只执行 Oracle / MySQL SELECT 1 连接测试。')
    parser.add_argument('--execute', action='store_true', help='显式执行 MySQL upsert；未提供时仅 dry-run。')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = OdsFaStorageRawConfig(
        mode=args.mode,
        start_time=_coerce_datetime(args.start_time) if args.mode == 'modified-window' else None,
        end_time=_coerce_datetime(args.end_time) if args.mode == 'modified-window' else None,
        source_table=args.source_table,
        target_table=args.target_table,
        timeout_profile=args.timeout_profile,
    )
    run(
        config,
        conn_test_only=args.conn_test,
        execute=args.execute,
        chunk_size=args.chunk_size,
        confirm_full_load=args.confirm_full_load,
    )


if __name__ == '__main__':
    main()
