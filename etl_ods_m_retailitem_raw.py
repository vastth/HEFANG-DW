# -*- coding: utf-8 -*-
"""ODS raw 零售明细旁路抽取。

状态：M3 小窗口 / 完整业务日期装载实现；默认 dry-run 输出 SQL，只有显式 ``--execute`` 才写入
``ods_m_retailitem_raw``，且不接入 run_etl.py。

设计边界：
- 目标表草案见 SQL/draft_create_ods_m_retailitem_raw.sql。
- 来源表为 Oracle BOSNDS3.M_RETAILITEM，保留 M3 白名单字段。
- 明细增量需同时关注 MODIFIEDDATE 与 MODIFIEDDATE 为空时的 SETTIME。
- 所有真实 CREATE / DELETE / INSERT / 回填由用户人工授权执行。
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


DEFAULT_SOURCE_TABLE = 'M_RETAILITEM'
DEFAULT_RETAIL_TABLE = 'M_RETAIL'
DEFAULT_TARGET_TABLE = 'ods_m_retailitem_raw'

SOURCE_COLUMNS = (
    'id',
    'm_retail_id',
    'm_product_id',
    'm_productalias_id',
    'm_attributesetinstance_id',
    'qty',
    'pricelist',
    'priceactual',
    'tot_amt_actual',
    'tot_amt_list',
    'modifieddate',
    'settime',
    'orderno',
    'c_vip_id',
    'salesrep_id',
    'discount',
    'description',
    'status',
    'type',
    'rqty',
    'salesreps_id',
    'salesreps_name',
    'rcanqty',
    'm_retailitem_id',
)
LOAD_COLUMNS = SOURCE_COLUMNS + ('etl_batch_id', 'etl_loaded_at')


@dataclass(frozen=True)
class OdsRetailItemRawConfig:
    mode: str = 'incremental'
    start_time: datetime | None = None
    end_time: datetime | None = None
    start_date: int | None = None
    end_date: int | None = None
    source_table: str = DEFAULT_SOURCE_TABLE
    retail_table: str = DEFAULT_RETAIL_TABLE
    target_table: str = DEFAULT_TARGET_TABLE
    timeout_profile: str = 'etl'


def _default_window(days_back: int = 1) -> tuple[datetime, datetime]:
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days_back)
    return start_time, end_time


def _coerce_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def build_source_sql(config: OdsRetailItemRawConfig) -> str:
    """生成 Oracle 抽取 SQL；草案阶段不自动执行。"""

    where_clause = ''
    join_clause = ''
    order_clause = 'COALESCE(ri.MODIFIEDDATE, ri.SETTIME), ri.ID'
    if config.mode == 'incremental':
        where_clause = """
WHERE (
        ri.MODIFIEDDATE >= :start_time
    AND ri.MODIFIEDDATE < :end_time
)
OR (
        ri.MODIFIEDDATE IS NULL
    AND ri.SETTIME >= :start_time
    AND ri.SETTIME < :end_time
)
""".rstrip()
    elif config.mode == 'business-date':
        join_clause = f"""
JOIN {config.retail_table} r
    ON ri.M_RETAIL_ID = r.ID
""".rstrip()
        where_clause = """
WHERE r.BILLDATE >= :start_date
  AND r.BILLDATE <= :end_date
""".rstrip()
        order_clause = 'r.BILLDATE, ri.ID'

    return f"""
SELECT
    ri.ID AS id,
    ri.M_RETAIL_ID AS m_retail_id,
    ri.M_PRODUCT_ID AS m_product_id,
    ri.M_PRODUCTALIAS_ID AS m_productalias_id,
    ri.M_ATTRIBUTESETINSTANCE_ID AS m_attributesetinstance_id,
    ri.QTY AS qty,
    ri.PRICELIST AS pricelist,
    ri.PRICEACTUAL AS priceactual,
    ri.TOT_AMT_ACTUAL AS tot_amt_actual,
    ri.TOT_AMT_LIST AS tot_amt_list,
    ri.MODIFIEDDATE AS modifieddate,
    ri.SETTIME AS settime,
    ri.ORDERNO AS orderno,
    ri.C_VIP_ID AS c_vip_id,
    ri.SALESREP_ID AS salesrep_id,
    ri.DISCOUNT AS discount,
    ri.DESCRIPTION AS description,
    ri.STATUS AS status,
    ri.TYPE AS type,
    ri.RQTY AS rqty,
    ri.SALESREPS_ID AS salesreps_id,
    ri.SALESREPS_NAME AS salesreps_name,
    ri.RCANQTY AS rcanqty,
    ri.M_RETAILITEM_ID AS m_retailitem_id
FROM {config.source_table} ri
{join_clause}
{where_clause}
ORDER BY {order_clause}
""".strip()


def build_write_boundary(config: OdsRetailItemRawConfig) -> str:
    """输出后续人工授权时的写入边界；不返回可直接执行的批处理。"""

    if config.mode == 'incremental':
        return f"""
    -- 写入边界（默认 dry-run）：
    -- 1. 显式追加 --execute 后，按 MODIFIEDDATE / SETTIME 双水位窗口从 Oracle 抽取并 upsert 到 {config.target_table}。
    -- 2. 写入主键为 id；重复执行同一窗口会按 id 更新，不做窗口级 DELETE，避免误删其他批次。
    -- 3. 建议小窗口先使用近 1 天；MySQL timeout_profile={config.timeout_profile}。
    -- 4. 脚本不接入 run_etl.py / 总控。
""".strip()

    if config.mode == 'business-date':
        return f"""
    -- 写入边界（默认 dry-run）：
    -- 1. 显式追加 --execute 后，按 M_RETAIL.BILLDATE 完整业务日期窗口抽取对应 M_RETAILITEM 明细并 upsert 到 {config.target_table}。
    -- 2. 写入主键为 id；重复执行同一业务日期窗口会按 id 更新，不做窗口级 DELETE，避免误删其他批次。
    -- 3. 该模式用于补齐 DWD 与 DWS 日级对账所需的完整销售明细 raw。
    -- 4. MySQL timeout_profile={config.timeout_profile}；脚本不接入 run_etl.py / 总控。
""".strip()

    return f"""
-- 写入边界（默认 dry-run）：
-- 1. 全量初始化 {config.target_table} 属于长耗时操作，必须显式追加 --execute --confirm-full-load。
-- 2. 当前实现仍按 id upsert，不执行 TRUNCATE / DROP。
-- 3. 全量前需单独评估 Oracle 扫描耗时、MySQL 写入耗时与锁等待。
""".strip()


def _source_params(config: OdsRetailItemRawConfig) -> dict[str, datetime | int]:
    if config.mode == 'full':
        return {}
    if config.mode == 'incremental':
        if not config.start_time or not config.end_time:
            raise ValueError('incremental 模式必须提供 start_time 与 end_time')
        if config.start_time >= config.end_time:
            raise ValueError('start_time 必须早于 end_time')
        return {'start_time': config.start_time, 'end_time': config.end_time}
    if config.mode == 'business-date':
        if config.start_date is None or config.end_date is None:
            raise ValueError('business-date 模式必须提供 start_date 与 end_date')
        if config.start_date > config.end_date:
            raise ValueError('start_date 必须早于或等于 end_date')
        return {'start_date': config.start_date, 'end_date': config.end_date}
    raise ValueError(f'不支持的装载模式: {config.mode}')


def execute_load(config: OdsRetailItemRawConfig, *, chunk_size: int) -> int:
    """执行 raw ODS 小窗口 upsert。"""

    params = _source_params(config)
    sql = text(build_source_sql(config))
    batch_id = f"m3_retailitem_raw_{uuid4().hex[:12]}"
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
                logger.info('ods_m_retailitem_raw 已 upsert %s 行...', total_rows)
    finally:
        oracle_engine.dispose()
        mysql_engine.dispose()

    logger.info(
        'ods_m_retailitem_raw 装载完成：rows=%s, batch_id=%s, timeout_profile=%s',
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
    config: OdsRetailItemRawConfig,
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

    logger.info('生成 ods_m_retailitem_raw 候选抽取 SQL，mode=%s', config.mode)
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
    parser = argparse.ArgumentParser(description='ODS raw 零售明细旁路小窗口 / 完整业务日期装载。')
    parser.add_argument('--mode', choices=('incremental', 'business-date', 'full'), default='incremental')
    parser.add_argument('--start-time', default=default_start.isoformat(timespec='seconds'), help='增量开始时间，ISO 格式。')
    parser.add_argument('--end-time', default=default_end.isoformat(timespec='seconds'), help='增量结束时间，ISO 格式。')
    parser.add_argument('--start-date', type=int, default=None, help='business-date 模式开始业务日期 YYYYMMDD。')
    parser.add_argument('--end-date', type=int, default=None, help='business-date 模式结束业务日期 YYYYMMDD。')
    parser.add_argument('--source-table', default=DEFAULT_SOURCE_TABLE, help='Oracle 源表，默认 M_RETAILITEM。')
    parser.add_argument('--retail-table', default=DEFAULT_RETAIL_TABLE, help='business-date 模式关联的 Oracle 单头表，默认 M_RETAIL。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='MySQL raw 目标表名。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='etl')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Oracle 读取与 MySQL upsert 分批大小。')
    parser.add_argument('--confirm-full-load', action='store_true', help='确认执行 full 模式写入；小窗口不需要。')
    parser.add_argument('--conn-test', action='store_true', help='只执行 Oracle / MySQL SELECT 1 连接测试。')
    parser.add_argument('--execute', action='store_true', help='显式执行 MySQL upsert；未提供时仅 dry-run。')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = OdsRetailItemRawConfig(
        mode=args.mode,
        start_time=_coerce_datetime(args.start_time) if args.mode == 'incremental' else None,
        end_time=_coerce_datetime(args.end_time) if args.mode == 'incremental' else None,
        start_date=args.start_date if args.mode == 'business-date' else None,
        end_date=args.end_date if args.mode == 'business-date' else None,
        source_table=args.source_table,
        retail_table=args.retail_table,
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
