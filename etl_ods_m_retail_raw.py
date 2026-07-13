# -*- coding: utf-8 -*-
"""ODS raw 零售单头旁路抽取。

状态：M3 小窗口 / 完整业务日期装载实现；默认 dry-run 输出 SQL，只有显式 ``--execute`` 才写入
``ods_m_retail_raw``，且不接入 run_etl.py。

设计边界：
- 目标表草案见 SQL/draft_create_ods_m_retail_raw.sql。
- 来源表为 Oracle BOSNDS3.M_RETAIL，保留 M3 白名单字段。
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


DEFAULT_SOURCE_TABLE = 'M_RETAIL'
DEFAULT_TARGET_TABLE = 'ods_m_retail_raw'

SOURCE_COLUMNS = (
    'id',
    'docno',
    'billdate',
    'c_store_id',
    'oms_sourcecode',
    'tot_amt_actual',
    'tot_amt_list',
    'tot_qty',
    'status',
    'isactive',
    'modifieddate',
    'creationdate',
    'doctype',
    'description',
    'avg_discount',
    'c_vip_id',
    'salesrep_id',
    'pay_status',
    'payerid',
    'paytime',
    'close_status',
    'closerid',
    'closetime',
    'refno',
    'isreturned',
    'retailbilltype',
    'dateout',
    'datein',
)
LOAD_COLUMNS = SOURCE_COLUMNS + ('etl_batch_id', 'etl_loaded_at')


@dataclass(frozen=True)
class OdsRetailRawConfig:
    mode: str = 'incremental'
    start_time: datetime | None = None
    end_time: datetime | None = None
    start_date: int | None = None
    end_date: int | None = None
    source_table: str = DEFAULT_SOURCE_TABLE
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


def build_source_sql(config: OdsRetailRawConfig) -> str:
    """生成 Oracle 抽取 SQL；草案阶段不自动执行。"""

    where_clause = ''
    order_clause = 'r.MODIFIEDDATE, r.ID'
    if config.mode == 'incremental':
        where_clause = """
WHERE r.MODIFIEDDATE >= :start_time
  AND r.MODIFIEDDATE < :end_time
""".rstrip()
    elif config.mode == 'business-date':
        where_clause = """
WHERE r.BILLDATE >= :start_date
  AND r.BILLDATE <= :end_date
""".rstrip()
        order_clause = 'r.BILLDATE, r.ID'

    return f"""
SELECT
    r.ID AS id,
    r.DOCNO AS docno,
    r.BILLDATE AS billdate,
    r.C_STORE_ID AS c_store_id,
    r.OMS_SOURCECODE AS oms_sourcecode,
    r.TOT_AMT_ACTUAL AS tot_amt_actual,
    r.TOT_AMT_LIST AS tot_amt_list,
    r.TOT_QTY AS tot_qty,
    r.STATUS AS status,
    r.ISACTIVE AS isactive,
    r.MODIFIEDDATE AS modifieddate,
    r.CREATIONDATE AS creationdate,
    r.DOCTYPE AS doctype,
    r.DESCRIPTION AS description,
    r.AVG_DISCOUNT AS avg_discount,
    r.C_VIP_ID AS c_vip_id,
    r.SALESREP_ID AS salesrep_id,
    r.PAY_STATUS AS pay_status,
    r.PAYERID AS payerid,
    r.PAYTIME AS paytime,
    r.CLOSE_STATUS AS close_status,
    r.CLOSERID AS closerid,
    r.CLOSETIME AS closetime,
    r.REFNO AS refno,
    r.ISRETURNED AS isreturned,
    r.RETAILBILLTYPE AS retailbilltype,
    r.DATEOUT AS dateout,
    r.DATEIN AS datein
FROM {config.source_table} r
{where_clause}
ORDER BY {order_clause}
""".strip()


def build_write_boundary(config: OdsRetailRawConfig) -> str:
    """输出后续人工授权时的写入边界；不返回可直接执行的批处理。"""

    if config.mode == 'incremental':
        return f"""
    -- 写入边界（默认 dry-run）：
    -- 1. 显式追加 --execute 后，按 MODIFIEDDATE 窗口从 Oracle 抽取并 upsert 到 {config.target_table}。
    -- 2. 写入主键为 id；重复执行同一窗口会按 id 更新，不做窗口级 DELETE，避免误删其他批次。
    -- 3. 建议小窗口先使用近 1 天；MySQL timeout_profile={config.timeout_profile}。
    -- 4. 脚本不接入 run_etl.py / 总控。
""".strip()

    if config.mode == 'business-date':
        return f"""
    -- 写入边界（默认 dry-run）：
    -- 1. 显式追加 --execute 后，按 BILLDATE 完整业务日期窗口从 Oracle 抽取并 upsert 到 {config.target_table}。
    -- 2. 写入主键为 id；重复执行同一业务日期窗口会按 id 更新，不做窗口级 DELETE，避免误删其他批次。
    -- 3. 该模式用于补齐 DWD 与 DWS 日级对账所需的完整销售单头 raw。
    -- 4. MySQL timeout_profile={config.timeout_profile}；脚本不接入 run_etl.py / 总控。
""".strip()

    return f"""
-- 写入边界（默认 dry-run）：
-- 1. 全量初始化 {config.target_table} 属于长耗时操作，必须显式追加 --execute --confirm-full-load。
-- 2. 当前实现仍按 id upsert，不执行 TRUNCATE / DROP。
-- 3. 全量前需单独评估 Oracle 扫描耗时、MySQL 写入耗时与锁等待。
""".strip()


def _source_params(config: OdsRetailRawConfig) -> dict[str, datetime | int]:
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


def execute_load(config: OdsRetailRawConfig, *, chunk_size: int) -> int:
    """执行 raw ODS 小窗口 upsert。"""

    params = _source_params(config)
    sql = text(build_source_sql(config))
    batch_id = f"m3_retail_raw_{uuid4().hex[:12]}"
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
                logger.info('ods_m_retail_raw 已 upsert %s 行...', total_rows)
    finally:
        oracle_engine.dispose()
        mysql_engine.dispose()

    logger.info(
        'ods_m_retail_raw 装载完成：rows=%s, batch_id=%s, timeout_profile=%s',
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
    config: OdsRetailRawConfig,
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

    logger.info('生成 ods_m_retail_raw 候选抽取 SQL，mode=%s', config.mode)
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
    parser = argparse.ArgumentParser(description='ODS raw 零售单头旁路小窗口 / 完整业务日期装载。')
    parser.add_argument('--mode', choices=('incremental', 'business-date', 'full'), default='incremental')
    parser.add_argument('--start-time', default=default_start.isoformat(timespec='seconds'), help='增量开始时间，ISO 格式。')
    parser.add_argument('--end-time', default=default_end.isoformat(timespec='seconds'), help='增量结束时间，ISO 格式。')
    parser.add_argument('--start-date', type=int, default=None, help='business-date 模式开始业务日期 YYYYMMDD。')
    parser.add_argument('--end-date', type=int, default=None, help='business-date 模式结束业务日期 YYYYMMDD。')
    parser.add_argument('--source-table', default=DEFAULT_SOURCE_TABLE, help='Oracle 源表，默认 M_RETAIL。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='MySQL raw 目标表名。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='etl')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Oracle 读取与 MySQL upsert 分批大小。')
    parser.add_argument('--confirm-full-load', action='store_true', help='确认执行 full 模式写入；小窗口不需要。')
    parser.add_argument('--conn-test', action='store_true', help='只执行 Oracle / MySQL SELECT 1 连接测试。')
    parser.add_argument('--execute', action='store_true', help='显式执行 MySQL upsert；未提供时仅 dry-run。')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = OdsRetailRawConfig(
        mode=args.mode,
        start_time=_coerce_datetime(args.start_time) if args.mode == 'incremental' else None,
        end_time=_coerce_datetime(args.end_time) if args.mode == 'incremental' else None,
        start_date=args.start_date if args.mode == 'business-date' else None,
        end_date=args.end_date if args.mode == 'business-date' else None,
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
