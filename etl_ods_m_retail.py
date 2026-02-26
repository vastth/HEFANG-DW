# -*- coding: utf-8 -*-
"""
何方珠宝 - ODS零售单主表ETL
从Oracle M_RETAIL同步到MySQL ods_m_retail
策略：全量覆盖
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from config import ORACLE_CONFIG, ORACLE_DSN, MYSQL_CONN_STR

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def _get_sync_state(engine, table_name):
    query = text(
        """
        SELECT last_sync, current_window_start, current_window_end, status
        FROM ods_sync_state
        WHERE table_name = :table_name
        """
    )
    with engine.connect() as conn:
        row = conn.execute(query, {"table_name": table_name}).fetchone()
        if not row:
            return None
        return {
            "last_sync": row[0],
            "current_window_start": row[1],
            "current_window_end": row[2],
            "status": row[3],
        }


def _update_sync_state(engine, table_name, last_sync, rows_written):
    sql = text(
        """
        INSERT INTO ods_sync_state
            (table_name, last_sync, updated_at, rows_written, status, current_window_start, current_window_end)
        VALUES (:table_name, :last_sync, NOW(), :rows_written, 'success', NULL, NULL)
        ON DUPLICATE KEY UPDATE
            last_sync = VALUES(last_sync),
            updated_at = NOW(),
            rows_written = VALUES(rows_written),
            status = 'success',
            current_window_start = NULL,
            current_window_end = NULL
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {
            "table_name": table_name,
            "last_sync": last_sync,
            "rows_written": rows_written,
        })


def _update_window_state(engine, table_name, window_start, window_end, status):
    sql = text(
        """
        INSERT INTO ods_sync_state
            (table_name, current_window_start, current_window_end, status, updated_at)
        VALUES (:table_name, :window_start, :window_end, :status, NOW())
        ON DUPLICATE KEY UPDATE
            current_window_start = VALUES(current_window_start),
            current_window_end = VALUES(current_window_end),
            status = VALUES(status),
            updated_at = NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, {
            "table_name": table_name,
            "window_start": window_start,
            "window_end": window_end,
            "status": status,
        })


def _get_modified_range(oracle_engine):
    sql = text("""
        SELECT MIN(r.MODIFIEDDATE) AS min_dt, MAX(r.MODIFIEDDATE) AS max_dt
        FROM M_RETAIL r
        WHERE r.MODIFIEDDATE IS NOT NULL
    """)
    with oracle_engine.connect() as conn:
        row = conn.execute(sql).fetchone()
        return (row[0], row[1]) if row else (None, None)


def extract_and_load(mode="incremental", backfill_days=7, window_days=None):
    """从Oracle抽取并写入MySQL（分批写入，避免内存峰值）"""

    if window_days is None:
        window_days = 7 if mode == "full" else 1

    sql = """
    SELECT
        r.ID AS id,
        r.DOCNO AS docno,
        r.BILLDATE AS billdate,
        r.C_STORE_ID AS c_store_id,
        r.TOT_AMT_ACTUAL AS tot_amt_actual,
        r.TOT_AMT_LIST AS tot_amt_list,
        r.TOT_QTY AS tot_qty,
        r.STATUS AS status,
        r.ISACTIVE AS isactive,
        r.MODIFIEDDATE AS modifieddate
    FROM M_RETAIL r
    """

    batch_id = int(datetime.now().strftime('%Y%m%d%H%M%S'))

    logger.info("连接Oracle数据库...")
    oracle_url = URL.create(
        "oracle+oracledb",
        username=ORACLE_CONFIG['user'],
        password=ORACLE_CONFIG['password'],
        host=ORACLE_CONFIG['host'],
        port=ORACLE_CONFIG['port'],
        database=ORACLE_CONFIG['service_name'],
    )
    oracle_engine = create_engine(oracle_url, pool_pre_ping=True, pool_recycle=1800)

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    try:
        sync_state = _get_sync_state(engine, "ods_m_retail")
        last_sync = sync_state["last_sync"] if sync_state else None
        if mode == "incremental" and last_sync:
            start_time = last_sync - timedelta(days=backfill_days)
            end_time = datetime.now()
            logger.info(f"增量模式：回刷起点 {start_time}")
        elif mode == "incremental":
            logger.info("增量模式：未找到水位，自动转为全量")
            mode = "full"
            start_time = None
            end_time = None
        else:
            start_time = None
            end_time = None

        if sync_state and sync_state.get("current_window_start") and sync_state.get("current_window_end"):
            cw_start = sync_state["current_window_start"]
            cw_end = sync_state["current_window_end"]
            if (not start_time) or cw_start >= start_time:
                start_time = cw_start
                if end_time and cw_end <= end_time:
                    logger.info(f"断点续跑：从窗口 {cw_start} ~ {cw_end} 继续")
                else:
                    logger.info(f"断点续跑：从窗口 {cw_start} 继续")

        if mode == "full":
            logger.info("清空目标表 ods_m_retail...")
            with engine.begin() as mysql_conn:
                mysql_conn.execute(text("TRUNCATE TABLE ods_m_retail"))

        logger.info("开始分批写入 ods_m_retail...")
        total_rows = 0
        max_modified = None

        if mode == "full":
            min_dt, max_dt = _get_modified_range(oracle_engine)
            if min_dt and max_dt:
                start_time = min_dt
                end_time = max_dt + timedelta(days=1)
            else:
                start_time = None
                end_time = None

            # 先处理 modifieddate 为空的记录
            null_query = sql + " WHERE r.MODIFIEDDATE IS NULL ORDER BY r.ID"
            for chunk in pd.read_sql(null_query, oracle_engine, chunksize=50000):
                chunk.columns = [c.lower() for c in chunk.columns]
                chunk['etl_batch_id'] = batch_id
                chunk['etl_loaded_at'] = datetime.now()
                chunk.to_sql(
                    name='ods_m_retail',
                    con=engine,
                    if_exists='append',
                    index=False,
                    chunksize=5000
                )
                total_rows += len(chunk)
                logger.info(f"已写入 {total_rows} 条记录")

        if start_time and end_time:
            window_start = start_time
            while window_start < end_time:
                window_end = min(window_start + timedelta(days=window_days), end_time)

                _update_window_state(engine, "ods_m_retail", window_start, window_end, "running")

                if mode == "incremental":
                    with engine.begin() as mysql_conn:
                        mysql_conn.execute(
                            text(
                                "DELETE FROM ods_m_retail "
                                "WHERE modifieddate >= :start_time AND modifieddate < :end_time"
                            ),
                            {"start_time": window_start, "end_time": window_end},
                        )

                query = sql + (
                    " WHERE r.MODIFIEDDATE >= :start_time AND r.MODIFIEDDATE < :end_time"
                    " ORDER BY r.MODIFIEDDATE, r.ID"
                )
                params = {"start_time": window_start, "end_time": window_end}
                for chunk in pd.read_sql(query, oracle_engine, chunksize=50000, params=params):
                    chunk.columns = [c.lower() for c in chunk.columns]
                    chunk['etl_batch_id'] = batch_id
                    chunk['etl_loaded_at'] = datetime.now()
                    if 'modifieddate' in chunk.columns:
                        # Normalize to datetime; ignore NaT/NaN to avoid type compare errors
                        md_series = pd.to_datetime(chunk['modifieddate'], errors='coerce')
                        chunk_max = md_series.max()
                        if pd.notna(chunk_max):
                            if max_modified is None or chunk_max > max_modified:
                                max_modified = chunk_max
                    chunk.to_sql(
                        name='ods_m_retail',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=5000
                    )
                    total_rows += len(chunk)
                    logger.info(f"已写入 {total_rows} 条记录")

                next_start = window_end
                next_end = min(window_end + timedelta(days=window_days), end_time)
                _update_window_state(engine, "ods_m_retail", next_start, next_end, "pending")
                window_start = window_end

        if mode in ("incremental", "full") and max_modified is not None:
            _update_sync_state(engine, "ods_m_retail", max_modified, total_rows)
        logger.info(f"写入完成，共 {total_rows} 条记录")
        return total_rows

    finally:
        oracle_engine.dispose()
        engine.dispose()


def run(mode="incremental", backfill_days=7, window_days=None):
    """执行ETL"""
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 ods_m_retail ETL")
    logger.info("=" * 50)

    try:
        total_rows = extract_and_load(mode=mode, backfill_days=backfill_days, window_days=window_days)
        end_time = datetime.now()
        duration = (end_time - start_time).seconds

        logger.info("=" * 50)
        logger.info(f"✓ ETL执行成功！耗时 {duration} 秒，写入 {total_rows} 条")
        logger.info("=" * 50)
        return True

    except Exception as e:
        logger.error(f"✗ ETL执行失败: {str(e)}")
        raise


if __name__ == '__main__':
    run()
