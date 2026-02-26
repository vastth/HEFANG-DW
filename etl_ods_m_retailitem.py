# -*- coding: utf-8 -*-
"""
何方珠宝 - ODS零售单明细表ETL
从Oracle M_RETAILITEM同步到MySQL ods_m_retailitem
策略：全量覆盖
"""

import logging
from datetime import datetime, timedelta
from uuid import uuid4

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
        SELECT MIN(ri.MODIFIEDDATE) AS min_dt, MAX(ri.MODIFIEDDATE) AS max_dt
        FROM M_RETAILITEM ri
        WHERE ri.MODIFIEDDATE IS NOT NULL
    """)
    with oracle_engine.connect() as conn:
        row = conn.execute(sql).fetchone()
        return (row[0], row[1]) if row else (None, None)


def _get_settime_range(oracle_engine):
    sql = text("""
        SELECT MIN(ri.SETTIME) AS min_dt, MAX(ri.SETTIME) AS max_dt
        FROM M_RETAILITEM ri
        WHERE ri.MODIFIEDDATE IS NULL AND ri.SETTIME IS NOT NULL
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
        ri.ID AS id,
        ri.M_RETAIL_ID AS m_retail_id,
        ri.M_PRODUCT_ID AS m_product_id,
        ri.M_PRODUCTALIAS_ID AS m_productalias_id,
        ri.QTY AS qty,
        ri.PRICELIST AS pricelist,
        ri.PRICEACTUAL AS priceactual,
        ri.TOT_AMT_ACTUAL AS tot_amt_actual,
        ri.TOT_AMT_LIST AS tot_amt_list,
        ri.MODIFIEDDATE AS modifieddate,
        ri.SETTIME AS settime
    FROM M_RETAILITEM ri
    """

    batch_id = uuid4().hex

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
        sync_state = _get_sync_state(engine, "ods_m_retailitem")
        last_sync = sync_state["last_sync"] if sync_state else None
        settime_state = _get_sync_state(engine, "ods_m_retailitem_settime") if mode == "incremental" else None
        last_settime = settime_state["last_sync"] if settime_state else None
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

        set_start_time = None
        set_end_time = None
        if mode == "incremental":
            if last_settime:
                set_start_time = last_settime - timedelta(days=backfill_days)
                set_end_time = datetime.now()
                logger.info(f"增量模式：SETTIME 回刷起点 {set_start_time}")
            else:
                min_set, max_set = _get_settime_range(oracle_engine)
                if min_set and max_set:
                    set_start_time = min_set
                    set_end_time = max_set + timedelta(days=1)
                    logger.info("增量模式：SETTIME 无水位，执行全量回刷")

        if sync_state and sync_state.get("current_window_start") and sync_state.get("current_window_end"):
            cw_start = sync_state["current_window_start"]
            cw_end = sync_state["current_window_end"]
            if (not start_time) or cw_start >= start_time:
                start_time = cw_start
                if end_time and cw_end <= end_time:
                    logger.info(f"断点续跑：从窗口 {cw_start} ~ {cw_end} 继续")
                else:
                    logger.info(f"断点续跑：从窗口 {cw_start} 继续")

        if settime_state and settime_state.get("current_window_start") and settime_state.get("current_window_end"):
            st_start = settime_state["current_window_start"]
            st_end = settime_state["current_window_end"]
            if (not set_start_time) or st_start >= set_start_time:
                set_start_time = st_start
                if set_end_time and st_end <= set_end_time:
                    logger.info(f"SETTIME 断点续跑：从窗口 {st_start} ~ {st_end} 继续")
                else:
                    logger.info(f"SETTIME 断点续跑：从窗口 {st_start} 继续")

        if mode == "full":
            logger.info("清空目标表 ods_m_retailitem...")
            with engine.begin() as mysql_conn:
                mysql_conn.execute(text("TRUNCATE TABLE ods_m_retailitem"))

        logger.info("开始分批写入 ods_m_retailitem...")
        total_rows = 0
        max_modified = None
        max_settime = None
        settime_rows = 0

        if mode == "full":
            min_dt, max_dt = _get_modified_range(oracle_engine)
            if min_dt and max_dt:
                start_time = min_dt
                end_time = max_dt + timedelta(days=1)
            else:
                start_time = None
                end_time = None

            min_set, max_set = _get_settime_range(oracle_engine)
            if min_set and max_set:
                max_settime = max_set

            # 先处理 modifieddate 为空的记录
            null_query = sql + " WHERE ri.MODIFIEDDATE IS NULL ORDER BY ri.ID"
            for chunk in pd.read_sql(null_query, oracle_engine, chunksize=50000):
                chunk.columns = [c.lower() for c in chunk.columns]
                chunk['etl_batch_id'] = batch_id
                chunk['etl_loaded_at'] = datetime.now()
                chunk.to_sql(
                    name='ods_m_retailitem',
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

                _update_window_state(engine, "ods_m_retailitem", window_start, window_end, "running")

                if mode == "incremental":
                    with engine.begin() as mysql_conn:
                        mysql_conn.execute(
                            text(
                                "DELETE FROM ods_m_retailitem "
                                "WHERE modifieddate >= :start_time AND modifieddate < :end_time"
                            ),
                            {"start_time": window_start, "end_time": window_end},
                        )

                query = sql + (
                    " WHERE ri.MODIFIEDDATE >= :start_time AND ri.MODIFIEDDATE < :end_time"
                    " ORDER BY ri.MODIFIEDDATE, ri.ID"
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
                        name='ods_m_retailitem',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=5000
                    )
                    total_rows += len(chunk)
                    logger.info(f"已写入 {total_rows} 条记录")

                next_start = window_end
                next_end = min(window_end + timedelta(days=window_days), end_time)
                _update_window_state(engine, "ods_m_retailitem", next_start, next_end, "pending")
                window_start = window_end

        if mode == "incremental" and set_start_time and set_end_time:
            window_start = set_start_time
            while window_start < set_end_time:
                window_end = min(window_start + timedelta(days=window_days), set_end_time)

                _update_window_state(engine, "ods_m_retailitem_settime", window_start, window_end, "running")

                with engine.begin() as mysql_conn:
                    mysql_conn.execute(
                        text(
                            "DELETE FROM ods_m_retailitem "
                            "WHERE modifieddate IS NULL AND settime >= :start_time AND settime < :end_time"
                        ),
                        {"start_time": window_start, "end_time": window_end},
                    )

                query = sql + (
                    " WHERE ri.MODIFIEDDATE IS NULL AND ri.SETTIME >= :start_time AND ri.SETTIME < :end_time"
                    " ORDER BY ri.SETTIME, ri.ID"
                )
                params = {"start_time": window_start, "end_time": window_end}
                for chunk in pd.read_sql(query, oracle_engine, chunksize=50000, params=params):
                    chunk.columns = [c.lower() for c in chunk.columns]
                    chunk['etl_batch_id'] = batch_id
                    chunk['etl_loaded_at'] = datetime.now()
                    if 'settime' in chunk.columns:
                        st_series = pd.to_datetime(chunk['settime'], errors='coerce')
                        st_max = st_series.max()
                        if pd.notna(st_max):
                            if max_settime is None or st_max > max_settime:
                                max_settime = st_max
                    chunk.to_sql(
                        name='ods_m_retailitem',
                        con=engine,
                        if_exists='append',
                        index=False,
                        chunksize=5000
                    )
                    total_rows += len(chunk)
                    settime_rows += len(chunk)
                    logger.info(f"已写入 {total_rows} 条记录")

                next_start = window_end
                next_end = min(window_end + timedelta(days=window_days), set_end_time)
                _update_window_state(engine, "ods_m_retailitem_settime", next_start, next_end, "pending")
                window_start = window_end

        if mode in ("incremental", "full") and max_modified is not None:
            _update_sync_state(engine, "ods_m_retailitem", max_modified, total_rows)
        if mode in ("incremental", "full") and max_settime is not None:
            _update_sync_state(
                engine,
                "ods_m_retailitem_settime",
                max_settime,
                settime_rows if mode == "incremental" else 0,
            )
        logger.info(f"写入完成，共 {total_rows} 条记录")
        return total_rows

    finally:
        oracle_engine.dispose()
        engine.dispose()


def run(mode="incremental", backfill_days=7, window_days=None):
    """执行ETL"""
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 ods_m_retailitem ETL")
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
