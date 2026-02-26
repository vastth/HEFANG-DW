# -*- coding: utf-8 -*-
"""
何方珠宝 - ODS库存表ETL
从Oracle FA_STORAGE同步到MySQL ods_fa_storage
策略：全量覆盖
"""

import logging
from datetime import datetime
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


def extract_and_load():
    """从Oracle抽取并写入MySQL（分批写入，避免内存峰值）"""

    sql = """
    SELECT
        fs.ID AS id,
        fs.C_STORE_ID AS c_store_id,
        fs.M_PRODUCT_ID AS m_product_id,
        fs.M_PRODUCTALIAS_ID AS m_productalias_id,
        fs.QTY AS qty,
        fs.QTYVALID AS qtyvalid,
        fs.ISACTIVE AS isactive
    FROM FA_STORAGE fs
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
    oracle_engine = create_engine(oracle_url)

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    try:
        logger.info("清空目标表 ods_fa_storage...")
        with engine.begin() as mysql_conn:
            mysql_conn.execute(text("TRUNCATE TABLE ods_fa_storage"))

        logger.info("开始分批写入 ods_fa_storage...")
        total_rows = 0
        for chunk in pd.read_sql(sql, oracle_engine, chunksize=50000):
            chunk.columns = [c.lower() for c in chunk.columns]
            chunk['etl_batch_id'] = batch_id
            chunk['etl_loaded_at'] = datetime.now()
            chunk.to_sql(
                name='ods_fa_storage',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=5000
            )
            total_rows += len(chunk)
            logger.info(f"已写入 {total_rows} 条记录")

        logger.info(f"写入完成，共 {total_rows} 条记录")
        return total_rows

    finally:
        oracle_engine.dispose()
        engine.dispose()


def run():
    """执行ETL"""
    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 ods_fa_storage ETL")
    logger.info("=" * 50)

    try:
        total_rows = extract_and_load()
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
