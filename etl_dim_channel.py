# -*- coding: utf-8 -*-
"""
何方珠宝 - 电商渠道维度ETL
从Oracle O2O_RETAIL_CHANNEL同步到MySQL dim_channel
策略：全量覆盖
"""

import logging
from datetime import datetime

import oracledb
import pandas as pd
from sqlalchemy import create_engine, text

from config import ORACLE_CONFIG, ORACLE_DSN, MYSQL_CONN_STR


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


MAIN_CHANNEL_IDS = (11, 19, 28, 57, 60, 85, 300)


def extract_from_oracle():
    """从Oracle抽取电商渠道数据"""

    main_ids_sql = ",".join(str(channel_id) for channel_id in MAIN_CHANNEL_IDS)
    sql = f"""
        SELECT ch.ID AS channel_id,
               ch.NAME AS channel_name,
               ch.CODE AS channel_code,
               ch.WING_CODE AS wing_code,
               CASE
                   WHEN ch.ID IN ({main_ids_sql}) THEN 1
                   ELSE 0
               END AS is_main,
               CASE
                   WHEN ch.NAME LIKE '%天猫%' THEN '天猫'
                   WHEN ch.NAME LIKE '%京东%' THEN '京东'
                   WHEN ch.NAME LIKE '%抖音%' THEN '抖音'
                   WHEN ch.NAME LIKE '%小红书%' THEN '小红书'
                   WHEN ch.NAME LIKE '%视频号%' OR ch.NAME LIKE '%微信%' THEN '视频号'
                   WHEN ch.NAME LIKE '%唯品会%' THEN '唯品会'
                   WHEN ch.NAME LIKE '%得物%' THEN '得物'
                   ELSE '其他'
               END AS platform_type,
               ch.ISACTIVE AS is_active
          FROM O2O_RETAIL_CHANNEL ch
    """

    logger.info("连接Oracle数据库...")
    conn = oracledb.connect(
        user=ORACLE_CONFIG['user'],
        password=ORACLE_CONFIG['password'],
        dsn=ORACLE_DSN
    )

    logger.info("执行SQL查询...")
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [col[0].lower() for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    df = df.rename(columns={'wing_code': 'WING_CODE'})

    cursor.close()
    conn.close()

    logger.info(f"抽取完成，共 {len(df)} 条记录")
    return df


def transform(df):
    """数据转换清洗"""

    logger.info("开始数据转换...")

    df['channel_name'] = df['channel_name'].fillna('未知渠道')
    df['channel_code'] = df['channel_code'].fillna('UNKNOWN')
    df['platform_type'] = df['platform_type'].fillna('其他')
    df['is_active'] = df['is_active'].fillna('Y')

    df['channel_id'] = df['channel_id'].astype('int64')
    df['is_main'] = df['is_main'].astype('int')
    df['created_at'] = datetime.now()

    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df):
    """加载到MySQL"""

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    logger.info("清空目标表 dim_channel...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_channel"))

    logger.info("写入数据...")
    df.to_sql(
        name='dim_channel',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )

    logger.info(f"写入完成，共 {len(df)} 条记录")
    engine.dispose()


def run():
    """执行ETL"""

    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 dim_channel ETL")
    logger.info("=" * 50)

    try:
        df = extract_from_oracle()
        df = transform(df)
        load_to_mysql(df)

        end_time = datetime.now()
        duration = (end_time - start_time).seconds

        logger.info("=" * 50)
        logger.info(f"✓ ETL执行成功！耗时 {duration} 秒")
        logger.info("=" * 50)
        return True
    except Exception as e:
        logger.error(f"✗ ETL执行失败: {str(e)}")
        raise


if __name__ == '__main__':
    run()