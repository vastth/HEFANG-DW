# -*- coding: utf-8 -*-
"""
何方珠宝 - 店仓维度ETL
从Oracle C_STORE同步到MySQL dim_store
策略：全量覆盖
"""

import oracledb
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging

from config import ORACLE_CONFIG, ORACLE_DSN, MYSQL_CONN_STR

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_from_oracle():
    """从Oracle抽取店仓数据"""

    # 移除了不存在的CREATED和UPDATED字段
    sql = """
          SELECT s.ID                         AS store_id, \
                 s.CODE                       AS store_code, \
                 s.NAME                       AS store_name, \
                 s.C_AREA_ID                  AS area_id, \
                 a.NAME                       AS area_name, \
                 s.DM_ISWAREHOUSE             AS is_warehouse, \
                 s.DM_ISSTORE                 AS is_store, \
                 NVL(s.IS_ALLO2OSTORAGE, 'N') AS is_cloud_store, \
                 NVL(s.ISCENTER, 'N')         AS is_center, \
                 CASE \
                     WHEN s.CODE = '001' THEN '总仓' \
                     WHEN s.CODE LIKE 'DS%' THEN '电商' \
                     WHEN s.CODE LIKE 'RT%' THEN '门店' \
                     WHEN s.CODE LIKE 'CS%' THEN '测试' \
                     ELSE '功能仓' \
                     END                      AS store_type, \
                 s.ISACTIVE                   AS is_active
          FROM C_STORE s
                   LEFT JOIN C_AREA a ON s.C_AREA_ID = a.ID
          WHERE s.ISACTIVE = 'Y' \
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

    cursor.close()
    conn.close()

    logger.info(f"抽取完成，共 {len(df)} 条记录")
    return df


def transform(df):
    """数据转换清洗"""

    logger.info("开始数据转换...")

    # 处理空值
    df['area_name'] = df['area_name'].fillna('未知区域')
    df['is_warehouse'] = df['is_warehouse'].fillna(0)
    df['is_store'] = df['is_store'].fillna(0)

    # 转换数据类型
    df['store_id'] = df['store_id'].astype('int64')
    df['area_id'] = df['area_id'].fillna(0).astype('int64')
    df['is_warehouse'] = df['is_warehouse'].astype('int')
    df['is_store'] = df['is_store'].astype('int')

    # 添加ETL时间戳
    df['created_at'] = datetime.now()

    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df):
    """加载到MySQL"""

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    # 全量覆盖
    logger.info("清空目标表 dim_store...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_store"))

    logger.info("写入数据...")
    df.to_sql(
        name='dim_store',
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
    logger.info("开始执行 dim_store ETL")
    logger.info("=" * 50)

    try:
        # Extract
        df = extract_from_oracle()

        # Transform
        df = transform(df)

        # Load
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