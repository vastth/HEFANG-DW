# -*- coding: utf-8 -*-
"""
何方珠宝 - SKU维度ETL
从Oracle M_PRODUCT_ALIAS同步到MySQL dim_sku
策略：全量覆盖

修复说明（2026-01-29）：
- 解决Oracle保留字冲突：COLOR/SIZE → sku_color/sku_size
- Oracle保留字不能直接用作列别名
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
    """从Oracle抽取SKU维度数据"""

    # ⚠️ 注意：COLOR/SIZE 是 Oracle 保留字，必须改为 sku_color/sku_size
    sql = """
    SELECT
        pa.ID AS sku_id,
        pa.NO AS sku_barcode,
        pa.M_PRODUCT_ID AS product_id,
        asi.VALUE1 AS sku_color,
        asi.VALUE2 AS sku_size,
        pa.ISACTIVE AS is_active,
        pa.CREATIONDATE AS created_at
    FROM M_PRODUCT_ALIAS pa
    LEFT JOIN M_ATTRIBUTESETINSTANCE asi ON pa.M_ATTRIBUTESETINSTANCE_ID = asi.ID
    WHERE pa.ISACTIVE = 'Y'
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

    if df.empty:
        logger.warning("没有数据需要处理")
        return df

    # 类型转换
    df['sku_id'] = df['sku_id'].astype('int64')
    df['product_id'] = df['product_id'].astype('int64')

    # 空值处理
    df['sku_barcode'] = df['sku_barcode'].fillna('')
    df['sku_color'] = df['sku_color'].fillna('')
    df['sku_size'] = df['sku_size'].fillna('')  # ⚠️ 改为 sku_size
    df['is_active'] = df['is_active'].fillna('Y')

    # 更新时间戳
    df['updated_at'] = datetime.now()

    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df):
    """加载到MySQL（全量覆盖）"""

    if df.empty:
        logger.warning("没有数据需要写入")
        return

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    logger.info("清空目标表 dim_sku...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_sku"))

    logger.info("写入数据...")
    df.to_sql(
        name='dim_sku',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=5000
    )

    logger.info(f"写入完成，共 {len(df)} 条记录")
    engine.dispose()


def run():
    """执行ETL"""

    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 dim_sku ETL")
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