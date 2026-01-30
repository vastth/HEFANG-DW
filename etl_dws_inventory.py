# -*- coding: utf-8 -*-
"""
何方珠宝 - 库存数据ETL
从Oracle FA_STORAGE同步到MySQL dws_inventory_daily
策略：每日全量快照
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
    """从Oracle抽取当前库存数据"""

    # 移除了不存在的QTYOCCUPY字段
    # ⚠️ 注意：不要过滤QTY=0的记录！Oracle原SQL没有此过滤
    #         FA_STORAGE中QTY=0的记录仍然表示该商品在仓库中存在过/被管理
    sql = """
    SELECT
        fs.C_STORE_ID AS store_id,
        s.CODE AS store_code,
        NVL(s.IS_ALLO2OSTORAGE, 'N') AS is_cloud_store,
        fs.M_PRODUCT_ID AS product_id,
        fs.M_PRODUCTALIAS_ID AS m_productalias_id,
        fs.QTY AS qty,
        fs.QTY AS qty_valid,
        NVL(fs.QTYPURCHASEREM, 0) AS qtypurchaserem
    FROM FA_STORAGE fs
    LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
    LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
    WHERE fs.ISACTIVE = 'Y'
        AND fs.M_PRODUCTALIAS_ID IS NOT NULL
        AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
        AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
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

    # 转换数据类型
    df['store_id'] = df['store_id'].astype('int64')
    df['product_id'] = df['product_id'].astype('int64')
    if 'm_productalias_id' in df.columns:
        df['m_productalias_id'] = df['m_productalias_id'].astype('Int64')
    else:
        df['m_productalias_id'] = pd.Series([pd.NA] * len(df), dtype='Int64')

    # 处理空值
    df['qty'] = df['qty'].fillna(0)
    df['qty_valid'] = df['qty_valid'].fillna(0)
    if 'store_code' in df.columns:
        df['store_code'] = df['store_code'].fillna('')
    else:
        df['store_code'] = ''
    if 'is_cloud_store' in df.columns:
        df['is_cloud_store'] = df['is_cloud_store'].fillna('N')
    else:
        df['is_cloud_store'] = 'N'

    # 去重：如果同一个(store_id, product_id, m_productalias_id)有多条记录，合并数量与在途采购欠数
    duplicate_count = len(df) - len(df.groupby(['store_id', 'product_id', 'm_productalias_id']).size())
    if duplicate_count > 0:
        logger.warning(f"发现 {duplicate_count} 条重复记录，将按(store_id, product_id, m_productalias_id)合并数量")
        df = df.groupby(['store_id', 'product_id', 'm_productalias_id'], as_index=False).agg({
            'qty': 'sum',
            'qty_valid': 'sum',
            'qtypurchaserem': 'sum'
        })

    # 添加占用数量（设为0，因为源表没有这个字段）
    df['qty_occupy'] = 0

    # 添加ETL时间戳
    df['etl_time'] = datetime.now()

    # 添加快照日期（在去重后添加，避免groupby时出错）
    today = int(datetime.now().strftime('%Y%m%d'))
    df['date_id'] = today

    # 调整列顺序（匹配MySQL表结构，新增 qtypurchaserem）
    if 'qtypurchaserem' not in df.columns:
        df['qtypurchaserem'] = 0
    df = df[['date_id', 'store_id', 'store_code', 'is_cloud_store', 'product_id', 'm_productalias_id', 'qty', 'qty_valid', 'qty_occupy', 'qtypurchaserem', 'etl_time']]

    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df):
    """加载到MySQL（当日快照覆盖）

    将删除与写入置于同一事务中，异常自动回滚，避免连接处于无效事务状态。
    """

    if df.empty:
        logger.warning("没有数据需要写入")
        return

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    today = int(datetime.now().strftime('%Y%m%d'))

    try:
        logger.info(f"删除当天旧数据（{today}）并写入新数据（单事务）...")
        # 使用同一事务执行删除与批量插入；若中途失败，SQLAlchemy将回滚事务
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM dws_inventory_daily WHERE date_id = :d"), {"d": today})
            df.to_sql(
                name='dws_inventory_daily',
                con=conn,
                if_exists='append',
                index=False,
                chunksize=5000,
                method=None  # 走默认插入方式以保证事务一致性
            )

        logger.info(f"写入完成，共 {len(df)} 条记录")
    finally:
        engine.dispose()


def run():
    """执行ETL"""

    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 dws_inventory_daily ETL")
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