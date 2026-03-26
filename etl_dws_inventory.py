# -*- coding: utf-8 -*-
"""
何方珠宝 - 库存数据ETL
从MySQL ODS ods_fa_storage 聚合到 dws_inventory_daily
策略：每日全量快照
"""

import logging
import time
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

from config import MYSQL_CONN_STR

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


RETRYABLE_MYSQL_LOCK_KEYWORDS = (
    '1213',
    '1205',
    'deadlock found',
    'lock wait timeout exceeded',
    '未能获取命名锁',
)


def _is_retryable_mysql_lock_error(exc):
    message = str(exc).lower()
    return any(keyword in message for keyword in RETRYABLE_MYSQL_LOCK_KEYWORDS)


def extract_from_ods():
    """从MySQL ODS 抽取当前库存数据"""

    # 移除了不存在的QTYOCCUPY字段
    # ⚠️ 注意：不要过滤QTY=0的记录！Oracle原SQL没有此过滤
    #         FA_STORAGE中QTY=0的记录仍然表示该商品在仓库中存在过/被管理
    sql = text("""
    SELECT
        fs.c_store_id AS store_id,
        COALESCE(s.store_code, '') AS store_code,
        COALESCE(s.is_cloud_store, 'N') AS is_cloud_store,
        fs.m_product_id AS product_id,
        fs.m_productalias_id AS m_productalias_id,
        fs.qty AS qty,
        fs.qty AS qty_valid,
        COALESCE(fs.qtypurchaserem, 0) AS qtypurchaserem
    FROM ods_fa_storage fs
    LEFT JOIN dim_store s ON fs.c_store_id = s.store_id
    WHERE fs.isactive = 'Y'
      AND fs.m_productalias_id IS NOT NULL
      AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
    """)

    logger.info("连接MySQL数据库，读取 ODS 库存数据...")
    engine = create_engine(MYSQL_CONN_STR)

    logger.info("执行SQL查询...")
    try:
        df = pd.read_sql(sql, engine)
    finally:
        engine.dispose()

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
    lock_name = 'hefang_dw:dws_inventory_daily'
    max_attempts = 3

    try:
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"删除当天旧数据（{today}）并写入新数据（单事务，第 {attempt}/{max_attempts} 次）...")
                with engine.begin() as conn:
                    got_lock = conn.execute(
                        text("SELECT GET_LOCK(:lock_name, :timeout_seconds)"),
                        {"lock_name": lock_name, "timeout_seconds": 30},
                    ).scalar()
                    if got_lock != 1:
                        raise TimeoutError(f"未能获取命名锁: {lock_name}")

                    conn.execute(text("DELETE FROM dws_inventory_daily WHERE date_id = :d"), {"d": today})
                    df.to_sql(
                        name='dws_inventory_daily',
                        con=conn,
                        if_exists='append',
                        index=False,
                        chunksize=5000,
                        method=None,
                    )

                logger.info(f"写入完成，共 {len(df)} 条记录")
                break
            except Exception as exc:
                if attempt >= max_attempts or not _is_retryable_mysql_lock_error(exc):
                    raise
                wait_seconds = attempt * 5
                logger.warning(
                    f"检测到可重试锁冲突（第 {attempt}/{max_attempts} 次）：{exc}；{wait_seconds} 秒后重试..."
                )
                time.sleep(wait_seconds)
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
        df = extract_from_ods()

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