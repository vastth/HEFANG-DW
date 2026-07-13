# -*- coding: utf-8 -*-
"""
何方珠宝 - 销售数据ETL
从MySQL ODS ods_m_retail/ods_m_retailitem 聚合到 dws_sales_daily
策略：增量同步（按日期）
"""

import pandas as pd
from sqlalchemy import text
from datetime import datetime, timedelta
import logging
import sys
import time

from db_connections import create_mysql_engine

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


def extract_from_ods(start_date, end_date):
    """从MySQL ODS 聚合销售数据"""

    sql = text("""
    SELECT
        r.billdate AS date_id,
        r.c_store_id AS store_id,
        COALESCE(s.store_code, '') AS store_code,
        COALESCE(s.is_cloud_store, 'N') AS is_cloud_store,
        ri.m_product_id AS product_id,
        ri.m_productalias_id AS m_productalias_id,
        SUM(CASE WHEN r.tot_amt_actual > 0 OR (r.tot_amt_actual = 0 AND ri.qty > 0) THEN ri.qty ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN r.tot_amt_actual > 0 OR (r.tot_amt_actual = 0 AND ri.qty > 0) THEN ri.tot_amt_actual ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN r.tot_amt_actual > 0 OR (r.tot_amt_actual = 0 AND ri.qty > 0) THEN ri.tot_amt_list ELSE 0 END) AS sales_amount_list,
        SUM(CASE WHEN r.tot_amt_actual < 0 OR (r.tot_amt_actual = 0 AND ri.qty < 0) THEN ABS(ri.qty) ELSE 0 END) AS return_qty,
        SUM(CASE WHEN r.tot_amt_actual < 0 OR (r.tot_amt_actual = 0 AND ri.qty < 0) THEN ABS(ri.tot_amt_actual) ELSE 0 END) AS return_amount,
        COUNT(DISTINCT CASE WHEN r.tot_amt_actual > 0 OR (r.tot_amt_actual = 0 AND ri.qty > 0) THEN r.id END) AS order_count
    FROM ods_m_retailitem ri
    INNER JOIN ods_m_retail r ON ri.m_retail_id = r.id
    LEFT JOIN dim_store s ON r.c_store_id = s.store_id
    WHERE r.isactive = 'Y'
      AND r.status = 2
      AND r.billdate >= :start_date
      AND r.billdate <= :end_date
      AND ri.m_productalias_id IS NOT NULL
    GROUP BY r.billdate, r.c_store_id, COALESCE(s.store_code, ''), COALESCE(s.is_cloud_store, 'N'), ri.m_product_id, ri.m_productalias_id
    """)

    logger.info("连接MySQL数据库，聚合 ODS 销售数据...")
    engine = create_mysql_engine()

    logger.info(f"执行SQL查询（日期范围：{start_date} - {end_date}）...")
    try:
        df = pd.read_sql(sql, engine, params={"start_date": start_date, "end_date": end_date})
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
    df['date_id'] = df['date_id'].astype('int64')
    df['store_id'] = df['store_id'].astype('int64')
    df['product_id'] = df['product_id'].astype('int64')
    if 'm_productalias_id' in df.columns:
        df['m_productalias_id'] = df['m_productalias_id'].astype('Int64')
    else:
        df['m_productalias_id'] = pd.Series([pd.NA] * len(df), dtype='Int64')

    # store_code/is_cloud_store 现由 dim_store 回补
    if 'store_code' in df.columns:
        df['store_code'] = df['store_code'].fillna('')
    else:
        df['store_code'] = ''
    if 'is_cloud_store' in df.columns:
        df['is_cloud_store'] = df['is_cloud_store'].fillna('N')
    else:
        df['is_cloud_store'] = 'N'
    
    # 处理空值
    numeric_cols = ['sales_qty', 'sales_amount', 'sales_amount_list', 
                    'return_qty', 'return_amount', 'order_count']
    for col in numeric_cols:
        df[col] = df[col].fillna(0)

    integer_cols = ['sales_qty', 'return_qty', 'order_count']
    for col in integer_cols:
        df[col] = df[col].round().astype('int64')
    
    # 添加ETL时间戳
    df['etl_time'] = datetime.now()
    
    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df, start_date, end_date):
    """加载到MySQL（增量：先删后插）"""
    
    if df.empty:
        logger.warning("没有数据需要写入")
        return
    
    logger.info("连接MySQL数据库...")
    engine = create_mysql_engine()
    lock_name = 'hefang_dw:dws_sales_daily'
    max_attempts = 3
    
    try:
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(
                    f"删除旧数据（{start_date} - {end_date}）并写入新数据（单事务，第 {attempt}/{max_attempts} 次）..."
                )
                with engine.begin() as conn:
                    got_lock = conn.execute(
                        text("SELECT GET_LOCK(:lock_name, :timeout_seconds)"),
                        {"lock_name": lock_name, "timeout_seconds": 30},
                    ).scalar()
                    if got_lock != 1:
                        raise TimeoutError(f"未能获取命名锁: {lock_name}")

                    conn.execute(
                        text("DELETE FROM dws_sales_daily WHERE date_id >= :start_date AND date_id <= :end_date"),
                        {"start_date": start_date, "end_date": end_date},
                    )

                    df.to_sql(
                        name='dws_sales_daily',
                        con=conn,
                        if_exists='append',
                        index=False,
                        chunksize=5000,
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


def run(days_back=1, include_today=False):
    """
    执行ETL（智能判断模式）
    days_back: 回溯天数，默认1（只同步昨天/今天）
    include_today: 是否启用智能模式，默认False
    """
    
    start_time = datetime.now()
    logger.info("="*50)
    logger.info("开始执行 dws_sales_daily ETL")
    logger.info("="*50)
    
    # 计算日期范围（智能判断）
    current_time = datetime.now()
    current_hour = current_time.hour
    if include_today:
        if 0 <= current_hour < 6:
            # 凌晨执行：查询昨天完整数据
            end_dt = current_time - timedelta(days=1)
            logger.info("模式：凌晨执行，查询昨天完整数据")
        else:
            # 白天执行：查询今天实时数据
            end_dt = current_time
            logger.info("模式：白天执行，查询今天实时数据")
    else:
        # 强制查询昨天
        end_dt = current_time - timedelta(days=1)
        logger.info("模式：强制查询昨天数据")

    start_dt = end_dt - timedelta(days=days_back-1)
    
    start_date = int(start_dt.strftime('%Y%m%d'))
    end_date = int(end_dt.strftime('%Y%m%d'))
    
    logger.info(f"同步日期范围：{start_date} - {end_date}")
    
    try:
        # Extract
        df = extract_from_ods(start_date, end_date)
        
        # Transform
        df = transform(df)
        
        # Load
        load_to_mysql(df, start_date, end_date)
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        
        logger.info("="*50)
        logger.info(f"✓ ETL执行成功！耗时 {duration} 秒")
        logger.info("="*50)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ ETL执行失败: {str(e)}")
        raise


def backfill(start_date, end_date):
    """
    补数函数：补历史数据
    start_date: 开始日期，格式YYYYMMDD
    end_date: 结束日期，格式YYYYMMDD
    """
    start_time = datetime.now()
    logger.info("="*50)
    logger.info(f"开始补数：{start_date} - {end_date}")
    logger.info("="*50)
    
    try:
        df = extract_from_ods(start_date, end_date)
        df = transform(df)
        load_to_mysql(df, start_date, end_date)
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        logger.info(f"✓ 补数完成！耗时 {duration} 秒")
        
    except Exception as e:
        logger.error(f"✗ 补数失败: {str(e)}")
        raise


if __name__ == '__main__':
    # 默认同步昨天数据
    # 如需补历史，使用: backfill(20260101, 20260113)
    
    if len(sys.argv) > 1:
        # 支持命令行指定回溯天数
        days = int(sys.argv[1])
        run(days_back=days)
    else:
        run(days_back=1)
