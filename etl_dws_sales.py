# -*- coding: utf-8 -*-
"""
何方珠宝 - 销售数据ETL
从Oracle M_RETAIL/M_RETAILITEM同步到MySQL dws_sales_daily
策略：增量同步（按日期）
"""

import oracledb
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import sys

from config import ORACLE_CONFIG, ORACLE_DSN, MYSQL_CONN_STR

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_from_oracle(start_date, end_date):
    """从Oracle抽取销售数据"""
    
    sql = f"""
    SELECT
        r.BILLDATE AS date_id,
        r.C_STORE_ID AS store_id,
        s.CODE AS store_code,
        NVL(s.IS_ALLO2OSTORAGE, 'N') AS is_cloud_store,
        ri.M_PRODUCT_ID AS product_id,
        ri.M_PRODUCTALIAS_ID AS m_productalias_id,
        -- 销售数据（正单）
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_LIST ELSE 0 END) AS sales_amount_list,
        -- 退货数据（负单）
        SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS return_qty,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) AS return_amount,
        -- 订单数
        COUNT(DISTINCT CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN r.ID END) AS order_count
    FROM M_RETAILITEM ri
    LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
    LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    WHERE r.ISACTIVE = 'Y' 
        AND r.STATUS = 2
        AND r.BILLDATE >= {start_date}
        AND r.BILLDATE <= {end_date}
        AND ri.M_PRODUCTALIAS_ID IS NOT NULL
        AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
        AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
    GROUP BY r.BILLDATE, r.C_STORE_ID, s.CODE, NVL(s.IS_ALLO2OSTORAGE, 'N'), ri.M_PRODUCT_ID, ri.M_PRODUCTALIAS_ID
    """
    
    logger.info("连接Oracle数据库...")
    conn = oracledb.connect(
        user=ORACLE_CONFIG['user'],
        password=ORACLE_CONFIG['password'],
        dsn=ORACLE_DSN
    )
    
    logger.info(f"执行SQL查询（日期范围：{start_date} - {end_date}）...")
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
    df['date_id'] = df['date_id'].astype('int64')
    df['store_id'] = df['store_id'].astype('int64')
    df['product_id'] = df['product_id'].astype('int64')
    if 'm_productalias_id' in df.columns:
        df['m_productalias_id'] = df['m_productalias_id'].astype('Int64')
    else:
        df['m_productalias_id'] = pd.Series([pd.NA] * len(df), dtype='Int64')
    # store_code/is_cloud_store 来自 C_STORE
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
    engine = create_engine(MYSQL_CONN_STR)
    
    try:
        with engine.begin() as conn:
            # 先删除该日期范围的旧数据
            logger.info(f"删除旧数据（{start_date} - {end_date}）...")
            conn.execute(text(f"DELETE FROM dws_sales_daily WHERE date_id >= {start_date} AND date_id <= {end_date}"))
            
            logger.info("写入新数据...")
            df.to_sql(
                name='dws_sales_daily',
                con=conn,
                if_exists='append',
                index=False,
                chunksize=5000
            )
        
        logger.info(f"写入完成，共 {len(df)} 条记录")
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
        df = extract_from_oracle(start_date, end_date)
        
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
        df = extract_from_oracle(start_date, end_date)
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
