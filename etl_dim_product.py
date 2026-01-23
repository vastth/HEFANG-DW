# -*- coding: utf-8 -*-
"""
何方珠宝 - 商品维度ETL
从Oracle M_PRODUCT同步到MySQL dim_product
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
    """从Oracle抽取商品数据"""

    # 使用实际存在的字段名（不使用行尾反斜杠，保持 SQL 可读）
    sql = """
    SELECT
        p.ID AS product_id,
        p.NAME AS product_code,
        p.VALUE AS product_name,
        p.M_DIM4_ID AS category_id,
        d4.ATTRIBNAME AS category_name,
        p.M_DIM5_ID AS property_id,
        d5.ATTRIBNAME AS property_name,
        p.M_DIM6_ID AS series_id,
        d6.ATTRIBNAME AS series_name,
        p.M_DIM1_ID AS brand_id,
        d1.ATTRIBNAME AS brand_name,
        p.PRICELIST AS price_list,
        p.FABELEMENT AS material,
        p.PRECOST AS price_cost,
        CASE WHEN p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145) THEN 'Y' ELSE 'N' END AS is_main_product,
        p.ISACTIVE AS is_active,
        p.CREATIONDATE AS created_at,
        asi.VALUE1 AS color_attr,
        asi.VALUE2 AS size_attr
    FROM M_PRODUCT p
    LEFT JOIN M_DIM d1 ON p.M_DIM1_ID = d1.ID
    LEFT JOIN M_DIM d4 ON p.M_DIM4_ID = d4.ID
    LEFT JOIN M_DIM d5 ON p.M_DIM5_ID = d5.ID
    LEFT JOIN M_DIM d6 ON p.M_DIM6_ID = d6.ID
    LEFT JOIN (
        SELECT inner_pa.M_PRODUCT_ID, inner_pa.M_ATTRIBUTESETINSTANCE_ID FROM (
            SELECT pa.M_PRODUCT_ID, pa.M_ATTRIBUTESETINSTANCE_ID,
                   ROW_NUMBER() OVER (PARTITION BY pa.M_PRODUCT_ID ORDER BY pa.ID) AS rn
            FROM M_PRODUCT_ALIAS pa
            WHERE pa.ISACTIVE = 'Y'
        ) inner_pa WHERE inner_pa.rn = 1
    ) pa_alias ON p.ID = pa_alias.M_PRODUCT_ID
    LEFT JOIN M_ATTRIBUTESETINSTANCE asi ON pa_alias.M_ATTRIBUTESETINSTANCE_ID = asi.ID
    WHERE p.ISACTIVE = 'Y'
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
    df['category_name'] = df['category_name'].fillna('未分类')
    df['property_name'] = df['property_name'].fillna('未知')
    df['series_name'] = df['series_name'].fillna('')
    df['brand_name'] = df['brand_name'].fillna('')
    df['material'] = df.get('material', '').fillna('')
    df['price_list'] = df['price_list'].fillna(0)
    df['price_cost'] = df['price_cost'].fillna(0)

    # 转换数据类型
    df['product_id'] = df['product_id'].astype('int64')
    df['category_id'] = df['category_id'].fillna(0).astype('int64')
    df['property_id'] = df['property_id'].fillna(0).astype('int64')
    df['series_id'] = df['series_id'].fillna(0).astype('int64')
    df['brand_id'] = df['brand_id'].fillna(0).astype('int64')
    # material 保持字符串
    if 'material' in df.columns:
        df['material'] = df['material'].fillna('')

    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df):
    """加载到MySQL"""

    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)

    # 全量覆盖：先清空再写入
    logger.info("清空目标表 dim_product...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_product"))

    logger.info("写入数据...")
    # 写入 dim_product 前，移除属性列（颜色/尺寸），属性单独写入 dim_product_attr
    df_product = df.copy()
    for col in ['color_attr', 'size_attr']:
        if col in df_product.columns:
            df_product.drop(columns=[col], inplace=True)

    df_product.to_sql(
        name='dim_product',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=5000
    )

    logger.info(f"写入完成，共 {len(df)} 条记录")
    engine.dispose()


def load_attr_to_mysql(df):
    """将颜色/尺寸写入 dim_product_attr 表，便于下游使用"""
    logger.info("开始写入 dim_product_attr...")
    engine = create_engine(MYSQL_CONN_STR)

    # color_attr/size_attr 来自 M_ATTRIBUTESETINSTANCE
    df_attr = df[['product_id', 'color_attr', 'size_attr']].drop_duplicates()
    # 填充空值
    df_attr['color_attr'] = df_attr['color_attr'].fillna('')
    df_attr['size_attr'] = df_attr['size_attr'].fillna('')

    # 重命名列为通用字段名（color / size）写入目标表
    df_attr = df_attr.rename(columns={'color_attr': 'color', 'size_attr': 'size'})
    df_attr.to_sql(
        name='dim_product_attr',
        con=engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )

    logger.info(f"写入完成，共 {len(df_attr)} 条记录")
    engine.dispose()


def run():
    """执行ETL"""

    start_time = datetime.now()
    logger.info("=" * 50)
    logger.info("开始执行 dim_product ETL")
    logger.info("=" * 50)

    try:
        # Extract
        df = extract_from_oracle()

        # Transform
        df = transform(df)

        # Load
        load_to_mysql(df)
        # 写入颜色尺寸属性表
        load_attr_to_mysql(df)

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