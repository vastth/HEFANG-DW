# -*- coding: utf-8 -*-
"""
何方珠宝 - 店仓维度ETL
从Oracle C_STORE同步到MySQL dim_store
策略：全量覆盖，保留 is_active 状态
"""

import pandas as pd
from sqlalchemy import text
from datetime import datetime
import logging

from db_connections import connect_oracle, create_mysql_engine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


DIM_STORE_LOAD_COLUMNS = (
    'store_id',
    'store_code',
    'store_name',
    'area_id',
    'area_name',
    'is_warehouse',
    'is_store',
    'is_cloud_store',
    'is_center',
    'store_type',
    'is_active',
    'open_date',
    'created_at',
)

EXTRACT_SQL = """
      SELECT s.ID                         AS store_id,
             s.CODE                       AS store_code,
             s.NAME                       AS store_name,
             s.C_AREA_ID                  AS area_id,
             a.NAME                       AS area_name,
             s.DM_ISWAREHOUSE             AS is_warehouse,
             s.DM_ISSTORE                 AS is_store,
             NVL(s.IS_ALLO2OSTORAGE, 'N') AS is_cloud_store,
             NVL(s.ISCENTER, 'N')         AS is_center,
             CASE
                 WHEN s.CODE = '001' THEN '总仓'
                 WHEN s.CODE LIKE 'DS%' THEN '电商'
                 WHEN s.CODE LIKE 'RT%' THEN '门店'
                 WHEN s.CODE LIKE 'CS%' THEN '测试'
                 ELSE '功能仓'
                 END                      AS store_type,
             s.ISACTIVE                   AS is_active,
             s.OPENDATE                   AS source_opendate_raw,
             CASE
                 WHEN s.OPENDATE IS NULL THEN 1
                 ELSE 0
                 END                      AS source_opendate_is_null,
             CASE
                 WHEN s.OPENDATE IS NOT NULL
                      AND TO_DATE(
                          TRIM(TO_CHAR(s.OPENDATE)) DEFAULT NULL ON CONVERSION ERROR,
                          'YYYYMMDD'
                      ) IS NULL THEN 1
                 ELSE 0
                 END                      AS source_opendate_is_invalid,
             TO_DATE(
                 TRIM(TO_CHAR(s.OPENDATE)) DEFAULT NULL ON CONVERSION ERROR,
                 'YYYYMMDD'
             )                            AS open_date
      FROM C_STORE s
               LEFT JOIN C_AREA a ON s.C_AREA_ID = a.ID
      """


def _build_open_date_quality_summary(df, sample_limit=10):
    source_null_mask = df['source_opendate_is_null'].fillna(0).astype(int).eq(1)
    source_invalid_mask = df['source_opendate_is_invalid'].fillna(0).astype(int).eq(1)
    invalid_samples = df.loc[
        source_invalid_mask,
        ['store_id', 'store_code', 'store_name', 'source_opendate_raw'],
    ].head(sample_limit)

    return {
        'source_store_count': int(len(df)),
        'source_opendate_null_count': int(source_null_mask.sum()),
        'source_opendate_invalid_count': int(source_invalid_mask.sum()),
        'source_opendate_invalid_samples': invalid_samples.to_dict('records'),
    }


def _fetch_target_columns(engine):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = DATABASE()
                  AND table_name = 'dim_store'
                """
            )
        ).fetchall()
    return {row[0] for row in rows}


def _validate_target_columns(target_columns):
    missing_columns = [column for column in DIM_STORE_LOAD_COLUMNS if column not in target_columns]
    if missing_columns:
        raise RuntimeError(
            'dim_store 缺少 ETL 必需字段: '
            f"{', '.join(missing_columns)}；请先由用户人工执行对应 DDL，再运行维表全量刷新"
        )


def extract_from_oracle():
    """从Oracle抽取店仓数据"""

    logger.info("连接Oracle数据库...")
    conn = connect_oracle()

    logger.info("执行SQL查询...")
    cursor = conn.cursor()
    cursor.execute(EXTRACT_SQL)
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
    df = df.copy()

    open_date_quality = _build_open_date_quality_summary(df)
    logger.info(
        'C_STORE.OPENDATE 源端质量: source_store_count=%s, source_opendate_null_count=%s, '
        'source_opendate_invalid_count=%s',
        open_date_quality['source_store_count'],
        open_date_quality['source_opendate_null_count'],
        open_date_quality['source_opendate_invalid_count'],
    )
    if open_date_quality['source_opendate_invalid_count'] > 0:
        logger.warning(
            'C_STORE.OPENDATE 存在不可转换日期，已安全转为 NULL: samples=%s',
            open_date_quality['source_opendate_invalid_samples'],
        )

    # 处理空值
    df['area_name'] = df['area_name'].fillna('未知区域')
    df['is_warehouse'] = df['is_warehouse'].fillna(0)
    df['is_store'] = df['is_store'].fillna(0)

    # 转换数据类型
    df['store_id'] = df['store_id'].astype('int64')
    df['area_id'] = df['area_id'].fillna(0).astype('int64')
    df['is_warehouse'] = df['is_warehouse'].astype('int')
    df['is_store'] = df['is_store'].astype('int')
    df['open_date'] = pd.to_datetime(df['open_date'], errors='coerce').dt.date

    # 添加ETL时间戳
    df['created_at'] = datetime.now()

    # 源端质量审计字段仅用于日志，不落入 dim_store。
    df = df.loc[:, DIM_STORE_LOAD_COLUMNS]

    logger.info(f"转换完成，共 {len(df)} 条记录")
    return df


def load_to_mysql(df):
    """加载到MySQL"""

    logger.info("连接MySQL数据库...")
    engine = create_mysql_engine(timeout_profile='etl')

    # 必须在 TRUNCATE 前检查目标结构，避免 DDL 未执行时先清空维表。
    _validate_target_columns(_fetch_target_columns(engine))
    load_df = df.loc[:, DIM_STORE_LOAD_COLUMNS]

    # 全量覆盖
    logger.info("清空目标表 dim_store...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_store"))

    logger.info("写入数据...")
    load_df.to_sql(
        name='dim_store',
        con=engine,
        if_exists='append',
        index=False,
        chunksize=1000
    )

    logger.info(f"写入完成，共 {len(load_df)} 条记录")
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