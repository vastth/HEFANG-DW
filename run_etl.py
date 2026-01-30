# -*- coding: utf-8 -*-
"""
何方珠宝 - ETL主调度脚本
一键执行所有ETL任务
"""

import logging
from datetime import datetime, timedelta
import sys
import os
from sqlalchemy import create_engine, text

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# 确保输出使用 UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# 确保可导入同目录下的 ETL 模块（在某些运行器中默认不包含当前目录）
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)


def run_all():
    """执行所有ETL任务"""
    
    start_time = datetime.now()
    logger.info("#"*60)
    logger.info("#  何方珠宝 - 数仓ETL开始执行")
    logger.info("#"*60)
    
    results = {}
    
    # 1. 商品维度
    logger.info("\n>>> [1/6] Syncing product dimensions...")
    try:
        from etl_dim_product import run as run_dim_product
        run_dim_product()
        results['dim_product'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_product'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_product failed: {error_msg}")
    
    # 2. SKU维度
    logger.info("\n>>> [2/6] Syncing sku dimensions...")
    try:
        from etl_dim_sku import run as run_dim_sku
        run_dim_sku()
        results['dim_sku'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_sku'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_sku failed: {error_msg}")

    # 3. 店仓维度
    logger.info("\n>>> [3/6] Syncing store dimensions...")
    try:
        from etl_dim_store import run as run_dim_store
        run_dim_store()
        results['dim_store'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_store'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_store failed: {error_msg}")
    
    # 4. 销售数据
    logger.info("\n>>> [4/6] Syncing sales data...")
    try:
        from etl_dws_sales import run as run_dws_sales, backfill as backfill_dws_sales
        run_dws_sales(days_back=1, include_today=True)  # 实时同步（含当天）

        # 覆盖性校验：若近30天数据不完整，则自动回补
        end_dt = datetime.now() - timedelta(days=1)
        start_dt = end_dt - timedelta(days=29)
        end_date = int(end_dt.strftime('%Y%m%d'))
        start_date = int(start_dt.strftime('%Y%m%d'))

        from config import MYSQL_CONN_STR
        engine = create_engine(MYSQL_CONN_STR)
        with engine.connect() as conn:
            row = conn.execute(text(
                """
                SELECT COUNT(DISTINCT date_id) AS day_cnt
                FROM dws_sales_daily
                WHERE date_id BETWEEN :start_date AND :end_date
                """
            ), {"start_date": start_date, "end_date": end_date}).fetchone()
        engine.dispose()

        day_cnt = row[0] if row else 0
        if day_cnt < 30:
            logger.warning(f"近30天销售数据仅覆盖{day_cnt}天，执行回补（{start_date} - {end_date}）...")
            backfill_dws_sales(start_date, end_date)
        results['dws_sales'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dws_sales'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dws_sales failed: {error_msg}")
    
    # 5. 库存数据
    logger.info("\n>>> [5/6] Syncing inventory data...")
    try:
        from etl_dws_inventory import run as run_dws_inventory
        run_dws_inventory()
        results['dws_inventory'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dws_inventory'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dws_inventory failed: {error_msg}")
    
    # 6. 库存健康度计算
    logger.info("\n>>> [6/6] Calculating inventory health...")
    try:
        from etl_ads_health import run as run_ads_health
        run_ads_health()
        results['ads_health'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['ads_health'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"ads_health failed: {error_msg}")
    
    # 汇总结果
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    logger.info("\n" + "#"*60)
    logger.info("#  ETL执行完成 - 结果汇总")
    logger.info("#"*60)
    
    all_success = True
    for task, result in results.items():
        logger.info(f"  {task}: {result}")
        if 'fail' in result.lower() or 'error' in result.lower():
            all_success = False
    
    logger.info(f"\nTotal time: {duration} seconds")
    
    if all_success:
        logger.info("All tasks executed successfully!")
    else:
        logger.warning("Some tasks failed, please check the logs")
    
    return all_success


if __name__ == '__main__':
    run_all()
