# -*- coding: utf-8 -*-
"""
何方珠宝 - ETL主调度脚本
一键执行所有ETL任务
"""

import logging
from datetime import datetime
import sys
import os

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
    logger.info("\n>>> [1/5] Syncing product dimensions...")
    try:
        from etl_dim_product import run as run_dim_product
        run_dim_product()
        results['dim_product'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_product'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_product failed: {error_msg}")
    
    # 2. 店仓维度
    logger.info("\n>>> [2/5] Syncing store dimensions...")
    try:
        from etl_dim_store import run as run_dim_store
        run_dim_store()
        results['dim_store'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_store'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_store failed: {error_msg}")
    
    # 3. 销售数据
    logger.info("\n>>> [3/5] Syncing sales data...")
    try:
        from etl_dws_sales import run as run_dws_sales
        run_dws_sales(days_back=1)  # 默认同步昨天
        results['dws_sales'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dws_sales'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dws_sales failed: {error_msg}")
    
    # 4. 库存数据
    logger.info("\n>>> [4/5] Syncing inventory data...")
    try:
        from etl_dws_inventory import run as run_dws_inventory
        run_dws_inventory()
        results['dws_inventory'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dws_inventory'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dws_inventory failed: {error_msg}")
    
    # 5. 库存健康度计算
    logger.info("\n>>> [5/5] Calculating inventory health...")
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
