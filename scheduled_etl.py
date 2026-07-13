# -*- coding: utf-8 -*-
"""
ETL自动化调度配置
可通过Windows任务计划程序或cron调度
"""

import os
import sys
import argparse
from datetime import datetime
import logging

from cutover_controls import (
    CUTOVER_MODE_LEGACY,
    CUTOVER_MODE_SHADOW_COMPARE,
    CUTOVER_MODE_V2,
)

# 设置工作目录
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(PROJECT_DIR)

# 设置UTF-8编码
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# 配置日志文件
LOG_DIR = os.path.join(PROJECT_DIR, 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_file = os.path.join(LOG_DIR, f"etl_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def run_etl_with_error_handling(cutover_mode=None, rollback_to_legacy=False):
    """带错误处理的ETL执行"""
    try:
        logger.info("="*80)
        logger.info("ETL自动化调度开始")
        logger.info(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)

        conn_test_flag = ('--conn-test' in sys.argv) or (os.getenv('ETL_CONN_TEST', '0') == '1')
        logger.info('scheduled_etl cutover_mode=%s rollback_to_legacy=%s', cutover_mode, rollback_to_legacy)
        
        # 统一走 run_etl.py 入口（内含重试与企业微信摘要）
        from run_etl import run_main
        exit_code = run_main(
            conn_test_flag=conn_test_flag,
            cutover_mode=cutover_mode,
            rollback_to_legacy=rollback_to_legacy,
        )

        if exit_code == 0:
            logger.info("✅ ETL执行成功")

            # 执行测试验证
            if conn_test_flag:
                logger.info("\n连接测试模式：跳过数据验证")
                return 0

            logger.info("\n开始执行数据验证...")
            from test_etl_automation import main as test_main
            test_success = test_main()

            if test_success:
                logger.info("✅ 数据验证通过")
                return 0
            else:
                logger.warning("⚠️ 数据验证发现问题")
                return 1
        else:
            logger.error(f"❌ ETL执行失败，退出码: {exit_code}")
            return exit_code
            
    except Exception as e:
        logger.error(f"❌ ETL调度异常: {e}", exc_info=True)
        return 3

def build_parser():
    parser = argparse.ArgumentParser(description='ETL 自动化调度包装入口')
    parser.add_argument('--conn-test', action='store_true', help='只做连接测试，不执行写数 ETL')
    parser.add_argument(
        '--cutover-mode',
        choices=(CUTOVER_MODE_LEGACY, CUTOVER_MODE_SHADOW_COMPARE, CUTOVER_MODE_V2),
        default=None,
        help='透传给 run_etl.py 的主链 cutover 模式',
    )
    parser.add_argument('--rollback-to-legacy', action='store_true', help='显式回滚到 legacy 模式')
    return parser


if __name__ == '__main__':
    args = build_parser().parse_args()
    exit_code = run_etl_with_error_handling(
        cutover_mode=args.cutover_mode,
        rollback_to_legacy=args.rollback_to_legacy,
    )
    sys.exit(exit_code)
