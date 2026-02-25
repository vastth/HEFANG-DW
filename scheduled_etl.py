# -*- coding: utf-8 -*-
"""
ETL自动化调度配置
可通过Windows任务计划程序或cron调度
"""

import os
import sys
from datetime import datetime
import logging
import subprocess

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

def run_etl_with_error_handling():
    """带错误处理的ETL执行"""
    try:
        logger.info("="*80)
        logger.info("ETL自动化调度开始")
        logger.info(f"执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        # 统一走 run_etl.py 入口（内含重试与企业微信摘要）
        run_etl_file = os.path.join(PROJECT_DIR, 'run_etl.py')
        # 将 run_etl 子进程的 stdout/stderr 重定向到同一日志文件，保证调度日志包含子进程输出
        with open(log_file, 'ab') as fh:
            # 写入一条分隔，方便查看
            fh.write(("\n--- Subprocess run_etl output start ---\n").encode('utf-8'))
            fh.flush()
            result = subprocess.run([sys.executable, run_etl_file], cwd=PROJECT_DIR, stdout=fh, stderr=fh)
            fh.write(("\n--- Subprocess run_etl output end ---\n").encode('utf-8'))
            fh.flush()

        if result.returncode == 0:
            logger.info("✅ ETL执行成功")

            # 执行测试验证
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
            logger.error(f"❌ ETL执行失败，退出码: {result.returncode}")
            return result.returncode
            
    except Exception as e:
        logger.error(f"❌ ETL调度异常: {e}", exc_info=True)
        return 3

if __name__ == '__main__':
    exit_code = run_etl_with_error_handling()
    sys.exit(exit_code)
