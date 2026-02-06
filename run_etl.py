# -*- coding: utf-8 -*-
"""
何方珠宝 - ETL主调度脚本
一键执行所有ETL任务
"""

import logging
from datetime import datetime, timedelta
import sys
import os
import time
import traceback
from alerts import send_wechat_alert
from config import (
    WECHAT_WEBHOOK,
    TASK_DISPLAY_NAME,
    ETL_NON_RETRYABLE_ERROR_KEYWORDS,
    ETL_RETRYABLE_ERROR_KEYWORDS,
    ETL_DEFAULT_MAX_RETRIES,
    ETL_DEFAULT_RETRY_SLEEP,
)
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
    logger.info("\n>>> [1/7] Syncing product dimensions...")
    try:
        from etl_dim_product import run as run_dim_product
        run_dim_product()
        results['dim_product'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_product'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_product failed: {error_msg}")
    
    # 2. SKU维度
    logger.info("\n>>> [2/7] Syncing sku dimensions...")
    try:
        from etl_dim_sku import run as run_dim_sku
        run_dim_sku()
        results['dim_sku'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_sku'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_sku failed: {error_msg}")

    # 3. 店仓维度
    logger.info("\n>>> [3/7] Syncing store dimensions...")
    try:
        from etl_dim_store import run as run_dim_store
        run_dim_store()
        results['dim_store'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dim_store'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dim_store failed: {error_msg}")
    
    # 4. 销售数据
    logger.info("\n>>> [4/7] Syncing sales data...")
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
    logger.info("\n>>> [5/7] Syncing inventory data...")
    try:
        from etl_dws_inventory import run as run_dws_inventory
        run_dws_inventory()
        results['dws_inventory'] = 'SUCCESS'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dws_inventory'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dws_inventory failed: {error_msg}")

    # 6. 达播数据就绪检查（外部项目产出）
    logger.info("\n>>> [6/7] Checking dabo data readiness...")
    dabo_ready = False
    try:
        from config import MYSQL_CONN_STR
        engine = create_engine(MYSQL_CONN_STR)
        today = datetime.now().strftime('%Y-%m-%d')
        with engine.connect() as conn:
            row = conn.execute(text(
                """
                SELECT COUNT(*) AS cnt, MAX(sale_date) AS latest_date
                FROM ads_dabo_daily_sales
                WHERE sale_date = :today
                """
            ), {"today": today}).fetchone()
        engine.dispose()

        dabo_cnt = row[0] if row else 0
        latest_date = row[1] if row else None
        if dabo_cnt > 0:
            dabo_ready = True
            logger.info(f"达播数据就绪：今日记录 {dabo_cnt} 条（latest_date={latest_date}）")
        else:
            logger.warning(f"达播数据未就绪：今日无记录（latest_date={latest_date}），将继续执行ADS计算")
        results['dabo_ready'] = 'SUCCESS' if dabo_ready else 'WARNING: NO_DATA_TODAY'
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        results['dabo_ready'] = f'FAILED: {error_msg[:100]}'
        logger.error(f"dabo_ready check failed: {error_msg}")
    
    # 7. 库存健康度计算
    logger.info("\n>>> [7/7] Calculating inventory health...")
    try:
        from etl_ads_health import run as run_ads_health, backfill_dabo_fields
        run_ads_health()

        # 达播数据就绪时，回填当日达播/自然字段（避免外部项目时序影响）
        if dabo_ready:
            backfill_dabo_fields()
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
    
    return all_success, results


if __name__ == '__main__':
    # 从配置中读取企业微信 webhook（可在 config.py 中配置变量 `WECHAT_WEBHOOK`）
    try:
        from config import WECHAT_WEBHOOK
    except Exception:
        WECHAT_WEBHOOK = None

    # send_wechat_alert 已移至 alerts.py

    # 仅做连接测试（不写入）
    def run_conn_test():
        """仅测试 Oracle 与 MySQL 连接，不做任何写操作，返回 (ok, details)

        details: dict, e.g. {'oracle': 'SUCCESS' or 'FAILED: ...', 'mysql': 'SUCCESS' or 'FAILED: ...'}
        """
        ok = True
        details = {}
        # Oracle
        try:
            from config import ORACLE_CONFIG, ORACLE_DSN
            logger.info('ConnTest: testing Oracle connection...')
            try:
                import oracledb
                conn = oracledb.connect(user=ORACLE_CONFIG['user'], password=ORACLE_CONFIG['password'], dsn=ORACLE_DSN)
                conn.close()
                logger.info('ConnTest: Oracle OK')
                details['oracle'] = 'SUCCESS'
            except Exception as e:
                err = str(e).encode('utf-8', errors='ignore').decode('utf-8')
                logger.error(f'ConnTest: Oracle connection failed: {err}')
                details['oracle'] = f'FAILED: {_extract_error_summary(err)}'
                ok = False
        except Exception as e:
            err = str(e).encode('utf-8', errors='ignore').decode('utf-8')
            logger.error(f'ConnTest: Oracle test skipped (config error): {err}')
            details['oracle'] = f'FAILED: {err}'
            ok = False

        # MySQL
        try:
            from config import MYSQL_CONN_STR
            logger.info('ConnTest: testing MySQL connection...')
            try:
                engine = create_engine(MYSQL_CONN_STR)
                with engine.connect() as conn:
                    conn.execute(text('SELECT 1'))
                engine.dispose()
                logger.info('ConnTest: MySQL OK')
                details['mysql'] = 'SUCCESS'
            except Exception as e:
                err = str(e).encode('utf-8', errors='ignore').decode('utf-8')
                logger.error(f'ConnTest: MySQL connection failed: {err}')
                details['mysql'] = f'FAILED: {_extract_error_summary(err)}'
                ok = False
        except Exception as e:
            err = str(e).encode('utf-8', errors='ignore').decode('utf-8')
            logger.error(f'ConnTest: MySQL test skipped (config error): {err}')
            details['mysql'] = f'FAILED: {err}'
            ok = False

        return ok, details

    # 包装入口：支持重试和异常告警；可传入运行函数以复用重试逻辑
    def _extract_error_summary(text):
        """从异常文本或 traceback 中提取最有信息量的一行摘要。

        策略：
        1) 倒序查找包含关键字的行（如 ORA-, Access denied, OperationalError, timeout, exception 等）
        2) 若无匹配，倒序查找首个非空且不是帮助链接/traceback 文件行的行
        3) 返回该行的简短摘要
        """
        if not text:
            return ''
        import re

        keywords = ['ORA-', 'ORA_', 'Access denied', 'invalid username', 'OperationalError', 'timeout', 'timed out', 'Connection refused', 'authentication failed', 'Traceback']
        # 拆分并去除空行
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        # 优先过滤掉明显无用的帮助/URL/文件定位行
        def is_noise(ln):
            low = ln.lower()
            if low.startswith('help:'):
                return True
            if 'background on this error' in low:
                return True
            if ln.startswith('File "'):
                return True
            if ln.startswith('http://') or ln.startswith('https://'):
                return True
            return False

        useful_lines = [ln for ln in lines if not is_noise(ln)]

        # 0) 特殊优先：寻找 ORA-12345 类的错误码行
        ora_match = re.search(r'(ORA-\d{5,})', text, flags=re.IGNORECASE)
        if ora_match:
            # 返回包含 ORA- 的整行（从 useful_lines 或原始 lines 中寻找）
            for ln in useful_lines:
                if 'ora-' in ln.lower():
                    return ln
            for ln in lines:
                if 'ora-' in ln.lower():
                    return ln

        # 1) 在有用行中倒序匹配关键字
        for ln in reversed(useful_lines):
            for kw in keywords:
                if kw.lower() in ln.lower():
                    return ln

        # 2) 若没有有用行（或者未命中关键字），尝试在原始行中匹配关键字
        for ln in reversed(lines):
            for kw in keywords:
                if kw.lower() in ln.lower():
                    return ln

        # 3) 若仍无匹配，返回第一个非噪声的有意义行
        if useful_lines:
            return useful_lines[-1]

        # 4) 回退到原始最后一行或全文简短化
        return lines[-1] if lines else text.strip()


    def _compose_failure_summary_from_results(results_dict):
        # 使用从 config.py 导入的 TASK_DISPLAY_NAME
        parts = []
        for task, msg in results_dict.items():
            if not isinstance(msg, str):
                continue
            up = msg.upper()
            if not (up.startswith('FAILED') or 'ERROR' in up or 'WARNING' in up):
                continue

            # 去掉 'FAILED:' 前缀（如果存在），并提取有信息量的摘要
            content = msg
            if msg.startswith('FAILED:'):
                content = msg[len('FAILED:'):].strip()
            reason = _extract_error_summary(content)

            display = TASK_DISPLAY_NAME.get(task, task)
            parts.append(f"{display}：{reason}")

        return '\n'.join(parts) if parts else '部分任务失败，请查看日志获取更多信息。'


    def _should_retry_based_on_details(details):
        """根据结果详情决定是否继续重试。

        逻辑：
        - 若任一失败消息包含 ETL_NON_RETRYABLE_ERROR_KEYWORDS 中的关键字，认为不可重试。
        - 否则，如果 ETL_RETRYABLE_ERROR_KEYWORDS 非空，则只有当至少匹配一项时才重试。
        - 否则（没有非重试关键字，且未指定白名单），默认允许重试。
        """
        try:
            if not details:
                return True
            # details 是 dict task->msg
            for v in details.values():
                if not isinstance(v, str):
                    continue
                low = v.lower()
                for nk in ETL_NON_RETRYABLE_ERROR_KEYWORDS:
                    if nk.lower() in low:
                        return False

            # 如果定义了可重试关键字列表，则必须匹配其中至少一项才重试
            if ETL_RETRYABLE_ERROR_KEYWORDS:
                for v in details.values():
                    if not isinstance(v, str):
                        continue
                    low = v.lower()
                    for rk in ETL_RETRYABLE_ERROR_KEYWORDS:
                        if rk.lower() in low:
                            return True
                return False

            return True
        except Exception:
            return True


    def main_with_retries(run_func, max_retries=3, sleep_seconds=60):
        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                logger.info(f'ETL 主任务开始（尝试 {attempt}/{max_retries}）')
                run_result = run_func()

                # run_func 可能返回 bool 或 (bool, details)
                if isinstance(run_result, tuple) and len(run_result) == 2:
                    ok, details = run_result
                else:
                    ok = bool(run_result)
                    details = None

                if ok:
                    return 0
                else:
                    logger.warning(f'ETL finished with partial failures on attempt {attempt}.')
                    # 若有详细 results，判断是否应该继续重试
                    should_retry = True
                    if details:
                        should_retry = _should_retry_based_on_details(details)

                    if not should_retry:
                        # 发现确定性不可重试错误，立即告警并返回
                        summary = _compose_failure_summary_from_results(details) if details else '部分任务失败，详情见日志。'
                        content = f"❌ ETL 遇到不可重试的错误：\n{summary}"
                        send_wechat_alert(WECHAT_WEBHOOK, content)
                        return 2

                    # 到达最大重试次数则发送告警
                    if attempt >= max_retries:
                        if details:
                            summary = _compose_failure_summary_from_results(details)
                            content = f"❌ ETL 部分任务失败：\n{summary}"
                        else:
                            content = '❌ ETL 部分任务失败：请检查日志以获取详情。'
                        send_wechat_alert(WECHAT_WEBHOOK, content)
                        return 2

                    logger.info(f'等待 {sleep_seconds} 秒后重试...')
                    time.sleep(sleep_seconds)
            except Exception:
                tb = traceback.format_exc()
                logger.error(f'ETL 主任务抛出未捕获异常（尝试 {attempt}/{max_retries}）: {tb}')
                summary_line = _extract_error_summary(tb)
                if attempt >= max_retries:
                    content = f"❌ ETL 遇到严重错误：{summary_line}"
                    send_wechat_alert(WECHAT_WEBHOOK, content)
                    return 1
                else:
                    logger.info(f'等待 {sleep_seconds} 秒后重试...')
                    time.sleep(sleep_seconds)
        return 1

    # 解析是否启用连接测试模式（命令行或环境变量）
    conn_test_flag = ('--conn-test' in sys.argv) or (os.getenv('ETL_CONN_TEST', '0') == '1')
    # 支持通过环境变量临时覆盖最大重试次数（便于测试）
    try:
        MAX_RETRIES = int(os.getenv('ETL_MAX_RETRIES', '3'))
    except Exception:
        MAX_RETRIES = 3
    if conn_test_flag:
        logger.info('运行模式：仅连接测试（--conn-test / ETL_CONN_TEST=1）')
        runner = run_conn_test
    else:
        runner = run_all

    sys.exit(main_with_retries(runner, max_retries=MAX_RETRIES))
