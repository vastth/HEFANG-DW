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
def _reconfigure_stream_encoding(stream):
    reconfigure = getattr(stream, 'reconfigure', None)
    if callable(reconfigure):
        reconfigure(encoding='utf-8')


_reconfigure_stream_encoding(sys.stdout)
_reconfigure_stream_encoding(sys.stderr)

# 确保可导入同目录下的 ETL 模块（在某些运行器中默认不包含当前目录）
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

STEP_ORDER = [
    'dim_product',
    'dim_sku',
    'dim_store',
    'dim_channel',
    'ods_sync',
    'dws_sales',
    'dws_inventory',
    'dabo_ready',
    'ads_health',
]


def _truncate_text(text, max_len=160):
    if not isinstance(text, str):
        return ''
    return text if len(text) <= max_len else text[:max_len] + '...'


def _status_from_legacy_message(message):
    if not isinstance(message, str):
        return 'UNKNOWN', ''
    msg_upper = message.upper()
    if msg_upper.startswith('FAILED') or 'ERROR' in msg_upper:
        detail = message.split(':', 1)[1].strip() if ':' in message else message
        return 'FAILED', _truncate_text(detail)
    if msg_upper.startswith('WARNING'):
        detail = message.split(':', 1)[1].strip() if ':' in message else message
        return 'WARNING', _truncate_text(detail)
    if msg_upper.startswith('SUCCESS'):
        detail = message.split(':', 1)[1].strip() if ':' in message else ''
        return 'SUCCESS', _truncate_text(detail)
    return 'UNKNOWN', _truncate_text(message)


def _normalize_step_reports(step_reports):
    normalized = {}
    if not step_reports:
        return normalized

    for task, payload in step_reports.items():
        if isinstance(payload, dict):
            status = str(payload.get('status', 'UNKNOWN')).upper()
            detail = _truncate_text(str(payload.get('detail', '')).strip())
            duration = payload.get('duration_seconds')
            normalized[task] = {
                'status': status,
                'detail': detail,
                'duration_seconds': duration,
            }
            continue

        status, detail = _status_from_legacy_message(payload)
        normalized[task] = {
            'status': status,
            'detail': detail,
            'duration_seconds': None,
        }

    return normalized


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

    keywords = [
        'ORA-', 'ORA_', 'Access denied', 'invalid username', 'OperationalError',
        'timeout', 'timed out', 'Connection refused', 'authentication failed', 'Traceback'
    ]
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


def _compose_run_summary_message(step_reports, overall_status, started_at, ended_at, attempt=None, max_retries=None, extra_message=None):
    normalized = _normalize_step_reports(step_reports)
    all_steps = [k for k in STEP_ORDER if k in normalized]
    if not all_steps:
        all_steps = list(normalized.keys())

    success_cnt = 0
    warning_cnt = 0
    failed_cnt = 0
    for task in all_steps:
        st = normalized[task]['status']
        if st == 'SUCCESS':
            success_cnt += 1
        elif st == 'WARNING':
            warning_cnt += 1
        elif st in ('FAILED', 'ERROR'):
            failed_cnt += 1

    duration_seconds = int((ended_at - started_at).total_seconds())
    title_map = {
        'SUCCESS': '✅ ETL执行完成',
        'FAILED': '❌ ETL执行完成（存在失败）',
        'ERROR': '❌ ETL执行异常',
    }
    status_icon = {
        'SUCCESS': '✅',
        'WARNING': '⚠️',
        'FAILED': '❌',
        'ERROR': '❌',
        'UNKNOWN': '❔',
    }

    title = title_map.get(overall_status, '📊 ETL执行摘要')
    lines = [title]
    lines.append(f"时间: {started_at.strftime('%Y-%m-%d %H:%M:%S')} ~ {ended_at.strftime('%H:%M:%S')}")
    lines.append(f"耗时: {duration_seconds} 秒")
    lines.append(f"结果: 成功{success_cnt} / 警告{warning_cnt} / 失败{failed_cnt}")
    if attempt is not None and max_retries is not None:
        lines.append(f"重试: 第 {attempt}/{max_retries} 次")

    lines.append('步骤明细:')
    for idx, task in enumerate(all_steps, start=1):
        report = normalized[task]
        display = TASK_DISPLAY_NAME.get(task, task)
        icon = status_icon.get(report['status'], '❔')
        detail = f"（{report['detail']}）" if report['detail'] else ''
        duration = report.get('duration_seconds')
        if duration is None:
            cost = ''
        else:
            cost = f" [{duration}s]"
        lines.append(f"{idx}. {icon} {display}: {report['status']}{cost}{detail}")

    issues = []
    for task in all_steps:
        report = normalized[task]
        if report['status'] in ('FAILED', 'ERROR', 'WARNING'):
            display = TASK_DISPLAY_NAME.get(task, task)
            issues.append(f"- {display}: {report['detail'] or report['status']}")
    if issues:
        lines.append('异常/提示:')
        lines.extend(issues)

    if extra_message:
        lines.append(extra_message)

    return '\n'.join(lines)


def _compose_failure_summary_from_results(results_dict):
    normalized = _normalize_step_reports(results_dict)
    parts = []
    for task, report in normalized.items():
        status = report.get('status', 'UNKNOWN')
        if status not in ('FAILED', 'ERROR', 'WARNING'):
            continue
        reason = _extract_error_summary(report.get('detail', ''))

        display = TASK_DISPLAY_NAME.get(task, task)
        parts.append(f"{display}：{reason}")

    return '\n'.join(parts) if parts else '部分任务失败，请查看日志获取更多信息。'


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


def run_all():
    """执行所有ETL任务"""
    
    start_time = datetime.now()
    logger.info("#"*60)
    logger.info("#  何方珠宝 - 数仓ETL开始执行")
    logger.info("#"*60)
    
    step_reports = {}

    def update_step(task_name, status, detail='', step_start=None):
        duration_seconds = None
        if step_start is not None:
            duration_seconds = int((datetime.now() - step_start).total_seconds())
        step_reports[task_name] = {
            'status': status,
            'detail': _truncate_text(detail),
            'duration_seconds': duration_seconds,
        }
    
    # 1. 商品维度
    logger.info("\n>>> [1/9] Syncing product dimensions...")
    step_start = datetime.now()
    try:
        from etl_dim_product import run as run_dim_product
        run_dim_product()
        update_step('dim_product', 'SUCCESS', '同步完成', step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dim_product', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dim_product failed: {error_msg}")
    
    # 2. SKU维度
    logger.info("\n>>> [2/9] Syncing sku dimensions...")
    step_start = datetime.now()
    try:
        from etl_dim_sku import run as run_dim_sku
        run_dim_sku()
        update_step('dim_sku', 'SUCCESS', '同步完成', step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dim_sku', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dim_sku failed: {error_msg}")

    # 3. 店仓维度
    logger.info("\n>>> [3/9] Syncing store dimensions...")
    step_start = datetime.now()
    try:
        from etl_dim_store import run as run_dim_store
        run_dim_store()
        update_step('dim_store', 'SUCCESS', '同步完成', step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dim_store', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dim_store failed: {error_msg}")
    
    # 4. 渠道维度
    logger.info("\n>>> [4/9] Syncing channel dimensions...")
    step_start = datetime.now()
    try:
        from etl_dim_channel import run as run_dim_channel
        run_dim_channel()
        update_step('dim_channel', 'SUCCESS', '同步完成', step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dim_channel', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dim_channel failed: {error_msg}")

    # 5. ODS 同步
    logger.info("\n>>> [5/9] Syncing ODS data...")
    step_start = datetime.now()
    try:
        from run_ods import run as run_ods_sync
        run_ods_sync(mode='incremental', backfill_days=7, window_days=None, run_qc=True, qc_days=7)
        update_step('ods_sync', 'SUCCESS', '增量同步与质量校验完成', step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('ods_sync', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"ods_sync failed: {error_msg}")

    # 6. 销售数据
    logger.info("\n>>> [6/9] Syncing sales data...")
    step_start = datetime.now()
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
        backfilled = False
        if day_cnt < 30:
            logger.warning(f"近30天销售数据仅覆盖{day_cnt}天，执行回补（{start_date} - {end_date}）...")
            backfill_dws_sales(start_date, end_date)
            backfilled = True
        detail = f"近30天覆盖{day_cnt}/30天"
        if backfilled:
            detail += f"，已回补[{start_date}-{end_date}]"
        update_step('dws_sales', 'SUCCESS', detail, step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dws_sales', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dws_sales failed: {error_msg}")
    
    # 7. 库存数据
    logger.info("\n>>> [7/9] Syncing inventory data...")
    step_start = datetime.now()
    try:
        from etl_dws_inventory import run as run_dws_inventory
        run_dws_inventory()
        update_step('dws_inventory', 'SUCCESS', '同步完成', step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dws_inventory', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dws_inventory failed: {error_msg}")

    # 8. 达播数据就绪检查（外部项目产出）
    logger.info("\n>>> [8/9] Checking dabo data readiness...")
    dabo_ready = False
    step_start = datetime.now()
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
            update_step('dabo_ready', 'SUCCESS', f"今日记录{dabo_cnt}条，latest_date={latest_date}", step_start)
        else:
            logger.warning(f"达播数据未就绪：今日无记录（latest_date={latest_date}），将继续执行ADS计算")
            update_step('dabo_ready', 'WARNING', f"今日无记录，latest_date={latest_date}", step_start)
    except Exception as e:
        error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
        update_step('dabo_ready', 'FAILED', _extract_error_summary(error_msg), step_start)
        logger.error(f"dabo_ready check failed: {error_msg}")
    
    # 9. 库存健康度计算
    logger.info("\n>>> [9/9] Calculating inventory health...")
    step_start = datetime.now()
    upstream_blockers = []
    if step_reports.get('dws_sales', {}).get('status') != 'SUCCESS':
        upstream_blockers.append('dws_sales')
    if step_reports.get('dws_inventory', {}).get('status') != 'SUCCESS':
        upstream_blockers.append('dws_inventory')

    if upstream_blockers:
        detail = f"因上游未成功跳过: {', '.join(upstream_blockers)}"
        update_step('ads_health', 'WARNING', detail, step_start)
        logger.warning(detail)
    else:
        try:
            from etl_ads_health import run as run_ads_health, backfill_dabo_fields
            run_ads_health()

            # 达播数据就绪时，回填当日达播/自然字段（避免外部项目时序影响）
            if dabo_ready:
                backfill_dabo_fields()
                update_step('ads_health', 'SUCCESS', '健康度计算完成，已回填当日达播字段', step_start)
            else:
                update_step('ads_health', 'SUCCESS', '健康度计算完成，未回填达播字段', step_start)
        except Exception as e:
            error_msg = str(e).encode('utf-8', errors='ignore').decode('utf-8')
            update_step('ads_health', 'FAILED', _extract_error_summary(error_msg), step_start)
            logger.error(f"ads_health failed: {error_msg}")
    
    # 汇总结果
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    logger.info("\n" + "#"*60)
    logger.info("#  ETL执行完成 - 结果汇总")
    logger.info("#"*60)
    
    all_success = True
    for task in STEP_ORDER:
        if task not in step_reports:
            continue
        report = step_reports[task]
        result_text = report['status']
        if report['detail']:
            result_text = f"{result_text}: {report['detail']}"
        logger.info(f"  {task}: {result_text}")
        if report['status'] in ('FAILED', 'ERROR'):
            all_success = False
    
    logger.info(f"\nTotal time: {duration} seconds")
    
    if all_success:
        logger.info("All tasks executed successfully!")
    else:
        logger.warning("Some tasks failed, please check the logs")
    
    return all_success, step_reports


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
        normalized = _normalize_step_reports(details)
        for v in normalized.values():
            status = str(v.get('status', '')).upper()
            if status not in ('FAILED', 'ERROR'):
                continue
            low = str(v.get('detail', '')).lower()
            for nk in ETL_NON_RETRYABLE_ERROR_KEYWORDS:
                if nk.lower() in low:
                    return False

        # 如果定义了可重试关键字列表，则必须匹配其中至少一项才重试
        if ETL_RETRYABLE_ERROR_KEYWORDS:
            for v in normalized.values():
                status = str(v.get('status', '')).upper()
                if status not in ('FAILED', 'ERROR'):
                    continue
                low = str(v.get('detail', '')).lower()
                for rk in ETL_RETRYABLE_ERROR_KEYWORDS:
                    if rk.lower() in low:
                        return True
            return False

        return True
    except Exception:
        return True


def main_with_retries(run_func, max_retries=3, sleep_seconds=60, webhook_url=None):
    if webhook_url is None:
        try:
            from config import WECHAT_WEBHOOK as webhook_url
        except Exception:
            webhook_url = None

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        run_started_at = datetime.now()
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
                ended_at = datetime.now()
                summary_content = _compose_run_summary_message(
                    details,
                    overall_status='SUCCESS',
                    started_at=run_started_at,
                    ended_at=ended_at,
                    attempt=attempt,
                    max_retries=max_retries,
                )
                send_wechat_alert(webhook_url, summary_content)
                return 0
            else:
                logger.warning(f'ETL finished with partial failures on attempt {attempt}.')
                # 若有详细 results，判断是否应该继续重试
                should_retry = True
                if details:
                    should_retry = _should_retry_based_on_details(details)

                if not should_retry:
                    # 发现确定性不可重试错误，立即告警并返回
                    ended_at = datetime.now()
                    summary_content = _compose_run_summary_message(
                        details,
                        overall_status='FAILED',
                        started_at=run_started_at,
                        ended_at=ended_at,
                        attempt=attempt,
                        max_retries=max_retries,
                        extra_message='失败原因：命中不可重试错误，已停止重试。',
                    )
                    send_wechat_alert(webhook_url, summary_content)
                    return 2

                # 到达最大重试次数则发送告警
                if attempt >= max_retries:
                    ended_at = datetime.now()
                    summary_content = _compose_run_summary_message(
                        details,
                        overall_status='FAILED',
                        started_at=run_started_at,
                        ended_at=ended_at,
                        attempt=attempt,
                        max_retries=max_retries,
                        extra_message='已达到最大重试次数。',
                    )
                    send_wechat_alert(webhook_url, summary_content)
                    return 2

                logger.info(f'等待 {sleep_seconds} 秒后重试...')
                time.sleep(sleep_seconds)
        except Exception:
            tb = traceback.format_exc()
            logger.error(f'ETL 主任务抛出未捕获异常（尝试 {attempt}/{max_retries}）: {tb}')
            summary_line = _extract_error_summary(tb)
            if attempt >= max_retries:
                ended_at = datetime.now()
                synthetic_report = {
                    'runner_exception': {
                        'status': 'FAILED',
                        'detail': summary_line,
                        'duration_seconds': int((ended_at - run_started_at).total_seconds()),
                    }
                }
                summary_content = _compose_run_summary_message(
                    synthetic_report,
                    overall_status='ERROR',
                    started_at=run_started_at,
                    ended_at=ended_at,
                    attempt=attempt,
                    max_retries=max_retries,
                )
                send_wechat_alert(webhook_url, summary_content)
                return 1
            else:
                logger.info(f'等待 {sleep_seconds} 秒后重试...')
                time.sleep(sleep_seconds)
    return 1


def run_main(conn_test_flag=None, max_retries=None, retry_sleep=None):
    # 解析是否启用连接测试模式（命令行或环境变量）
    if conn_test_flag is None:
        conn_test_flag = ('--conn-test' in sys.argv) or (os.getenv('ETL_CONN_TEST', '0') == '1')

    # 支持通过环境变量临时覆盖最大重试次数（便于测试）
    if max_retries is None:
        try:
            max_retries = int(os.getenv('ETL_MAX_RETRIES', str(ETL_DEFAULT_MAX_RETRIES)))
        except Exception:
            max_retries = ETL_DEFAULT_MAX_RETRIES
    if retry_sleep is None:
        try:
            retry_sleep = int(os.getenv('ETL_RETRY_SLEEP', str(ETL_DEFAULT_RETRY_SLEEP)))
        except Exception:
            retry_sleep = ETL_DEFAULT_RETRY_SLEEP

    if conn_test_flag:
        logger.info('运行模式：仅连接测试（--conn-test / ETL_CONN_TEST=1）')
        runner = run_conn_test
    else:
        runner = run_all

    return main_with_retries(runner, max_retries=max_retries, sleep_seconds=retry_sleep)


if __name__ == '__main__':
    sys.exit(run_main())
