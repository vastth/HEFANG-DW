# -*- coding: utf-8 -*-
"""门店日报专题调度入口。

当前脚本负责 NAS 目标文件、负责人快照文件的正式调度导入、受影响日期判断，以及按日期列表批量重跑日报：
1. 解析最新目标文件
2. 自动模式下判断最新快照是否属于本轮自动 report_date 所在月份
3. 基于 log_store_target_import 做 MD5 幂等判重
4. 调用既有导入工具写入 cfg_store_target_daily / dim_store_report_attr / 共同考核配置表
5. 调用负责人快照导入工具写入 cfg_store_operation_owner_snapshot / dim_store_operation_owner_assignment
6. 按冻结规则、自然日覆盖缺口和 DWS freshness 产出受影响日期集合
7. 在需要时按日期列表顺序触发门店层、主体层与销售看板 ADS 批量重跑
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from pymysql.cursors import DictCursor

from alerts import send_wechat_alert
from cutover_controls import (
    CUTOVER_MODE_LEGACY,
    CUTOVER_MODE_SHADOW_COMPARE,
    CUTOVER_MODE_V2,
    derive_store_daily_freshness_source,
    resolve_cutover_mode,
)
from control_chain_summary import (
    should_suppress_child_wechat_alert,
    write_total_control_chain_summary,
)
from config import (
    ETL_DEFAULT_MAX_RETRIES,
    ETL_DEFAULT_RETRY_SLEEP,
    ETL_NON_RETRYABLE_ERROR_KEYWORDS,
    ETL_RETRYABLE_ERROR_KEYWORDS,
    WECHAT_WEBHOOK,
)
from db_connections import connect_mysql
from etl_ads_daily_sales import run as run_daily_sales_ads
from etl_ads_store_daily_report import run as run_store_daily_report_ads
from etl_ads_store_daily_subject_report import run as run_store_daily_subject_report_ads
from tools.import_cfg_store_target_daily_from_nas import (
    DEFAULT_NAS_DIR as TARGET_DEFAULT_NAS_DIR,
    DEFAULT_SHEET_NAME as TARGET_DEFAULT_SHEET_NAME,
    LOG_TABLE_NAME as TARGET_LOG_TABLE_NAME,
    LOG_TABLE_SQL_PATH as TARGET_LOG_TABLE_SQL_PATH,
    _compute_file_md5 as _compute_target_file_md5,
    _discover_nas_target_files,
    _parse_target_month_arg,
    _parse_workbook,
    _resolve_input_file as _resolve_target_input_file,
)
from tools.import_store_operation_owner_from_nas import (
    DDL_FILE_PATH as OWNER_DDL_FILE_PATH,
    LOG_TABLE_NAME as OWNER_LOG_TABLE_NAME,
    _compute_file_md5 as _compute_owner_file_md5,
    _resolve_input_file as _resolve_owner_input_file,
)
from tools.import_duty_free_store_mtd_sales_from_nas import (
    DEFAULT_NAS_FILE_PATH as DUTY_FREE_DEFAULT_NAS_FILE_PATH,
    DEFAULT_SHEET_NAME as DUTY_FREE_DEFAULT_SHEET_NAME,
    LOG_TABLE_NAME as DUTY_FREE_LOG_TABLE_NAME,
    LOG_TABLE_SQL_PATH as DUTY_FREE_LOG_TABLE_SQL_PATH,
    _compute_file_md5 as _compute_duty_free_file_md5,
    _parse_workbook as _parse_duty_free_workbook,
    _resolve_input_file as _resolve_duty_free_input_file,
)


PROJECT_DIR = Path(__file__).resolve().parent
os.chdir(PROJECT_DIR)


def _reconfigure_text_stream(stream: object) -> None:
    reconfigure = getattr(stream, 'reconfigure', None)
    if callable(reconfigure):
        reconfigure(encoding='utf-8')


_reconfigure_text_stream(sys.stdout)
_reconfigure_text_stream(sys.stderr)

LOG_DIR = PROJECT_DIR / 'logs'
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"store_daily_report_schedule_{datetime.now().strftime('%Y%m%d')}.log"
TARGET_IMPORT_SCRIPT_PATH = PROJECT_DIR / 'tools' / 'import_cfg_store_target_daily_from_nas.py'
OWNER_IMPORT_SCRIPT_PATH = PROJECT_DIR / 'tools' / 'import_store_operation_owner_from_nas.py'
DUTY_FREE_IMPORT_SCRIPT_PATH = PROJECT_DIR / 'tools' / 'import_duty_free_store_mtd_sales_from_nas.py'
DEFAULT_CREATED_BY = 'scheduled_store_daily_report'
AFFECTED_ADS_SHORT_LABEL = '门店层+主体层+销售看板'
AFFECTED_ADS_DETAIL_LABEL = '门店层、主体层与销售看板 ADS'
AFFECTED_ADS_FAILURE_LABEL = '门店层、主体层或销售看板 ADS'
AFFECTED_ADS_TABLE_DATE_COLUMNS = (
    ('ads_store_daily_report', 'report_date'),
    ('ads_store_daily_subject_report', 'report_date'),
    ('ads_daily_sales', 'report_date'),
)
DWS_SALES_FRESHNESS_LOOKBACK_DAYS = 7
LEGACY_SALES_FRESHNESS_TABLE = 'dws_sales_daily'
V2_SALES_FRESHNESS_TABLE = 'dws_sales_daily_v2'
AUTO_REPORT_DATE_MODE_PREVIOUS_DAY = 'previous-day'
AUTO_REPORT_DATE_MODE_CURRENT_DAY = 'current-day'
AUTO_REPORT_DATE_MODE_CHOICES = (
    AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
    AUTO_REPORT_DATE_MODE_CURRENT_DAY,
)
SCHEDULE_LOCK_NAME = 'hefang_dw:scheduled_store_daily_report'
NON_RETRYABLE_MESSAGE_KEYWORDS = (
    'validation_status',
    '未命中',
    '歧义',
    '重叠',
    '目标版本',
    '目标月份',
    '用户名或密码',
    'NAS 路径',
    'NAS 环境变量',
    '自动鉴权',
    '未找到工作表',
    '导入模板',
    '缺少',
    '格式不支持',
    '不是合法数字',
    '多个目标月份',
    '多个版本文件',
    '未找到日志表',
    '门店类型',
    '主体编码',
    '主店名称',
    '考核模式',
    '归属角色',
    '共同考核',
)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ImportInspection:
    file_path: Path
    file_md5: str
    target_month: str
    target_month_start: date
    target_version: str
    sheet_name: str
    source_row_count: int
    available_target_months: list[str]
    file_modified_at: str


@dataclass(frozen=True)
class OwnerImportInspection:
    file_path: Path
    file_md5: str
    snapshot_date: date
    sheet_name: str | None


@dataclass(frozen=True)
class OwnerImportRunResult:
    outcome: str
    inspection: OwnerImportInspection
    summary: dict | None
    existing_log: dict | None


@dataclass(frozen=True)
class DutyFreeImportInspection:
    file_path: Path
    file_md5: str
    target_month: str
    target_month_start: date
    data_version: str
    sheet_name: str
    source_row_count: int
    file_modified_at: str


@dataclass(frozen=True)
class DutyFreeImportRunResult:
    outcome: str
    inspection: DutyFreeImportInspection
    summary: dict | None
    existing_log: dict | None


@dataclass(frozen=True)
class ScheduleRunResult:
    outcome: str
    inspection: ImportInspection
    summary: dict | None
    existing_log: dict | None
    owner_result: OwnerImportRunResult | None
    affected_date_summary: dict
    ads_backfill_summary: dict
    duty_free_result: DutyFreeImportRunResult | None = None


@dataclass(frozen=True)
class AdsBackfillContext:
    data_version: str
    report_dates: list[str]
    source: str
    inspection: ImportInspection | None = None
    import_summary: dict | None = None
    target_outcome: str | None = None
    owner_result: OwnerImportRunResult | None = None
    duty_free_result: DutyFreeImportRunResult | None = None
    affected_date_summary: dict | None = None
    requested_report_dates: list[str] | None = None
    completed_report_dates: list[str] | None = None


class ScheduledImportError(RuntimeError):
    def __init__(
        self,
        message: str,
        retryable: bool = True,
        inspection: ImportInspection | OwnerImportInspection | None = None,
    ):
        super().__init__(message)
        self.retryable = retryable
        self.inspection = inspection


class ScheduledImportSkip(RuntimeError):
    pass


class ScheduledAdsBackfillError(RuntimeError):
    def __init__(
        self,
        message: str,
        retryable: bool,
        context: AdsBackfillContext,
        failed_details: list[dict[str, str]],
    ):
        super().__init__(message)
        self.retryable = retryable
        self.context = context
        self.failed_details = failed_details


def _connect():
    return connect_mysql(
        cursorclass=DictCursor,
        autocommit=True,
    )


def _acquire_singleton_lock(lock_name: str, timeout_seconds: int = 0) -> tuple[object, int | None]:
    lock_conn = _connect()
    try:
        with lock_conn.cursor() as cursor:
            cursor.execute(
                'SELECT CONNECTION_ID() AS connection_id, GET_LOCK(%s, %s) AS got_lock',
                (lock_name, timeout_seconds),
            )
            row = cursor.fetchone()
    except Exception:
        lock_conn.close()
        raise

    connection_id = row.get('connection_id') if row else None
    got_lock = row.get('got_lock') if row else None
    if got_lock != 1:
        lock_conn.close()
        raise ScheduledImportError(
            f'已有其他门店日报专题调度实例在运行，请勿并发重复触发: lock_name={lock_name}',
            retryable=False,
        )
    return lock_conn, connection_id


def _release_singleton_lock(lock_conn, lock_name: str) -> None:
    if lock_conn is None:
        return

    try:
        with lock_conn.cursor() as cursor:
            cursor.execute('SELECT RELEASE_LOCK(%s) AS released_lock', (lock_name,))
    except Exception as exc:
        logger.warning('释放专题调度单实例锁失败: lock_name=%s, error=%s', lock_name, exc)
    finally:
        lock_conn.close()


def _format_datetime_value(value: object) -> str:
    if value is None:
        return '-'
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)


def _current_target_month(run_date: date | None = None) -> str:
    return (run_date or date.today()).strftime('%Y-%m')


def _resolve_auto_report_anchor_date(
    run_date: date,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> date:
    if report_date_mode == AUTO_REPORT_DATE_MODE_CURRENT_DAY:
        return run_date
    if report_date_mode == AUTO_REPORT_DATE_MODE_PREVIOUS_DAY:
        return run_date - timedelta(days=1)
    raise ValueError(f'未识别的自动 report_date 模式: {report_date_mode}')


def _expected_auto_target_month(
    run_date: date | None = None,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> str:
    anchor_date = _resolve_auto_report_anchor_date(run_date or date.today(), report_date_mode)
    return anchor_date.strftime('%Y-%m')


def _format_date_value(value: date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _normalize_report_dates(report_dates: Sequence[date | str]) -> list[str]:
    normalized_dates: list[str] = []
    seen_dates: set[str] = set()
    for raw_value in report_dates:
        report_date_text = raw_value.isoformat() if isinstance(raw_value, date) else str(raw_value)
        if report_date_text in seen_dates:
            continue
        seen_dates.add(report_date_text)
        normalized_dates.append(report_date_text)
    return normalized_dates


def _format_report_dates(report_dates: list[str]) -> str:
    if not report_dates:
        return '-'
    if len(report_dates) <= 10:
        return ','.join(report_dates)
    return f"{report_dates[0]} ~ {report_dates[-1]}"


def _last_day_of_month(month_start: date) -> date:
    next_month_anchor = month_start.replace(day=28) + timedelta(days=4)
    return next_month_anchor - timedelta(days=next_month_anchor.day)


def _build_date_range(start_date: date, end_date: date) -> list[str]:
    if start_date > end_date:
        return []

    dates: list[str] = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.isoformat())
        current_date += timedelta(days=1)
    return dates


def _resolve_auto_report_date_upper_bound(
    target_month_start: date,
    run_date: date,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> date:
    month_end = _last_day_of_month(target_month_start)
    raw_upper_bound = _resolve_auto_report_anchor_date(run_date, report_date_mode)
    return min(month_end, raw_upper_bound)


def _resolve_default_owner_snapshot_date(
    target_month_start: date,
    run_date: date,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> date:
    return _resolve_auto_report_date_upper_bound(
        target_month_start,
        run_date,
        report_date_mode,
    )


def _build_disabled_natural_progress_branch(note: str) -> dict:
    return {
        'enabled': False,
        'rule': 'disabled',
        'start_date': None,
        'end_date': None,
        'date_count': 0,
        'latest_report_dates': {},
        'missing_tables': [],
        'note': note,
    }


def _fetch_ads_latest_report_dates(
    target_month_start: date,
    upper_bound: date,
    data_version: str,
) -> dict[str, str | None]:
    latest_report_dates: dict[str, str | None] = {}
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            for table_name, date_column in AFFECTED_ADS_TABLE_DATE_COLUMNS:
                cursor.execute(
                    f"""
                    SELECT MAX({date_column}) AS latest_report_date
                    FROM {table_name}
                    WHERE data_version = %s
                      AND {date_column} BETWEEN %s AND %s
                    """,
                    (data_version, target_month_start, upper_bound),
                )
                row = cursor.fetchone() or {}
                raw_value = row.get('latest_report_date')
                if isinstance(raw_value, datetime):
                    latest_report_dates[table_name] = raw_value.date().isoformat()
                elif isinstance(raw_value, date):
                    latest_report_dates[table_name] = raw_value.isoformat()
                elif raw_value is None:
                    latest_report_dates[table_name] = None
                else:
                    latest_report_dates[table_name] = str(raw_value)
    finally:
        conn.close()
    return latest_report_dates


def _build_disabled_source_freshness_branch(note: str) -> dict:
    return {
        'enabled': False,
        'rule': 'disabled',
        'start_date': None,
        'end_date': None,
        'date_count': 0,
        'stale_dates': [],
        'stale_table_counts': {},
        'note': note,
    }


def _coerce_date_value(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _resolve_sales_freshness_table_name(source_mode: str) -> str:
    if source_mode == 'legacy':
        return LEGACY_SALES_FRESHNESS_TABLE
    if source_mode == 'v2':
        return V2_SALES_FRESHNESS_TABLE
    raise ValueError(f'不支持的 freshness 来源模式: {source_mode}')


def _fetch_sales_etl_time_map(
    conn,
    start_date: date,
    end_date: date,
    sales_table_name: str,
) -> dict[date, datetime]:
    start_date_id = int(start_date.strftime('%Y%m%d'))
    end_date_id = int(end_date.strftime('%Y%m%d'))
    sql = f"""
        SELECT
            STR_TO_DATE(CAST(date_id AS CHAR), '%%Y%%m%%d') AS report_date_alias,
            MAX(etl_time) AS etl_time_alias
        FROM {sales_table_name}
        WHERE date_id BETWEEN %s AND %s
        GROUP BY STR_TO_DATE(CAST(date_id AS CHAR), '%%Y%%m%%d')
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (start_date_id, end_date_id))
        rows = cursor.fetchall()

    result: dict[date, datetime] = {}
    for row in rows:
        report_date = _coerce_date_value(row.get('report_date_alias'))
        etl_time = row.get('etl_time_alias')
        if report_date is not None and isinstance(etl_time, datetime):
            result[report_date] = etl_time
    return result


def _fetch_ads_etl_time_map(
    conn,
    table_name: str,
    date_column: str,
    start_date: date,
    end_date: date,
    data_version: str,
) -> dict[date, datetime]:
    sql = f"""
        SELECT
            {date_column} AS report_date_alias,
            MAX(etl_time) AS etl_time_alias
        FROM {table_name}
        WHERE data_version = %s
          AND {date_column} BETWEEN %s AND %s
        GROUP BY {date_column}
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (data_version, start_date, end_date))
        rows = cursor.fetchall()

    result: dict[date, datetime] = {}
    for row in rows:
        report_date = _coerce_date_value(row.get('report_date_alias'))
        etl_time = row.get('etl_time_alias')
        if report_date is not None and isinstance(etl_time, datetime):
            result[report_date] = etl_time
    return result


def _fetch_stale_ads_dates_by_dws_freshness(
    target_month_start: date,
    upper_bound: date,
    data_version: str,
    sales_freshness_source_mode: str = 'legacy',
) -> dict:
    freshness_start = max(
        target_month_start,
        upper_bound - timedelta(days=DWS_SALES_FRESHNESS_LOOKBACK_DAYS - 1),
    )
    if freshness_start > upper_bound:
        return _build_disabled_source_freshness_branch('DWS freshness 检查窗口为空')

    sales_freshness_table = _resolve_sales_freshness_table_name(sales_freshness_source_mode)
    conn = _connect()
    try:
        dws_etl_time_map = _fetch_sales_etl_time_map(
            conn,
            freshness_start,
            upper_bound,
            sales_freshness_table,
        )
        ads_etl_time_maps = {
            table_name: _fetch_ads_etl_time_map(
                conn,
                table_name,
                date_column,
                freshness_start,
                upper_bound,
                data_version,
            )
            for table_name, date_column in AFFECTED_ADS_TABLE_DATE_COLUMNS
        }
    finally:
        conn.close()

    stale_dates: list[str] = []
    stale_table_counts = {table_name: 0 for table_name, _ in AFFECTED_ADS_TABLE_DATE_COLUMNS}
    for report_date, dws_etl_time in sorted(dws_etl_time_map.items()):
        stale_tables: list[str] = []
        for table_name, _ in AFFECTED_ADS_TABLE_DATE_COLUMNS:
            ads_etl_time = ads_etl_time_maps[table_name].get(report_date)
            if ads_etl_time is None or dws_etl_time > ads_etl_time:
                stale_tables.append(table_name)
                stale_table_counts[table_name] += 1
        if stale_tables:
            stale_dates.append(report_date.isoformat())

    if not stale_dates:
        return {
            **_build_disabled_source_freshness_branch(
                f'近 7 天专题 ADS etl_time 均不早于 {sales_freshness_table}，本次不因源刷新触发重跑'
            ),
            'rule': f'{sales_freshness_table}_etl_time_newer_than_ads',
            'start_date': freshness_start.isoformat(),
            'end_date': upper_bound.isoformat(),
            'sales_freshness_table': sales_freshness_table,
        }

    return {
        'enabled': True,
        'rule': f'{sales_freshness_table}_etl_time_newer_than_ads',
        'start_date': stale_dates[0],
        'end_date': stale_dates[-1],
        'date_count': len(stale_dates),
        'stale_dates': stale_dates,
        'stale_table_counts': {key: value for key, value in stale_table_counts.items() if value > 0},
        'note': f'检测到主链 {sales_freshness_table} 的 etl_time 晚于专题 ADS，按近 7 天 freshness 窗口触发重跑',
        'sales_freshness_table': sales_freshness_table,
    }


def _apply_natural_progress_fallback(
    inspection: ImportInspection,
    affected_date_summary: dict,
    sales_freshness_source_mode: str = 'legacy',
) -> dict:
    summary = dict(affected_date_summary)
    if summary['affected_date_count'] > 0:
        summary['natural_progress_branch'] = _build_disabled_natural_progress_branch(
            '当前已存在配置/负责人驱动的受影响日期集合，不触发自然日推进兜底'
        )
        return summary

    upper_bound_text = summary.get('upper_bound')
    if not upper_bound_text:
        summary['natural_progress_branch'] = _build_disabled_natural_progress_branch(
            '当前未产生统一上界，不触发自然日推进兜底'
        )
        return summary

    upper_bound = date.fromisoformat(str(upper_bound_text))
    if upper_bound < inspection.target_month_start:
        summary['natural_progress_branch'] = _build_disabled_natural_progress_branch(
            '统一上界早于目标月首日，不触发自然日推进兜底'
        )
        return summary

    latest_report_dates = _fetch_ads_latest_report_dates(
        inspection.target_month_start,
        upper_bound,
        inspection.target_version,
    )
    missing_tables = [table_name for table_name, latest_date in latest_report_dates.items() if latest_date is None]

    if missing_tables:
        fallback_start_date = inspection.target_month_start
        fallback_dates = _build_date_range(fallback_start_date, upper_bound)
        branch_rule = 'full_month_if_any_ads_table_missing'
        branch_note = '检测到至少一张 ADS 在当前月份仍无数据，按整月缺口触发自然日推进兜底'
    else:
        min_latest_date = min(date.fromisoformat(latest_date) for latest_date in latest_report_dates.values() if latest_date)
        fallback_start_date = min_latest_date + timedelta(days=1)
        fallback_dates = _build_date_range(fallback_start_date, upper_bound)
        branch_rule = 'min_ads_latest_report_date_plus_one_to_upper_bound'
        branch_note = '按最落后 ADS 的最新 report_date 补齐到统一上界'

    if not fallback_dates:
        source_freshness_branch = _fetch_stale_ads_dates_by_dws_freshness(
            inspection.target_month_start,
            upper_bound,
            inspection.target_version,
            sales_freshness_source_mode=sales_freshness_source_mode,
        )
        if source_freshness_branch['enabled']:
            summary['affected_dates'] = source_freshness_branch['stale_dates']
            summary['affected_date_count'] = source_freshness_branch['date_count']
            summary['source_freshness_branch'] = source_freshness_branch
            summary['natural_progress_branch'] = {
                'enabled': False,
                'rule': branch_rule,
                'start_date': None,
                'end_date': None,
                'date_count': 0,
                'latest_report_dates': latest_report_dates,
                'missing_tables': missing_tables,
                'note': '当前 ADS report_date 覆盖已达到统一上界，改由 DWS freshness 触发重跑',
            }
            summary['note'] = (
                f"{summary['note']}；DWS freshness 检查发现 {source_freshness_branch['date_count']} 天专题 ADS 早于主链，"
                f"范围 {source_freshness_branch['start_date']} ~ {source_freshness_branch['end_date']}"
            )
            return summary

        summary['natural_progress_branch'] = {
            'enabled': False,
            'rule': branch_rule,
            'start_date': None,
            'end_date': None,
            'date_count': 0,
            'latest_report_dates': latest_report_dates,
            'missing_tables': missing_tables,
            'note': '当前 ADS 覆盖已达到统一上界，不触发自然日推进兜底',
        }
        summary['source_freshness_branch'] = source_freshness_branch
        return summary

    summary['affected_dates'] = fallback_dates
    summary['affected_date_count'] = len(fallback_dates)
    summary['natural_progress_branch'] = {
        'enabled': True,
        'rule': branch_rule,
        'start_date': fallback_dates[0],
        'end_date': fallback_dates[-1],
        'date_count': len(fallback_dates),
        'latest_report_dates': latest_report_dates,
        'missing_tables': missing_tables,
        'note': branch_note,
    }
    summary['note'] = (
        f"{summary['note']}；自然日推进兜底补充 {len(fallback_dates)} 天缺口，"
        f"范围 {fallback_dates[0]} ~ {fallback_dates[-1]}"
    )
    return summary


def _parse_summary_date(summary: dict, field_name: str, inspection: ImportInspection) -> date:
    raw_value = summary.get(field_name)
    if raw_value in (None, ''):
        raise ScheduledImportError(
            f'导入摘要缺少 {field_name}，无法生成受影响日期集合',
            retryable=False,
            inspection=inspection,
        )

    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value

    try:
        return date.fromisoformat(str(raw_value))
    except ValueError as exc:
        raise ScheduledImportError(
            f'导入摘要中的 {field_name} 不是合法日期: {raw_value}',
            retryable=False,
            inspection=inspection,
        ) from exc


def _build_affected_date_summary(
    outcome: str,
    inspection: ImportInspection,
    summary: dict | None,
    schedule_run_date: date | None = None,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> dict:
    run_date = schedule_run_date or date.today()
    empty_branch = {
        'enabled': False,
        'rule': 'disabled',
        'start_date': None,
        'end_date': None,
        'date_count': 0,
    }
    store_attr_branch = dict(empty_branch)
    store_attr_branch['effective_start_date'] = None

    if outcome == 'CONN_TEST':
        return {
            'outcome': outcome,
            'target_month': inspection.target_month,
            'target_version': inspection.target_version,
            'schedule_run_date': run_date.isoformat(),
            'upper_bound': None,
            'target_branch': dict(empty_branch),
            'store_attr_branch': store_attr_branch,
            'affected_dates': [],
            'affected_date_count': 0,
            'note': '命中 conn-test 跳过规则，不产生新的受影响日期集合',
        }

    if outcome == 'SKIPPED':
        upper_bound = _resolve_auto_report_date_upper_bound(
            inspection.target_month_start,
            run_date,
            report_date_mode=report_date_mode,
        )
        return {
            'outcome': outcome,
            'target_month': inspection.target_month,
            'target_version': inspection.target_version,
            'schedule_run_date': run_date.isoformat(),
            'upper_bound': upper_bound.isoformat(),
            'target_branch': dict(empty_branch),
            'store_attr_branch': store_attr_branch,
            'affected_dates': [],
            'affected_date_count': 0,
            'note': '命中 file_md5 + target_month + target_version 已成功导入跳过规则，不产生新的受影响日期集合',
        }

    if outcome != 'IMPORTED' or summary is None:
        raise ScheduledImportError(
            f'未识别的专题调度结果: {outcome}',
            retryable=False,
            inspection=inspection,
        )

    upper_bound = _resolve_auto_report_date_upper_bound(
        inspection.target_month_start,
        run_date,
        report_date_mode=report_date_mode,
    )
    target_dates = _build_date_range(inspection.target_month_start, upper_bound)
    target_branch = {
        'enabled': True,
        'rule': 'cfg_store_target_daily_apply_full_month',
        'start_date': _format_date_value(inspection.target_month_start if target_dates else None),
        'end_date': _format_date_value(upper_bound if target_dates else None),
        'date_count': len(target_dates),
    }

    store_attr_effective_start_date = None
    store_attr_dates: list[str] = []
    if summary.get('sync_store_report_attr'):
        store_attr_effective_start_date = _parse_summary_date(summary, 'store_attr_effective_start_date', inspection)
        store_attr_dates = _build_date_range(store_attr_effective_start_date, upper_bound)
        store_attr_branch = {
            'enabled': True,
            'rule': 'store_attr_effective_start_to_upper_bound',
            'effective_start_date': store_attr_effective_start_date.isoformat(),
            'start_date': _format_date_value(store_attr_effective_start_date if store_attr_dates else None),
            'end_date': _format_date_value(upper_bound if store_attr_dates else None),
            'date_count': len(store_attr_dates),
        }

    affected_dates = sorted(set(target_dates) | set(store_attr_dates))
    if affected_dates:
        note = f'已生成受影响日期集合；默认由专题调度继续消费并批量重跑{AFFECTED_ADS_DETAIL_LABEL}'
    elif upper_bound < inspection.target_month_start:
        note = '统一上界早于目标月首日，本次受影响日期集合为空'
    else:
        note = '按冻结规则计算后，本次受影响日期集合为空'

    return {
        'outcome': outcome,
        'target_month': inspection.target_month,
        'target_version': inspection.target_version,
        'schedule_run_date': run_date.isoformat(),
        'upper_bound': upper_bound.isoformat(),
        'target_branch': target_branch,
        'store_attr_branch': store_attr_branch,
        'affected_dates': affected_dates,
        'affected_date_count': len(affected_dates),
        'note': note,
    }


def _build_disabled_owner_affected_date_summary(schedule_run_date: date | None = None) -> dict:
    run_date = schedule_run_date or date.today()
    return {
        'outcome': 'DISABLED',
        'snapshot_date': None,
        'schedule_run_date': run_date.isoformat(),
        'upper_bound': None,
        'owner_branch': {
            'enabled': False,
            'rule': 'disabled_by_cli',
            'snapshot_date': None,
            'history_change_count': 0,
            'start_date': None,
            'end_date': None,
            'date_count': 0,
        },
        'affected_dates': [],
        'affected_date_count': 0,
        'note': '命中 --no-run-owner-import，本次不执行负责人快照导入，也不新增负责人受影响日期集合',
    }


def _count_owner_history_changes(summary: dict | None) -> int:
    if not summary:
        return 0
    history_diff_counts = summary.get('history_diff_counts') or {}
    return sum(int(history_diff_counts.get(key, 0) or 0) for key in ('changed', 'new', 'exited'))


def _resolve_owner_affected_start_date(summary: dict, owner_snapshot_date: date) -> date:
    raw_value = summary.get('earliest_history_effective_start_date')
    if raw_value in (None, ''):
        return owner_snapshot_date
    if isinstance(raw_value, datetime):
        return raw_value.date()
    if isinstance(raw_value, date):
        return raw_value
    return date.fromisoformat(str(raw_value))


def _build_owner_affected_date_summary(
    owner_result: OwnerImportRunResult | None,
    inspection: ImportInspection,
    schedule_run_date: date | None = None,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> dict:
    run_date = schedule_run_date or date.today()
    empty_branch = {
        'enabled': False,
        'rule': 'disabled',
        'snapshot_date': None,
        'history_change_count': 0,
        'start_date': None,
        'end_date': None,
        'date_count': 0,
    }

    if owner_result is None:
        return _build_disabled_owner_affected_date_summary(run_date)

    base_summary = {
        'outcome': owner_result.outcome,
        'snapshot_date': owner_result.inspection.snapshot_date.isoformat(),
        'schedule_run_date': run_date.isoformat(),
        'upper_bound': None,
        'owner_branch': dict(empty_branch),
        'affected_dates': [],
        'affected_date_count': 0,
    }

    if owner_result.outcome == 'CONN_TEST':
        base_summary['note'] = '命中 conn-test 跳过规则，不产生新的负责人受影响日期集合'
        return base_summary

    if owner_result.outcome == 'SKIPPED':
        base_summary['note'] = '命中 file_md5 + snapshot_date 已成功导入跳过规则，不产生新的负责人受影响日期集合'
        return base_summary

    if owner_result.outcome != 'IMPORTED' or owner_result.summary is None:
        raise ScheduledImportError(
            f'未识别的负责人专题调度结果: {owner_result.outcome}',
            retryable=False,
            inspection=owner_result.inspection,
        )

    owner_history_change_count = _count_owner_history_changes(owner_result.summary)
    upper_bound = _resolve_auto_report_date_upper_bound(
        inspection.target_month_start,
        run_date,
        report_date_mode=report_date_mode,
    )
    owner_effective_start_date = _resolve_owner_affected_start_date(
        owner_result.summary,
        owner_result.inspection.snapshot_date,
    )
    start_date = max(owner_effective_start_date, inspection.target_month_start)
    owner_dates = []
    if owner_history_change_count > 0:
        owner_dates = _build_date_range(start_date, upper_bound)

    owner_branch = {
        'enabled': owner_history_change_count > 0,
        'rule': 'owner_effective_start_to_upper_bound',
        'snapshot_date': owner_result.inspection.snapshot_date.isoformat(),
        'effective_start_date': owner_effective_start_date.isoformat(),
        'history_change_count': owner_history_change_count,
        'start_date': _format_date_value(start_date if owner_dates else None),
        'end_date': _format_date_value(upper_bound if owner_dates else None),
        'date_count': len(owner_dates),
    }

    if owner_history_change_count == 0:
        note = '负责人快照导入完成，但未产生 changed/new/exited 历史切片，不新增负责人受影响日期集合'
    elif upper_bound < start_date:
        note = '负责人快照导入完成，但专题调度统一上界早于负责人受影响日期起点，本次负责人受影响日期集合为空'
    else:
        note = '已根据负责人快照历史变更生成负责人受影响日期集合'

    return {
        'outcome': owner_result.outcome,
        'snapshot_date': owner_result.inspection.snapshot_date.isoformat(),
        'schedule_run_date': run_date.isoformat(),
        'upper_bound': upper_bound.isoformat(),
        'owner_branch': owner_branch,
        'affected_dates': owner_dates,
        'affected_date_count': len(owner_dates),
        'note': note,
    }


def _merge_affected_date_summaries(
    target_affected_date_summary: dict,
    owner_affected_date_summary: dict,
    duty_free_affected_date_summary: dict | None = None,
) -> dict:
    if duty_free_affected_date_summary is None:
        duty_free_affected_date_summary = _build_disabled_duty_free_affected_date_summary()

    affected_dates = sorted(
        set(target_affected_date_summary['affected_dates'])
        | set(owner_affected_date_summary['affected_dates'])
        | set(duty_free_affected_date_summary['affected_dates'])
    )
    upper_bound_candidates = [
        candidate
        for candidate in (
            target_affected_date_summary.get('upper_bound'),
            owner_affected_date_summary.get('upper_bound'),
            duty_free_affected_date_summary.get('upper_bound'),
        )
        if candidate
    ]
    if affected_dates:
        note = f'已汇总目标链路、负责人链路与免税月累计链路受影响日期；默认由专题调度继续消费并批量重跑{AFFECTED_ADS_DETAIL_LABEL}'
    else:
        note = (
            f"目标链路: {target_affected_date_summary['note']}；"
            f"负责人链路: {owner_affected_date_summary['note']}；"
            f"免税链路: {duty_free_affected_date_summary['note']}"
        )

    return {
        'outcome': target_affected_date_summary['outcome'],
        'target_month': target_affected_date_summary['target_month'],
        'target_version': target_affected_date_summary['target_version'],
        'schedule_run_date': target_affected_date_summary['schedule_run_date'],
        'upper_bound': max(upper_bound_candidates) if upper_bound_candidates else None,
        'target_branch': target_affected_date_summary['target_branch'],
        'store_attr_branch': target_affected_date_summary['store_attr_branch'],
        'owner_branch': owner_affected_date_summary['owner_branch'],
        'duty_free_branch': duty_free_affected_date_summary['duty_free_branch'],
        'target_note': target_affected_date_summary['note'],
        'owner_note': owner_affected_date_summary['note'],
        'duty_free_note': duty_free_affected_date_summary['note'],
        'affected_dates': affected_dates,
        'affected_date_count': len(affected_dates),
        'note': note,
        'natural_progress_branch': _build_disabled_natural_progress_branch('尚未执行自然日推进兜底判定'),
        'source_freshness_branch': _build_disabled_source_freshness_branch('尚未执行 DWS freshness 判定'),
    }


def _format_affected_date_detail(affected_date_summary: dict) -> str:
    affected_dates = affected_date_summary['affected_dates']
    if not affected_dates:
        affected_date_display = '-'
    elif len(affected_dates) <= 10:
        affected_date_display = ','.join(affected_dates)
    else:
        affected_date_display = f"{affected_dates[0]} ~ {affected_dates[-1]}"

    return (
        f"outcome={affected_date_summary['outcome']}, "
        f"count={affected_date_summary['affected_date_count']}, "
        f"upper_bound={affected_date_summary['upper_bound'] or '-'}, "
        f"target_branch={affected_date_summary['target_branch']['date_count']}, "
        f"store_attr_branch={affected_date_summary['store_attr_branch']['date_count']}, "
        f"owner_branch={affected_date_summary['owner_branch']['date_count']}, "
        f"duty_free_branch={affected_date_summary.get('duty_free_branch', {}).get('date_count', 0)}, "
        f"natural_progress_branch={affected_date_summary.get('natural_progress_branch', {}).get('date_count', 0)}, "
        f"source_freshness_branch={affected_date_summary.get('source_freshness_branch', {}).get('date_count', 0)}, "
        f"dates={affected_date_display}, "
        f"note={affected_date_summary['note']}"
    )


def _build_skipped_ads_backfill_summary(
    reason: str,
    data_version: str,
    source: str,
    report_dates: list[str] | None = None,
) -> dict:
    requested_report_dates = _normalize_report_dates(report_dates or [])
    reason_note_map = {
        'conn_test': f'命中 conn-test，本次不触发{AFFECTED_ADS_DETAIL_LABEL}批量重跑',
        'import_skipped': f'命中 file_md5 + target_month + target_version 幂等跳过规则，本次不触发{AFFECTED_ADS_DETAIL_LABEL}批量重跑',
        'empty_affected_dates': f'本次受影响日期集合为空，不触发{AFFECTED_ADS_DETAIL_LABEL}批量重跑',
        'disabled_by_cli': f'命中 --no-run-affected-ads，本次仅记录受影响日期，不执行{AFFECTED_ADS_DETAIL_LABEL}批量重跑',
    }
    return {
        'mode': 'SKIPPED',
        'reason': reason,
        'source': source,
        'data_version': data_version,
        'requested_report_dates': requested_report_dates,
        'requested_date_count': len(requested_report_dates),
        'completed_report_dates': [],
        'completed_date_count': 0,
        'failed_report_dates': [],
        'failed_date_count': 0,
        'failed_details': [],
        'note': reason_note_map[reason],
    }


def _format_ads_backfill_detail(ads_backfill_summary: dict) -> str:
    return (
        f"mode={ads_backfill_summary['mode']}, "
        f"source={ads_backfill_summary['source']}, "
        f"data_version={ads_backfill_summary['data_version']}, "
        f"requested={ads_backfill_summary['requested_date_count']}, "
        f"completed={ads_backfill_summary['completed_date_count']}, "
        f"failed={ads_backfill_summary['failed_date_count']}, "
        f"dates={_format_report_dates(ads_backfill_summary['requested_report_dates'])}, "
        f"note={ads_backfill_summary['note']}"
    )


def _build_explicit_ads_backfill_context(args: argparse.Namespace) -> AdsBackfillContext:
    requested_report_dates = _normalize_report_dates(args.rerun_report_date or [])
    return AdsBackfillContext(
        data_version=args.rerun_data_version,
        report_dates=requested_report_dates,
        source='explicit_dates',
        requested_report_dates=requested_report_dates,
        completed_report_dates=[],
    )


def _build_affected_ads_backfill_context(
    inspection: ImportInspection,
    summary: dict | None,
    target_outcome: str,
    owner_result: OwnerImportRunResult | None,
    duty_free_result: DutyFreeImportRunResult | None,
    affected_date_summary: dict,
) -> AdsBackfillContext:
    requested_report_dates = _normalize_report_dates(affected_date_summary['affected_dates'])
    return AdsBackfillContext(
        data_version=inspection.target_version,
        report_dates=requested_report_dates,
        source='affected_dates',
        inspection=inspection,
        import_summary=summary,
        target_outcome=target_outcome,
        owner_result=owner_result,
        duty_free_result=duty_free_result,
        affected_date_summary=affected_date_summary,
        requested_report_dates=requested_report_dates,
        completed_report_dates=[],
    )


def _run_ads_backfill_context(
    context: AdsBackfillContext,
    max_retries: int,
    retry_sleep: int,
    store_run_func=run_store_daily_report_ads,
    subject_run_func=run_store_daily_subject_report_ads,
    daily_sales_run_func=run_daily_sales_ads,
) -> dict:
    requested_report_dates = _normalize_report_dates(context.requested_report_dates or context.report_dates)
    pending_report_dates = _normalize_report_dates(context.report_dates)
    completed_report_dates = list(context.completed_report_dates or [])

    if not pending_report_dates:
        return {
            'mode': 'EXECUTED',
            'source': context.source,
            'data_version': context.data_version,
            'requested_report_dates': requested_report_dates,
            'requested_date_count': len(requested_report_dates),
            'completed_report_dates': completed_report_dates,
            'completed_date_count': len(completed_report_dates),
            'failed_report_dates': [],
            'failed_date_count': 0,
            'failed_details': [],
            'note': '全部目标日期已处理完成',
        }

    failed_details: list[dict[str, str]] = []
    for index, report_date_text in enumerate(pending_report_dates, start=1):
        logger.info(
            '开始执行门店日报 ADS 批量重跑: report_date=%s, data_version=%s, source=%s, progress=%s/%s',
            report_date_text,
            context.data_version,
            context.source,
            index,
            len(pending_report_dates),
        )
        try:
            store_result = store_run_func(
                report_date=date.fromisoformat(report_date_text),
                data_version=context.data_version,
                max_retries=max_retries,
                retry_sleep=retry_sleep,
            )
            subject_result = subject_run_func(
                report_date=date.fromisoformat(report_date_text),
                data_version=context.data_version,
                max_retries=max_retries,
                retry_sleep=retry_sleep,
            )
            daily_sales_result = daily_sales_run_func(
                report_date=date.fromisoformat(report_date_text),
                data_version=context.data_version,
                max_retries=max_retries,
                retry_sleep=retry_sleep,
            )
            store_output_row_count = store_result.get('output_row_count') if isinstance(store_result, dict) else None
            store_duration_seconds = store_result.get('duration_seconds', 0) if isinstance(store_result, dict) else 0
            subject_output_row_count = subject_result.get('output_row_count') if isinstance(subject_result, dict) else None
            subject_duration_seconds = subject_result.get('duration_seconds', 0) if isinstance(subject_result, dict) else 0
            daily_sales_output_row_count = (
                daily_sales_result.get('output_row_count') if isinstance(daily_sales_result, dict) else None
            )
            daily_sales_duration_seconds = (
                daily_sales_result.get('duration_seconds', 0) if isinstance(daily_sales_result, dict) else 0
            )
            completed_report_dates.append(report_date_text)
            logger.info(
                '门店日报 ADS 批量重跑完成: report_date=%s, store_output=%s, subject_output=%s, daily_sales_output=%s, duration=%s秒',
                report_date_text,
                store_output_row_count,
                subject_output_row_count,
                daily_sales_output_row_count,
                max(
                    store_duration_seconds,
                    subject_duration_seconds,
                    daily_sales_duration_seconds,
                ),
            )
        except Exception as exc:
            failed_details.append(
                {
                    'report_date': report_date_text,
                    'error_message': str(exc),
                }
            )
            logger.error(
                '门店日报 ADS 批量重跑失败: report_date=%s, data_version=%s, error=%s',
                report_date_text,
                context.data_version,
                exc,
            )

    if failed_details:
        remaining_context = AdsBackfillContext(
            data_version=context.data_version,
            report_dates=[item['report_date'] for item in failed_details],
            source=context.source,
            inspection=context.inspection,
            import_summary=context.import_summary,
            target_outcome=context.target_outcome,
            owner_result=context.owner_result,
            duty_free_result=context.duty_free_result,
            affected_date_summary=context.affected_date_summary,
            requested_report_dates=requested_report_dates,
            completed_report_dates=completed_report_dates,
        )
        first_error_message = failed_details[0]['error_message']
        raise ScheduledAdsBackfillError(
            message=(
                f'{AFFECTED_ADS_FAILURE_LABEL}批量重跑未完成，'
                f"已完成 {len(completed_report_dates)} 天，剩余 {len(failed_details)} 天，首个错误: {first_error_message}"
            ),
            retryable=_is_retryable_message(first_error_message, False),
            context=remaining_context,
            failed_details=failed_details,
        )

    return {
        'mode': 'EXECUTED',
        'source': context.source,
        'data_version': context.data_version,
        'requested_report_dates': requested_report_dates,
        'requested_date_count': len(requested_report_dates),
        'completed_report_dates': completed_report_dates,
        'completed_date_count': len(completed_report_dates),
        'failed_report_dates': [],
        'failed_date_count': 0,
        'failed_details': [],
        'note': f'已按日期列表顺序完成{AFFECTED_ADS_DETAIL_LABEL}批量重跑',
    }


def _resolve_target_file(file_path_arg: str | None, target_month: str | None) -> Path:
    if file_path_arg or target_month is not None:
        return _resolve_target_input_file(file_path_arg, target_month)

    candidates = _discover_nas_target_files(TARGET_DEFAULT_NAS_DIR)
    if not candidates:
        raise ScheduledImportSkip(
            f'NAS 目录 {TARGET_DEFAULT_NAS_DIR} 下暂无可处理的目标快照，本轮不执行自动导入'
        )

    latest = max(candidates, key=lambda item: (item.last_modified_at, item.file_path.name))
    return latest.file_path


def _inspect_target_file(
    file_path_arg: str | None,
    target_month: str | None,
    sheet_name: str,
    sync_store_report_attr: bool,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
    run_date: date | None = None,
) -> ImportInspection:
    try:
        file_path = _resolve_target_file(file_path_arg, target_month)
        file_md5 = _compute_target_file_md5(file_path)
        _, workbook_summary = _parse_workbook(
            file_path,
            sheet_name,
            require_store_type=sync_store_report_attr,
            target_month_filter=target_month,
        )
        if file_path_arg is None and target_month is None:
            anchor_date = _resolve_auto_report_anchor_date(run_date or date.today(), report_date_mode)
            expected_target_month = _expected_auto_target_month(run_date, report_date_mode)
            if workbook_summary['target_month'] != expected_target_month:
                raise ScheduledImportSkip(
                    '最新 NAS 快照 '
                    f"{file_path.name} 的目标月份为 {workbook_summary['target_month']}，"
                    f'不是本轮自动 report_date {anchor_date.isoformat()} 所在月份 {expected_target_month}；'
                    '本轮不处理历史或未来月份快照'
                )
        target_month_start = datetime.strptime(workbook_summary['target_month'], '%Y-%m').date().replace(day=1)
        return ImportInspection(
            file_path=file_path.resolve(),
            file_md5=file_md5,
            target_month=workbook_summary['target_month'],
            target_month_start=target_month_start,
            target_version=workbook_summary['target_version'],
            sheet_name=sheet_name,
            source_row_count=int(workbook_summary['source_row_count']),
            available_target_months=list(workbook_summary.get('available_target_months', [workbook_summary['target_month']])),
            file_modified_at=datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
        )
    except ScheduledImportSkip:
        raise
    except Exception as exc:
        raise ScheduledImportError(
            str(exc),
            retryable=_is_retryable_message(str(exc), False),
        ) from exc


def _ensure_log_table_exists() -> None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name AS table_name_alias
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = %s
                """,
                                (TARGET_LOG_TABLE_NAME,),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
                ddl_hint = TARGET_LOG_TABLE_SQL_PATH.relative_to(PROJECT_DIR).as_posix()
                raise RuntimeError(f'未找到日志表 {TARGET_LOG_TABLE_NAME}，请先执行 {ddl_hint}')


def _fetch_existing_success_log(inspection: ImportInspection) -> dict | None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id AS id_alias,
                    created_at AS created_at_alias,
                    finished_at AS finished_at_alias,
                    records_inserted AS records_inserted_alias,
                    message AS message_alias
                                FROM {TARGET_LOG_TABLE_NAME}
                WHERE status = 'SUCCESS'
                  AND file_md5 = %s
                  AND target_month = %s
                  AND target_version = %s
                ORDER BY COALESCE(finished_at, created_at) DESC, id DESC
                LIMIT 1
                """,
                (inspection.file_md5, inspection.target_month_start, inspection.target_version),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return {
        'id': row['id_alias'],
        'created_at': row['created_at_alias'],
        'finished_at': row['finished_at_alias'],
        'records_inserted': row['records_inserted_alias'],
        'message': row['message_alias'],
    }


def _build_import_command(
    inspection: ImportInspection,
    apply: bool,
    sync_store_report_attr: bool,
    created_by: str,
) -> tuple[list[str], Path]:
    output_path = Path(tempfile.gettempdir()) / (
        f'scheduled_store_daily_report_{os.getpid()}_{int(time.time() * 1000)}.json'
    )
    command = [
        sys.executable,
        str(TARGET_IMPORT_SCRIPT_PATH),
        '--file-path',
        str(inspection.file_path),
        '--sheet-name',
        inspection.sheet_name,
        '--target-month',
        inspection.target_month,
        '--preview-limit',
        '0',
        '--output-json',
        str(output_path),
    ]
    if sync_store_report_attr:
        command.append('--sync-store-report-attr')
    if apply:
        command.extend(['--apply', '--created-by', created_by])
    return command, output_path


def _ensure_owner_log_table_exists() -> None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name AS table_name_alias
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = %s
                """,
                (OWNER_LOG_TABLE_NAME,),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        ddl_hint = OWNER_DDL_FILE_PATH.relative_to(PROJECT_DIR).as_posix()
        raise RuntimeError(f'未找到日志表 {OWNER_LOG_TABLE_NAME}，请先执行 {ddl_hint}')


def _inspect_owner_file(
    file_path_arg: str | None,
    snapshot_date: date,
    sheet_name: str | None,
) -> OwnerImportInspection:
    try:
        file_path = _resolve_owner_input_file(file_path_arg)
        return OwnerImportInspection(
            file_path=file_path.resolve(),
            file_md5=_compute_owner_file_md5(file_path),
            snapshot_date=snapshot_date,
            sheet_name=sheet_name,
        )
    except Exception as exc:
        raise ScheduledImportError(
            str(exc),
            retryable=_is_retryable_message(str(exc), False),
        ) from exc


def _fetch_existing_success_owner_log(inspection: OwnerImportInspection) -> dict | None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id AS id_alias,
                    created_at AS created_at_alias,
                    finished_at AS finished_at_alias,
                    snapshot_rows_inserted AS snapshot_rows_inserted_alias,
                    history_rows_opened AS history_rows_opened_alias,
                    history_rows_closed AS history_rows_closed_alias,
                    message AS message_alias
                FROM {OWNER_LOG_TABLE_NAME}
                WHERE status = 'SUCCESS'
                  AND file_md5 = %s
                  AND snapshot_date = %s
                ORDER BY COALESCE(finished_at, created_at) DESC, id DESC
                LIMIT 1
                """,
                (inspection.file_md5, inspection.snapshot_date),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return {
        'id': row['id_alias'],
        'created_at': row['created_at_alias'],
        'finished_at': row['finished_at_alias'],
        'snapshot_rows_inserted': row['snapshot_rows_inserted_alias'],
        'history_rows_opened': row['history_rows_opened_alias'],
        'history_rows_closed': row['history_rows_closed_alias'],
        'message': row['message_alias'],
    }


def _build_owner_import_command(
    inspection: OwnerImportInspection,
    apply: bool,
    created_by: str,
) -> tuple[list[str], Path]:
    output_path = Path(tempfile.gettempdir()) / (
        f'scheduled_store_daily_owner_import_{os.getpid()}_{int(time.time() * 1000)}.json'
    )
    command = [
        sys.executable,
        str(OWNER_IMPORT_SCRIPT_PATH),
        '--file-path',
        str(inspection.file_path),
        '--snapshot-date',
        inspection.snapshot_date.isoformat(),
        '--preview-limit',
        '0',
        '--output-json',
        str(output_path),
    ]
    if inspection.sheet_name:
        command.extend(['--sheet-name', inspection.sheet_name])
    if apply:
        command.extend(['--apply', '--created-by', created_by])
    return command, output_path


def _ensure_duty_free_log_table_exists() -> None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT table_name AS table_name_alias
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                  AND table_name = %s
                """,
                (DUTY_FREE_LOG_TABLE_NAME,),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        ddl_hint = DUTY_FREE_LOG_TABLE_SQL_PATH.relative_to(PROJECT_DIR).as_posix()
        raise RuntimeError(f'未找到日志表 {DUTY_FREE_LOG_TABLE_NAME}，请先执行 {ddl_hint}')


def _inspect_duty_free_file(
    file_path_arg: str | None,
    sheet_name: str,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
    run_date: date | None = None,
) -> DutyFreeImportInspection:
    try:
        file_path = _resolve_duty_free_input_file(file_path_arg)
        file_md5 = _compute_duty_free_file_md5(file_path)
        _, workbook_summary = _parse_duty_free_workbook(file_path, sheet_name)
        if file_path_arg is None:
            anchor_date = _resolve_auto_report_anchor_date(run_date or date.today(), report_date_mode)
            expected_target_month = _expected_auto_target_month(run_date, report_date_mode)
            if workbook_summary['target_month'] != expected_target_month:
                raise ScheduledImportSkip(
                    '最新免税月累计快照 '
                    f"{file_path.name} 的 目标月份为 {workbook_summary['target_month']}，"
                    f'不是本轮自动 report_date {anchor_date.isoformat()} 所在月份 {expected_target_month}；'
                    '本轮不处理历史或未来月份快照'
                )
        return DutyFreeImportInspection(
            file_path=file_path.resolve(),
            file_md5=file_md5,
            target_month=workbook_summary['target_month'],
            target_month_start=date.fromisoformat(workbook_summary['target_month_start']),
            data_version=workbook_summary['data_version'],
            sheet_name=sheet_name,
            source_row_count=int(workbook_summary['source_row_count']),
            file_modified_at=datetime.fromtimestamp(file_path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
        )
    except ScheduledImportSkip:
        raise
    except Exception as exc:
        raise ScheduledImportError(
            str(exc),
            retryable=_is_retryable_message(str(exc), False),
        ) from exc


def _fetch_existing_success_duty_free_log(inspection: DutyFreeImportInspection) -> dict | None:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    id AS id_alias,
                    created_at AS created_at_alias,
                    finished_at AS finished_at_alias,
                    records_inserted AS records_inserted_alias,
                    changed_store_count AS changed_store_count_alias,
                    new_store_count AS new_store_count_alias,
                    exited_store_count AS exited_store_count_alias,
                    message AS message_alias
                FROM {DUTY_FREE_LOG_TABLE_NAME}
                WHERE status = 'SUCCESS'
                  AND file_md5 = %s
                                    AND target_month = %s
                  AND data_version = %s
                ORDER BY COALESCE(finished_at, created_at) DESC, id DESC
                LIMIT 1
                """,
                                (inspection.file_md5, inspection.target_month_start, inspection.data_version),
            )
            row = cursor.fetchone()
    finally:
        conn.close()

    if row is None:
        return None

    return {
        'id': row['id_alias'],
        'created_at': row['created_at_alias'],
        'finished_at': row['finished_at_alias'],
        'records_inserted': row['records_inserted_alias'],
        'changed_store_count': row['changed_store_count_alias'],
        'new_store_count': row['new_store_count_alias'],
        'exited_store_count': row['exited_store_count_alias'],
        'message': row['message_alias'],
    }


def _build_duty_free_import_command(
    inspection: DutyFreeImportInspection,
    apply: bool,
    created_by: str,
) -> tuple[list[str], Path]:
    output_path = Path(tempfile.gettempdir()) / (
        f'scheduled_store_daily_duty_free_import_{os.getpid()}_{int(time.time() * 1000)}.json'
    )
    command = [
        sys.executable,
        str(DUTY_FREE_IMPORT_SCRIPT_PATH),
        '--file-path',
        str(inspection.file_path),
        '--sheet-name',
        inspection.sheet_name,
        '--preview-limit',
        '0',
        '--output-json',
        str(output_path),
    ]
    if apply:
        command.extend(['--apply', '--created-by', created_by])
    return command, output_path


def _run_duty_free_import_tool(
    inspection: DutyFreeImportInspection,
    apply: bool,
    created_by: str,
) -> dict:
    command, output_path = _build_duty_free_import_command(
        inspection,
        apply=apply,
        created_by=created_by,
    )
    logger.info('执行免税月累计导入命令: %s', ' '.join(command))

    summary = None
    try:
        child_env = os.environ.copy()
        child_env['PYTHONUTF8'] = '1'
        child_env['PYTHONIOENCODING'] = 'utf-8'
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=child_env,
            check=False,
        )
        summary = _load_json_output(output_path)
        if completed.returncode != 0:
            validation_failed = bool(summary) and summary.get('validation_status') == 'FAILED'
            message = _extract_failure_message(summary, completed.stdout, completed.stderr)
            raise ScheduledImportError(
                message,
                retryable=_is_retryable_message(message, validation_failed),
                inspection=inspection,
            )
        if summary is None:
            raise ScheduledImportError('免税月累计导入工具执行成功，但未生成摘要 JSON', retryable=False, inspection=inspection)
        return summary
    finally:
        if output_path.exists():
            output_path.unlink(missing_ok=True)


def _format_duty_free_import_detail(summary: dict) -> str:
    return ', '.join(
        [
            f"target_month={summary['target_month']}",
            f"version={summary['data_version']}",
            f"matched={summary['matched_store_count']}",
            f"changed={summary['changed_store_count']}",
            f"new={summary['new_store_count']}",
            f"exited={summary['exited_store_count']}",
            f"inserted={summary['records_inserted']}",
        ]
    )


def _load_json_output(output_path: Path) -> dict | None:
    if not output_path.exists():
        return None
    return json.loads(output_path.read_text(encoding='utf-8'))


def _extract_failure_message(summary: dict | None, stdout_text: str, stderr_text: str) -> str:
    if summary and summary.get('error_message'):
        return str(summary['error_message'])

    for raw_text in (stderr_text, stdout_text):
        normalized = raw_text.strip()
        if not normalized:
            continue
        lines = [line.strip() for line in normalized.splitlines() if line.strip()]
        if lines:
            return lines[-1]
        return normalized

    return '导入工具执行失败，但未返回明确错误信息'


def _get_import_warning_messages(summary: dict | None) -> list[str]:
    if not summary:
        return []

    raw_messages = summary.get('warning_messages')
    if not isinstance(raw_messages, list):
        return []
    return [str(message).strip() for message in raw_messages if str(message).strip()]


def _has_import_warnings(summary: dict | None) -> bool:
    if not summary:
        return False
    return bool(_get_import_warning_messages(summary)) or summary.get('validation_status') == 'WARNING'


def _is_retryable_message(message: str, validation_failed: bool) -> bool:
    if validation_failed:
        return False

    lowered = message.lower()
    if any(keyword.lower() in lowered for keyword in ETL_NON_RETRYABLE_ERROR_KEYWORDS):
        return False
    if any(keyword in message for keyword in NON_RETRYABLE_MESSAGE_KEYWORDS):
        return False
    if any(keyword.lower() in lowered for keyword in ETL_RETRYABLE_ERROR_KEYWORDS):
        return True
    return True


def _run_import_tool(
    inspection: ImportInspection,
    apply: bool,
    sync_store_report_attr: bool,
    created_by: str,
) -> dict:
    command, output_path = _build_import_command(
        inspection,
        apply=apply,
        sync_store_report_attr=sync_store_report_attr,
        created_by=created_by,
    )
    logger.info('执行门店日报目标导入命令: %s', ' '.join(command))

    summary = None
    try:
        child_env = os.environ.copy()
        child_env['PYTHONUTF8'] = '1'
        child_env['PYTHONIOENCODING'] = 'utf-8'
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=child_env,
            check=False,
        )
        summary = _load_json_output(output_path)
        if completed.returncode != 0:
            validation_failed = bool(summary) and summary.get('validation_status') == 'FAILED'
            message = _extract_failure_message(summary, completed.stdout, completed.stderr)
            raise ScheduledImportError(
                message,
                retryable=_is_retryable_message(message, validation_failed),
                inspection=inspection,
            )
        if summary is None:
            raise ScheduledImportError('导入工具执行成功，但未生成摘要 JSON', retryable=False, inspection=inspection)
        return summary
    finally:
        if output_path.exists():
            output_path.unlink(missing_ok=True)


def _format_import_detail(summary: dict) -> str:
    detail_parts = [
        f"month={summary['target_month']}",
        f"version={summary['target_version']}",
        f"stores={summary['matched_store_count']}",
        f"rows={summary['expanded_row_count']}",
    ]
    warning_messages = _get_import_warning_messages(summary)
    if warning_messages:
        detail_parts.append(f'warnings={len(warning_messages)}')
    if 'records_inserted' in summary:
        detail_parts.append(f"inserted={summary['records_inserted']}")
    if summary.get('sync_store_report_attr'):
        detail_parts.append(f"store_attr_inserted={summary.get('store_attr_records_inserted', 0)}")
    if summary.get('sync_assessment'):
        detail_parts.append(f"subject_targets_inserted={summary.get('subject_target_records_inserted', 0)}")
        detail_parts.append(
            f"assessment_assignments_inserted={summary.get('assessment_assignment_records_inserted', 0)}"
        )
    return ', '.join(detail_parts)


def _run_owner_import_tool(
    inspection: OwnerImportInspection,
    apply: bool,
    created_by: str,
) -> dict:
    command, output_path = _build_owner_import_command(
        inspection,
        apply=apply,
        created_by=created_by,
    )
    logger.info('执行负责人快照导入命令: %s', ' '.join(command))

    summary = None
    try:
        child_env = os.environ.copy()
        child_env['PYTHONUTF8'] = '1'
        child_env['PYTHONIOENCODING'] = 'utf-8'
        completed = subprocess.run(
            command,
            cwd=str(PROJECT_DIR),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            env=child_env,
            check=False,
        )
        summary = _load_json_output(output_path)
        if completed.returncode != 0:
            validation_failed = bool(summary) and summary.get('validation_status') == 'FAILED'
            message = _extract_failure_message(summary, completed.stdout, completed.stderr)
            raise ScheduledImportError(
                message,
                retryable=_is_retryable_message(message, validation_failed),
                inspection=inspection,
            )
        if summary is None:
            raise ScheduledImportError('负责人导入工具执行成功，但未生成摘要 JSON', retryable=False, inspection=inspection)
        return summary
    finally:
        if output_path.exists():
            output_path.unlink(missing_ok=True)


def _format_owner_import_detail(summary: dict) -> str:
    history_diff_counts = summary.get('history_diff_counts') or {}
    detail_parts = [
        f"snapshot_date={summary['snapshot_date']}",
        f"matched={summary['matched_entity_count']}",
        f"blank_owner={summary['blank_owner_count']}",
        f"changed={history_diff_counts.get('changed', 0)}",
        f"new={history_diff_counts.get('new', 0)}",
        f"exited={history_diff_counts.get('exited', 0)}",
    ]
    earliest_history_effective_start_date = summary.get('earliest_history_effective_start_date')
    if earliest_history_effective_start_date:
        detail_parts.append(f"earliest_affected={earliest_history_effective_start_date}")
    if 'snapshot_rows_inserted' in summary:
        detail_parts.append(f"snapshot_inserted={summary.get('snapshot_rows_inserted', 0)}")
    if 'history_rows_opened' in summary:
        detail_parts.append(f"history_opened={summary.get('history_rows_opened', 0)}")
    if 'history_rows_closed' in summary:
        detail_parts.append(f"history_closed={summary.get('history_rows_closed', 0)}")
    return ', '.join(detail_parts)


def _run_owner_schedule_once(
    args: argparse.Namespace,
    default_snapshot_date: date | None = None,
) -> OwnerImportRunResult | None:
    if not args.run_owner_import:
        logger.info('命中 --no-run-owner-import，本次跳过负责人快照导入')
        return None

    snapshot_date = args.owner_snapshot_date or default_snapshot_date or date.today()
    inspection = _inspect_owner_file(args.owner_file_path, snapshot_date, args.owner_sheet_name)
    _ensure_owner_log_table_exists()
    logger.info(
        '已解析负责人快照文件: file=%s, snapshot_date=%s, md5=%s, sheet=%s',
        inspection.file_path.name,
        inspection.snapshot_date.isoformat(),
        inspection.file_md5,
        inspection.sheet_name or 'AUTO',
    )

    if args.conn_test:
        summary = _run_owner_import_tool(
            inspection,
            apply=False,
            created_by=args.created_by,
        )
        logger.info('负责人连接测试通过: %s', _format_owner_import_detail(summary))
        return OwnerImportRunResult(
            outcome='CONN_TEST',
            inspection=inspection,
            summary=summary,
            existing_log=None,
        )

    existing_log = _fetch_existing_success_owner_log(inspection)
    if existing_log is not None:
        logger.info(
            '检测到相同 MD5 + snapshot_date 的负责人成功导入记录，跳过本次 apply: id=%s, finished_at=%s, history_opened=%s, history_closed=%s',
            existing_log['id'],
            _format_datetime_value(existing_log.get('finished_at') or existing_log.get('created_at')),
            existing_log.get('history_rows_opened'),
            existing_log.get('history_rows_closed'),
        )
        return OwnerImportRunResult(
            outcome='SKIPPED',
            inspection=inspection,
            summary=None,
            existing_log=existing_log,
        )

    summary = _run_owner_import_tool(
        inspection,
        apply=True,
        created_by=args.created_by,
    )
    logger.info('负责人导入完成: %s', _format_owner_import_detail(summary))
    return OwnerImportRunResult(
        outcome='IMPORTED',
        inspection=inspection,
        summary=summary,
        existing_log=None,
    )


def _run_duty_free_schedule_once(args: argparse.Namespace) -> DutyFreeImportRunResult | None:
    if not args.run_duty_free_import:
        logger.info('命中 --no-run-duty-free-import，本次跳过免税月累计导入')
        return None

    inspection = _inspect_duty_free_file(
        args.duty_free_file_path,
        args.duty_free_sheet_name,
        report_date_mode=args.auto_report_date_mode,
    )
    _ensure_duty_free_log_table_exists()
    logger.info(
        '已解析免税月累计文件: file=%s, target_month=%s, version=%s, md5=%s, rows=%s, modified_at=%s',
        inspection.file_path.name,
        inspection.target_month,
        inspection.data_version,
        inspection.file_md5,
        inspection.source_row_count,
        inspection.file_modified_at,
    )

    if args.conn_test:
        summary = _run_duty_free_import_tool(
            inspection,
            apply=False,
            created_by=args.created_by,
        )
        logger.info('免税月累计连接测试通过: %s', _format_duty_free_import_detail(summary))
        return DutyFreeImportRunResult(
            outcome='CONN_TEST',
            inspection=inspection,
            summary=summary,
            existing_log=None,
        )

    existing_log = _fetch_existing_success_duty_free_log(inspection)
    if existing_log is not None:
        logger.info(
            '检测到相同 MD5 + target_month + data_version 的免税月累计成功导入记录，跳过本次 apply: id=%s, finished_at=%s, changed=%s, new=%s, exited=%s',
            existing_log['id'],
            _format_datetime_value(existing_log.get('finished_at') or existing_log.get('created_at')),
            existing_log.get('changed_store_count'),
            existing_log.get('new_store_count'),
            existing_log.get('exited_store_count'),
        )
        return DutyFreeImportRunResult(
            outcome='SKIPPED',
            inspection=inspection,
            summary=None,
            existing_log=existing_log,
        )

    summary = _run_duty_free_import_tool(
        inspection,
        apply=True,
        created_by=args.created_by,
    )
    logger.info('免税月累计导入完成: %s', _format_duty_free_import_detail(summary))
    return DutyFreeImportRunResult(
        outcome='IMPORTED',
        inspection=inspection,
        summary=summary,
        existing_log=None,
    )


def _build_disabled_duty_free_affected_date_summary(schedule_run_date: date | None = None) -> dict:
    run_date = schedule_run_date or date.today()
    return {
        'outcome': 'DISABLED',
        'target_month': None,
        'schedule_run_date': run_date.isoformat(),
        'upper_bound': None,
        'duty_free_branch': {
            'enabled': False,
            'rule': 'disabled_by_cli',
            'target_month': None,
            'changed_store_count': 0,
            'new_store_count': 0,
            'exited_store_count': 0,
            'date_count': 0,
        },
        'affected_dates': [],
        'affected_date_count': 0,
        'note': '命中 --no-run-duty-free-import，本次不执行免税月累计导入，也不新增免税受影响日期集合',
    }


def _build_duty_free_affected_date_summary(
    duty_free_result: DutyFreeImportRunResult | None,
    schedule_run_date: date | None = None,
    report_date_mode: str = AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
) -> dict:
    run_date = schedule_run_date or date.today()
    if duty_free_result is None:
        return _build_disabled_duty_free_affected_date_summary(run_date)

    base_summary = {
        'outcome': duty_free_result.outcome,
        'target_month': duty_free_result.inspection.target_month,
        'schedule_run_date': run_date.isoformat(),
        'upper_bound': None,
        'duty_free_branch': {
            'enabled': False,
            'rule': 'duty_free_target_month_overlay',
            'target_month': duty_free_result.inspection.target_month,
            'changed_store_count': 0,
            'new_store_count': 0,
            'exited_store_count': 0,
            'date_count': 0,
        },
        'affected_dates': [],
        'affected_date_count': 0,
    }
    if duty_free_result.outcome == 'CONN_TEST':
        base_summary['note'] = '命中 conn-test 跳过规则，不产生新的免税受影响日期集合'
        return base_summary
    if duty_free_result.outcome == 'SKIPPED':
        upper_bound = _resolve_auto_report_date_upper_bound(
            duty_free_result.inspection.target_month_start,
            run_date,
            report_date_mode=report_date_mode,
        )
        base_summary['upper_bound'] = upper_bound.isoformat()
        base_summary['note'] = '命中 file_md5 + target_month + data_version 已成功导入跳过规则，不产生新的免税受影响日期集合'
        return base_summary
    if duty_free_result.outcome != 'IMPORTED' or duty_free_result.summary is None:
        raise ScheduledImportError(
            f'未识别的免税专题调度结果: {duty_free_result.outcome}',
            retryable=False,
            inspection=duty_free_result.inspection,
        )

    upper_bound = _resolve_auto_report_date_upper_bound(
        duty_free_result.inspection.target_month_start,
        run_date,
        report_date_mode=report_date_mode,
    )
    base_summary['upper_bound'] = upper_bound.isoformat()
    changed_store_count = int(duty_free_result.summary.get('changed_store_count', 0) or 0)
    new_store_count = int(duty_free_result.summary.get('new_store_count', 0) or 0)
    exited_store_count = int(duty_free_result.summary.get('exited_store_count', 0) or 0)
    has_changes = bool(duty_free_result.summary.get('has_changes'))
    affected_dates = []
    if has_changes and upper_bound >= duty_free_result.inspection.target_month_start:
        affected_dates = [upper_bound.isoformat()]
    base_summary['duty_free_branch'] = {
        'enabled': has_changes,
        'rule': 'duty_free_target_month_latest_snapshot',
        'target_month': duty_free_result.inspection.target_month,
        'changed_store_count': changed_store_count,
        'new_store_count': new_store_count,
        'exited_store_count': exited_store_count,
        'date_count': len(affected_dates),
    }
    base_summary['affected_dates'] = affected_dates
    base_summary['affected_date_count'] = len(affected_dates)
    if has_changes:
        if affected_dates:
            base_summary['note'] = '免税月累计导入完成，将按专题调度统一上界重跑当天 ADS 以刷新月累计与月达成'
        else:
            base_summary['note'] = '免税月累计导入完成，但专题调度统一上界早于目标月份起点，本次不新增免税受影响日期集合'
    else:
        base_summary['note'] = '免税月累计导入完成，但与当前库内快照一致，不新增免税受影响日期集合'
    return base_summary


def _describe_branch_action(label: str, outcome: str) -> str:
    outcome_map = {
        'IMPORTED': f'{label}已执行',
        'SKIPPED': f'{label}命中幂等跳过',
        'DISABLED': f'{label}按 CLI 关闭',
        'CONN_TEST': f'{label}dry-run 检查通过',
    }
    return outcome_map.get(outcome, f'{label}状态={outcome}')


def _build_success_action_line(
    target_outcome: str,
    owner_result: OwnerImportRunResult | None,
    duty_free_result: DutyFreeImportRunResult | None,
) -> str:
    owner_outcome = owner_result.outcome if owner_result is not None else 'DISABLED'
    duty_free_outcome = duty_free_result.outcome if duty_free_result is not None else 'DISABLED'
    return '动作：' + '，'.join(
        [
            _describe_branch_action('NAS目标导入', target_outcome),
            _describe_branch_action('负责人快照导入', owner_outcome),
            _describe_branch_action('免税月累计导入', duty_free_outcome),
        ]
    )


def _build_warning_action_line(target_outcome: str) -> str:
    if target_outcome == 'CONN_TEST':
        return '动作：dry-run 检测到未命中 dim_store 的门店，正式调度时将跳过这些门店'
    return '动作：已跳过未命中 dim_store 的门店，其余门店链路继续执行'


def _append_owner_alert_lines(lines: list[str], owner_result: OwnerImportRunResult | None) -> None:
    if owner_result is None:
        lines.append('负责人快照：DISABLED')
        return

    lines.append(f'负责人快照：{owner_result.outcome}')
    lines.append(f'负责人文件：{owner_result.inspection.file_path.name}')
    lines.append(f'负责人快照日：{owner_result.inspection.snapshot_date.isoformat()}')
    lines.append(f'负责人MD5：{owner_result.inspection.file_md5}')
    if owner_result.summary is not None:
        history_diff_counts = owner_result.summary.get('history_diff_counts') or {}
        lines.append(f'负责人匹配实体：{owner_result.summary.get("matched_entity_count", 0)}')
        lines.append(
            '负责人历史变更：'
            f"changed={history_diff_counts.get('changed', 0)}, "
            f"new={history_diff_counts.get('new', 0)}, "
            f"exited={history_diff_counts.get('exited', 0)}"
        )
    elif owner_result.existing_log is not None:
        lines.append(
            '负责人既有导入：'
            f"id={owner_result.existing_log['id']}, "
            f"finished_at={_format_datetime_value(owner_result.existing_log.get('finished_at') or owner_result.existing_log.get('created_at'))}"
        )


def _append_duty_free_alert_lines(lines: list[str], duty_free_result: DutyFreeImportRunResult | None) -> None:
    if duty_free_result is None:
        lines.append('免税月累计：DISABLED')
        return

    lines.append(f'免税月累计：{duty_free_result.outcome}')
    lines.append(f'免税文件：{duty_free_result.inspection.file_path.name}')
    lines.append(f'免税目标月份：{duty_free_result.inspection.target_month}')
    lines.append(f'免税MD5：{duty_free_result.inspection.file_md5}')
    if duty_free_result.summary is not None:
        lines.append(f'免税匹配门店：{duty_free_result.summary.get("matched_store_count", 0)}')
        lines.append(
            '免税快照变更：'
            f"changed={duty_free_result.summary.get('changed_store_count', 0)}, "
            f"new={duty_free_result.summary.get('new_store_count', 0)}, "
            f"exited={duty_free_result.summary.get('exited_store_count', 0)}"
        )
    elif duty_free_result.existing_log is not None:
        lines.append(
            '免税既有导入：'
            f"id={duty_free_result.existing_log['id']}, "
            f"finished_at={_format_datetime_value(duty_free_result.existing_log.get('finished_at') or duty_free_result.existing_log.get('created_at'))}"
        )


def _compose_success_alert(
    target_outcome: str,
    summary: dict | None,
    inspection: ImportInspection,
    owner_result: OwnerImportRunResult | None,
    duty_free_result: DutyFreeImportRunResult | None,
    affected_date_summary: dict,
    ads_backfill_summary: dict,
    attempt: int,
    max_retries: int,
) -> str:
    lines = [
        '何方珠宝门店日报专题调度 SUCCESS',
        _build_success_action_line(target_outcome, owner_result, duty_free_result),
        f'文件：{inspection.file_path.name}',
        f'月份/版本：{inspection.target_month} / {inspection.target_version}',
        f'MD5：{inspection.file_md5}',
        f'受影响日期数：{affected_date_summary["affected_date_count"]}',
        f'ADS批量重跑：{ads_backfill_summary["completed_date_count"]}/{ads_backfill_summary["requested_date_count"]}（{AFFECTED_ADS_SHORT_LABEL}）',
    ]
    if summary is not None:
        lines.append(f'门店数：{summary["matched_store_count"]}')
        lines.append(f'目标写入：{summary.get("records_inserted", 0)}')
        if summary.get('sync_store_report_attr'):
            lines.append(f'门店属性写入：{summary.get("store_attr_records_inserted", 0)}')
        if summary.get('sync_assessment'):
            lines.append(f'主体目标写入：{summary.get("subject_target_records_inserted", 0)}')
            lines.append(f'考核归属写入：{summary.get("assessment_assignment_records_inserted", 0)}')
    _append_owner_alert_lines(lines, owner_result)
    _append_duty_free_alert_lines(lines, duty_free_result)
    lines.append(f'负责人受影响日期数：{affected_date_summary["owner_branch"]["date_count"]}')
    lines.append(f'免税受影响日期数：{affected_date_summary.get("duty_free_branch", {}).get("date_count", 0)}')
    if affected_date_summary['affected_date_count'] > 0:
        affected_dates = affected_date_summary['affected_dates']
        lines.append(f'受影响日期范围：{affected_dates[0]} ~ {affected_dates[-1]}')
    lines.append(f'ADS重跑说明：{ads_backfill_summary["note"]}')
    lines.append(f'尝试：{attempt}/{max_retries}')
    return '\n'.join(lines)


def _compose_warning_alert(
    target_outcome: str,
    summary: dict,
    inspection: ImportInspection,
    owner_result: OwnerImportRunResult | None,
    duty_free_result: DutyFreeImportRunResult | None,
    affected_date_summary: dict,
    ads_backfill_summary: dict,
    attempt: int,
    max_retries: int,
) -> str:
    warning_messages = _get_import_warning_messages(summary)
    lines = [
        '何方珠宝门店日报专题调度 WARNING',
        _build_warning_action_line(target_outcome),
        f'文件：{inspection.file_path.name}',
        f'月份/版本：{inspection.target_month} / {inspection.target_version}',
        f'MD5：{inspection.file_md5}',
        f'告警数：{len(warning_messages)}',
        f'受影响日期数：{affected_date_summary["affected_date_count"]}',
        f'ADS批量重跑：{ads_backfill_summary["completed_date_count"]}/{ads_backfill_summary["requested_date_count"]}（{AFFECTED_ADS_SHORT_LABEL}）',
        f'门店数：{summary["matched_store_count"]}',
        f'目标写入：{summary.get("records_inserted", 0)}',
    ]
    if summary.get('sync_store_report_attr'):
        lines.append(f'门店属性写入：{summary.get("store_attr_records_inserted", 0)}')
    if summary.get('sync_assessment'):
        lines.append(f'主体目标写入：{summary.get("subject_target_records_inserted", 0)}')
        lines.append(f'考核归属写入：{summary.get("assessment_assignment_records_inserted", 0)}')
    _append_owner_alert_lines(lines, owner_result)
    _append_duty_free_alert_lines(lines, duty_free_result)
    lines.append(f'负责人受影响日期数：{affected_date_summary["owner_branch"]["date_count"]}')
    lines.append(f'免税受影响日期数：{affected_date_summary.get("duty_free_branch", {}).get("date_count", 0)}')
    if affected_date_summary['affected_date_count'] > 0:
        affected_dates = affected_date_summary['affected_dates']
        lines.append(f'受影响日期范围：{affected_dates[0]} ~ {affected_dates[-1]}')
    lines.append(f'ADS重跑说明：{ads_backfill_summary["note"]}')
    for warning_message in warning_messages:
        lines.append(f'告警：{warning_message}')
    lines.append(f'尝试：{attempt}/{max_retries}')
    return '\n'.join(lines)


def _compose_failure_alert(
    inspection: ImportInspection | OwnerImportInspection | DutyFreeImportInspection | None,
    error_message: str,
    attempt: int,
    max_retries: int,
    exhausted: bool,
    stage: str = 'import',
    ads_context: AdsBackfillContext | None = None,
    target_completed: bool = False,
) -> str:
    if stage == 'ads_backfill':
        if ads_context is not None and ads_context.owner_result is not None and ads_context.owner_result.outcome == 'IMPORTED':
            action_line = f'动作：NAS 目标导入与负责人快照导入已完成，但{AFFECTED_ADS_FAILURE_LABEL}批量重跑未完成'
        else:
            action_line = f'动作：NAS 目标导入已完成，但{AFFECTED_ADS_FAILURE_LABEL}批量重跑未完成'
    elif stage == 'explicit_ads_backfill':
        action_line = f'动作：{AFFECTED_ADS_FAILURE_LABEL}按日期列表批量重跑未完成'
    elif stage == 'owner_import':
        action_line = '动作：NAS 目标导入已完成，但负责人快照导入未完成' if target_completed else '动作：负责人快照导入未完成'
    elif stage == 'duty_free_import':
        action_line = '动作：NAS 目标导入已完成，但免税月累计导入未完成' if target_completed else '动作：免税月累计导入未完成'
    else:
        action_line = '动作：NAS 目标导入未完成'

    lines = ['何方珠宝门店日报专题调度 FAILED', action_line]
    if inspection is not None:
        lines.append(f'文件：{inspection.file_path.name}')
        if isinstance(inspection, ImportInspection):
            lines.append(f'月份/版本：{inspection.target_month} / {inspection.target_version}')
        if isinstance(inspection, OwnerImportInspection):
            lines.append(f'负责人快照日：{inspection.snapshot_date.isoformat()}')
        if isinstance(inspection, DutyFreeImportInspection):
            lines.append(f'免税目标月份/版本：{inspection.target_month} / {inspection.data_version}')
        lines.append(f'MD5：{inspection.file_md5}')
    if ads_context is not None:
        lines.extend([
            f'ADS版本：{ads_context.data_version}',
            f'失败日期数：{len(ads_context.report_dates)}',
            f'失败日期：{_format_report_dates(ads_context.report_dates)}',
        ])
    lines.append(f'尝试：{attempt}/{max_retries}')
    lines.append(f'状态：{"已耗尽重试" if exhausted else "命中不可重试错误"}')
    lines.append(f'原因：{error_message}')
    return '\n'.join(lines)


def _send_schedule_alert_if_enabled(content: str) -> None:
    if not should_suppress_child_wechat_alert():
        send_wechat_alert(WECHAT_WEBHOOK, content)


def _build_topic_chain_summary_payload(
    status: str,
    started_at: datetime,
    ended_at: datetime,
    summary_lines: list[str],
    detail_lines: list[str] | None = None,
    issue_lines: list[str] | None = None,
    headline: str | None = None,
) -> dict:
    headline_map = {
        'SUCCESS': '门店销售专题调度完成',
        'WARNING': '门店销售专题调度告警',
        'SKIPPED': '门店销售专题调度跳过',
        'FAILED': '门店销售专题调度失败',
        'ERROR': '门店销售专题调度异常',
    }
    return {
        'chain_key': 'store_daily_topic',
        'chain_label': '门店销售专题',
        'status': status,
        'headline': headline or headline_map.get(status, '门店销售专题调度摘要'),
        'started_at': started_at,
        'ended_at': ended_at,
        'duration_seconds': int((ended_at - started_at).total_seconds()),
        'summary_lines': summary_lines,
        'detail_lines': detail_lines or [],
        'issue_lines': issue_lines or [],
    }


def _build_schedule_result_chain_summary_payload(
    result: ScheduleRunResult,
    attempt: int,
    max_retries: int,
    started_at: datetime,
    ended_at: datetime,
) -> dict:
    warning_messages = _get_import_warning_messages(result.summary)
    if result.outcome == 'CONN_TEST':
        status = 'WARNING' if warning_messages else 'SUCCESS'
    elif warning_messages:
        status = 'WARNING'
    elif _result_has_imported_branch(result) or result.ads_backfill_summary.get('mode') == 'EXECUTED':
        status = 'SUCCESS'
    else:
        status = 'SKIPPED'

    owner_outcome = result.owner_result.outcome if result.owner_result is not None else 'DISABLED'
    duty_free_outcome = result.duty_free_result.outcome if result.duty_free_result is not None else 'DISABLED'
    summary_lines = [
        _build_warning_action_line(result.outcome)
        if warning_messages
        else _build_success_action_line(result.outcome, result.owner_result, result.duty_free_result),
        f'月份/版本：{result.inspection.target_month} / {result.inspection.target_version}',
        f'受影响日期数：{result.affected_date_summary["affected_date_count"]}',
        (
            'ADS批量重跑：'
            f"{result.ads_backfill_summary['completed_date_count']}/{result.ads_backfill_summary['requested_date_count']}"
            f'（{AFFECTED_ADS_SHORT_LABEL}）'
        ),
        f'负责人快照：{owner_outcome}',
        f'免税月累计：{duty_free_outcome}',
        f'尝试：{attempt}/{max_retries}',
    ]
    if warning_messages:
        summary_lines.insert(3, f'门店映射告警：{len(warning_messages)}')
    detail_lines = [
        f'目标文件：{result.inspection.file_path.name}',
        f'MD5：{result.inspection.file_md5}',
        f'受影响日期说明：{result.affected_date_summary["note"]}',
        f'ADS重跑说明：{result.ads_backfill_summary["note"]}',
        f'免税受影响日期数：{result.affected_date_summary.get("duty_free_branch", {}).get("date_count", 0)}',
    ]
    if result.affected_date_summary['affected_date_count'] > 0:
        affected_dates = result.affected_date_summary['affected_dates']
        detail_lines.append(f'受影响日期范围：{affected_dates[0]} ~ {affected_dates[-1]}')
    if result.owner_result is not None:
        detail_lines.append(f'负责人文件：{result.owner_result.inspection.file_path.name}')
        detail_lines.append(f'负责人快照日：{result.owner_result.inspection.snapshot_date.isoformat()}')
    if result.duty_free_result is not None:
        detail_lines.append(f'免税文件：{result.duty_free_result.inspection.file_path.name}')
        detail_lines.append(f'免税目标月份：{result.duty_free_result.inspection.target_month}')
    if status == 'SKIPPED' and result.existing_log is not None:
        detail_lines.append(
            '沿用既有成功记录：'
            f"id={result.existing_log['id']}, finished_at="
            f"{_format_datetime_value(result.existing_log.get('finished_at') or result.existing_log.get('created_at'))}"
        )
    return _build_topic_chain_summary_payload(
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        summary_lines=summary_lines,
        detail_lines=detail_lines,
        issue_lines=[f'- {message}' for message in warning_messages],
    )


def _build_schedule_skip_chain_summary_payload(
    reason: str,
    attempt: int,
    max_retries: int,
    started_at: datetime,
    ended_at: datetime,
) -> dict:
    return _build_topic_chain_summary_payload(
        status='SKIPPED',
        started_at=started_at,
        ended_at=ended_at,
        summary_lines=[
            '本轮未执行门店销售专题写库链路',
            f'原因：{reason}',
            f'尝试：{attempt}/{max_retries}',
        ],
    )


def _build_explicit_ads_chain_summary_payload(
    ads_backfill_summary: dict,
    attempt: int,
    max_retries: int,
    started_at: datetime,
    ended_at: datetime,
) -> dict:
    return _build_topic_chain_summary_payload(
        status='SUCCESS',
        started_at=started_at,
        ended_at=ended_at,
        headline='门店销售专题显式批量重跑完成',
        summary_lines=[
            '动作：按显式日期列表执行 ADS 批量重跑',
            (
                'ADS批量重跑：'
                f"{ads_backfill_summary['completed_date_count']}/{ads_backfill_summary['requested_date_count']}"
                f'（{AFFECTED_ADS_SHORT_LABEL}）'
            ),
            f'数据版本：{ads_backfill_summary["data_version"]}',
            f'尝试：{attempt}/{max_retries}',
        ],
        detail_lines=[
            f"日期范围：{_format_report_dates(ads_backfill_summary['requested_report_dates'])}",
            f"重跑说明：{ads_backfill_summary['note']}",
        ],
    )


def _build_failure_chain_summary_payload(
    error_message: str,
    attempt: int,
    max_retries: int,
    started_at: datetime,
    ended_at: datetime,
    inspection: ImportInspection | OwnerImportInspection | DutyFreeImportInspection | None = None,
    stage: str = 'import',
    ads_context: AdsBackfillContext | None = None,
    exhausted: bool = False,
) -> dict:
    summary_lines = [
        f'阶段：{stage}',
        f'状态：{"已耗尽重试" if exhausted else "命中不可重试错误"}',
        f'尝试：{attempt}/{max_retries}',
    ]
    detail_lines: list[str] = []
    if inspection is not None:
        detail_lines.append(f'文件：{inspection.file_path.name}')
        detail_lines.append(f'MD5：{inspection.file_md5}')
        if isinstance(inspection, ImportInspection):
            detail_lines.append(f'月份/版本：{inspection.target_month} / {inspection.target_version}')
        elif isinstance(inspection, OwnerImportInspection):
            detail_lines.append(f'负责人快照日：{inspection.snapshot_date.isoformat()}')
        elif isinstance(inspection, DutyFreeImportInspection):
            detail_lines.append(f'免税目标月份/版本：{inspection.target_month} / {inspection.data_version}')
    if ads_context is not None:
        detail_lines.append(f'ADS版本：{ads_context.data_version}')
        detail_lines.append(f'失败日期：{_format_report_dates(ads_context.report_dates)}')
    return _build_topic_chain_summary_payload(
        status='FAILED',
        started_at=started_at,
        ended_at=ended_at,
        summary_lines=summary_lines,
        detail_lines=detail_lines,
        issue_lines=[f'- 原因：{error_message}'],
    )


def _validate_args(args: argparse.Namespace) -> None:
    if args.conn_test and args.rerun_report_date:
        raise ScheduledImportError(
            f'--conn-test 与 --rerun-report-date 不能同时使用；显式批量重跑模式默认会写{AFFECTED_ADS_DETAIL_LABEL}',
            retryable=False,
        )


def run_schedule_once(args: argparse.Namespace) -> ScheduleRunResult:
    effective_cutover_mode = resolve_cutover_mode(
        getattr(args, 'cutover_mode', None),
        rollback_to_legacy=getattr(args, 'rollback_to_legacy', False),
    )
    sales_freshness_source_mode = derive_store_daily_freshness_source(
        effective_cutover_mode,
        explicit_source=getattr(args, 'sales_freshness_source', None),
    )
    inspection = _inspect_target_file(
        args.file_path,
        args.target_month,
        args.sheet_name,
        args.sync_store_report_attr,
        report_date_mode=args.auto_report_date_mode,
    )
    default_owner_snapshot_date = _resolve_default_owner_snapshot_date(
        inspection.target_month_start,
        date.today(),
        args.auto_report_date_mode,
    )
    _ensure_log_table_exists()
    logger.info(
        '已解析门店日报目标文件: file=%s, month=%s, version=%s, md5=%s, rows=%s, modified_at=%s',
        inspection.file_path.name,
        inspection.target_month,
        inspection.target_version,
        inspection.file_md5,
        inspection.source_row_count,
        inspection.file_modified_at,
    )

    owner_result: OwnerImportRunResult | None = None
    duty_free_result: DutyFreeImportRunResult | None = None
    if args.conn_test:
        summary = _run_import_tool(
            inspection,
            apply=False,
            sync_store_report_attr=args.sync_store_report_attr,
            created_by=args.created_by,
        )
        owner_result = _run_owner_schedule_once(args, default_snapshot_date=default_owner_snapshot_date)
        duty_free_result = _run_duty_free_schedule_once(args)
        target_affected_date_summary = _build_affected_date_summary(
            'CONN_TEST',
            inspection,
            summary,
            report_date_mode=args.auto_report_date_mode,
        )
        owner_affected_date_summary = _build_owner_affected_date_summary(
            owner_result,
            inspection,
            report_date_mode=args.auto_report_date_mode,
        )
        duty_free_affected_date_summary = _build_duty_free_affected_date_summary(
            duty_free_result,
            report_date_mode=args.auto_report_date_mode,
        )
        affected_date_summary = _merge_affected_date_summaries(
            target_affected_date_summary,
            owner_affected_date_summary,
            duty_free_affected_date_summary,
        )
        ads_backfill_summary = _build_skipped_ads_backfill_summary(
            reason='conn_test',
            data_version=inspection.target_version,
            source='affected_dates',
        )
        logger.info('连接测试通过: %s', _format_import_detail(summary))
        if owner_result is not None and owner_result.summary is not None:
            logger.info('负责人连接测试通过: %s', _format_owner_import_detail(owner_result.summary))
        if duty_free_result is not None and duty_free_result.summary is not None:
            logger.info('免税月累计连接测试通过: %s', _format_duty_free_import_detail(duty_free_result.summary))
        logger.info('受影响日期判断结果: %s', _format_affected_date_detail(affected_date_summary))
        logger.info('ADS批量重跑结果: %s', _format_ads_backfill_detail(ads_backfill_summary))
        return ScheduleRunResult(
            outcome='CONN_TEST',
            inspection=inspection,
            summary=summary,
            existing_log=None,
            owner_result=owner_result,
            duty_free_result=duty_free_result,
            affected_date_summary=affected_date_summary,
            ads_backfill_summary=ads_backfill_summary,
        )

    target_outcome = 'SKIPPED'
    summary: dict | None = None
    existing_log = _fetch_existing_success_log(inspection)
    if existing_log is not None:
        logger.info(
            '检测到相同 MD5 成功导入记录，跳过本次 apply: id=%s, finished_at=%s, inserted=%s',
            existing_log['id'],
            _format_datetime_value(existing_log.get('finished_at') or existing_log.get('created_at')),
            existing_log.get('records_inserted'),
        )
    else:
        target_outcome = 'IMPORTED'
        summary = _run_import_tool(
            inspection,
            apply=True,
            sync_store_report_attr=args.sync_store_report_attr,
            created_by=args.created_by,
        )

    owner_result = _run_owner_schedule_once(args, default_snapshot_date=default_owner_snapshot_date)
    duty_free_result = _run_duty_free_schedule_once(args)
    target_affected_date_summary = _build_affected_date_summary(
        target_outcome,
        inspection,
        summary,
        report_date_mode=args.auto_report_date_mode,
    )
    owner_affected_date_summary = _build_owner_affected_date_summary(
        owner_result,
        inspection,
        report_date_mode=args.auto_report_date_mode,
    )
    duty_free_affected_date_summary = _build_duty_free_affected_date_summary(
        duty_free_result,
        report_date_mode=args.auto_report_date_mode,
    )
    affected_date_summary = _merge_affected_date_summaries(
        target_affected_date_summary,
        owner_affected_date_summary,
        duty_free_affected_date_summary,
    )
    affected_date_summary = _apply_natural_progress_fallback(
        inspection,
        affected_date_summary,
        sales_freshness_source_mode=sales_freshness_source_mode,
    )
    if affected_date_summary['affected_date_count'] == 0:
        ads_backfill_summary = _build_skipped_ads_backfill_summary(
            reason='empty_affected_dates',
            data_version=inspection.target_version,
            source='affected_dates',
        )
    elif not args.run_affected_ads:
        ads_backfill_summary = _build_skipped_ads_backfill_summary(
            reason='disabled_by_cli',
            data_version=inspection.target_version,
            source='affected_dates',
            report_dates=affected_date_summary['affected_dates'],
        )
    else:
        ads_backfill_summary = _run_ads_backfill_context(
            _build_affected_ads_backfill_context(
                inspection,
                summary,
                target_outcome,
                owner_result,
                duty_free_result,
                affected_date_summary,
            ),
            max_retries=args.max_retries,
            retry_sleep=args.retry_sleep,
        )
    if summary is not None:
        logger.info('目标导入完成: %s', _format_import_detail(summary))
        for warning_message in _get_import_warning_messages(summary):
            logger.warning('目标导入告警: %s', warning_message)
    if owner_result is not None:
        if owner_result.summary is not None:
            logger.info('负责人导入完成: %s', _format_owner_import_detail(owner_result.summary))
        elif owner_result.existing_log is not None:
            logger.info(
                '负责人导入沿用既有成功记录: id=%s, message=%s',
                owner_result.existing_log['id'],
                owner_result.existing_log.get('message'),
            )
    if duty_free_result is not None:
        if duty_free_result.summary is not None:
            logger.info('免税月累计导入完成: %s', _format_duty_free_import_detail(duty_free_result.summary))
        elif duty_free_result.existing_log is not None:
            logger.info(
                '免税月累计导入沿用既有成功记录: id=%s, message=%s',
                duty_free_result.existing_log['id'],
                duty_free_result.existing_log.get('message'),
            )
    logger.info('受影响日期判断结果: %s', _format_affected_date_detail(affected_date_summary))
    logger.info('ADS批量重跑结果: %s', _format_ads_backfill_detail(ads_backfill_summary))
    return ScheduleRunResult(
        outcome=target_outcome,
        inspection=inspection,
        summary=summary,
        existing_log=existing_log,
        owner_result=owner_result,
        duty_free_result=duty_free_result,
        affected_date_summary=affected_date_summary,
        ads_backfill_summary=ads_backfill_summary,
    )


def _result_has_imported_branch(result: ScheduleRunResult) -> bool:
    return result.outcome == 'IMPORTED' or (
        result.owner_result is not None and result.owner_result.outcome == 'IMPORTED'
    ) or (
        result.duty_free_result is not None and result.duty_free_result.outcome == 'IMPORTED'
    )


def run_with_retries(args: argparse.Namespace) -> int:
    pending_ads_backfill_context: AdsBackfillContext | None = None
    effective_cutover_mode = resolve_cutover_mode(args.cutover_mode, rollback_to_legacy=args.rollback_to_legacy)
    sales_freshness_source_mode = derive_store_daily_freshness_source(
        effective_cutover_mode,
        explicit_source=args.sales_freshness_source,
    )

    try:
        _validate_args(args)
    except ScheduledImportError as exc:
        logger.error('门店日报专题调度参数检查失败: %s', exc)
        return 2

    try:
        lock_conn, lock_connection_id = _acquire_singleton_lock(SCHEDULE_LOCK_NAME)
    except ScheduledImportError as exc:
        logger.error('门店日报专题调度单实例锁检查失败: %s', exc)
        return 2

    logger.info(
        '已获取门店日报专题调度单实例锁: lock_name=%s, connection_id=%s',
        SCHEDULE_LOCK_NAME,
        lock_connection_id,
    )
    logger.info(
        '门店日报专题 cutover_mode=%s, sales_freshness_source_mode=%s, auto_report_date_mode=%s',
        effective_cutover_mode,
        sales_freshness_source_mode,
        args.auto_report_date_mode,
    )

    try:
        attempt = 0
        while attempt < args.max_retries:
            attempt += 1
            attempt_started_at = datetime.now()
            inspection: ImportInspection | None = None
            try:
                if args.rerun_report_date:
                    if pending_ads_backfill_context is None:
                        pending_ads_backfill_context = _build_explicit_ads_backfill_context(args)
                    logger.info('门店日报显式批量重跑开始（尝试 %s/%s）', attempt, args.max_retries)
                    ads_backfill_summary = _run_ads_backfill_context(
                        pending_ads_backfill_context,
                        max_retries=args.max_retries,
                        retry_sleep=args.retry_sleep,
                    )
                    logger.info('门店日报显式批量重跑完成: %s', _format_ads_backfill_detail(ads_backfill_summary))
                    write_total_control_chain_summary(
                        _build_explicit_ads_chain_summary_payload(
                            ads_backfill_summary,
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                        )
                    )
                    return 0

                if pending_ads_backfill_context is not None:
                    logger.info('门店日报专题调度继续执行剩余 ADS 批量重跑（尝试 %s/%s）', attempt, args.max_retries)
                    ads_backfill_summary = _run_ads_backfill_context(
                        pending_ads_backfill_context,
                        max_retries=args.max_retries,
                        retry_sleep=args.retry_sleep,
                    )
                    logger.info('剩余 ADS 批量重跑完成: %s', _format_ads_backfill_detail(ads_backfill_summary))
                    if (
                        pending_ads_backfill_context.inspection is not None
                        and pending_ads_backfill_context.affected_date_summary is not None
                    ):
                        _send_schedule_alert_if_enabled(
                            _compose_success_alert(
                                pending_ads_backfill_context.target_outcome or 'SKIPPED',
                                pending_ads_backfill_context.import_summary,
                                pending_ads_backfill_context.inspection,
                                pending_ads_backfill_context.owner_result,
                                pending_ads_backfill_context.duty_free_result,
                                pending_ads_backfill_context.affected_date_summary,
                                ads_backfill_summary,
                                attempt,
                                args.max_retries,
                            )
                        )
                        write_total_control_chain_summary(
                            _build_topic_chain_summary_payload(
                                status='SUCCESS',
                                started_at=attempt_started_at,
                                ended_at=datetime.now(),
                                summary_lines=[
                                    '动作：继续执行剩余 ADS 批量重跑',
                                    (
                                        'ADS批量重跑：'
                                        f"{ads_backfill_summary['completed_date_count']}/"
                                        f"{ads_backfill_summary['requested_date_count']}"
                                        f'（{AFFECTED_ADS_SHORT_LABEL}）'
                                    ),
                                    f'尝试：{attempt}/{args.max_retries}',
                                ],
                                detail_lines=[
                                    f"重跑说明：{ads_backfill_summary['note']}",
                                ],
                            )
                        )
                    return 0

                logger.info('门店日报专题调度开始（尝试 %s/%s）', attempt, args.max_retries)
                result = run_schedule_once(args)
                inspection = result.inspection
                if _result_has_imported_branch(result) and not args.conn_test:
                    if result.summary is not None and _has_import_warnings(result.summary):
                        _send_schedule_alert_if_enabled(
                            _compose_warning_alert(
                                result.outcome,
                                result.summary,
                                result.inspection,
                                result.owner_result,
                                result.duty_free_result,
                                result.affected_date_summary,
                                result.ads_backfill_summary,
                                attempt,
                                args.max_retries,
                            )
                        )
                    else:
                        _send_schedule_alert_if_enabled(
                            _compose_success_alert(
                                result.outcome,
                                result.summary,
                                result.inspection,
                                result.owner_result,
                                result.duty_free_result,
                                result.affected_date_summary,
                                result.ads_backfill_summary,
                                attempt,
                                args.max_retries,
                            )
                        )
                if result.outcome == 'SKIPPED' and result.existing_log is not None:
                    logger.info(
                        '本次未执行写库，沿用既有成功记录: id=%s, message=%s',
                        result.existing_log['id'],
                        result.existing_log.get('message'),
                    )
                write_total_control_chain_summary(
                    _build_schedule_result_chain_summary_payload(
                        result,
                        attempt,
                        args.max_retries,
                        attempt_started_at,
                        datetime.now(),
                    )
                )
                return 0
            except ScheduledImportSkip as exc:
                logger.info('门店日报专题调度本轮跳过: %s', exc)
                write_total_control_chain_summary(
                    _build_schedule_skip_chain_summary_payload(
                        str(exc),
                        attempt,
                        args.max_retries,
                        attempt_started_at,
                        datetime.now(),
                    )
                )
                return 0
            except ScheduledAdsBackfillError as exc:
                inspection = exc.context.inspection or inspection
                logger.error('门店日报专题调度批量重跑失败: %s', exc)
                if not exc.retryable:
                    if not args.rerun_report_date and not args.conn_test:
                        _send_schedule_alert_if_enabled(
                            _compose_failure_alert(
                                inspection,
                                str(exc),
                                attempt,
                                args.max_retries,
                                exhausted=False,
                                stage='ads_backfill',
                                ads_context=exc.context,
                            )
                        )
                    write_total_control_chain_summary(
                        _build_failure_chain_summary_payload(
                            str(exc),
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                            inspection=inspection,
                            stage='ads_backfill',
                            ads_context=exc.context,
                            exhausted=False,
                        )
                    )
                    return 2
                if attempt >= args.max_retries:
                    if not args.rerun_report_date and not args.conn_test:
                        _send_schedule_alert_if_enabled(
                            _compose_failure_alert(
                                inspection,
                                str(exc),
                                attempt,
                                args.max_retries,
                                exhausted=True,
                                stage='ads_backfill',
                                ads_context=exc.context,
                            )
                        )
                    write_total_control_chain_summary(
                        _build_failure_chain_summary_payload(
                            str(exc),
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                            inspection=inspection,
                            stage='ads_backfill',
                            ads_context=exc.context,
                            exhausted=True,
                        )
                    )
                    return 1
                pending_ads_backfill_context = exc.context
                logger.info('等待 %s 秒后重试剩余 ADS 批量重跑...', args.retry_sleep)
                time.sleep(args.retry_sleep)
            except ScheduledImportError as exc:
                previous_inspection = inspection
                inspection = exc.inspection or inspection
                logger.error('门店日报专题调度失败: %s', exc)
                if isinstance(exc.inspection, OwnerImportInspection):
                    failure_stage = 'owner_import'
                elif isinstance(exc.inspection, DutyFreeImportInspection):
                    failure_stage = 'duty_free_import'
                else:
                    failure_stage = 'import'
                target_completed = previous_inspection is not None and isinstance(
                    exc.inspection,
                    (OwnerImportInspection, DutyFreeImportInspection),
                )
                if not exc.retryable:
                    if not args.conn_test:
                        _send_schedule_alert_if_enabled(
                            _compose_failure_alert(
                                inspection,
                                str(exc),
                                attempt,
                                args.max_retries,
                                exhausted=False,
                                stage=failure_stage,
                                target_completed=target_completed,
                            )
                        )
                    write_total_control_chain_summary(
                        _build_failure_chain_summary_payload(
                            str(exc),
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                            inspection=inspection,
                            stage=failure_stage,
                            exhausted=False,
                        )
                    )
                    return 2
                if attempt >= args.max_retries:
                    if not args.conn_test:
                        _send_schedule_alert_if_enabled(
                            _compose_failure_alert(
                                inspection,
                                str(exc),
                                attempt,
                                args.max_retries,
                                exhausted=True,
                                stage=failure_stage,
                                target_completed=target_completed,
                            )
                        )
                    write_total_control_chain_summary(
                        _build_failure_chain_summary_payload(
                            str(exc),
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                            inspection=inspection,
                            stage=failure_stage,
                            exhausted=True,
                        )
                    )
                    return 1
                logger.info('等待 %s 秒后重试...', args.retry_sleep)
                time.sleep(args.retry_sleep)
            except Exception:
                error_trace = traceback.format_exc()
                error_message = error_trace.splitlines()[-1] if error_trace.splitlines() else '未捕获异常'
                logger.error('门店日报专题调度出现未捕获异常: %s', error_trace)
                if not _is_retryable_message(error_message, False):
                    if not args.conn_test:
                        _send_schedule_alert_if_enabled(
                            _compose_failure_alert(inspection, error_message, attempt, args.max_retries, exhausted=False)
                        )
                    write_total_control_chain_summary(
                        _build_failure_chain_summary_payload(
                            error_message,
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                            inspection=inspection,
                            stage='exception',
                            exhausted=False,
                        )
                    )
                    return 2
                if attempt >= args.max_retries:
                    if not args.conn_test:
                        _send_schedule_alert_if_enabled(
                            _compose_failure_alert(inspection, error_message, attempt, args.max_retries, exhausted=True)
                        )
                    write_total_control_chain_summary(
                        _build_failure_chain_summary_payload(
                            error_message,
                            attempt,
                            args.max_retries,
                            attempt_started_at,
                            datetime.now(),
                            inspection=inspection,
                            stage='exception',
                            exhausted=True,
                        )
                    )
                    return 1
                logger.info('等待 %s 秒后重试...', args.retry_sleep)
                time.sleep(args.retry_sleep)
        return 1
    finally:
        _release_singleton_lock(lock_conn, SCHEDULE_LOCK_NAME)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='门店日报专题调度入口')
    parser.add_argument('--file-path', help='可选：显式指定目标 Excel 路径')
    parser.add_argument(
        '--target-month',
        type=_parse_target_month_arg,
        help='可选：显式指定目标月份，格式 YYYY-MM；不传时默认按 NAS 最新修改文件解析',
    )
    parser.add_argument('--sheet-name', default=TARGET_DEFAULT_SHEET_NAME, help='目标导入模板工作表名称')
    parser.add_argument('--owner-file-path', help='可选：显式指定负责人快照 Excel 路径')
    parser.add_argument('--owner-sheet-name', help='可选：显式指定负责人快照工作表名称；不传时按工具默认规则自动匹配')
    parser.add_argument('--duty-free-file-path', help='可选：显式指定免税月累计 Excel 路径')
    parser.add_argument('--duty-free-sheet-name', default=DUTY_FREE_DEFAULT_SHEET_NAME, help='免税月累计工作表名称')
    parser.add_argument(
        '--owner-snapshot-date',
        type=date.fromisoformat,
        default=None,
        help='负责人快照日期，格式 YYYY-MM-DD；不传时默认跟随专题本轮实际处理的 report_date 上界',
    )
    parser.add_argument('--created-by', default=DEFAULT_CREATED_BY, help='正式导入时写入 created_by / updated_by')
    parser.add_argument('--conn-test', action='store_true', help='只做文件解析、日志表检查和 dry-run，不写库')
    parser.add_argument(
        '--cutover-mode',
        choices=(CUTOVER_MODE_LEGACY, CUTOVER_MODE_SHADOW_COMPARE, CUTOVER_MODE_V2),
        default=None,
        help='专题链 cutover 模式；未显式指定 freshness 来源时按该模式派生来源表',
    )
    parser.add_argument('--rollback-to-legacy', action='store_true', help='显式回滚到 legacy 模式')
    parser.add_argument(
        '--sales-freshness-source',
        choices=('legacy', 'v2'),
        default=None,
        help='专题 freshness 依赖来源：legacy=dws_sales_daily，v2=dws_sales_daily_v2；不传则按 cutover_mode 自动派生',
    )
    parser.add_argument(
        '--auto-report-date-mode',
        choices=AUTO_REPORT_DATE_MODE_CHOICES,
        default=AUTO_REPORT_DATE_MODE_PREVIOUS_DAY,
        help='自动模式下受影响日期统一上界：previous-day 默认按前一天生成最终版，current-day 按当天生成临时快照；显式 --rerun-report-date 模式不受影响',
    )
    parser.add_argument('--max-retries', type=int, default=ETL_DEFAULT_MAX_RETRIES, help='最大重试次数')
    parser.add_argument('--retry-sleep', type=int, default=ETL_DEFAULT_RETRY_SLEEP, help='重试等待秒数')
    parser.add_argument(
        '--rerun-report-date',
        action='append',
        type=date.fromisoformat,
        help='显式指定需批量重跑的日报日期，格式 YYYY-MM-DD；可重复传入多次',
    )
    parser.add_argument('--rerun-data-version', default='v1', help='显式批量重跑模式使用的数据版本号，默认 v1')
    parser.set_defaults(run_affected_ads=True)
    parser.add_argument(
        '--no-run-affected-ads',
        dest='run_affected_ads',
        action='store_false',
        help='导入成功后只记录受影响日期，不自动触发门店层、主体层与销售看板 ADS 批量重跑',
    )
    parser.set_defaults(sync_store_report_attr=True)
    parser.add_argument(
        '--no-sync-store-report-attr',
        dest='sync_store_report_attr',
        action='store_false',
        help='只导入 cfg_store_target_daily，不同步 dim_store_report_attr',
    )
    parser.set_defaults(run_owner_import=True)
    parser.add_argument(
        '--no-run-owner-import',
        dest='run_owner_import',
        action='store_false',
        help='关闭负责人快照导入，只保留目标导入与 ADS 批量重跑',
    )
    parser.set_defaults(run_duty_free_import=True)
    parser.add_argument(
        '--no-run-duty-free-import',
        dest='run_duty_free_import',
        action='store_false',
        help='关闭免税月累计导入，只保留目标导入、负责人导入与 ADS 批量重跑',
    )
    return parser


if __name__ == '__main__':
    sys.exit(run_with_retries(build_parser().parse_args()))