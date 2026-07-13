# -*- coding: utf-8 -*-
"""主链 + 销售专题总控调度入口。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from alerts import send_wechat_alert
from cutover_controls import (
    CUTOVER_MODE_LEGACY,
    CUTOVER_MODE_SHADOW_COMPARE,
    CUTOVER_MODE_V2,
    resolve_cutover_mode,
)
from config import WECHAT_WEBHOOK
from control_chain_summary import (
    TOTAL_CONTROL_SUMMARY_PATH_ENV,
    TOTAL_CONTROL_SUPPRESS_CHILD_WECHAT_ENV,
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
LOG_FILE = LOG_DIR / f"scheduled_total_control_{datetime.now().strftime('%Y%m%d')}.log"
SCHEDULED_ETL_PATH = PROJECT_DIR / 'scheduled_etl.py'
SCHEDULED_STORE_DAILY_REPORT_PATH = PROJECT_DIR / 'scheduled_store_daily_report.py'
SCHEDULED_DWS_V2_SHADOW_PATH = PROJECT_DIR / 'scheduled_dws_v2_shadow.py'
TOPIC_REPORT_DATE_MODE_CHOICES = ('previous-day', 'current-day')


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

EMOJI_RE = re.compile(
    '['
    '\U0001F300-\U0001FAFF'
    '\u200d'
    '\u20e3'
    '\u2600-\u27BF'
    '\ufe0f'
    ']'
)
STATUS_EMOJI = {
    'SUCCESS': '✅',
    'WARNING': '⚠️',
    'FAILED': '❌',
    'ERROR': '🚨',
    'SKIPPED': '⏭️',
    'UNKNOWN': '❔',
}
STATUS_TEXT = {
    'SUCCESS': '成功',
    'WARNING': '警告',
    'FAILED': '失败',
    'ERROR': '异常',
    'SKIPPED': '跳过',
    'UNKNOWN': '未知',
}
INDEX_EMOJI = ['1️⃣', '2️⃣', '3️⃣', '4️⃣', '5️⃣', '6️⃣', '7️⃣', '8️⃣', '9️⃣']
WECHAT_TEXT_MAX_BYTES = 2048
PYTHON_CLEANUP_EXIT_CODE = 120
WECHAT_ALERT_LAYOUT_VARIANTS = (
    (3, 2, 220, True, True),
    (2, 1, 180, True, True),
    (1, 1, 140, False, True),
    (0, 1, 120, False, False),
)


@dataclass(frozen=True)
class ChainDefinition:
    key: str
    label: str
    script_path: Path
    block_on_failure: bool = True


@dataclass(frozen=True)
class ChildRunResult:
    chain: ChainDefinition
    exit_code: int
    summary: dict


CHAIN_DEFINITIONS = {
    'main': ChainDefinition(
        key='main_etl',
        label='主链调度',
        script_path=SCHEDULED_ETL_PATH,
        block_on_failure=True,
    ),
    'store_daily_topic': ChainDefinition(
        key='store_daily_topic',
        label='门店销售专题',
        script_path=SCHEDULED_STORE_DAILY_REPORT_PATH,
        block_on_failure=True,
    ),
    'dws_v2_shadow': ChainDefinition(
        key='dws_v2_shadow',
        label='DWS v2 Shadow',
        script_path=SCHEDULED_DWS_V2_SHADOW_PATH,
        block_on_failure=False,
    ),
    'dws_v2_pre_refresh': ChainDefinition(
        key='dws_v2_pre_refresh',
        label='DWS v2 读源预刷新',
        script_path=SCHEDULED_DWS_V2_SHADOW_PATH,
        block_on_failure=True,
    ),
}


def _build_python_command(
    chain: ChainDefinition,
    conn_test: bool,
    *,
    cutover_mode: str | None,
    rollback_to_legacy: bool,
    topic_report_date_mode: str | None,
) -> list[str]:
    command = [sys.executable, str(chain.script_path)]
    if conn_test:
        command.append('--conn-test')
    if chain.key in ('main_etl', 'store_daily_topic'):
        if cutover_mode:
            command.extend(['--cutover-mode', cutover_mode])
        if rollback_to_legacy:
            command.append('--rollback-to-legacy')
    if chain.key == 'store_daily_topic' and topic_report_date_mode:
        command.extend(['--auto-report-date-mode', topic_report_date_mode])
    if chain.key == 'dws_v2_pre_refresh':
        command.append('--skip-ads-shadow-validation')
    return command


def _format_datetime_value(value: datetime) -> str:
    return value.strftime('%Y-%m-%d %H:%M:%S')


def _strip_emoji_for_local_log(text: object) -> str:
    plain_text = EMOJI_RE.sub('', str(text))
    return re.sub(r'[ \t]{2,}', ' ', plain_text).strip()


def _format_message_line(text: object, *, emoji: bool, indent: str = '   ', bullet: str = '-') -> str:
    display_text = str(text).strip()
    if display_text.startswith('-'):
        display_text = display_text[1:].strip()
    if not emoji:
        display_text = _strip_emoji_for_local_log(display_text)
    return f'{indent}{bullet} {display_text}'


def _truncate_utf8_text(text: str, max_bytes: int) -> str:
    if max_bytes <= 0:
        return ''

    encoded = text.encode('utf-8')
    if len(encoded) <= max_bytes:
        return text

    suffix = '...'
    suffix_bytes = len(suffix.encode('utf-8'))
    if max_bytes <= suffix_bytes:
        return suffix[:max_bytes]

    truncated = encoded[: max_bytes - suffix_bytes]
    while truncated:
        try:
            display_text = truncated.decode('utf-8')
            return display_text.rstrip() + suffix
        except UnicodeDecodeError:
            truncated = truncated[:-1]
    return suffix


def _format_wechat_message_line(
    text: object,
    *,
    indent: str = '      ',
    bullet: str = '•',
    line_max_bytes: int | None = None,
) -> str:
    display_text = str(text).strip()
    if display_text.startswith('-'):
        display_text = display_text[1:].strip()

    prefix = f'{indent}{bullet} '
    if line_max_bytes is not None:
        budget = max(line_max_bytes - len(prefix.encode('utf-8')), 0)
        display_text = _truncate_utf8_text(display_text, budget)
    return f'{prefix}{display_text}'


def _count_child_statuses(child_results: list[ChildRunResult]) -> tuple[int, int, int, int]:
    success_cnt = 0
    warning_cnt = 0
    failed_cnt = 0
    skipped_cnt = 0
    for child_result in child_results:
        status = str(child_result.summary.get('status', 'UNKNOWN')).upper()
        if status == 'SUCCESS':
            success_cnt += 1
        elif status == 'WARNING':
            warning_cnt += 1
        elif status in ('FAILED', 'ERROR'):
            failed_cnt += 1
        elif status == 'SKIPPED':
            skipped_cnt += 1
    return success_cnt, warning_cnt, failed_cnt, skipped_cnt


def _create_summary_output_path(chain_key: str) -> Path:
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return LOG_DIR / f'total_control_{chain_key}_{timestamp}_{os.getpid()}_{int(time.time() * 1000)}.json'


def _should_downgrade_python_cleanup_exit(exit_code: int, payload: dict) -> bool:
    return exit_code == PYTHON_CLEANUP_EXIT_CODE and str(payload.get('status', '')).upper() == 'SUCCESS'


def _apply_python_cleanup_exit_warning(payload: dict, exit_code: int) -> None:
    payload['status'] = 'WARNING'
    payload.setdefault('issue_lines', [])
    issue_line = (
        f'- 子链业务摘要已成功，但 Python 在进程清理/标准流刷新阶段返回退出码 {exit_code}；'
        '总控按告警继续执行'
    )
    if issue_line not in payload['issue_lines']:
        payload['issue_lines'].append(issue_line)
    payload['python_cleanup_exit_downgraded'] = True


def _load_child_summary(
    chain: ChainDefinition,
    output_path: Path,
    exit_code: int,
    started_at: datetime,
    ended_at: datetime,
) -> dict:
    payload: dict
    if output_path.exists():
        try:
            payload = json.loads(output_path.read_text(encoding='utf-8'))
        except Exception as exc:
            logger.warning('读取子链摘要失败，回退到默认摘要: chain=%s, error=%s', chain.label, exc)
            payload = {}
        finally:
            output_path.unlink(missing_ok=True)
    else:
        payload = {}

    if not payload:
        status = 'SUCCESS' if exit_code == 0 else 'FAILED'
        headline = '子链执行完成' if exit_code == 0 else '子链执行失败'
        summary_lines = [f'退出码：{exit_code}']
        if exit_code != 0:
            summary_lines.append('子链未输出结构化摘要，请查看对应日志文件定位失败原因')
        payload = {
            'chain_key': chain.key,
            'chain_label': chain.label,
            'status': status,
            'headline': headline,
            'started_at': _format_datetime_value(started_at),
            'ended_at': _format_datetime_value(ended_at),
            'duration_seconds': int((ended_at - started_at).total_seconds()),
            'summary_lines': summary_lines,
            'detail_lines': [],
            'issue_lines': [],
        }

    payload.setdefault('chain_key', chain.key)
    payload.setdefault('chain_label', chain.label)
    payload.setdefault('status', 'SUCCESS' if exit_code == 0 else 'FAILED')
    payload.setdefault('headline', '子链执行摘要')
    payload.setdefault('started_at', _format_datetime_value(started_at))
    payload.setdefault('ended_at', _format_datetime_value(ended_at))
    payload.setdefault('duration_seconds', int((ended_at - started_at).total_seconds()))
    payload.setdefault('summary_lines', [])
    payload.setdefault('detail_lines', [])
    payload.setdefault('issue_lines', [])
    payload.setdefault('python_cleanup_exit_downgraded', False)
    if _should_downgrade_python_cleanup_exit(exit_code, payload):
        _apply_python_cleanup_exit_warning(payload, exit_code)
    elif exit_code != 0 and chain.block_on_failure:
        payload['status'] = 'FAILED'
        payload.setdefault('issue_lines', [])
        payload['issue_lines'].append(f'- 子链退出码为 {exit_code}，按阻断链路处理')
    return payload


def _effective_child_exit_code(child_result: ChildRunResult) -> int:
    if child_result.summary.get('python_cleanup_exit_downgraded'):
        return 0
    return child_result.exit_code


def _run_child(
    chain: ChainDefinition,
    conn_test: bool,
    *,
    cutover_mode: str | None,
    rollback_to_legacy: bool,
    topic_report_date_mode: str | None,
) -> ChildRunResult:
    command = _build_python_command(
        chain,
        conn_test=conn_test,
        cutover_mode=cutover_mode,
        rollback_to_legacy=rollback_to_legacy,
        topic_report_date_mode=topic_report_date_mode,
    )
    logger.info('开始执行子链路: %s, command=%s', chain.label, ' '.join(command))
    child_env = os.environ.copy()
    child_env['PYTHONUTF8'] = '1'
    child_env['PYTHONIOENCODING'] = 'utf-8'
    child_env[TOTAL_CONTROL_SUPPRESS_CHILD_WECHAT_ENV] = '1'
    output_path = _create_summary_output_path(chain.key)
    child_env[TOTAL_CONTROL_SUMMARY_PATH_ENV] = str(output_path)
    started_at = datetime.now()

    completed = subprocess.run(
        command,
        cwd=str(PROJECT_DIR),
        env=child_env,
        check=False,
    )
    ended_at = datetime.now()
    logger.info('子链路执行完成: %s, exit_code=%s', chain.label, completed.returncode)
    return ChildRunResult(
        chain=chain,
        exit_code=completed.returncode,
        summary=_load_child_summary(
            chain,
            output_path,
            completed.returncode,
            started_at,
            ended_at,
        ),
    )


def _build_skipped_child_result(chain: ChainDefinition, reason: str) -> ChildRunResult:
    now = datetime.now()
    return ChildRunResult(
        chain=chain,
        exit_code=0,
        summary={
            'chain_key': chain.key,
            'chain_label': chain.label,
            'status': 'SKIPPED',
            'headline': '子链未执行',
            'started_at': _format_datetime_value(now),
            'ended_at': _format_datetime_value(now),
            'duration_seconds': 0,
            'summary_lines': [reason],
            'detail_lines': [],
            'issue_lines': [],
        },
    )


def _compose_total_control_alert(
    child_results: list[ChildRunResult],
    overall_status: str,
    started_at: datetime,
    ended_at: datetime,
    conn_test: bool,
    main_only: bool,
    topic_only: bool,
    shadow_only: bool,
) -> str:
    last_message = ''
    for summary_line_limit, issue_line_limit, line_max_bytes, include_mode, include_headline in WECHAT_ALERT_LAYOUT_VARIANTS:
        message = _compose_total_control_high_level_alert(
            child_results,
            overall_status,
            started_at,
            ended_at,
            conn_test,
            main_only,
            topic_only,
            shadow_only,
            summary_line_limit=summary_line_limit,
            issue_line_limit=issue_line_limit,
            line_max_bytes=line_max_bytes,
            include_mode=include_mode,
            include_headline=include_headline,
        )
        if len(message.encode('utf-8')) <= WECHAT_TEXT_MAX_BYTES:
            return message
        last_message = message

    return _truncate_utf8_text(last_message, WECHAT_TEXT_MAX_BYTES)


def _compose_total_control_high_level_alert(
    child_results: list[ChildRunResult],
    overall_status: str,
    started_at: datetime,
    ended_at: datetime,
    conn_test: bool,
    main_only: bool,
    topic_only: bool,
    shadow_only: bool,
    *,
    summary_line_limit: int,
    issue_line_limit: int,
    line_max_bytes: int,
    include_mode: bool,
    include_headline: bool,
) -> str:
    title_map = {
        'SUCCESS': '✅【总控调度完成】',
        'WARNING': '⚠️【总控调度完成 · 存在告警】',
        'FAILED': '❌【总控调度完成 · 存在失败】',
        'ERROR': '🚨【总控调度异常】',
    }

    success_cnt, warning_cnt, failed_cnt, skipped_cnt = _count_child_statuses(child_results)

    lines = [title_map.get(overall_status, '📊【总控调度摘要】')]
    lines.append(f'🕒 时间：{_format_datetime_value(started_at)} ~ {ended_at.strftime("%H:%M:%S")}')
    lines.append(f'⏱️ 耗时：{int((ended_at - started_at).total_seconds())} 秒')
    if include_mode:
        lines.append(f'🧭 模式：conn_test={conn_test}, main_only={main_only}, topic_only={topic_only}, shadow_only={shadow_only}')
    lines.append(f'📌 链路结果：✅ 成功{success_cnt} / ⚠️ 警告{warning_cnt} / ❌ 失败{failed_cnt} / ⏭️ 跳过{skipped_cnt}')
    lines.append(f'🗂️ 详细明细见日志：{LOG_FILE.name}')
    lines.append('')
    lines.append('📋 链路摘要')
    for index, child_result in enumerate(child_results, start=1):
        summary = child_result.summary
        status = str(summary.get('status', 'UNKNOWN')).upper()
        icon = STATUS_EMOJI.get(status, '❔')
        status_text = STATUS_TEXT.get(status, '未知')
        index_icon = INDEX_EMOJI[index - 1] if index <= len(INDEX_EMOJI) else f'{index}.'
        duration_seconds = summary.get('duration_seconds')
        cost = '' if duration_seconds is None else f' [{duration_seconds}s]'
        lines.append(f'{index_icon} {icon} {child_result.chain.label}: {status}{cost}（{status_text}）')
        headline = summary.get('headline')
        if include_headline and headline:
            lines.append(f'   🧾 {headline}')
        summary_lines = summary.get('summary_lines') or []
        issue_lines = summary.get('issue_lines') or []
        if summary_lines and summary_line_limit > 0:
            lines.append('   📝 摘要')
            for text in summary_lines[:summary_line_limit]:
                lines.append(
                    _format_wechat_message_line(
                        text,
                        indent='      ',
                        bullet='•',
                        line_max_bytes=line_max_bytes,
                    )
                )
        if issue_lines and issue_line_limit > 0:
            lines.append('   ⚠️ 异常/提示')
            for text in issue_lines[:issue_line_limit]:
                lines.append(
                    _format_wechat_message_line(
                        text,
                        indent='      ',
                        bullet='•',
                        line_max_bytes=line_max_bytes,
                    )
                )
    return '\n'.join(lines)


def _compose_total_control_local_summary(
    child_results: list[ChildRunResult],
    overall_status: str,
    started_at: datetime,
    ended_at: datetime,
    conn_test: bool,
    main_only: bool,
    topic_only: bool,
    shadow_only: bool,
) -> str:
    title_map = {
        'SUCCESS': '总控调度完成',
        'WARNING': '总控调度完成（存在告警）',
        'FAILED': '总控调度完成（存在失败）',
        'ERROR': '总控调度异常',
    }

    success_cnt, warning_cnt, failed_cnt, skipped_cnt = _count_child_statuses(child_results)

    lines = [title_map.get(overall_status, '总控调度摘要')]
    lines.append('-' * 64)
    lines.append(f'时间范围: {_format_datetime_value(started_at)} ~ {ended_at.strftime("%H:%M:%S")}')
    lines.append(f'总耗时: {int((ended_at - started_at).total_seconds())} 秒')
    lines.append(f'运行模式: conn_test={conn_test}, main_only={main_only}, topic_only={topic_only}, shadow_only={shadow_only}')
    lines.append(f'链路结果: 成功={success_cnt}, 警告={warning_cnt}, 失败={failed_cnt}, 跳过={skipped_cnt}')
    lines.append('')
    lines.append('链路摘要:')
    for index, child_result in enumerate(child_results, start=1):
        summary = child_result.summary
        status = str(summary.get('status', 'UNKNOWN')).upper()
        duration_seconds = summary.get('duration_seconds')
        cost = '' if duration_seconds is None else f' [{duration_seconds}s]'
        lines.append(f'  {index}. {child_result.chain.label}: {status}{cost}')
        headline = summary.get('headline')
        if headline:
            lines.append(f'     标题: {_strip_emoji_for_local_log(headline)}')
        for text in summary.get('summary_lines', []):
            lines.append(_format_message_line(text, emoji=False, indent='     ', bullet='-'))
        detail_lines = summary.get('detail_lines') or []
        if detail_lines:
            lines.append('     明细:')
            for text in detail_lines:
                lines.append(_format_message_line(text, emoji=False, indent='       ', bullet='-'))
        issue_lines = summary.get('issue_lines') or []
        if issue_lines:
            lines.append('     异常/提示:')
            for text in issue_lines:
                lines.append(_format_message_line(text, emoji=False, indent='       ', bullet='-'))
    return '\n'.join(lines)


def _send_total_control_alert(
    child_results: list[ChildRunResult],
    overall_status: str,
    started_at: datetime,
    ended_at: datetime,
    conn_test: bool,
    main_only: bool,
    topic_only: bool,
    shadow_only: bool,
) -> None:
    local_summary = _compose_total_control_local_summary(
        child_results,
        overall_status,
        started_at,
        ended_at,
        conn_test,
        main_only,
        topic_only,
        shadow_only,
    )
    logger.info('总控调度本地摘要:\n%s', local_summary)
    send_wechat_alert(
        WECHAT_WEBHOOK,
        _compose_total_control_alert(
            child_results,
            overall_status,
            started_at,
            ended_at,
            conn_test,
            main_only,
            topic_only,
            shadow_only,
        ),
    )


def _has_warning_child(child_results: list[ChildRunResult]) -> bool:
    for child_result in child_results:
        status = str(child_result.summary.get('status', 'UNKNOWN')).upper()
        if status == 'WARNING':
            return True
        if _effective_child_exit_code(child_result) != 0 and not child_result.chain.block_on_failure:
            return True
    return False


def run_total_control(
    conn_test: bool = False,
    main_only: bool = False,
    topic_only: bool = False,
    shadow_only: bool = False,
    cutover_mode: str | None = None,
    rollback_to_legacy: bool = False,
    topic_report_date_mode: str | None = None,
) -> int:
    started_at = datetime.now()
    logger.info('=' * 80)
    logger.info('总控调度开始')
    logger.info(
        '运行模式: conn_test=%s, main_only=%s, topic_only=%s, shadow_only=%s, cutover_mode=%s, rollback_to_legacy=%s, topic_report_date_mode=%s',
        conn_test,
        main_only,
        topic_only,
        shadow_only,
        cutover_mode,
        rollback_to_legacy,
        topic_report_date_mode,
    )
    logger.info('=' * 80)

    effective_cutover_mode = resolve_cutover_mode(
        cutover_mode,
        rollback_to_legacy=rollback_to_legacy,
    )

    selected_only_flags = sum(1 for flag in (main_only, topic_only, shadow_only) if flag)
    if selected_only_flags > 1:
        logger.error('参数冲突: --main-only / --topic-only / --shadow-only 只能指定一个')
        return 2

    child_results: list[ChildRunResult] = []

    pre_refresh_done = False
    if effective_cutover_mode == CUTOVER_MODE_V2 and not conn_test and not shadow_only:
        pre_refresh_result = _run_child(
            CHAIN_DEFINITIONS['dws_v2_pre_refresh'],
            conn_test=False,
            cutover_mode=cutover_mode,
            rollback_to_legacy=rollback_to_legacy,
            topic_report_date_mode=topic_report_date_mode,
        )
        child_results.append(pre_refresh_result)
        pre_refresh_done = True
        pre_refresh_exit_code = _effective_child_exit_code(pre_refresh_result)
        if pre_refresh_exit_code != 0:
            logger.error('V2 读源预刷新失败，停止触发主链与专题链: exit_code=%s', pre_refresh_exit_code)
            child_results.append(
                _build_skipped_child_result(
                    CHAIN_DEFINITIONS['main'],
                    '未执行：V2 读源预刷新失败，为避免 ads_inventory_health 读到空/旧 v2 源，总控已停止主链',
                )
            )
            if not main_only:
                child_results.append(
                    _build_skipped_child_result(
                        CHAIN_DEFINITIONS['store_daily_topic'],
                        '未执行：V2 读源预刷新失败，总控已停止专题链',
                    )
                )
                child_results.append(
                    _build_skipped_child_result(
                        CHAIN_DEFINITIONS['dws_v2_shadow'],
                        '未执行：V2 读源预刷新失败，不再执行后置 shadow',
                    )
                )
            _send_total_control_alert(
                child_results,
                'FAILED',
                started_at,
                datetime.now(),
                conn_test,
                main_only,
                topic_only,
                shadow_only,
            )
            return pre_refresh_exit_code

    if shadow_only:
        shadow_result = _run_child(
            CHAIN_DEFINITIONS['dws_v2_shadow'],
            conn_test=conn_test,
            cutover_mode=cutover_mode,
            rollback_to_legacy=rollback_to_legacy,
            topic_report_date_mode=topic_report_date_mode,
        )
        child_results.append(shadow_result)
        shadow_exit_code = _effective_child_exit_code(shadow_result)
        overall_status = 'SUCCESS' if shadow_exit_code == 0 and str(shadow_result.summary.get('status', 'SUCCESS')).upper() == 'SUCCESS' else 'WARNING'
        _send_total_control_alert(
            child_results,
            overall_status,
            started_at,
            datetime.now(),
            conn_test,
            main_only,
            topic_only,
            shadow_only,
        )
        return shadow_exit_code

    if topic_only:
        topic_result = _run_child(
            CHAIN_DEFINITIONS['store_daily_topic'],
            conn_test=conn_test,
            cutover_mode=cutover_mode,
            rollback_to_legacy=rollback_to_legacy,
            topic_report_date_mode=topic_report_date_mode,
        )
        child_results.append(topic_result)
        overall_status = 'SUCCESS'
        topic_exit_code = _effective_child_exit_code(topic_result)
        if topic_exit_code != 0:
            overall_status = 'FAILED'
        elif str(topic_result.summary.get('status', 'SUCCESS')).upper() == 'WARNING':
            overall_status = 'WARNING'
        _send_total_control_alert(
            child_results,
            overall_status,
            started_at,
            datetime.now(),
            conn_test,
            main_only,
            topic_only,
            shadow_only,
        )
        return topic_exit_code

    main_result = _run_child(
        CHAIN_DEFINITIONS['main'],
        conn_test=conn_test,
        cutover_mode=cutover_mode,
        rollback_to_legacy=rollback_to_legacy,
        topic_report_date_mode=topic_report_date_mode,
    )
    child_results.append(main_result)
    main_exit_code = _effective_child_exit_code(main_result)
    if main_exit_code != 0:
        logger.error('主链调度失败，停止继续触发后续子链: exit_code=%s', main_exit_code)
        if not main_only:
            child_results.append(
                _build_skipped_child_result(
                    CHAIN_DEFINITIONS['store_daily_topic'],
                    '未执行：主链调度失败，总控已停止后续专题链',
                )
            )
            child_results.append(
                _build_skipped_child_result(
                    CHAIN_DEFINITIONS['dws_v2_shadow'],
                    '未执行：主链调度失败，总控已停止后续 shadow 链',
                )
            )
        _send_total_control_alert(
            child_results,
            'FAILED',
            started_at,
            datetime.now(),
            conn_test,
            main_only,
            topic_only,
            shadow_only,
        )
        return main_exit_code

    if main_only:
        logger.info('命中 --main-only，仅执行主链调度')
        _send_total_control_alert(
            child_results,
            'SUCCESS',
            started_at,
            datetime.now(),
            conn_test,
            main_only,
            topic_only,
            shadow_only,
        )
        return 0

    blocking_exit_code = 0

    topic_result = _run_child(
        CHAIN_DEFINITIONS['store_daily_topic'],
        conn_test=conn_test,
        cutover_mode=cutover_mode,
        rollback_to_legacy=rollback_to_legacy,
        topic_report_date_mode=topic_report_date_mode,
    )
    child_results.append(topic_result)
    topic_exit_code = _effective_child_exit_code(topic_result)
    if topic_exit_code != 0:
        logger.error('销售专题调度失败: exit_code=%s', topic_exit_code)
        blocking_exit_code = topic_exit_code

    if pre_refresh_done:
        child_results.append(
            _build_skipped_child_result(
                CHAIN_DEFINITIONS['dws_v2_shadow'],
                '未执行：V2 模式已在主链前完成 DWS v2 读源预刷新，本轮不重复执行后置 shadow',
            )
        )
    else:
        shadow_result = _run_child(
            CHAIN_DEFINITIONS['dws_v2_shadow'],
            conn_test=conn_test,
            cutover_mode=cutover_mode,
            rollback_to_legacy=rollback_to_legacy,
            topic_report_date_mode=topic_report_date_mode,
        )
        child_results.append(shadow_result)
        shadow_exit_code = _effective_child_exit_code(shadow_result)
        if shadow_exit_code != 0:
            logger.warning('DWS v2 shadow 调度返回非 0：exit_code=%s（不阻断总控主退出码）', shadow_exit_code)

    overall_status = 'SUCCESS'
    if blocking_exit_code != 0:
        overall_status = 'FAILED'
    elif _has_warning_child(child_results):
        overall_status = 'WARNING'

    logger.info('总控调度完成，整体状态=%s', overall_status)
    _send_total_control_alert(
        child_results,
        overall_status,
        started_at,
        datetime.now(),
        conn_test,
        main_only,
        topic_only,
        shadow_only,
    )
    return blocking_exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='主链 + 销售专题总控调度入口')
    parser.add_argument('--conn-test', action='store_true', help='主链与销售专题链都以连接测试模式运行')
    parser.add_argument('--main-only', action='store_true', help='仅运行主链调度，不触发销售专题链')
    parser.add_argument('--topic-only', action='store_true', help='仅运行销售专题链，不触发主链调度')
    parser.add_argument('--shadow-only', action='store_true', help='仅运行 DWS v2 shadow 链，不触发主链与销售专题链')
    parser.add_argument(
        '--cutover-mode',
        choices=(CUTOVER_MODE_LEGACY, CUTOVER_MODE_SHADOW_COMPARE, CUTOVER_MODE_V2),
        default=None,
        help='透传给主链与专题链的 cutover 模式',
    )
    parser.add_argument('--rollback-to-legacy', action='store_true', help='显式回滚到 legacy 模式')
    parser.add_argument(
        '--topic-report-date-mode',
        choices=TOPIC_REPORT_DATE_MODE_CHOICES,
        default=None,
        help='销售专题自动模式下的 report_date 上界：previous-day 生成前一天最终版，current-day 生成当天临时快照；不传则沿用专题默认 previous-day',
    )
    return parser


if __name__ == '__main__':
    args = build_parser().parse_args()
    sys.exit(
        run_total_control(
            conn_test=args.conn_test,
            main_only=args.main_only,
            topic_only=args.topic_only,
            shadow_only=args.shadow_only,
            cutover_mode=args.cutover_mode,
            rollback_to_legacy=args.rollback_to_legacy,
            topic_report_date_mode=args.topic_report_date_mode,
        )
    )