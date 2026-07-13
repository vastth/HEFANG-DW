# -*- coding: utf-8 -*-
"""DWS v2 手工写入分支的通用辅助函数。

本模块只提供确认令牌、命名锁、运行证据 JSON 等基础设施能力；
具体业务 SQL 仍由各 DWS v2 脚本显式构造。
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_REPORT_DIR = REPO_ROOT / 'reports' / 'context_cache'


def validate_lock_settings(lock_name: str, lock_timeout_seconds: int) -> None:
    """校验 MySQL GET_LOCK 参数，避免运行期才失败。"""

    if not lock_name or not lock_name.strip():
        raise ValueError('lock_name 不能为空')
    if len(lock_name) > 64:
        raise ValueError(f'MySQL 命名锁名称不能超过 64 字符: {lock_name}')
    if lock_timeout_seconds < 0:
        raise ValueError('lock_timeout_seconds 不能为负数')


def ensure_write_confirmation(confirm_write: str | None, expected_token: str, target_table: str) -> None:
    """要求用户手工执行写入时提供精确确认令牌。"""

    if confirm_write != expected_token:
        raise RuntimeError(
            f'拒绝写入 {target_table}：必须同时传入 --execute '
            f'--confirm-write {expected_token}。'
        )


def commit_if_open(conn) -> None:
    """结束 SQLAlchemy 隐式事务，便于随后显式开启写事务。"""

    try:
        if conn.in_transaction():
            conn.commit()
    except AttributeError:
        return


def acquire_named_lock(conn, lock_name: str, lock_timeout_seconds: int) -> None:
    """获取 MySQL 命名锁；失败时抛出异常。"""

    validate_lock_settings(lock_name, lock_timeout_seconds)
    lock_result = conn.execute(
        text('SELECT GET_LOCK(:lock_name, :lock_timeout_seconds)'),
        {
            'lock_name': lock_name,
            'lock_timeout_seconds': lock_timeout_seconds,
        },
    ).scalar()
    commit_if_open(conn)
    if lock_result != 1:
        raise RuntimeError(f'获取 MySQL 命名锁失败: {lock_name}, result={lock_result}')


def release_named_lock(conn, lock_name: str) -> None:
    """释放 MySQL 命名锁；释放失败时抛出异常，保留给调用方记录。"""

    release_result = conn.execute(
        text('SELECT RELEASE_LOCK(:lock_name)'),
        {'lock_name': lock_name},
    ).scalar()
    commit_if_open(conn)
    if release_result not in (0, 1):
        raise RuntimeError(f'释放 MySQL 命名锁状态未知: {lock_name}, result={release_result}')


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=' ')
    return str(value)


def _normalize_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_default(value) for key, value in mapping.items()}


def fetch_one_mapping(conn, sql: str, params: dict[str, Any]) -> dict[str, Any]:
    """读取单行 SQL 结果并转成可 JSON 序列化的 dict。"""

    row = conn.execute(text(sql), params).mappings().first()
    return _normalize_mapping(dict(row)) if row else {}


def fetch_all_mappings(conn, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """读取多行 SQL 结果并转成可 JSON 序列化的 dict 列表。"""

    rows = conn.execute(text(sql), params).mappings().all()
    return [_normalize_mapping(dict(row)) for row in rows]


def write_runtime_report(report: dict[str, Any], prefix: str, output_json: str | None = None) -> Path:
    """将运行证据写入 reports/context_cache 或用户指定路径。"""

    if output_json:
        output_path = Path(output_json)
        if not output_path.is_absolute():
            output_path = REPO_ROOT / output_path
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = DEFAULT_REPORT_DIR / f'{prefix}_{timestamp}.json'

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding='utf-8',
    )
    return output_path