# -*- coding: utf-8 -*-
"""统一数据库连接工厂。

本模块只封装连接生命周期的基础设施参数，不承载任何业务口径。
短生命周期 ETL 调用方仍应在任务结束后显式 `dispose()` Engine 或 `close()` 直连。
"""

from __future__ import annotations

import os
from typing import Any

import oracledb
import pymysql
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

from config import MYSQL_CONFIG, MYSQL_CONN_STR, ORACLE_CONFIG, ORACLE_DSN


def _int_env(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default)).strip()
    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"环境变量 {name} 必须是整数，当前值: {raw_value}") from exc


MYSQL_POOL_SIZE = _int_env('MYSQL_POOL_SIZE', 5)
MYSQL_MAX_OVERFLOW = _int_env('MYSQL_MAX_OVERFLOW', 5)
MYSQL_POOL_TIMEOUT = _int_env('MYSQL_POOL_TIMEOUT', 30)
MYSQL_POOL_RECYCLE = _int_env('MYSQL_POOL_RECYCLE', 1800)
MYSQL_CONNECT_TIMEOUT = _int_env('MYSQL_CONNECT_TIMEOUT', 10)
MYSQL_READ_TIMEOUT = _int_env('MYSQL_READ_TIMEOUT', 60)
MYSQL_WRITE_TIMEOUT = _int_env('MYSQL_WRITE_TIMEOUT', 60)
MYSQL_ETL_READ_TIMEOUT = _int_env('MYSQL_ETL_READ_TIMEOUT', max(MYSQL_READ_TIMEOUT, 300))
MYSQL_ETL_WRITE_TIMEOUT = _int_env('MYSQL_ETL_WRITE_TIMEOUT', max(MYSQL_WRITE_TIMEOUT, 300))
MYSQL_LONG_RUNNING_READ_TIMEOUT = _int_env('MYSQL_LONG_RUNNING_READ_TIMEOUT', max(MYSQL_ETL_READ_TIMEOUT, 600))
MYSQL_LONG_RUNNING_WRITE_TIMEOUT = _int_env('MYSQL_LONG_RUNNING_WRITE_TIMEOUT', max(MYSQL_ETL_WRITE_TIMEOUT, 600))

ORACLE_POOL_SIZE = _int_env('ORACLE_POOL_SIZE', 3)
ORACLE_MAX_OVERFLOW = _int_env('ORACLE_MAX_OVERFLOW', 2)
ORACLE_POOL_TIMEOUT = _int_env('ORACLE_POOL_TIMEOUT', 30)
ORACLE_POOL_RECYCLE = _int_env('ORACLE_POOL_RECYCLE', 1800)

MYSQL_TIMEOUT_OPTION_KEYS = ('connect_timeout', 'read_timeout', 'write_timeout')
MYSQL_TIMEOUT_PROFILES: dict[str, dict[str, int]] = {
    'default': {
        'connect_timeout': MYSQL_CONNECT_TIMEOUT,
        'read_timeout': MYSQL_READ_TIMEOUT,
        'write_timeout': MYSQL_WRITE_TIMEOUT,
    },
    'etl': {
        'connect_timeout': MYSQL_CONNECT_TIMEOUT,
        'read_timeout': MYSQL_ETL_READ_TIMEOUT,
        'write_timeout': MYSQL_ETL_WRITE_TIMEOUT,
    },
    'long_running': {
        'connect_timeout': MYSQL_CONNECT_TIMEOUT,
        'read_timeout': MYSQL_LONG_RUNNING_READ_TIMEOUT,
        'write_timeout': MYSQL_LONG_RUNNING_WRITE_TIMEOUT,
    },
}


def _get_mysql_timeout_options(timeout_profile: str) -> dict[str, int]:
    try:
        return dict(MYSQL_TIMEOUT_PROFILES[timeout_profile])
    except KeyError as exc:
        available_profiles = ', '.join(sorted(MYSQL_TIMEOUT_PROFILES))
        raise ValueError(
            f"未知的 MySQL 超时档位: {timeout_profile}；可选值: {available_profiles}"
        ) from exc


def _merge_mysql_timeout_overrides(timeout_options: dict[str, int], overrides: dict[str, Any]) -> dict[str, int]:
    merged_options = dict(timeout_options)
    for option_name in MYSQL_TIMEOUT_OPTION_KEYS:
        option_value = overrides.pop(option_name, None)
        if option_value is not None:
            merged_options[option_name] = option_value
    return merged_options


def create_mysql_engine(timeout_profile: str = 'default', **overrides: Any) -> Engine:
    """创建带统一池参数的 MySQL SQLAlchemy Engine。"""

    connect_args = _get_mysql_timeout_options(timeout_profile)
    custom_connect_args = overrides.pop('connect_args', None) or {}
    connect_args.update(custom_connect_args)
    connect_args = _merge_mysql_timeout_overrides(connect_args, overrides)

    options: dict[str, Any] = {
        'pool_pre_ping': True,
        'pool_size': MYSQL_POOL_SIZE,
        'max_overflow': MYSQL_MAX_OVERFLOW,
        'pool_timeout': MYSQL_POOL_TIMEOUT,
        'pool_recycle': MYSQL_POOL_RECYCLE,
        'connect_args': connect_args,
    }
    options.update(overrides)
    return create_engine(MYSQL_CONN_STR, **options)


def connect_mysql(timeout_profile: str = 'default', **overrides: Any):
    """创建带统一超时参数的 PyMySQL 直连。"""

    timeout_options = _merge_mysql_timeout_overrides(
        _get_mysql_timeout_options(timeout_profile),
        overrides,
    )

    options: dict[str, Any] = {
        **MYSQL_CONFIG,
        **timeout_options,
    }
    options.update(overrides)
    return pymysql.connect(**options)


def create_oracle_engine(**overrides: Any) -> Engine:
    """创建带统一池参数的 Oracle SQLAlchemy Engine。"""

    oracle_url = URL.create(
        'oracle+oracledb',
        username=ORACLE_CONFIG['user'],
        password=ORACLE_CONFIG['password'],
        host=ORACLE_CONFIG['host'],
        port=ORACLE_CONFIG['port'],
        database=ORACLE_CONFIG['service_name'],
    )
    options: dict[str, Any] = {
        'pool_pre_ping': True,
        'pool_size': ORACLE_POOL_SIZE,
        'max_overflow': ORACLE_MAX_OVERFLOW,
        'pool_timeout': ORACLE_POOL_TIMEOUT,
        'pool_recycle': ORACLE_POOL_RECYCLE,
    }
    options.update(overrides)
    return create_engine(oracle_url, **options)


def connect_oracle(**overrides: Any):
    """创建 Oracle 直连。"""

    options: dict[str, Any] = {
        'user': ORACLE_CONFIG['user'],
        'password': ORACLE_CONFIG['password'],
        'dsn': ORACLE_DSN,
    }
    options.update(overrides)
    return oracledb.connect(**options)