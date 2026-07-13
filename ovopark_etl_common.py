# -*- coding: utf-8 -*-
"""万店掌 ETL 公共工具。"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Iterable

from pymysql.cursors import DictCursor

from db_connections import connect_mysql


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def connect_mysql_dict(*, timeout_profile: str = 'long_running', autocommit: bool = False):
    return connect_mysql(
        timeout_profile=timeout_profile,
        cursorclass=DictCursor,
        autocommit=autocommit,
    )


def fetch_missing_tables(conn, required_tables: Iterable[str]) -> list[str]:
    table_list = tuple(required_tables)
    placeholders = ', '.join(['%s'] * len(table_list))
    sql = f"""
        SELECT table_name AS table_name_alias
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name IN ({placeholders})
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, table_list)
        existing_tables = {
            row.get('table_name_alias') or row.get('TABLE_NAME_ALIAS')
            for row in cursor.fetchall()
        }
    return [table_name for table_name in table_list if table_name not in existing_tables]


def ensure_tables(conn, required_tables: Iterable[str]) -> None:
    missing_tables = fetch_missing_tables(conn, required_tables)
    if missing_tables:
        raise RuntimeError(f"缺少依赖表: {', '.join(missing_tables)}")


def iter_dates(start_date: date, end_date: date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def to_date_id(value: date) -> int:
    return int(value.strftime('%Y%m%d'))


def month_start(value: date) -> date:
    return value.replace(day=1)


def month_end(value: date) -> date:
    if value.month == 12:
        next_month = value.replace(year=value.year + 1, month=1, day=1)
    else:
        next_month = value.replace(month=value.month + 1, day=1)
    return next_month - timedelta(days=1)


def parse_datetime(value):
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def as_int(value):
    if value in (None, ''):
        return None
    return int(value)


def as_decimal_value(value):
    if value in (None, ''):
        return None
    return float(value)
