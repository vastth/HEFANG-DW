# -*- coding: utf-8 -*-
"""M3 raw / DWD 小窗口装载公共工具。

本模块只封装批量 upsert、标识符校验和 DataFrame 清洗，不承载业务口径。
真实写库入口仍由各 ETL 脚本的 ``--execute`` 显式触发。
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Connection


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_identifier(identifier: str) -> str:
    """校验并引用 MySQL 标识符。"""

    if not IDENTIFIER_RE.fullmatch(identifier):
        raise ValueError(f"非法 SQL 标识符: {identifier}")
    return f"`{identifier}`"


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """统一列名小写，并将 pandas 缺失值转为 None。"""

    normalized = df.copy()
    normalized.columns = [str(column).lower() for column in normalized.columns]
    return normalized.where(pd.notna(normalized), None)


def _clean_value(value: Any) -> Any:
    if value is pd.NaT:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def dataframe_to_records(df: pd.DataFrame, columns: Sequence[str]) -> list[dict[str, Any]]:
    """按指定列顺序转换为 SQLAlchemy executemany 记录。"""

    missing_columns = [column for column in columns if column not in df.columns]
    if missing_columns:
        raise KeyError(f"DataFrame 缺少目标列: {missing_columns}")

    records: list[dict[str, Any]] = []
    for row in df.loc[:, list(columns)].to_dict(orient='records'):
        records.append({key: _clean_value(value) for key, value in row.items()})
    return records


def build_upsert_values_sql(
    table_name: str,
    insert_columns: Sequence[str],
    update_columns: Sequence[str],
    extra_update_expressions: Iterable[str] = (),
):
    """生成 ``INSERT ... VALUES ... ON DUPLICATE KEY UPDATE`` 语句。"""

    table_sql = quote_identifier(table_name)
    insert_sql = ', '.join(quote_identifier(column) for column in insert_columns)
    value_sql = ', '.join(f":{column}" for column in insert_columns)
    update_sql = ', '.join(
        f"{quote_identifier(column)} = VALUES({quote_identifier(column)})"
        for column in update_columns
    )
    extra_sql = ', '.join(extra_update_expressions)
    if update_sql and extra_sql:
        update_sql = f"{update_sql}, {extra_sql}"
    elif extra_sql:
        update_sql = extra_sql

    return text(
        f"INSERT INTO {table_sql} ({insert_sql}) VALUES ({value_sql}) "
        f"ON DUPLICATE KEY UPDATE {update_sql}"
    )


def upsert_dataframe(
    conn: Connection,
    table_name: str,
    df: pd.DataFrame,
    insert_columns: Sequence[str],
    update_columns: Sequence[str],
    extra_update_expressions: Iterable[str] = (),
) -> int:
    """将一个 DataFrame 批量 upsert 到 MySQL，返回输入记录数。"""

    if df.empty:
        return 0

    normalized = normalize_dataframe(df)
    records = dataframe_to_records(normalized, insert_columns)
    if not records:
        return 0

    conn.execute(
        build_upsert_values_sql(
            table_name,
            insert_columns,
            update_columns,
            extra_update_expressions=extra_update_expressions,
        ),
        records,
    )
    return len(records)