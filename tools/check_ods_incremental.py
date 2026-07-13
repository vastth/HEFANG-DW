# -*- coding: utf-8 -*-
"""
ODS增量对账（行数 + 金额/数量聚合）
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import create_mysql_engine, create_oracle_engine


def _oracle_engine():
    return create_oracle_engine()


def _mysql_engine():
    return create_mysql_engine()


def _fetch_one(engine, sql, params):
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).fetchone()
        return tuple(row) if row else (0, 0, 0)


def _fetch_pair(engine, sql, params):
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).fetchone()
        return tuple(row) if row else (0, 0)


def _fetch_scalar(engine, sql, params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).scalar() or 0


def _has_single_column_unique_index(engine, table_name, column_name):
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                                SELECT index_name AS index_name_alias,
                                             seq_in_index AS seq_in_index_alias,
                                             column_name AS column_name_alias
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = :table_name
                  AND non_unique = 0
                ORDER BY index_name, seq_in_index
                """
            ),
            {"table_name": table_name},
        ).fetchall()

    index_columns = {}
    for row in rows:
        row_mapping = getattr(row, "_mapping", row)
        index_name = _row_value(row_mapping, "index_name_alias", "index_name", "INDEX_NAME")
        column_name = _row_value(row_mapping, "column_name_alias", "column_name", "COLUMN_NAME")
        index_columns.setdefault(index_name, []).append(column_name)

    return any(columns == [column_name] for columns in index_columns.values())


def _fetch_duplicate_id_count(engine, table_name, duplicate_sql, params):
    if _has_single_column_unique_index(engine, table_name, "id"):
        return 0
    return _fetch_scalar(engine, duplicate_sql, params)


def _row_value(row_mapping, *keys):
    for key in keys:
        if key in row_mapping:
            return row_mapping[key]
    raise KeyError(keys[0])


def _print_diff(name, oracle_vals, mysql_vals):
    labels = ["count", "sum_qty", "sum_amount"]
    print(f"\n[{name}]")
    for idx, label in enumerate(labels):
        o_val = oracle_vals[idx] or 0
        m_val = mysql_vals[idx] or 0
        diff = (m_val - o_val)
        print(f"  {label}: oracle={o_val:,} mysql={m_val:,} diff={diff:+,}")


def _print_duplicate_state(name, duplicate_id_count):
    if duplicate_id_count:
        print(f"  duplicate_id_count: {duplicate_id_count:,}  <-- WARNING")
    else:
        print("  duplicate_id_count: 0")


def _print_fill_diff(name, oracle_vals, mysql_vals):
    oracle_total = oracle_vals[0] or 0
    oracle_filled = oracle_vals[1] or 0
    mysql_total = mysql_vals[0] or 0
    mysql_filled = mysql_vals[1] or 0
    oracle_blank = oracle_total - oracle_filled
    mysql_blank = mysql_total - mysql_filled

    print(f"\n[{name}]")
    print(f"  total_rows: oracle={oracle_total:,} mysql={mysql_total:,} diff={mysql_total - oracle_total:+,}")
    print(f"  filled_rows: oracle={oracle_filled:,} mysql={mysql_filled:,} diff={mysql_filled - oracle_filled:+,}")
    print(f"  blank_rows: oracle={oracle_blank:,} mysql={mysql_blank:,} diff={mysql_blank - oracle_blank:+,}")


def _parse_as_of(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("--as-of format should be YYYY-MM-DD or YYYY-MM-DD HH:MM:SS") from exc


def main(days, end_date=None, as_of=None):
    start_date = int((datetime.now() - timedelta(days=days)).strftime("%Y%m%d"))
    params = {"start_date": start_date}
    date_filter = " AND BILLDATE >= :start_date"
    if end_date:
        date_filter += " AND BILLDATE <= :end_date"
        params["end_date"] = end_date
    if as_of:
        params["as_of"] = as_of
        as_of_filter = " AND r.MODIFIEDDATE <= :as_of"
    else:
        as_of_filter = ""

    oracle = _oracle_engine()
    mysql = _mysql_engine()

    try:
        retail_oracle_sql = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(TOT_QTY) AS sum_qty,
                   SUM(TOT_AMT_ACTUAL) AS sum_amount
            FROM M_RETAIL r
            WHERE 1=1 {date_filter}{as_of_filter}
        """

        retail_mysql_sql = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(tot_qty) AS sum_qty,
                   SUM(tot_amt_actual) AS sum_amount
            FROM ods_m_retail r
            WHERE 1=1 {date_filter}{as_of_filter}
        """

        retailitem_oracle_sql = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.QTY) AS sum_qty,
                   SUM(ri.TOT_AMT_ACTUAL) AS sum_amount
            FROM M_RETAILITEM ri
            JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
            WHERE 1=1 {date_filter}{as_of_filter}
        """

        retailitem_mysql_sql = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.qty) AS sum_qty,
                   SUM(ri.tot_amt_actual) AS sum_amount
            FROM ods_m_retailitem ri
            JOIN ods_m_retail r ON ri.m_retail_id = r.id
            WHERE 1=1 {date_filter}{as_of_filter}
        """

        retail_oracle = _fetch_one(oracle, retail_oracle_sql, params)
        retail_mysql = _fetch_one(mysql, retail_mysql_sql, params)
        _print_diff("ods_m_retail", retail_oracle, retail_mysql)
        retail_dup_count = _fetch_duplicate_id_count(
            mysql,
            "ods_m_retail",
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT r.id
                FROM ods_m_retail r
                WHERE 1=1 {date_filter}{as_of_filter}
                GROUP BY r.id
                HAVING COUNT(*) > 1
            ) t
            """,
            params,
        )
        _print_duplicate_state("ods_m_retail", retail_dup_count)

        retail_oms_oracle_sql = f"""
            SELECT COUNT(*) AS total_rows,
                   SUM(CASE WHEN OMS_SOURCECODE IS NOT NULL AND TRIM(OMS_SOURCECODE) IS NOT NULL THEN 1 ELSE 0 END) AS filled_rows
            FROM M_RETAIL r
            WHERE 1=1 {date_filter}{as_of_filter}
        """

        retail_oms_mysql_sql = f"""
            SELECT COUNT(*) AS total_rows,
                   SUM(CASE WHEN oms_sourcecode IS NOT NULL AND oms_sourcecode <> '' THEN 1 ELSE 0 END) AS filled_rows
            FROM ods_m_retail r
            WHERE 1=1 {date_filter}{as_of_filter}
        """

        retail_oms_oracle = _fetch_pair(oracle, retail_oms_oracle_sql, params)
        retail_oms_mysql = _fetch_pair(mysql, retail_oms_mysql_sql, params)
        _print_fill_diff("ods_m_retail.oms_sourcecode", retail_oms_oracle, retail_oms_mysql)

        retailitem_oracle = _fetch_one(oracle, retailitem_oracle_sql, params)
        retailitem_mysql = _fetch_one(mysql, retailitem_mysql_sql, params)
        _print_diff("ods_m_retailitem", retailitem_oracle, retailitem_mysql)
        retailitem_dup_count = _fetch_duplicate_id_count(
            mysql,
            "ods_m_retailitem",
            f"""
            SELECT COUNT(*)
            FROM (
                SELECT ri.id
                FROM ods_m_retailitem ri
                JOIN ods_m_retail r ON ri.m_retail_id = r.id
                WHERE 1=1 {date_filter}{as_of_filter}
                GROUP BY ri.id
                HAVING COUNT(*) > 1
            ) t
            """,
            params,
        )
        _print_duplicate_state("ods_m_retailitem", retailitem_dup_count)

    finally:
        oracle.dispose()
        mysql.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ODS incremental validation")
    parser.add_argument("--days", type=int, default=7, help="days back for reconciliation")
    parser.add_argument("--end-date", type=int, default=None, help="end date YYYYMMDD")
    parser.add_argument("--as-of", type=str, default=None, help="cutoff datetime YYYY-MM-DD[ HH:MM:SS]")
    args = parser.parse_args()
    cutoff = _parse_as_of(args.as_of)
    main(args.days, end_date=args.end_date, as_of=cutoff)
