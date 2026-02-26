# -*- coding: utf-8 -*-
"""
ODS增量对账（行数 + 金额/数量聚合）
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import ORACLE_CONFIG, MYSQL_CONN_STR


def _oracle_engine():
    oracle_url = URL.create(
        "oracle+oracledb",
        username=ORACLE_CONFIG['user'],
        password=ORACLE_CONFIG['password'],
        host=ORACLE_CONFIG['host'],
        port=ORACLE_CONFIG['port'],
        database=ORACLE_CONFIG['service_name'],
    )
    return create_engine(oracle_url)


def _mysql_engine():
    return create_engine(MYSQL_CONN_STR)


def _fetch_one(engine, sql, params):
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).fetchone()
        return tuple(row) if row else (0, 0, 0)


def _print_diff(name, oracle_vals, mysql_vals):
    labels = ["count", "sum_qty", "sum_amount"]
    print(f"\n[{name}]")
    for idx, label in enumerate(labels):
        o_val = oracle_vals[idx] or 0
        m_val = mysql_vals[idx] or 0
        diff = (m_val - o_val)
        print(f"  {label}: oracle={o_val:,} mysql={m_val:,} diff={diff:+,}")


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

        retailitem_oracle = _fetch_one(oracle, retailitem_oracle_sql, params)
        retailitem_mysql = _fetch_one(mysql, retailitem_mysql_sql, params)
        _print_diff("ods_m_retailitem", retailitem_oracle, retailitem_mysql)

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
