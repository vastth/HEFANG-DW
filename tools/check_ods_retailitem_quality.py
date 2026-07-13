# -*- coding: utf-8 -*-
"""
ODS 明细质量对账工具（按全量或日期范围）
- 对比 Oracle 与 MySQL 的行数/数量/金额
- 按线上/线下通道（MODIFIEDDATE / SETTIME）拆分
"""

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

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


def _print_diff(name, oracle_vals, mysql_vals):
    labels = ["count", "sum_qty", "sum_amount"]
    print(f"\n[{name}]")
    for idx, label in enumerate(labels):
        o_val = oracle_vals[idx] or 0
        m_val = mysql_vals[idx] or 0
        diff = m_val - o_val
        print(f"  {label}: oracle={o_val:,} mysql={m_val:,} diff={diff:+,}")


def _parse_as_of(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("--as-of format should be YYYY-MM-DD or YYYY-MM-DD HH:MM:SS") from exc


def _date_range_args(args):
    if args.all:
        return None, None
    if args.start_date and args.end_date:
        return int(args.start_date), int(args.end_date)
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=args.days)
    return int(start_dt.strftime("%Y%m%d")), int(end_dt.strftime("%Y%m%d"))


def _date_filter_sql(prefix, start_date, end_date, as_of):
    if start_date is None:
        date_filter = ""
        params = {}
    else:
        date_filter = f" AND {prefix}BILLDATE >= :start_date AND {prefix}BILLDATE <= :end_date"
        params = {
            "start_date": start_date,
            "end_date": end_date,
        }
    if as_of:
        date_filter += f" AND {prefix}MODIFIEDDATE <= :as_of"
        params["as_of"] = as_of
    return date_filter, params


def main(args):
    start_date, end_date = _date_range_args(args)
    as_of = _parse_as_of(args.as_of)

    oracle = _oracle_engine()
    mysql = _mysql_engine()

    try:
        oracle_date_where, oracle_params = _date_filter_sql("r.", start_date, end_date, as_of)
        mysql_date_where, mysql_params = _date_filter_sql("r.", start_date, end_date, as_of)

        oracle_base_join = "FROM M_RETAILITEM ri LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID"
        mysql_base_join = "FROM ods_m_retailitem ri LEFT JOIN ods_m_retail r ON ri.m_retail_id = r.id"

        oracle_all = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.QTY) AS sum_qty,
                   SUM(ri.TOT_AMT_ACTUAL) AS sum_amount
            {oracle_base_join}
            WHERE 1=1 {oracle_date_where}
        """

        mysql_all = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.qty) AS sum_qty,
                   SUM(ri.tot_amt_actual) AS sum_amount
            {mysql_base_join}
            WHERE 1=1 {mysql_date_where}
        """

        _print_diff(
            "ods_m_retailitem_all",
            _fetch_one(oracle, oracle_all, oracle_params),
            _fetch_one(mysql, mysql_all, mysql_params),
        )

        oracle_online = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.QTY) AS sum_qty,
                   SUM(ri.TOT_AMT_ACTUAL) AS sum_amount
            {oracle_base_join}
            WHERE ri.MODIFIEDDATE IS NOT NULL {oracle_date_where}
        """

        mysql_online = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.qty) AS sum_qty,
                   SUM(ri.tot_amt_actual) AS sum_amount
            {mysql_base_join}
            WHERE ri.modifieddate IS NOT NULL {mysql_date_where}
        """

        _print_diff(
            "ods_m_retailitem_online_modifieddate",
            _fetch_one(oracle, oracle_online, oracle_params),
            _fetch_one(mysql, mysql_online, mysql_params),
        )

        oracle_offline = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.QTY) AS sum_qty,
                   SUM(ri.TOT_AMT_ACTUAL) AS sum_amount
            {oracle_base_join}
            WHERE ri.MODIFIEDDATE IS NULL AND ri.SETTIME IS NOT NULL {oracle_date_where}
        """

        mysql_offline = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.qty) AS sum_qty,
                   SUM(ri.tot_amt_actual) AS sum_amount
            {mysql_base_join}
            WHERE ri.modifieddate IS NULL AND ri.settime IS NOT NULL {mysql_date_where}
        """

        _print_diff(
            "ods_m_retailitem_offline_settime",
            _fetch_one(oracle, oracle_offline, oracle_params),
            _fetch_one(mysql, mysql_offline, mysql_params),
        )

        oracle_unknown = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.QTY) AS sum_qty,
                   SUM(ri.TOT_AMT_ACTUAL) AS sum_amount
            {oracle_base_join}
            WHERE ri.MODIFIEDDATE IS NULL AND ri.SETTIME IS NULL {oracle_date_where}
        """

        mysql_unknown = f"""
            SELECT COUNT(*) AS cnt,
                   SUM(ri.qty) AS sum_qty,
                   SUM(ri.tot_amt_actual) AS sum_amount
            {mysql_base_join}
            WHERE ri.modifieddate IS NULL AND ri.settime IS NULL {mysql_date_where}
        """

        _print_diff(
            "ods_m_retailitem_unknown_nulls",
            _fetch_one(oracle, oracle_unknown, oracle_params),
            _fetch_one(mysql, mysql_unknown, mysql_params),
        )

    finally:
        oracle.dispose()
        mysql.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ODS retailitem quality check")
    parser.add_argument("--all", action="store_true", help="compare full dataset (ignore billdate)")
    parser.add_argument("--days", type=int, default=7, help="days back for reconciliation")
    parser.add_argument("--start-date", type=str, default=None, help="start date YYYYMMDD")
    parser.add_argument("--end-date", type=str, default=None, help="end date YYYYMMDD")
    parser.add_argument("--as-of", type=str, default=None, help="cutoff datetime YYYY-MM-DD[ HH:MM:SS]")
    main(parser.parse_args())
