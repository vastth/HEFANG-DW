# -*- coding: utf-8 -*-
"""同步达播订单到零售单头的 MySQL 桥接缓存。"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import create_mysql_engine, create_oracle_engine


ORACLE_IN_BATCH_SIZE = 500


def build_mysql_engine():
    return create_mysql_engine()


def build_oracle_engine():
    return create_oracle_engine()


def chunked(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def ensure_table(mysql_engine):
    sql = (REPO_ROOT / "SQL" / "create_ads_dabo_order_retail_bridge.sql").read_text(encoding="utf-8")
    with mysql_engine.begin() as conn:
        conn.execute(text(sql))


def fetch_scope_order_ids(mysql_engine, source_file):
    sql = text(
        """
        SELECT DISTINCT main_order_id
        FROM ads_dabo_order_bridge
        WHERE source_file = :source_file
          AND main_order_id IS NOT NULL
          AND main_order_id <> ''
        ORDER BY main_order_id
        """
    )
    with mysql_engine.connect() as conn:
        rows = conn.execute(sql, {"source_file": source_file}).fetchall()
    return [row[0] for row in rows]


def load_bridge(mysql_engine, oracle_engine, source_file):
    order_ids = fetch_scope_order_ids(mysql_engine, source_file)
    if not order_ids:
        return 0, 0

    insert_sql = text(
        """
        INSERT INTO ads_dabo_order_retail_bridge (
            source_file,
            main_order_id,
            retail_id,
            billdate,
            retail_tot_amt_actual,
            retail_status,
            retail_isactive,
            synced_at
        ) VALUES (
            :source_file,
            :main_order_id,
            :retail_id,
            :billdate,
            :retail_tot_amt_actual,
            :retail_status,
            :retail_isactive,
            NOW()
        )
        ON DUPLICATE KEY UPDATE
            main_order_id = VALUES(main_order_id),
            billdate = VALUES(billdate),
            retail_tot_amt_actual = VALUES(retail_tot_amt_actual),
            retail_status = VALUES(retail_status),
            retail_isactive = VALUES(retail_isactive),
            synced_at = NOW()
        """
    )

    with mysql_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM ads_dabo_order_retail_bridge WHERE source_file = :source_file"),
            {"source_file": source_file},
        )

    inserted = 0
    for batch in chunked(order_ids, ORACLE_IN_BATCH_SIZE):
        bind_names = [f"oid_{idx}" for idx in range(len(batch))]
        bind_expr = ", ".join(f":{name}" for name in bind_names)
        params = {name: value for name, value in zip(bind_names, batch)}
        oracle_sql = text(
            f"""
            SELECT
                OMS_SOURCECODE AS main_order_id,
                ID AS retail_id,
                BILLDATE AS billdate,
                TOT_AMT_ACTUAL AS retail_tot_amt_actual,
                STATUS AS retail_status,
                ISACTIVE AS retail_isactive
            FROM M_RETAIL
            WHERE OMS_SOURCECODE IN ({bind_expr})
            """
        )
        with oracle_engine.connect() as conn:
            rows = conn.execute(oracle_sql, params).mappings().all()

        if not rows:
            continue

        payload = [
            {
                "source_file": source_file,
                "main_order_id": str(row["main_order_id"]),
                "retail_id": int(row["retail_id"]),
                "billdate": int(row["billdate"]),
                "retail_tot_amt_actual": row["retail_tot_amt_actual"],
                "retail_status": row["retail_status"],
                "retail_isactive": row["retail_isactive"],
            }
            for row in rows
        ]
        with mysql_engine.begin() as conn:
            conn.execute(insert_sql, payload)
        inserted += len(payload)

    return len(order_ids), inserted


def main():
    parser = argparse.ArgumentParser(description="同步达播订单到零售单头桥接缓存")
    parser.add_argument("--source-file", required=True, help="达播样本文件名，例如 dabo_20260204.csv")
    args = parser.parse_args()

    mysql_engine = build_mysql_engine()
    oracle_engine = build_oracle_engine()
    try:
        ensure_table(mysql_engine)
        order_count, inserted = load_bridge(mysql_engine, oracle_engine, args.source_file)
        print(
            {
                "source_file": args.source_file,
                "scope_order_count": order_count,
                "bridge_rows": inserted,
            },
            flush=True,
        )
    finally:
        oracle_engine.dispose()
        mysql_engine.dispose()


if __name__ == "__main__":
    main()