# -*- coding: utf-8 -*-
"""回填 ods_m_retail.oms_sourcecode。"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
import sys
import time

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import create_mysql_engine, create_oracle_engine


STAGE_TABLE = "tmp_ods_m_retail_oms_sourcecode_stage"
CHUNK_SIZE = 20000
ORACLE_IN_BATCH_SIZE = 500
MYSQL_UPDATE_BATCH_SIZE = 50
FULL_APPLY_BATCH_SIZE = 10000
LOCK_WAIT_SLEEP_SECONDS = 1
LOCK_WAIT_RETRY_TIMES = 3


def build_oracle_engine():
    return create_oracle_engine()


def build_mysql_engine():
    return create_mysql_engine()


def _is_lock_wait_error(exc):
    error_text = str(exc).lower()
    return "lock wait timeout" in error_text or "deadlock found" in error_text


def prepare_stage(mysql_engine):
    with mysql_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGE_TABLE}"))
        conn.execute(text(
            f"""
            CREATE TABLE {STAGE_TABLE} (
                id BIGINT NOT NULL PRIMARY KEY,
                oms_sourcecode VARCHAR(512) NOT NULL,
                KEY idx_{STAGE_TABLE}_sourcecode (oms_sourcecode)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        ))


def load_stage(oracle_engine, mysql_engine):
    oracle_sql = text(
        """
        SELECT
            ID AS id,
            OMS_SOURCECODE AS oms_sourcecode
        FROM M_RETAIL
        WHERE OMS_SOURCECODE IS NOT NULL
          AND LENGTH(TRIM(OMS_SOURCECODE)) > 0
        ORDER BY ID
        """
    )
    insert_sql = text(
        f"""
        INSERT INTO {STAGE_TABLE} (id, oms_sourcecode)
        VALUES (:id, :oms_sourcecode)
        """
    )

    loaded = 0
    for chunk in pd.read_sql(oracle_sql, oracle_engine, chunksize=CHUNK_SIZE):
        records = [
            {"id": int(row.id), "oms_sourcecode": str(row.oms_sourcecode)}
            for row in chunk.itertuples(index=False)
        ]
        with mysql_engine.begin() as conn:
            conn.execute(insert_sql, records)
        loaded += len(records)
        print(f"[{datetime.now()}] stage loaded rows={loaded}", flush=True)

    return loaded


def _execute_update_statement(mysql_engine, sql, params):
    for attempt in range(1, LOCK_WAIT_RETRY_TIMES + 1):
        try:
            with mysql_engine.begin() as conn:
                result = conn.execute(sql, params)
            return result.rowcount
        except OperationalError as exc:
            if not _is_lock_wait_error(exc) or attempt == LOCK_WAIT_RETRY_TIMES:
                raise
            time.sleep(LOCK_WAIT_SLEEP_SECONDS)
    return 0


def _fetch_stage_batch(mysql_engine, last_id, batch_size):
    sql = text(
        f"""
        SELECT MIN(id) AS batch_start_id, MAX(id) AS batch_end_id, COUNT(*) AS batch_row_count
        FROM (
            SELECT id
            FROM {STAGE_TABLE}
            WHERE id > :last_id
            ORDER BY id
            LIMIT {batch_size}
        ) t
        """
    )
    with mysql_engine.connect() as conn:
        row = conn.execute(sql, {"last_id": last_id}).mappings().first()
    if not row or row["batch_end_id"] is None:
        return None
    return {
        "batch_start_id": int(row["batch_start_id"]),
        "batch_end_id": int(row["batch_end_id"]),
        "batch_row_count": int(row["batch_row_count"]),
    }


def apply_backfill(mysql_engine, apply_batch_size):
    update_sql = text(
        f"""
        UPDATE ods_m_retail t
        INNER JOIN {STAGE_TABLE} s ON s.id = t.id
        SET t.oms_sourcecode = s.oms_sourcecode
        WHERE s.id >= :batch_start_id
          AND s.id <= :batch_end_id
          AND (
              t.oms_sourcecode IS NULL
              OR t.oms_sourcecode = ''
              OR t.oms_sourcecode <> s.oms_sourcecode
          )
        """
    )

    last_id = 0
    batch_count = 0
    processed_stage_rows = 0
    updated_rows = 0
    while True:
        batch = _fetch_stage_batch(mysql_engine, last_id, apply_batch_size)
        if batch is None:
            break

        batch_count += 1
        batch_updated = _execute_update_statement(
            mysql_engine,
            update_sql,
            {
                "batch_start_id": batch["batch_start_id"],
                "batch_end_id": batch["batch_end_id"],
            },
        )
        updated_rows += batch_updated
        processed_stage_rows += batch["batch_row_count"]
        last_id = batch["batch_end_id"]
        print(
            {
                "batch_no": batch_count,
                "batch_start_id": batch["batch_start_id"],
                "batch_end_id": batch["batch_end_id"],
                "batch_stage_rows": batch["batch_row_count"],
                "batch_updated_rows": batch_updated,
                "processed_stage_rows": processed_stage_rows,
                "updated_rows": updated_rows,
            },
            flush=True,
        )

    with mysql_engine.connect() as conn:
        filled_rows = conn.execute(text(
            """
            SELECT COUNT(*)
            FROM ods_m_retail
            WHERE oms_sourcecode IS NOT NULL
              AND oms_sourcecode <> ''
            """
        )).scalar()
        blank_rows = conn.execute(text(
            """
            SELECT COUNT(*)
            FROM ods_m_retail
            WHERE oms_sourcecode IS NULL
               OR oms_sourcecode = ''
            """
        )).scalar()

    with mysql_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGE_TABLE}"))

    return updated_rows, filled_rows, blank_rows, batch_count, processed_stage_rows


def _chunked(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


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


def apply_scope_backfill(mysql_engine, oracle_engine, source_file):
    order_ids = fetch_scope_order_ids(mysql_engine, source_file)
    if not order_ids:
        return 0, 0, 0, 0, 0

    update_sql = text(
        """
        UPDATE ods_m_retail
        SET oms_sourcecode = :oms_sourcecode
        WHERE id = :id
          AND (
              oms_sourcecode IS NULL
              OR oms_sourcecode = ''
              OR oms_sourcecode <> :oms_sourcecode
          )
        """
    )

    def execute_update_batch(payload):
        return _execute_update_statement(mysql_engine, update_sql, payload)

    def execute_update_rows(payload):
        updated = 0
        deferred = []
        for row in payload:
            try:
                updated += execute_update_batch([row])
            except OperationalError as exc:
                if _is_lock_wait_error(exc):
                    deferred.append(row)
                    continue
                raise

        for row in deferred:
            updated += execute_update_batch([row])
        return updated

    matched_retail_rows = 0
    update_count = 0
    for batch in _chunked(order_ids, ORACLE_IN_BATCH_SIZE):
        bind_names = [f"oid_{idx}" for idx in range(len(batch))]
        bind_expr = ", ".join(f":{name}" for name in bind_names)
        params = {name: value for name, value in zip(bind_names, batch)}
        oracle_sql = text(
            f"""
            SELECT ID AS id, OMS_SOURCECODE AS oms_sourcecode
            FROM M_RETAIL
            WHERE OMS_SOURCECODE IN ({bind_expr})
            """
        )
        with oracle_engine.connect() as conn:
            rows = conn.execute(oracle_sql, params).mappings().all()
        if not rows:
            continue

        matched_retail_rows += len(rows)
        payload = [
            {"id": int(row["id"]), "oms_sourcecode": str(row["oms_sourcecode"])}
            for row in rows
        ]
        for update_batch in _chunked(payload, MYSQL_UPDATE_BATCH_SIZE):
            try:
                update_count += execute_update_batch(update_batch)
            except OperationalError as exc:
                if not _is_lock_wait_error(exc):
                    raise
                update_count += execute_update_rows(update_batch)

    with mysql_engine.connect() as conn:
        matched_scope_orders = conn.execute(text(
            """
            SELECT COUNT(*)
            FROM (
                SELECT DISTINCT b.main_order_id
                FROM ads_dabo_order_bridge b
                INNER JOIN ods_m_retail r
                    ON r.oms_sourcecode = b.main_order_id
                WHERE b.source_file = :source_file
                  AND b.main_order_id IS NOT NULL
                  AND b.main_order_id <> ''
            ) t
            """
        ), {"source_file": source_file}).scalar()
        filled_rows = conn.execute(text(
            """
            SELECT COUNT(*)
            FROM ods_m_retail
            WHERE oms_sourcecode IS NOT NULL
              AND oms_sourcecode <> ''
            """
        )).scalar()

    return len(order_ids), matched_retail_rows, update_count, matched_scope_orders, filled_rows


def main():
    parser = argparse.ArgumentParser(description="回填 ods_m_retail.oms_sourcecode")
    parser.add_argument(
        "--apply-only",
        action="store_true",
        help="仅将已装载完成的暂存表应用到 ods_m_retail，并清理暂存表",
    )
    parser.add_argument(
        "--source-file",
        help="仅回填指定达播样本文件涉及的主订单编号，例如 dabo_20260204.csv",
    )
    parser.add_argument(
        "--apply-batch-size",
        type=int,
        default=FULL_APPLY_BATCH_SIZE,
        help=f"全量 apply 阶段每批处理的暂存行数，默认 {FULL_APPLY_BATCH_SIZE}",
    )
    args = parser.parse_args()

    if args.apply_batch_size <= 0:
        raise ValueError("--apply-batch-size 必须为正整数")

    oracle_engine = build_oracle_engine()
    mysql_engine = build_mysql_engine()

    try:
        if args.source_file:
            order_count, matched_retail_rows, updated_rows, matched_scope_orders, filled_rows = apply_scope_backfill(
                mysql_engine, oracle_engine, args.source_file
            )
            print(
                {
                    "source_file": args.source_file,
                    "scope_order_count": order_count,
                    "matched_retail_rows": matched_retail_rows,
                    "updated_rows": updated_rows,
                    "matched_scope_orders": matched_scope_orders,
                    "filled_rows": filled_rows,
                },
                flush=True,
            )
            return

        if not args.apply_only:
            print(f"[{datetime.now()}] prepare stage", flush=True)
            prepare_stage(mysql_engine)
            loaded = load_stage(oracle_engine, mysql_engine)
            print(f"[{datetime.now()}] stage ready rows={loaded}", flush=True)
        updated_rows, filled_rows, blank_rows, batch_count, processed_stage_rows = apply_backfill(
            mysql_engine,
            args.apply_batch_size,
        )
        print(
            {
                "apply_batch_size": args.apply_batch_size,
                "apply_batches": batch_count,
                "processed_stage_rows": processed_stage_rows,
                "updated_rows": updated_rows,
                "filled_rows": filled_rows,
                "blank_rows": blank_rows,
            },
            flush=True,
        )
    finally:
        oracle_engine.dispose()
        mysql_engine.dispose()


if __name__ == "__main__":
    main()