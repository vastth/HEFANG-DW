import argparse
import json
from datetime import datetime
from pathlib import Path

import pymysql

from config import MYSQL_CONFIG


def fetch_schema():
    schema_name = MYSQL_CONFIG.get("database")
    if not schema_name:
        raise RuntimeError("MYSQL_CONFIG.database is empty")

    conn = pymysql.connect(**MYSQL_CONFIG)
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME, ENGINE, TABLE_COMMENT
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME
                """,
                (schema_name,),
            )
            tables = cursor.fetchall()

            cursor.execute(
                """
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,
                       IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, EXTRA,
                       COLUMN_COMMENT, ORDINAL_POSITION
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                (schema_name,),
            )
            columns = cursor.fetchall()
    finally:
        conn.close()

    columns_by_table = {}
    for col in columns:
        columns_by_table.setdefault(col["TABLE_NAME"], []).append(
            {
                "column_name": col["COLUMN_NAME"],
                "data_type": col["DATA_TYPE"],
                "column_type": col["COLUMN_TYPE"],
                "is_nullable": col["IS_NULLABLE"],
                "column_default": col["COLUMN_DEFAULT"],
                "column_key": col["COLUMN_KEY"],
                "extra": col["EXTRA"],
                "column_comment": col["COLUMN_COMMENT"],
                "ordinal_position": col["ORDINAL_POSITION"],
            }
        )

    table_items = []
    for table in tables:
        name = table["TABLE_NAME"]
        table_items.append(
            {
                "table_name": name,
                "engine": table.get("ENGINE"),
                "table_comment": table.get("TABLE_COMMENT"),
                "columns": columns_by_table.get(name, []),
            }
        )

    snapshot = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "database": schema_name,
        "tables": table_items,
    }
    return snapshot


def main():
    parser = argparse.ArgumentParser(description="Export MySQL schema snapshot.")
    parser.add_argument(
        "--output",
        default="reports/snapshot_mysql_hefangdw_schema.json",
        help=(
            "Output JSON path (default: reports/snapshot_mysql_hefangdw_schema.json)"
        ),
    )
    args = parser.parse_args()

    snapshot = fetch_schema()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(snapshot, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    print(f"snapshot: {output_path}")
    print(f"tables: {len(snapshot['tables'])}")


if __name__ == "__main__":
    main()
