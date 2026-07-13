import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import connect_oracle


def _resolve_path(path_str: str) -> Path:
    output_path = Path(path_str)
    if not output_path.is_absolute():
        output_path = REPO_ROOT / output_path
    return output_path


def load_oracle_table_list(doc_path: Path):
    tables = []
    try:
        lines = doc_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return tables

    in_table = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("### 1.1 核心业务表清单"):
            in_table = True
            continue
        if in_table and stripped.startswith("---"):
            break
        if in_table and stripped.startswith("|") and "|" in stripped[1:]:
            parts = [p.strip() for p in stripped.strip("|").split("|")]
            if len(parts) >= 2 and parts[0].isdigit():
                name = parts[1]
                if name:
                    tables.append(name)
    return tables


def _format_oracle_type(row):
    data_type = row.get("DATA_TYPE")
    length = row.get("DATA_LENGTH")
    precision = row.get("DATA_PRECISION")
    scale = row.get("DATA_SCALE")

    if data_type in ("VARCHAR2", "CHAR", "NCHAR", "NVARCHAR2"):
        return f"{data_type}({length})" if length else data_type
    if data_type == "NUMBER":
        if precision is None:
            return "NUMBER"
        if scale is None:
            return f"NUMBER({precision})"
        return f"NUMBER({precision},{scale})"
    return data_type


def fetch_schema(schema_name: str):
    doc_path = REPO_ROOT / "docs" / "数据结构与映射手册.md"
    table_filter = {name.upper() for name in load_oracle_table_list(doc_path)}

    conn = connect_oracle()
    try:
        cursor = conn.cursor()
        if table_filter:
            placeholders = ",".join([":t" + str(i) for i in range(len(table_filter))])
            params = {"owner": schema_name}
            for idx, name in enumerate(sorted(table_filter)):
                params[f"t{idx}"] = name
            cursor.execute(
                f"""
                SELECT t.table_name, c.comments
                FROM all_tables t
                LEFT JOIN all_tab_comments c
                    ON c.owner = t.owner
                   AND c.table_name = t.table_name
                WHERE t.owner = :owner
                  AND t.table_name IN ({placeholders})
                ORDER BY t.table_name
                """,
                params,
            )
        else:
            cursor.execute(
                """
                SELECT t.table_name, c.comments
                FROM all_tables t
                LEFT JOIN all_tab_comments c
                    ON c.owner = t.owner
                   AND c.table_name = t.table_name
                WHERE t.owner = :owner
                ORDER BY t.table_name
                """,
                {"owner": schema_name},
            )
        tables = cursor.fetchall()

        if table_filter:
            placeholders = ",".join([":t" + str(i) for i in range(len(table_filter))])
            params = {"owner": schema_name}
            for idx, name in enumerate(sorted(table_filter)):
                params[f"t{idx}"] = name
            cursor.execute(
                f"""
                SELECT c.table_name,
                       c.column_name,
                       c.data_type,
                       c.data_length,
                       c.data_precision,
                       c.data_scale,
                       c.nullable,
                       c.data_default,
                       c.column_id,
                       cc.comments
                FROM all_tab_columns c
                LEFT JOIN all_col_comments cc
                    ON cc.owner = c.owner
                   AND cc.table_name = c.table_name
                   AND cc.column_name = c.column_name
                WHERE c.owner = :owner
                  AND c.table_name IN ({placeholders})
                ORDER BY c.table_name, c.column_id
                """,
                params,
            )
        else:
            cursor.execute(
                """
                SELECT c.table_name,
                       c.column_name,
                       c.data_type,
                       c.data_length,
                       c.data_precision,
                       c.data_scale,
                       c.nullable,
                       c.data_default,
                       c.column_id,
                       cc.comments
                FROM all_tab_columns c
                LEFT JOIN all_col_comments cc
                    ON cc.owner = c.owner
                   AND cc.table_name = c.table_name
                   AND cc.column_name = c.column_name
                WHERE c.owner = :owner
                ORDER BY c.table_name, c.column_id
                """,
                {"owner": schema_name},
            )
        columns = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    columns_by_table = {}
    for row in columns:
        record = {
            "TABLE_NAME": row[0],
            "COLUMN_NAME": row[1],
            "DATA_TYPE": row[2],
            "DATA_LENGTH": row[3],
            "DATA_PRECISION": row[4],
            "DATA_SCALE": row[5],
            "NULLABLE": row[6],
            "DATA_DEFAULT": row[7].strip() if row[7] else None,
            "COLUMN_ID": row[8],
            "COLUMN_COMMENT": row[9],
        }
        columns_by_table.setdefault(record["TABLE_NAME"], []).append(
            {
                "column_name": record["COLUMN_NAME"],
                "data_type": record["DATA_TYPE"],
                "column_type": _format_oracle_type(record),
                "is_nullable": record["NULLABLE"],
                "column_default": record["DATA_DEFAULT"],
                "column_comment": record["COLUMN_COMMENT"],
                "ordinal_position": record["COLUMN_ID"],
            }
        )

    table_items = []
    for table_name, comment in tables:
        table_items.append(
            {
                "table_name": table_name,
                "table_comment": comment,
                "columns": columns_by_table.get(table_name, []),
            }
        )

    snapshot = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "schema": schema_name,
        "tables": table_items,
    }
    return snapshot


def main():
    parser = argparse.ArgumentParser(
        description="导出 Oracle BOSNDS3 结构快照。"
    )
    parser.add_argument(
        "--schema",
        default="BOSNDS3",
        help="Oracle schema owner，默认 BOSNDS3",
    )
    parser.add_argument(
        "--output",
        default="reports/snapshot_oracle_bosnds3_schema.json",
        help="输出 JSON 路径，默认写入 reports/snapshot_oracle_bosnds3_schema.json",
    )
    args = parser.parse_args()

    if not args.schema:
        raise RuntimeError("Oracle schema is empty")

    snapshot = fetch_schema(args.schema.upper())
    output_path = _resolve_path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(snapshot, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )

    print(f"snapshot: {output_path}")
    print(f"tables: {len(snapshot['tables'])}")


if __name__ == "__main__":
    main()
