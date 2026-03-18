# -*- coding: utf-8 -*-
"""通用只读数据库查询工具。"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from config import MYSQL_CONN_STR, ORACLE_CONFIG


READ_ONLY_BLOCKLIST = re.compile(
    r"\b(INSERT|UPDATE|DELETE|ALTER|DROP|TRUNCATE|CREATE|REPLACE|MERGE|GRANT|REVOKE|CALL|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)

SQL_TEMPLATES = {
    "mysql_sales_rank_7d": {
        "source": "mysql",
        "description": "最近 7 天销售排行（按销售额前 20）",
        "sql": """
            SELECT
                product_id,
                product_code,
                product_name,
                SUM(sales_qty) AS sales_qty,
                SUM(sales_amount) AS sales_amount,
                SUM(return_qty) AS return_qty,
                SUM(return_amount) AS return_amount
            FROM dws_sales_daily
            WHERE STR_TO_DATE(CAST(date_id AS CHAR), '%Y%m%d') >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            GROUP BY product_id, product_code, product_name
            ORDER BY sales_amount DESC
            LIMIT 20
        """,
    },
    "mysql_inventory_latest_summary": {
        "source": "mysql",
        "description": "最新库存快照汇总",
        "sql": """
            SELECT
                date_id AS snapshot_date,
                COUNT(*) AS row_count,
                SUM(total_qty) AS total_qty,
                SUM(warehouse_qty) AS warehouse_qty,
                SUM(cloud_qty) AS cloud_qty,
                SUM(sales_qty_30d) AS sales_qty_30d
            FROM dws_inventory_daily
            WHERE date_id = (SELECT MAX(date_id) FROM dws_inventory_daily)
            GROUP BY date_id
        """,
    },
    "mysql_ads_inventory_health_latest_sample": {
        "source": "mysql",
        "description": "最新 ads_inventory_health 样本（前 20 行）",
        "sql": """
            SELECT
                snapshot_date,
                product_id,
                product_code,
                product_name,
                category_name,
                total_qty,
                sales_qty_7d,
                inventory_status,
                sku_grade,
                suggest_qty
            FROM ads_inventory_health
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ads_inventory_health)
            ORDER BY product_id, product_code
            LIMIT 20
        """,
    },
    "oracle_retail_docs_7d": {
        "source": "oracle",
        "description": "最近 7 天 Oracle 零售单据统计",
        "sql": """
            SELECT
                BILLDATE,
                COUNT(*) AS doc_count,
                SUM(TOT_QTY) AS total_qty,
                SUM(TOT_AMT_ACTUAL) AS total_amount
            FROM M_RETAIL
            WHERE BILLDATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE) - 7, 'YYYYMMDD'))
            GROUP BY BILLDATE
            ORDER BY BILLDATE DESC
        """,
    },
}


def _strip_sql_comments(sql: str) -> str:
    without_block = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    without_line = re.sub(r"--.*?$", " ", without_block, flags=re.MULTILINE)
    return without_line


def _validate_read_only_sql(sql: str) -> str:
    cleaned = _strip_sql_comments(sql)
    normalized = re.sub(r"\s+", " ", cleaned).strip()
    if not normalized:
        raise ValueError("SQL 不能为空")

    trimmed = normalized.rstrip(";").strip()
    if ";" in trimmed:
        raise ValueError("只允许单条只读 SQL，禁止多语句执行")
    if not re.match(r"^(SELECT|WITH)\b", trimmed, flags=re.IGNORECASE):
        raise ValueError("仅允许以 SELECT 或 WITH 开头的只读查询")
    if READ_ONLY_BLOCKLIST.search(trimmed):
        raise ValueError("检测到非只读关键字，查询已拒绝执行")
    if re.search(r"\bINTO\s+OUTFILE\b", trimmed, flags=re.IGNORECASE):
        raise ValueError("检测到导出型写操作，查询已拒绝执行")
    return trimmed


def _build_oracle_engine():
    oracle_url = URL.create(
        "oracle+oracledb",
        username=ORACLE_CONFIG["user"],
        password=ORACLE_CONFIG["password"],
        host=ORACLE_CONFIG["host"],
        port=ORACLE_CONFIG["port"],
        database=ORACLE_CONFIG["service_name"],
    )
    return create_engine(oracle_url)


def _build_mysql_engine():
    return create_engine(MYSQL_CONN_STR)


def _parse_param(raw_value: str):
    lower_value = raw_value.lower()
    if lower_value == "null":
        return None
    if lower_value == "true":
        return True
    if lower_value == "false":
        return False
    if re.fullmatch(r"-?\d+", raw_value):
        return int(raw_value)
    if re.fullmatch(r"-?\d+\.\d+", raw_value):
        return float(raw_value)
    return raw_value


def _parse_params(param_items: list[str]) -> dict:
    params = {}
    for item in param_items:
        if "=" not in item:
            raise ValueError(f"参数格式错误：{item}，应为 key=value")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"参数名不能为空：{item}")
        params[key] = _parse_param(value.strip())
    return params


def _resolve_sql(source: str, sql: str | None, template_name: str | None) -> tuple[str, str]:
    if bool(sql) == bool(template_name):
        raise ValueError("--sql 与 --template 必须二选一")

    if template_name:
        template = SQL_TEMPLATES.get(template_name)
        if template is None:
            raise ValueError(f"未找到模板：{template_name}")
        if template["source"] != source:
            raise ValueError(
                f"模板 {template_name} 仅支持 {template['source']}，请调整 --source"
            )
        return _validate_read_only_sql(template["sql"]), template_name

    return _validate_read_only_sql(sql or ""), "custom"


def _resolve_output_path(output_kind: str, output_path: str | None, sql_name: str) -> Path | None:
    if output_kind == "table":
        return None

    target = Path(output_path) if output_path else REPO_ROOT / (
        f"query_result_{sql_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )
    if not target.is_absolute():
        target = REPO_ROOT / target

    suffix_map = {
        "json": ".json",
        "csv": ".csv",
        "excel": ".xlsx",
    }
    expected_suffix = suffix_map[output_kind]
    if target.suffix.lower() != expected_suffix:
        target = target.with_suffix(expected_suffix)
    return target


def _execute_query(engine, sql: str, params: dict) -> pd.DataFrame:
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        rows = result.fetchall()
        columns = list(result.keys())
    return pd.DataFrame(rows, columns=columns)


def _print_table(df: pd.DataFrame, preview_rows: int):
    if df.empty:
        print("查询成功，结果为空。")
        return

    preview = df.head(preview_rows)
    print(preview.to_string(index=False))
    if len(df) > preview_rows:
        print(f"\n仅展示前 {preview_rows} 行，实际共 {len(df)} 行。")
    else:
        print(f"\n共 {len(df)} 行。")


def _write_result(df: pd.DataFrame, output_kind: str, output_path: Path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_kind == "csv":
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
    elif output_kind == "excel":
        df.to_excel(output_path, index=False)
    elif output_kind == "json":
        json_text = df.to_json(orient="records", force_ascii=False, date_format="iso")
        output_path.write_text(
            json.dumps(json.loads(json_text), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    else:
        raise ValueError(f"不支持的输出格式：{output_kind}")


def _list_templates():
    print("可用模板：")
    for name, meta in sorted(SQL_TEMPLATES.items()):
        print(f"- {name} [{meta['source']}]：{meta['description']}")


def main():
    parser = argparse.ArgumentParser(
        description="通用只读数据库查询工具，支持 MySQL 与 Oracle。"
    )
    parser.add_argument(
        "--source",
        choices=["mysql", "oracle"],
        default="mysql",
        help="数据源类型，默认 mysql",
    )
    parser.add_argument("--sql", help="原始 SQL，只允许单条 SELECT/WITH 查询")
    parser.add_argument("--template", help="内置查询模板名称")
    parser.add_argument(
        "--param",
        action="append",
        default=[],
        metavar="key=value",
        help="查询参数，可重复传入多次",
    )
    parser.add_argument(
        "--output",
        choices=["table", "json", "csv", "excel"],
        default="table",
        help="输出格式，默认 table",
    )
    parser.add_argument(
        "--output-path",
        help="输出文件路径；相对路径按仓库根目录解析",
    )
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=20,
        help="table 输出时展示的预览行数，默认 20",
    )
    parser.add_argument(
        "--list-templates",
        action="store_true",
        help="列出全部内置模板并退出",
    )
    args = parser.parse_args()

    if args.list_templates:
        _list_templates()
        return

    if args.preview_rows <= 0:
        raise ValueError("--preview-rows 必须大于 0")

    params = _parse_params(args.param)
    sql, sql_name = _resolve_sql(args.source, args.sql, args.template)
    output_path = _resolve_output_path(args.output, args.output_path, sql_name)

    engine = _build_mysql_engine() if args.source == "mysql" else _build_oracle_engine()
    df = _execute_query(engine, sql, params)

    if args.output == "table":
        _print_table(df, args.preview_rows)
        return

    if output_path is None:
        raise RuntimeError("输出路径解析失败")

    _write_result(df, args.output, output_path)
    print(f"查询完成，共 {len(df)} 行。")
    print(f"结果已写入：{output_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"执行失败：{exc}")
        sys.exit(1)