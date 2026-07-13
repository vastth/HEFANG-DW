# -*- coding: utf-8 -*-
"""导出 ads_inventory_health 只读快照。"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
  sys.path.insert(0, str(REPO_ROOT))

from db_connections import create_mysql_engine


def _resolve_path(path_str: str | None, snapshot_date: int, suffix: str) -> Path:
  if path_str:
    output_path = Path(path_str)
    if not output_path.is_absolute():
      output_path = REPO_ROOT / output_path
    return output_path
  return REPO_ROOT / f"ads_inventory_health_{snapshot_date}{suffix}"


def _detect_format(output_path: Path) -> str:
  suffix = output_path.suffix.lower()
  if suffix == ".xlsx":
    return "excel"
  if suffix in {"", ".csv"}:
    return "csv"
  raise ValueError("--output 仅支持 .csv 或 .xlsx")


def _get_snapshot_date(engine, snapshot_date: int | None) -> int:
  if snapshot_date is not None:
    return snapshot_date
  with engine.connect() as conn:
    latest_date = conn.execute(
      text("SELECT MAX(snapshot_date) FROM ads_inventory_health")
    ).scalar()
  if latest_date is None:
    raise RuntimeError("ads_inventory_health 当前没有可导出的快照数据")
  return int(latest_date)


def export_ads(snapshot_date: int | None, output: str | None) -> Path:
  engine = create_mysql_engine()
  target_snapshot = _get_snapshot_date(engine, snapshot_date)

  sql = text(
    """
    SELECT
      snapshot_date, product_id, product_code, product_name, category_id, category_name,
      total_qty, warehouse_qty, cloud_qty, purchase_rem_qty,
      sales_qty_30d, sales_qty_7d, return_qty_30d, daily_avg_sales, daily_avg_sales_7d, sales_velocity,
      turnover_days, inventory_status, sku_grade, suggest_qty, etl_time
    FROM ads_inventory_health
    WHERE snapshot_date = :snapshot_date
    """
  )
  df = pd.read_sql(sql, engine, params={"snapshot_date": target_snapshot})

  output_path = _resolve_path(output, target_snapshot, ".csv")
  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_format = _detect_format(output_path)

  if output_format == "excel":
    df.to_excel(output_path, index=False)
  else:
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

  print(f"导出完成：{output_path}")
  print(f"快照日期：{target_snapshot}")
  print(f"记录数：{len(df)}")
  return output_path


def main():
  parser = argparse.ArgumentParser(description="导出 ads_inventory_health 快照数据。")
  parser.add_argument(
    "--snapshot-date",
    type=int,
    help="快照日期，格式 YYYYMMDD；不传则导出最新快照",
  )
  parser.add_argument(
    "--output",
    help="输出文件路径，支持 .csv 或 .xlsx；相对路径按仓库根目录解析",
  )
  args = parser.parse_args()
  export_ads(args.snapshot_date, args.output)


if __name__ == "__main__":
  main()
