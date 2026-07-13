# -*- coding: utf-8 -*-
"""导出 HEFANG 复刻 Retail Toy 看板所需的 Oracle 只读数据源。"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db_connections import create_oracle_engine


DEFAULT_OUTPUT_DIR = Path(
    r"d:\tianhao\Documents\我的 Tableau 存储库\工作簿\#VOTD Sales Dashboard (Retail Toy Store)_v2025.3\HEFANG复刻_data_source"
)
DEFAULT_MONTHS_BACK = 24
DEFAULT_STORE_PREFIX = "RT%"
SALES_CHUNK_SIZE = 50000


@dataclass(frozen=True)
class ExportWindow:
    start_date: int
    end_date: int


def _shift_month(anchor: date, months_delta: int) -> date:
    month_index = anchor.year * 12 + (anchor.month - 1) + months_delta
    target_year = month_index // 12
    target_month = month_index % 12 + 1
    return date(target_year, target_month, 1)


def _default_window(months_back: int) -> ExportWindow:
    month_anchor = date.today().replace(day=1)
    start_dt = _shift_month(month_anchor, -(months_back - 1))
    end_dt = date.today()
    return ExportWindow(
        start_date=int(start_dt.strftime("%Y%m%d")),
        end_date=int(end_dt.strftime("%Y%m%d")),
    )


def _resolve_output_dir(path_str: str | None) -> Path:
    if not path_str:
        return DEFAULT_OUTPUT_DIR
    output_dir = Path(path_str)
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir
    return output_dir


STORES_SQL = text(
    """
    SELECT DISTINCT
        s.ID AS store_id,
        s.CODE AS store_code,
        s.NAME AS store_name,
        COALESCE(c.NAME, '未知城市') AS store_city,
        COALESCE(a.NAME, '未分区') AS store_location,
        CASE
            WHEN s.CODE LIKE 'RT%' THEN '线下门店'
            WHEN s.CODE LIKE 'DS%' THEN '电商'
            WHEN s.CODE = '001' THEN '总仓'
            ELSE '其他'
        END AS store_type,
        NVL(s.ISACTIVE, 'N') AS is_active
    FROM BOSNDS3.C_STORE s
    LEFT JOIN BOSNDS3.C_CITY c
        ON s.C_CITY_ID = c.ID
    LEFT JOIN BOSNDS3.C_AREA a
        ON s.C_AREA_ID = a.ID
    WHERE s.CODE LIKE :store_prefix
      AND EXISTS (
        SELECT 1
        FROM BOSNDS3.M_RETAIL r
        WHERE r.C_STORE_ID = s.ID
          AND r.BILLDATE BETWEEN :start_date AND :end_date
          AND r.ISACTIVE = 'Y'
          AND r.STATUS = 2
      )
    ORDER BY s.ID
    """
)


PRODUCTS_SQL = text(
    """
    SELECT DISTINCT
        p.ID AS product_id,
        p.NAME AS product_code,
        p.VALUE AS product_name,
        COALESCE(d4.ATTRIBNAME, '未分类') AS product_category,
        COALESCE(d1.ATTRIBNAME, '') AS brand_name,
        COALESCE(d5.ATTRIBNAME, '') AS property_name,
        COALESCE(d6.ATTRIBNAME, '') AS series_name,
        NVL(p.PRECOST, 0) AS product_cost,
        NVL(p.PRICELIST, 0) AS product_price,
        NVL(p.ISACTIVE, 'N') AS is_active
    FROM BOSNDS3.M_PRODUCT p
    LEFT JOIN BOSNDS3.M_DIM d1
        ON p.M_DIM1_ID = d1.ID
    LEFT JOIN BOSNDS3.M_DIM d4
        ON p.M_DIM4_ID = d4.ID
    LEFT JOIN BOSNDS3.M_DIM d5
        ON p.M_DIM5_ID = d5.ID
    LEFT JOIN BOSNDS3.M_DIM d6
        ON p.M_DIM6_ID = d6.ID
    WHERE EXISTS (
        SELECT 1
        FROM BOSNDS3.M_RETAILITEM ri
        JOIN BOSNDS3.M_RETAIL r
            ON ri.M_RETAIL_ID = r.ID
        JOIN BOSNDS3.C_STORE s
            ON r.C_STORE_ID = s.ID
        WHERE ri.M_PRODUCT_ID = p.ID
          AND r.BILLDATE BETWEEN :start_date AND :end_date
          AND r.ISACTIVE = 'Y'
          AND r.STATUS = 2
          AND ri.ISACTIVE = 'Y'
          AND s.CODE LIKE :store_prefix
    )
    ORDER BY p.ID
    """
)


SALES_SQL = text(
    """
    SELECT
        r.ID AS sale_id,
        ri.ID AS retail_item_id,
        TO_CHAR(TO_DATE(TO_CHAR(r.BILLDATE), 'YYYYMMDD'), 'YYYY-MM-DD') AS sales_date,
        r.C_STORE_ID AS store_id,
        ri.M_PRODUCT_ID AS product_id,
        NVL(ri.QTY, 0) AS units,
        NVL(ri.TOT_AMT_ACTUAL, 0) AS line_actual_amt,
        NVL(ri.TOT_AMT_LIST, 0) AS line_list_amt,
        NVL(ri.PRICEACTUAL, 0) AS price_actual,
        NVL(ri.PRICELIST, 0) AS price_list,
        NVL(ri.DISCOUNT, 0) AS discount_rate,
        NVL(r.STATUS, 0) AS retail_status,
        NVL(r.ISACTIVE, 'N') AS retail_isactive
    FROM BOSNDS3.M_RETAILITEM ri
    JOIN BOSNDS3.M_RETAIL r
        ON ri.M_RETAIL_ID = r.ID
    JOIN BOSNDS3.C_STORE s
        ON r.C_STORE_ID = s.ID
    WHERE r.BILLDATE BETWEEN :start_date AND :end_date
      AND r.ISACTIVE = 'Y'
      AND r.STATUS = 2
      AND ri.ISACTIVE = 'Y'
      AND s.CODE LIKE :store_prefix
    ORDER BY r.BILLDATE, r.ID, ri.ORDERNO, ri.ID
    """
)


def _write_calendar_csv(output_dir: Path, window: ExportWindow) -> int:
    start_text = str(window.start_date)
    end_text = str(window.end_date)
    dates = pd.date_range(
        start=f"{start_text[:4]}-{start_text[4:6]}-{start_text[6:8]}",
        end=f"{end_text[:4]}-{end_text[4:6]}-{end_text[6:8]}",
        freq="D",
    )
    df = pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "year": dates.year,
            "month_num": dates.month,
            "month_start": dates.to_period("M").to_timestamp().strftime("%Y-%m-%d"),
            "month_label": dates.strftime("%Y-%m"),
        }
    )
    df.to_csv(output_dir / "calendar.csv", index=False, encoding="utf-8-sig")
    return len(df)


def _write_dataframe_csv(df: pd.DataFrame, output_path: Path) -> int:
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    return len(df)


def _export_sales(engine, output_path: Path, params: dict[str, object]) -> int:
    total_rows = 0
    first_chunk = True
    for chunk in pd.read_sql(SALES_SQL, engine, params=params, chunksize=SALES_CHUNK_SIZE):
        if "sales_date" in chunk.columns:
            chunk = chunk.rename(columns={"sales_date": "date"})
        chunk.to_csv(
            output_path,
            index=False,
            encoding="utf-8-sig" if first_chunk else "utf-8",
            mode="w" if first_chunk else "a",
            header=first_chunk,
            quoting=csv.QUOTE_MINIMAL,
        )
        total_rows += len(chunk)
        first_chunk = False
    return total_rows


def _write_readme(output_dir: Path, window: ExportWindow, counts: dict[str, int]) -> None:
    readme_text = f"""# HEFANG 复刻 Retail Toy 数据源

导出窗口：{window.start_date} - {window.end_date}

文件说明：
- calendar.csv：日历表，供月/日趋势和月份参数使用
- products.csv：商品维度，来自 Oracle M_PRODUCT + M_DIM
- stores.csv：门店维度，来自 Oracle C_STORE + C_CITY + C_AREA
- sales.csv：销售明细，来自 Oracle M_RETAIL + M_RETAILITEM

字段映射说明：
- store_city：直接取 C_CITY.NAME
- store_location：当前复刻阶段用 C_AREA.NAME 代替模板里的位置类型，作为稳定的门店分组维度
- sale_id：取 M_RETAIL.ID，保留订单级标识
- retail_item_id：保留行级明细主键，便于后续排查
- units：取 M_RETAILITEM.QTY，可包含退货负数
- line_actual_amt / line_list_amt：保留真实金额，后续在 Tableau 中可替换模板原始的 price * units 逻辑

导出行数：
- calendar.csv：{counts['calendar']}
- products.csv：{counts['products']}
- stores.csv：{counts['stores']}
- sales.csv：{counts['sales']}
"""
    (output_dir / "README.md").write_text(readme_text, encoding="utf-8")


def export_source(output_dir: Path, window: ExportWindow, store_prefix: str) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    params = {
        "start_date": window.start_date,
        "end_date": window.end_date,
        "store_prefix": store_prefix,
    }

    engine = create_oracle_engine()
    try:
        stores_df = pd.read_sql(STORES_SQL, engine, params=params)
        products_df = pd.read_sql(PRODUCTS_SQL, engine, params=params)

        counts = {
            "calendar": _write_calendar_csv(output_dir, window),
            "stores": _write_dataframe_csv(stores_df, output_dir / "stores.csv"),
            "products": _write_dataframe_csv(products_df, output_dir / "products.csv"),
            "sales": _export_sales(engine, output_dir / "sales.csv", params),
        }
        _write_readme(output_dir, window, counts)
        return counts
    finally:
        engine.dispose()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="导出 HEFANG 复刻 Retail Toy 看板所需的 Oracle 数据源。")
    parser.add_argument("--output-dir", help="输出目录；默认导到 HEFANG复刻.twb 同级目录。")
    parser.add_argument("--start-date", type=int, help="开始日期，格式 YYYYMMDD。")
    parser.add_argument("--end-date", type=int, help="结束日期，格式 YYYYMMDD。")
    parser.add_argument("--months-back", type=int, default=DEFAULT_MONTHS_BACK, help="未传开始日期时，默认回溯月份数。")
    parser.add_argument("--store-prefix", default=DEFAULT_STORE_PREFIX, help="门店编码前缀过滤，默认 RT%%。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_date and args.end_date:
        window = ExportWindow(start_date=args.start_date, end_date=args.end_date)
    elif args.start_date or args.end_date:
        raise ValueError("--start-date 和 --end-date 必须同时传，或都不传。")
    else:
        window = _default_window(args.months_back)

    output_dir = _resolve_output_dir(args.output_dir)
    counts = export_source(output_dir, window, args.store_prefix)

    print(f"导出完成：{output_dir}")
    print(f"时间窗口：{window.start_date} - {window.end_date}")
    for name in ("calendar", "products", "stores", "sales"):
        print(f"{name}.csv 行数：{counts[name]}")


if __name__ == "__main__":
    main()