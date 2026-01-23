# export_ads.py
import pandas as pd
from sqlalchemy import create_engine
from config import MYSQL_CONN_STR  # 使用仓库已有配置

snapshot = 20260120  # 修改为目标日期
engine = create_engine(MYSQL_CONN_STR)

sql = f"""
SELECT
  snapshot_date, product_id, product_code, product_name, category_id, category_name,
  total_qty, warehouse_qty, cloud_qty, purchase_rem_qty,
  sales_qty_30d, sales_qty_7d, return_qty_30d, daily_avg_sales, daily_avg_sales_7d, sales_velocity,
  turnover_days, inventory_status, sku_grade, suggest_qty, etl_time
FROM ads_inventory_health
WHERE snapshot_date = {snapshot}
"""
df = pd.read_sql(sql, engine)

out_csv = f"ads_inventory_health_{snapshot}.csv"
df.to_csv(out_csv, index=False, encoding='utf-8-sig')
print("导出完成：", out_csv)
