import pymysql
import os

conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER', 'dba'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE', 'hefang_dw')
)

cursor = conn.cursor()

print("=" * 80)
print("检查 dws_inventory_daily 数据覆盖情况")
print("=" * 80)

# 1. dws_inventory_daily 有多少个主销品商品有记录（去重）
sql1 = """
SELECT COUNT(DISTINCT i.product_id)
FROM dws_inventory_daily i
JOIN dim_product p ON i.product_id = p.product_id
JOIN dim_store s ON i.store_id = s.store_id
WHERE i.date_id = 20260120
  AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
  AND p.is_main_product = 'Y'
"""
cursor.execute(sql1)
inv_count = cursor.fetchone()[0]
print(f"dws_inventory_daily中有记录的主销品数: {inv_count:,}")

# 2. 总记录数（按store展开）
sql2 = """
SELECT COUNT(*)
FROM dws_inventory_daily i
JOIN dim_product p ON i.product_id = p.product_id
JOIN dim_store s ON i.store_id = s.store_id
WHERE i.date_id = 20260120
  AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
  AND p.is_main_product = 'Y'
"""
cursor.execute(sql2)
total_records = cursor.fetchone()[0]
print(f"总记录数(按store展开):                 {total_records:,}")

# 3. dim_product 主销品总数
sql3 = "SELECT COUNT(*) FROM dim_product WHERE is_main_product = 'Y'"
cursor.execute(sql3)
total_main = cursor.fetchone()[0]
print(f"dim_product主销品总数:                {total_main:,}")

print()
print("=" * 80)
print("差异分析")
print("=" * 80)
print(f"Oracle SQL数据量:                     2,508")
print(f"dws_inventory_daily有记录商品数:      {inv_count:,}")
print(f"差异:                                 {abs(inv_count - 2508):,}")
print()
print(f"如果MySQL改为以dws_inventory_daily为主表（类似Oracle）：")
print(f"  预计数据量: {inv_count:,}")
print(f"  与Oracle差异: {abs(inv_count - 2508):,} ({abs(inv_count - 2508)/2508*100:.1f}%)")

conn.close()
