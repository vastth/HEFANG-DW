import pymysql
import os

# 连接MySQL
conn = pymysql.connect(
    host=os.getenv('MYSQL_HOST', 'localhost'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER', 'dba'),
    password=os.getenv('MYSQL_PASSWORD'),
    database=os.getenv('MYSQL_DATABASE', 'hefang_dw')
)

cursor = conn.cursor()

# 查询当前数据分布
sql = """
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN COALESCE(total_qty,0) > 0 OR COALESCE(sales_amt_30d,0) > 0 THEN 1 END) as with_inv_or_sales,
    COUNT(CASE WHEN COALESCE(total_qty,0) = 0 AND COALESCE(sales_amt_30d,0) = 0 THEN 1 END) as zero_both,
    COUNT(CASE WHEN inventory_status = '停售' THEN 1 END) as stopped
FROM ads_inventory_health 
WHERE snapshot_date = 20260120
"""

cursor.execute(sql)
row = cursor.fetchone()

print("=" * 70)
print("MySQL ads_inventory_health 当前数据分布")
print("=" * 70)
print(f"总记录数:           {row[0]:,}")
print(f"有库存或销售:       {row[1]:,}")
print(f"零库存零销售:       {row[2]:,}")
print(f"停售状态:           {row[3]:,}")
print()

# 查询主销品总数
sql2 = "SELECT COUNT(*) FROM dim_product WHERE is_main_product = 'Y'"
cursor.execute(sql2)
main_products = cursor.fetchone()[0]
print(f"dim_product主销品总数: {main_products:,}")
print()

# 对比分析
print("=" * 70)
print("数据差异分析")
print("=" * 70)
print(f"Oracle SQL数据量:    2,508")
print(f"MySQL ETL数据量:     {row[0]:,}")
print(f"差异:                {row[0] - 2508:,} 条")
print()

if row[0] > 2508:
    print("⚠️  MySQL数据量 > Oracle数据量")
    print("   可能原因: Oracle SQL有过滤条件，MySQL移除了过滤")
    print()
    print(f"   如果恢复 '有库存或销售' 过滤:")
    print(f"   预计数据量: {row[1]:,} 条")
    print(f"   与Oracle差异: {row[1] - 2508:,} 条")

conn.close()
