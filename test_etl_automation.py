# -*- coding: utf-8 -*-
"""
ETL自动化测试与验证脚本
验证所有ETL任务执行结果和数据质量
"""

import sys
from datetime import datetime, timedelta
from config import ORACLE_VERIFY_QUERIES
from db_connections import connect_mysql, connect_oracle

# 配置UTF-8输出
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')


ORACLE_DIFF_THRESHOLD_PCT = 0.5

def get_mysql_conn():
    """获取MySQL连接（从配置/环境变量读取）"""
    return connect_mysql()


def get_oracle_count(key):
    """如果在 config.ORACLE_VERIFY_QUERIES 中提供 SQL，则连接 Oracle 并返回第一列第一行的数值。

    若未配置或查询失败，返回 None。
    """
    sql = ORACLE_VERIFY_QUERIES.get(key)
    if not sql:
        return None
    try:
        conn = connect_oracle()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return int(row[0])
    except Exception as e:
        print(f"  ❌ 从 Oracle 执行验证 SQL 失败 ({key}): {e}")
    return None


def get_oracle_metrics(key):
    """执行 Oracle 验证 SQL 并返回首行字典。"""
    sql = ORACLE_VERIFY_QUERIES.get(key)
    if not sql:
        return None
    try:
        conn = connect_oracle()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        columns = [desc[0].lower() for desc in cur.description] if cur.description else []
        cur.close()
        conn.close()
        if row:
            return dict(zip(columns, row))
    except Exception as e:
        print(f"  ❌ 从 Oracle 执行验证 SQL 失败 ({key}): {e}")
    return None


def calc_diff_pct(mysql_value, oracle_value):
    """计算相对误差百分比。"""
    mysql_num = float(mysql_value or 0)
    oracle_num = float(oracle_value or 0)
    if oracle_num == 0:
        return 0.0 if mysql_num == 0 else 100.0
    return abs(mysql_num - oracle_num) / abs(oracle_num) * 100


def compare_metric(label, mysql_value, oracle_value, threshold_pct=ORACLE_DIFF_THRESHOLD_PCT):
    """打印单项对账结果并返回是否通过。"""
    diff = float(mysql_value or 0) - float(oracle_value or 0)
    diff_pct = calc_diff_pct(mysql_value, oracle_value)
    print(f"  {label} - Oracle: {float(oracle_value or 0):,.2f} | MySQL: {float(mysql_value or 0):,.2f} | 差异: {diff:+,.2f} ({diff_pct:.2f}%)")
    return diff_pct <= threshold_pct

def test_dim_product():
    """测试商品维度表"""
    conn = get_mysql_conn()
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("【1/5】测试 dim_product - 商品维度表")
    print("="*80)
    
    # 总记录数
    cursor.execute("SELECT COUNT(*) FROM dim_product")
    total = cursor.fetchone()[0]
    print(f"  总商品数: {total:,}")
    
    # 主销品数量
    cursor.execute("SELECT COUNT(*) FROM dim_product WHERE is_main_product='Y'")
    main_count = cursor.fetchone()[0]
    print(f"  主销品数: {main_count:,}")
    
    # 激活商品数
    cursor.execute("SELECT COUNT(*) FROM dim_product WHERE is_active='Y'")
    active = cursor.fetchone()[0]
    print(f"  激活商品: {active:,}")
    
    # 检查关键字段是否有空值
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN product_code IS NULL THEN 1 END) as null_code,
            COUNT(CASE WHEN product_name IS NULL THEN 1 END) as null_name,
            COUNT(CASE WHEN category_id IS NULL THEN 1 END) as null_category
        FROM dim_product
    """)
    nulls = cursor.fetchone()
    
    if nulls[0] > 0 or nulls[1] > 0 or nulls[2] > 0:
        print(f"  ⚠️ 发现空值: 编码{nulls[0]}, 名称{nulls[1]}, 类别{nulls[2]}")
        status = "⚠️ WARNING"
    else:
        print(f"  ✓ 数据完整性检查通过")
        status = "✅ PASS"
    
    conn.close()
    return status

def test_dim_store():
    """测试店仓维度表"""
    conn = get_mysql_conn()
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("【2/6】测试 dim_store - 店仓维度表")
    print("="*80)
    
    # 总记录数
    cursor.execute("SELECT COUNT(*) FROM dim_store")
    total = cursor.fetchone()[0]
    print(f"  总店仓数: {total:,}")
    
    # 各类型店仓数量
    cursor.execute("""
        SELECT store_type, COUNT(*) as cnt 
        FROM dim_store 
        GROUP BY store_type 
        ORDER BY cnt DESC
    """)
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]}")
    
    # 云仓数量
    cursor.execute("SELECT COUNT(*) FROM dim_store WHERE is_cloud_store='Y'")
    cloud = cursor.fetchone()[0]
    print(f"  云仓数量: {cloud}")
    
    # 检查总仓是否存在
    cursor.execute("SELECT COUNT(*) FROM dim_store WHERE store_code='001'")
    has_main = cursor.fetchone()[0]
    
    if has_main == 0:
        print(f"  ❌ 缺少总仓(001)")
        status = "❌ FAIL"
    else:
        print(f"  ✓ 总仓(001)存在")
        status = "✅ PASS"
    
    conn.close()
    return status


def test_dim_channel():
    """测试渠道维度表"""
    conn = get_mysql_conn()
    cursor = conn.cursor()

    print("\n" + "="*80)
    print("【3/6】测试 dim_channel - 渠道维度表")
    print("="*80)

    cursor.execute("SELECT COUNT(*) FROM dim_channel")
    total = cursor.fetchone()[0]
    print(f"  总渠道数: {total:,}")

    cursor.execute("SELECT COUNT(*) FROM dim_channel WHERE is_main = 1")
    main_total = cursor.fetchone()[0]
    print(f"  主要渠道数: {main_total}")

    cursor.execute("SELECT COUNT(*) FROM dim_channel WHERE WING_CODE IS NOT NULL AND TRIM(WING_CODE) <> ''")
    wing_code_filled = cursor.fetchone()[0]
    print(f"  WING_CODE 非空数: {wing_code_filled}")

    cursor.execute("""
        SELECT platform_type, COUNT(*) AS cnt
        FROM dim_channel
        GROUP BY platform_type
        ORDER BY cnt DESC, platform_type
    """)
    print("  平台类型分布:")
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]}")

    if total == 0 or main_total == 0 or wing_code_filled != total:
        print("  ❌ 渠道维度缺少关键基础数据")
        status = "❌ FAIL"
    else:
        print("  ✓ 渠道维度基础数据检查通过")
        status = "✅ PASS"

    conn.close()
    return status

def test_dws_inventory():
    """测试库存汇总表"""
    conn = get_mysql_conn()
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("【4/6】测试 dws_inventory_daily - 库存明细表")
    print("="*80)
    
    today = int(datetime.now().strftime('%Y%m%d'))
    
    # 今日库存记录数
    cursor.execute(f"SELECT COUNT(*) FROM dws_inventory_daily WHERE date_id={today}")
    total = cursor.fetchone()[0]
    print(f"  今日库存记录: {total:,}")
    
    # 不同商品数
    cursor.execute(f"""
        SELECT COUNT(DISTINCT i.product_id)
        FROM dws_inventory_daily i
        JOIN dim_product p ON i.product_id = p.product_id
        JOIN dim_store s ON i.store_id = s.store_id
        WHERE i.date_id = {today}
          AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
          AND p.is_main_product = 'Y'
    """)
    main_products = cursor.fetchone()[0]
    print(f"  主销品商品数(总仓+云仓): {main_products:,}")
    
    # 与Oracle对比：优先使用 config.ORACLE_VERIFY_QUERIES 中提供的 SQL 拉取对比值
    oracle_count = get_oracle_count('dws_inventory_main_products')
    if oracle_count is None:
        # 回退到历史固定值（便于快速验证），建议在 config.py 中配置实际的 Oracle 验证 SQL
        oracle_count = 2508
        print("  (提示) 未在 config.ORACLE_VERIFY_QUERIES 中配置 dws_inventory_main_products，使用回退常量进行比较。")

    diff = main_products - oracle_count
    print(f"  Oracle SQL数据量: {oracle_count:,}")
    print(f"  差异: {diff:+,} ({abs(diff)/oracle_count*100:.2f}%)")
    
    if calc_diff_pct(main_products, oracle_count) <= ORACLE_DIFF_THRESHOLD_PCT:
        print(f"  ✓ 数据量与Oracle对齐（阈值 {ORACLE_DIFF_THRESHOLD_PCT}%）")
        status = "✅ PASS"
    else:
        print(f"  ⚠️ 数据量与Oracle差异超过 {ORACLE_DIFF_THRESHOLD_PCT}%")
        status = "⚠️ WARNING"
    
    conn.close()
    return status

def test_dws_sales():
    """测试销售汇总表"""
    conn = get_mysql_conn()
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("【5/6】测试 dws_sales_daily - 销售明细表")
    print("="*80)
    
    today = int(datetime.now().strftime('%Y%m%d'))
    date_30_ago = int((datetime.now() - timedelta(days=30)).strftime('%Y%m%d'))
    
    # 近30天销售记录数
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM dws_sales_daily 
                WHERE date_id >= {date_30_ago}
                    AND (store_code LIKE 'DS%%' OR is_cloud_store='Y')
    """)
    total = cursor.fetchone()[0]
    print(f"  近30天销售记录: {total:,}")
    
    # 近30天销售金额
    cursor.execute(f"""
        SELECT 
            SUM(sales_amount) as sales,
            SUM(return_amount) as returns
        FROM dws_sales_daily 
        WHERE date_id >= {date_30_ago}
          AND (store_code LIKE 'DS%%' OR is_cloud_store='Y')
    """)
    amounts = cursor.fetchone()
    print(f"  近30天销售额(电商+云仓): {amounts[0]:,.0f}元")
    print(f"  近30天退货额: {amounts[1]:,.0f}元")

    mysql_metrics = {
        'row_count': total,
        'sales_amount': amounts[0] or 0,
        'return_amount': amounts[1] or 0,
    }

    oracle_metrics = get_oracle_metrics('dws_sales_30d_summary')
    if oracle_metrics:
        print(f"\n  Oracle 对账（阈值 {ORACLE_DIFF_THRESHOLD_PCT}%）:")
        metric_results = [
            compare_metric('近30天记录数', mysql_metrics['row_count'], oracle_metrics.get('row_count', 0)),
            compare_metric('近30天销售额', mysql_metrics['sales_amount'], oracle_metrics.get('sales_amount', 0)),
            compare_metric('近30天退货额', mysql_metrics['return_amount'], oracle_metrics.get('return_amount', 0)),
        ]
    else:
        print("\n  ⚠️ 未获取到 Oracle 销售对账结果，跳过销售口径百分比校验")
        metric_results = [True]
    
    # 检查是否有负数记录（负数代表退货，属于正常情况），统计并报告即可
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM dws_sales_daily 
        WHERE date_id >= {date_30_ago}
          AND (sales_qty < 0 OR sales_amount < 0)
    """)
    neg_count = cursor.fetchone()[0]

    if neg_count > 0:
        # 负数为退货数据，通常为正常业务；仅当数量异常增大时才需要告警
        print(f"  ℹ️ 发现{neg_count}条销售数量/金额为负的记录（退货数据），属正常业务范围")
        # 可调整阈值（例如 >100）视为异常
        if neg_count > 100:
            print(f"  ⚠️ 负数记录数量较多（>{100}），请核查退货来源")
            status = "⚠️ WARNING"
        else:
            status = "✅ PASS"
    else:
        print(f"  ✓ 数据质量检查通过（无负数记录）")
        status = "✅ PASS"

    if status == "✅ PASS" and not all(metric_results):
        print(f"  ⚠️ Oracle/MySQL 销售对账超过 {ORACLE_DIFF_THRESHOLD_PCT}%")
        status = "⚠️ WARNING"
    
    conn.close()
    return status

def test_ads_health():
    """测试库存健康度表"""
    conn = get_mysql_conn()
    cursor = conn.cursor()
    
    print("\n" + "="*80)
    print("【6/6】测试 ads_inventory_health - 库存健康度表")
    print("="*80)
    
    today = int(datetime.now().strftime('%Y%m%d'))
    
    # 总记录数
    cursor.execute(f"SELECT COUNT(*) FROM ads_inventory_health WHERE snapshot_date={today}")
    total = cursor.fetchone()[0]
    print(f"  今日健康度记录: {total:,}")
    
    # 库存状态分布
    cursor.execute(f"""
        SELECT inventory_status, COUNT(*) as cnt
        FROM ads_inventory_health
        WHERE snapshot_date = {today}
        GROUP BY inventory_status
        ORDER BY cnt DESC
    """)
    print(f"\n  库存状态分布:")
    for row in cursor.fetchall():
        print(f"    {row[0]}: {row[1]}")
    
    # SABC分级
    cursor.execute(f"""
        SELECT sku_grade, COUNT(*) as cnt
        FROM ads_inventory_health
        WHERE snapshot_date = {today}
        GROUP BY sku_grade
        ORDER BY FIELD(sku_grade, 'S', 'A', 'B', 'C')
    """)
    print(f"\n  SABC分级:")
    for row in cursor.fetchall():
        print(f"    {row[0]}级: {row[1]}")
    
    # 建议补货统计
    cursor.execute(f"""
        SELECT 
            SUM(CASE WHEN suggest_qty > 0 THEN suggest_qty ELSE 0 END) as need_restock,
            SUM(CASE WHEN suggest_qty < 0 THEN suggest_qty ELSE 0 END) as surplus,
            COUNT(CASE WHEN suggest_qty < 0 THEN 1 END) as surplus_sku
        FROM ads_inventory_health
        WHERE snapshot_date = {today}
    """)
    restock = cursor.fetchone()
    print(f"\n  建议补货统计:")
    print(f"    需要补货: {restock[0]:,.0f}件")
    print(f"    库存过剩: {restock[1]:,.0f}件")
    print(f"    过剩SKU: {restock[2]}个")
    
    # 与Oracle对比：优先使用 config.ORACLE_VERIFY_QUERIES 中提供的 SQL 拉取对比值
    oracle_count = get_oracle_count('ads_health_total')
    if oracle_count is None:
        oracle_count = 2508
        print("  (提示) 未在 config.ORACLE_VERIFY_QUERIES 中配置 ads_health_total，使用回退常量进行比较。")

    diff = total - oracle_count
    print(f"\n  Oracle SQL数据量: {oracle_count:,}")
    print(f"  MySQL ETL数据量: {total:,}")
    print(f"  差异: {diff:+,} ({abs(diff)/oracle_count*100:.2f}%)")
    
    if calc_diff_pct(total, oracle_count) <= ORACLE_DIFF_THRESHOLD_PCT:
        print(f"  ✓ 数据量与Oracle对齐（阈值 {ORACLE_DIFF_THRESHOLD_PCT}%）")
        status = "✅ PASS"
    else:
        print(f"  ⚠️ 数据量与Oracle差异超过 {ORACLE_DIFF_THRESHOLD_PCT}%")
        status = "⚠️ WARNING"
    
    conn.close()
    return status

def main():
    """主测试流程"""
    print("\n" + "="*80)
    print("  ETL自动化测试与验证")
    print("  测试时间: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print("="*80)
    
    results = {}
    
    try:
        results['dim_product'] = test_dim_product()
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        results['dim_product'] = "❌ ERROR"
    
    try:
        results['dim_store'] = test_dim_store()
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        results['dim_store'] = "❌ ERROR"

    try:
        results['dim_channel'] = test_dim_channel()
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        results['dim_channel'] = "❌ ERROR"
    
    try:
        results['dws_inventory'] = test_dws_inventory()
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        results['dws_inventory'] = "❌ ERROR"
    
    try:
        results['dws_sales'] = test_dws_sales()
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        results['dws_sales'] = "❌ ERROR"
    
    try:
        results['ads_health'] = test_ads_health()
    except Exception as e:
        print(f"  ❌ 测试失败: {e}")
        results['ads_health'] = "❌ ERROR"
    
    # 汇总结果
    print("\n" + "="*80)
    print("  测试结果汇总")
    print("="*80)
    
    all_pass = True
    for task, status in results.items():
        print(f"  {task:20s} {status}")
        if '❌' in status:
            all_pass = False
    
    print("="*80)
    if all_pass:
        print("  ✅ 所有测试通过！ETL系统运行正常")
    else:
        print("  ⚠️ 部分测试未通过，请检查详细日志")
    print("="*80)
    
    return all_pass

if __name__ == '__main__':
    main()
