# -*- coding: utf-8 -*-
"""
何方珠宝 - 库存健康度计算（优化版 v2.0）
在MySQL内基于dws层计算ads_inventory_health

优化内容（2026-01-19 口径冻结会议）：
1. 建议补货公式：加入退货扣减 + 采购欠数扣减
2. 新增字段：return_qty_30d（近30天退货）、purchase_rem_qty（采购欠数）
3. SABC分级：S<30%, A<70%, B<90%, C>=90%
4. 新增销售加速度：sales_velocity = 7天日均 / 30天日均

策略：每日重新计算
"""

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging

from config import MYSQL_CONN_STR

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def ensure_table_columns(engine):
    """确保ads_inventory_health表有新增的字段"""
    
    new_columns = [
        ("return_qty_30d", "INT DEFAULT 0 COMMENT '近30天退货数量'"),
        ("purchase_rem_qty", "INT DEFAULT 0 COMMENT '采购欠数/在途库存'"),
        ("sales_velocity", "DECIMAL(5,2) DEFAULT NULL COMMENT '销售加速度(7天日均/30天日均)'"),
        ("daily_avg_sales_7d", "DECIMAL(10,2) DEFAULT 0 COMMENT '近7天日均销量'"),
        ("status_priority", "INT DEFAULT NULL COMMENT '库存状态优先级(1紧急缺货->6停售)'"),
        ("created_at", "DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'"),
    ]
    
    with engine.connect() as conn:
        for col_name, col_def in new_columns:
            try:
                # 检查字段是否存在
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM information_schema.COLUMNS 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = 'ads_inventory_health' 
                    AND COLUMN_NAME = '{col_name}'
                """))
                exists = result.fetchone()[0] > 0
                
                if not exists:
                    logger.info(f"添加新字段: {col_name}")
                    conn.execute(text(f"ALTER TABLE ads_inventory_health ADD COLUMN {col_name} {col_def}"))
                    conn.commit()
            except Exception as e:
                logger.warning(f"添加字段 {col_name} 时出错（可能已存在）: {e}")


def calculate_inventory_health():
    """计算库存健康度（优化版）"""
    
    logger.info("连接MySQL数据库...")
    engine = create_engine(MYSQL_CONN_STR)
    
    # 确保表有新字段
    ensure_table_columns(engine)
    
    today = int(datetime.now().strftime('%Y%m%d'))
    date_30_ago = int((datetime.now() - timedelta(days=30)).strftime('%Y%m%d'))
    date_7_ago = int((datetime.now() - timedelta(days=7)).strftime('%Y%m%d'))
    
    # 优化后的大SQL：一次性计算所有指标
    sql = f"""
     INSERT INTO ads_inventory_health 
     (snapshot_date, product_id, product_code, product_name, category_id, category_name,
      property_id, property_name, series_id, series_name, price_list, total_qty, warehouse_qty, cloud_qty,
          purchase_rem_qty, sales_qty_30d, sales_amt_30d, sales_qty_7d, return_qty_30d,
      return_amount_30d, daily_avg_sales, daily_avg_sales_7d, sales_velocity,
      turnover_days, inventory_status, sku_grade, suggest_qty, status_priority, etl_time, created_at)
    
    SELECT
        {today} AS snapshot_date,
        p.product_id,
        p.product_code,
        p.product_name,
        p.category_id,
        p.category_name,
        p.property_id,
        p.property_name,
        p.series_id,
        p.series_name,
        p.price_list,
        
        -- 库存数量
        COALESCE(inv.total_qty, 0) AS total_qty,
        COALESCE(inv.warehouse_qty, 0) AS warehouse_qty,
        COALESCE(inv.cloud_qty, 0) AS cloud_qty,
        
        -- ⭐新增：采购欠数（在途库存）
        COALESCE(inv.purchase_rem_qty, 0) AS purchase_rem_qty,
        
        -- 销售数量
        COALESCE(sales.sales_qty_30d, 0) AS sales_qty_30d,
        -- ⭐新增：近30天销售金额（来自 dws_sales_daily.sales_amount）
        COALESCE(sales.sales_amt_30d, 0) AS sales_amt_30d,
        COALESCE(sales.sales_qty_7d, 0) AS sales_qty_7d,
        
        -- ⭐新增：近30天退货数量
        COALESCE(sales.return_qty_30d, 0) AS return_qty_30d,
        -- （可选）近30天退货金额
        COALESCE(sales.return_amount_30d, 0) AS return_amount_30d,
        
        -- 日均销量（30天）
        ROUND(COALESCE(sales.sales_qty_30d, 0) / 30, 2) AS daily_avg_sales,
        
        -- ⭐新增：日均销量（7天）
        ROUND(COALESCE(sales.sales_qty_7d, 0) / 7, 2) AS daily_avg_sales_7d,
        
        -- ⭐新增：销售加速度 = 7天日均 / 30天日均
        CASE 
            WHEN COALESCE(sales.sales_qty_30d, 0) = 0 THEN NULL
            ELSE ROUND((COALESCE(sales.sales_qty_7d, 0) / 7) / (COALESCE(sales.sales_qty_30d, 0) / 30), 2)
        END AS sales_velocity,
        
        -- 周转天数
        CASE 
            WHEN COALESCE(sales.sales_qty_30d, 0) = 0 THEN 9999
            ELSE ROUND(COALESCE(inv.total_qty, 0) / (COALESCE(sales.sales_qty_30d, 0) / 30), 1)
        END AS turnover_days,
        
        -- 库存状态
        CASE
            WHEN COALESCE(inv.total_qty, 0) > 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN '滞销'
            WHEN COALESCE(inv.total_qty, 0) = 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN '停售'
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 30 THEN '紧急缺货'
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 70 THEN '需补货'
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) <= 90 THEN '正常'
            ELSE '库存过高'
        END AS inventory_status,
        
        -- SKU分级（先设为C，后续update_sku_grade函数会更新）
        'C' AS sku_grade,
        
        -- ⭐优化：建议补货数量 = (90天目标 - 当前周转天数) × 日均销量 - 退货 - 采购欠数
        -- ⭐ 修改：移除GREATEST(0,...)，允许负数表示库存过剩（与Oracle逻辑一致）
        CASE
            WHEN COALESCE(sales.sales_qty_30d, 0) = 0 THEN 0
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) >= 90 THEN 0
            ELSE ROUND(
                (90 - COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0)) 
                * (COALESCE(sales.sales_qty_30d, 0) / 30)
                - COALESCE(sales.return_qty_30d, 0)  -- 扣减退货（预计会返回仓库）
                - COALESCE(inv.purchase_rem_qty, 0)  -- 扣减采购欠数（在途库存）
            , 0)
        END AS suggest_qty,
        
        -- 库存状态优先级（1最高，6最低）
        CASE
            WHEN COALESCE(inv.total_qty, 0) > 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN 5  -- 滞销
            WHEN COALESCE(inv.total_qty, 0) = 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN 6  -- 停售
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 30 THEN 1  -- 紧急缺货
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 70 THEN 2  -- 需补货
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) <= 90 THEN 3 -- 正常
            ELSE 4  -- 库存过高
        END AS status_priority,
        
        NOW() AS etl_time,
        NOW() AS created_at
        
    -- ⚠️ 修改：改为以库存表为主表（与Oracle SQL逻辑一致）
    -- Oracle SQL: FROM stock st LEFT JOIN sales sa
    -- MySQL ETL: FROM inv_base LEFT JOIN dim_product LEFT JOIN sales
    FROM (
        -- 库存汇总（总仓+云仓，含采购欠数）- 作为主表
        SELECT
            i.product_id,
            SUM(i.qty) AS total_qty,
            SUM(CASE WHEN s.store_code = '001' THEN i.qty ELSE 0 END) AS warehouse_qty,
            SUM(CASE WHEN s.is_cloud_store = 'Y' THEN i.qty ELSE 0 END) AS cloud_qty,
            SUM(COALESCE(i.qtypurchaserem, 0)) AS purchase_rem_qty
        FROM dws_inventory_daily i
        LEFT JOIN dim_store s ON i.store_id = s.store_id
        WHERE i.date_id = {today}
            AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
        GROUP BY i.product_id
    ) inv
    
    -- 关联商品维度
    LEFT JOIN dim_product p ON inv.product_id = p.product_id
    
    -- 销售汇总（含退货数量）
    LEFT JOIN (
        SELECT
            product_id,
            -- 销售数量（正单）
            SUM(sales_qty) AS sales_qty_30d,
            -- 近30天销售金额（使用 dws_sales_daily.sales_amount 汇总，避免 qty*price_list 估算误差）
            SUM(sales_amount) AS sales_amt_30d,
            SUM(CASE WHEN date_id >= {date_7_ago} THEN sales_qty ELSE 0 END) AS sales_qty_7d,
            -- ⭐新增：退货数量/退货金额
            SUM(return_qty) AS return_qty_30d,
            SUM(return_amount) AS return_amount_30d
        FROM dws_sales_daily
        WHERE date_id >= {date_30_ago}
            -- ⭐按口径：电商+云仓门店
            AND (store_code LIKE 'DS%%' OR is_cloud_store = 'Y')
        GROUP BY product_id
    ) sales ON inv.product_id = sales.product_id
    
    WHERE p.is_main_product = 'Y'
        -- ⚠️ 注意：现在以库存表为主，自动只包含dws_inventory_daily中有记录的商品
        --         这与Oracle SQL逻辑完全一致（FROM stock st LEFT JOIN sales sa）
    """
    
    # 先清空当天数据
    logger.info(f"清空当天数据（{today}）...")
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM ads_inventory_health WHERE snapshot_date = {today}"))
    
    # 执行计算
    logger.info("执行库存健康度计算...")
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    
    # 查询写入记录数
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM ads_inventory_health WHERE snapshot_date = {today}"))
        count = result.fetchone()[0]
    
    logger.info(f"计算完成，共 {count} 条记录")
    engine.dispose()
    
    return count


def update_sku_grade():
    """
    更新SKU分级（SABC分类）
    
    分级标准：
    - S级：累计销售额占比 < 30%（爆款）
    - A级：累计销售额占比 30% - 70%（核心款）
    - B级：累计销售额占比 70% - 90%（常规款）
    - C级：累计销售额占比 >= 90% + 无销售（长尾/滞销）
    """
    
    logger.info("开始计算SABC分级（S<30%, A<70%, B<90%, C>=90%）...")
    engine = create_engine(MYSQL_CONN_STR)
    today = int(datetime.now().strftime('%Y%m%d'))
    
    # 使用单条 MySQL SQL（窗口函数）批量计算 sales_rank / sales_ratio / cumulative_ratio / sku_grade
    logger.info("使用 MySQL 窗口函数批量计算分级与排名...")
    sql_update = f"""
    UPDATE ads_inventory_health a
    JOIN (
        SELECT product_id, sales_amt_30d, total_sales, cum_sales, sales_rank FROM (
            SELECT
                product_id,
                COALESCE(sales_amt_30d, 0) AS sales_amt_30d,
                SUM(COALESCE(sales_amt_30d,0)) OVER () AS total_sales,
                SUM(COALESCE(sales_amt_30d,0)) OVER (ORDER BY COALESCE(sales_amt_30d,0) DESC, product_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sales,
                ROW_NUMBER() OVER (ORDER BY COALESCE(sales_amt_30d,0) DESC, product_id) AS sales_rank
            FROM ads_inventory_health
            WHERE snapshot_date = {today}
        ) t
    ) r ON a.product_id = r.product_id AND a.snapshot_date = {today}
    SET
        a.sales_rank = r.sales_rank,
        a.sales_ratio = ROUND(r.sales_amt_30d / NULLIF(r.total_sales, 0) * 100, 2),
        a.cumulative_ratio = ROUND(r.cum_sales / NULLIF(r.total_sales, 0) * 100, 2),
        a.sku_grade = CASE
            WHEN r.total_sales = 0 OR r.sales_amt_30d = 0 THEN 'C'
            WHEN (r.cum_sales - r.sales_amt_30d) / r.total_sales < 0.30 THEN 'S'
            WHEN r.cum_sales / r.total_sales <= 0.70 THEN 'A'
            WHEN r.cum_sales / r.total_sales <= 0.90 THEN 'B'
            ELSE 'C' END
    """

    with engine.connect() as conn:
        conn.execute(text(sql_update))
        conn.commit()

    # 计算销售趋势文本（基于 sales_velocity）
    sql_trend = f"""
    UPDATE ads_inventory_health
    SET sales_trend = CASE
        WHEN sales_qty_30d = 0 THEN '无销售'
        WHEN sales_velocity >= 1.3 THEN '快速上升'
        WHEN sales_velocity >= 1.0 THEN '稳定'
        WHEN sales_velocity >= 0.7 THEN '降温'
        ELSE '快速下滑' END
    WHERE snapshot_date = {today}
    """
    with engine.connect() as conn:
        conn.execute(text(sql_trend))
        conn.commit()

    # 统计分级结果
    sql_counts = f"SELECT sku_grade, COUNT(*) FROM ads_inventory_health WHERE snapshot_date = {today} GROUP BY sku_grade"
    with engine.connect() as conn:
        rows = conn.execute(text(sql_counts)).fetchall()
    counts = {r[0]: r[1] for r in rows}
    logger.info(f"分级完成：S类{counts.get('S',0)}个，A类{counts.get('A',0)}个，B类{counts.get('B',0)}个，C类{counts.get('C',0)}个")
    engine.dispose()


def print_summary():
    """打印今日汇总统计"""
    
    logger.info("生成今日汇总...")
    engine = create_engine(MYSQL_CONN_STR)
    today = int(datetime.now().strftime('%Y%m%d'))
    
    # 库存状态分布
    sql_status = f"""
    SELECT inventory_status, COUNT(*) AS sku_count, SUM(total_qty) AS total_qty
    FROM ads_inventory_health
    WHERE snapshot_date = {today}
    GROUP BY inventory_status
    ORDER BY FIELD(inventory_status, '紧急缺货', '需补货', '正常', '库存过高', '滞销', '停售')
    """
    
    # SABC分级分布
    sql_grade = f"""
    SELECT sku_grade, COUNT(*) AS sku_count, SUM(sales_qty_30d) AS sales_qty
    FROM ads_inventory_health
    WHERE snapshot_date = {today}
    GROUP BY sku_grade
    ORDER BY FIELD(sku_grade, 'S', 'A', 'B', 'C')
    """
    
    # 采购欠数 & 建议补货汇总（包含负数统计）
    sql_purchase = f"""
    SELECT 
        COUNT(CASE WHEN purchase_rem_qty > 0 THEN 1 END) AS sku_with_rem,
        SUM(purchase_rem_qty) AS total_rem_qty,
        SUM(CASE WHEN suggest_qty > 0 THEN suggest_qty ELSE 0 END) AS total_positive_suggest,
        SUM(CASE WHEN suggest_qty < 0 THEN suggest_qty ELSE 0 END) AS total_negative_suggest,
        SUM(suggest_qty) AS total_suggest_qty,
        COUNT(CASE WHEN suggest_qty < 0 THEN 1 END) AS sku_with_negative
    FROM ads_inventory_health
    WHERE snapshot_date = {today}
    """
    
    with engine.connect() as conn:
        print("\n" + "="*60)
        print(f"📊 库存健康度汇总 ({today})")
        print("="*60)
        
        # 库存状态
        result = conn.execute(text(sql_status))
        print("\n【库存状态分布】")
        print(f"{'状态':<12} {'SKU数':>8} {'库存数量':>12}")
        print("-"*36)
        for row in result:
            print(f"{row[0]:<12} {row[1]:>8} {row[2]:>12,}")
        
        # SABC分级
        result = conn.execute(text(sql_grade))
        print("\n【SABC分级分布】")
        print(f"{'分级':<6} {'SKU数':>8} {'销售数量':>12}")
        print("-"*30)
        for row in result:
            print(f"{row[0]:<6} {row[1]:>8} {row[2]:>12,}")
        
        # 采购欠数 & 建议补货（包含负数统计）
        result = conn.execute(text(sql_purchase))
        row = result.fetchone()
        print("\n【采购欠数 & 建议补货】")
        print(f"  有采购欠数的SKU: {row[0]:,} 个")
        print(f"  采购欠数合计: {row[1]:,} 件")
        print(f"  需要补货合计: {row[2]:,} 件 (正数)")
        print(f"  库存过剩合计: {row[3]:,} 件 (负数)")
        print(f"  净建议补货: {row[4]:,} 件 (正-负)")
        print(f"  库存过剩SKU: {row[5]:,} 个")
        
        print("="*60 + "\n")
    
    engine.dispose()


def run():
    """执行计算"""
    
    start_time = datetime.now()
    logger.info("="*50)
    logger.info("开始执行 ads_inventory_health 计算（优化版 v2.0）")
    logger.info("="*50)
    
    try:
        # 计算库存健康度
        count = calculate_inventory_health()
        
        # 更新SABC分级
        if count > 0:
            update_sku_grade()
            print_summary()
        
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        
        logger.info("="*50)
        logger.info(f"✓ 计算执行成功！耗时 {duration} 秒")
        logger.info("="*50)
        
        return True
        
    except Exception as e:
        logger.error(f"✗ 计算执行失败: {str(e)}")
        raise


if __name__ == '__main__':
    run()