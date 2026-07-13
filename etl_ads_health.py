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

import logging
import re
import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import text

from config import MAIN_CATEGORY_IDS
from db_connections import create_mysql_engine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


ADS_INVENTORY_HEALTH_TABLE = 'ads_inventory_health'
LEGACY_DWS_INVENTORY_TABLE = 'dws_inventory_daily'
LEGACY_DWS_SALES_TABLE = 'dws_sales_daily'
SHADOW_DWS_INVENTORY_TABLE = 'dws_inventory_daily_v2'
SHADOW_DWS_SALES_TABLE = 'dws_sales_daily_v2'
INVENTORY_HEALTH_COMPARE_SAMPLE_LIMIT = 20
SQL_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

INVENTORY_HEALTH_COMPARE_COLUMNS = [
    'snapshot_date',
    'product_id',
    'sku_id',
    'sku_barcode',
    'color',
    'size',
    'product_code',
    'product_name',
    'category_id',
    'category_name',
    'property_id',
    'property_name',
    'series_id',
    'series_name',
    'price_list',
    'total_qty',
    'warehouse_qty',
    'cloud_qty',
    'purchase_rem_qty',
    'sales_qty_30d',
    'sales_amt_30d',
    'sales_qty_7d',
    'dabo_sales_qty_30d',
    'dabo_sales_qty_7d',
    'dabo_latest_date',
    'dabo_revenue_30d',
    'dabo_revenue_7d',
    'natural_sales_qty_30d',
    'natural_sales_qty_7d',
    'natural_revenue_30d',
    'natural_revenue_7d',
    'return_qty_30d',
    'return_amount_30d',
    'daily_avg_sales',
    'daily_avg_sales_7d',
    'natural_daily_avg_sales',
    'natural_daily_avg_sales_7d',
    'sales_velocity',
    'natural_sales_velocity',
    'turnover_days',
    'inventory_status',
    'sku_grade',
    'suggest_qty',
    'sales_trend',
    'status_priority',
    'sales_rank',
    'sales_ratio',
    'cumulative_ratio',
]


RETRYABLE_MYSQL_LOCK_KEYWORDS = (
    '1213',
    '1205',
    'deadlock found',
    'lock wait timeout exceeded',
    '未能获取命名锁',
)


def _is_retryable_mysql_lock_error(exc):
    message = str(exc).lower()
    return any(keyword in message for keyword in RETRYABLE_MYSQL_LOCK_KEYWORDS)


def _table_exists(conn, table_name):
    result = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = :table_name
            """
        ),
        {"table_name": table_name},
    )
    return (result.scalar() or 0) > 0


def _quote_sql_literal(value):
    return value.replace("'", "''")


def _ensure_safe_identifier(identifier, *, field_name):
    if not isinstance(identifier, str) or not SQL_IDENTIFIER_RE.match(identifier):
        raise ValueError(f'{field_name} 不是合法 SQL 标识符: {identifier}')
    return identifier


def _normalize_snapshot_datetime(snapshot_dt=None):
    if snapshot_dt is None:
        return datetime.now()
    if isinstance(snapshot_dt, datetime):
        return snapshot_dt
    raise TypeError(f'不支持的 snapshot_dt 类型: {type(snapshot_dt)!r}')


def _resolve_dws_source_tables(inventory_table=None, sales_table=None):
    resolved_inventory_table = _ensure_safe_identifier(
        inventory_table or LEGACY_DWS_INVENTORY_TABLE,
        field_name='inventory_table',
    )
    resolved_sales_table = _ensure_safe_identifier(
        sales_table or LEGACY_DWS_SALES_TABLE,
        field_name='sales_table',
    )
    return resolved_inventory_table, resolved_sales_table


def _build_inventory_health_insert_sql(snapshot_dt, inventory_table, sales_table, dabo_rollup_sql):
    snapshot_dt = _normalize_snapshot_datetime(snapshot_dt)
    inventory_table, sales_table = _resolve_dws_source_tables(inventory_table, sales_table)

    today = int(snapshot_dt.strftime('%Y%m%d'))
    date_30_ago = int((snapshot_dt - timedelta(days=30)).strftime('%Y%m%d'))
    date_7_ago = int((snapshot_dt - timedelta(days=7)).strftime('%Y%m%d'))
    main_category_ids = ",".join(str(x) for x in MAIN_CATEGORY_IDS)

    return f"""
     INSERT INTO {ADS_INVENTORY_HEALTH_TABLE}
    (snapshot_date, product_id, sku_id, sku_barcode, color, size, product_code, product_name, category_id, category_name,
     property_id, property_name, series_id, series_name, price_list, total_qty, warehouse_qty, cloud_qty,
     purchase_rem_qty, sales_qty_30d, sales_amt_30d, sales_qty_7d, return_qty_30d,
     return_amount_30d, daily_avg_sales, daily_avg_sales_7d, sales_velocity,
     dabo_sales_qty_30d, dabo_sales_qty_7d, dabo_latest_date, dabo_revenue_30d, dabo_revenue_7d,
     natural_sales_qty_30d, natural_sales_qty_7d, natural_revenue_30d, natural_revenue_7d,
     natural_daily_avg_sales, natural_daily_avg_sales_7d, natural_sales_velocity,
     turnover_days, inventory_status, sku_grade, suggest_qty, status_priority, etl_time, created_at)

    SELECT
        {today} AS snapshot_date,
        p.product_id,
        inv.sku_id,
        sku.sku_barcode,
        sku.sku_color AS color,
        sku.sku_size AS size,
        p.product_code,
        p.product_name,
        p.category_id,
        p.category_name,
        p.property_id,
        p.property_name,
        p.series_id,
        p.series_name,
        p.price_list,
        COALESCE(inv.total_qty, 0) AS total_qty,
        COALESCE(inv.warehouse_qty, 0) AS warehouse_qty,
        COALESCE(inv.cloud_qty, 0) AS cloud_qty,
        COALESCE(inv.purchase_rem_qty, 0) AS purchase_rem_qty,
        COALESCE(sales.sales_qty_30d, 0) AS sales_qty_30d,
        COALESCE(sales.sales_amt_30d, 0) AS sales_amt_30d,
        COALESCE(sales.sales_qty_7d, 0) AS sales_qty_7d,
        COALESCE(sales.return_qty_30d, 0) AS return_qty_30d,
        COALESCE(sales.return_amount_30d, 0) AS return_amount_30d,
        ROUND(COALESCE(sales.sales_qty_30d, 0) / 30, 2) AS daily_avg_sales,
        ROUND(COALESCE(sales.sales_qty_7d, 0) / 7, 2) AS daily_avg_sales_7d,
        CASE
            WHEN COALESCE(sales.sales_qty_30d, 0) = 0 THEN NULL
            ELSE ROUND((COALESCE(sales.sales_qty_7d, 0) / 7) / (COALESCE(sales.sales_qty_30d, 0) / 30), 2)
        END AS sales_velocity,
        COALESCE(dabo.dabo_sales_qty_30d, 0) AS dabo_sales_qty_30d,
        COALESCE(dabo.dabo_sales_qty_7d, 0) AS dabo_sales_qty_7d,
        dabo.dabo_latest_date AS dabo_latest_date,
        COALESCE(dabo.dabo_revenue_30d, 0) AS dabo_revenue_30d,
        COALESCE(dabo.dabo_revenue_7d, 0) AS dabo_revenue_7d,
        (COALESCE(sales.sales_qty_30d, 0) - COALESCE(dabo.dabo_sales_qty_30d, 0)) AS natural_sales_qty_30d,
        (COALESCE(sales.sales_qty_7d, 0) - COALESCE(dabo.dabo_sales_qty_7d, 0)) AS natural_sales_qty_7d,
        (COALESCE(sales.sales_amt_30d, 0) - COALESCE(dabo.dabo_revenue_30d, 0)) AS natural_revenue_30d,
        (COALESCE(sales.sales_amt_7d, 0) - COALESCE(dabo.dabo_revenue_7d, 0)) AS natural_revenue_7d,
        ROUND((COALESCE(sales.sales_qty_30d, 0) - COALESCE(dabo.dabo_sales_qty_30d, 0)) / 30, 2) AS natural_daily_avg_sales,
        ROUND((COALESCE(sales.sales_qty_7d, 0) - COALESCE(dabo.dabo_sales_qty_7d, 0)) / 7, 2) AS natural_daily_avg_sales_7d,
        CASE
            WHEN (COALESCE(sales.sales_qty_30d, 0) - COALESCE(dabo.dabo_sales_qty_30d, 0)) = 0 THEN NULL
            ELSE ROUND(
                ((COALESCE(sales.sales_qty_7d, 0) - COALESCE(dabo.dabo_sales_qty_7d, 0)) / 7)
                / ((COALESCE(sales.sales_qty_30d, 0) - COALESCE(dabo.dabo_sales_qty_30d, 0)) / 30)
            , 2)
        END AS natural_sales_velocity,
        CASE
            WHEN COALESCE(sales.sales_qty_30d, 0) = 0 THEN 9999
            ELSE ROUND(COALESCE(inv.total_qty, 0) / (COALESCE(sales.sales_qty_30d, 0) / 30), 1)
        END AS turnover_days,
        CASE
            WHEN COALESCE(inv.total_qty, 0) > 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN '滞销'
            WHEN COALESCE(inv.total_qty, 0) = 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN '停售'
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 30 THEN '紧急缺货'
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 70 THEN '需补货'
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) <= 90 THEN '正常'
            ELSE '库存过高'
        END AS inventory_status,
        'C' AS sku_grade,
        CASE
            WHEN COALESCE(sales.sales_qty_30d, 0) = 0 THEN 0
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) >= 90 THEN 0
            ELSE ROUND(
                (90 - COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0))
                * (COALESCE(sales.sales_qty_30d, 0) / 30)
                - COALESCE(sales.return_qty_30d, 0)
                - COALESCE(inv.purchase_rem_qty, 0)
            , 0)
        END AS suggest_qty,
        CASE
            WHEN COALESCE(inv.total_qty, 0) > 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN 5
            WHEN COALESCE(inv.total_qty, 0) = 0 AND COALESCE(sales.sales_qty_30d, 0) = 0 THEN 6
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 30 THEN 1
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) < 70 THEN 2
            WHEN COALESCE(inv.total_qty, 0) / NULLIF(COALESCE(sales.sales_qty_30d, 0) / 30, 0) <= 90 THEN 3
            ELSE 4
        END AS status_priority,
        NOW() AS etl_time,
        NOW() AS created_at
    FROM (
        SELECT
            i.product_id,
            i.m_productalias_id AS sku_id,
            SUM(i.qty) AS total_qty,
            SUM(CASE WHEN s.store_code = '001' THEN i.qty ELSE 0 END) AS warehouse_qty,
            SUM(CASE WHEN s.is_cloud_store = 'Y' THEN i.qty ELSE 0 END) AS cloud_qty,
            SUM(COALESCE(i.qtypurchaserem, 0)) AS purchase_rem_qty
        FROM {inventory_table} i
        LEFT JOIN dim_store s ON i.store_id = s.store_id
        WHERE i.date_id = {today}
            AND i.m_productalias_id IS NOT NULL
            AND (s.store_code = '001' OR s.is_cloud_store = 'Y')
        GROUP BY i.product_id, i.m_productalias_id
    ) inv
    LEFT JOIN dim_product p ON inv.product_id = p.product_id
    LEFT JOIN dim_sku sku ON inv.sku_id = sku.sku_id
    LEFT JOIN (
        SELECT
            ds.product_id,
            ds.m_productalias_id AS sku_id,
            SUM(ds.sales_qty) AS sales_qty_30d,
            SUM(ds.sales_amount) AS sales_amt_30d,
            SUM(CASE WHEN ds.date_id >= {date_7_ago} THEN ds.sales_amount ELSE 0 END) AS sales_amt_7d,
            SUM(CASE WHEN ds.date_id >= {date_7_ago} THEN ds.sales_qty ELSE 0 END) AS sales_qty_7d,
            SUM(ds.return_qty) AS return_qty_30d,
            SUM(ds.return_amount) AS return_amount_30d
        FROM {sales_table} ds
        LEFT JOIN dim_store s ON ds.store_id = s.store_id
        WHERE ds.date_id >= {date_30_ago}
            AND (s.store_code LIKE 'DS%%' OR s.is_cloud_store = 'Y')
            AND ds.m_productalias_id IS NOT NULL
        GROUP BY ds.product_id, ds.m_productalias_id
    ) sales ON inv.product_id = sales.product_id AND inv.sku_id = sales.sku_id
    LEFT JOIN (
        {dabo_rollup_sql}
    ) dabo ON inv.sku_id = dabo.sku_id
    WHERE p.is_main_product = 'Y'
        AND p.category_id IN ({main_category_ids})
    """


def _build_shadow_inventory_health_projection_sql(snapshot_dt, inventory_table, sales_table, dabo_rollup_sql):
    base_insert_sql = _build_inventory_health_insert_sql(
        snapshot_dt,
        inventory_table,
        sales_table,
        dabo_rollup_sql,
    )
    select_sql = base_insert_sql.split('SELECT', 1)[1]
    base_projection_sql = f"SELECT {select_sql}"

    return f"""
        SELECT
            ranked.snapshot_date,
            ranked.product_id,
            ranked.sku_id,
            ranked.sku_barcode,
            ranked.color,
            ranked.size,
            ranked.product_code,
            ranked.product_name,
            ranked.category_id,
            ranked.category_name,
            ranked.property_id,
            ranked.property_name,
            ranked.series_id,
            ranked.series_name,
            ranked.price_list,
            ranked.total_qty,
            ranked.warehouse_qty,
            ranked.cloud_qty,
            ranked.purchase_rem_qty,
            ranked.sales_qty_30d,
            ranked.sales_amt_30d,
            ranked.sales_qty_7d,
            ranked.dabo_sales_qty_30d,
            ranked.dabo_sales_qty_7d,
            ranked.dabo_latest_date,
            ranked.dabo_revenue_30d,
            ranked.dabo_revenue_7d,
            ranked.natural_sales_qty_30d,
            ranked.natural_sales_qty_7d,
            ranked.natural_revenue_30d,
            ranked.natural_revenue_7d,
            ranked.return_qty_30d,
            ranked.return_amount_30d,
            ranked.daily_avg_sales,
            ranked.daily_avg_sales_7d,
            ranked.natural_daily_avg_sales,
            ranked.natural_daily_avg_sales_7d,
            ranked.sales_velocity,
            ranked.natural_sales_velocity,
            ranked.turnover_days,
            ranked.inventory_status,
            CASE
                WHEN ranked.total_sales = 0 OR ranked.sales_amt_30d = 0 THEN 'C'
                WHEN (ranked.cumulative_sales - ranked.sales_amt_30d) / ranked.total_sales < 0.30 THEN 'S'
                WHEN ranked.cumulative_sales / ranked.total_sales <= 0.70 THEN 'A'
                WHEN ranked.cumulative_sales / ranked.total_sales <= 0.90 THEN 'B'
                ELSE 'C'
            END AS sku_grade,
            ranked.suggest_qty,
            CASE
                WHEN ranked.sales_qty_30d = 0 THEN '无销售'
                WHEN ranked.sales_velocity >= 1.3 THEN '快速上升'
                WHEN ranked.sales_velocity >= 1.0 THEN '稳定'
                WHEN ranked.sales_velocity >= 0.7 THEN '降温'
                ELSE '快速下滑'
            END AS sales_trend,
            ranked.status_priority,
            ranked.sales_rank,
            ROUND(ranked.sales_amt_30d / NULLIF(ranked.total_sales, 0) * 100, 2) AS sales_ratio,
            ROUND(ranked.cumulative_sales / NULLIF(ranked.total_sales, 0) * 100, 2) AS cumulative_ratio
        FROM (
            SELECT
                base.*,
                SUM(COALESCE(base.sales_amt_30d, 0)) OVER () AS total_sales,
                SUM(COALESCE(base.sales_amt_30d, 0)) OVER (
                    ORDER BY COALESCE(base.sales_amt_30d, 0) DESC, base.sku_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_sales,
                ROW_NUMBER() OVER (ORDER BY COALESCE(base.sales_amt_30d, 0) DESC, base.sku_id) AS sales_rank
            FROM (
                {base_projection_sql}
            ) base
        ) ranked
    """


def _build_persisted_inventory_health_projection_sql(snapshot_dt):
    snapshot_dt = _normalize_snapshot_datetime(snapshot_dt)
    today = int(snapshot_dt.strftime('%Y%m%d'))
    return f"""
        SELECT
            snapshot_date,
            product_id,
            sku_id,
            sku_barcode,
            color,
            size,
            product_code,
            product_name,
            category_id,
            category_name,
            property_id,
            property_name,
            series_id,
            series_name,
            price_list,
            total_qty,
            warehouse_qty,
            cloud_qty,
            purchase_rem_qty,
            sales_qty_30d,
            sales_amt_30d,
            sales_qty_7d,
            dabo_sales_qty_30d,
            dabo_sales_qty_7d,
            dabo_latest_date,
            dabo_revenue_30d,
            dabo_revenue_7d,
            natural_sales_qty_30d,
            natural_sales_qty_7d,
            natural_revenue_30d,
            natural_revenue_7d,
            return_qty_30d,
            return_amount_30d,
            daily_avg_sales,
            daily_avg_sales_7d,
            natural_daily_avg_sales,
            natural_daily_avg_sales_7d,
            sales_velocity,
            natural_sales_velocity,
            turnover_days,
            inventory_status,
            sku_grade,
            suggest_qty,
            sales_trend,
            status_priority,
            sales_rank,
            sales_ratio,
            cumulative_ratio
        FROM {ADS_INVENTORY_HEALTH_TABLE}
        WHERE snapshot_date = {today}
    """


def _build_inventory_health_compare_equal_expr(left_alias, right_alias):
    return ' AND '.join(
        f"{left_alias}.{column} <=> {right_alias}.{column}"
        for column in INVENTORY_HEALTH_COMPARE_COLUMNS
    )


def _build_inventory_health_compare_scope_sql(baseline_sql, shadow_sql):
    equal_expr = _build_inventory_health_compare_equal_expr('baseline', 'shadow')
    return f"""
        SELECT
            COALESCE(baseline.snapshot_date, shadow.snapshot_date) AS snapshot_date,
            COALESCE(baseline.product_id, shadow.product_id) AS product_id,
            COALESCE(baseline.sku_id, shadow.sku_id) AS sku_id,
            CASE
                WHEN baseline.sku_id IS NULL THEN 'missing_baseline'
                WHEN shadow.sku_id IS NULL THEN 'missing_shadow'
                ELSE 'value_mismatch'
            END AS mismatch_type,
            baseline.inventory_status AS baseline_inventory_status,
            shadow.inventory_status AS shadow_inventory_status,
            baseline.total_qty AS baseline_total_qty,
            shadow.total_qty AS shadow_total_qty,
            baseline.sales_qty_30d AS baseline_sales_qty_30d,
            shadow.sales_qty_30d AS shadow_sales_qty_30d,
            baseline.sales_amt_30d AS baseline_sales_amt_30d,
            shadow.sales_amt_30d AS shadow_sales_amt_30d,
            baseline.turnover_days AS baseline_turnover_days,
            shadow.turnover_days AS shadow_turnover_days,
            baseline.suggest_qty AS baseline_suggest_qty,
            shadow.suggest_qty AS shadow_suggest_qty,
            baseline.sales_rank AS baseline_sales_rank,
            shadow.sales_rank AS shadow_sales_rank,
            baseline.sku_grade AS baseline_sku_grade,
            shadow.sku_grade AS shadow_sku_grade
        FROM (
            {baseline_sql}
        ) baseline
        LEFT JOIN (
            {shadow_sql}
        ) shadow
            ON baseline.snapshot_date <=> shadow.snapshot_date
           AND baseline.sku_id <=> shadow.sku_id
        WHERE shadow.sku_id IS NULL OR NOT ({equal_expr})

        UNION ALL

        SELECT
            COALESCE(baseline.snapshot_date, shadow.snapshot_date) AS snapshot_date,
            COALESCE(baseline.product_id, shadow.product_id) AS product_id,
            COALESCE(baseline.sku_id, shadow.sku_id) AS sku_id,
            'missing_persisted' AS mismatch_type,
            baseline.inventory_status AS baseline_inventory_status,
            shadow.inventory_status AS shadow_inventory_status,
            baseline.total_qty AS baseline_total_qty,
            shadow.total_qty AS shadow_total_qty,
            baseline.sales_qty_30d AS baseline_sales_qty_30d,
            shadow.sales_qty_30d AS shadow_sales_qty_30d,
            baseline.sales_amt_30d AS baseline_sales_amt_30d,
            shadow.sales_amt_30d AS shadow_sales_amt_30d,
            baseline.turnover_days AS baseline_turnover_days,
            shadow.turnover_days AS shadow_turnover_days,
            baseline.suggest_qty AS baseline_suggest_qty,
            shadow.suggest_qty AS shadow_suggest_qty,
            baseline.sales_rank AS baseline_sales_rank,
            shadow.sales_rank AS shadow_sales_rank,
            baseline.sku_grade AS baseline_sku_grade,
            shadow.sku_grade AS shadow_sku_grade
        FROM (
            {shadow_sql}
        ) shadow
        LEFT JOIN (
            {baseline_sql}
        ) baseline
            ON baseline.snapshot_date <=> shadow.snapshot_date
           AND baseline.sku_id <=> shadow.sku_id
        WHERE baseline.sku_id IS NULL
    """


def validate_inventory_health_shadow_against_persisted(
    dabo_source_mode='auto',
    *,
    snapshot_dt=None,
    inventory_table=SHADOW_DWS_INVENTORY_TABLE,
    sales_table=SHADOW_DWS_SALES_TABLE,
    sample_limit=INVENTORY_HEALTH_COMPARE_SAMPLE_LIMIT,
):
    snapshot_dt = _normalize_snapshot_datetime(snapshot_dt)
    inventory_table, sales_table = _resolve_dws_source_tables(inventory_table, sales_table)
    engine = create_mysql_engine()

    with engine.connect() as conn:
        if not _table_exists(conn, ADS_INVENTORY_HEALTH_TABLE):
            engine.dispose()
            return {
                'status': 'WARNING',
                'reason': 'persisted_ads_inventory_health_missing',
                'baseline_table': ADS_INVENTORY_HEALTH_TABLE,
                'shadow_inventory_table': inventory_table,
                'shadow_sales_table': sales_table,
                'mismatch_count': None,
                'sample_limit': sample_limit,
                'sample_mismatches': [],
            }

        dabo_rollup_sql, dabo_source_detail = _resolve_dabo_rollup_sql(conn, snapshot_dt, dabo_source_mode)
        baseline_sql = _build_persisted_inventory_health_projection_sql(snapshot_dt)
        shadow_sql = _build_shadow_inventory_health_projection_sql(
            snapshot_dt,
            inventory_table,
            sales_table,
            dabo_rollup_sql,
        )
        compare_scope_sql = _build_inventory_health_compare_scope_sql(baseline_sql, shadow_sql)
        mismatch_count = conn.execute(
            text(f"SELECT COUNT(*) FROM ({compare_scope_sql}) mismatch_scope")
        ).scalar() or 0
        baseline_row_count = conn.execute(text(f"SELECT COUNT(*) FROM ({baseline_sql}) baseline_scope")).scalar() or 0
        shadow_row_count = conn.execute(text(f"SELECT COUNT(*) FROM ({shadow_sql}) shadow_scope")).scalar() or 0
        sample_mismatches = []
        if mismatch_count > 0:
            sample_query = text(f"SELECT * FROM ({compare_scope_sql}) mismatch_scope LIMIT {int(sample_limit)}")
            sample_mismatches = [dict(row) for row in conn.execute(sample_query).mappings().all()]

    engine.dispose()
    return {
        'status': 'SUCCESS' if mismatch_count == 0 else 'WARNING',
        'reason': 'persisted_ads_matches_shadow_projection' if mismatch_count == 0 else 'persisted_ads_shadow_mismatch',
        'baseline_table': ADS_INVENTORY_HEALTH_TABLE,
        'shadow_inventory_table': inventory_table,
        'shadow_sales_table': sales_table,
        'dabo_source_mode': dabo_source_mode,
        'dabo_source_detail': dabo_source_detail,
        'baseline_row_count': int(baseline_row_count),
        'shadow_row_count': int(shadow_row_count),
        'mismatch_count': int(mismatch_count),
        'sample_limit': int(sample_limit),
        'sample_mismatches': sample_mismatches,
    }


def _get_latest_dabo_label_source(conn):
    if not _table_exists(conn, 'ads_dabo_order_label'):
        return None

    row = conn.execute(
        text(
            """
            SELECT source_file
            FROM ads_dabo_order_label
            GROUP BY source_file
            ORDER BY MAX(updated_at) DESC, MAX(source_file_mtime) DESC, source_file DESC
            LIMIT 1
            """
        )
    ).mappings().first()
    if not row:
        return None
    return row['source_file']


def _build_empty_dabo_rollup_sql():
    return """
        SELECT
            CAST(NULL AS SIGNED) AS sku_id,
            0 AS dabo_sales_qty_30d,
            0 AS dabo_sales_qty_7d,
            0.00 AS dabo_revenue_30d,
            0.00 AS dabo_revenue_7d,
            CAST(NULL AS DATE) AS dabo_latest_date
        WHERE 1 = 0
    """


def _build_legacy_dabo_rollup_sql(snapshot_dt):
    date_30_ago = (snapshot_dt - timedelta(days=30)).strftime('%Y-%m-%d')
    date_7_ago = (snapshot_dt - timedelta(days=7)).strftime('%Y-%m-%d')
    snapshot_date = snapshot_dt.strftime('%Y-%m-%d')

    return f"""
        SELECT
            sku.sku_id,
            SUM(d.dabo_sales_qty) AS dabo_sales_qty_30d,
            SUM(CASE WHEN d.sale_date >= '{date_7_ago}' THEN d.dabo_sales_qty ELSE 0 END) AS dabo_sales_qty_7d,
            SUM(d.dabo_revenue) AS dabo_revenue_30d,
            SUM(CASE WHEN d.sale_date >= '{date_7_ago}' THEN d.dabo_revenue ELSE 0 END) AS dabo_revenue_7d,
            MAX(d.sale_date) AS dabo_latest_date
        FROM ads_dabo_daily_sales d
        INNER JOIN dim_sku sku
            ON sku.sku_barcode COLLATE utf8mb4_unicode_ci = d.product_alias_code COLLATE utf8mb4_unicode_ci
        WHERE d.sale_date BETWEEN '{date_30_ago}' AND '{snapshot_date}'
        GROUP BY sku.sku_id
    """


def _build_label_dabo_rollup_sql(snapshot_dt, source_file):
    safe_source_file = _quote_sql_literal(source_file)
    snapshot_date_id = int(snapshot_dt.strftime('%Y%m%d'))
    date_30_ago_id = int((snapshot_dt - timedelta(days=30)).strftime('%Y%m%d'))
    date_7_ago_id = int((snapshot_dt - timedelta(days=7)).strftime('%Y%m%d'))

    order_scope_sql = f"""
        SELECT DISTINCT
            COALESCE(NULLIF(canonical_system_order_id, ''), system_order_id) AS bridge_system_order_id
        FROM ads_dabo_order_label
        WHERE source_file = '{safe_source_file}'
          AND is_dabo_order = 1
          AND system_order_id IS NOT NULL
          AND system_order_id <> ''
    """

    return f"""
        SELECT
            sku_metrics.sku_id,
            SUM(sku_metrics.dabo_sales_qty) AS dabo_sales_qty_30d,
            SUM(CASE WHEN sku_metrics.billdate >= {date_7_ago_id} THEN sku_metrics.dabo_sales_qty ELSE 0 END) AS dabo_sales_qty_7d,
            SUM(sku_metrics.dabo_revenue) AS dabo_revenue_30d,
            SUM(CASE WHEN sku_metrics.billdate >= {date_7_ago_id} THEN sku_metrics.dabo_revenue ELSE 0 END) AS dabo_revenue_7d,
            MAX(STR_TO_DATE(CAST(sku_metrics.billdate AS CHAR), '%Y%m%d')) AS dabo_latest_date
        FROM (
            SELECT
                retail.billdate,
                ri.m_productalias_id AS sku_id,
                SUM(CASE WHEN retail.tot_amt_actual > 0 OR (retail.tot_amt_actual = 0 AND ri.qty > 0) THEN ri.qty ELSE 0 END) AS dabo_sales_qty,
                ROUND(SUM(CASE WHEN retail.tot_amt_actual > 0 OR (retail.tot_amt_actual = 0 AND ri.qty > 0) THEN ri.tot_amt_actual ELSE 0 END), 2) AS dabo_revenue
            FROM (
                SELECT DISTINCT
                    r.id,
                    r.billdate,
                    r.tot_amt_actual
                FROM ods_m_retail r
                INNER JOIN (
                    {order_scope_sql}
                ) scope
                    ON scope.bridge_system_order_id = r.oms_sourcecode
                WHERE r.isactive = 'Y'
                  AND r.status = 2
                  AND r.billdate BETWEEN {date_30_ago_id} AND {snapshot_date_id}

                UNION

                SELECT DISTINCT
                    c.retail_id AS id,
                    c.billdate,
                    c.retail_tot_amt_actual AS tot_amt_actual
                FROM ads_dabo_order_retail_bridge c
                INNER JOIN (
                    {order_scope_sql}
                ) scope
                    ON scope.bridge_system_order_id = c.main_order_id
                WHERE c.source_file = '{safe_source_file}'
                  AND c.retail_isactive = 'Y'
                  AND c.retail_status = 2
                  AND c.billdate BETWEEN {date_30_ago_id} AND {snapshot_date_id}
                  AND NOT EXISTS (
                      SELECT 1
                      FROM ods_m_retail r
                      WHERE r.oms_sourcecode = c.main_order_id
                        AND r.isactive = 'Y'
                        AND r.status = 2
                  )
            ) retail
            INNER JOIN ods_m_retailitem ri
                ON ri.m_retail_id = retail.id
            WHERE ri.m_productalias_id IS NOT NULL
            GROUP BY retail.billdate, ri.m_productalias_id
        ) sku_metrics
        GROUP BY sku_metrics.sku_id
    """


def _resolve_dabo_rollup_sql(conn, snapshot_dt, source_mode='auto'):
    if source_mode not in {'auto', 'label', 'legacy', 'none'}:
        raise ValueError(f'不支持的达播来源模式: {source_mode}')

    latest_label_source = _get_latest_dabo_label_source(conn)
    has_legacy_table = _table_exists(conn, 'ads_dabo_daily_sales')

    if source_mode == 'none':
        return _build_empty_dabo_rollup_sql(), 'none: 未检测到可用达播源，达播字段按0处理'

    if source_mode == 'label':
        if latest_label_source:
            return _build_label_dabo_rollup_sql(snapshot_dt, latest_label_source), f'label: latest_source_file={latest_label_source}'
        return _build_empty_dabo_rollup_sql(), 'label: latest_source_file_missing，达播字段按0处理'

    if source_mode == 'legacy':
        if has_legacy_table:
            return _build_legacy_dabo_rollup_sql(snapshot_dt), 'legacy: ads_dabo_daily_sales'
        return _build_empty_dabo_rollup_sql(), 'legacy: table_missing，达播字段按0处理'

    if latest_label_source:
        return _build_label_dabo_rollup_sql(snapshot_dt, latest_label_source), f'label: latest_source_file={latest_label_source}'
    if has_legacy_table:
        return _build_legacy_dabo_rollup_sql(snapshot_dt), 'legacy: ads_dabo_daily_sales'
    return _build_empty_dabo_rollup_sql(), 'auto: no_label_no_legacy，达播字段按0处理'


def ensure_table_columns(engine):
    """确保ads_inventory_health表有新增的字段"""
    
    new_columns = [
        ("sku_id", "BIGINT DEFAULT NULL COMMENT 'SKU主键(M_PRODUCT_ALIAS.ID)'"),
        ("sku_barcode", "VARCHAR(80) DEFAULT NULL COMMENT '条码(M_PRODUCT_ALIAS.NO)'"),
        ("color", "VARCHAR(50) DEFAULT NULL COMMENT '颜色'"),
        ("size", "VARCHAR(50) DEFAULT NULL COMMENT '尺寸'"),
        ("return_qty_30d", "INT DEFAULT 0 COMMENT '近30天退货数量'"),
        ("purchase_rem_qty", "INT DEFAULT 0 COMMENT '采购欠数/在途库存'"),
        ("sales_velocity", "DECIMAL(5,2) DEFAULT NULL COMMENT '销售加速度(7天日均/30天日均)'"),
        ("daily_avg_sales_7d", "DECIMAL(10,2) DEFAULT 0 COMMENT '近7天日均销量'"),
        ("dabo_sales_qty_30d", "INT DEFAULT 0 COMMENT '近30天达播销量'"),
        ("dabo_sales_qty_7d", "INT DEFAULT 0 COMMENT '近7天达播销量'"),
        ("natural_sales_qty_30d", "INT DEFAULT 0 COMMENT '近30天自然销量(全量-达播)'"),
        ("natural_sales_qty_7d", "INT DEFAULT 0 COMMENT '近7天自然销量(全量-达播)'"),
        ("dabo_revenue_30d", "DECIMAL(14,2) DEFAULT 0.00 COMMENT '近30天达播销售额'"),
        ("dabo_revenue_7d", "DECIMAL(14,2) DEFAULT 0.00 COMMENT '近7天达播销售额'"),
        ("natural_revenue_30d", "DECIMAL(14,2) DEFAULT 0.00 COMMENT '近30天自然销售额(全量-达播)'"),
        ("natural_revenue_7d", "DECIMAL(14,2) DEFAULT 0.00 COMMENT '近7天自然销售额(全量-达播)'"),
        ("natural_daily_avg_sales", "DECIMAL(10,2) DEFAULT 0 COMMENT '近30天自然日均销量'"),
        ("natural_daily_avg_sales_7d", "DECIMAL(10,2) DEFAULT 0 COMMENT '近7天自然日均销量'"),
        ("natural_sales_velocity", "DECIMAL(5,2) DEFAULT NULL COMMENT '自然销售加速度'"),
        ("dabo_latest_date", "DATE DEFAULT NULL COMMENT '达播最新日期(按SKU)'"),
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


def calculate_inventory_health(
    dabo_source_mode='auto',
    *,
    snapshot_dt=None,
    inventory_table=LEGACY_DWS_INVENTORY_TABLE,
    sales_table=LEGACY_DWS_SALES_TABLE,
):
    """计算库存健康度（优化版）"""

    snapshot_dt = _normalize_snapshot_datetime(snapshot_dt)
    inventory_table, sales_table = _resolve_dws_source_tables(inventory_table, sales_table)
    logger.info("连接MySQL数据库...")
    engine = create_mysql_engine()

    # 确保表有新字段
    ensure_table_columns(engine)

    today = int(snapshot_dt.strftime('%Y%m%d'))
    with engine.connect() as conn:
        dabo_rollup_sql, dabo_source_detail = _resolve_dabo_rollup_sql(conn, snapshot_dt, dabo_source_mode)
    logger.info(
        '库存健康度达播来源：%s；DWS来源 inventory=%s sales=%s',
        dabo_source_detail,
        inventory_table,
        sales_table,
    )

    sql = _build_inventory_health_insert_sql(
        snapshot_dt,
        inventory_table,
        sales_table,
        dabo_rollup_sql,
    )
    
    lock_name = f'hefang_dw:{ADS_INVENTORY_HEALTH_TABLE}'
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            with engine.connect() as lock_conn:
                got_lock = lock_conn.execute(
                    text("SELECT GET_LOCK(:lock_name, :timeout_seconds)"),
                    {"lock_name": lock_name, "timeout_seconds": 30},
                ).scalar()
                if got_lock != 1:
                    raise TimeoutError(f"未能获取命名锁: {lock_name}")

                try:
                    logger.info(
                        f"清空当天数据（{today}）并执行库存健康度计算（单事务，第 {attempt}/{max_attempts} 次）..."
                    )
                    with engine.begin() as conn:
                        conn.execute(text(f"DELETE FROM {ADS_INVENTORY_HEALTH_TABLE} WHERE snapshot_date = {today}"))
                        conn.execute(text(sql))
                finally:
                    lock_conn.execute(text("SELECT RELEASE_LOCK(:lock_name)"), {"lock_name": lock_name})

            break
        except Exception as exc:
            if attempt >= max_attempts or not _is_retryable_mysql_lock_error(exc):
                raise
            wait_seconds = attempt * 5
            logger.warning(
                f"检测到可重试锁冲突（第 {attempt}/{max_attempts} 次）：{exc}；{wait_seconds} 秒后重试..."
            )
            time.sleep(wait_seconds)
    
    # 查询写入记录数
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {ADS_INVENTORY_HEALTH_TABLE} WHERE snapshot_date = {today}"))
        count_row = result.fetchone()
        count = count_row[0] if count_row is not None else 0
    
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
    engine = create_mysql_engine()
    today = int(datetime.now().strftime('%Y%m%d'))
    
    # 使用单条 MySQL SQL（窗口函数）批量计算 sales_rank / sales_ratio / cumulative_ratio / sku_grade
    logger.info("使用 MySQL 窗口函数批量计算分级与排名...")
    sql_update = f"""
    UPDATE ads_inventory_health a
    JOIN (
        SELECT sku_id, sales_amt_30d, total_sales, cum_sales, sales_rank FROM (
            SELECT
                sku_id,
                COALESCE(sales_amt_30d, 0) AS sales_amt_30d,
                SUM(COALESCE(sales_amt_30d,0)) OVER () AS total_sales,
                SUM(COALESCE(sales_amt_30d,0)) OVER (ORDER BY COALESCE(sales_amt_30d,0) DESC, sku_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_sales,
                ROW_NUMBER() OVER (ORDER BY COALESCE(sales_amt_30d,0) DESC, sku_id) AS sales_rank
            FROM ads_inventory_health
            WHERE snapshot_date = {today}
        ) t
    ) r ON a.sku_id = r.sku_id AND a.snapshot_date = {today}
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
    engine = create_mysql_engine()
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
        if row is None:
            row = (0, 0, 0, 0, 0, 0)
        print("\n【采购欠数 & 建议补货】")
        print(f"  有采购欠数的SKU: {row[0]:,} 个")
        print(f"  采购欠数合计: {row[1]:,} 件")
        print(f"  需要补货合计: {row[2]:,} 件 (正数)")
        print(f"  库存过剩合计: {row[3]:,} 件 (负数)")
        print(f"  净建议补货: {row[4]:,} 件 (正-负)")
        print(f"  库存过剩SKU: {row[5]:,} 个")
        
        print("="*60 + "\n")
    
    engine.dispose()


def backfill_dabo_fields(snapshot_date=None, dabo_source_mode='auto'):
    """按指定来源重算达播/自然字段（默认仅当天）"""

    if snapshot_date is None:
        snapshot_date = int(datetime.now().strftime('%Y%m%d'))

    snapshot_dt = datetime.strptime(str(snapshot_date), '%Y%m%d')
    date_30_ago = (snapshot_dt - timedelta(days=30)).strftime('%Y-%m-%d')
    date_7_ago = (snapshot_dt - timedelta(days=7)).strftime('%Y-%m-%d')
    snapshot_date_str = snapshot_dt.strftime('%Y-%m-%d')

    logger.info(f"回填达播/自然字段（snapshot_date={snapshot_date}）...")
    engine = create_mysql_engine()
    with engine.connect() as conn:
        dabo_rollup_sql, dabo_source_detail = _resolve_dabo_rollup_sql(conn, snapshot_dt, dabo_source_mode)
    logger.info(f"达播字段回填来源：{dabo_source_detail}")

    sql_dabo = f"""
    UPDATE ads_inventory_health a
    LEFT JOIN (
        {dabo_rollup_sql}
    ) d ON a.sku_id = d.sku_id
    SET
        a.dabo_sales_qty_30d = COALESCE(d.dabo_sales_qty_30d, 0),
        a.dabo_sales_qty_7d = COALESCE(d.dabo_sales_qty_7d, 0),
        a.dabo_revenue_30d = COALESCE(d.dabo_revenue_30d, 0),
        a.dabo_revenue_7d = COALESCE(d.dabo_revenue_7d, 0),
        a.dabo_latest_date = d.dabo_latest_date
    WHERE a.snapshot_date = {snapshot_date}
    """

    date_7_ago_id = int((snapshot_dt - timedelta(days=7)).strftime('%Y%m%d'))
    sql_natural = f"""
    UPDATE ads_inventory_health a
    LEFT JOIN (
        SELECT
            ds.m_productalias_id AS sku_id,
            SUM(ds.sales_amount) AS sales_amt_7d
        FROM dws_sales_daily ds
        LEFT JOIN dim_store s ON ds.store_id = s.store_id
        WHERE ds.date_id >= {date_7_ago_id}
          AND (s.store_code LIKE 'DS%%' OR s.is_cloud_store = 'Y')
          AND ds.m_productalias_id IS NOT NULL
        GROUP BY ds.m_productalias_id
    ) s7 ON a.sku_id = s7.sku_id
    SET
        a.natural_sales_qty_30d = COALESCE(a.sales_qty_30d, 0) - COALESCE(a.dabo_sales_qty_30d, 0),
        a.natural_sales_qty_7d = COALESCE(a.sales_qty_7d, 0) - COALESCE(a.dabo_sales_qty_7d, 0),
        a.natural_revenue_30d = COALESCE(a.sales_amt_30d, 0) - COALESCE(a.dabo_revenue_30d, 0),
        a.natural_revenue_7d = COALESCE(s7.sales_amt_7d, 0) - COALESCE(a.dabo_revenue_7d, 0),
        a.natural_daily_avg_sales = ROUND((COALESCE(a.sales_qty_30d, 0) - COALESCE(a.dabo_sales_qty_30d, 0)) / 30, 2),
        a.natural_daily_avg_sales_7d = ROUND((COALESCE(a.sales_qty_7d, 0) - COALESCE(a.dabo_sales_qty_7d, 0)) / 7, 2),
        a.natural_sales_velocity = CASE
            WHEN (COALESCE(a.sales_qty_30d, 0) - COALESCE(a.dabo_sales_qty_30d, 0)) = 0 THEN NULL
            ELSE ROUND(
                ((COALESCE(a.sales_qty_7d, 0) - COALESCE(a.dabo_sales_qty_7d, 0)) / 7)
                / ((COALESCE(a.sales_qty_30d, 0) - COALESCE(a.dabo_sales_qty_30d, 0)) / 30)
            , 2)
        END
    WHERE a.snapshot_date = {snapshot_date}
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(sql_dabo))
            conn.execute(text(sql_natural))
    finally:
        engine.dispose()


def run(
    dabo_source_mode='auto',
    *,
    snapshot_dt=None,
    inventory_table=LEGACY_DWS_INVENTORY_TABLE,
    sales_table=LEGACY_DWS_SALES_TABLE,
):
    """执行计算"""

    snapshot_dt = _normalize_snapshot_datetime(snapshot_dt)
    start_time = datetime.now()
    logger.info("="*50)
    logger.info("开始执行 ads_inventory_health 计算（优化版 v2.0）")
    logger.info("="*50)

    try:
        # 计算库存健康度
        count = calculate_inventory_health(
            dabo_source_mode=dabo_source_mode,
            snapshot_dt=snapshot_dt,
            inventory_table=inventory_table,
            sales_table=sales_table,
        )
        
        # 更新SABC分级
        if count > 0:
            backfill_dabo_fields(
                snapshot_date=int(snapshot_dt.strftime('%Y%m%d')),
                dabo_source_mode=dabo_source_mode,
            )
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