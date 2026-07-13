# -*- coding: utf-8 -*-
"""DWD 销售零售明细事实旁路 ETL。

状态：M3 小窗口装载实现；默认只输出 SQL / 做连接测试，只有显式 ``--execute``
才按主键 upsert 写入 ``dwd_sales_retail_item``，不接入 run_etl.py。

设计边界：
- 目标表草案见 SQL/draft_create_dwd_sales_retail_item.sql。
- 默认假设 M3 选择旁路 ods_*_raw 源表；若改为兼容扩字段方案，需把源表名改回现有 ODS。
- 所有真实 CREATE / INSERT / DELETE / 回填由用户人工授权执行。
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from sqlalchemy import text

from db_connections import create_mysql_engine


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


DEFAULT_RETAIL_TABLE = 'ods_m_retail_raw'
DEFAULT_RETAILITEM_TABLE = 'ods_m_retailitem_raw'
DEFAULT_TARGET_TABLE = 'dwd_sales_retail_item'

UPSERT_UPDATE_COLUMNS = (
    'retail_id',
    'docno',
    'date_id',
    'store_id',
    'store_code',
    'is_cloud_store',
    'oms_sourcecode',
    'retail_refno',
    'retail_doctype',
    'retail_bill_type',
    'retail_description',
    'product_id',
    'm_productalias_id',
    'attribute_set_instance_id',
    'item_order_no',
    'qty',
    'price_list',
    'price_actual',
    'discount_rate',
    'line_actual_amt',
    'line_list_amt',
    'r_qty',
    'r_can_qty',
    'retail_actual_amt',
    'retail_list_amt',
    'retail_total_qty',
    'retail_avg_discount',
    'retail_status',
    'retail_isactive',
    'item_status',
    'item_type',
    'is_returned',
    'related_retail_item_id',
    'retail_vip_id',
    'item_vip_id',
    'retail_salesrep_id',
    'item_salesrep_id',
    'item_salesreps_id',
    'item_salesreps_name',
    'pay_status',
    'payer_id',
    'pay_time',
    'close_status',
    'closer_id',
    'close_time',
    'has_retail_header_flag',
    'is_valid_retail_flag',
    'has_sku_flag',
    'is_positive_sale_flag',
    'is_return_flag',
    'dws_sales_scope_flag',
    'retail_created_at',
    'retail_modified_at',
    'item_modified_at',
    'item_set_time',
    'retail_source_loaded_at',
    'item_source_loaded_at',
    'retail_source_batch_id',
    'item_source_batch_id',
    'etl_time',
)


@dataclass(frozen=True)
class DwdSalesRetailItemConfig:
    start_date: int
    end_date: int
    retail_table: str = DEFAULT_RETAIL_TABLE
    retailitem_table: str = DEFAULT_RETAILITEM_TABLE
    target_table: str = DEFAULT_TARGET_TABLE
    timeout_profile: str = 'etl'


def _default_date_range(days_back: int = 7) -> tuple[int, int]:
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=days_back - 1)
    return int(start_dt.strftime('%Y%m%d')), int(end_dt.strftime('%Y%m%d'))


def build_insert_sql(config: DwdSalesRetailItemConfig) -> str:
    """生成小窗口 INSERT ... SELECT ... ON DUPLICATE KEY UPDATE SQL。"""

    update_clause = ',\n    '.join(
        f"{column} = VALUES({column})" for column in UPSERT_UPDATE_COLUMNS
    )

    return f"""
INSERT INTO {config.target_table} (
    retail_item_id,
    retail_id,
    docno,
    date_id,
    store_id,
    store_code,
    is_cloud_store,
    oms_sourcecode,
    retail_refno,
    retail_doctype,
    retail_bill_type,
    retail_description,
    product_id,
    m_productalias_id,
    attribute_set_instance_id,
    item_order_no,
    qty,
    price_list,
    price_actual,
    discount_rate,
    line_actual_amt,
    line_list_amt,
    r_qty,
    r_can_qty,
    retail_actual_amt,
    retail_list_amt,
    retail_total_qty,
    retail_avg_discount,
    retail_status,
    retail_isactive,
    item_status,
    item_type,
    is_returned,
    related_retail_item_id,
    retail_vip_id,
    item_vip_id,
    retail_salesrep_id,
    item_salesrep_id,
    item_salesreps_id,
    item_salesreps_name,
    pay_status,
    payer_id,
    pay_time,
    close_status,
    closer_id,
    close_time,
    has_retail_header_flag,
    is_valid_retail_flag,
    has_sku_flag,
    is_positive_sale_flag,
    is_return_flag,
    dws_sales_scope_flag,
    retail_created_at,
    retail_modified_at,
    item_modified_at,
    item_set_time,
    retail_source_loaded_at,
    item_source_loaded_at,
    retail_source_batch_id,
    item_source_batch_id,
    etl_time
)
SELECT
    ri.id AS retail_item_id,
    ri.m_retail_id AS retail_id,
    r.docno,
    r.billdate AS date_id,
    r.c_store_id AS store_id,
    COALESCE(s.store_code, '') AS store_code,
    COALESCE(s.is_cloud_store, 'N') AS is_cloud_store,
    LEFT(r.oms_sourcecode, 512) AS oms_sourcecode,
    r.refno AS retail_refno,
    r.doctype AS retail_doctype,
    r.retailbilltype AS retail_bill_type,
    r.description AS retail_description,
    ri.m_product_id AS product_id,
    ri.m_productalias_id,
    ri.m_attributesetinstance_id AS attribute_set_instance_id,
    ri.orderno AS item_order_no,
    ri.qty,
    ri.pricelist AS price_list,
    ri.priceactual AS price_actual,
    ri.discount AS discount_rate,
    ri.tot_amt_actual AS line_actual_amt,
    ri.tot_amt_list AS line_list_amt,
    ri.rqty AS r_qty,
    ri.rcanqty AS r_can_qty,
    r.tot_amt_actual AS retail_actual_amt,
    r.tot_amt_list AS retail_list_amt,
    r.tot_qty AS retail_total_qty,
    r.avg_discount AS retail_avg_discount,
    r.status AS retail_status,
    r.isactive AS retail_isactive,
    ri.status AS item_status,
    ri.type AS item_type,
    r.isreturned AS is_returned,
    ri.m_retailitem_id AS related_retail_item_id,
    r.c_vip_id AS retail_vip_id,
    ri.c_vip_id AS item_vip_id,
    r.salesrep_id AS retail_salesrep_id,
    ri.salesrep_id AS item_salesrep_id,
    ri.salesreps_id AS item_salesreps_id,
    ri.salesreps_name AS item_salesreps_name,
    r.pay_status,
    r.payerid AS payer_id,
    r.paytime AS pay_time,
    r.close_status,
    r.closerid AS closer_id,
    r.closetime AS close_time,
    CASE WHEN r.id IS NOT NULL THEN 'Y' ELSE 'N' END AS has_retail_header_flag,
    CASE WHEN r.isactive = 'Y' AND r.status = 2 THEN 'Y' ELSE 'N' END AS is_valid_retail_flag,
    CASE WHEN ri.m_productalias_id IS NOT NULL THEN 'Y' ELSE 'N' END AS has_sku_flag,
    CASE WHEN r.tot_amt_actual > 0 OR (r.tot_amt_actual = 0 AND ri.qty > 0) THEN 'Y' ELSE 'N' END AS is_positive_sale_flag,
    CASE WHEN r.tot_amt_actual < 0 OR (r.tot_amt_actual = 0 AND ri.qty < 0) THEN 'Y' ELSE 'N' END AS is_return_flag,
    CASE
        WHEN r.isactive = 'Y'
         AND r.status = 2
         AND r.billdate IS NOT NULL
         AND ri.m_productalias_id IS NOT NULL
        THEN 'Y' ELSE 'N'
    END AS dws_sales_scope_flag,
    r.creationdate AS retail_created_at,
    r.modifieddate AS retail_modified_at,
    ri.modifieddate AS item_modified_at,
    ri.settime AS item_set_time,
    r.etl_loaded_at AS retail_source_loaded_at,
    ri.etl_loaded_at AS item_source_loaded_at,
    CAST(r.etl_batch_id AS CHAR) AS retail_source_batch_id,
    CAST(ri.etl_batch_id AS CHAR) AS item_source_batch_id,
    NOW() AS etl_time
FROM {config.retailitem_table} ri
LEFT JOIN {config.retail_table} r
    ON ri.m_retail_id = r.id
LEFT JOIN dim_store s
    ON r.c_store_id = s.store_id
WHERE r.billdate >= :start_date
  AND r.billdate <= :end_date
ON DUPLICATE KEY UPDATE
    {update_clause},
    updated_at = NOW()
""".strip()


def build_source_count_sql(config: DwdSalesRetailItemConfig) -> str:
    return f"""
SELECT COUNT(*)
FROM {config.retailitem_table} ri
LEFT JOIN {config.retail_table} r
    ON ri.m_retail_id = r.id
WHERE r.billdate >= :start_date
  AND r.billdate <= :end_date
""".strip()


def execute_load(config: DwdSalesRetailItemConfig) -> dict[str, int]:
    """执行 DWD 销售明细小窗口 upsert。"""

    params = {'start_date': config.start_date, 'end_date': config.end_date}
    engine = create_mysql_engine(timeout_profile=config.timeout_profile)
    try:
        with engine.begin() as conn:
            source_rows = conn.execute(text(build_source_count_sql(config)), params).scalar() or 0
            if source_rows <= 0:
                raise RuntimeError('raw ODS 小窗口无销售明细数据，已停止 DWD 写入')
            result = conn.execute(text(build_insert_sql(config)), params)
            target_rows = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {config.target_table}
                    WHERE date_id >= :start_date
                      AND date_id <= :end_date
                    """
                ),
                params,
            ).scalar() or 0
    finally:
        engine.dispose()

    logger.info(
        'dwd_sales_retail_item 小窗口 upsert 完成：source_rows=%s, affected=%s, target_window_rows=%s',
        source_rows,
        result.rowcount,
        target_rows,
    )
    return {
        'source_rows': int(source_rows),
        'affected_rows': int(result.rowcount or 0),
        'target_window_rows': int(target_rows),
    }


def conn_test(timeout_profile: str) -> None:
    """只读连接测试。"""

    engine = create_mysql_engine(timeout_profile=timeout_profile)
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
    finally:
        engine.dispose()
    logger.info('MySQL conn-test 通过，timeout_profile=%s', timeout_profile)


def run(config: DwdSalesRetailItemConfig, *, conn_test_only: bool, execute: bool) -> None:
    if conn_test_only:
        conn_test(config.timeout_profile)
        return

    sql = build_insert_sql(config)
    logger.info('生成 dwd_sales_retail_item 候选 SQL，日期范围：%s - %s', config.start_date, config.end_date)
    print(sql)
    print({'start_date': config.start_date, 'end_date': config.end_date})

    if execute:
        summary = execute_load(config)
        print(summary)
    else:
        logger.info('未追加 --execute，仅完成 dry-run 输出；不会写入 MySQL。')


def parse_args() -> argparse.Namespace:
    default_start, default_end = _default_date_range()
    parser = argparse.ArgumentParser(description='DWD 销售零售明细事实旁路小窗口装载。')
    parser.add_argument('--start-date', type=int, default=default_start, help='开始日期 YYYYMMDD，默认近 7 天。')
    parser.add_argument('--end-date', type=int, default=default_end, help='结束日期 YYYYMMDD，默认今天。')
    parser.add_argument('--retail-table', default=DEFAULT_RETAIL_TABLE, help='零售单头 ODS 源表，默认 ods_m_retail_raw。')
    parser.add_argument('--retailitem-table', default=DEFAULT_RETAILITEM_TABLE, help='零售明细 ODS 源表，默认 ods_m_retailitem_raw。')
    parser.add_argument('--target-table', default=DEFAULT_TARGET_TABLE, help='DWD 目标表名。')
    parser.add_argument('--timeout-profile', choices=('default', 'etl', 'long_running'), default='etl')
    parser.add_argument('--conn-test', action='store_true', help='只执行 MySQL SELECT 1 连接测试。')
    parser.add_argument('--execute', action='store_true', help='显式执行 MySQL upsert；未提供时仅 dry-run。')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = DwdSalesRetailItemConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        retail_table=args.retail_table,
        retailitem_table=args.retailitem_table,
        target_table=args.target_table,
        timeout_profile=args.timeout_profile,
    )
    run(config, conn_test_only=args.conn_test, execute=args.execute)


if __name__ == '__main__':
    main()