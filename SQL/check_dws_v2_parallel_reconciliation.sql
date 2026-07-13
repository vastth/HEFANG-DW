-- DWS v2 parallel reconciliation SQL.
-- Status: 只读对账 SQL；不写库，不创建对象，不接入调度。
-- Scope: 对比 dws_sales_daily_v2 / dws_inventory_daily_v2 与 DWD 聚合结果、现有生产 DWS 表。
-- Before running: dws_sales_daily_v2 / dws_inventory_daily_v2 已由用户人工建表并完成空表核验；etl_dws_sales_v2.py / etl_dws_inventory_v2.py 现已提供默认 dry-run / conn-test 与受确认令牌保护的 S3 手工写入分支。本 SQL 应在用户手工执行 --execute --confirm-write 且生成运行证据后作为额外只读对账使用。
-- Inventory S4 rule: 若要把库存 v2 与旧 dws_inventory_daily 做精确比较，必须先固定同一 source snapshot timepoint。建议先运行第 6A 段读取旧 DWS 当日 MAX(etl_time)，再把该值作为 etl_dws_inventory_v2.py 的 --source-loaded-at-cutoff 或 --align-with-old-dws 输入，重载同一天的 dws_inventory_daily_v2 后再执行第 6C 段。
-- Default sales window: 20260428-20260430（与 M3 完整业务日期验证窗口一致）。
-- Default inventory snapshot: 20260507（与 M3 full raw 初始化快照一致）。

SET @sales_start_date = 20260428;
SET @sales_end_date = 20260430;
SET @inventory_snapshot_date = 20260507;
SET @inventory_source_loaded_at_cutoff = NULL;

-- 1. 销售 DWS v2 行数与指标总览。
SELECT
    date_id,
    COUNT(*) AS dws_v2_rows,
    SUM(COALESCE(source_dwd_row_count, 0)) AS source_dwd_rows,
    SUM(COALESCE(sales_qty, 0)) AS sales_qty,
    SUM(COALESCE(sales_amount, 0)) AS sales_amount,
    SUM(COALESCE(sales_amount_list, 0)) AS sales_amount_list,
    SUM(COALESCE(return_qty, 0)) AS return_qty,
    SUM(COALESCE(return_amount, 0)) AS return_amount,
    SUM(COALESCE(order_count, 0)) AS order_count,
    MIN(etl_time) AS min_etl_time,
    MAX(etl_time) AS max_etl_time
FROM dws_sales_daily_v2
WHERE date_id BETWEEN @sales_start_date AND @sales_end_date
GROUP BY date_id
ORDER BY date_id;

-- 2. 销售 DWS v2 与 DWD 直接聚合差异；正常应返回 0 行。
WITH dwd_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(qty, 0) ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_actual_amt, 0) ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_list_amt, 0) ELSE 0 END) AS sales_amount_list,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(qty, 0)) ELSE 0 END) AS return_qty,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(line_actual_amt, 0)) ELSE 0 END) AS return_amount,
        COUNT(DISTINCT CASE WHEN is_positive_sale_flag = 'Y' THEN retail_id END) AS order_count,
        COUNT(*) AS source_dwd_row_count
    FROM dwd_sales_retail_item
    WHERE date_id BETWEEN @sales_start_date AND @sales_end_date
      AND dws_sales_scope_flag = 'Y'
    GROUP BY date_id, store_id, product_id, m_productalias_id
), v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        sales_qty,
        sales_amount,
        sales_amount_list,
        return_qty,
        return_amount,
        order_count,
        source_dwd_row_count
    FROM dws_sales_daily_v2
    WHERE date_id BETWEEN @sales_start_date AND @sales_end_date
), combined_scope AS (
    SELECT 'DWD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count, source_dwd_row_count
    FROM dwd_scope
    UNION ALL
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count, source_dwd_row_count
    FROM v2_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_qty ELSE 0 END) AS dwd_sales_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_qty ELSE 0 END) AS v2_sales_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_amount ELSE 0 END) AS dwd_sales_amount,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_amount ELSE 0 END) AS v2_sales_amount,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_amount_list ELSE 0 END) AS dwd_sales_amount_list,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_amount_list ELSE 0 END) AS v2_sales_amount_list,
    SUM(CASE WHEN source_layer = 'DWD' THEN return_qty ELSE 0 END) AS dwd_return_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN return_qty ELSE 0 END) AS v2_return_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN return_amount ELSE 0 END) AS dwd_return_amount,
    SUM(CASE WHEN source_layer = 'V2' THEN return_amount ELSE 0 END) AS v2_return_amount,
    SUM(CASE WHEN source_layer = 'DWD' THEN order_count ELSE 0 END) AS dwd_order_count,
    SUM(CASE WHEN source_layer = 'V2' THEN order_count ELSE 0 END) AS v2_order_count,
    SUM(CASE WHEN source_layer = 'DWD' THEN source_dwd_row_count ELSE 0 END) AS dwd_source_rows,
    SUM(CASE WHEN source_layer = 'V2' THEN source_dwd_row_count ELSE 0 END) AS v2_source_rows
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_sales_qty - v2_sales_qty) > 0.0001
    OR ABS(dwd_sales_amount - v2_sales_amount) > 0.01
    OR ABS(dwd_sales_amount_list - v2_sales_amount_list) > 0.01
    OR ABS(dwd_return_qty - v2_return_qty) > 0.0001
    OR ABS(dwd_return_amount - v2_return_amount) > 0.01
    OR ABS(dwd_order_count - v2_order_count) > 0.0001
    OR ABS(dwd_source_rows - v2_source_rows) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;

-- 3. 销售 DWS v2 与现有 dws_sales_daily 差异；用于并行切换前观察。
WITH v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(sales_qty, 0)) AS sales_qty,
        SUM(COALESCE(sales_amount, 0)) AS sales_amount,
        SUM(COALESCE(sales_amount_list, 0)) AS sales_amount_list,
        SUM(COALESCE(return_qty, 0)) AS return_qty,
        SUM(COALESCE(return_amount, 0)) AS return_amount,
        SUM(COALESCE(order_count, 0)) AS order_count
    FROM dws_sales_daily_v2
    WHERE date_id BETWEEN @sales_start_date AND @sales_end_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), old_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(sales_qty, 0)) AS sales_qty,
        SUM(COALESCE(sales_amount, 0)) AS sales_amount,
        SUM(COALESCE(sales_amount_list, 0)) AS sales_amount_list,
        SUM(COALESCE(return_qty, 0)) AS return_qty,
        SUM(COALESCE(return_amount, 0)) AS return_amount,
        SUM(COALESCE(order_count, 0)) AS order_count
    FROM dws_sales_daily
    WHERE date_id BETWEEN @sales_start_date AND @sales_end_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count
    FROM v2_scope
    UNION ALL
    SELECT 'OLD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count
    FROM old_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_qty ELSE 0 END) AS v2_sales_qty,
    SUM(CASE WHEN source_layer = 'OLD' THEN sales_qty ELSE 0 END) AS old_sales_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN sales_amount ELSE 0 END) AS v2_sales_amount,
    SUM(CASE WHEN source_layer = 'OLD' THEN sales_amount ELSE 0 END) AS old_sales_amount,
    SUM(CASE WHEN source_layer = 'V2' THEN return_qty ELSE 0 END) AS v2_return_qty,
    SUM(CASE WHEN source_layer = 'OLD' THEN return_qty ELSE 0 END) AS old_return_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN return_amount ELSE 0 END) AS v2_return_amount,
    SUM(CASE WHEN source_layer = 'OLD' THEN return_amount ELSE 0 END) AS old_return_amount,
    SUM(CASE WHEN source_layer = 'V2' THEN order_count ELSE 0 END) AS v2_order_count,
    SUM(CASE WHEN source_layer = 'OLD' THEN order_count ELSE 0 END) AS old_order_count
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(v2_sales_qty - old_sales_qty) > 0.0001
    OR ABS(v2_sales_amount - old_sales_amount) > 0.01
    OR ABS(v2_return_qty - old_return_qty) > 0.0001
    OR ABS(v2_return_amount - old_return_amount) > 0.01
    OR ABS(v2_order_count - old_order_count) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;

-- 4. 库存 DWS v2 行数与指标总览。
SELECT
    date_id,
    COUNT(*) AS dws_v2_rows,
    SUM(COALESCE(source_dwd_row_count, 0)) AS source_dwd_rows,
    SUM(COALESCE(qty, 0)) AS qty,
    SUM(COALESCE(qty_valid, 0)) AS qty_valid,
    SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
    SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem,
    SUM(COALESCE(qty_preout, 0)) AS qty_preout,
    SUM(COALESCE(qty_prein, 0)) AS qty_prein,
    SUM(COALESCE(qty_freeze, 0)) AS qty_freeze,
    SUM(COALESCE(qty_oms, 0)) AS qty_oms,
    SUM(COALESCE(qty_oms_translate, 0)) AS qty_oms_translate,
    SUM(COALESCE(qty_preout1, 0)) AS qty_preout1,
    MIN(etl_time) AS min_etl_time,
    MAX(etl_time) AS max_etl_time
FROM dws_inventory_daily_v2
WHERE date_id = @inventory_snapshot_date
GROUP BY date_id;

-- 5. 库存 DWS v2 与 DWD 直接聚合差异；正常应返回 0 行。
WITH dwd_scope AS (
    SELECT
        snapshot_date AS date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty, 0)) AS qty_valid,
        0 AS qty_occupy,
        SUM(COALESCE(qty_purchase_rem, 0)) AS qtypurchaserem,
        SUM(COALESCE(qty_preout, 0)) AS qty_preout,
        SUM(COALESCE(qty_prein, 0)) AS qty_prein,
        SUM(COALESCE(qty_freeze, 0)) AS qty_freeze,
        SUM(COALESCE(qty_oms, 0)) AS qty_oms,
        SUM(COALESCE(qty_oms_translate, 0)) AS qty_oms_translate,
        SUM(COALESCE(qty_preout1, 0)) AS qty_preout1,
        COUNT(*) AS source_dwd_row_count
    FROM dwd_inventory_storage_snapshot
    WHERE snapshot_date = @inventory_snapshot_date
      AND dws_inventory_scope_flag = 'Y'
    GROUP BY snapshot_date, store_id, product_id, m_productalias_id
), v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        qty,
        qty_valid,
        qty_occupy,
        qtypurchaserem,
        qty_preout,
        qty_prein,
        qty_freeze,
        qty_oms,
        qty_oms_translate,
        qty_preout1,
        source_dwd_row_count
    FROM dws_inventory_daily_v2
    WHERE date_id = @inventory_snapshot_date
), combined_scope AS (
    SELECT 'DWD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem, qty_preout, qty_prein, qty_freeze, qty_oms,
           qty_oms_translate, qty_preout1, source_dwd_row_count
    FROM dwd_scope
    UNION ALL
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem, qty_preout, qty_prein, qty_freeze, qty_oms,
           qty_oms_translate, qty_preout1, source_dwd_row_count
    FROM v2_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty ELSE 0 END) AS dwd_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN qty ELSE 0 END) AS v2_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_valid ELSE 0 END) AS dwd_qty_valid,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_valid ELSE 0 END) AS v2_qty_valid,
    SUM(CASE WHEN source_layer = 'DWD' THEN qtypurchaserem ELSE 0 END) AS dwd_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'V2' THEN qtypurchaserem ELSE 0 END) AS v2_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'DWD' THEN source_dwd_row_count ELSE 0 END) AS dwd_source_rows,
    SUM(CASE WHEN source_layer = 'V2' THEN source_dwd_row_count ELSE 0 END) AS v2_source_rows
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_qty - v2_qty) > 0.0001
    OR ABS(dwd_qty_valid - v2_qty_valid) > 0.0001
    OR ABS(dwd_qtypurchaserem - v2_qtypurchaserem) > 0.0001
    OR ABS(dwd_source_rows - v2_source_rows) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;

-- 6A. 旧 dws_inventory_daily 对齐基线探针；先取同日 old_dws_max_etl_time，再据此重跑库存 v2。
SELECT
    COUNT(*) AS old_dws_row_count,
    COUNT(DISTINCT etl_time) AS old_dws_distinct_etl_time_count,
    MIN(etl_time) AS old_dws_min_etl_time,
    MAX(etl_time) AS old_dws_max_etl_time,
    SUM(COALESCE(qty, 0)) AS old_dws_qty,
    SUM(COALESCE(qtypurchaserem, 0)) AS old_dws_qtypurchaserem
FROM dws_inventory_daily
WHERE date_id = @inventory_snapshot_date;

-- 6B. 旧 dws_inventory_daily 与“按 source_loaded_at cutoff 截断后的 DWD 聚合”差异。
-- 使用方式：先把 @inventory_source_loaded_at_cutoff 设为 6A 的 old_dws_max_etl_time，
-- 例如 SET @inventory_source_loaded_at_cutoff = '2026-05-07 04:31:36';
WITH dwd_aligned_scope AS (
    SELECT
        snapshot_date AS date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty, 0)) AS qty_valid,
        0 AS qty_occupy,
        SUM(COALESCE(qty_purchase_rem, 0)) AS qtypurchaserem
    FROM dwd_inventory_storage_snapshot
    WHERE snapshot_date = @inventory_snapshot_date
      AND dws_inventory_scope_flag = 'Y'
      AND source_loaded_at <= @inventory_source_loaded_at_cutoff
    GROUP BY snapshot_date, store_id, product_id, m_productalias_id
), old_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty_valid, 0)) AS qty_valid,
        SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
        SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem
    FROM dws_inventory_daily
    WHERE date_id = @inventory_snapshot_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'DWD_ALIGNED' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM dwd_aligned_scope
    UNION ALL
    SELECT 'OLD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM old_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD_ALIGNED' THEN qty ELSE 0 END) AS dwd_aligned_qty,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty ELSE 0 END) AS old_qty,
    SUM(CASE WHEN source_layer = 'DWD_ALIGNED' THEN qty_valid ELSE 0 END) AS dwd_aligned_qty_valid,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_valid ELSE 0 END) AS old_qty_valid,
    SUM(CASE WHEN source_layer = 'DWD_ALIGNED' THEN qty_occupy ELSE 0 END) AS dwd_aligned_qty_occupy,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_occupy ELSE 0 END) AS old_qty_occupy,
    SUM(CASE WHEN source_layer = 'DWD_ALIGNED' THEN qtypurchaserem ELSE 0 END) AS dwd_aligned_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'OLD' THEN qtypurchaserem ELSE 0 END) AS old_qtypurchaserem
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_aligned_qty - old_qty) > 0.0001
    OR ABS(dwd_aligned_qty_valid - old_qty_valid) > 0.0001
    OR ABS(dwd_aligned_qty_occupy - old_qty_occupy) > 0.0001
    OR ABS(dwd_aligned_qtypurchaserem - old_qtypurchaserem) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;

-- 6C. 库存 DWS v2 与现有 dws_inventory_daily 差异；仅在同一天的 dws_inventory_daily_v2 已按同一 source_loaded_at cutoff 全量删后重灌时使用。
WITH v2_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty_valid, 0)) AS qty_valid,
        SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
        SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem
    FROM dws_inventory_daily_v2
    WHERE date_id = @inventory_snapshot_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), old_scope AS (
    SELECT
        date_id,
        store_id,
        product_id,
        m_productalias_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty_valid, 0)) AS qty_valid,
        SUM(COALESCE(qty_occupy, 0)) AS qty_occupy,
        SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem
    FROM dws_inventory_daily
    WHERE date_id = @inventory_snapshot_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'V2' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM v2_scope
    UNION ALL
    SELECT 'OLD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM old_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'V2' THEN qty ELSE 0 END) AS v2_qty,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty ELSE 0 END) AS old_qty,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_valid ELSE 0 END) AS v2_qty_valid,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_valid ELSE 0 END) AS old_qty_valid,
    SUM(CASE WHEN source_layer = 'V2' THEN qty_occupy ELSE 0 END) AS v2_qty_occupy,
    SUM(CASE WHEN source_layer = 'OLD' THEN qty_occupy ELSE 0 END) AS old_qty_occupy,
    SUM(CASE WHEN source_layer = 'V2' THEN qtypurchaserem ELSE 0 END) AS v2_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'OLD' THEN qtypurchaserem ELSE 0 END) AS old_qtypurchaserem
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(v2_qty - old_qty) > 0.0001
    OR ABS(v2_qty_valid - old_qty_valid) > 0.0001
    OR ABS(v2_qty_occupy - old_qty_occupy) > 0.0001
    OR ABS(v2_qtypurchaserem - old_qtypurchaserem) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;
