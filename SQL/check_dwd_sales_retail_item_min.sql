-- DWD sales retail item minimum reconciliation SQL.
-- Status: 只读对账 SQL；不写库，不创建对象，不接入调度。
-- Scope: 对比 dwd_sales_retail_item 的当前 DWS 范围聚合与 dws_sales_daily。
-- Before running: 先由用户确认 raw ODS / DWD 小窗口装载已完成。
-- Default window: 近 1 天；如需指定日期，请手工改写 @start_date / @end_date。

SET @start_date = CAST(DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), '%Y%m%d') AS UNSIGNED);
SET @end_date = CAST(DATE_FORMAT(CURDATE(), '%Y%m%d') AS UNSIGNED);

-- 1. DWD 小窗口行数与范围标识概览。
SELECT
    date_id,
    COUNT(*) AS dwd_row_count,
    SUM(CASE WHEN dws_sales_scope_flag = 'Y' THEN 1 ELSE 0 END) AS dws_scope_row_count,
    SUM(CASE WHEN has_retail_header_flag = 'N' THEN 1 ELSE 0 END) AS missing_header_row_count,
    SUM(CASE WHEN has_sku_flag = 'N' THEN 1 ELSE 0 END) AS missing_sku_row_count,
    SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN 1 ELSE 0 END) AS positive_line_count,
    SUM(CASE WHEN is_return_flag = 'Y' THEN 1 ELSE 0 END) AS return_line_count,
    MIN(retail_modified_at) AS min_retail_modified_at,
    MAX(retail_modified_at) AS max_retail_modified_at,
    MIN(item_modified_at) AS min_item_modified_at,
    MAX(item_modified_at) AS max_item_modified_at,
    MIN(item_set_time) AS min_item_set_time,
    MAX(item_set_time) AS max_item_set_time
FROM dwd_sales_retail_item
WHERE date_id >= @start_date
  AND date_id <= @end_date
GROUP BY date_id
ORDER BY date_id;

-- 2. 主键重复检查；正常应返回 0 行。
SELECT
    retail_item_id,
    COUNT(*) AS duplicate_count
FROM dwd_sales_retail_item
WHERE date_id >= @start_date
  AND date_id <= @end_date
GROUP BY retail_item_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, retail_item_id
LIMIT 50;

-- 3. DWD 按当前 DWS 口径聚合后，与现有 dws_sales_daily 做小窗口差异检查。
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
        COUNT(DISTINCT CASE WHEN is_positive_sale_flag = 'Y' THEN retail_id END) AS order_count
    FROM dwd_sales_retail_item
    WHERE date_id >= @start_date
      AND date_id <= @end_date
      AND dws_sales_scope_flag = 'Y'
    GROUP BY date_id, store_id, product_id, m_productalias_id
), dws_scope AS (
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
    WHERE date_id >= @start_date
      AND date_id <= @end_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'DWD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count
    FROM dwd_scope
    UNION ALL
    SELECT 'DWS' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           sales_qty, sales_amount, sales_amount_list, return_qty, return_amount, order_count
    FROM dws_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_qty ELSE 0 END) AS dwd_sales_qty,
    SUM(CASE WHEN source_layer = 'DWS' THEN sales_qty ELSE 0 END) AS dws_sales_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_amount ELSE 0 END) AS dwd_sales_amount,
    SUM(CASE WHEN source_layer = 'DWS' THEN sales_amount ELSE 0 END) AS dws_sales_amount,
    SUM(CASE WHEN source_layer = 'DWD' THEN sales_amount_list ELSE 0 END) AS dwd_sales_amount_list,
    SUM(CASE WHEN source_layer = 'DWS' THEN sales_amount_list ELSE 0 END) AS dws_sales_amount_list,
    SUM(CASE WHEN source_layer = 'DWD' THEN return_qty ELSE 0 END) AS dwd_return_qty,
    SUM(CASE WHEN source_layer = 'DWS' THEN return_qty ELSE 0 END) AS dws_return_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN return_amount ELSE 0 END) AS dwd_return_amount,
    SUM(CASE WHEN source_layer = 'DWS' THEN return_amount ELSE 0 END) AS dws_return_amount,
    SUM(CASE WHEN source_layer = 'DWD' THEN order_count ELSE 0 END) AS dwd_order_count,
    SUM(CASE WHEN source_layer = 'DWS' THEN order_count ELSE 0 END) AS dws_order_count
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_sales_qty - dws_sales_qty) > 0.0001
    OR ABS(dwd_sales_amount - dws_sales_amount) > 0.01
    OR ABS(dwd_sales_amount_list - dws_sales_amount_list) > 0.01
    OR ABS(dwd_return_qty - dws_return_qty) > 0.0001
    OR ABS(dwd_return_amount - dws_return_amount) > 0.01
    OR ABS(dwd_order_count - dws_order_count) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;

-- 4. 差异总览；用于判断是否需要下钻第 3 段明细。
WITH dwd_scope AS (
    SELECT
        date_id,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(qty, 0) ELSE 0 END) AS sales_qty,
        SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_actual_amt, 0) ELSE 0 END) AS sales_amount,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(qty, 0)) ELSE 0 END) AS return_qty,
        SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(line_actual_amt, 0)) ELSE 0 END) AS return_amount
    FROM dwd_sales_retail_item
    WHERE date_id >= @start_date
      AND date_id <= @end_date
      AND dws_sales_scope_flag = 'Y'
    GROUP BY date_id
), dws_scope AS (
    SELECT
        date_id,
        SUM(COALESCE(sales_qty, 0)) AS sales_qty,
        SUM(COALESCE(sales_amount, 0)) AS sales_amount,
        SUM(COALESCE(return_qty, 0)) AS return_qty,
        SUM(COALESCE(return_amount, 0)) AS return_amount
    FROM dws_sales_daily
    WHERE date_id >= @start_date
      AND date_id <= @end_date
    GROUP BY date_id
)
SELECT
    COALESCE(dwd_scope.date_id, dws_scope.date_id) AS date_id,
    COALESCE(dwd_scope.sales_qty, 0) AS dwd_sales_qty,
    COALESCE(dws_scope.sales_qty, 0) AS dws_sales_qty,
    COALESCE(dwd_scope.sales_amount, 0) AS dwd_sales_amount,
    COALESCE(dws_scope.sales_amount, 0) AS dws_sales_amount,
    COALESCE(dwd_scope.return_qty, 0) AS dwd_return_qty,
    COALESCE(dws_scope.return_qty, 0) AS dws_return_qty,
    COALESCE(dwd_scope.return_amount, 0) AS dwd_return_amount,
    COALESCE(dws_scope.return_amount, 0) AS dws_return_amount
FROM dwd_scope
LEFT JOIN dws_scope ON dwd_scope.date_id = dws_scope.date_id
UNION ALL
SELECT
    dws_scope.date_id AS date_id,
    0 AS dwd_sales_qty,
    dws_scope.sales_qty AS dws_sales_qty,
    0 AS dwd_sales_amount,
    dws_scope.sales_amount AS dws_sales_amount,
    0 AS dwd_return_qty,
    dws_scope.return_qty AS dws_return_qty,
    0 AS dwd_return_amount,
    dws_scope.return_amount AS dws_return_amount
FROM dws_scope
LEFT JOIN dwd_scope ON dwd_scope.date_id = dws_scope.date_id
WHERE dwd_scope.date_id IS NULL
ORDER BY date_id;
