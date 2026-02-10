-- ============================================================================
-- SPU vs SKU 粒度验证（MySQL - 基于DWS层）
-- 依赖表：dws_inventory_daily, dws_sales_daily
-- 可选：dim_product_attr（用于颜色/尺寸完整性）
-- ============================================================================

-- 建议先设置日期参数
SET @inv_date := (SELECT MAX(date_id) FROM dws_inventory_daily);
SET @sales_end := (SELECT MAX(date_id) FROM dws_sales_daily);
SET @sales_start := DATE_FORMAT(DATE_SUB(STR_TO_DATE(@sales_end, '%Y%m%d'), INTERVAL 29 DAY), '%Y%m%d');

-- ============================================================================
-- 验证1：SKU数量是否为SPU数量的2-3倍
-- ============================================================================
SELECT
    @inv_date AS snapshot_date,
    COUNT(DISTINCT product_id) AS spu_count,
    COUNT(DISTINCT m_productalias_id) AS sku_count,
    ROUND(COUNT(DISTINCT m_productalias_id) / NULLIF(COUNT(DISTINCT product_id), 0), 2) AS sku_spu_ratio
FROM dws_inventory_daily
WHERE date_id = @inv_date
  AND m_productalias_id IS NOT NULL;

-- ============================================================================
-- 验证2：同一货号下 SKU 库存合计 = SPU 粒度库存（缺失SKU会导致不相等）
-- ============================================================================
WITH spu AS (
    SELECT
        product_id,
        SUM(qty) AS spu_qty
    FROM dws_inventory_daily
    WHERE date_id = @inv_date
    GROUP BY product_id
),
sku AS (
    SELECT
        product_id,
        SUM(qty) AS sku_qty
    FROM dws_inventory_daily
    WHERE date_id = @inv_date
      AND m_productalias_id IS NOT NULL
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.spu_qty,
    k.sku_qty,
    (k.sku_qty - s.spu_qty) AS diff_qty
FROM spu s
LEFT JOIN sku k ON s.product_id = k.product_id
WHERE COALESCE(k.sku_qty, 0) <> s.spu_qty
ORDER BY ABS(k.sku_qty - s.spu_qty) DESC
LIMIT 50;

-- ============================================================================
-- 验证3：同一货号下 SKU 销售合计 = SPU 粒度销售（近30天）
-- ============================================================================
WITH spu AS (
    SELECT
        product_id,
        SUM(sales_qty) AS spu_sales_qty,
        SUM(sales_amount) AS spu_sales_amt,
        SUM(return_qty) AS spu_return_qty,
        SUM(return_amount) AS spu_return_amt
    FROM dws_sales_daily
    WHERE date_id BETWEEN @sales_start AND @sales_end
    GROUP BY product_id
),
sku AS (
    SELECT
        product_id,
        SUM(sales_qty) AS sku_sales_qty,
        SUM(sales_amount) AS sku_sales_amt,
        SUM(return_qty) AS sku_return_qty,
        SUM(return_amount) AS sku_return_amt
    FROM dws_sales_daily
    WHERE date_id BETWEEN @sales_start AND @sales_end
      AND m_productalias_id IS NOT NULL
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.spu_sales_qty,
    k.sku_sales_qty,
    (k.sku_sales_qty - s.spu_sales_qty) AS diff_sales_qty,
    s.spu_sales_amt,
    k.sku_sales_amt,
    ROUND(k.sku_sales_amt - s.spu_sales_amt, 2) AS diff_sales_amt,
    s.spu_return_qty,
    k.sku_return_qty,
    (k.sku_return_qty - s.spu_return_qty) AS diff_return_qty
FROM spu s
LEFT JOIN sku k ON s.product_id = k.product_id
WHERE COALESCE(k.sku_sales_qty, 0) <> s.spu_sales_qty
   OR COALESCE(k.sku_sales_amt, 0) <> s.spu_sales_amt
   OR COALESCE(k.sku_return_qty, 0) <> s.spu_return_qty
ORDER BY ABS(COALESCE(k.sku_sales_qty, 0) - s.spu_sales_qty) DESC
LIMIT 50;

-- ============================================================================
-- 验证4：颜色/尺寸字段完整性（基于 dim_product_attr）
-- 说明：目前DWS仅有 product_id + m_productalias_id，未落SKU级颜色/尺寸。
--       这里先检查SPU级颜色/尺寸完整性，用于间接评估。
--       若未来引入 dim_sku（含sku_id/颜色/尺寸），请改为SKU级核验。
-- ============================================================================
SELECT
    @inv_date AS snapshot_date,
    COUNT(DISTINCT i.product_id) AS product_count,
    COUNT(DISTINCT CASE WHEN pa.color IS NOT NULL AND pa.color <> '' THEN i.product_id END) AS product_with_color,
    COUNT(DISTINCT CASE WHEN pa.size IS NOT NULL AND pa.size <> '' THEN i.product_id END) AS product_with_size,
    COUNT(DISTINCT CASE WHEN pa.color IS NOT NULL AND pa.color <> '' AND pa.size IS NOT NULL AND pa.size <> '' THEN i.product_id END) AS product_with_color_size,
    ROUND(COUNT(DISTINCT CASE WHEN pa.color IS NOT NULL AND pa.color <> '' THEN i.product_id END) / NULLIF(COUNT(DISTINCT i.product_id), 0), 4) AS color_fill_rate,
    ROUND(COUNT(DISTINCT CASE WHEN pa.size IS NOT NULL AND pa.size <> '' THEN i.product_id END) / NULLIF(COUNT(DISTINCT i.product_id), 0), 4) AS size_fill_rate,
    ROUND(COUNT(DISTINCT CASE WHEN pa.color IS NOT NULL AND pa.color <> '' AND pa.size IS NOT NULL AND pa.size <> '' THEN i.product_id END) / NULLIF(COUNT(DISTINCT i.product_id), 0), 4) AS color_size_fill_rate
FROM dws_inventory_daily i
LEFT JOIN dim_product_attr pa ON i.product_id = pa.product_id
WHERE i.date_id = @inv_date;

-- ============================================================================
-- 结束
-- ============================================================================
