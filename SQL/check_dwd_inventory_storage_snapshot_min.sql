-- DWD inventory storage snapshot minimum reconciliation SQL.
-- Status: 只读对账 SQL；不写库，不创建对象，不接入调度。
-- Scope: 对比 dwd_inventory_storage_snapshot 的当前 DWS 范围聚合与 dws_inventory_daily。
-- Before running: 先由用户确认 raw ODS / DWD 小窗口装载已完成。
-- Note: 新 DWD 草案不再保留源侧全量为 0 的 FA_STORAGE.QTYVALID；当前 DWS 的 qty_valid 等价于 qty，故用 DWD qty 生成 qty_valid 等价对照值。
-- Default snapshot: 今天；如需指定快照日，请手工改写 @snapshot_date。

SET @snapshot_date = CAST(DATE_FORMAT(CURDATE(), '%Y%m%d') AS UNSIGNED);

-- 1. DWD 快照行数、范围标识与库存信号概览。
SELECT
    snapshot_date,
    COUNT(*) AS dwd_row_count,
    SUM(CASE WHEN dws_inventory_scope_flag = 'Y' THEN 1 ELSE 0 END) AS dws_scope_row_count,
    SUM(CASE WHEN is_active_storage_flag = 'Y' THEN 1 ELSE 0 END) AS active_row_count,
    SUM(CASE WHEN has_sku_flag = 'N' THEN 1 ELSE 0 END) AS missing_sku_row_count,
    SUM(CASE WHEN zero_qty_kept_flag = 'Y' THEN 1 ELSE 0 END) AS zero_qty_row_count,
    SUM(CASE WHEN negative_qty_flag = 'Y' THEN 1 ELSE 0 END) AS negative_qty_row_count,
    SUM(COALESCE(qty, 0)) AS sum_qty,
    SUM(COALESCE(qty, 0)) AS sum_qty_valid_equiv,
    SUM(COALESCE(qty_purchase_rem, 0)) AS sum_qty_purchase_rem,
    SUM(COALESCE(qty_preout, 0)) AS sum_qty_preout,
    SUM(COALESCE(qty_prein, 0)) AS sum_qty_prein,
    SUM(COALESCE(qty_freeze, 0)) AS sum_qty_freeze,
    SUM(COALESCE(qty_oms, 0)) AS sum_qty_oms,
    SUM(COALESCE(qty_oms_translate, 0)) AS sum_qty_oms_translate,
    SUM(COALESCE(qty_preout1, 0)) AS sum_qty_preout1,
    MIN(storage_modified_at) AS min_storage_modified_at,
    MAX(storage_modified_at) AS max_storage_modified_at
FROM dwd_inventory_storage_snapshot
WHERE snapshot_date = @snapshot_date
GROUP BY snapshot_date;

-- 2. 主键重复检查；正常应返回 0 行。
SELECT
    snapshot_date,
    storage_id,
    COUNT(*) AS duplicate_count
FROM dwd_inventory_storage_snapshot
WHERE snapshot_date = @snapshot_date
GROUP BY snapshot_date, storage_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, storage_id
LIMIT 50;

-- 3. DWD 按当前 DWS 口径聚合后，与现有 dws_inventory_daily 做快照差异检查。
WITH dwd_scope AS (
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
    WHERE snapshot_date = @snapshot_date
      AND dws_inventory_scope_flag = 'Y'
    GROUP BY snapshot_date, store_id, product_id, m_productalias_id
), dws_scope AS (
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
    WHERE date_id = @snapshot_date
    GROUP BY date_id, store_id, product_id, m_productalias_id
), combined_scope AS (
    SELECT 'DWD' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM dwd_scope
    UNION ALL
    SELECT 'DWS' AS source_layer, date_id, store_id, product_id, m_productalias_id,
           qty, qty_valid, qty_occupy, qtypurchaserem
    FROM dws_scope
)
SELECT
    date_id,
    store_id,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty ELSE 0 END) AS dwd_qty,
    SUM(CASE WHEN source_layer = 'DWS' THEN qty ELSE 0 END) AS dws_qty,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_valid ELSE 0 END) AS dwd_qty_valid_equiv,
    SUM(CASE WHEN source_layer = 'DWS' THEN qty_valid ELSE 0 END) AS dws_qty_valid,
    SUM(CASE WHEN source_layer = 'DWD' THEN qty_occupy ELSE 0 END) AS dwd_qty_occupy,
    SUM(CASE WHEN source_layer = 'DWS' THEN qty_occupy ELSE 0 END) AS dws_qty_occupy,
    SUM(CASE WHEN source_layer = 'DWD' THEN qtypurchaserem ELSE 0 END) AS dwd_qtypurchaserem,
    SUM(CASE WHEN source_layer = 'DWS' THEN qtypurchaserem ELSE 0 END) AS dws_qtypurchaserem
FROM combined_scope
GROUP BY date_id, store_id, product_id, m_productalias_id
HAVING ABS(dwd_qty - dws_qty) > 0.0001
    OR ABS(dwd_qty_valid_equiv - dws_qty_valid) > 0.0001
    OR ABS(dwd_qty_occupy - dws_qty_occupy) > 0.0001
    OR ABS(dwd_qtypurchaserem - dws_qtypurchaserem) > 0.0001
ORDER BY date_id, store_id, product_id, m_productalias_id
LIMIT 200;

-- 4. 差异总览；用于判断是否需要下钻第 3 段明细。
WITH dwd_scope AS (
    SELECT
        snapshot_date AS date_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty, 0)) AS qty_valid,
        SUM(COALESCE(qty_purchase_rem, 0)) AS qtypurchaserem
    FROM dwd_inventory_storage_snapshot
    WHERE snapshot_date = @snapshot_date
      AND dws_inventory_scope_flag = 'Y'
    GROUP BY snapshot_date
), dws_scope AS (
    SELECT
        date_id,
        SUM(COALESCE(qty, 0)) AS qty,
        SUM(COALESCE(qty_valid, 0)) AS qty_valid,
        SUM(COALESCE(qtypurchaserem, 0)) AS qtypurchaserem
    FROM dws_inventory_daily
    WHERE date_id = @snapshot_date
    GROUP BY date_id
)
SELECT
    COALESCE(dwd_scope.date_id, dws_scope.date_id) AS date_id,
    COALESCE(dwd_scope.qty, 0) AS dwd_qty,
    COALESCE(dws_scope.qty, 0) AS dws_qty,
    COALESCE(dwd_scope.qty_valid, 0) AS dwd_qty_valid_equiv,
    COALESCE(dws_scope.qty_valid, 0) AS dws_qty_valid,
    COALESCE(dwd_scope.qtypurchaserem, 0) AS dwd_qtypurchaserem,
    COALESCE(dws_scope.qtypurchaserem, 0) AS dws_qtypurchaserem
FROM dwd_scope
LEFT JOIN dws_scope ON dwd_scope.date_id = dws_scope.date_id
UNION ALL
SELECT
    dws_scope.date_id AS date_id,
    0 AS dwd_qty,
    dws_scope.qty AS dws_qty,
    0 AS dwd_qty_valid_equiv,
    dws_scope.qty_valid AS dws_qty_valid,
    0 AS dwd_qtypurchaserem,
    dws_scope.qtypurchaserem AS dws_qtypurchaserem
FROM dws_scope
LEFT JOIN dwd_scope ON dwd_scope.date_id = dws_scope.date_id
WHERE dwd_scope.date_id IS NULL
ORDER BY date_id;

-- 5. DWD 保留但不进入当前 DWS 范围的库存信号，用于验证 raw/DWD 是否承接了全店仓事实。
SELECT
    dws_inventory_scope_flag,
    is_active_storage_flag,
    has_sku_flag,
    is_total_warehouse_flag,
    is_cloud_store_flag,
    COUNT(*) AS row_count,
    SUM(COALESCE(qty, 0)) AS sum_qty,
    SUM(COALESCE(qty, 0)) AS sum_qty_valid_equiv,
    SUM(COALESCE(qty_purchase_rem, 0)) AS sum_qty_purchase_rem,
    SUM(COALESCE(qty_preout, 0)) AS sum_qty_preout,
    SUM(COALESCE(qty_prein, 0)) AS sum_qty_prein,
    SUM(COALESCE(qty_freeze, 0)) AS sum_qty_freeze,
    SUM(COALESCE(qty_oms, 0)) AS sum_qty_oms,
    SUM(COALESCE(qty_oms_translate, 0)) AS sum_qty_oms_translate,
    SUM(COALESCE(qty_preout1, 0)) AS sum_qty_preout1
FROM dwd_inventory_storage_snapshot
WHERE snapshot_date = @snapshot_date
GROUP BY dws_inventory_scope_flag, is_active_storage_flag, has_sku_flag, is_total_warehouse_flag, is_cloud_store_flag
ORDER BY dws_inventory_scope_flag DESC, row_count DESC;
