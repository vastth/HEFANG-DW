-- ============================================================================
-- 库存健康度分析 - SPU vs SKU粒度数据验证脚本
-- ============================================================================
-- 用途：快速验证v4.1（SPU）和v5.0（SKU）的数据差异
-- 使用：在MySQL或Oracle中执行，对比两个版本的关键指标
-- ============================================================================

-- ============================================================================
-- 验证1: 数据量对比
-- ============================================================================
-- 检查一个货号有多少个SKU
SELECT 
    p.NAME AS 商品编码,
    p.VALUE AS 商品名称,
    COUNT(pa.ID) AS SKU数量,
    GROUP_CONCAT(pa.NO ORDER BY pa.NO SEPARATOR ', ') AS 条码列表
FROM M_PRODUCT p
    LEFT JOIN M_PRODUCT_ALIAS pa ON p.ID = pa.M_PRODUCT_ID AND pa.ISACTIVE = 'Y'
WHERE p.NAME = 'HFM03705864-4'  -- 👈 替换为你要验证的货号
GROUP BY p.ID, p.NAME, p.VALUE;

/*
预期结果示例：
┌───────────────────┬──────────────┬──────┬────────────────────────────────────────┐
│ 商品编码          │ 商品名称     │ SKU  │ 条码列表                               │
├───────────────────┼──────────────┼──────┼────────────────────────────────────────┤
│ HFM03705864-4     │ 气泡方糖项链 │ 3    │ 6973130021301, 6973130021302, ...      │
└───────────────────┴──────────────┴──────┴────────────────────────────────────────┘
*/

-- ============================================================================
-- 验证2: 库存数据对比（SPU vs SKU）
-- ============================================================================
-- SPU粒度：货号级别库存
SELECT 
    p.NAME AS 商品编码,
    SUM(fs.QTY) AS 总库存_SPU粒度
FROM FA_STORAGE fs
    LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
WHERE 
    p.NAME = 'HFM03705864-4'  -- 👈 替换为你要验证的货号
    AND fs.ISACTIVE = 'Y'
GROUP BY p.NAME;

-- SKU粒度：条码级别库存
SELECT 
    pa.NO AS 条码,
    p.NAME AS 商品编码,
    asi.VALUE1 AS 颜色,
    asi.VALUE2 AS 尺寸,
    SUM(fs.QTY) AS 库存_SKU粒度
FROM FA_STORAGE fs
    LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
    LEFT JOIN M_PRODUCT_ALIAS pa ON fs.M_PRODUCTALIAS_ID = pa.ID
    LEFT JOIN M_ATTRIBUTESETINSTANCE asi ON pa.M_ATTRIBUTESETINSTANCE_ID = asi.ID
WHERE 
    p.NAME = 'HFM03705864-4'  -- 👈 替换为你要验证的货号
    AND fs.ISACTIVE = 'Y'
GROUP BY pa.NO, p.NAME, asi.VALUE1, asi.VALUE2
ORDER BY pa.NO;

/*
预期结果：SKU粒度的库存合计 = SPU粒度的库存
如果不相等，说明SQL有问题！
*/

-- ============================================================================
-- 验证3: 销售数据对比（SPU vs SKU）
-- ============================================================================
-- SPU粒度：货号级别销售
SELECT 
    p.NAME AS 商品编码,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS 销售数量_SPU粒度,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS 退货数量_SPU粒度
FROM M_RETAILITEM ri
    LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
WHERE 
    p.NAME = 'HFM03705864-4'  -- 👈 替换为你要验证的货号
    AND r.ISACTIVE = 'Y'
    AND r.STATUS = 2
    AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
GROUP BY p.NAME;

-- SKU粒度：条码级别销售
SELECT 
    pa.NO AS 条码,
    p.NAME AS 商品编码,
    asi.VALUE1 AS 颜色,
    asi.VALUE2 AS 尺寸,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS 销售数量_SKU粒度,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS 退货数量_SKU粒度
FROM M_RETAILITEM ri
    LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    LEFT JOIN M_PRODUCT_ALIAS pa ON ri.M_PRODUCTALIAS_ID = pa.ID
    LEFT JOIN M_ATTRIBUTESETINSTANCE asi ON pa.M_ATTRIBUTESETINSTANCE_ID = asi.ID
WHERE 
    p.NAME = 'HFM03705864-4'  -- 👈 替换为你要验证的货号
    AND r.ISACTIVE = 'Y'
    AND r.STATUS = 2
    AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
GROUP BY pa.NO, p.NAME, asi.VALUE1, asi.VALUE2
ORDER BY pa.NO;

/*
预期结果：SKU粒度的销售合计 = SPU粒度的销售
如果不相等，说明SQL有问题！
*/

-- ============================================================================
-- 验证4: 全局数据量对比
-- ============================================================================
-- 统计总记录数差异
SELECT 
    '全库统计' AS 说明,
    (SELECT COUNT(DISTINCT M_PRODUCT_ID) FROM FA_STORAGE WHERE ISACTIVE = 'Y') AS SPU数量,
    (SELECT COUNT(DISTINCT M_PRODUCTALIAS_ID) FROM FA_STORAGE WHERE ISACTIVE = 'Y' AND M_PRODUCTALIAS_ID IS NOT NULL) AS SKU数量,
    ROUND(
        (SELECT COUNT(DISTINCT M_PRODUCTALIAS_ID) FROM FA_STORAGE WHERE ISACTIVE = 'Y' AND M_PRODUCTALIAS_ID IS NOT NULL) * 1.0 /
        (SELECT COUNT(DISTINCT M_PRODUCT_ID) FROM FA_STORAGE WHERE ISACTIVE = 'Y'),
        2
    ) AS 平均每个货号有多少SKU;

/*
预期结果示例：
┌──────────┬──────────┬──────────┬─────────────────────┐
│ 说明     │ SPU数量  │ SKU数量  │ 平均每个货号有多少SKU│
├──────────┼──────────┼──────────┼─────────────────────┤
│ 全库统计 │ 10,000   │ 28,500   │ 2.85                │
└──────────┴──────────┴──────────┴─────────────────────┘
*/

-- ============================================================================
-- 验证5: 缺失数据检查
-- ============================================================================
-- 检查是否有SKU没有颜色尺寸信息
SELECT 
    pa.ID AS sku_id,
    pa.NO AS 条码,
    p.NAME AS 商品编码,
    asi.VALUE1 AS 颜色,
    asi.VALUE2 AS 尺寸,
    CASE 
        WHEN asi.VALUE1 IS NULL AND asi.VALUE2 IS NULL THEN '⚠️ 缺失颜色和尺寸'
        WHEN asi.VALUE1 IS NULL THEN '⚠️ 缺失颜色'
        WHEN asi.VALUE2 IS NULL THEN '⚠️ 缺失尺寸'
        ELSE '✓ 完整'
    END AS 数据完整性
FROM M_PRODUCT_ALIAS pa
    LEFT JOIN M_PRODUCT p ON pa.M_PRODUCT_ID = p.ID
    LEFT JOIN M_ATTRIBUTESETINSTANCE asi ON pa.M_ATTRIBUTESETINSTANCE_ID = asi.ID
WHERE 
    pa.ISACTIVE = 'Y'
    AND (asi.VALUE1 IS NULL OR asi.VALUE2 IS NULL)
LIMIT 10;

/*
如果有很多缺失，需要考虑：
1. 是否所有商品都有颜色尺寸？（如礼盒可能没有）
2. 数据质量问题需要修复
*/

-- ============================================================================
-- 验证6: SABC分类对比
-- ============================================================================
-- SPU粒度SABC分类
WITH spu_sales AS (
    SELECT 
        p.ID AS product_id,
        p.NAME AS 商品编码,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售金额
    FROM M_RETAILITEM ri
        LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
        LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    WHERE 
        r.ISACTIVE = 'Y'
        AND r.STATUS = 2
        AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
    GROUP BY p.ID, p.NAME
),
spu_ranking AS (
    SELECT 
        商品编码,
        销售金额,
        ROW_NUMBER() OVER (ORDER BY 销售金额 DESC) AS 排名,
        COUNT(*) OVER () AS 总数
    FROM spu_sales
)
SELECT 
    商品编码,
    销售金额,
    排名,
    CASE 
        WHEN 排名 <= 总数 * 0.3 THEN 'S'
        WHEN 排名 <= 总数 * 0.7 THEN 'A'
        WHEN 排名 <= 总数 * 0.9 THEN 'B'
        ELSE 'C'
    END AS SABC分级_SPU粒度
FROM spu_ranking
WHERE 商品编码 = 'HFM03705864-4';  -- 👈 替换为你要验证的货号

-- SKU粒度SABC分类
WITH sku_sales AS (
    SELECT 
        pa.ID AS sku_id,
        pa.NO AS 条码,
        p.NAME AS 商品编码,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售金额
    FROM M_RETAILITEM ri
        LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
        LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
        LEFT JOIN M_PRODUCT_ALIAS pa ON ri.M_PRODUCTALIAS_ID = pa.ID
    WHERE 
        r.ISACTIVE = 'Y'
        AND r.STATUS = 2
        AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
    GROUP BY pa.ID, pa.NO, p.NAME
),
sku_ranking AS (
    SELECT 
        条码,
        商品编码,
        销售金额,
        ROW_NUMBER() OVER (ORDER BY 销售金额 DESC) AS 排名,
        COUNT(*) OVER () AS 总数
    FROM sku_sales
)
SELECT 
    条码,
    商品编码,
    销售金额,
    排名,
    CASE 
        WHEN 排名 <= 总数 * 0.3 THEN 'S'
        WHEN 排名 <= 总数 * 0.7 THEN 'A'
        WHEN 排名 <= 总数 * 0.9 THEN 'B'
        ELSE 'C'
    END AS SABC分级_SKU粒度
FROM sku_ranking
WHERE 商品编码 = 'HFM03705864-4';  -- 👈 替换为你要验证的货号

/*
预期结果：
- SPU粒度：整个货号只有1个SABC等级
- SKU粒度：同一货号下的不同SKU可能有不同SABC等级

示例：
┌─────────────────┬──────────┬────────────────┐
│ 条码            │ 销售金额 │ SABC分级       │
├─────────────────┼──────────┼────────────────┤
│ 6973130021302   │ 50,000   │ S (超级爆款)   │
│ 6973130021301   │ 30,000   │ A (核心款)     │
│ 6973130021303   │ 5,000    │ C (长尾款)     │
└─────────────────┴──────────┴────────────────┘
*/

-- ============================================================================
-- 验证7: 性能对比（可选）
-- ============================================================================
-- 执行v4.1 SPU粒度SQL，记录执行时间
-- 执行v5.0 SKU粒度SQL，记录执行时间
-- 预期：v5.0会慢10-30%

/*
性能优化建议：
1. 确保FA_STORAGE.M_PRODUCTALIAS_ID有索引
2. 确保M_RETAILITEM.M_PRODUCTALIAS_ID有索引
3. 考虑在数仓ETL时预聚合SKU数据
*/

-- ============================================================================
-- 验证完成！
-- ============================================================================
/*
通过以上验证，你应该能够确认：
1. ✓ SKU数量是SPU数量的2-3倍
2. ✓ SKU粒度的库存/销售合计 = SPU粒度的库存/销售
3. ✓ SKU粒度能看到颜色/尺寸明细
4. ✓ SKU粒度的SABC分类更精细
5. ✓ 数据完整性良好（颜色尺寸不缺失）

如果验证通过，可以放心使用v5.0 SKU粒度SQL！
*/
