-- ============================================================================
-- 何方珠宝 - 库存健康度分析SQL v6.0 【SKU条码粒度 - 全品类版】
-- ============================================================================
-- 更新日期：2026-01-30
-- 版本说明：
--   v6.0 全品类升级：在v5.0基础上移除类别限制
--   - 监控所有品类（包括辅销品、赠品、礼盒等）
--   - 满足运营监控辅销品缺货需求
--   - 保留v5.0的SKU粒度和所有计算逻辑
--   
--   变更内容：
--   ❌ 删除 AND p.M_DIM4_ID IN (134,142,...) 类别过滤
--   ✅ 监控所有有库存、有SKU的商品
--   
--   与v5.0对比：
--   v5.0: ~3,000 SKU（仅核心珠宝）
--   v6.0: ~8,000 SKU（全品类）
-- ============================================================================

WITH 
-- ============================================================================
-- CTE 0: SKU条码属性（每个条码都保留，不再去重）
-- ============================================================================
sku_attr AS (
    SELECT 
        pa.ID AS sku_id,
        pa.NO AS 条码,
        pa.M_PRODUCT_ID,
        asi.VALUE1 AS 颜色,
        asi.VALUE2 AS 尺寸
    FROM M_PRODUCT_ALIAS pa
        LEFT JOIN M_ATTRIBUTESETINSTANCE asi ON pa.M_ATTRIBUTESETINSTANCE_ID = asi.ID
    WHERE pa.ISACTIVE = 'Y'
),

-- ============================================================================
-- CTE 1: 库存数据（中山总仓 + 云仓）- 按SKU聚合 - ⭐ v6.0全品类
-- ============================================================================
stock AS (
    SELECT
        fs.M_PRODUCTALIAS_ID AS sku_id,
        p.ID AS product_id,
        p.NAME AS 商品编码,
        p.VALUE AS 商品名称,
        d4.ATTRIBNAME AS 类别,
        d5.ATTRIBNAME AS 性质,
        d6.ATTRIBNAME AS 系列,
        p.FABELEMENT AS 材质,
        p.PRICELIST AS 吊牌价,
        SUM(fs.QTY) AS 库存数量,
        SUM(fs.QTYPURCHASEREM) AS 未到货采购量,
        SUM(CASE WHEN s.CODE = '001' THEN fs.QTY ELSE 0 END) AS 总仓库存,
        SUM(CASE WHEN s.IS_ALLO2OSTORAGE = 'Y' THEN fs.QTY ELSE 0 END) AS 云仓库存
    FROM FA_STORAGE fs
        LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
        LEFT JOIN M_DIM d4 ON p.M_DIM4_ID = d4.ID
        LEFT JOIN M_DIM d5 ON p.M_DIM5_ID = d5.ID
        LEFT JOIN M_DIM d6 ON p.M_DIM6_ID = d6.ID
        LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
    WHERE 
        fs.ISACTIVE = 'Y'
        AND fs.M_PRODUCTALIAS_ID IS NOT NULL
        AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
        -- ⭐ v6.0: 移除类别限制，监控所有品类
        -- ❌ v5.0: AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
    GROUP BY 
        fs.M_PRODUCTALIAS_ID,
        p.ID, p.NAME, p.VALUE, 
        d4.ATTRIBNAME, d5.ATTRIBNAME, d6.ATTRIBNAME,
        p.FABELEMENT, p.PRICELIST
),

-- ============================================================================
-- CTE 2: 销售数据（近30天）- 电商 + 云仓门店 - 按SKU聚合 - ⭐ v6.0全品类
-- ============================================================================
sales AS (
    SELECT
        ri.M_PRODUCTALIAS_ID AS sku_id,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS 销售数量,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS 退货数量,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售金额,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 
                 AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-7, 'YYYYMMDD'))
            THEN ri.QTY ELSE 0 END) AS 近7天销售数量
    FROM M_RETAILITEM ri 
        LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
        LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
    WHERE
        r.ISACTIVE = 'Y'
        AND r.STATUS = 2
        AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
        AND ri.M_PRODUCTALIAS_ID IS NOT NULL
        AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
        -- ⭐ v6.0: 移除类别限制
        -- ❌ v5.0需要JOIN M_PRODUCT并过滤类别，v6.0不需要
    GROUP BY ri.M_PRODUCTALIAS_ID
),

-- ============================================================================
-- CTE 3: 合并库存和销售
-- ============================================================================
base_data AS (
    SELECT
        st.sku_id,
        st.product_id,
        ska.条码,
        st.商品编码,
        st.商品名称,
        st.类别,
        st.性质,
        st.系列,
        st.材质,
        ska.颜色,
        ska.尺寸,
        st.吊牌价,
        st.库存数量,
        st.总仓库存,
        st.云仓库存,
        st.未到货采购量,
        NVL(sa.销售数量, 0) AS 销售数量,
        NVL(sa.退货数量, 0) AS 退货数量,
        NVL(sa.销售金额, 0) AS 销售金额,
        NVL(sa.近7天销售数量, 0) AS 近7天销售数量
    FROM stock st
        LEFT JOIN sku_attr ska ON st.sku_id = ska.sku_id
        LEFT JOIN sales sa ON st.sku_id = sa.sku_id
),

-- ============================================================================
-- CTE 4: SABC分类（基于SKU销售金额排名）
-- ============================================================================
sabc_ranking AS (
    SELECT
        sku_id,
        销售金额,
        SUM(销售金额) OVER (ORDER BY 销售金额 DESC, sku_id 
                           ROWS UNBOUNDED PRECEDING) AS 累计销售金额,
        SUM(销售金额) OVER () AS 总销售金额,
        ROW_NUMBER() OVER (ORDER BY 销售金额 DESC, sku_id) AS 销售排名,
        COUNT(*) OVER () AS 总SKU数
    FROM base_data
),

sabc_class AS (
    SELECT
        sku_id,
        销售金额,
        累计销售金额,
        总销售金额,
        销售排名,
        总SKU数,
        CASE 
            WHEN 总销售金额 = 0 THEN 0
            ELSE ROUND(累计销售金额 / 总销售金额 * 100, 2)
        END AS 累计销售占比,
        CASE 
            WHEN 销售金额 = 0 THEN 'C'
            WHEN 总销售金额 = 0 THEN 'C'
            WHEN (累计销售金额 - 销售金额) / 总销售金额 < 0.30 THEN 'S'
            WHEN 累计销售金额 / 总销售金额 <= 0.70 THEN 'A'
            WHEN 累计销售金额 / 总销售金额 <= 0.90 THEN 'B'
            ELSE 'C'
        END AS SKU分级
    FROM sabc_ranking
)

-- ============================================================================
-- 主查询
-- ============================================================================
SELECT
    bd.sku_id AS SKU_ID,
    bd.条码,
    bd.product_id AS PRODUCT_ID,
    bd.商品编码,
    bd.商品名称,
    bd.类别,  -- ⭐ v6.0: 类别字段会包含所有类型（珠宝+辅销品+赠品等）
    bd.性质,
    bd.系列,
    bd.材质,
    bd.颜色,
    bd.尺寸,
    bd.吊牌价,
    
    sabc.SKU分级,
    sabc.销售排名,
    ROUND(bd.销售金额 / NULLIF(sabc.总销售金额, 0) * 100, 2) AS 销售额占比,
    sabc.累计销售占比,
    
    bd.库存数量,
    bd.总仓库存,
    bd.云仓库存,
    bd.未到货采购量,
    ROUND(bd.库存数量 * bd.吊牌价, 2) AS 库存金额,
    
    bd.销售数量 AS 近30天销售数量,
    bd.退货数量 AS 近30天退货数量,
    bd.销售数量 - bd.退货数量 AS 近30天净销量,
    ROUND(bd.销售金额, 2) AS 近30天销售金额,
    
    ROUND(bd.近7天销售数量 / 7, 1) AS 近7天日均销量,
    ROUND(bd.销售数量 / 30, 1) AS 近30天日均销量,
    
    CASE 
        WHEN bd.销售数量 = 0 THEN NULL
        ELSE ROUND((bd.近7天销售数量 / 7) / (bd.销售数量 / 30), 2)
    END AS 销售加速度,
    
    CASE 
        WHEN bd.销售数量 = 0 THEN '无销售'
        WHEN (bd.近7天销售数量 / 7) / (bd.销售数量 / 30) >= 1.3 THEN '快速上升'
        WHEN (bd.近7天销售数量 / 7) / (bd.销售数量 / 30) >= 1.0 THEN '稳定'
        WHEN (bd.近7天销售数量 / 7) / (bd.销售数量 / 30) >= 0.7 THEN '降温'
        ELSE '快速下滑'
    END AS 销售趋势,
    
    CASE 
        WHEN bd.销售数量 = 0 THEN 9999 
        ELSE ROUND(bd.库存数量 / (bd.销售数量 / 30), 1)
    END AS 库存周转天数,
    
    CASE 
        WHEN bd.库存数量 > 0 AND bd.销售数量 = 0 THEN '滞销'
        WHEN bd.库存数量 = 0 AND bd.销售数量 = 0 THEN '停售'
        WHEN bd.销售数量 > 0 
             AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) < 30 THEN '紧急缺货'
        WHEN bd.销售数量 > 0 
             AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) >= 30
             AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) < 70 THEN '需补货'
        WHEN bd.销售数量 > 0 
             AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) > 90 THEN '库存过高'
        ELSE '正常'
    END AS 库存状态,
    
    CASE 
        WHEN bd.销售数量 = 0 THEN 0
        WHEN ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) >= 90 THEN 0
        ELSE ROUND((90 - bd.库存数量 / (bd.销售数量 / 30)) * (bd.销售数量 / 30), 0) - bd.退货数量 - bd.未到货采购量
    END AS 建议补货数量

FROM base_data bd
    LEFT JOIN sabc_class sabc ON bd.sku_id = sabc.sku_id

ORDER BY 
    CASE sabc.SKU分级 WHEN 'S' THEN 1 WHEN 'A' THEN 2 WHEN 'B' THEN 3 ELSE 4 END,
    CASE 
        WHEN bd.库存数量 > 0 AND bd.销售数量 = 0 THEN 5
        WHEN bd.库存数量 = 0 AND bd.销售数量 = 0 THEN 6
        WHEN bd.销售数量 > 0 AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) < 30 THEN 1
        WHEN bd.销售数量 > 0 AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) < 70 THEN 2
        WHEN bd.销售数量 > 0 AND ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) > 90 THEN 4
        ELSE 3
    END,
    CASE WHEN bd.销售数量 = 0 THEN 9999 ELSE ROUND(bd.库存数量 / (bd.销售数量 / 30), 1) END;

-- ============================================================================
-- 【v6.0 升级说明】
-- ============================================================================
/*
v6.0 vs v5.0 核心差异： 

1. 移除类别限制：
   v5.0: WHERE p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
   v6.0: 无此限制，监控所有品类

2. 预期数据量变化：
   v5.0: ~3,000 SKU（仅核心珠宝）
   v6.0: ~8,000 SKU（全品类，包括辅销品/赠品/礼盒等）

3. 新增监控品类：
   - 礼盒、礼品盒
   - 赠品
   - 配件、辅料
   - 其他辅销品

4. 业务价值：
   - 满足运营监控辅销品缺货需求
   - 避免礼盒、赠品等高频缺货品类的遗漏
   - 完整的全品类库存健康度视图

5. 兼容性：
   - 所有计算逻辑与v5.0完全一致
   - 仅扩大监控范围，不改变算法
   - Tableau报表可直接使用，增加类别筛选器即可

6. 性能影响：
   - 数据量增加约2.5倍（3K→8K）
   - SQL执行时间预计增加20-30%
   - 仍在可接受范围内（预计<5秒）

7. 后续优化（可选）：
   如发现某些类别确实不需要监控，可添加排除条件：
   AND p.M_DIM4_ID NOT IN (xxx, xxx)  -- 排除包材、道具等
*/

-- ============================================================================
-- 【查询示例 - 按类别查看缺货情况】
-- ============================================================================
/*
-- 查看各类别的缺货SKU数量
SELECT 
    类别,
    COUNT(*) AS SKU数,
    SUM(CASE WHEN 库存状态 = '紧急缺货' THEN 1 ELSE 0 END) AS 紧急缺货数,
    SUM(CASE WHEN 库存状态 = '需补货' THEN 1 ELSE 0 END) AS 需补货数,
    SUM(建议补货数量) AS 总建议补货量
FROM (
    -- v6.0主查询
    ...
) 
GROUP BY 类别
ORDER BY 紧急缺货数 DESC, 需补货数 DESC;
*/