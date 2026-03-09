-- ============================================================================
-- 何方珠宝 - 库存健康度分析SQL v5.0 【SKU条码粒度 - 全渠道版】
-- ============================================================================
-- 更新日期：2026年1月29日
-- 版本说明：
--   v5.0 重大升级：从SPU货号粒度升级到SKU条码粒度
--   - 主键从 M_PRODUCT.ID → M_PRODUCT_ALIAS.ID
--   - 新增条码字段 M_PRODUCT_ALIAS.NO
--   - 库存/销售都按 M_PRODUCTALIAS_ID 聚合
--   - 保留SABC分类（S类前30%，A类30-70%，B类70-90%，C类90-100%+无销售）
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
-- CTE 1: 库存数据（中山总仓 + 云仓）- 按SKU聚合
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
        AND fs.M_PRODUCTALIAS_ID IS NOT NULL  -- 必须有条码
        AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
        AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
    GROUP BY 
        fs.M_PRODUCTALIAS_ID,
        p.ID, p.NAME, p.VALUE, 
        d4.ATTRIBNAME, d5.ATTRIBNAME, d6.ATTRIBNAME,
        p.FABELEMENT, p.PRICELIST
),

-- ============================================================================
-- CTE 2: 销售数据（近30天）- 电商 + 云仓门店 - 按SKU聚合
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
        AND ri.M_PRODUCTALIAS_ID IS NOT NULL  -- 必须有条码
        AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
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
-- S类：前30%  A类：30%-70%  B类：70%-90%  C类：90%-100%+无销售
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
    bd.类别,
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
-- 【SKU条码粒度 v5.0】核心改动说明
-- ============================================================================
/*
v5.0 重大升级 - 从SPU到SKU：

1. 粒度变化：
   v4.1: 一个商品编码(货号) = 1条记录 → SPU粒度
   v5.0: 一个商品编码(货号) = N条记录(N个条码) → SKU粒度

2. 主键变化：
   v4.1: M_PRODUCT.ID (product_id)
   v5.0: M_PRODUCT_ALIAS.ID (sku_id) + 条码字段(NO)

3. 聚合字段变化：
   库存: FA_STORAGE.M_PRODUCTALIAS_ID
   销售: M_RETAILITEM.M_PRODUCTALIAS_ID

4. 业务影响：
   - 现在可以看到同一款式不同颜色/尺寸的库存和销售情况
   - SABC分类是基于SKU级别的销售额排名
   - 库存预警、补货建议都是SKU级别的

示例对比：
┌────────────┬───────────┬────────┬────────┬────────┐
│ 版本       │ 商品编码  │ 颜色   │ 尺寸   │ 条数   │
├────────────┼───────────┼────────┼────────┼────────┤
│ v4.1 SPU   │ HF001     │ (合并) │ (合并) │ 1条    │
│ v5.0 SKU   │ HF001     │ 银色   │ 16寸   │ 1条    │
│            │ HF001     │ 金色   │ 16寸   │ 1条    │
│            │ HF001     │ 银色   │ 18寸   │ 1条    │
└────────────┴───────────┴────────┴────────┴────────┘
*/

-- ============================================================================
-- 口径说明（与v4.1保持一致）
-- ============================================================================
/*
库存口径：总仓(CODE='001') + 云仓门店(IS_ALLO2OSTORAGE='Y')
销售口径：电商(CODE LIKE 'DS%') + 云仓门店(IS_ALLO2OSTORAGE='Y')

SABC分类规则：
┌──────┬─────────────────┬─────────────┬─────────────────────────────┐
│ 分级 │ 累计销售占比    │ 说明        │ 管理策略                    │
├──────┼─────────────────┼─────────────┼─────────────────────────────┤
│ S类  │ 前30%           │ 超级爆款    │ 最高优先级，确保库存充足    │
│ A类  │ 30%-70%         │ 核心款      │ 重点监控，优先补货          │
│ B类  │ 70%-90%         │ 常规款      │ 定期检查，正常补货          │
│ C类  │ 90%-100%+无销售 │ 长尾/滞销   │ 降低关注，考虑清仓          │
└──────┴─────────────────┴─────────────┴─────────────────────────────┘
*/