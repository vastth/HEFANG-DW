WITH 
-- ============================================================================
-- 【CTE 1】渠道映射表（修正版）
-- ============================================================================
channel_mapping AS (
    SELECT 'DS001' AS CODE, '天猫旗舰店' AS 渠道名称, '一组' AS 组别, 1 AS 排序 FROM DUAL
    UNION ALL SELECT 'DS019', '天猫奥莱店', '一组', 2 FROM DUAL
    UNION ALL SELECT 'DS031', '天猫国际直营店', '一组', 3 FROM DUAL
    UNION ALL SELECT 'DS009', '抖音', '二组', 4 FROM DUAL
    UNION ALL SELECT 'DS002', '京东POP店', '三组', 5 FROM DUAL
    UNION ALL SELECT 'DS030', '京东自营店', '三组', 6 FROM DUAL
    UNION ALL SELECT 'DS006', '小红书', '三组', 7 FROM DUAL
    UNION ALL SELECT 'DS024', '视频号', '三组', 8 FROM DUAL
    UNION ALL SELECT 'DS011', '唯品会', '三组', 9 FROM DUAL
    UNION ALL SELECT 'DS015', '得物', '三组', 10 FROM DUAL
    UNION ALL SELECT 'DS032', '得物', '三组', 10 FROM DUAL
    UNION ALL SELECT 'DS008', '散客', '散客', 11 FROM DUAL
),

-- ============================================================================
-- 【CTE 2】主销品销售明细
-- ============================================================================
main_product_sales AS (
    SELECT 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END AS 渠道编码,
        
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS 总销_数量,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_LIST ELSE 0 END) AS 总销_吊牌额,
        SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 总销_让利后金额,
        
        -SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS 退货_数量,
        -SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_LIST) ELSE 0 END) AS 退货_吊牌额,
        -SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) AS 退货_让利后金额,
        
        SUM(ri.QTY) AS 实销_数量,
        SUM(ri.TOT_AMT_LIST) AS 实销_吊牌额,
        SUM(ri.TOT_AMT_ACTUAL) AS 实销_让利后金额
        
    FROM BOSNDS3.M_RETAILITEM ri
    LEFT JOIN BOSNDS3.M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN BOSNDS3.M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    LEFT JOIN BOSNDS3.M_DIM d4 ON p.M_DIM4_ID = d4.ID
    LEFT JOIN BOSNDS3.C_STORE s ON r.C_STORE_ID = s.ID
    
    WHERE 
        r.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYYMMDD'))
        AND r.BILLDATE <  TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
        AND r.ISACTIVE = 'Y'
        AND r.STATUS = 2
        AND s.CODE IN ('DS001', 'DS019', 'DS031', 'DS009', 'DS002', 
                       'DS030', 'DS006', 'DS024', 'DS011', 'DS015', 
                       'DS032', 'DS008')
        AND (d4.ATTRIBNAME NOT IN ('辅料', '辅销品', '办公用品') 
             OR d4.ATTRIBNAME IS NULL)
    
    GROUP BY 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END
),

-- ============================================================================
-- 【CTE 3】赠品（辅销品）销售明细
-- ============================================================================
gift_sales AS (
    SELECT 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END AS 渠道编码,
        
        SUM(ri.QTY) AS 赠品_数量,
        SUM(ri.TOT_AMT_LIST) AS 赠品_吊牌额
        
    FROM BOSNDS3.M_RETAILITEM ri
    LEFT JOIN BOSNDS3.M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN BOSNDS3.M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    LEFT JOIN BOSNDS3.M_DIM d4 ON p.M_DIM4_ID = d4.ID
    LEFT JOIN BOSNDS3.C_STORE s ON r.C_STORE_ID = s.ID
    
    WHERE 
        r.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYYMMDD'))
        AND r.BILLDATE <  TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
        AND r.ISACTIVE = 'Y'
        AND r.STATUS = 2
        AND s.CODE IN ('DS001', 'DS019', 'DS031', 'DS009', 'DS002', 
                       'DS030', 'DS006', 'DS024', 'DS011', 'DS015', 
                       'DS032', 'DS008')
        AND d4.ATTRIBNAME = '辅销品'
    
    GROUP BY 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END
),

-- ============================================================================
-- 【CTE 4】绝版款销售明细
-- ============================================================================
vintage_sales AS (
    SELECT 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END AS 渠道编码,
        
        SUM(ri.QTY) AS 绝版款_数量,
        SUM(ri.TOT_AMT_LIST) AS 绝版款_吊牌额,
        SUM(ri.TOT_AMT_ACTUAL) AS 绝版款_让利后金额
        
    FROM BOSNDS3.M_RETAILITEM ri
    LEFT JOIN BOSNDS3.M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN BOSNDS3.M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    LEFT JOIN BOSNDS3.M_DIM d4 ON p.M_DIM4_ID = d4.ID
    LEFT JOIN BOSNDS3.M_DIM d5 ON p.M_DIM5_ID = d5.ID
    LEFT JOIN BOSNDS3.C_STORE s ON r.C_STORE_ID = s.ID
    
    WHERE 
        r.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYYMMDD'))
        AND r.BILLDATE <  TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
        AND r.ISACTIVE = 'Y'
        AND r.STATUS = 2
        AND s.CODE IN ('DS001', 'DS019', 'DS031', 'DS009', 'DS002', 
                       'DS030', 'DS006', 'DS024', 'DS011', 'DS015', 
                       'DS032', 'DS008')
        AND (d4.ATTRIBNAME NOT IN ('辅料', '辅销品', '办公用品') 
             OR d4.ATTRIBNAME IS NULL)
        AND d5.ATTRIBNAME IN ('绝版款-线上', '绝版款-线下', '绝版款-同步')
    
    GROUP BY 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END
)
-- ============================================================================
-- 【主查询】合并所有数据，计算派生指标
-- ============================================================================
SELECT 
    -- 组别和渠道
    CASE 
        WHEN m.渠道编码 = 'DS001' THEN '一组'
				WHEN m.渠道编码 = 'DS002' THEN '一组'
				WHEN m.渠道编码 = 'DS011' THEN '一组'
        WHEN m.渠道编码 = 'DS019' THEN '一组'
				WHEN m.渠道编码 = 'DS030' THEN '一组'
        WHEN m.渠道编码 = 'DS031' THEN '一组'
        WHEN m.渠道编码 IN ('DS009' ,'DS006', 'DS024', '得物') THEN '二组'
        WHEN m.渠道编码 = 'DS008' THEN '散客'
        ELSE '其他'
    END AS 组别,
    
    CASE 
        WHEN m.渠道编码 = 'DS001' THEN '天猫旗舰店'
        WHEN m.渠道编码 = 'DS019' THEN '天猫奥莱店'
        WHEN m.渠道编码 = 'DS031' THEN '天猫国际直营店'
        WHEN m.渠道编码 = 'DS009' THEN '抖音'
        WHEN m.渠道编码 = 'DS002' THEN '京东POP店'
        WHEN m.渠道编码 = 'DS030' THEN '京东自营店'
        WHEN m.渠道编码 = 'DS006' THEN '小红书'
        WHEN m.渠道编码 = 'DS024' THEN '视频号'
        WHEN m.渠道编码 = 'DS011' THEN '唯品会'
        WHEN m.渠道编码 = '得物' THEN '得物'
        WHEN m.渠道编码 = 'DS008' THEN '散客'
        ELSE m.渠道编码
    END AS 渠道,
    
    -- 【主销品-总销】
    NVL(m.总销_数量, 0) AS "主销品(总销)-数量",
    ROUND(NVL(m.总销_吊牌额, 0), 2) AS "主销品(总销)-吊牌额",
    ROUND(NVL(m.总销_让利后金额, 0), 2) AS "主销品(总销)-让利后金额",
    
    -- 【主销品-退货】
    NVL(m.退货_数量, 0) AS "主销品(退货)-数量",
    ROUND(NVL(m.退货_吊牌额, 0), 2) AS "主销品(退货)-吊牌额",
    ROUND(NVL(m.退货_让利后金额, 0), 2) AS "主销品(退货)-让利后金额",
    
    -- 退货率
    CASE 
        WHEN NVL(m.总销_数量, 0) = 0 THEN 0
        ELSE ROUND(m.退货_数量 / m.总销_数量, 6)
    END AS "主销品(退货)-退货率",
    
    -- 【主销品-实销】
    NVL(m.实销_数量, 0) AS "主销品(实销)-数量",
    ROUND(NVL(m.实销_吊牌额, 0), 2) AS "主销品(实销)-吊牌额",
    ROUND(NVL(m.实销_让利后金额, 0), 2) AS "主销品(实销)-让利后金额",
    
    -- 成本 = 吊牌额 × 35%
    ROUND(NVL(m.实销_吊牌额, 0) * 0.35, 2) AS "主销品(实销)-成本",
    
    -- 毛利额 = 让利后金额 - 成本
    ROUND(
        NVL(m.实销_让利后金额, 0) 
        - NVL(m.实销_吊牌额, 0) * 0.35
    , 2) AS "主销品(实销)-毛利额",
    
    -- 毛利率
    CASE 
        WHEN NVL(m.实销_让利后金额, 0) = 0 THEN NULL
        ELSE ROUND(
            (m.实销_让利后金额 - m.实销_吊牌额 * 0.35) 
            / m.实销_让利后金额
        , 6)
    END AS "主销品(实销)-毛利率",
    
    -- 【赠品-实销】
    NVL(g.赠品_数量, 0) AS "赠品(实销)-数量",
    ROUND(NVL(g.赠品_吊牌额, 0), 2) AS "赠品(实销)-吊牌额",
    ROUND(NVL(g.赠品_吊牌额, 0) * 0.25, 2) AS "赠品(实销)-成本",
    
    -- 【主销品+赠品-实销】
    NVL(m.实销_数量, 0) + NVL(g.赠品_数量, 0) AS "主销品+赠品(实销)-数量",
    ROUND(NVL(m.实销_吊牌额, 0) + NVL(g.赠品_吊牌额, 0), 2) AS "主销品+赠品(实销)-吊牌价",
    ROUND(NVL(m.实销_让利后金额, 0), 2) AS "主销品+赠品(实销)-让利后金额",
    
    -- 综合毛利率
    CASE 
        WHEN NVL(m.实销_让利后金额, 0) = 0 THEN NULL
        ELSE ROUND(
            (
                (NVL(m.实销_让利后金额, 0) - NVL(m.实销_吊牌额, 0) * 0.35)
                - NVL(g.赠品_吊牌额, 0) * 0.25
            ) / NVL(m.实销_让利后金额, 0)
        , 6)
    END AS "主销品+赠品(实销)-毛利率",
    
    -- 【绝版款-实销】
    NVL(v.绝版款_数量, 0) AS "绝版款(实销)-数量",
    ROUND(NVL(v.绝版款_吊牌额, 0), 2) AS "绝版款(实销)-吊牌价",
    ROUND(NVL(v.绝版款_让利后金额, 0), 2) AS "绝版款(实销)-让利后金额",
    
    -- 绝版款毛利率
    CASE 
        WHEN NVL(v.绝版款_让利后金额, 0) = 0 THEN NULL
        ELSE ROUND(
            (NVL(v.绝版款_让利后金额, 0) - NVL(v.绝版款_吊牌额, 0) * 0.35) 
            / NVL(v.绝版款_让利后金额, 0)
        , 6)
    END AS "绝版款(实销)-毛利率"

FROM main_product_sales m
LEFT JOIN gift_sales g 
    ON m.渠道编码 = g.渠道编码
LEFT JOIN vintage_sales v 
    ON m.渠道编码 = v.渠道编码

ORDER BY 
    CASE 
        WHEN m.渠道编码 = 'DS001' THEN 1
        WHEN m.渠道编码 = 'DS019' THEN 2
        WHEN m.渠道编码 = 'DS031' THEN 3
        WHEN m.渠道编码 = 'DS002' THEN 4
        WHEN m.渠道编码 = 'DS030' THEN 5
				WHEN m.渠道编码 = 'DS011' THEN 6
				WHEN m.渠道编码 = 'DS009' THEN 7
        WHEN m.渠道编码 = 'DS006' THEN 8
        WHEN m.渠道编码 = 'DS024' THEN 9
        WHEN m.渠道编码 = '得物' THEN 10
        WHEN m.渠道编码 = 'DS008' THEN 11
        ELSE 99
    END;
