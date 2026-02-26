-- ============================================
-- 何方珠宝 - 日报SQL（生意额/实收金额）
-- 版本：v5（修正版）
-- 更新时间：2026-01-08
-- 
-- 修改内容：
--   1. 移除GMV相关字段
--   2. 修正同期累计结束日期（原来多算了1天）
--   3. 固定渠道排序
--
-- 口径说明：
--   生意额 = TOT_AMT_ACTUAL = 实收金额（含退款）
--   今日 = 系统日期前一天（SYSDATE - 1）
--   月累计 = 本月1日 ~ 昨天
--   同期累计 = 去年同月1日 ~ 去年的昨天
--
-- 日期示例（假设今天2026/1/8）：
--   今日 = 2026/1/7
--   月累计 = 2026/1/1 ~ 2026/1/7
--   同期当日 = 2025/1/7
--   同期累计 = 2025/1/1 ~ 2025/1/7
-- ============================================

SELECT 
    -- 渠道映射
    CASE 
        WHEN s.CODE = 'DS001' THEN '天猫旗舰店'
        WHEN s.CODE = 'DS019' THEN '天猫奥莱店'
        WHEN s.CODE = 'DS002' THEN '京东POP店'
        WHEN s.CODE = 'DS009' THEN '抖音'
        WHEN s.CODE = 'DS006' THEN '小红书'
        WHEN s.CODE = 'DS024' THEN '视频号'
        WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
        WHEN s.CODE = 'DS008' THEN '散客'
        ELSE '其他'
    END AS 渠道,

    -- ═══════════════════════════════════════════════════════════════
    -- 【今日生意额】= 昨天的数据
    -- ═══════════════════════════════════════════════════════════════
    SUM(CASE 
        WHEN mr.BILLDATE = TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))
        THEN mr.TOT_AMT_ACTUAL 
        ELSE 0 
    END) AS 今日生意额,

    -- ═══════════════════════════════════════════════════════════════
    -- 【同期当日生意额】= 去年的昨天
    -- ═══════════════════════════════════════════════════════════════
    SUM(CASE 
        WHEN mr.BILLDATE = TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD'))
        THEN mr.TOT_AMT_ACTUAL 
        ELSE 0 
    END) AS 同期当日生意额,

    -- ═══════════════════════════════════════════════════════════════
    -- 【当日同比%】= (今日 - 同期当日) / |同期当日| * 100
    -- ═══════════════════════════════════════════════════════════════
    ROUND(
        CASE 
            WHEN SUM(CASE WHEN mr.BILLDATE = TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD'))
                         THEN mr.TOT_AMT_ACTUAL ELSE 0 END) = 0 
            THEN NULL
            ELSE (
                SUM(CASE WHEN mr.BILLDATE = TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD')) 
                         THEN mr.TOT_AMT_ACTUAL ELSE 0 END)
                - SUM(CASE WHEN mr.BILLDATE = TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD')) 
                           THEN mr.TOT_AMT_ACTUAL ELSE 0 END)
            ) / ABS(SUM(CASE WHEN mr.BILLDATE = TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD')) 
                              THEN mr.TOT_AMT_ACTUAL ELSE 0 END)) * 100
        END
    , 2) AS "当日同比(%)",

    -- ═══════════════════════════════════════════════════════════════
    -- 【月累计生意额】= 本月1日 ~ 昨天
    -- ═══════════════════════════════════════════════════════════════
    SUM(CASE 
        WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
         AND mr.BILLDATE <= TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))
        THEN mr.TOT_AMT_ACTUAL 
        ELSE 0 
    END) AS 月累计生意额,

    -- ═══════════════════════════════════════════════════════════════
    -- 【同期累计生意额】= 去年同月1日 ~ 去年的昨天
    -- ⭐ 修正：结束日期用 ADD_MONTHS(SYSDATE - 1, -12)
    -- ═══════════════════════════════════════════════════════════════
    SUM(CASE 
        WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -12), 'YYYYMMDD'))
         AND mr.BILLDATE <= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD'))
        THEN mr.TOT_AMT_ACTUAL 
        ELSE 0 
    END) AS 同期累计生意额,

    -- ═══════════════════════════════════════════════════════════════
    -- 【累计同比%】= (月累计 - 同期累计) / |同期累计| * 100
    -- ═══════════════════════════════════════════════════════════════
    ROUND(
        CASE 
            WHEN SUM(CASE WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -12), 'YYYYMMDD'))
                          AND mr.BILLDATE <= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD'))
                         THEN mr.TOT_AMT_ACTUAL ELSE 0 END) = 0 
            THEN NULL
            ELSE (
                SUM(CASE WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
                          AND mr.BILLDATE <= TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD'))
                         THEN mr.TOT_AMT_ACTUAL ELSE 0 END)
                - SUM(CASE WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -12), 'YYYYMMDD'))
                            AND mr.BILLDATE <= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD'))
                           THEN mr.TOT_AMT_ACTUAL ELSE 0 END)
            ) / ABS(SUM(CASE WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -12), 'YYYYMMDD'))
                              AND mr.BILLDATE <= TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD'))
                             THEN mr.TOT_AMT_ACTUAL ELSE 0 END)) * 100
        END
    , 2) AS "累计同比(%)"

FROM BOSNDS3.M_RETAIL mr
LEFT JOIN BOSNDS3.C_STORE s ON mr.C_STORE_ID = s.ID

WHERE 
    mr.ISACTIVE = 'Y'
    AND mr.STATUS = 2
    AND s.CODE IN (
        'DS001', 'DS019', 'DS002', 'DS009',
        'DS006', 'DS024', 'DS015', 'DS032', 'DS008'
    )

GROUP BY 
    CASE 
        WHEN s.CODE = 'DS001' THEN '天猫旗舰店'
        WHEN s.CODE = 'DS019' THEN '天猫奥莱店'
        WHEN s.CODE = 'DS002' THEN '京东POP店'
        WHEN s.CODE = 'DS009' THEN '抖音'
        WHEN s.CODE = 'DS006' THEN '小红书'
        WHEN s.CODE = 'DS024' THEN '视频号'
        WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
        WHEN s.CODE = 'DS008' THEN '散客'
        ELSE '其他'
    END

-- ⭐ 固定渠道排序（使用MIN获取组内最小CODE用于排序）
ORDER BY 
    CASE 
        WHEN MIN(s.CODE) = 'DS001' THEN 1                    -- 天猫旗舰店
        WHEN MIN(s.CODE) = 'DS019' THEN 2                    -- 天猫奥莱店
        WHEN MIN(s.CODE) = 'DS002' THEN 3                    -- 京东POP店
        WHEN MIN(s.CODE) = 'DS009' THEN 4                    -- 抖音
        WHEN MIN(s.CODE) = 'DS006' THEN 5                    -- 小红书
        WHEN MIN(s.CODE) = 'DS024' THEN 6                    -- 视频号
        WHEN MIN(s.CODE) IN ('DS015', 'DS032') THEN 7        -- 得物
        WHEN MIN(s.CODE) = 'DS008' THEN 8                    -- 散客
        ELSE 99
    END;


-- ============================================
-- 日期逻辑验证SQL（可单独运行检查）
-- ============================================
/*
SELECT 
    SYSDATE AS 系统时间,
    TO_CHAR(SYSDATE - 1, 'YYYY-MM-DD') AS 今日统计日期,
    TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYY-MM-DD') AS 月累计起始,
    TO_CHAR(SYSDATE - 1, 'YYYY-MM-DD') AS 月累计结束,
    TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYY-MM-DD') AS 同期当日,
    TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -12), 'YYYY-MM-DD') AS 同期累计起始,
    TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYY-MM-DD') AS 同期累计结束
FROM DUAL;

-- 预期结果（假设今天2026/1/8）：
-- 今日统计日期：2026-01-07
-- 月累计：2026-01-01 ~ 2026-01-07
-- 同期当日：2025-01-07
-- 同期累计：2025-01-01 ~ 2025-01-07
*/


-- ============================================
-- 修正说明
-- ============================================
/*
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           同期累计日期 修正对比                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  假设今天是 2026/1/8：                                                          │
│                                                                                 │
│  【修正前】                                                                      │
│  结束日期 = ADD_MONTHS(TRUNC(SYSDATE,'MM'), -12) + (SYSDATE - TRUNC(SYSDATE,'MM'))│
│          = 2025/1/1 + 7天 = 2025/1/8  ❌ 多了1天！                              │
│                                                                                 │
│  【修正后】                                                                      │
│  结束日期 = ADD_MONTHS(SYSDATE - 1, -12)                                        │
│          = ADD_MONTHS(2026/1/7, -12) = 2025/1/7  ✅ 正确！                      │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
*/