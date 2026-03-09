-- 上个月生意额月累计（按渠道）
SELECT 
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

    SUM(CASE
        WHEN mr.BILLDATE >= TO_NUMBER(TO_CHAR(ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1), 'YYYYMMDD'))
         AND mr.BILLDATE <  TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
        THEN mr.TOT_AMT_ACTUAL
        ELSE 0
    END) AS 上个月累计生意额

FROM BOSNDS3.M_RETAIL mr
LEFT JOIN BOSNDS3.C_STORE s ON mr.C_STORE_ID = s.ID
WHERE mr.ISACTIVE = 'Y'
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
ORDER BY 
    CASE 
        WHEN MIN(s.CODE) = 'DS001' THEN 1
        WHEN MIN(s.CODE) = 'DS019' THEN 2
        WHEN MIN(s.CODE) = 'DS002' THEN 3
        WHEN MIN(s.CODE) = 'DS009' THEN 4
        WHEN MIN(s.CODE) = 'DS006' THEN 5
        WHEN MIN(s.CODE) = 'DS024' THEN 6
        WHEN MIN(s.CODE) IN ('DS015', 'DS032') THEN 7
        WHEN MIN(s.CODE) = 'DS008' THEN 8
        ELSE 99
    END;