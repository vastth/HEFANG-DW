/* ================================================================================
   文件名     : 线上销售月报SQL_3_0.sql
   用途       : 统计上月各线上渠道的销售数据，输出月报核心指标
   数据库     : Oracle（伯俊ERP，Schema: BOSNDS3）
   维护人     : David
   版本历史   :
     v1.0  初版，基础销售汇总
     v2.0  拆分主销品 / 赠品 / 绝版款三类口径
     v3.0  修复单据头金额=0（全额优惠券核销单）导致总销+退货≠实销的问题
================================================================================

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【一、核心表结构说明】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  BOSNDS3.M_RETAIL         零售单据头表
    ID                     单据唯一ID
    BILLDATE               单据日期，格式 YYYYMMDD，存储为 NUMBER 类型
    TOT_AMT_ACTUAL         单据头实收金额合计（正=销售单，负=退货单，0=全额优惠核销单）
    C_STORE_ID             关联门店/渠道 ID
    ISACTIVE               是否有效（过滤条件：= 'Y'）
    STATUS                 单据状态（过滤条件：= 2，即已审核）

  BOSNDS3.M_RETAILITEM     零售单据明细行表（与 M_RETAIL 一对多）
    ID                     明细行唯一ID
    M_RETAIL_ID            关联单据头 ID
    M_PRODUCT_ID           关联商品 ID
    QTY                    数量（正=出库/销售，负=入库/退货）
    TOT_AMT_LIST           行级吊牌额（含税标价 × 数量）
    TOT_AMT_ACTUAL         行级实收金额（让利后，即顾客实际支付金额）

  BOSNDS3.M_PRODUCT        商品主档表
    ID                     商品唯一ID
    M_DIM4_ID              商品分类维度4（关联 M_DIM，区分主销品/辅销品/辅料等）
    M_DIM5_ID              商品分类维度5（关联 M_DIM，标记绝版款类型）

  BOSNDS3.M_DIM            维度字典表（通用，多维度共用此表）
    ID                     维度ID
    ATTRIBNAME             维度值名称
      DIM4 常用值：'辅销品'（赠品）/ '辅料' / '办公用品' / NULL（主销品）
      DIM5 常用值：'绝版款-线上' / '绝版款-线下' / '绝版款-同步'

  BOSNDS3.C_STORE          门店/渠道主档表
    ID                     渠道唯一ID
    CODE                   渠道编码（见下方渠道映射）
    NAME                   渠道名称

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【二、渠道编码映射】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  渠道编码    渠道名称            所属组别
  --------    ----------------    --------
  DS001       天猫旗舰店          一组
  DS019       天猫奥莱店          一组
  DS031       天猫国际直营店      一组
  DS002       京东POP店           一组
  DS030       京东自营店          一组
  DS011       唯品会              一组
  DS009       抖音                二组
  DS006       小红书              二组
  DS024       视频号              二组
  DS015       得物（旧码）        二组  ← DS015 和 DS032 合并为同一渠道"得物"输出
  DS032       得物（新码）        二组
  DS008       散客                散客

  注意：得物有两个渠道码（DS015/DS032），GROUP BY 和输出时统一合并为"得物"。

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【三、商品品类口径定义】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. 主销品
     条件：d4.ATTRIBNAME NOT IN ('辅料','辅销品','办公用品') OR d4.ATTRIBNAME IS NULL
     说明：珠宝正式销售商品，是月报核心统计对象

  2. 赠品（辅销品）
     条件：d4.ATTRIBNAME = '辅销品'
     说明：买赠活动附赠配件。ri.TOT_AMT_ACTUAL=0 但有实物出入库和吊牌额。
           成本按吊牌额 × 25% 估算（主销品成本率为 35%）

  3. 绝版款
     条件：主销品基础上，d5.ATTRIBNAME IN ('绝版款-线上','绝版款-线下','绝版款-同步')
     说明：主销品的子集，单独拆分用于追踪绝版商品销售表现

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【四、关键业务指标口径】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  总销       = 当月所有正向出库销售记录（含最终被退回的部分）
  退货       = 当月发生的退货记录，输出为负数
  实销       = 总销 + 退货（净销售量），恒等式：SUM(ri.QTY) = 总销_数量 + 退货_数量
  让利后金额 = ri.TOT_AMT_ACTUAL，顾客实际支付（已扣折扣/优惠券）
  吊牌额     = ri.TOT_AMT_LIST，商品标价金额（未折扣）
  成本       = 吊牌额 × 35%（主销品估算成本率，赠品为 25%）
  毛利额     = 让利后金额 - 成本
  毛利率     = 毛利额 / 让利后金额
  退货率     = ABS(退货_数量) / 总销_数量

  综合毛利率（主销品+赠品）：
    = (主销品毛利额 - 赠品成本) / 主销品让利后金额
    说明：赠品无收入，其成本直接冲减整体毛利

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【五、总销/退货分类判断逻辑（含历史 BUG 说明）】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  判断依据：优先用单据头金额 r.TOT_AMT_ACTUAL，=0 时按行级 ri.QTY 正负兜底

  r.TOT_AMT_ACTUAL > 0  →  销售单，明细行归入【总销】
  r.TOT_AMT_ACTUAL < 0  →  退货单，明细行归入【退货】
  r.TOT_AMT_ACTUAL = 0  →  全额优惠券/平台补贴核销单（顾客实付为零但有实物出库）
                             按行级 ri.QTY 再判断：
                               ri.QTY > 0 → 归入【总销】
                               ri.QTY < 0 → 归入【退货】

  历史 BUG（v2.0 及以前）：
    TOT_AMT_ACTUAL = 0 的记录被总销和退货双双漏计，
    导致「总销_数量 + 退货_数量 ≠ 实销_数量」。
    天猫旗舰店实测：17 张核销单，差值 21 件（v3.0 已修复）。

  校验方法：总销_数量 + 退货_数量 应严格等于 实销_数量

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【六、时间范围】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  自动取上月完整自然月，无需手动修改日期：
    起始：ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)  → 上月1日
    截止：TRUNC(SYSDATE, 'MM')                  → 本月1日（不含）
  注意：BILLDATE 存储为 NUMBER 类型，需用 TO_NUMBER(TO_CHAR(...,'YYYYMMDD')) 转换比较

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
【七、CTE 结构概览】
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  CTE 1: channel_mapping      渠道编码→名称→组别静态字典（供参考，主查询未 JOIN）
  CTE 2: main_product_sales   主销品总销/退货/实销汇总，按渠道编码 GROUP BY
  CTE 3: gift_sales           赠品数量和吊牌额汇总
  CTE 4: vintage_sales        绝版款数量/吊牌额/让利后金额汇总（主销品子集）
  主查询: 四表 LEFT JOIN，计算毛利/毛利率/退货率等派生指标，按渠道固定顺序排序

================================================================================ */

WITH 
-- ============================================================================
-- 【CTE 1】渠道映射表（静态字典，供参考）
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
-- 品类过滤：排除辅料、辅销品、办公用品（d4），保留主销品
-- 总销/退货判断：单据头 TOT_AMT_ACTUAL 正负为主，=0 时按行级 QTY 正负兜底
-- ============================================================================
main_product_sales AS (
    SELECT 
        CASE 
            WHEN s.CODE IN ('DS015', 'DS032') THEN '得物'
            ELSE s.CODE
        END AS 渠道编码,
        
        -- 【总销】单据头>0，或单据头=0且行级数量为正（全额优惠核销单）
        SUM(CASE 
            WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY
            WHEN r.TOT_AMT_ACTUAL = 0 AND ri.QTY > 0 THEN ri.QTY
            ELSE 0 
        END) AS 总销_数量,
        SUM(CASE 
            WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_LIST
            WHEN r.TOT_AMT_ACTUAL = 0 AND ri.QTY > 0 THEN ri.TOT_AMT_LIST
            ELSE 0 
        END) AS 总销_吊牌额,
        SUM(CASE 
            WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL
            WHEN r.TOT_AMT_ACTUAL = 0 AND ri.QTY > 0 THEN ri.TOT_AMT_ACTUAL
            ELSE 0 
        END) AS 总销_让利后金额,
        
        -- 【退货】单据头<0，或单据头=0且行级数量为负（全额优惠核销退货）
        -SUM(CASE 
            WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY)
            WHEN r.TOT_AMT_ACTUAL = 0 AND ri.QTY < 0 THEN ABS(ri.QTY)
            ELSE 0 
        END) AS 退货_数量,
        -SUM(CASE 
            WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_LIST)
            WHEN r.TOT_AMT_ACTUAL = 0 AND ri.QTY < 0 THEN ABS(ri.TOT_AMT_LIST)
            ELSE 0 
        END) AS 退货_吊牌额,
        -SUM(CASE 
            WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL)
            WHEN r.TOT_AMT_ACTUAL = 0 AND ri.QTY < 0 THEN ABS(ri.TOT_AMT_ACTUAL)
            ELSE 0 
        END) AS 退货_让利后金额,
        
        -- 【实销】全部明细行直接合计，恒等式：实销 = 总销 + 退货
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
-- 品类过滤：d4.ATTRIBNAME = '辅销品'
-- 说明：赠品行级 TOT_AMT_ACTUAL=0，直接 SUM 实销口径，不拆总销/退货
--       成本在主查询中按吊牌额 × 25% 计算
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
-- 品类过滤：主销品基础上，d5.ATTRIBNAME IN ('绝版款-线上','绝版款-线下','绝版款-同步')
-- 说明：绝版款是主销品子集，直接 SUM 实销口径
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
-- 【主查询】四表 LEFT JOIN，计算派生指标，按渠道固定顺序排序输出
-- ============================================================================
SELECT 
    CASE 
        WHEN m.渠道编码 = 'DS001' THEN '一组'
        WHEN m.渠道编码 = 'DS002' THEN '一组'
        WHEN m.渠道编码 = 'DS011' THEN '一组'
        WHEN m.渠道编码 = 'DS019' THEN '一组'
        WHEN m.渠道编码 = 'DS030' THEN '一组'
        WHEN m.渠道编码 = 'DS031' THEN '一组'
        WHEN m.渠道编码 IN ('DS009', 'DS006', 'DS024', '得物') THEN '二组'
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
    
    -- 【主销品-总销】含全额优惠核销单的正向出库
    NVL(m.总销_数量, 0) AS "主销品(总销)-数量",
    ROUND(NVL(m.总销_吊牌额, 0), 2) AS "主销品(总销)-吊牌额",
    ROUND(NVL(m.总销_让利后金额, 0), 2) AS "主销品(总销)-让利后金额",
    
    -- 【主销品-退货】输出为负数
    NVL(m.退货_数量, 0) AS "主销品(退货)-数量",
    ROUND(NVL(m.退货_吊牌额, 0), 2) AS "主销品(退货)-吊牌额",
    ROUND(NVL(m.退货_让利后金额, 0), 2) AS "主销品(退货)-让利后金额",
    
    -- 退货率 = ABS(退货_数量) / 总销_数量
    CASE 
        WHEN NVL(m.总销_数量, 0) = 0 THEN 0
        ELSE ROUND(m.退货_数量 / m.总销_数量, 6)
    END AS "主销品(退货)-退货率",
    
    -- 【主销品-实销】= 总销 + 退货（净销售）
    NVL(m.实销_数量, 0) AS "主销品(实销)-数量",
    ROUND(NVL(m.实销_吊牌额, 0), 2) AS "主销品(实销)-吊牌额",
    ROUND(NVL(m.实销_让利后金额, 0), 2) AS "主销品(实销)-让利后金额",
    
    -- 成本 = 实销吊牌额 × 35%
    ROUND(NVL(m.实销_吊牌额, 0) * 0.35, 2) AS "主销品(实销)-成本",
    
    -- 毛利额 = 让利后金额 - 成本
    ROUND(NVL(m.实销_让利后金额, 0) - NVL(m.实销_吊牌额, 0) * 0.35, 2) AS "主销品(实销)-毛利额",
    
    -- 毛利率 = 毛利额 / 让利后金额
    CASE 
        WHEN NVL(m.实销_让利后金额, 0) = 0 THEN NULL
        ELSE ROUND((m.实销_让利后金额 - m.实销_吊牌额 * 0.35) / m.实销_让利后金额, 6)
    END AS "主销品(实销)-毛利率",
    
    -- 【赠品-实销】数量和吊牌额（让利后金额=0，不参与收入）
    NVL(g.赠品_数量, 0) AS "赠品(实销)-数量",
    ROUND(NVL(g.赠品_吊牌额, 0), 2) AS "赠品(实销)-吊牌额",
    -- 赠品成本 = 赠品吊牌额 × 25%
    ROUND(NVL(g.赠品_吊牌额, 0) * 0.25, 2) AS "赠品(实销)-成本",
    
    -- 【主销品+赠品-实销】合并口径
    NVL(m.实销_数量, 0) + NVL(g.赠品_数量, 0) AS "主销品+赠品(实销)-数量",
    ROUND(NVL(m.实销_吊牌额, 0) + NVL(g.赠品_吊牌额, 0), 2) AS "主销品+赠品(实销)-吊牌价",
    ROUND(NVL(m.实销_让利后金额, 0), 2) AS "主销品+赠品(实销)-让利后金额",
    
    -- 综合毛利率 = (主销品毛利额 - 赠品成本) / 主销品让利后金额
    CASE 
        WHEN NVL(m.实销_让利后金额, 0) = 0 THEN NULL
        ELSE ROUND(
            ((NVL(m.实销_让利后金额, 0) - NVL(m.实销_吊牌额, 0) * 0.35) - NVL(g.赠品_吊牌额, 0) * 0.25)
            / NVL(m.实销_让利后金额, 0)
        , 6)
    END AS "主销品+赠品(实销)-毛利率",
    
    -- 【绝版款-实销】主销品子集
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
LEFT JOIN gift_sales g ON m.渠道编码 = g.渠道编码
LEFT JOIN vintage_sales v ON m.渠道编码 = v.渠道编码

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