# 何方珠宝 - SQL开发手册

> SQL模板 | 开发规范 | 常用场景 | 快速参考

---

## 📋 目录

1. [必须遵守的规则](#一必须遵守的规则)
2. [标准SQL模板](#二标准sql模板)
3. [常用分析场景](#三常用分析场景)
4. [快速参考卡片](#四快速参考卡片)

---

## 一、必须遵守的规则

### 1.1 每个查询必加的条件

```sql
-- 零售单必加
WHERE r.ISACTIVE = 'Y' AND r.STATUS = 2

-- 库存表必加
WHERE fs.ISACTIVE = 'Y'

-- 商品表必加
WHERE p.ISACTIVE = 'Y'
```

---

### 1.2 主销品类别筛选（口径类SQL）

```sql
-- 这个ID列表要背下来（用于销售/ADS等口径统计）
AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
```

> 说明：库存明细快照可保留全量SKU，主销品过滤可在ADS层统一控制。

---

### 1.3 电商可售库存口径

```sql
-- 总仓+云仓，不要只写总仓
AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
```

---

### 1.4 SKU过滤（必加）

```sql
-- 只保留SKU级别明细
AND ri.M_PRODUCTALIAS_ID IS NOT NULL
```

---

### 1.5 日期格式

```sql
-- Oracle日期是NUMBER(8)，格式YYYYMMDD
WHERE BILLDATE = 20260113
WHERE BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))

-- MySQL日期
WHERE date_id = DATE_FORMAT(CURDATE(), '%Y%m%d')
WHERE date_id >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY), '%Y%m%d')
```

---

### 1.6 日报月累计窗口边界

```sql
-- 日报模板建议先在 date_params CTE 中统一定义窗口参数
-- 非每月1日：本月1日 ~ 昨天
-- 每月1日：上一个完整自然月
TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD')) AS report_day,
TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD')) AS report_day_last_year,
TO_NUMBER(TO_CHAR(
    CASE
        WHEN TRUNC(SYSDATE) = TRUNC(SYSDATE, 'MM')
        THEN ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)
        ELSE TRUNC(SYSDATE, 'MM')
    END,
    'YYYYMMDD'
)) AS month_start_day,
TO_NUMBER(TO_CHAR(SYSDATE - 1, 'YYYYMMDD')) AS month_end_day,
TO_NUMBER(TO_CHAR(ADD_MONTHS(
    CASE
        WHEN TRUNC(SYSDATE) = TRUNC(SYSDATE, 'MM')
        THEN ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -1)
        ELSE TRUNC(SYSDATE, 'MM')
    END,
    -12
), 'YYYYMMDD')) AS month_start_day_last_year,
TO_NUMBER(TO_CHAR(ADD_MONTHS(SYSDATE - 1, -12), 'YYYYMMDD')) AS month_end_day_last_year
```

> 说明：这样可以避免 4/1 这类月初场景出现“统计起始日大于结束日”，导致月累计与同期累计被汇总为 0。

---

### 1.7 门店经营日报宽表注意事项

```sql
-- 1. 当前冻结：绝对金额小于 1 的明细整体排除
AND ABS(ri.TOT_AMT_ACTUAL) >= 1

-- 2. 日订单数 / 月累计订单数按过滤后商品范围单号净额记号求和
-- 先基于日报有效交易集按 retail_id 去重，再按 >0 / =0 / <0 记 1 / 0 / -1
SUM(
    CASE
        WHEN ABS(filtered_retail_amt) < 0.0001 THEN 0
        WHEN filtered_retail_amt > 0 THEN 1
        WHEN filtered_retail_amt < 0 THEN -1
        ELSE 0
    END
) AS order_cnt

-- 3. 目标版本必须精确匹配，不自动兜底“最新版本”
WHERE t.TARGET_DATE = p.report_date
    AND t.TARGET_VERSION = p.data_version

-- 4. LEFT JOIN 汇总后再算比率时，分子必须先 COALESCE
CASE
        WHEN COALESCE(td.month_target, 0) = 0 THEN NULL
        ELSE ROUND(COALESCE(mf.mtd_sales_amt, 0.00) / td.month_target, 4)
END AS month_ach_rate

-- 5. 目标配置少于有效门店数时当前只告警，不在 SQL 层强行拦截
-- 原因：未来门店数量可能收缩，允许部分门店暂时无目标但保留日报行

-- 6. month_target 与 day_target 独立存储，不要求月内日目标合计等于月目标
-- 原因：月目标是当月固定控制值，日目标会按业务节奏动态调整并冻结

-- 7. 门店日报商品范围当前固定排除 147=辅料、149=办公用品、150=道具，其余 category_id 默认纳入
-- 其中 146=配件、148=辅销品、394=配饰 当前继续纳入门店日报，不改变库存健康等链路的 12 类模板

-- 8. 共同考核主体层只认显式归属表，不能按商场/城市自行猜测合并关系
LEFT JOIN cfg_store_assessment_assignment a
    ON a.store_id = base.store_id
 AND p.report_date BETWEEN a.effective_start_date AND a.effective_end_date

-- 9. 统计主体目标优先取主体目标表；缺失时再回退主体内门店目标求和
COALESCE(subject_target.day_target, SUM(store_target.day_target)) AS subject_day_target
```

> 说明：门店日报宽表当前采用净额 / 净量 / 净单口径；其中订单数必须先在日报有效交易集内按零售单去重，再按过滤后商品范围的单号净额 `>0 / =0 / <0` 分别记 `1 / 0 / -1`，并对 `ABS(金额) < 0.0001` 的净零单按 0 处理。当前正式 ADS 已对齐业务对账侧，绝对金额小于 1 的小额非零明细不再纳入有效交易集。如果目标大于 0 但销售汇总来自 LEFT JOIN，分子不先 `COALESCE`，就会把“应为 0 的达成率”误算成 `NULL`。目标配置行数少于有效门店数时，当前按业务确认只告警，不在 SQL 层强行失败。`month_target` 与 `day_target` 当前按业务含义独立维护，SQL 不应再额外校验“月内日目标合计 = 月目标”。商品范围当前固定排除 `147=辅料`、`149=办公用品`、`150=道具`，其余 category_id 默认纳入，不再依赖 `dim_report_product_rule` 圈定 active 集合。若扩展到统计主体层，合并关系与主体目标也必须来自显式配置表，不能在 SQL 中临时猜测。

---

### 1.9 销售看板月度战役宽表注意事项

```sql
-- 1. battle_month 固定为 report_date 所在自然月月初，sales_date 只覆盖月初到 report_date
WITH RECURSIVE date_scope AS (
    SELECT DATE_FORMAT(@report_date, '%Y-%m-01') AS sales_date
    UNION ALL
    SELECT DATE_ADD(sales_date, INTERVAL 1 DAY)
    FROM date_scope
    WHERE sales_date < @report_date
)

-- 2. 组织范围先取当前月目标门店，再按 report_date 当天优先、否则月内最近有效的 dim_store_report_attr 取切片
WHERE EXISTS (
                SELECT 1
                FROM cfg_store_target_daily t
                WHERE t.store_code = ds.store_code
                    AND t.target_date BETWEEN DATE_FORMAT(@report_date, '%Y-%m-01') AND @report_date
        )
    AND sra.is_include_in_daily_report = 'Y'

-- 3. 日目标必须先看共同考核主体日目标，缺失时再回退经营实体内门店 day_target 求和
COALESCE(MAX(std.day_target_amt), SUM(tdsd.day_target_amt), 0) AS day_target_amt

-- 4. 日实际与去年同期累计都必须回到 ODS 净额明细，并套用门店日报商品范围
LEFT JOIN excluded_category_scope ecs
        ON dp.category_id = ecs.category_id
WHERE r.isactive = 'Y'
  AND r.status = 2
  AND ABS(ri.tot_amt_actual) >= 1
    AND dp.category_id IS NOT NULL
    AND ecs.category_id IS NULL

-- 5. 月累计目标 / 月累计实际 / 去年同期累计实际都应按 area_name + report_channel_type 的 sales_date 序列累加
SUM(day_target_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS cum_target_amt
SUM(day_actual_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS cum_actual_amt
SUM(last_year_day_actual_amt) OVER (PARTITION BY area_name, report_channel_type ORDER BY sales_date) AS last_year_cum_actual_amt

-- 6. 去年同期累计实际要把去年同月同日净额先对齐到当年 sales_date，再做窗口累加
DATE_ADD(STR_TO_DATE(CAST(ds.date_id AS CHAR(8)), '%Y%m%d'), INTERVAL 1 YEAR) AS sales_date
```

> 说明：`ads_daily_sales` 当前最小对账 SQL 必须同步 `etl_ads_daily_sales.py` 的经营实体与商品范围规则：组织集合先来自 `cfg_store_target_daily` 在 `battle_month ~ report_date` 内出现过目标记录的门店，再按 `dim_store_report_attr` 当前日优先、否则月内最近有效切片冻结组织属性；经共同考核主店归并后再按 `area_name + report_channel_type` 产出。`day_target_amt` 先取主体日目标，再回退门店日目标；`day_actual_amt` 与去年同期累计都回到 `ods_m_retail + ods_m_retailitem` 净额明细，并固定排除 `147=辅料`、`149=办公用品`、`150=道具`。月累计目标仍只能直接累计 `day_target`，不能偷换成 `month_target / 当月天数` 的平均拆分，也不能依赖旧版 `全国 / 全部` 总盘过滤。

---

## 二、标准SQL模板

### 2.1 标准JOIN模板（Oracle）

```sql
-- 零售数据标准写法
FROM M_RETAILITEM ri
LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
LEFT JOIN M_DIM d4 ON p.M_DIM4_ID = d4.ID        -- 类别
LEFT JOIN M_DIM d5 ON p.M_DIM5_ID = d5.ID        -- 性质
LEFT JOIN M_DIM d6 ON p.M_DIM6_ID = d6.ID        -- 系列
LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
WHERE r.ISACTIVE = 'Y' AND r.STATUS = 2
    AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
    AND ri.M_PRODUCTALIAS_ID IS NOT NULL
    AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')

-- 库存数据标准写法
FROM FA_STORAGE fs
LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
LEFT JOIN M_DIM d4 ON p.M_DIM4_ID = d4.ID
LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
WHERE fs.ISACTIVE = 'Y'
    AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
    AND fs.M_PRODUCTALIAS_ID IS NOT NULL
    AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
```

---

### 2.2 销售指标计算模板

```sql
-- 销售数量（出库）
SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS 销售数量,

-- 退货数量（入库）
SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS 退货数量,

-- 销售金额
SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售额,

-- 退货金额
SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) AS 退货额,

-- 净销量
SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) 
  - SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.QTY) ELSE 0 END) AS 净销量,

-- 净销售额
SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END)
  - SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) AS 净销售额,

-- 客单价
SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) 
  / NULLIF(COUNT(DISTINCT CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN r.ID END), 0) AS 客单价,

-- 退货率
SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) 
  / NULLIF(SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END), 0) AS 退货率
```

---

### 2.3 库存指标计算模板

```sql
-- 周转天数（Oracle）
CASE 
    WHEN NVL(销售数量, 0) = 0 THEN 9999 
    ELSE ROUND(库存数量 / (销售数量 / 30), 1)
END AS 库存周转天数,

-- 周转天数（MySQL）
CASE 
    WHEN COALESCE(销售数量, 0) = 0 THEN 9999
    ELSE ROUND(库存数量 / (销售数量 / 30), 1)
END AS 库存周转天数,

-- 库存状态
CASE 
    WHEN 库存数量 > 0 AND 销售数量 = 0 THEN '滞销'
    WHEN 库存数量 = 0 AND 销售数量 = 0 THEN '停售'
    WHEN 销售数量 > 0 AND 周转天数 < 30 THEN '紧急缺货'
    WHEN 销售数量 > 0 AND 周转天数 < 70 THEN '需补货'
    WHEN 销售数量 > 0 AND 周转天数 <= 90 THEN '正常'
    WHEN 销售数量 > 0 AND 周转天数 > 90 THEN '库存过高'
    ELSE '正常'
END AS 库存状态,

-- 建议补货（允许负数表示库存过剩）
CASE 
    WHEN 销售数量 = 0 THEN 0
    WHEN 周转天数 >= 90 THEN 0
    ELSE ROUND(
        (90 - 周转天数) * (销售数量 / 30) 
        - 退货数量 
        - 采购欠数
    , 0)
END AS 建议补货数量
```

---

### 2.4 常用筛选条件速查

```sql
-- 电商渠道
WHERE s.CODE LIKE 'DS%'

-- 线下门店
WHERE s.CODE LIKE 'RT%'

-- 中山总仓
WHERE s.CODE = '001'

-- 云仓
WHERE s.IS_ALLO2OSTORAGE = 'Y'

-- 总仓+云仓
WHERE (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')

-- 天猫
WHERE s.CODE = 'DS001'

-- 抖音
WHERE s.CODE = 'DS009'

-- 在售款
WHERE p.M_DIM5_ID IN (224, 296, 297)

-- 新品
WHERE p.M_DIM5_ID IN (225, 298, 299)

-- 绝版款
WHERE p.M_DIM5_ID IN (127, 126, 152)

-- 近30天
WHERE BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))

-- 近7天
WHERE BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-7, 'YYYYMMDD'))

-- 昨天
WHERE BILLDATE = TO_NUMBER(TO_CHAR(SYSDATE-1, 'YYYYMMDD'))

-- 本月
WHERE BILLDATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
```

---

## 三、常用分析场景

### 3.1 昨日各渠道销售

```sql
-- 业务问题：昨天各渠道卖了多少？
SELECT
    s.NAME AS 渠道,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售额,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.QTY ELSE 0 END) AS 销量
FROM M_RETAILITEM ri
LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
WHERE r.ISACTIVE = 'Y' AND r.STATUS = 2
    AND r.BILLDATE = TO_NUMBER(TO_CHAR(SYSDATE-1, 'YYYYMMDD'))
    AND ri.M_PRODUCTALIAS_ID IS NOT NULL
    AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
    AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
GROUP BY s.NAME
ORDER BY 销售额 DESC;
```

---

### 3.2 本月TOP10商品

```sql
-- 业务问题：这个月哪些款卖得最好？
SELECT * FROM (
    SELECT
        p.NAME AS 商品编码,
        p.VALUE AS 商品名称,
        d4.ATTRIBNAME AS 类别,
        SUM(ri.QTY) AS 销量,
        SUM(ri.TOT_AMT_ACTUAL) AS 销售额,
        ROW_NUMBER() OVER (ORDER BY SUM(ri.TOT_AMT_ACTUAL) DESC) AS 排名
    FROM M_RETAILITEM ri
    LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
    LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
    LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
    LEFT JOIN M_DIM d4 ON p.M_DIM4_ID = d4.ID
    WHERE r.ISACTIVE = 'Y' AND r.STATUS = 2
        AND r.TOT_AMT_ACTUAL > 0
        AND r.BILLDATE >= TO_NUMBER(TO_CHAR(TRUNC(SYSDATE, 'MM'), 'YYYYMMDD'))
        AND ri.M_PRODUCTALIAS_ID IS NOT NULL
        AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
        AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
    GROUP BY p.NAME, p.VALUE, d4.ATTRIBNAME
)
WHERE 排名 <= 10;
```

---

### 3.3 日销售趋势

```sql
-- 业务问题：最近30天销售趋势如何？
SELECT
    r.BILLDATE AS 日期,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售额
FROM M_RETAILITEM ri
LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
WHERE r.ISACTIVE = 'Y' AND r.STATUS = 2
    AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
    AND ri.M_PRODUCTALIAS_ID IS NOT NULL
    AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
    AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
GROUP BY r.BILLDATE
ORDER BY r.BILLDATE;
```

---

### 3.4 达播日销售汇总

```sql
-- 业务问题：达播每天销量和销售额
SELECT
    sale_date AS 日期,
    SUM(dabo_sales_qty) AS 销量,
    SUM(dabo_revenue) AS 销售额,
    SUM(dabo_order_count) AS 订单数
FROM ads_dabo_daily_sales
WHERE sale_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY sale_date
ORDER BY sale_date;
```

---

### 3.4a 达播订单标签驱动的日实收汇总

```sql
-- 业务问题：基于统一 Excel 订单标签，按渠道统计 ODS 日实收/退款
WITH tagged_orders AS (
    SELECT DISTINCT
        COALESCE(NULLIF(canonical_system_order_id, ''), system_order_id) AS bridge_system_order_id,
        dabo_channel_code,
        dabo_channel_name
    FROM ads_dabo_order_label
    WHERE is_dabo_order = 1
), retail_match AS (
    SELECT
        r.billdate,
        t.dabo_channel_code,
        t.dabo_channel_name,
        r.id AS retail_id,
        r.tot_amt_actual
    FROM tagged_orders t
    INNER JOIN ods_m_retail r
        ON r.oms_sourcecode = t.bridge_system_order_id
    WHERE r.isactive = 'Y'
      AND r.status = 2
      AND r.billdate >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 30 DAY), '%Y%m%d')
)
SELECT
    billdate AS 日期,
    dabo_channel_name AS 达播渠道,
    COUNT(DISTINCT retail_id) AS 订单数,
    SUM(CASE WHEN tot_amt_actual > 0 THEN tot_amt_actual ELSE 0 END) AS 销售额,
    SUM(CASE WHEN tot_amt_actual < 0 THEN ABS(tot_amt_actual) ELSE 0 END) AS 退款额,
    SUM(tot_amt_actual) AS 净额
FROM retail_match
GROUP BY billdate, dabo_channel_code, dabo_channel_name
ORDER BY billdate, dabo_channel_code;
```

---

### 3.5 缺货商品清单（MySQL）

```sql
-- 业务问题：哪些A类商品要断货了？
SELECT
    product_code AS 商品编码,
    product_name AS 商品名称,
    category_name AS 类别,
    total_qty AS 库存,
    turnover_days AS 周转天数,
    suggest_qty AS 建议补货
FROM ads_inventory_health
WHERE snapshot_date = CURDATE()
    AND sku_grade = 'A'
    AND inventory_status IN ('紧急缺货', '需补货')
ORDER BY turnover_days;
```

---

### 3.6 滞销商品清单（MySQL）

```sql
-- 业务问题：哪些货卖不动？
SELECT
    product_code AS 商品编码,
    product_name AS 商品名称,
    category_name AS 类别,
    total_qty AS 库存,
    total_qty * price_list AS 库存金额
FROM ads_inventory_health ih
LEFT JOIN dim_product p ON ih.product_id = p.product_id
WHERE snapshot_date = CURDATE()
    AND inventory_status = '滞销'
ORDER BY 库存金额 DESC;
```

---

### 3.7 各类别库存分布（MySQL）

```sql
-- 业务问题：库存在各品类怎么分布？
SELECT
    category_name AS 类别,
    SUM(total_qty) AS 库存数量,
    COUNT(*) AS SKU数,
    SUM(CASE WHEN inventory_status = '紧急缺货' THEN 1 ELSE 0 END) AS 缺货SKU
FROM ads_inventory_health
WHERE snapshot_date = CURDATE()
GROUP BY category_name
ORDER BY 库存数量 DESC;
```

---

### 3.8 同比分析（Oracle）

```sql
-- 业务问题：和去年同期比怎么样？
WITH 
today AS (
    SELECT SUM(TOT_AMT_ACTUAL) AS 销售额
    FROM M_RETAIL
    WHERE ISACTIVE = 'Y' AND STATUS = 2 AND TOT_AMT_ACTUAL > 0
        AND BILLDATE = TO_NUMBER(TO_CHAR(SYSDATE-1, 'YYYYMMDD'))
),
lastyear AS (
    SELECT SUM(TOT_AMT_ACTUAL) AS 销售额
    FROM M_RETAIL
    WHERE ISACTIVE = 'Y' AND STATUS = 2 AND TOT_AMT_ACTUAL > 0
        AND BILLDATE = TO_NUMBER(TO_CHAR(SYSDATE-365, 'YYYYMMDD'))
)
SELECT
    t.销售额 AS 昨日销售额,
    l.销售额 AS 去年同期,
    ROUND((t.销售额 - l.销售额) / NULLIF(l.销售额, 0) * 100, 2) AS 同比增长率
FROM today t, lastyear l;
```

---

### 3.9 退货分析（Oracle）

```sql
-- 业务问题：退货率高的商品有哪些？
SELECT
    p.NAME AS 商品编码,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) AS 销售额,
    SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) AS 退货额,
    ROUND(
        SUM(CASE WHEN r.TOT_AMT_ACTUAL < 0 THEN ABS(ri.TOT_AMT_ACTUAL) ELSE 0 END) /
        NULLIF(SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END), 0) * 100
    , 2) AS 退货率
FROM M_RETAILITEM ri
LEFT JOIN M_RETAIL r ON ri.M_RETAIL_ID = r.ID
LEFT JOIN M_PRODUCT p ON ri.M_PRODUCT_ID = p.ID
LEFT JOIN C_STORE s ON r.C_STORE_ID = s.ID
WHERE r.ISACTIVE = 'Y' AND r.STATUS = 2
    AND r.BILLDATE >= TO_NUMBER(TO_CHAR(SYSDATE-30, 'YYYYMMDD'))
    AND ri.M_PRODUCTALIAS_ID IS NOT NULL
    AND (s.CODE LIKE 'DS%' OR s.IS_ALLO2OSTORAGE = 'Y')
    AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
GROUP BY p.NAME
HAVING SUM(CASE WHEN r.TOT_AMT_ACTUAL > 0 THEN ri.TOT_AMT_ACTUAL ELSE 0 END) > 10000
ORDER BY 退货率 DESC;
```

---

## 四、快速参考卡片

### 4.1 开发检查清单

写完SQL后检查：
- [ ] ISACTIVE = 'Y' 加了？
- [ ] STATUS = 2 加了（零售单）？
- [ ] 主销品类别筛选了？（口径类SQL/ADS）
- [ ] SKU过滤（M_PRODUCTALIAS_ID IS NOT NULL）加了？
- [ ] 渠道口径（DS%或云仓）加了？
- [ ] 日期范围正确？
- [ ] 仓库口径是总仓+云仓？
- [ ] 正负单分开统计了？
- [ ] 空值用NVL/COALESCE处理了？

---

### 4.2 Oracle特有语法

```sql
-- 空值处理
NVL(字段, 0)

-- 字符串拼接
字段1 || '-' || 字段2

-- 日期转数字
TO_NUMBER(TO_CHAR(SYSDATE, 'YYYYMMDD'))

-- 避免除零
NULLIF(分母, 0)

-- 四舍五入
ROUND(数值, 小数位数)

-- 窗口函数
ROW_NUMBER() OVER (ORDER BY 字段 DESC)
SUM(字段) OVER (PARTITION BY 分组字段)
```

---

### 4.3 MySQL特有语法

```sql
-- 空值处理
COALESCE(字段, 0)

-- 字符串拼接
CONCAT(字段1, '-', 字段2)

-- 日期转数字
DATE_FORMAT(CURDATE(), '%Y%m%d')

-- 日期计算
DATE_SUB(CURDATE(), INTERVAL 30 DAY)

-- 四舍五入
ROUND(数值, 小数位数)
```

---

### 4.4 核心ID速查

**主销品类别：**
```
134,142,139,138,141,143,133,136,140,137,144,145
```

**在售款性质：**
```
224,296,297
```

**新品性质：**
```
225,298,299
```

**绝版款性质：**
```
127,126,152
```

---

### 4.5 常用渠道店仓（C_STORE.CODE 口径）

以下映射仅适用于店仓维度与零售/库存相关 SQL 中的 `C_STORE.CODE`，不适用于 `dim_channel.WING_CODE`。

| 渠道 | 店仓CODE |
|------|----------|
| 天猫 | DS001 |
| 抖音 | DS009 |
| 京东 | DS002 |
| 小红书 | DS006 |
| 中山总仓 | 001 |

---

### 4.6 SPU vs SKU 对账字段说明

用于 [SQL/test_spu_vs_sku_mysql.sql](../SQL/test_spu_vs_sku_mysql.sql) 的差异字段：

| 字段 | 含义 |
|------|------|
| inv_date | 库存快照日期（取 dws_inventory_daily 最大 date_id） |
| spu_qty / sku_qty | SPU/SKU 粒度库存汇总数量 |
| diff_qty | SKU 库存合计 - SPU 库存合计 |
| spu_sales_qty / sku_sales_qty | SPU/SKU 粒度近30天销量汇总 |
| diff_sales_qty | SKU 近30天销量 - SPU 近30天销量 |
| spu_return_qty / sku_return_qty | SPU/SKU 粒度近30天退货数量汇总 |
| diff_return_qty | SKU 近30天退货量 - SPU 近30天退货量 |
| str_to_date | 日期转换函数（用于回推 sales_start） |

---

### 4.7 索引与脚本变量速记

| 名称 | 含义 | 来源脚本 |
|------|------|----------|
| idx_inv_store_code | dws_inventory_daily 店仓编码索引（可选） | SQL/alter_dws_inventory_add_store_fields.sql |
| idx_product_id | dim_sku.product_id 普通索引 | SQL/create_dim_sku.sql |
| index_name | information_schema.statistics 中的索引名字段 | SQL/alter_dws_inventory_unique_key.sql |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v2.24 | 2026-06-08 | 将门店日报与 ads_daily_sales 的 SQL 注意事项更新为固定排除 `147/149/150`，不再依赖 `dim_report_product_rule` active 集合 |
| v2.23 | 2026-06-06 | 退役 3 张销售专题 ADS，并删除对应销售专题 SQL 注意事项 |
| v2.22 | 2026-05-14 | 将 4 张销售主题 ADS 的最小对账 SQL 注意事项统一到“当前月目标门店 + 月内最近组织属性/共同考核快照”边界，并补记销售专题月级组织层逐月最近目标回放 |
| v2.21 | 2026-04-29 | 将门店日报与销售专题 SKU 层的订单数 SQL 注意事项改为按过滤后净额与近零容差判单 |
| v2.20 | 2026-04-28 | 新增销售专题月级组织层最小对账 SQL 注意事项，并明确当前月需与销售专题组织日层 MTD 对平 |
| v2.19 | 2026-04-27 | 将销售主题 ADS 最小对账 SQL 与 SQL 示例说明统一到门店日报权威口径 |
| v2.18 | 2026-04-23 | 同步销售主题 ADS 改为 report_channel_type 明细口径，并明确最小对账 SQL 改为按明细切片聚合 |
| v2.17 | 2026-04-17 | 新增销售专题 SKU 层连带贡献的 ODS 订单级 SQL 口径模板 |
| v2.16 | 2026-04-16 | 补充 ads_daily_sales 最小对账 SQL 的中文排序规则兼容注意事项 |
| v2.15 | 2026-04-15 | 新增 ads_daily_sales 的 battle_month / sales_date / 累计字段 SQL 注意事项 |
| v2.14 | 2026-04-10 | 补充门店日报统计主体层 SQL 注意事项，明确共同考核与主体目标只能走显式配置 |
| v2.13 | 2026-04-10 | 更新门店日报商品范围 active 集合为 15 类，补记 148=辅销品、394=配饰 已纳入日报口径 |
| v2.12 | 2026-04-09 | 更新达播订单标签驱动 SQL 示例为优先使用 canonical_system_order_id 做 ODS 桥接 |
| v2.11 | 2026-04-08 | 明确门店日报商品范围跟随 dim_report_product_rule，当前补纳 146=配件 |
| v2.10 | 2026-04-08 | 新增基于 ads_dabo_order_label 的订单标签驱动 SQL 示例 |
| v2.9 | 2026-04-03 | 明确门店日报月目标与日目标独立维护，SQL 不校验月内日目标合计等于月目标 |
| v2.8 | 2026-04-03 | 补充门店日报目标配置少于有效门店数时只告警的实现约束 |
| v2.7 | 2026-04-03 | 补充门店经营日报宽表的 0 金额过滤、目标版本匹配与 COALESCE 规则 |
| v2.6 | 2026-04-01 | 补充日报模板月初累计窗口规则，避免每月1日出现反向日期区间 |
| v2.5 | 2026-03-23 | 明确常用渠道店仓映射仅适用于 C_STORE.CODE，不适用于 dim_channel.WING_CODE |
| v2.2 | 2026-02-27 | 更新SQL模板与速查内容 |
| v2.3 | 2026-02-28 | 补充SPU vs SKU对账字段说明 |
| v2.4 | 2026-02-28 | 补充索引与脚本变量速记 |
