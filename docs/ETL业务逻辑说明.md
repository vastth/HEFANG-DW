# 何方珠宝 - ETL业务逻辑说明（人话版）

> 每个ETL脚本干了什么？数据从哪来到哪去？为什么这么做？
> 面向：业务同事、新入职开发、运维人员

---

## 📋 目录

1. [总览：数据怎么流的](#一总览数据怎么流的)
2. [etl_dim_product.py — 商品信息同步](#二etl_dim_productpy--商品信息同步)
3. [etl_dim_sku.py — SKU信息同步](#三etl_dim_skupy--sku信息同步)
4. [etl_dim_store.py — 店仓信息同步](#四etl_dim_storepy--店仓信息同步)
5. [etl_dws_sales.py — 销售数据同步](#五etl_dws_salespy--销售数据同步)
6. [etl_dws_inventory.py — 库存快照同步](#六etl_dws_inventorypy--库存快照同步)
7. [etl_ads_health.py — 库存健康度计算](#七etl_ads_healthpy--库存健康度计算)
8. [etl_ods_fa_storage.py — ODS库存原始层](#八etl_ods_fa_storagepy--ods库存原始层)
9. [etl_ods_m_retail.py — ODS零售单原始层](#九etl_ods_m_retailpy--ods零售单原始层)
10. [etl_ods_m_retailitem.py — ODS零售明细原始层](#十etl_ods_m_retailitempy--ods零售明细原始层)
11. [run_etl.py — 主控调度](#十一run_etlpy--主控调度)
12. [run_ods.py — ODS独立入口](#十二run_odspy--ods独立入口)
13. [scheduled_etl.py — 定时调度包装](#十三scheduled_etlpy--定时调度包装)
14. [config.py — 配置中心](#十四configpy--配置中心)
15. [alerts.py — 企业微信告警](#十五alertspy--企业微信告警)

---

## 一、总览：数据怎么流的

### 每天凌晨自动发生的事(具体实际取决于windows计划任务设置的时间)

```
Oracle（伯俊ERP）                         MySQL（数仓）
━━━━━━━━━━━━━━━                          ━━━━━━━━━━━━
                                         
M_PRODUCT ──────────────────────────────→ dim_product      (商品信息)
  + M_DIM (类别/性质/系列/品牌)            + dim_product_attr (颜色/尺寸)
                                         
M_PRODUCT_ALIAS ────────────────────────→ dim_sku          (SKU条码)
  + M_ATTRIBUTESETINSTANCE (颜色/尺寸)
                                         
C_STORE + C_AREA ───────────────────────→ dim_store        (店仓信息)

O2O_RETAIL_CHANNEL ─────────────────────→ dim_channel      (渠道信息)

M_RETAIL + M_RETAILITEM ────────────────→ dws_sales_daily  (销售汇总)
  + C_STORE

FA_STORAGE + C_STORE ───────────────────→ dws_inventory_daily (库存快照)


 MySQL内部计算（不查Oracle）
━━━━━━━━━━━━━━━━━━━━━━━━━━
dws_inventory_daily ┐
dws_sales_daily     ├──→ ads_inventory_health (库存健康度)
ads_dabo_daily_sales│    (周转天数/SABC分级/建议补货...)
dim_product         │
dim_sku             │
dim_store           ┘
```

### 执行顺序（8步）

| 步骤 | 脚本 | 做什么 | 耗时 |
|------|------|--------|------|
| 1 | etl_dim_product | 把Oracle里的商品信息复制到MySQL | ~3分钟 |
| 2 | etl_dim_sku | 把Oracle里的SKU条码信息复制到MySQL | ~1分钟 |
| 3 | etl_dim_store | 把Oracle里的店仓信息复制到MySQL | ~1分钟 |
| 4 | etl_dim_channel | 把Oracle里的电商渠道信息复制到MySQL | ~1分钟 |
| 5 | etl_dws_sales | 把昨天（或今天）的销售数据拉过来 | ~5分钟 |
| 6 | etl_dws_inventory | 拍一张当天的库存"照片" | ~10分钟 |
| 7 | (达播就绪检查) | 看看今天的达播CSV是否已导入 | ~1秒 |
| 8 | etl_ads_health | 在MySQL里算库存健康度 | ~5分钟 |

**调度任务键名（run_etl.py）**：
dim_product / dim_sku / dim_store / dim_channel / dws_sales / dws_inventory / dabo_ready / ads_health

**依赖关系**：步骤1-4可以独立跑，步骤8依赖步骤1-6的结果。

---

## 二、etl_dim_product.py — 商品信息同步

### 一句话

**把伯俊ERP里所有有效商品的基本信息（款号、品名、类别、性质、系列、吊牌价等），每天全量覆盖写到MySQL的dim_product表里。**

### 数据流

```
Oracle                              MySQL
━━━━━                              ━━━━━
M_PRODUCT (商品主表)  ─┐
M_DIM d1  (品牌)      │
M_DIM d4  (类别)      ├──→  dim_product (商品维度表)
M_DIM d5  (性质)      │     dim_product_attr (颜色尺寸，附加表)
M_DIM d6  (系列)      │
M_PRODUCT_ALIAS (条码)─┘
M_ATTRIBUTESETINSTANCE (属性)
```

### 具体做了什么

1. **从Oracle抽数**：一条SQL把商品表和4个维度表JOIN起来，拿到每个商品的完整信息
2. **计算is_main_product**：如果商品的类别ID在主销品列表里（134/142/139...共12个），标记为"Y"，否则标记"N"
3. **抽取颜色/尺寸**：通过SKU条码表关联属性表，每个商品取第一个SKU的颜色和尺寸（ROW_NUMBER排序取第一个）
  - 关联键：M_PRODUCT_ALIAS.M_ATTRIBUTESETINSTANCE_ID → M_ATTRIBUTESETINSTANCE.ID
4. **清空MySQL表**：TRUNCATE TABLE dim_product —— 完全清空后重写
5. **写入商品数据**：去掉颜色/尺寸列后写入dim_product
6. **写入属性数据**：颜色/尺寸单独写入dim_product_attr表（replace模式）

### 为什么全量覆盖

商品信息可能改动（改名、调价、调类别），增量同步太复杂，一万多条商品全量写入只要几分钟。

### 关键字段映射

| MySQL字段 | Oracle来源 | 说明 |
|-----------|------------|------|
| product_id | M_PRODUCT.ID | 主键 |
| product_code | M_PRODUCT.NAME | 款号（如HFM03705864-4） |
| product_name | M_PRODUCT.VALUE | 品名（如气泡方糖项链） |
| category_id/name | M_DIM4.ID/ATTRIBNAME | 类别（项链/耳环等） |
| property_id/name | M_DIM5.ID/ATTRIBNAME | 性质（在售/绝版等） |
| series_id/name | M_DIM6.ID/ATTRIBNAME | 系列 |
| brand_id/name | M_DIM1.ID/ATTRIBNAME | 品牌 |
| material | M_PRODUCT.FABELEMENT | 材质成分 |
| price_list | M_PRODUCT.PRICELIST | 吊牌价 |
| price_cost | M_PRODUCT.PRECOST | 成本价 |
| is_main_product | 计算字段 | 类别ID在12个主销品ID内=Y |
| is_active | M_PRODUCT.ISACTIVE | 只抽有效商品(Y) |

### 注意事项

- 只抽取 `ISACTIVE = 'Y'` 的商品
- dim_product_attr 是附加表，每次 replace 覆盖
- MySQL表中的 `year_id`/`year_name` 字段当前ETL未填充，未在代码实现写入（对应 Oracle M_DIM2_ID 维度），历史遗留

---

## 三、etl_dim_sku.py — SKU信息同步

### 一句话

**把每个SKU（款号+颜色+尺码=一个SKU）的条码、颜色、尺寸信息，从Oracle同步到MySQL。**

### 数据流

```
Oracle                              MySQL
━━━━━                              ━━━━━
M_PRODUCT_ALIAS (条码表)    ─┐
M_ATTRIBUTESETINSTANCE (属性)─┘──→  dim_sku (SKU维度表)
```

### 具体做了什么

1. **从Oracle抽数**：JOIN条码表和属性表，拿到每个SKU的条码号、关联商品ID、颜色、尺寸
2. **只取有效SKU**：`WHERE pa.ISACTIVE = 'Y'`
3. **全量覆盖写入**：TRUNCATE → INSERT
4. **属性关联键**：M_PRODUCT_ALIAS.M_ATTRIBUTESETINSTANCE_ID → M_ATTRIBUTESETINSTANCE.ID

### 为什么需要这张表

- **dim_product** 是款号级别的（一个款有多个颜色尺码）
- **dim_sku** 是最细粒度——一个条码就是一个SKU
- 库存和销售数据都是SKU粒度的，需要这张表来关联商品信息

### 关键字段映射

| MySQL字段 | Oracle来源 | 说明 |
|-----------|------------|------|
| sku_id | M_PRODUCT_ALIAS.ID | SKU主键 |
| sku_barcode | M_PRODUCT_ALIAS.NO | SKU条码 |
| product_id | M_PRODUCT_ALIAS.M_PRODUCT_ID | 关联的商品ID |
| sku_color | M_ATTRIBUTESETINSTANCE.VALUE1 | 颜色 |
| sku_size | M_ATTRIBUTESETINSTANCE.VALUE2 | 尺寸 |

### 注意事项

- Oracle的COLOR/SIZE是保留字，不能直接做别名，所以SQL里写成 `sku_color`/`sku_size`
- 数据量约15000+ SKU，全量写入约1分钟

---

## 四、etl_dim_store.py — 店仓信息同步

### 一句话

**把所有有效的门店和仓库信息（编码、名称、区域、是否云仓等），从Oracle同步到MySQL。**

### 数据流

```
Oracle                       MySQL
━━━━━                       ━━━━━
C_STORE (店仓档案) ─┐
C_AREA  (区域表)   ─┘──→  dim_store (店仓维度表)
```

### 具体做了什么

1. **从Oracle抽数**：JOIN店仓表和区域表
2. **计算store_type**：根据编码前缀自动判断类型
   - `001` → 总仓
   - `DS开头` → 电商
   - `RT开头` → 门店
   - `CS开头` → 测试
   - 其他 → 功能仓
3. **处理云仓标识**：`NVL(IS_ALLO2OSTORAGE, 'N')` → 空值填N
4. **只取有效店仓**：`WHERE ISACTIVE = 'Y'`
5. **全量覆盖**：TRUNCATE → INSERT

### 为什么云仓标识很重要

- 电商可售库存 = 总仓(001) + 云仓门店(IS_ALLO2OSTORAGE='Y')
- 48家门店参与云仓，云仓库存17.9万件，比总仓10.9万件还多
- 不标识云仓就会严重低估可售库存

### 关键字段映射

| MySQL字段 | Oracle来源 | 说明 |
|-----------|------------|------|
| store_id | C_STORE.ID | 主键 |
| store_code | C_STORE.CODE | 编码(001/DS001/RT001...) |
| store_name | C_STORE.NAME | 名称 |
| is_cloud_store | C_STORE.IS_ALLO2OSTORAGE | 是否云仓(Y/N) |
| store_type | 计算字段 | 根据CODE前缀判断 |
| area_name | C_AREA.NAME | 区域名称 |

---

## 四点五、etl_dim_channel.py — 渠道信息同步

### 一句话

**把 Oracle 里的电商渠道主档 O2O_RETAIL_CHANNEL 同步到 MySQL dim_channel，补齐仓库内可追溯链路。目标库现存数据是否已替换，仍需执行脚本后验证。**

### 数据流

```
Oracle                              MySQL
━━━━━                              ━━━━━
O2O_RETAIL_CHANNEL ───────────────→ dim_channel (渠道维度表)
```

### 具体做了什么

1. **从Oracle抽数**：读取 `ID / NAME / CODE / WING_CODE / ISACTIVE`
2. **补齐店仓映射**：`WING_CODE = O2O_RETAIL_CHANNEL.WING_CODE`，直接保留 DS001 这类店仓编码
3. **计算主要渠道**：按已在文档中确认的渠道ID集合打标 `is_main`
4. **归类平台类型**：根据渠道名称归到天猫/京东/抖音/小红书/视频号/唯品会/得物/其他
5. **全量覆盖写入**：TRUNCATE → INSERT

### 关键字段映射

| MySQL字段 | Oracle来源 | 说明 |
|-----------|------------|------|
| channel_id | O2O_RETAIL_CHANNEL.ID | 主键 |
| channel_name | O2O_RETAIL_CHANNEL.NAME | 渠道名称 |
| channel_code | O2O_RETAIL_CHANNEL.CODE | 渠道档案编码，不直接等同店仓编码 |
| WING_CODE | O2O_RETAIL_CHANNEL.WING_CODE | 店仓编码直接来源 |
| is_main | 计算字段 | 主要渠道ID集映射 |
| platform_type | 计算字段 | 按名称归类平台 |
| is_active | O2O_RETAIL_CHANNEL.ISACTIVE | 是否有效 |

### 为什么要单独建这张表

- 原来 dim_channel 只有数据库里一张表，仓库内没有装载入口，审计时无法归因
- 现在渠道维度与店仓维度一样，仓库内已经补齐 Oracle → MySQL 全量同步链路
- 但该链路尚未在最新交接记录中证明已对目标库执行过真实写入，所以关闭待办前仍要检查 `WING_CODE` 是否已回填为 DS 编码

---

## 五、etl_dws_sales.py — 销售数据同步

### 一句话

**每天从Oracle拉取零售单数据，按「日期+店仓+商品+SKU」粒度汇总出销售量、销售额、退货量、退货额，写入MySQL。**

### 数据流

```
Oracle                                  MySQL
━━━━━                                  ━━━━━
M_RETAILITEM (零售明细) ─┐
M_RETAIL     (零售主表) ─┤
C_STORE      (店仓)     ─┤──→  dws_sales_daily (日销售汇总)
M_PRODUCT    (商品)     ─┘
```

### 具体做了什么

1. **确定日期范围**（智能模式）：
   - 凌晨0-6点运行 → 查昨天完整数据
   - 白天运行 → 查今天实时数据
   - 可通过参数 `days_back` 回溯更多天

2. **从Oracle聚合抽数**：一条GROUP BY的SQL，直接在Oracle端完成汇总
   - 分组维度：日期、店仓ID、店仓编码、云仓标识、商品ID、SKU ID
   - 正单（TOT_AMT_ACTUAL > 0）汇总为销售数量/销售额
   - 负单（TOT_AMT_ACTUAL < 0）汇总为退货数量/退货额（取绝对值）
   - 订单数：**只计正单的不重复零售单数**

3. **先删后插**（增量同步）：
   - 先删除MySQL中该日期范围的旧数据
   - 再插入新数据

4. **自动回补**（run_etl.py中）：
   - 检查近30天数据是否覆盖完整
   - 如果不足30天，自动拉取补齐

### 关键业务规则

```
销售判断：看零售单主表的 TOT_AMT_ACTUAL
  > 0 → 这是一笔销售（出库）
  < 0 → 这是一笔退货（入库）

⚠️ 重要过滤条件（写死在SQL里）：
  - ISACTIVE = 'Y' 且 STATUS = 2（已审核）
  - M_PRODUCTALIAS_ID IS NOT NULL（必须有SKU）

⚠️ 这里不做任何业务过滤！
  - 不过滤渠道（电商/门店都有）
  - 不过滤品类（主销品/辅销品都有）
  - 业务口径过滤统一在ADS层做
```

### 输出字段

| 字段 | 说明 | 计算逻辑 |
|------|------|----------|
| date_id | 日期 | M_RETAIL.BILLDATE |
| store_id | 店仓ID | M_RETAIL.C_STORE_ID |
| store_code | 店仓编码 | C_STORE.CODE |
| is_cloud_store | 云仓标识 | C_STORE.IS_ALLO2OSTORAGE |
| product_id | 商品ID | M_RETAILITEM.M_PRODUCT_ID |
| m_productalias_id | SKU ID | M_RETAILITEM.M_PRODUCTALIAS_ID |
| sales_qty | 销售数量 | SUM(QTY) WHERE 正单 |
| sales_amount | 销售金额 | SUM(TOT_AMT_ACTUAL) WHERE 正单 |
| sales_amount_list | 吊牌金额 | SUM(TOT_AMT_LIST) WHERE 正单 |
| return_qty | 退货数量 | SUM(ABS(QTY)) WHERE 负单 |
| return_amount | 退货金额 | SUM(ABS(TOT_AMT_ACTUAL)) WHERE 负单 |
| order_count | 订单数 | COUNT(DISTINCT 正单的M_RETAIL.ID) |
| etl_time | ETL时间 | 写入时间戳 |

> **注意**：MySQL表中有 `net_qty` 和 `net_amount` 字段，但ETL不填充它们，未在代码实现写入（MySQL默认值为0）。如需净销量/净销售额，请在查询时自行计算：`net_qty = sales_qty - return_qty`。

### backfill() 补数功能

```python
# 需要补历史数据时，传入起止日期即可
from etl_dws_sales import backfill
backfill(20260101, 20260130)  # 补2026年1月整月数据
```

---

## 六、etl_dws_inventory.py — 库存快照同步

### 一句话

**每天从Oracle拍一张库存"照片"——记录当天每个SKU在总仓和云仓的库存数量、采购欠数，写入MySQL。**

### 数据流

```
Oracle                              MySQL
━━━━━                              ━━━━━
FA_STORAGE (实时库存) ─┐
C_STORE    (店仓)     ─┤──→  dws_inventory_daily (日库存快照)
M_PRODUCT  (商品)     ─┘
```

### 具体做了什么

1. **从Oracle查当前库存**：
   - 只查总仓(001)和云仓门店(IS_ALLO2OSTORAGE='Y')的库存
   - 只查有SKU条码的记录（M_PRODUCTALIAS_ID IS NOT NULL）
   - ISACTIVE = 'Y'（有效记录）
   - **不做品类过滤** — 全量SKU都拍快照，品类过滤在ADS层做

2. **处理重复记录**：
   - 如果同一个(店仓, 商品, SKU)有多条库存记录，合并数量
   - 发现重复时会在日志中打warning

3. **加日期标签**：给每条记录打上今天的日期（date_id = YYYYMMDD）

4. **写入MySQL**（单事务）：
   - 删除今天已有的数据（支持重跑）
   - 写入新数据
   - 如果中途失败，自动回滚，不会出现删了没写入的情况

### 关键字段

| 字段 | 说明 | 来源 |
|------|------|------|
| date_id | 快照日期 | 当天日期(YYYYMMDD) |
| store_id | 店仓ID | FA_STORAGE.C_STORE_ID |
| store_code | 店仓编码 | C_STORE.CODE |
| is_cloud_store | 云仓标识 | C_STORE.IS_ALLO2OSTORAGE |
| product_id | 商品ID | FA_STORAGE.M_PRODUCT_ID |
| m_productalias_id | SKU ID | FA_STORAGE.M_PRODUCTALIAS_ID |
| qty | 库存数量 | FA_STORAGE.QTY |
| qty_valid | 可用库存 | FA_STORAGE.QTY（注意：不是QTYVALID，因为源系统该字段未维护，全为0） |
| qty_occupy | 占用数量 | 固定填0（源表字段未使用） |
| qtypurchaserem | 采购欠数/在途 | FA_STORAGE.QTYPURCHASEREM（已下单未到货） |
| etl_time | ETL时间 | 写入时间戳 |

### 为什么要每天拍快照

- 库存是**状态数据**，今天10件明天可能变8件
- 如果只存当前值，就无法分析"上周库存是多少"
- 每天拍照，就能画出库存的变化趋势
- 也可以追溯"某天某个SKU库存到底是多少"

### 为什么不过滤品类

- 快照保留全量SKU（包括辅销品、配件等）
- 主销品过滤在ADS层统一控制
- 好处：如果将来品类分类调整，历史快照不受影响

---

## 七、etl_ads_health.py — 库存健康度计算

### 一句话

**不查Oracle，纯在MySQL内部计算：把库存+销售+商品+达播数据JOIN到一起，算出每个SKU的周转天数、库存状态、SABC分级、建议补货数量。**

### 数据流

```
MySQL内部
━━━━━━━━
dws_inventory_daily (当天库存) ─┐
dws_sales_daily (近30天销售)   ─┤
ads_dabo_daily_sales (达播数据)─┤──→ ads_inventory_health
dim_product (商品维度)         ─┤    (每个SKU的健康度报告)
dim_sku (SKU维度)              ─┤
dim_store (店仓维度)           ─┘
```

### 具体做了什么（分4步）

#### 第0步：字段补齐与时间窗口

- **字段补齐**：检查 ads_inventory_health 是否缺列，按列名（COLUMN_NAME）补齐，内部变量为 col_name。
- **时间变量**：date_30_ago_date / date_7_ago_date / today_date 用于达播时间窗口；date_7_ago_id 用于近7天销售窗口。

#### 第1步：一条大SQL算出所有指标

以库存汇总表为主表，LEFT JOIN销售、商品、SKU、达播数据：

- **库存汇总**：总仓+云仓，按(product_id, sku_id)汇总 → 得到total_qty/warehouse_qty/cloud_qty/purchase_rem_qty
- **销售汇总**：电商(DS%)+云仓门店，近30天+近7天 → 得到sales_qty_30d/sales_amt_30d/sales_qty_7d/return_qty_30d
- **达播汇总**：ads_dabo_daily_sales，近30天+近7天 → 按SKU条码匹配
- **自然销量** = 全量销量 - 达播销量（用于分析剔除达播后的自然增长趋势）

然后计算：

| 指标 | 公式 | 说明 |
|------|------|------|
| 日均销量(30天) | sales_qty_30d / 30 | |
| 日均销量(7天) | sales_qty_7d / 7 | |
| 销售加速度 | (7天日均) / (30天日均) | >1加速，<1减速 |
| 周转天数 | total_qty / 日均销量 | 库存够卖几天 |
| 库存状态 | 看下方规则 | |
| 建议补货 | 看下方公式 | |

**库存状态判断**：
```
有库存 + 无销售 → 滞销
无库存 + 无销售 → 停售
周转 < 30天 → 紧急缺货
30 ≤ 周转 < 70 → 需补货
70 ≤ 周转 ≤ 90 → 正常
周转 > 90 → 库存过高
```

**状态优先级（status_priority）**：
```
紧急缺货=1，需补货=2，正常=3，库存过高=4，滞销=5，停售=6
```

**建议补货公式**：
```
建议补货 = (90天目标 - 当前周转天数) × 日均销量 - 退货量 - 采购欠数
```

- 退货扣减：退货的货物会返回仓库，等于预期会有回补
- 采购欠数扣减：已下单未到的货也是预期回补
- **允许负数**：负数表示库存过剩，不需要补货反而需要消化

#### 第2步：SABC分级

按近30天销售额从高到低排序，计算每个SKU的累计销售占比：

| 分级 | 规则（代码实际逻辑） | 含义 |
|------|---------------------|------|
| S | 该SKU之前的累计占比 < 30% | 贡献前30%销售额的超级爆款 |
| A | 含该SKU的累计占比 ≤ 70% | 贡献30%~70%的核心款 |
| B | 含该SKU的累计占比 ≤ 90% | 贡献70%~90%的常规款 |
| C | 累计占比 > 90% 或无销售 | 长尾/滞销款 |

补充字段：
- sales_rank：按 sales_amt_30d 降序排名
- sales_ratio：单SKU销售占比（%）
- cumulative_ratio：累计销售占比（%）

#### 第3步：销售趋势判断

根据销售加速度（sales_velocity）判断趋势：

| 加速度范围 | 趋势 |
|-----------|------|
| ≥ 1.3 | 快速上升 |
| 1.0 ~ 1.3 | 稳定 |
| 0.7 ~ 1.0 | 降温 |
| < 0.7 | 快速下滑 |
| 无销售 | 无销售 |

#### 第4步：达播回填（可选）

如果今天的达播CSV数据已就绪（ads_dabo_daily_sales有今日记录），则：
1. 将达播销量/销售额更新到当日ads_inventory_health
2. 重新计算自然销量 = 全量销量 - 达播销量
3. 重新计算自然加速度

#### 第5步：汇总输出（日志）

- total_rem_qty：采购欠数合计
- total_suggest_qty：净建议补货合计（正数-负数）

### 口径说明（⚠️ 核心）

| 数据 | 口径 | 为什么 |
|------|------|--------|
| 库存 | 总仓(001) + 云仓(IS_ALLO2OSTORAGE='Y') | 电商可售库存 |
| 销售 | 电商(DS%) + 云仓门店 | 线上+云仓出货 |
| 品类 | 仅主销品（12个类别ID） | 排除配件辅料等 |
| SKU | M_PRODUCTALIAS_ID IS NOT NULL | 必须有条码 |
| 达播 | 按SKU条码匹配 | 条码 = product_alias_code |

---

## 八、etl_ods_fa_storage.py — ODS库存原始层

### 一句话

**把Oracle FA_STORAGE表原封不动全量复制到MySQL的ods_fa_storage表。**

### 特点

- **全量覆盖**：每次TRUNCATE后重新写入
- **不做任何过滤**：不管ISACTIVE、不管店仓、不管品类——全量搬运
- **分批写入**：每批5万条，避免内存爆掉
- **带批次号**：内部变量 batch_id 写入 etl_batch_id，标记本次执行

### 与dws_inventory_daily的区别

| | ods_fa_storage | dws_inventory_daily |
|--|---------------|-------------------|
| 数据范围 | 全部库存记录 | 仅总仓+云仓 |
| 过滤 | 无 | ISACTIVE='Y' + SKU非空 |
| 用途 | 原始存档、排查问题 | 作为ADS计算的输入 |
| 是否日常必须 | 可选 | 必须 |

---

## 九、etl_ods_m_retail.py — ODS零售单原始层

### 一句话

**把Oracle M_RETAIL表（零售单主表）同步到MySQL的ods_m_retail表，支持全量和增量两种模式。**

### 增量同步逻辑

- **水位字段**：MODIFIEDDATE（修改时间）
- **回刷窗口**：默认回刷7天，防止漏数据
- **窗口滑动**：按天切分窗口，每个窗口先删后插
- **断点续跑**：通过ods_sync_state表记录窗口进度，中断后可从断点继续
- **空MODIFIEDDATE**：全量模式下先处理MODIFIEDDATE为空的记录
- **批次号**：内部变量 batch_id 写入 etl_batch_id，标记本次执行

### 与dws_sales_daily的区别

- ODS保留原始字段，DWS做了聚合汇总
- ODS是独立执行的（run_ods.py），不影响日常ETL链路

---

## 十、etl_ods_m_retailitem.py — ODS零售明细原始层

### 一句话

**把Oracle M_RETAILITEM表（零售明细）同步到MySQL，难点在于线上和线下两条数据写入链路。**

### 双水位策略（核心设计）

```
线上订单（电商渠道）：
  标准字段 MODIFIEDDATE 有值
  → 按 MODIFIEDDATE 做增量水位

线下订单（门店收银）：
  MODIFIEDDATE 通常为空
  SETTIME（设置时间）有值
  → 按 SETTIME 做增量水位
```

- **两条水位独立记录**：
  - `ods_m_retailitem`：记录 MODIFIEDDATE 水位
  - `ods_m_retailitem_settime`：记录 SETTIME 水位
- 增量模式下两条通道并行执行
- 全量模式下先处理MODIFIEDDATE为空的记录，再按窗口处理有MODIFIEDDATE的
- **SETTIME窗口**：增量模式会计算 set_start_time / set_end_time 作为线下通道回刷窗口
- **批次号**：内部变量 batch_id 写入 etl_batch_id，标记本次执行

### 为什么需要双水位

- 伯俊ERP线下门店收银时，不走标准审计字段（MODIFIEDDATE为空）
- 如果只按MODIFIEDDATE做增量，门店订单会全部漏掉
- 所以必须双轨并行：MODIFIEDDATE（线上）+ SETTIME（线下）

---

## 十一、run_etl.py — 主控调度

### 一句话

**ETL的"总指挥"——按顺序执行8个步骤，统一处理异常、重试、发送企业微信通知。**

### 执行流程

```
run_etl.py
  ├─> [1/8] etl_dim_product.run()     商品维度
  ├─> [2/8] etl_dim_sku.run()         SKU维度
  ├─> [3/8] etl_dim_store.run()       店仓维度
  ├─> [4/8] etl_dim_channel.run()     渠道维度
  ├─> [5/8] etl_dws_sales.run()       销售数据
  │         ↳ 自动检查近30天覆盖度，不足则backfill
  ├─> [6/8] etl_dws_inventory.run()   库存快照
  ├─> [7/8] 达播数据就绪检查           查MySQL看今天有没有达播数据
  └─> [8/8] etl_ads_health.run()      库存健康度计算
            ↳ 如果达播就绪，还会执行 backfill_dabo_fields()

- **达播latest_date**：达播就绪检查会记录最新日期（MAX sale_date）用于日志与告警。
- 通用耗时统计：各ETL脚本使用 `start_time`/`end_time` 计算 duration（秒）。
```

### 重试机制

| 配置 | 默认值 | 说明 |
|------|--------|------|
| ETL_MAX_RETRIES | 3 | 最多重试几次 |
| ETL_RETRY_SLEEP | 60秒 | 两次重试之间等多久 |

**不可重试的错误**（遇到直接放弃，不浪费时间）：
- ORA-01017（用户名/密码错误）
- Access denied（MySQL权限问题）
- ORA-01000（超出最大游标数）

**可重试的错误**：
- timeout（超时）
- Connection refused（连接被拒）
- deadlock（死锁）

### 企业微信通知

- **成功**：发一条摘要，包含每步状态、耗时、关键指标
- **失败**：也发一条摘要，附上失败原因
- **统一模板**：执行时间、总耗时、成功/警告/失败计数、步骤明细
- **步骤键名**：task_name 作为步骤字典键，用于汇总明细

### 连接测试模式

```bash
# 只测试Oracle和MySQL能不能连上，不写数据
python run_etl.py --conn-test
```

---

## 十二、run_ods.py — ODS独立入口

### 一句话

**ODS层的"专用入口"——按顺序执行3个ODS表同步，结束后自动运行质量校验。**

### 执行流程

```
run_ods.py
  ├─> etl_ods_fa_storage.run()     ODS库存（全量）
  ├─> etl_ods_m_retail.run()       ODS零售单（增量/全量）
  ├─> etl_ods_m_retailitem.run()   ODS零售明细（增量/全量）
  └─> 质量校验（可选）
       ├─> check_ods_incremental.py     对账（Oracle vs MySQL行数）
       └─> check_ods_retailitem_quality.py  明细双通道拆分校验
```

### 常用命令

```bash
# 默认：增量模式，回刷7天
python run_ods.py

# 强制全量
python run_ods.py --full

# 跳过质量校验
python run_ods.py --skip-qc

# 调整回刷天数
python run_ods.py --backfill-days 14

# 质量校验日期范围（仅影响QC）
# 参数名：qc_start_date / qc_end_date（YYYYMMDD）
python run_ods.py --qc-start-date 20250101 --qc-end-date 20250131

# 质量校验输出字段
# count / sum_qty / sum_amount
```

### 与run_etl.py的关系

- **完全独立**：run_ods.py 不影响 run_etl.py 的执行
- **DWS/ADS层直接读Oracle**：不依赖ODS表
- **ODS的价值**：保留原始数据用于排查问题、数据对账

---

## 十三、scheduled_etl.py — 定时调度包装

### 一句话

**Windows计划任务的入口脚本——设置好日志文件、调用run_etl.py、成功后再跑一遍数据验证。**

### 调度链路

```
Windows计划任务（每天凌晨3:00）
  → run_scheduled_etl.bat
    → scheduled_etl.py
      → run_etl.run_main()      # 含重试 + 企微通知
      → test_etl_automation.py  # ETL成功后验证数据
```

### 日志

- 文件位置：`logs/etl_YYYYMMDD.log`
- 同时输出到控制台和日志文件
- ETL失败时返回非零退出码
- exit_code：0=成功，1=数据验证异常，3=调度异常
- 数据验证中会输出 null_code / null_name（商品编码/名称空值计数）

---

## 十四、config.py — 配置中心

### 一句话

**所有配置集中管理——数据库连接、业务常量、告警设置、重试策略。**

### 配置分类

| 类别 | 内容 | 敏感性 |
|------|------|--------|
| Oracle连接 | host/port/user/password/service | 🔒 从环境变量读取 |
| MySQL连接 | host/port/user/password/database | 🔒 从环境变量读取 |
| 主销品类别ID | (134,142,139,...共12个) | 业务常量 |
| 性质ID | 在售/新品/绝版 | 业务常量 |
| 企业微信 | WECHAT_WEBHOOK | 🔒 从环境变量读取 |
| 重试策略 | 最大次数/间隔/重试关键字/不重试关键字 | 运维配置 |
| Oracle校验SQL | ORACLE_VERIFY_QUERIES | 测试用 |

### ⚠️ 安全提醒

数据库密码和webhook地址**不要**写死在代码里，请通过环境变量注入：
```powershell
$env:ORACLE_USER = 'your_user'
$env:ORACLE_PASSWORD = 'your_password'
$env:WECHAT_WEBHOOK = 'https://qyapi.weixin.qq.com/...'
```

---

## 十五、alerts.py — 企业微信告警

### 一句话

**封装企业微信机器人webhook调用，ETL完成（成功或失败）后发送通知。**

### 功能

- 发送text类型消息
- 自动截断超长文本（默认最大1500字）
- 支持@指定手机号
- 未配置webhook时静默跳过
- 10秒超时，异常不影响主流程
- status_code：用于判断发送成功/失败并写入日志

### 如何替换告警渠道

只需修改 `alerts.py`，把 `send_wechat_alert` 函数改为调用钉钉/邮件/Slack等，其他代码无需改动。

---

## 附录：快速问答

### Q: 为什么DWS层直接读Oracle，不经过ODS？

A: 设计上DWS层是核心链路，直接读Oracle保证数据最新；ODS是可选层，主要用于数据存档和排查。两者独立运行互不影响。

### Q: 为什么销售数据不过滤品类和渠道？

A: "全量抽取，按需过滤"原则。DWS层保留全量数据，业务口径（主销品类、电商+云仓渠道）在ADS层统一控制。好处是DWS层不因业务口径变化而需要重跑。

### Q: 库存快照为什么保留QTY=0的记录？

A: FA_STORAGE中QTY=0的记录仍表示该商品在该仓库中被管理过。保留这些记录有助于分析商品在各仓库的分布情况。

### Q: 退货算法里为什么要扣减退货量？

A: 退货的货物最终会返回仓库。假设某SKU这个月退了50件，这50件会回到可售库存中，所以建议补货时要减掉这部分。

### Q: 达播数据和普通销售数据怎么关联？

A: 通过SKU条码（barcode）匹配。达播CSV中的"商家编码" = dim_sku的"sku_barcode" = ads_dabo_daily_sales的"product_alias_code"。由于字符集可能不同，关联时使用了 `COLLATE utf8mb4_unicode_ci`。

### Q: SABC分级中S级和A级的边界怎么定？

A: 按销售额降序排列SKU，逐个累加占比。当一个SKU让累计占比首次超过30%，这个SKU及之前的都是S级；继续累加到70%的是A级，到90%的B级，之后是C级。

---

## 附录：代码字段命名对照

| 字段名 | 含义 | 说明 |
|--------|------|------|
| start_time | 任务开始时间 | 脚本内变量，用于计算耗时 |
| end_time | 任务结束时间 | 脚本内变量，用于计算耗时 |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-02-27 | 初版：ETL人话版说明 |
| v1.1 | 2026-02-28 | 补充调度任务键名（run_etl.py） |
| v1.2 | 2026-02-28 | 补充ODS批次号与质量校验参数说明 |
| v1.3 | 2026-02-28 | 补充库存健康度时间变量与告警返回码说明 |
| v1.4 | 2026-02-28 | 补充属性关联键与校验字段说明 |
| v1.5 | 2026-02-28 | 标注year_id/year_name与net字段未在代码实现写入 |
| v1.6 | 2026-02-28 | 补充start_time/end_time耗时统计说明 |
| v1.7 | 2026-02-28 | 统一start_time/end_time反引号标注 |
| v1.8 | 2026-02-28 | 增加代码字段命名对照表 |
| v1.9 | 2026-03-18 | 新增 dim_channel 人话说明并同步主控调度为 8 步 |
| v2.0 | 2026-03-18 | 将 dim_channel 店仓字段重命名为 WING_CODE 并对齐 Oracle 来源 |
