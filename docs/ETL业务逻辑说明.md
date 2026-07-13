# 何方珠宝 - ETL业务逻辑说明（人话版）

> 每个ETL脚本干了什么？数据从哪来到哪去？为什么这么做？
> 面向：业务同事、新入职开发、运维人员

---

## 重要实施边界

- 2026-05-13 用户已将 Windows 计划任务入口切到 `run_scheduled_total_control_v2.bat`，M6 已进入 V2 调度入口观察；09:09 失败根因是主链 V2 先读 `_v2` DWS 计算 `ads_inventory_health`，而当日 `_v2` DWS 尚未刷新。
- 当前 `scheduled_total_control.py` 已修正为 V2 模式先执行阻断型 `DWS v2 读源预刷新`，再触发 `scheduled_etl.py` 与销售专题；预刷新调用 `scheduled_dws_v2_shadow.py --skip-ads-shadow-validation`，只刷新 raw / DWD / DWS v2，不在主链 ADS 重算前比较持久化 ADS。未显式传参时默认仍是 `legacy`。
- 当前 ADS 相关 MySQL 表已被 Tableau 和其他下游消费；未来若由影子链替代旧链，只允许新增字段，不允许改名或删除既有 ADS 字段。

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

### 执行顺序（9步）

| 步骤 | 脚本 | 做什么 | 耗时 |
|------|------|--------|------|
| 1 | etl_dim_product | 把Oracle里的商品信息复制到MySQL | ~3分钟 |
| 2 | etl_dim_sku | 把Oracle里的SKU条码信息复制到MySQL | ~1分钟 |
| 3 | etl_dim_store | 把Oracle里的店仓信息复制到MySQL | ~1分钟 |
| 4 | etl_dim_channel | 把Oracle里的电商渠道信息复制到MySQL | ~1分钟 |
| 5 | ods_sync（run_ods） | 同步 ODS 原始层并执行 ODS 质量校验 | ~5分钟 |
| 6 | etl_dws_sales | 主链按近7天窗口回带销售数据（独立运行仍保留昨天/今天智能模式，已消费ODS） | ~5分钟 |
| 7 | etl_dws_inventory | 拍一张当天的库存"照片"（已消费ODS） | ~10分钟 |
| 8 | (达播就绪检查) | 看看今天的达播CSV是否已导入 | ~1秒 |
| 9 | etl_ads_health | 在MySQL里算库存健康度 | ~5分钟 |

**调度任务键名（run_etl.py）**：
dim_product / dim_sku / dim_store / dim_channel / ods_sync / dws_sales / dws_inventory / dabo_ready / ads_health

**依赖关系**：步骤1-4可以独立跑；步骤5 为 ODS 原始层准备步骤；步骤6-7 当前已消费 ODS；步骤9依赖步骤1-7的结果。

**独立专项任务**：`etl_ads_store_daily_report.py`、`etl_ads_store_daily_subject_report.py` 与 `etl_ads_daily_sales.py` 当前都不在这 9 步主链里，保持手工或专题调度独立触发；前两者分别产出门店经营日报最终经营实体层与统计主体层，`etl_ads_daily_sales.py` 现可由 `scheduled_store_daily_report.py` 在受影响日期、自然日缺口或 DWS freshness 命中日期批量重跑时触发。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L49)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L450)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L984)

---

## 二、etl_dim_product.py — 商品信息同步

### 一句话

**把 Oracle 商品主表、维度枚举和首个 SKU 属性整理成 MySQL 的商品维度表 `dim_product` 与附表 `dim_product_attr`。** 来源：[etl_dim_product.py](../etl_dim_product.py#L1)

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

**把所有店仓信息（含已失活门店，保留 `is_active` 状态）同步到 MySQL，供下游按需判定是否纳入口径。**

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
4. **全量保留店仓**：不再过滤 `ISACTIVE='N'`，而是把 `ISACTIVE` 直接落到 `is_active`
5. **安全处理开业日期**：将 `C_STORE.OPENDATE` 按 `YYYYMMDD` 安全转换为 `open_date`；原始空值和不可转换日期均落 NULL，非法日期只告警而不使整批维表刷新失败
6. **全量覆盖**：先校验目标字段齐全，再执行 TRUNCATE → INSERT

当前这样处理的原因是：`dim_store` 属于基础门店维表，闭店/停用门店仍可能被历史配置、月目标或专题 ADS 引用；是否继续参与业务统计，应由下游口径基于 `is_active`、配置表生效区间或目标范围决定，而不是在维表层直接物理剔除。

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
| open_date | C_STORE.OPENDATE | 安全转换后的开业日期；源值为空或非法时为 NULL |

`open_date` 的目标字段由 `SQL/alter_dim_store_add_open_date.sql` 提供，DDL 必须由用户人工执行。ETL 使用显式列清单写入；若需要回滚到旧版 ETL，可保留这个可空列，旧版按 DataFrame 列名写入不会因额外可空列失败。

---

## 四点五、etl_dim_channel.py — 渠道信息同步

### 一句话

**把 Oracle 里的电商渠道主档 O2O_RETAIL_CHANNEL 同步到 MySQL dim_channel，补齐仓库内可追溯链路。2026-03-23 已实查确认目标库现存数据已完成回填。**

### 数据流

```
Oracle                              MySQL
━━━━━                              ━━━━━
O2O_RETAIL_CHANNEL ───────────────→ dim_channel (渠道维度表)
```

### 具体做了什么

1. **从Oracle抽数**：读取 `ID / NAME / CODE / WING_CODE / ISACTIVE`
2. **补齐店仓映射**：`WING_CODE = O2O_RETAIL_CHANNEL.WING_CODE`，直接保留 Oracle 源值；2026-03-23 已核对当前源表与目标表中该字段均为非空，但现网值并不体现为 `DS001` 这类编码
3. **计算主要渠道**：按已在文档中确认的渠道ID集合打标 `is_main`
4. **归类平台类型**：根据渠道名称归到天猫/京东/抖音/小红书/视频号/唯品会/得物/其他
5. **全量覆盖写入**：TRUNCATE → INSERT

### 关键字段映射

| MySQL字段 | Oracle来源 | 说明 |
|-----------|------------|------|
| channel_id | O2O_RETAIL_CHANNEL.ID | 主键 |
| channel_name | O2O_RETAIL_CHANNEL.NAME | 渠道名称 |
| channel_code | O2O_RETAIL_CHANNEL.CODE | 渠道档案编码，不直接等同店仓编码 |
| WING_CODE | O2O_RETAIL_CHANNEL.WING_CODE | 渠道挂接码，按 Oracle 原值保留 |
| is_main | 计算字段 | 主要渠道ID集映射 |
| platform_type | 计算字段 | 按名称归类平台 |
| is_active | O2O_RETAIL_CHANNEL.ISACTIVE | 是否有效 |

### 为什么要单独建这张表

- 原来 dim_channel 只有数据库里一张表，仓库内没有装载入口，审计时无法归因
- 现在渠道维度与店仓维度一样，仓库内已经补齐 Oracle → MySQL 全量同步链路
- 2026-03-23 已核对 Oracle `O2O_RETAIL_CHANNEL` 与 MySQL `dim_channel`：两边均为 87 条记录，`WING_CODE` 全部非空，说明现网已完成真实回填

---

## 五、etl_dws_sales.py — 销售数据同步

### 一句话

**每天从 ODS 拉取零售单数据，按「日期+店仓+商品+SKU」粒度汇总出销售量、销售额、退货量、退货额，写入MySQL。**

### 数据流

```
Oracle                                  MySQL
━━━━━                                  ━━━━━
ods_m_retailitem (零售明细ODS) ─┐
ods_m_retail     (零售主表ODS) ─┤
dim_store        (店仓维度)    ─┤──→  dws_sales_daily (日销售汇总)
                               ┘
```

### 具体做了什么

1. **确定日期范围**：
  - 独立运行 `etl_dws_sales.run()` 时，仍按智能模式执行：凌晨0-6点查昨天完整数据，白天查今天实时数据
  - 可通过参数 `days_back` 回溯更多天
  - `run_etl.py` 主链当前固定把该步骤扩展为“近7天回带 + include_today=True”，用来承接 ODS 默认 7 天回刷后的晚到补数

2. **从MySQL ODS 聚合抽数**：在 MySQL 端从 ODS 主表、ODS 明细表和店仓维度完成汇总
   - 分组维度：日期、店仓ID、店仓编码、云仓标识、商品ID、SKU ID
  - 正单（零售单主表 `tot_amt_actual > 0`）汇总为销售数量/销售额
  - 负单（零售单主表 `tot_amt_actual < 0`）汇总为退货数量/退货额（取绝对值）
  - 当 `tot_amt_actual = 0` 时，按行级 `qty` 正负兜底：正数归总销，负数归退货
  - 订单数：**计入正单，或 `tot_amt_actual = 0` 且行级数量为正的零售单**

3. **先删后插**（增量同步）：
  - 先申请 `hefang_dw:dws_sales_daily` 命名锁，避免多个重跑会话同时覆盖同一批日期
  - 在同一事务里删除 MySQL 中该日期范围的旧数据，再插入新数据
  - 若遇到 `1213/1205` 或命名锁超时，最多退避重试 3 次

4. **自动回补**（run_etl.py中）：
  - 先执行 `run_ods_sync(backfill_days=7, qc_days=7)`，把 ODS 最近 7 天窗口统一补齐
  - 再执行 `etl_dws_sales.run(days_back=7, include_today=True)`，把同一窗口内的销售增量继续下沉到 DWS
  - 近30天覆盖度检查仍保留为兜底；如果 `COUNT(DISTINCT date_id) < 30`，再额外触发 `backfill`

来源：[run_etl.py](../run_etl.py#L59)；[run_etl.py](../run_etl.py#L526)；[run_etl.py](../run_etl.py#L544)；[run_etl.py](../run_etl.py#L570)；[etl_dws_sales.py](../etl_dws_sales.py#L178)

### 关键业务规则

```
销售判断：优先看 ODS 零售单主表的 tot_amt_actual
  > 0 → 这是一笔销售（出库）
  < 0 → 这是一笔退货（入库）
  = 0 → 用行级 qty 正负兜底，避免全额优惠/核销单被漏算

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
| date_id | 日期 | ods_m_retail.billdate |
| store_id | 店仓ID | ods_m_retail.c_store_id |
| store_code | 店仓编码 | dim_store.store_code |
| is_cloud_store | 云仓标识 | dim_store.is_cloud_store |
| product_id | 商品ID | ods_m_retailitem.m_product_id |
| m_productalias_id | SKU ID | ods_m_retailitem.m_productalias_id |
| sales_qty | 销售数量 | SUM(QTY) WHERE 正单或 `tot_amt_actual=0 且 qty>0` |
| sales_amount | 销售金额 | SUM(TOT_AMT_ACTUAL) WHERE 正单或 `tot_amt_actual=0 且 qty>0` |
| sales_amount_list | 吊牌金额 | SUM(TOT_AMT_LIST) WHERE 正单或 `tot_amt_actual=0 且 qty>0` |
| return_qty | 退货数量 | SUM(ABS(QTY)) WHERE 负单或 `tot_amt_actual=0 且 qty<0` |
| return_amount | 退货金额 | SUM(ABS(TOT_AMT_ACTUAL)) WHERE 负单或 `tot_amt_actual=0 且 qty<0` |
| order_count | 订单数 | COUNT(DISTINCT 正单，或 `tot_amt_actual=0 且 qty>0` 的 ods_m_retail.id) |
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

**每天从 ODS 拍一张库存"照片"——记录当天每个SKU在总仓和云仓的库存数量、采购欠数，写入MySQL。**

### 数据流

```
MySQL                                  MySQL
━━━━━                                  ━━━━━
ods_fa_storage (库存ODS) ─┐
dim_store      (店仓维度) ─┤──→  dws_inventory_daily (日库存快照)
                          ┘
```

### 具体做了什么

1. **从 ODS 查当前库存**：
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
  - 写入前会先申请 `hefang_dw:dws_inventory_daily` 命名锁，避免多个重跑会话同时覆盖同一天快照
  - 如果遇到 MySQL 死锁或锁等待超时，会按 5 秒、10 秒的节奏最多重试 3 次

### 关键字段

| 字段 | 说明 | 来源 |
|------|------|------|
| date_id | 快照日期 | 当天日期(YYYYMMDD) |
| store_id | 店仓ID | ods_fa_storage.c_store_id |
| store_code | 店仓编码 | dim_store.store_code |
| is_cloud_store | 云仓标识 | dim_store.is_cloud_store |
| product_id | 商品ID | ods_fa_storage.m_product_id |
| m_productalias_id | SKU ID | ods_fa_storage.m_productalias_id |
| qty | 库存数量 | ods_fa_storage.qty |
| qty_valid | 可用库存 | ods_fa_storage.qty（注意：当前仍沿用 qty 作为可用库存口径） |
| qty_occupy | 占用数量 | 固定填0（源表字段未使用） |
| qtypurchaserem | 采购欠数/在途 | ods_fa_storage.qtypurchaserem |
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
ads_dabo_order_label + ODS/缓存 ─┤──→ ads_inventory_health
dim_product (商品维度)         ─┤    (每个SKU的健康度报告)
dim_sku (SKU维度)              ─┤
dim_store (店仓维度)           ─┘
```

### 具体做了什么（分4步）

#### 第0步：字段补齐与时间窗口

- **字段补齐**：检查 ads_inventory_health 是否缺列，按列名（COLUMN_NAME）补齐，内部变量为 col_name。
- **时间变量**：date_30_ago_date / date_7_ago_date / today_date 用于达播时间窗口；date_7_ago_id 用于近7天销售窗口。

#### 第1步：一条大SQL算出所有指标

- 写入前会先申请 `hefang_dw:ads_inventory_health` 命名锁，避免多个会话同时重算当天 ADS 快照
- 删除当天旧数据与重新插入结果放在同一事务里；如果插入失败，会整笔回滚，不会留下“当天数据已被清空”的中间态
- 如果遇到 MySQL 死锁或锁等待超时，会按 5 秒、10 秒的节奏最多重试 3 次

以库存汇总表为主表，LEFT JOIN销售、商品、SKU、达播数据：

- **库存汇总**：总仓+云仓，按(product_id, sku_id)汇总 → 得到total_qty/warehouse_qty/cloud_qty/purchase_rem_qty
- **销售汇总**：电商(DS%)+云仓门店，近30天+近7天 → 得到sales_qty_30d/sales_amt_30d/sales_qty_7d/return_qty_30d
- **达播汇总**：优先使用最新 `ads_dabo_order_label` 批次，在 ODS 内按 `COALESCE(canonical_system_order_id, system_order_id) = oms_sourcecode` 识别达播订单，再对 `ods_m_retailitem` 按 SKU 汇总近30天+近7天销量/销售额；若 ODS 尚无对应订单，则回退 `ads_dabo_order_retail_bridge` 缓存；如果标签批次不可用，才回退 `ads_dabo_daily_sales` 兼容聚合表。来源：[etl_ads_health.py](../etl_ads_health.py#L195)；[etl_ads_health.py](../etl_ads_health.py#L270)
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

#### 第4步：达播来源选择与字段回填

`run_etl.py` 会先检查 `ads_dabo_order_label` 最新批次是否存在且最近 1 天有更新，并将该结果作为 `dabo_ready` 的主判定；同时附带输出 `ads_dabo_daily_sales` 的 legacy 状态。

主调度的选择规则变成：
1. 标签主线就绪：`ads_health` 直接按最新标签批次 + ODS/缓存兜底重算达播字段。
2. 标签主线未就绪但 legacy 当日可用：回退 `ads_dabo_daily_sales` 兼容表。
3. 两条路径都不可用：达播字段按 0 处理，但库存健康主链继续完成。

`etl_ads_health.run()` 在主 INSERT 后会统一调用 `backfill_dabo_fields()`，确保达播销量、达播销售额、自然销量、自然销售额和自然加速度都按同一来源重算。来源：[etl_ads_health.py](../etl_ads_health.py#L672)；[etl_ads_health.py](../etl_ads_health.py#L742)；[run_etl.py](../run_etl.py#L649)

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

## 七点五、etl_ads_store_daily_report.py — 门店经营日报

### 一句话

**不再靠手工执行临时 SQL，而是把已通过样本对账的日报 SQL 骨架封装成正式独立 ETL：直接按最终经营实体产出 `ads_store_daily_report`。**

### 数据流

```
MySQL内部
━━━━━━━━
ods_m_retail / ods_m_retailitem ─┐
dim_product / dim_store         ─┼─→ etl_ads_store_daily_report.py
dim_store_report_attr           ─┤    └─→ ads_store_daily_report
dim_product（固定排除147/149/150） ─┤
cfg_store_target_daily          ─┤
dim_store_operation_owner_assignment ─┤
cfg_store_assessment_subject_target_daily ─┤
cfg_store_assessment_assignment           ─┘
```

### 具体做了什么（分5步）

#### 第0步：只读前置检查

- 检查脚本内置 SQL 模板关键片段是否完整，不依赖外部 `.sql` 文件。
- 检查依赖表是否齐全：`dim_store_report_attr`、`cfg_store_target_daily`、`cfg_store_assessment_subject_target_daily`、`cfg_store_assessment_assignment`、`dim_store_operation_owner_assignment`、`ads_store_daily_report`、`ods_m_retail`、`ods_m_retailitem`、`dim_product`、`dim_store`。
- 同时检查目标表是否已具备 `owner_name` 与 `mtd_list_amt` 物理列，以及 `dim_store.open_date`；若目标库尚未执行对应 DDL，脚本会在真正删插前直接失败。
- `--conn-test` 模式只做到这里，不会写入数据。

#### 第1步：配置表重叠校验

- 先看 `dim_store_report_attr`：同一个 `report_date` 下，同一 `store_id` 不能命中多条有效配置。
- 再看 `dim_store_operation_owner_assignment`：同一个 `report_date` 下，同一经营实体不能命中多条负责人切片；若负责人历史表已开始维护但当前实体缺少有效切片，也直接阻断。
- 这样做的原因是：MySQL DDL 只能约束“起始日唯一”，但不能直接防住生效区间重叠；如果不先拦住，后面的 JOIN 会把门店或负责人实体重复放大。

#### 第2步：把运行参数注入脚本内置 SQL 模板

- ETL 层只负责传 3 个变量：`@report_date`、`@data_version`、`@etl_time`。
- 真正的计算口径当前直接内置在 `etl_ads_store_daily_report.py` 的 SQL 模板常量中；`docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql` 仅保留为设计参考，避免运行时再依赖外部文件。

#### 第3步：按日期+版本做先删后插

- 删除范围：`ads_store_daily_report WHERE report_date = @report_date AND data_version = @data_version`
- 再执行一条 `INSERT ... WITH ... SELECT ...`，生成当日该版本的整张日报宽表。
- 当前脚本仍是独立入口，**没有**并入 `run_etl.py` 主链，也没有默认调度时点。

#### 第4步：按最终经营实体收口日报

- **门店范围**：来自 `dim_store_report_attr` 当前生效且 `is_include_in_daily_report='Y'`、并且在 `cfg_store_target_daily` 当天存在 `target_date = report_date AND target_version = data_version` 目标行的门店；预建店或未来生效门店不会仅因已建档就提前进入日报口径。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L93)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L110)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L683)
- **经营实体映射**：未配置共同考核时保持原门店；命中 `cfg_store_assessment_assignment` 时，按挂靠主店与 `subject_code/subject_name` 把主店、快闪店等物理门店先映射到同一经营实体，再统一汇总销售事实与目标。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L109)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L148)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L184)
- **同店同比辅助口径**：以完整源物理门店范围驱动，先在源门店粒度按 `open_date <= 去年同期月份第一天` 判定资格，再回卷到最终经营实体；`open_date` 不可用时判非同店、记录告警且不回退旧销售额规则。两侧销售事实均左连接，去年同期金额为 0 的合格门店仍参与聚合但行级同比为 NULL；本期为 0、去年同期为正数的合格门店保留并产出 `-100%`。快闪成员继续排除纯同店辅助金额；月中快闪合并仍将去年同期累计上界截到最早快闪生效日前一天的去年同日。
- **负责人映射**：在经营实体映射完成后，再按 `report_entity_type + store_code + report_date BETWEEN effective_start_date AND effective_end_date` 左联 `dim_store_operation_owner_assignment`，把负责人下沉到 `owner_name`；负责人名称允许为空，但必须先命中唯一有效切片。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L149)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L370)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L466)
- **商品范围**：固定排除 `category_id in (147, 149, 150)`，即 `147=辅料`、`149=办公用品`、`150=道具`；其余有 `dim_product.category_id` 的商品默认纳入门店日报。因此 `146=配件`、`148=辅销品`、`394=配饰` 与后续新增 category_id 当前都会纳入口径，但**不回写**库存健康等链路沿用的主销品 12 类口径。
- **交易范围**：只取 `ABS(ri.tot_amt_actual) >= 1` 的明细行；绝对金额小于 1 的小额非零明细也整体排除，以与业务对账侧保持一致。退货负值直接冲减销售额和销量，所以日报当前是**净额 / 净量**口径。
- **订单数范围**：基于日报有效交易集先按 `retail_id` 去重，再按过滤后商品范围内的单号净额判断：`>0` 记 `1`，`<0` 记 `-1`，`ABS(金额) < 0.0001` 记 `0`；因此当前等价于“过滤后商品范围内正向净单数 - 负向净单数”，净零单不会被浮点残差误判。
- **月累计吊牌金额**：`mtd_list_amt` 按同一日报有效交易集与商品范围累计 `ods_m_retailitem.tot_amt_list`。Tableau 门店明细总计月折扣率按非免税口径重算：非免税 `mtd_sales_amt` 汇总 / 非免税 `mtd_list_amt` 汇总；免税外部月累计销售额只参与月总达成，不进入月客单价、月折扣率等其它 KPI 分子。
- **目标版本**：`cfg_store_target_daily` 按 `target_date = report_date` 且 `target_version = data_version` 精确匹配；若当前经营实体命中共同考核配置，则优先取 `cfg_store_assessment_subject_target_daily` 的主体目标。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L353)
- **目标字段关系**：`month_target` 是该门店当月固定目标；`day_target` 是该日期冻结后的日目标。业务可在月内动态调整每日目标，所以同一自然月内 `day_target` 合计允许不等于 `month_target`，脚本不做等值校验。
- **目标导入路径**：`cfg_store_target_daily` 的正式交付方案已明确为“业务投递 Excel 到 NAS 指定目录，由独立 Python 任务定时扫描并导入”；当前已冻结 NAS 目录为 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，并按月份分文件管理，当前推荐命名规则已切换为 `YYYYMM考核数据配置表.xlsx`；导入脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。主 `导入模板` 可选维护 `生效开始日`、`生效结束日`；空值分别默认目标月月初、月末，且必须落在目标月份自然月内。当前仓库已提供 `tools/import_cfg_store_target_daily_from_nas.py`，默认只做 dry-run，只有 `--apply` 才会写库；若 NAS 目录内同时存在多个 `目标月份` 文件，必须额外传入 `--target-month YYYY-MM` 显式选择本次导入月份；若同月同时存在多个版本文件，则需改用 `--file-path` 显式指定。若模板显式提供 `门店类型` 列，可追加 `--sync-store-report-attr` 同步刷新 `dim_store_report_attr`。若工作簿同时提供 `统计主体目标` 与 `门店考核归属` 两张可选 sheet，脚本还会同步刷新 `cfg_store_assessment_subject_target_daily` 与 `cfg_store_assessment_assignment`；其中 `门店考核归属` 当前新增必填列 `门店ID`，列名虽沿用 `门店ID`，但业务填写值应为 RT 门店编码，脚本优先按 `store_code` 命中 `dim_store`，纯数字时兼容 `store_id`。
- **新增品类同步边界**：若新品已在 `dim_product` 落好 `category_id`，且不属于 `147/149/150` 三类固定排除品类，则会默认纳入门店日报；只有当业务新增“排除类目”时，才需要修改 ETL / 最小对账 SQL，并按影响日期人工重跑 `ads_store_daily_report` 及其复用商品范围口径的下游表。
- **导入方式**：脚本按首行表头读取 `导入模板` sheet，按 `store_name` 做大小写不敏感匹配，再把月宽表按门店行自己的生效区间展开成 `cfg_store_target_daily` 的日粒度记录；只有 `生效开始日 ~ 生效结束日` 内的日期会写入目标表，用于控制月中新店、预建店或阶段性调整从指定日期才进入日报与负责人范围。`门店考核归属` sheet 则要求业务显式维护 `门店ID`；列名沿用 `门店ID`，但业务填写值应以 RT 门店编码为准。导入脚本优先按 `dim_store.store_code` 关联 `dim_store`，若填写纯数字则兼容 `store_id`，即使门店名称将来改名，只要该门店标识正确仍可继续导入；若 Excel 门店名称与 `dim_store.store_name` 不一致，dry-run 只给出 warning，不阻断写库。启用 `--sync-store-report-attr` 时，会把 `门店类型` 原值直接写入 `report_channel_type` 作为最终业务真值，并同步派生 `report_channel_type_group` 粗分类预览；当前只把行级 `生效开始日` 作为门店属性新版本的起始下界，不额外收窄历史 open-ended 结束日。`SQL/alter_dim_store_report_attr_add_channel_type_group.sql` 已于 2026-04-08 执行到现网，当前表内粗分类由生成列自动承接。若未传 `--target-month` 且模板同时维护多个月份，脚本直接失败并返回当前可选月份，避免跨月误导入。若只提供 `统计主体目标` 或 `门店考核归属` 其中一张 sheet，脚本会直接失败；两张都存在但都无有效数据时，表示清空当月共同考核配置。
- **导入日志**：现网已于 2026-04-03 完成 `log_store_target_import` 建表并写入首条 SUCCESS 日志；新环境首次启用 `--apply` 前仍需先执行 `SQL/create_log_store_target_import.sql`。`--apply` 成功或失败都会把执行摘要写入 `log_store_target_import`。
- **门店属性版本**：启用 `--sync-store-report-attr` 时，脚本默认沿用目标月内现有最新 `effective_start_date`；若目标月无现存版本，则回退到月份首日，也可通过 `--attr-effective-start-date` 显式指定。写入前若发现该生效日存在其他不同起始日的有效配置重叠，脚本直接失败。
- **快照登记**：若只想先沉淀 NAS 权威快照与现网差异，而不立即执行门店属性 apply，可运行 `tools/register_store_attr_snapshot.py`。该工具会先复用 `tools/diff_store_report_attr_snapshot.py` 的只读比对逻辑，再把 `file_md5 / compare_date / diff_counts / status` 记录到 `reports/store_attr_snapshot_registry.json`；当最新 NAS 细分类门店类型尚未正式落到现网时，登记状态为 `pending_apply`，这属于预期，不会阻断快照登记本身。
- **排名**：日销和月销都使用 `RANK()`，同分时再按 `store_id` 保证重复产出顺序稳定。

#### 第5步：最小 DQ 收口

- 输出行数必须等于当日最终经营实体数，否则直接报错。
- 若当前 `report_date` 存在共同考核归属，但缺少对应主体目标，直接报错。
- 若当前负责人历史表已开始维护，但 `report_date` 命中的经营实体存在负责人切片重叠或缺切片，直接报错。
- 如果 `day_target > 0` 但 `day_ach_rate` 为空，直接报错。
- 如果 `month_target > 0` 但 `month_ach_rate` 为空，直接报错。
- `owner_name` 允许为空，因此当前只把 `null_owner_name_count` 作为运行后监控指标，不视为错误。
- 当前脚本对 `cfg_store_target_daily` 行数与有效门店数不一致统一只打告警、不直接拦截；其中“目标配置少于有效门店数”已由业务确认允许，因为未来门店数量可能收缩，允许部分门店暂时无目标但保留日报行。

### 口径说明（⚠️ 当前冻结事实）

| 数据 | 口径 | 说明 |
|------|------|------|
| 日销售额 / 月累计销售额 | 净额 | 退货负值直接冲减，不额外拆毛销字段 |
| 日销量 / 月累计销量 | 净量 | 退货负数直接冲减 |
| 日订单数 / 月累计订单数 | 净单数 | 先按零售单去重，再按过滤后商品范围的单号净额 `>0=1 / =0=0 / <0=-1` 汇总；`ABS(金额) < 0.0001` 视为 0 |
| 小额明细 | 排除 | 当前冻结为 `ABS(ri.tot_amt_actual) >= 1`，绝对金额小于 1 的明细整体排除 |
| 达成率 | 目标=0返回NULL | 目标>0但无销售时返回0，而不是NULL |
| 去年同期 | 同期累计 | 取去年同周期累计值做同比 |
| 月中快闪合并下的同店去年同期 | 截止到合并前一天 | 仅对当前月中才生效的 `快闪` 合并经营体生效，去年同期分母上界截到最早 `快闪` 生效日前一天的去年同日 |

---

## 七点五a、tools/import_store_operation_owner_from_nas.py — 门店经营负责人快照导入

### 一句话

**把 NAS 上业务维护的负责人映射表落到 `cfg_store_operation_owner_snapshot`，兼容显式生效/失效日期，并在 MySQL 内自动维护 `dim_store_operation_owner_assignment` 的 SCD2 历史。**

### 数据流

```
NAS 当前快照 / 显式区间兼容
━━━━━━━━━━━━━━
门店负责人映射表.xlsx                     ─┐
dim_store_report_attr                     ─┼─→ tools/import_store_operation_owner_from_nas.py
cfg_store_assessment_assignment           ─┤    ├─→ cfg_store_operation_owner_snapshot
cfg_store_assessment_subject_target_daily ─┘    ├─→ dim_store_operation_owner_assignment
                                                    └─→ log_store_operation_owner_import
```

### 具体做了什么（分4步）

#### 第0步：文件与依赖检查

- 默认读取 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx`，兼容 `门店负责人映射表 / 门店负责人映射模板` 两个 sheet 名。
- 正式 NAS 文件可同时包含 `填写说明` sheet，用于给业务冻结录入口径；导入时会显式忽略说明 sheet，只读取数据 sheet 与首行表头。
- 首行必须包含 `门店编码`、`门店名称`、`负责人`；`备注`、`生效日期`、`失效日期` 可选。
- 运行前检查 `cfg_store_operation_owner_snapshot`、`dim_store_operation_owner_assignment`、`log_store_operation_owner_import` 以及用于推导经营实体的三张依赖表是否齐全；缺表时直接提示先执行 `SQL/create_store_operation_owner_tables.sql`。来源：[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L27)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L29)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L39)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L115)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L142)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L223)；[SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L1)；[SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L23)；[SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L48)

#### 第1步：推导当前应维护的经营实体

- 先按 `snapshot_date` 读取 `dim_store_report_attr` 当前有效、纳入口径且当日在 `cfg_store_target_daily` 已存在目标行的门店。
- 再读取 `cfg_store_assessment_assignment` 当前有效的共同考核归属，并从 `cfg_store_assessment_subject_target_daily` 取当天主体名称。
- 未命中共同考核时，负责人快照维护 `STORE`；命中共同考核时，负责人快照维护 `SUBJECT`，实体 ID 取挂靠主店。来源：[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L256)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L299)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L320)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L364)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L403)

#### 第2步：校验负责人快照是否和经营实体清单一致

- 若 Excel 缺少当前应维护的经营实体，会记为 `missing_entities`。
- 若 Excel 里出现当前不应维护的实体编码，会记为 `unexpected_entities`。
- 若当前已存在共同考核经营体，则负责人快照推荐只保留经营体行；但在同一目标月的生效切换过渡期内，若被吸收的 RT 成员门店与对应 `SUBJECT` 行并存，或在正式生效日前已提前维护 `SUBJECT` 且成员 `STORE` 仍保留，脚本会把这些实体降级为 warning，不再直接阻断。`RT007 -> SUBJ_SZ_WXTD` 的吸收场景与“提前维护 `SUBJECT`”场景均已由最小单测覆盖。来源：[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L437)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)；[test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L16)

#### 第3步：按快照差异维护 SCD2 历史

- 若 Excel 未填写 `生效日期` / `失效日期`，脚本默认把当前行解释为 `effective_start_date = snapshot_date`、`effective_end_date = 9999-12-31`。
- 若 Excel 未填写 `生效日期` / `失效日期` 且负责人、实体名称、实体 ID 都未变化，该行仍按 `unchanged` 处理，不会因为默认日期值每天都重开一段新历史；只有显式区间变化或负责人实际变化时，才会进入 `changed_rows`。来源：[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L59)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L276)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L683)；[test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L237)
- 若 Excel 显式填写了 `生效日期` / `失效日期`，该区间必须覆盖 `snapshot_date`；否则该行会进入 `invalid_effective_date_rows`，`--apply` 直接失败。
- 先把当前快照与快照日命中的历史切片做 `unchanged / changed / new / exited` 分类；其中 `changed_rows` 的历史切换点改为 `snapshot.effective_start_date`，而不是固定 `snapshot_date`。
- `changed / exited` 会先按切换起点关旧；若旧切片起始日已经大于等于新起始日，则直接删除旧切片，避免生成“起始日晚于结束日”的反向区间；`changed / new` 再按当前快照行的显式起止日期开新。
- 若新快照与紧邻上一版历史切片完全一致，则直接把旧切片重开为当前版本，不新增重复切片；导入摘要会额外输出 `earliest_history_effective_start_date`，供专题调度计算回刷起点。来源：[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L545)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L561)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L802)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L842)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L967)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1104)；[test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L108)

#### 第4步：dry-run / apply 输出

- 默认模式是 dry-run，只输出摘要和预览，不写库。
- `--apply` 时会先覆盖写入 `cfg_store_operation_owner_snapshot`，再维护历史切片，并把成功/失败摘要写入 `log_store_operation_owner_import`。
- 若存在 `missing_entities`、未被过渡规则吸收的 `unexpected_entities`、实体名称不一致或历史重叠，`--apply` 直接失败；若只出现共同考核过渡期的 `tolerated_transition_entities`，则导入状态降为 `WARNING` 并继续写库。来源：[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L775)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L997)；[tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1164)

### 关键边界

- 负责人快照允许 `负责人` 为空，表示当前未分配负责人。
- 业务侧默认仍维护“当前真值”单行映射；若需要补录“今天导入、从前几天起生效”的负责人变更，可在 Excel 中显式填写 `生效日期`、`失效日期` 两列，历史仍由 `dim_store_operation_owner_assignment` 承接。
- NAS 正式文件应同步维护 `填写说明` sheet 和表头批注，把业务录入规则冻结在工作簿内；说明页不参与导入。
- 当前负责人快照链路已接入 `scheduled_store_daily_report.py`；专题调度会在目标导入之后执行负责人导入，并在 `changed/new/exited` 发生时优先使用导入摘要中的 `earliest_history_effective_start_date` 作为负责人链路受影响日期起点，再与目标月月初取较大值生成回刷窗口。若只想保留目标链路，可追加 `--no-run-owner-import`。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L765)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L822)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L833)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L1607)

---

## 七点六、etl_ads_store_daily_subject_report.py — 门店经营日报统计主体层

### 一句话

**在最终经营实体层 `ads_store_daily_report` 之上，补齐统计主体编码、主店锚点与成员门店数，生成兼容口径的 `ads_store_daily_subject_report`。**

### 数据流

```
MySQL内部
━━━━━━━━
ads_store_daily_report                    ─┐
cfg_store_assessment_subject_target_daily ─┼─→ etl_ads_store_daily_subject_report.py
cfg_store_assessment_assignment           ─┘    └─→ ads_store_daily_subject_report
```

### 具体做了什么（分4步）

#### 第0步：依赖检查

- 检查 `ads_store_daily_report`、`cfg_store_assessment_subject_target_daily`、`cfg_store_assessment_assignment`、`ads_store_daily_subject_report` 是否存在。
- `--conn-test` 模式只做到这里，不会写入数据。

#### 第1步：校验共同考核配置是否可用

- 同一 `report_date` 下，同一门店最多只能命中 1 条有效归属配置。
- 若门店被显式归到某个统计主体，而该主体同日缺少目标配置，脚本直接失败。

#### 第2步：基于最终经营实体识别统计主体

- 已配置经营实体：按 `subject_code + subject_name` 与最终经营实体结果匹配，回填完整主体编码、主店锚点与成员门店数。
- 未配置门店：自动回退为独立主体，主体编码形如 `STORE_<store_code>`。
- 日销售额、月累计销售额、日销量、月累计销量、日订单数、月累计订单数全部直接复用 `ads_store_daily_report` 已合并好的结果，不再对物理门店做二次汇总。来源：[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L139)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L154)

#### 第3步：目标优先级与排名

- 主体目标与销售事实直接复用 `ads_store_daily_report` 当前行值。
- 产出 `day_rank`、`mtd_rank`、达成率、同比和时间进度，排序稳定键从 `store_id` 切换为 `subject_code`。

#### 第4步：按日期+版本先删后插

- 删除范围：`ads_store_daily_subject_report WHERE report_date = @report_date AND data_version = @data_version`
- 当前专题调度会固定按“门店层 -> 主体层 -> 销售看板月度战役”顺序重跑，既保证主体层消费到新的最终经营实体结果，也让保留的销售看板 ADS 与同批目标/门店属性同步刷新。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L984)

### 关键边界

- 共同考核只认显式配置，不按商场、城市或 RT 编码自动推断。
- `ads_store_daily_report` 已直接改写为最终经营实体粒度；主体层只是补主体编码兼容，不再承担共同考核事实合并职责。
- 若两张共同考核 sheet 同时存在但都为空，视为“清空当月共同考核配置”，主体层全部回退到一店一主体。

---

## 七点八、etl_ads_daily_sales.py — 销售看板月度战役

### 一句话

**把销售看板里“月内日销售进度 Daily Pace”对应的日节奏底表先做成仓库样板：按 `battle_month + sales_date + 战区 + 经营渠道细分类` 产出 `ads_daily_sales`。**

### 数据流

```
MySQL内部
━━━━━━━━
ods_m_retail / ods_m_retailitem           ─┐
dim_store / dim_store_report_attr         ─┼─→ etl_ads_daily_sales.py
dim_product（固定排除147/149/150）      ─┤    └─→ ads_daily_sales
cfg_store_target_daily                    ─┤
cfg_store_assessment_subject_target_daily ─┤
cfg_store_assessment_assignment           ─┘
```

### 具体做了什么（分4步）

#### 第0步：连接与依赖检查

- 检查脚本内置 SQL 模板关键片段是否完整。
- `--conn-test` 只检查源依赖与目标表结构前提；如果目标表 `ads_daily_sales` 还没建，会输出告警但不直接失败，提醒先执行 `SQL/create_ads_daily_sales.sql`。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L1)；[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)

#### 第1步：冻结当前组织范围并生成战役日历

- 组织范围取 `report_date` 当天同时命中 `dim_store_report_attr` 当前有效记录、`is_include_in_daily_report='Y'`，且在 `cfg_store_target_daily` 中已存在同 `target_version` 当日目标的门店；`ads_daily_sales` 只对这批“目标已生效门店”展开整段战役日历。
- `battle_month` 固定取 `report_date` 所在自然月月初，`sales_date` 只生成从月初到 `report_date` 的自然日序列。
- 当前历史事实统一按 `report_date` 当天的组织属性回看归类，不回溯历史组织属性版本。

#### 第2步：按经营实体日序列生成目标、实际与累计字段

- `day_target_amt` 已统一到门店日报目标规则：共同考核经营体按 `sales_date` 优先取 `cfg_store_assessment_subject_target_daily.day_target`，未命中时才回退经营实体内门店日目标求和。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L118)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L169)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L175)
- `day_actual_amt` 改为在 `ods_m_retail + ods_m_retailitem` 上按门店日报门店范围与商品范围汇总净额，不再复用旧版 `dws_sales_daily.sales_amount - return_amount`。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L186)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L223)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L272)
- `cum_target_amt`、`cum_actual_amt` 与 `last_year_cum_actual_amt` 统一在 `area_name + report_channel_type` 明细切片上按 `sales_date` 升序做窗口累加。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L302)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L311)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L313)
- 首版只落物理字段，不物化 `forecast_month_end_amt`、`forecast_gap_amt`、`required_daily_amt_from_today` 等预测字段。来源：[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)

#### 第3步：只产出明细切片

- 明细层粒度：`report_date + battle_month + sales_date + area_name + report_channel_type`。
- 物理层不再补 `area_name + 全部`、`全国 + report_channel_type` 或 `全国 + 全部` 这类总盘行；消费侧如需总计，统一在 Tableau 或 SQL 查询层聚合。

#### 第4步：最小 DQ 收口

- 输出行数必须等于 `天数 * 明细组合数`。
- 每个 `sales_date` 都必须完整产出一套明细切片。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L585)
- 若当前战役月份内 `cfg_store_target_daily` 出现同店同日重复目标记录，直接失败。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L576)
- 当前仓库已同步补 `SQL/check_ads_daily_sales_min.sql`，用于复核行数、唯一键，以及按全部明细切片聚合后的整段日序列。来源：[SQL/check_ads_daily_sales_min.sql](../SQL/check_ads_daily_sales_min.sql#L1)

### 当前边界

- `ads_daily_sales` 当前代码已接入 `scheduled_store_daily_report.py` 专题调度的受影响日期批量重跑，但仍未接入 `run_etl.py` 主调度。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L456)；[test_scheduled_store_daily_report.py](../test_scheduled_store_daily_report.py#L28)
- 历史 `2026-04-15 / v1` 与 `2026-04 / v2` 的写库与最小对账记录形成于旧版销售主题逻辑；本轮统一到 `ads_store_daily_report` 权威口径后，不能直接视为新逻辑验证结果，后续需按新口径重新复核。来源：[AGENT_HANDOFF_archive.md](AGENT_HANDOFF_archive.md#L460)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L186)
- 未获授权前，Agent 只提供 `SQL/create_ads_daily_sales.sql`、`etl_ads_daily_sales.py` 与 `SQL/check_ads_daily_sales_min.sql`；本轮已在用户授权下完成正式跑数与最小对账，后续历史回跑或其他日期执行仍由用户手工决策。来源：[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)

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
- **窗口滑动**：按天切分窗口；每个窗口先按时间范围清理，再对当前源分块按 `id` 删除旧行后 append，避免同一业务 `id` 因时间窗漂移残留旧副本
- **断点续跑**：通过ods_sync_state表记录窗口进度，中断后可从断点继续
- **空MODIFIEDDATE**：全量模式下先处理MODIFIEDDATE为空的记录
- **批次号**：内部变量 batch_id 写入 etl_batch_id，标记本次执行
- **并发防护**：`run()` 使用 MySQL 命名锁 `hefang_dw:ods_m_retail` 串行化同表同步，并对可重试锁冲突做最多3次重试
- **桥接字段**：同步 `OMS_SOURCECODE` 到 `ods_m_retail.oms_sourcecode`，供达播主订单在 MySQL ODS 内桥接
- **运行兜底**：若历史 `ods_m_retail.oms_sourcecode` 尚未回填完成，可先通过 `tools/sync_dabo_order_retail_bridge.py` 将指定达播样本文件的订单号桥接到 `ads_dabo_order_retail_bridge`
- **历史补齐**：`tools/backfill_ods_m_retail_oms_sourcecode.py` 会先把 Oracle `OMS_SOURCECODE` 装载到 MySQL 暂存表，再按 `id` 分批 apply 到 `ods_m_retail`；若中断，可用 `--apply-only` 从暂存表继续批量应用，避免单条大 UPDATE 长事务
- **唯一键治理**：新建环境执行 `SQL/create_ods_tables.sql` 时已直接声明 `uk_ods_m_retail_id`；现网历史库若仍有重复装载，需要先由用户手工执行 `SQL/alter_ods_m_retail_enforce_unique_id.sql` 再落约束
- **治理提醒**：`ods_m_retail.id` 当前不能直接当作所有现网环境都已落实的 MySQL 主键；若后续直接承接 MCP 或销售联表查询，必须同步评估主键/唯一键可行性与头表过滤索引，而不是只保证能落表。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L46-L64)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L243-L270)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L293-L331)；[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

### 与dws_sales_daily的区别

- ODS保留原始字段，DWS做了聚合汇总
- ODS是独立执行的（run_ods.py），不影响日常ETL链路
- 达播日实收场景优先在 MySQL 内用 `COALESCE(ads_dabo_order_label.canonical_system_order_id, ads_dabo_order_label.system_order_id) = ods_m_retail.oms_sourcecode` 做桥接，再复用 `dws_sales` 同口径按 `billdate` 汇总
- 当 `ods_m_retail.oms_sourcecode` 仍有历史缺口时，日报模板会自动回退到 `ads_dabo_order_retail_bridge`，仍然在 MySQL 内完成达播日实收/退款汇总

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
- **重复装载治理**：两条通道都会在窗口清理后，对当前 chunk 按源 `id` 删除旧行再写入，避免线上/线下记录在回刷窗口移动时留下旧副本
- **并发防护**：`run()` 使用 MySQL 命名锁 `hefang_dw:ods_m_retailitem` 串行化同表同步，并对可重试锁冲突做最多3次重试
- **唯一键治理**：新建环境执行 `SQL/create_ods_tables.sql` 时已直接声明 `uk_ods_m_retailitem_id`；现网历史库若仍有重复装载，需要先由用户手工执行 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql` 再落约束
- **治理提醒**：`ods_m_retailitem` 不能只保留双水位同步索引；`dws_sales` 等下游直接按 `m_retail_id` 联表消费时，必须同步评估连接索引与过滤索引，否则容易退化为明细全表扫描。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L47-L65)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L293-L354)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L385-L423)；[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

### 为什么需要双水位

- 伯俊ERP线下门店收银时，不走标准审计字段（MODIFIEDDATE为空）
- 如果只按MODIFIEDDATE做增量，门店订单会全部漏掉
- 所以必须双轨并行：MODIFIEDDATE（线上）+ SETTIME（线下）

---

## 十一、run_etl.py — 主控调度

### 一句话

**ETL的"总指挥"——按顺序执行9个步骤，统一处理异常、重试、发送企业微信通知。**

### 执行流程

```
run_etl.py
  ├─> [1/8] etl_dim_product.run()     商品维度
  ├─> [2/9] etl_dim_sku.run()         SKU维度
  ├─> [3/9] etl_dim_store.run()       店仓维度
  ├─> [4/9] etl_dim_channel.run()     渠道维度
  ├─> [5/9] run_ods.run()             ODS 同步与质检（主链默认回刷近7天）
  ├─> [6/9] etl_dws_sales.run()       销售数据（主链近7天回带）
  │         ↳ 先按 days_back=7, include_today=True 刷近7天，再检查近30天覆盖度，不足则backfill
  ├─> [7/9] etl_dws_inventory.run()   库存快照
  ├─> [8/9] 达播数据就绪检查           查MySQL看今天有没有达播数据
  └─> [9/9] etl_ads_health.run()      库存健康度计算
            ↳ 如果达播就绪，还会执行 backfill_dabo_fields()
            ↳ 如果 dws_sales 或 dws_inventory 未成功，则本轮跳过 ADS，避免在不完整上游数据上继续计算

- **达播latest_date**：达播就绪检查会记录最新日期（MAX sale_date）用于日志与告警。
- 通用耗时统计：各ETL脚本使用 `start_time`/`end_time` 计算 duration（秒）。
```

### cutover / rollback 开关

- 默认 `legacy`：`ads_inventory_health` 继续读取 `dws_inventory_daily + dws_sales_daily`。
- `shadow_compare`：生产写数仍走旧 DWS，但会额外调用 `validate_inventory_health_shadow_against_persisted()`，对 `dws_inventory_daily_v2 + dws_sales_daily_v2` 生成报告型对账结果。
- `v2`：显式改用 `dws_inventory_daily_v2 + dws_sales_daily_v2` 计算 `ads_inventory_health`。
- `--rollback-to-legacy`：优先回退到 `legacy`；`scheduled_etl.py` 与 `scheduled_total_control.py` 只负责透传同一组参数，不显式传参时不会自动切换。

来源：[cutover_controls.py](../cutover_controls.py#L29)；[cutover_controls.py](../cutover_controls.py#L55)；[run_etl.py](../run_etl.py#L767)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)；[run_etl.py](../run_etl.py#L998)；[run_etl.py](../run_etl.py#L1013)；[scheduled_etl.py](../scheduled_etl.py#L47)；[scheduled_total_control.py](../scheduled_total_control.py#L131)；[scheduled_total_control.py](../scheduled_total_control.py#L453)

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
  ├─> full 后 recent catch-up      仅在 --full 下，对 retail / retailitem 再补一轮固定 as-of 增量
  └─> 质量校验（可选）
       ├─> check_ods_incremental.py     对账（Oracle vs MySQL行数）
       └─> check_ods_retailitem_quality.py  明细双通道拆分校验
```

### 常用命令

```bash
# 默认：增量模式，回刷7天
python run_ods.py

# 强制全量（默认补 1 天 full 后 catch-up）
python run_ods.py --full

# 调整或关闭 full 后补追
python run_ods.py --full --full-catchup-days 2
python run_ods.py --full --full-catchup-days 0

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

### 这个入口新补了什么保护

- 增量模式不变：继续按双水位回刷最近窗口。
- 全量模式新增一层兜底：`ods_m_retail` / `ods_m_retailitem` 全量写完后，会冻结一个固定 `as-of`，再各自补一轮最近窗口的增量 catch-up。
- 后续 ODS 质检会复用这个固定 `as-of`，避免“刚补完又因为 Oracle 继续出新而看起来仍然对不齐”。

来源：[run_ods.py](../run_ods.py#L72-L125)；[etl_ods_m_retail.py](../etl_ods_m_retail.py#L91-L151)；[etl_ods_m_retailitem.py](../etl_ods_m_retailitem.py#L134-L206)

### 与run_etl.py的关系

- **可独立运行，也被主链调用**：`run_ods.py` 仍可作为 ODS 专项入口独立执行；同时 `run_etl.py` 当前已在 `ods_sync` 步骤中调用 `run_ods.run(mode='incremental', backfill_days=7, run_qc=True)`，先同步 ODS 再继续 DWS/ADS 主链。来源：[run_etl.py](../run_etl.py#L631-L652)
- **DWS当前从MySQL ODS消费**：`etl_dws_sales.py` 当前从 `ods_m_retail + ods_m_retailitem` 聚合到 `dws_sales_daily`；`etl_dws_inventory.py` 当前从 `ods_fa_storage` 抽取到 `dws_inventory_daily`。DWD 层尚未实现，不能把当前链路写成 DWD 来源。来源：[etl_dws_sales.py](../etl_dws_sales.py#L39-L65)；[etl_dws_inventory.py](../etl_dws_inventory.py#L39-L59)
- **ADS来源按专题区分**：库存健康 `ads_inventory_health` 仍依赖 DWS 库存与销售汇总，但主链已支持显式 `legacy / shadow_compare / v2` 三种 cutover 模式；门店销售专题部分 ADS 已直接读取 ODS 明细事实，专题调度的 freshness 代理则默认按 `cutover_mode` 在 `dws_sales_daily` 与 `dws_sales_daily_v2` 之间派生，也可用 `--sales-freshness-source` 覆盖。来源：[etl_ads_health.py](../etl_ads_health.py#L523)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)；[cutover_controls.py](../cutover_controls.py#L55)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L474)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2076)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2575)
- **M3 raw / DWD 仍为旁路验证链路**：架构完善子项目中 `ods_m_retail_raw`、`ods_m_retailitem_raw`、`ods_fa_storage_raw`、`dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` 已由用户人工建表，并由 Copilot 按授权完成近 1 天小窗口、20260428-20260430 销售完整业务日期 raw / DWD、20260507 库存 full raw / DWD 快照验证；脚本仍默认 dry-run，只有显式 `--execute` 才写库，且未接入 `run_etl.py` / 总控，当前 DWS / ADS 不消费。来源：[ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md](ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md)；[../etl_ods_m_retail_raw.py](../etl_ods_m_retail_raw.py#L1)；[../etl_ods_m_retailitem_raw.py](../etl_ods_m_retailitem_raw.py#L1)；[../etl_ods_fa_storage_raw.py](../etl_ods_fa_storage_raw.py#L1)；[../reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json](../reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json)
- **M4 / M6 DWS v2 当前状态**：`dws_sales_daily_v2`、`dws_inventory_daily_v2` 已由用户人工建表并完成结构核验；`etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py` 和 `dws_v2_write_utils.py` 仍保留 dry-run / conn-test / S3 手工写入能力。当前已在用户明确授权下完成一次 S3 实跑验收：销售脚本按 `20260428-20260430` 写入 3417 行且 DWD-v2 mismatch 为 0；库存脚本按 `20260507` 写入 75104 行且 DWD-v2 mismatch 为 0。当前 `run_etl.py`、`scheduled_etl.py` 与 `scheduled_total_control.py` 已支持显式 `--cutover-mode legacy|shadow_compare|v2` 与 `--rollback-to-legacy`：默认不传仍按旧 DWS 运行，`shadow_compare` 只做报告型对账，`v2` 才让 `ads_inventory_health` 改读 `_v2`；总控 V2 模式会先阻断运行 `DWS v2 读源预刷新`，避免主链 ADS 在 `_v2` 当日源为空时写出 0 行。销售脚本默认 `timeout_profile='etl'`；库存脚本默认 `timeout_profile='long_running'`。来源：[ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md](ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md)；[../dws_v2_write_utils.py](../dws_v2_write_utils.py#L1)；[../etl_dws_sales_v2.py](../etl_dws_sales_v2.py#L1)；[../etl_dws_inventory_v2.py](../etl_dws_inventory_v2.py#L1)；[../run_etl.py](../run_etl.py#L998)；[../scheduled_total_control.py](../scheduled_total_control.py#L503)；[../scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L1165)；[../reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json](../reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json)；[../reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json](../reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json)

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

- 当前 `scheduled_etl.py` 也支持 `--cutover-mode legacy|shadow_compare|v2` 与 `--rollback-to-legacy`，只负责把同一组参数透传给 `run_etl.run_main()`；若主链执行成功，仍会继续跑 `test_etl_automation.py`。来源：[scheduled_etl.py](../scheduled_etl.py#L47)；[scheduled_etl.py](../scheduled_etl.py#L97)

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

### Q: DWS层现在是否直接读Oracle，不经过ODS？

A: 不是。这个问答只反映早期设计阶段，当前代码已经变更：`run_etl.py` 会先执行 ODS 增量同步；`dws_sales_daily` 与 `dws_inventory_daily` 当前都从 MySQL ODS 层取数。DWD 层仍未实现，后续若建设 DWD，需要另行设计、验证并经用户确认后再切换。来源：[run_etl.py](../run_etl.py#L631-L652)；[etl_dws_sales.py](../etl_dws_sales.py#L39-L65)；[etl_dws_inventory.py](../etl_dws_inventory.py#L39-L59)

### Q: 为什么销售数据不过滤品类和渠道？

A: "全量抽取，按需过滤"原则。DWS层保留全量数据，业务口径（主销品类、电商+云仓渠道）在ADS层统一控制。好处是DWS层不因业务口径变化而需要重跑。

### Q: 库存快照为什么保留QTY=0的记录？

A: FA_STORAGE中QTY=0的记录仍表示该商品在该仓库中被管理过。保留这些记录有助于分析商品在各仓库的分布情况。

### Q: 退货算法里为什么要扣减退货量？

A: 退货的货物最终会返回仓库。假设某SKU这个月退了50件，这50件会回到可售库存中，所以建议补货时要减掉这部分。

### Q: 日报模板在每月1日时，月累计为什么不能直接写“本月1日到昨天”？

A: 因为每月1日的“昨天”已经落到上一个自然月，如果起始日仍取本月1日，就会出现“起始日大于结束日”的反向区间，月累计和同期累计都会被汇总成 0。当前日报模板已改为：非月初取本月1日到昨天；每月1日自动回退为上一个完整自然月，去年同期窗口也随之整体回退 12 个月。

### Q: 达播数据和普通销售数据怎么关联？

A: 现在有两条路径。库存健康链路仍通过 SKU 条码匹配：达播 CSV 中的“商家编码” = dim_sku 的 “sku_barcode” = `ads_dabo_daily_sales.product_alias_code`。而统一 Excel 内部主线则优先通过订单号匹配：`COALESCE(ads_dabo_order_label.canonical_system_order_id, ads_dabo_order_label.system_order_id) = ods_m_retail.oms_sourcecode`，先给 ODS 订单打上“是否达播 / 达播渠道”标签；其中原始 `system_order_id` 只保留追溯，异常组合单会补 canonical 值用于优先桥接，再在 SQL 中按标签筛选计算日实收、退款和净额。

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
| v2.73 | 2026-07-13 | 为 `etl_dim_store.py` 补充 `OPENDATE` 安全转换、缺列前置保护与回滚兼容说明；同步门店日报同店资格切换为开业日期判断 |
| v2.72 | 2026-06-18 | 将负责人共同考核说明更新为“推荐维护 SUBJECT，但同月过渡允许 STORE + SUBJECT 并存且仅告警”，并补记 2026-06-18/19 生效切换验证结论 |
| v2.71 | 2026-06-18 | 纠正 `门店考核归属` 的 `门店ID` 字段语义：业务填写 RT 门店编码，脚本优先按 `store_code` 命中并兼容纯数字 `store_id` |
| v2.70 | 2026-06-18 | 将 `门店考核归属` 说明更新为必填 `门店ID`，并明确共同考核导入优先按 `store_id` 命中、门店名称不一致仅告警 |
| v2.69 | 2026-06-08 | 将门店日报与销售看板月度战役的商品范围切换为固定排除 `147/149/150`，其余 category_id 默认纳入，并移除“新增品类先补配置”的旧边界 |
| v2.68 | 2026-05-26 | 按用户确认补记免税外部月累计销售只参与月总达成，Tableau 明细总计月客单价 / 月折扣率按非免税口径重算 |
| v2.68 | 2026-06-06 | 退役 3 张销售专题 ADS，并将专题调度说明收口到门店层、主体层、销售看板月度战役 |
| v2.67 | 2026-05-20 | 补记 `ads_store_daily_report.mtd_list_amt` 作为月累计吊牌金额物理字段，并说明其用于月累计折扣率和 Tableau 明细总计分母 |
| v2.66 | 2026-05-13 | 补记 M6 总控 V2 前置刷新顺序：先刷新 DWS v2 读源，再让主链计算 ads_inventory_health，避免新日期 `_v2` 源为空导致 ADS 写 0 行 |
| v2.65 | 2026-05-12 | 补记主链 / 定时包装 / 总控已新增 cutover / rollback 开关，且门店专题 freshness 来源可按 cutover_mode 派生 legacy / v2 |
| v2.64 | 2026-05-12 | 补记未显式填写日期的当前真值行会保持 unchanged，不会因默认 snapshot_date 每天重切负责人历史 |
| v2.65 | 2026-06-01 | 修复 `ads_sku_daily` 明细底表窗口仅取最近 30 天导致 31 天月末漏掉月初 SKU 组合的问题，并补记底表窗口改为“月初/滚动30天起点取更早者” |
| v2.63 | 2026-05-12 | 补记 DWS v2 最新无参数 shadow 已给出 ADS gate READY，下一步仅进入总控非阻断观察，并冻结 ADS 既有字段名不可改的兼容边界 |
| v2.62 | 2026-05-12 | 将负责人导入说明更新为兼容 Excel 显式生效/失效日期，并补记专题调度按 earliest_history_effective_start_date 起算回刷窗口 |
| v2.61 | 2026-05-08 | 将 `ads_sales_org_monthly` 的门店范围说明收口到 `report_date` 当天已生效目标门店，并补记 RT116 在 5 月上旬仅造成范围漂移、未造成金额漂移 |
| v2.60 | 2026-05-08 | 将 `ads_daily_sales`、`ads_sales_org_daily` 的有效门店范围收口到 `report_date` 当天已生效目标门店，消除与门店日报专题范围不一致的侧向告警 |
| v2.59 | 2026-05-07 | 将门店日报与负责人快照说明更新为按 `cfg_store_target_daily` 当日生效范围收口，并补记目标模板支持 `生效开始日/生效结束日` |
| v2.58 | 2026-05-07 | 补记 DWS v2 已完成一次 S3 实跑验收：销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0；仍未接生产主链 |
| v2.57 | 2026-05-07 | 同步 M3 raw / DWD 旁路表已完成销售完整业务日期和库存 full raw 初始化验证，但仍未接生产主链 |
| v2.56 | 2026-04-29 | 补记 M3 raw 旁路 ODS / DWD 草案对象仅为未执行旁路设计，不属于当前生产主链 |
| v2.55 | 2026-04-29 | 将 ads_sales_org_monthly 的 month_order_cnt 说明改为承接 ads_store_daily_report.day_order_cnt，并将 ads_sku_daily 的 mtd_order_cnt 说明改为按 SKU 过滤后净额与近零容差判单 |
| v2.54 | 2026-04-29 | 校准 ODS 与 DWS/ADS 当前来源关系：DWS 已从 MySQL ODS 消费，DWD 仍未实现，ADS 来源按专题区分 |
| v2.53 | 2026-04-28 | 将 ads_sales_org_monthly 说明改为共同考核主体目标优先 + ODS净额事实，并补记同月补跑缓存与当前月对勾 ads_sales_org_daily MTD |
| v2.52 | 2026-04-27 | 将 ads_sku_daily 说明与其最小对账 SQL 统一到门店日报权威口径与 ODS 净单逻辑 |
| v2.51 | 2026-04-27 | 将 ads_sales_org_daily 与 ads_daily_sales 的说明统一到 ads_store_daily_report 权威口径，并修复 etl_dim_product 章节误插内容 |
| v2.50 | 2026-04-27 | 将门店销售专题说明更新为六层 ADS，并补记 ads_sales_org_monthly 接入专题链与 DWS freshness 触发规则 |
| v2.49 | 2026-04-23 | 同步销售主题 ADS 改为 report_channel_type 明细口径，并移除 全国/全部 物理汇总行执行说明 |
| v2.48 | 2026-04-23 | 补记 run_etl.py 已将 dws_sales 主链窗口固定为近7天回带，并同步 ODS→DWS 承接逻辑 |
| v2.47 | 2026-04-23 | 补记 ads_sku_daily 连带贡献精度要求提升到 DECIMAL(14,2)，并同步 2026-04-22/v2 五层调度结果 |
| v2.46 | 2026-04-22 | 补记 ads_store_daily_report 负责人字段、负责人切片校验与负责人快照接入专题调度 |
| v2.45 | 2026-04-22 | 补记负责人映射正式文件内置填写说明页与表头批注，明确说明页不参与导入 |
| v2.44 | 2026-04-21 | 新增门店经营负责人快照导入逻辑说明，并明确当前真值快照与 SCD2 自动维护边界 |
| v2.43 | 2026-04-17 | 补记 ads_sku_daily 已完成专题调度第五层显式重跑验证，并更新五层写库状态 |
| v2.42 | 2026-04-17 | 将 ads_sku_daily 接入专题调度第五层，并补记其已正式写库、当前仅完成代码接链与单元测试验证 |
| v2.41 | 2026-04-17 | 将 ads_sku_daily 更新为含 attach_contribution 的二期样板，并补记 ODS 订单级口径与旧结构告警 |
| v2.40 | 2026-04-17 | 将 ads_sku_daily 更新为已补 sales_mix_pct、rank_no、trend_tag 的二期样板，并补记 alter 脚本与派生字段 DQ |
| v2.39 | 2026-04-16 | 新增 ads_sales_org_monthly 与 ads_sku_daily 的样板 ETL 说明，并注明当前仅完成 conn-test 验证 |
| v2.38 | 2026-04-16 | 将 ads_sales_org_daily 接入专题调度第四层，并补记四层实跑验证结果 |
| v2.37 | 2026-04-16 | 同步专题调度自动跳过与显式 rerun 写库验证状态，并补记 ads_sales_org_daily 的 v2 复验与接链建议 |
| v2.36 | 2026-04-16 | 将 ads_daily_sales 纳入专题调度三层批量重跑，并注明当前仅完成单元测试验证 |
| v2.35 | 2026-04-16 | 更新 ads_daily_sales 为已完成 2026-04-15/v1 首轮样本与最小对账验证状态 |
| v2.34 | 2026-04-16 | 更新 ads_daily_sales 为已建表空表，待首轮样本与最小对账 |
| v2.34 | 2026-04-30 | 校正门店日报订单数说明为“按过滤后商品范围净额判单”，并补记新增品类需补配置后人工重跑历史结果 |
| v2.33 | 2026-04-15 | 新增 ads_daily_sales 仓库样板 ETL，并回写 ads_sales_org_daily 已完成单日验证状态 |
| v2.32 | 2026-04-15 | 新增 ads_sales_org_daily 仓库样板 ETL、净额/YTD 目标首版规则与最小对账说明 |
| v2.31 | 2026-04-15 | 将门店日报目标导入 NAS 根目录从 月度日目标配置表 更新为 目标配置表 |
| v2.30 | 2026-04-10 | 将门店日报改为最终经营实体粒度，并同步主体层适配逻辑说明 |
| v2.29 | 2026-04-10 | 更新门店日报目标 NAS 命名约定为 YYYYMM考核数据配置表.xlsx，并注明导入脚本兼容历史旧文件名 |
| v2.28 | 2026-04-10 | 新增门店日报统计主体层 ETL，并补充共同考核多 sheet 导入与双层重跑说明 |
| v2.27 | 2026-04-10 | 更新门店日报商品范围为 15 类，补纳 148=辅销品、394=配饰，并记录 2026-04-01~2026-04-07 历史重跑 |
| v2.26 | 2026-04-09 | 将 ads_inventory_health 的达播来源更新为标签主线优先、legacy 回退兜底，并同步主调度行为 |
| v2.25 | 2026-04-09 | 更新 dabo_ready 为达播标签主线优先检查，并明确 ads_health 仅在 legacy CSV 当日可用时回填 |
| v2.24 | 2026-04-09 | 为 ads_dabo_order_label 增加 canonical_system_order_id 归一桥接说明，并将达播日实收桥接更新为优先使用 canonical 值 |
| v2.23 | 2026-04-08 | 明确门店日报商品范围已补纳 146=配件，且不影响主销品12类口径 |
| v2.22 | 2026-04-08 | 新增门店属性快照登记机制说明，并明确 pending_apply 不阻断登记 |
| v2.21 | 2026-04-08 | 更新门店日报渠道粗分类生成列为现网已执行状态 |
| v2.20 | 2026-04-08 | 调整门店日报渠道模型为细分类真值，并补充 report_channel_type_group 生成列说明 |
| v2.6 | 2026-04-08 | 新增 ads_dabo_order_label 订单标签主线说明，并将达播日实收桥接优先切换为 system_order_id |
| v2.19 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充目录选档逻辑 |
| v2.18 | 2026-04-08 | 补充门店日报目标导入在多月份文件下需显式传入 --target-month 的逻辑 |
| v2.17 | 2026-04-07 | 补充 run_ods --full 默认执行固定 as-of recent catch-up 的入口逻辑 |
| v2.16 | 2026-04-03 | 补充门店日报目标导入支持门店类型同步与默认生效日策略 |
| v2.15 | 2026-04-03 | 更新门店日报目标导入日志说明为现网已建表并完成首轮 SUCCESS 验证 |
| v2.14 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本与日志表说明 |
| v2.13 | 2026-04-03 | 冻结门店日报目标 NAS 目录与固定文件命名约定 |
| v2.12 | 2026-04-03 | 明确门店日报月目标固定、日目标动态调整且月内日目标合计可不等于月目标 |
| v2.11 | 2026-04-03 | 明确门店日报目标配置采用 NAS 投递目录加 Python 定时扫描导入的正式路径 |
| v2.10 | 2026-04-03 | 补充门店日报目标配置少于有效门店数时只告警的业务确认规则 |
| v2.8 | 2026-04-02 | 补充 ODS 重复装载治理、命名锁与现网唯一键治理说明 |
| v2.9 | 2026-04-03 | 新增门店经营日报独立 ETL 的运行逻辑与最小 DQ 说明 |
| v2.7 | 2026-04-02 | 补充 ods_m_retail 与 ods_m_retailitem 的主键治理与查询路径索引提醒 |
| v2.6 | 2026-04-01 | 补充日报模板在每月1日回退到上一个完整自然月的窗口说明 |
| v2.3 | 2026-03-23 | 修正 dim_channel 现网核对结论，明确 WING_CODE 不应假设为 DS 编码 |
| v2.2 | 2026-03-23 | 补充 dws_sales 命名锁重试与 `tot_amt_actual=0` 行级数量兜底口径 |
| v2.1 | 2026-03-23 | 补充 9 步主链、库存/ADS 命名锁重试与 ADS 上游失败跳过逻辑 |
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
| v2.4 | 2026-03-31 | 补充 ods_m_retail 的 oms_sourcecode 同步与达播 MySQL 内桥接说明 |
| v2.5 | 2026-03-31 | 补充 oms_sourcecode 历史回填已改为暂存表加分批 apply 的执行方式 |
