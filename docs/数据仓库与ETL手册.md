# 何方珠宝 - 数据仓库与ETL手册

> 数仓建设方案 | ETL同步逻辑 | 调度说明

---

## 📋 目录

1. [数仓架构设计](#一数仓架构设计)
2. [分层与表结构](#二分层与表结构)
3. [ETL同步逻辑](#三etl同步逻辑)
4. [调度与监控](#四调度与监控)

---

## 一、数仓架构设计

### 1.1 整体架构

```
┌──────────────────────────────────────────────────────────┐
│                     数据仓库架构                         │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Oracle生产库（伯俊ERP）                                  │
│       ↓                                                  │
│  Python ETL（每日凌晨执行）                               │
│       ↓                                                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │              MySQL本地数据仓库                     │  │
│  │                                                    │  │
│  │  ┌────┐  ┌────┐  ┌────┐  ┌────┐  ┌────┐          │  │
│  │  │ODS │→ │DIM │→ │DWD │→ │DWS │→ │ADS │          │  │
│  │  │原始│  │维度│  │明细│  │汇总│  │应用│          │  │
│  │  └────┘  └────┘  └────┘  └────┘  └────┘          │  │
│  └────────────────────────────────────────────────────┘  │
│       ↓                                                  │
│  Tableau / FineBI / Excel                                │
│       ↓                                                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │  电商日报 | 库存健康度 | 月度报告 | 进销存看板     │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

### 1.2 分层说明

| 层级 | 名称 | 说明 | 示例表 |
|------|------|------|--------|
| ODS | 原始数据层 | 1:1复制源表（保留） | ods_m_retail, ods_fa_storage |
| DIM | 维度层 | 维度表 | dim_product, dim_product_attr, dim_store, dim_channel, dim_sku, dim_date |
| DWD | 明细事实层 | 清洗后的明细（保留） | 暂无（DWD层未在代码实现） |
| DWS | 汇总事实层 | 按主题汇总 | dws_sales_daily, dws_inventory_daily |
| ADS | 应用层 | 面向应用 | ads_inventory_health, ads_dabo_daily_sales（已实现）；ads_daily_report/ads_sales_summary（规划，未在代码实现） |

---

### 1.3 星型模型设计

```
                         ┌───────────────┐
                         │  dim_date     │
                         │  日期维度      │
                         └───────┬───────┘
                                 │
┌───────────────┐       ┌───────┴───────┐       ┌───────────────┐
│  dim_store    │       │ fact_sales    │       │ dim_product   │
│  店仓维度      │◄──────│ 销售事实表    │──────►│ 商品维度       │
└───────────────┘       │               │       └───────────────┘
                        │ • date_id     │
                        │ • store_id    │
                        │ • product_id  │
                        │ • 销量        │
                        │ • 销售额      │
                        │ • 退货量      │
                        │ • 退货额      │
                        └───────────────┘
```

> 注：SKU维度来自 dim_sku（sku_id），用于SKU粒度分析。

---

## 二、分层与表结构

### 2.1 维度表设计

**dim_product（商品维度）**
```sql
CREATE TABLE dim_product (
    product_id      BIGINT PRIMARY KEY,
    product_code    VARCHAR(80),
    product_name    VARCHAR(200),
    category_id     INT,
    category_name   VARCHAR(50),
    property_id     INT,
    property_name   VARCHAR(50),
    series_id       INT,
    series_name     VARCHAR(100),
    brand_id        INT,
    brand_name      VARCHAR(50),
    year_id         INT,
    year_name       VARCHAR(20),
    price_list      DECIMAL(12,2),
    price_cost      DECIMAL(12,2),
    material        TEXT,
    is_main_product CHAR(1),
    is_active       CHAR(1),
    created_at      DATETIME,
    updated_at      DATETIME
);
```

> 注：`material` 来自 Oracle `M_PRODUCT.FABELEMENT`（材质成分）。`year_id`/`year_name` 对应 Oracle `M_DIM2_ID` 维度，当前ETL未填充。

**dim_product_attr（商品属性表）**
```sql
CREATE TABLE dim_product_attr (
    product_id      BIGINT,
    color           TEXT,
    size            TEXT
);
```

**dim_store（店仓维度）**
```sql
CREATE TABLE dim_store (
    store_id        BIGINT PRIMARY KEY,
    store_code      VARCHAR(40),
    store_name      VARCHAR(255),
    area_id         INT,
    area_name       VARCHAR(100),
    is_warehouse    TINYINT,
    is_store        TINYINT,
    is_cloud_store  CHAR(1),
    is_center       CHAR(1),
    store_type      VARCHAR(20),
    is_active       CHAR(1),
    created_at      DATETIME,
    updated_at      DATETIME
);
```

**dim_channel（渠道维度）**
```sql
CREATE TABLE dim_channel (
    channel_id      INT PRIMARY KEY,
    channel_name    VARCHAR(50),
    channel_code    VARCHAR(20),
    WING_CODE       VARCHAR(40),
    is_main         TINYINT,
    platform_type   VARCHAR(20),
    is_active       CHAR(1),
    created_at      DATETIME
);
```

**dim_sku（SKU维度）**
```sql
CREATE TABLE dim_sku (
    sku_id            BIGINT PRIMARY KEY,
    product_id        BIGINT,
    sku_barcode       VARCHAR(80),
    sku_color         VARCHAR(60),
    sku_size          VARCHAR(60),
    is_active         CHAR(1),
    created_at        DATETIME,
    updated_at        DATETIME
);
```

**dim_date（日期维度）**
说明：dim_date 为静态维度表，当前未在代码实现自动生成。
```sql
CREATE TABLE dim_date (
    date_id         INT PRIMARY KEY,
    date_value      DATE,
    date_year       INT,
    date_month      INT,
    date_day        INT,
    date_quarter    INT,
    week_of_year    INT,
    day_of_week     INT,
    day_name_cn     VARCHAR(10),
    month_name_cn   VARCHAR(10),
    is_weekend      TINYINT,
    is_holiday      TINYINT,
    holiday_name    VARCHAR(50),
    year_month      VARCHAR(7),
    created_at      DATETIME
);
```

---

### 2.2 事实表设计

**dws_sales_daily（日销售汇总）**
说明：表结构与索引为SQL/人工建表，当前未在代码实现自动建表/建索引；`net_qty`/`net_amount` 也未在代码实现写入。
```sql
CREATE TABLE dws_sales_daily (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_id         INT,
    store_id        BIGINT,
    product_id      BIGINT,
    m_productalias_id BIGINT,
    sales_qty       INT,
    sales_amount    DECIMAL(14,2),
    sales_amount_list DECIMAL(14,2),
    return_qty      INT,
    return_amount   DECIMAL(14,2),
    net_qty         INT,          -- ⚠️ 当前ETL未填充，MySQL默认0
    net_amount      DECIMAL(14,2),-- ⚠️ 当前ETL未填充，MySQL默认0
    order_count     INT,          -- 仅统计正单(TOT_AMT_ACTUAL>0)的不重复零售单数
    store_code      VARCHAR(32),
    is_cloud_store  CHAR(1),
    created_at      DATETIME,
    updated_at      DATETIME,
    etl_time        DATETIME,
    
    INDEX idx_date (date_id),
    INDEX idx_store (store_id),
    INDEX idx_product (product_id)
);
```

**dws_inventory_daily（日库存快照）**
```sql
CREATE TABLE dws_inventory_daily (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_id         INT,
    store_id        BIGINT,
    store_code      VARCHAR(32),
    is_cloud_store  CHAR(1),
    product_id      BIGINT,
    m_productalias_id BIGINT,
    qty             INT,
    qty_valid       INT,
    qty_occupy      INT,
    qtypurchaserem  BIGINT,
    created_at      DATETIME,
    etl_time        DATETIME,
    
    INDEX idx_date (date_id),
    INDEX idx_store (store_id),
    INDEX idx_product (product_id)
);
```

---

### 2.3 应用表设计

**ads_inventory_health（库存健康度）**

此表在MySQL内基于dws层计算，不从Oracle直接抽取：

```sql
-- 数据来源
库存数据 ← dws_inventory_daily (当天)
销售数据 ← dws_sales_daily (近30天)
达播销量 ← ads_dabo_daily_sales (近30天/近7天)
商品信息 ← dim_product
SKU信息 ← dim_sku
仓库信息 ← dim_store

- 周转天数 = 库存 / (30天销量 / 30)
- 库存状态（滞销/停售/紧急缺货/需补货/正常/库存过高）
- 状态优先级（紧急缺货=1…停售=6）
- SABC分级（按销售额累计占比）
- 销售排名/占比/累计占比（sales_rank/sales_ratio/cumulative_ratio）
- 建议补货 = (90-周转天数)*日均销量 - 退货 - 采购欠数
- 自然销量 = 电商+云仓销量 - 达播销量（用于自然销售加速度与趋势判断）
```

---

## 三、ETL同步逻辑

### 3.1 各表同步策略

| 目标表 | 源表 | 策略 | 说明 |
|--------|------|------|------|
| ods_fa_storage | FA_STORAGE | 全量覆盖（可选） | ODS原始层，独立执行，不影响现有DWS/ADS |
| ods_m_retail | M_RETAIL | 增量为主（默认回刷7天，可全量） | ODS原始层，独立执行，不影响现有DWS/ADS |
| ods_m_retailitem | M_RETAILITEM | 双水位增量（MODIFIEDDATE + SETTIME，可全量） | ODS原始层，独立执行，不影响现有DWS/ADS |
| dim_product | M_PRODUCT + M_DIM + M_ATTRIBUTESETINSTANCE | 全量覆盖 | 商品信息可能改，含材质字段 |
| dim_product_attr | M_ATTRIBUTESETINSTANCE（通过M_PRODUCT_ALIAS关联） | 全量覆盖(replace) | 每个商品取第一个SKU的颜色/尺寸 |
| dim_sku | M_PRODUCT_ALIAS + M_ATTRIBUTESETINSTANCE | 全量覆盖 | SKU信息可能改 |
| dim_store | C_STORE + C_AREA | 全量覆盖 | 门店可能新增 |
| dim_channel | O2O_RETAIL_CHANNEL | 全量覆盖 | 电商渠道与店仓映射 |
| dws_sales_daily | M_RETAIL + M_RETAILITEM + C_STORE + M_PRODUCT | 增量（按日期） | 智能判断：凌晨查昨天，白天查今天（全渠道、全品类；业务筛选下沉ADS） |
| dws_inventory_daily | FA_STORAGE + C_STORE + M_PRODUCT | 全量快照 | 每日记录当天库存（总仓+云仓，不做主销品类过滤） |
| ads_inventory_health | MySQL内计算 | 重新计算 | 基于dws层 |
| ads_dabo_daily_sales | CSV文件 | 文件驱动 | 监听例行/紧急目录 |
| log_dabo_import | ETL日志 | 追加写入 | 每次导入记录 |

**ODS增量同步规则（阶段二）**
1. 增量条件：主通道按 `MODIFIEDDATE` 回刷窗口，默认回刷 7 天。
2. 双水位策略：
    - 线上通道（`MODIFIEDDATE IS NOT NULL`）按 `MODIFIEDDATE` 增量。
    - 线下通道（`MODIFIEDDATE IS NULL` 且 `SETTIME IS NOT NULL`）按 `SETTIME` 增量。
3. 窗口边界：使用半开区间 `>= start_ts` 且 `< end_ts`，避免重叠/漏数。
4. 稳定排序：
    - `MODIFIEDDATE` 通道按 `MODIFIEDDATE, ID` 排序。
    - `SETTIME` 通道按 `SETTIME, ID` 排序。
5. 断点续跑：ODS 使用 `ods_sync_state` 记录窗口起止与状态，支持双水位。
    - `ods_m_retailitem` 记录 `MODIFIEDDATE` 水位。
    - `ods_m_retailitem_settime` 记录 `SETTIME` 水位。
6. 性能前提：Oracle 侧需有 `M_RETAIL(MODIFIEDDATE, ID)`、
    `M_RETAILITEM(MODIFIEDDATE, ID)`、`M_RETAILITEM(SETTIME, ID)` 索引。 (DBA无法判断加索引的风险，所以暂时没有实施 建立该索引)

> 运行入口 `run_ods.py` 默认走增量模式并回刷7天，可通过 `--full` 强制全量；回刷天数可用 `--backfill-days` 调整。

---

### 3.2 同步时序

**ODS（可选，独立执行）**
```
任意时间  run_ods.py（默认增量，--full 可全量）
```

```
03:00  ETL开始
03:05  同步dim_product（约3分钟）
03:08  同步dim_sku（约1分钟）
03:10  同步dim_store（约1分钟）
03:11  同步dim_channel（约1分钟）
03:13  同步dws_sales_daily（智能判断）（约5分钟）
03:18  同步dws_inventory_daily（约10分钟）
03:28  达播数据就绪检查/回填（当日）
03:30  计算ads_inventory_health（约5分钟）
03:35  ETL结束
06:00  Tableau数据源刷新
实时  达播CSV监听（例行/紧急目录）
```

---

### 3.3 增量同步逻辑

**销售数据增量：**
```python
# 智能判断：凌晨查昨天，白天查今天（可通过 days_back 回溯）
current_time = datetime.now()
if include_today:
    end_dt = current_time - timedelta(days=1) if current_time.hour < 6 else current_time
else:
    end_dt = current_time - timedelta(days=1)

start_dt = end_dt - timedelta(days=days_back-1)
start_date = int(start_dt.strftime('%Y%m%d'))
end_date = int(end_dt.strftime('%Y%m%d'))

# 先删后插
mysql.execute(f"DELETE FROM dws_sales_daily WHERE date_id >= {start_date} AND date_id <= {end_date}")
df = oracle.query(sales_sql.format(start_date=start_date, end_date=end_date))
# 关键过滤：
# - 只取SKU（M_PRODUCTALIAS_ID IS NOT NULL）
# - 业务筛选在ADS层完成（如电商/云仓、主销品类）
mysql.to_sql(df, 'dws_sales_daily', if_exists='append')
```

**补数逻辑：**
```python
# 如果需要补历史数据
def backfill(start_date, end_date):
    mysql.execute(f"DELETE FROM dws_sales_daily WHERE date_id >= {start_date} AND date_id <= {end_date}")
    df = oracle.query(sales_sql.format(start=start_date, end=end_date))
    mysql.to_sql(df, 'dws_sales_daily', if_exists='append')
```

---

### 3.4 库存快照逻辑

```python
# 每天记录当天库存状态
today = datetime.now().strftime('%Y%m%d')

# 删除今天旧数据（如果重跑）
mysql.execute(f"DELETE FROM dws_inventory_daily WHERE date_id = {today}")

# 抽取当前库存
df = oracle.query("""
    SELECT 
        fs.C_STORE_ID AS store_id,
        s.CODE AS store_code,
        NVL(s.IS_ALLO2OSTORAGE, 'N') AS is_cloud_store,
        fs.M_PRODUCT_ID AS product_id,
        fs.M_PRODUCTALIAS_ID AS m_productalias_id,
        fs.QTY AS qty,
        fs.QTY AS qty_valid,
        NVL(fs.QTYPURCHASEREM, 0) AS qtypurchaserem
    FROM FA_STORAGE fs
        LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
        LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
        WHERE fs.ISACTIVE = 'Y'
            AND fs.M_PRODUCTALIAS_ID IS NOT NULL
            AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
        -- 库存快照不做主销品过滤，口径在ADS层统一控制
""")
df['date_id'] = int(today)
df['qty_occupy'] = 0

# 写入
mysql.to_sql(df, 'dws_inventory_daily', if_exists='append')
```

---

### 3.5 达播CSV同步逻辑

> 说明：达播数据ETL已独立为外部项目，本仓库仅保留流程概览与口径说明。

```
触发方式：监听例行/紧急目录
处理流程：读取CSV → 字段校验 → 清洗 → 按发货日期+SKU聚合 → SKU匹配校验
写入策略：删除近60天数据后插入
异常处理：命名不符/校验失败/重复文件 → 隔离或跳过
```

**为什么要每日快照**：
- 库存是状态数据，今天的库存明天就变了
- 快照可以分析库存趋势
- 可以追溯历史某天的库存

---

### 3.6 应用表计算逻辑

ads_inventory_health不从Oracle抽，而是在MySQL内计算：

```sql
-- 数据来源
库存数据 ← dws_inventory_daily (当天)
销售数据 ← dws_sales_daily (近30天)
达播销量 ← ads_dabo_daily_sales (近30天/近7天)
商品信息 ← dim_product
SKU信息 ← dim_sku
仓库信息 ← dim_store

-- 计算步骤
1. 汇总库存（总仓+云仓）
2. 汇总销售（近30天/近7天）
3. 计算周转天数
4. 判断库存状态
5. 计算ABC分级
6. 生成建议补货
7. 计算自然销量与自然加速度（电商+云仓销量 - 达播销量）
8. 达播数据就绪时回填当日达播/自然字段
```

---

## 四、调度与监控

### 4.1 调度方式

**方式一：Windows任务计划程序**
```batch
@echo off
cd /d C:\Users\tianhao\PycharmProjects\hefang_dw
call run_scheduled_etl.bat
```

**方式二：run_scheduled_etl.bat**
- 时间：每天凌晨3:00
- 日志：logs/etl_YYYYMMDD.log
- 入口链路：`run_scheduled_etl.bat` -> `scheduled_etl.py` -> `run_etl.py`
- 说明：`run_etl.py` 为统一执行入口，负责重试与企业微信摘要发送（成功/失败都会发送）。

---

### 4.2 异常处理

| 异常 | 处理 |
|------|------|
| Oracle连接失败 | 按重试策略执行，达到上限或不可重试时发送企业微信摘要 |
| MySQL写入失败 | 回滚，记录日志 |
| 数据量异常（差>10%） | 在验证环节提示，人工确认 |
| ETL执行完成（成功/失败） | 发送统一摘要（步骤状态、耗时、关键指标） |

---

### 4.3 监控检查

**每日检查项：**
- [ ] ETL是否成功完成
- [ ] 企业微信是否收到当日执行摘要（成功或失败）
- [ ] 各表记录数是否正常
- [ ] 昨日销售额是否有数据
- [ ] 库存快照是否生成

**快速检查SQL：**
```sql
-- 查看最新数据日期
SELECT 'dws_sales_daily' AS tbl, MAX(date_id) AS latest 
FROM dws_sales_daily
UNION ALL
SELECT 'dws_inventory_daily', MAX(date_id) 
FROM dws_inventory_daily
UNION ALL
SELECT 'ads_inventory_health', MAX(snapshot_date) 
FROM ads_inventory_health;

-- 查看昨日销售额
SELECT date_id, SUM(sales_amount) AS 销售额
FROM dws_sales_daily
WHERE date_id = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 DAY), '%Y%m%d')
GROUP BY date_id;
```

---

### 4.4 数据质量检查

**验证方法：**

1. **数据源验证**
   - 总库存 vs ERP系统（差异<1%）
   - 销售额 vs 各渠道后台（差异<3%）

2. **抽样验证**
   - 仓库抽查10个SKU实物库存
   - 商品部确认滞销商品清单
   - 运营确认TOP10热销排名

3. **签字确认**
   - 商品部：类别筛选、滞销判断
   - 仓库：库存数量
   - 运营：销售数据、周转阈值

---

### 4.5 常见问题排查

**问题1：ETL执行失败**
```bash
# 查看日志
tail -f logs/etl_20260120.log

# 手动重跑
python run_etl.py
```

**问题2：数据量异常**
```sql
-- 检查记录数变化
SELECT 
    DATE(etl_time) AS 日期,
    COUNT(*) AS 记录数
FROM dws_inventory_daily
GROUP BY DATE(etl_time)
ORDER BY 日期 DESC
LIMIT 7;
```

**问题3：数据对不上**
- 检查筛选条件（ISACTIVE='Y', STATUS=2）
- 检查仓库口径（总仓+云仓）
- 检查类别筛选（主销品ID列表）
- 检查日期范围

---

### 4.6 ETL配置文件

**config.py示例：**
```python
# Oracle配置
ORACLE_CONFIG = {
    'user': 'username',
    'password': 'password',
    'host': '192.168.1.100',
    'port': 1521,
    'service_name': 'ORCL'
}

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'dba',
    'password': 'password',
    'database': 'hefang_dw',
    'charset': 'utf8mb4'
}

# ETL重试配置（环境变量）
# 说明：run_etl.py 会优先读取环境变量，未设置时回落到 config.py 默认值
ETL_MAX_RETRIES = 3   # 最大重试次数
ETL_RETRY_SLEEP = 60  # 重试间隔秒数
```

---

### 4.7 文档同步闭环（SOP）

**定义：文档是代码的派生物（Single Source of Truth）**
规则：Python/SQL/配置是事实源；md 只能解释事实，不能自创事实。

**目标：** 避免“看起来完整”而编造字段、流程、表关系。

**闭环流程（6步）：**
1. **事实源约束**：以 `*.py`、`*.sql`、配置为准，文档只能解释代码中的事实。
2. **重构前审计**：生成“差异清单”（机器可读 JSON）。
    - 产出字段：`docs_only`、`code_only`、`intersection`、`risk_level`。
    - 输出路径：`reports/docs_code_alignment.json`。
    - 审计命令：`python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。
    - 降噪规则：审计脚本会自过滤审计元术语（如 advisories/non_blocking 标记词），避免将脚本自身新增输出词计入 `code_only` 噪音。
3. **任务拆分为三阶段**：
    - 阶段A：仅扫描，不改文档。输出差异清单 + 风险分级（高/中/低）。
    - 阶段B：只改高风险项（表名、字段名、核心计算逻辑、任务入口）。
    - 阶段C：回归复扫，确认差异数量下降；否则继续下一轮。
4. **Agent 改文档约束**（每轮固定提示）：
    - 仅允许修改 `docs/*.md` 与 `README.md`。
    - 每条修改必须附“来源代码文件 + 行号”。
    - 禁止补充无法从代码证明的描述。
    - 若 MySQL 表结构存在字段，则 `DATA_CONTRACTS` 的“关键字段”必须包含；且当 ETL 未写入时，含义中必须注明“字段存在但当前ETL不填充”。
    - 输出“未能确认项”（不得猜测补全）。
    - 每轮最多改 N 条（例如 20 条）。
5. **文档验收门禁（可验收条件）**：
    - 高风险差异项为 0。
    - 标记为 `reason=field_exists_but_not_filled` 的项仅作提醒，不计入阶段B/阶段C阻断条件。
    - 关键对象（任务名、表名、入口脚本）与代码一致。
    - 本轮新增代码若无文档映射，阻断合并。
6. **标准化 SOP**：
    - 开发改代码 → 运行审计 → 按清单分批改文档 → 人工复核高风险 → 复跑审计 → 合并。

**任务模板（中文，可直接用）：**
你现在是文档同步审计员。
第一步只读取 `reports/docs_code_alignment.json`，不要改文档。
输出：高/中/低风险差异清单（含原因）。

第二步仅修复高风险项，且只允许修改 `README.md` 和 `docs/*.md`。
每条修改必须给出“来源代码文件+行号”。
禁止补充无法从代码证明的信息。

第三步给出本轮修改摘要 + 未确认项列表。
最后提示复跑审计命令并比较前后差异数量。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v2.3 | 2026-02-27 | 更新ETL配置说明与排查建议 |
| v2.4 | 2026-02-28 | 新增文档同步闭环（SOP） |
| v2.5 | 2026-02-28 | 补充文档审计命令与统一输出路径 |
| v2.6 | 2026-02-28 | 明确DWD/ADS示例实现范围 |
| v2.7 | 2026-02-28 | 标注dim_date未在代码实现自动生成 |
| v2.8 | 2026-02-28 | 标注dws_sales_daily未在代码实现自动建表与net字段写入 |
| v2.9 | 2026-03-02 | 补充审计规则：结构字段入契约与未填充标注 |
| v3.0 | 2026-03-02 | 增加未填充字段提醒项 non-blocking 规则 |
| v3.1 | 2026-03-02 | 增加审计元术语自过滤降噪说明 |
| v3.2 | 2026-03-02 | 增加仅过滤审计脚本内部函数名策略 |
| v3.3 | 2026-03-18 | 新增 dim_channel 维度设计与主流水线时序 |
