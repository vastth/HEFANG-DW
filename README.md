# 何方珠宝数据仓库（HEFANG DW）

<div align="center">

**基于Oracle到MySQL的珠宝电商数据仓库项目**

[![Python](https://img.shields.io/badge/Python-3.13.9-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Oracle](https://img.shields.io/badge/Oracle-19c_EE-F80000?logo=oracle&logoColor=white)](https://www.oracle.com/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0.44-4479A1?logo=mysql&logoColor=white)](https://www.mysql.com/)

</div>

---

## 📖 项目简介

**何方珠宝数据仓库**是一个为广东何方珠宝有限公司打造的企业级数据仓库解决方案，旨在整合线上线下全渠道销售与库存数据，支撑库存优化、补货决策、销售分析等业务需求。

**更新日志**：详见 [CHANGELOG.md](CHANGELOG.md)

### 业务背景
- **企业**：广东何方珠宝有限公司（HEFANG Jewelry）
- **主营**：时尚珠宝首饰（925银饰为主）
- **业务模式**：电商（天猫、抖音、京东、小红书等）+ 线下门店（直营+加盟）
- **ERP系统**：伯俊ERP（Oracle 11g数据库）
- **数据规模**：15,000+ SKU / 500,000+ 订单 / 150+ 店仓

### 核心价值
✅ **库存优化**：精准计算库存周转天数与建议补货数量  
✅ **销售洞察**：全渠道销售趋势分析与SABC分级  
✅ **云仓管理**：支持门店云仓机制，扩大电商可售库存池  
✅ **自动化**：定时ETL任务，保障数据时效性  
✅ **可视化**：对接Tableau，支持高管驾驶舱与运营报表

---

## 🏗️ 架构设计

### 技术架构

```
┌─────────────────────────────────────────────────────────────┐
│                      数据消费层                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Tableau Dashboard │ Excel导出     │   API接口     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                           ▲
                           │
┌─────────────────────────────────────────────────────────────┐
│                   MySQL数据仓库 (hefang_dw)                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ ADS应用层: ads_inventory_health (库存健康度)         │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ DWS汇总层: dws_sales_daily | dws_inventory_daily     │   │
│  └──────────────────────────────────────────────────────────┘   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ DIM维度层: dim_product | dim_store | dim_sku         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                           ▲
                           │ Python ETL Scripts
                           │
┌─────────────────────────────────────────────────────────────┐
│                   Oracle源数据库 (伯俊ERP)                   │
│  M_RETAIL | M_RETAILITEM | FA_STORAGE | M_PRODUCT          │
│  C_STORE | M_PURCHASE | M_PURCHASEITEM | M_DIM             │
└─────────────────────────────────────────────────────────────┘
```

### 数据分层模型

| 数据层 | 名称 | 表前缀 | 说明 | 更新频率 |
|--------|------|--------|------|----------|
| **ODS** | 原始层 | `ods_*` | 1:1复制源表（可选） | 视需要全量 |
| **DIM** | 维度层 | `dim_*` | 商品、店仓等主数据 | 每日全量 |
| **DWS** | 汇总层 | `dws_*` | 日粒度销售、库存明细 | 销售增量/库存快照 |
| **ADS** | 应用层 | `ads_*` | 业务主题宽表（库存健康度等） | 每日全量 |

注：零售明细存在两条写入链路（线上/线下），增量同步采用“双水位”策略：
- `MODIFIEDDATE`：线上通道
- `SETTIME`：线下门店通道（`DOCNO/ORDERNO` 前缀含 `RT`）

---

## 📁 目录结构

```
hefang_dw/
├── config.py                    # 数据库连接配置
│
├── run_etl.py                   # ETL总控脚本（全量执行）
├── scheduled_etl.py             # 定时任务调度脚本
├── run_scheduled_etl.bat        # Windows计划任务脚本
│
├── etl_dim_product.py           # 商品维度ETL
├── etl_dim_sku.py               # SKU维度ETL
├── etl_dim_store.py             # 店仓维度ETL
├── etl_dws_sales.py             # 销售明细ETL（SKU粒度）
├── etl_dws_inventory.py         # 库存明细ETL（SKU粒度）
├── etl_ads_health.py            # 库存健康度ETL
├── etl_ods_fa_storage.py         # ODS库存全量同步（可选）
├── etl_ods_m_retail.py           # ODS零售主表全量同步（可选）
├── etl_ods_m_retailitem.py       # ODS零售明细全量同步（可选）
├── run_ods.py                    # ODS全量同步入口（可选）
├── test_etl_automation.py       # ETL自动化测试
│
├── tools/                       # 辅助工具脚本（非运行链路）
│   ├── test_connection.py       # 数据库连接测试工具
│   ├── check_data.py            # 数据质量检查脚本
│   ├── check_dws_inventory.py   # 库存专项检查
│   ├── check_ods_incremental.py # ODS对账（主表/明细）
│   ├── check_ods_retailitem_quality.py # ODS明细质量对账（双通道拆分）
│   └── export_ads.py            # ADS数据导出
│
├── notebooks/                   # 数据探索Jupyter笔记本（非运行链路）
│   ├── explore_M_IN_OUT_.ipynb
│   ├── explore_M_PURCHASE.ipynb
│   ├── explore_M_TRANSFER.ipynb
│   └── explore_RP_SIMPLESTORAGE.ipynb
│
├── docs/                        # 项目文档（⭐推荐阅读）
│   ├── 数据仓库与ETL手册.md     # 数仓架构与ETL流程
│   ├── 数据结构与映射手册.md     # 源表与目标表映射
│   ├── 业务逻辑与指标规范.md     # 指标定义与计算公式
│   ├── SQL开发手册.md           # SQL模板与开发规范
│   ├── 问题排查手册.md          # 常见问题与解决方案
│   ├── mysql_data_dictionary.md # MySQL数据字典（主）
│   └── misc/                    # 其他文档
│
├── SQL/                         # SQL脚本
│   └── create_ods_tables.sql     # ODS建表SQL（可选）
│
├── README.md                    # 本文档
├── logs/                        # 日志输出目录
└── __pycache__/                 # Python缓存目录
```

---

## ⚡ 快速开始

### 1. 环境准备

**依赖安装**
```bash
# 推荐使用 python-oracledb（thin 模式）或当需要时安装 Oracle Instant Client
pip install python-oracledb pymysql pandas openpyxl
```

**Oracle 连接说明（thin vs Instant Client）**
- `python-oracledb` 支持两种模式：
   - thin 模式（纯 Python，通常无需安装 Oracle Instant Client，适合大多数场景）。
   - thick/OCI 模式（依赖 Oracle Instant Client），当需要使用某些 Oracle 客户端特性或更高性能时才需要安装。详见官方文档。
- 如果你确实需要安装 Instant Client（Windows），请参考：
   - 下载：https://www.oracle.com/database/technologies/instant-client/downloads.html
   - 解压并配置环境变量：将 instantclient 路径加入 `PATH`。

### 2. 配置数据库连接

编辑 [config.py](config.py)，填入数据库信息：

```python
# Oracle源数据库（伯俊ERP）
ORACLE_CONFIG = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:1521/your_service_name'
}

# MySQL目标数仓
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'database': 'hefang_dw',
    'charset': 'utf8mb4'
}
```

### 3. 测试连接

```bash
python test_connection.py
```

预期输出：
```
✅ Oracle连接成功！
✅ MySQL连接成功！
```

### 3.1 告警与快速测试（新增）

项目支持通过企业微信机器人发送 ETL 执行摘要，并提供安全的连接测试模式以验证消息发送与重试策略：

- 环境变量：
   - `WECHAT_WEBHOOK`：企业微信机器人完整 webhook URL（建议通过环境变量注入，不要写入代码仓库）。
   - `ETL_CONN_TEST`：设置为 `1` 或在命令行添加 `--conn-test` 启用“仅连接测试”模式（不会写入数据）。
   - `ETL_MAX_RETRIES`：可选，覆盖默认最大重试次数（默认 3）。
   - `ETL_RETRY_SLEEP`：可选，覆盖重试间隔秒数（默认 60）。

- 消息发送策略：
   - 成功：7 个 ETL 步骤全部完成后，发送“成功摘要”。
   - 失败：重试结束或命中不可重试错误后，发送“失败摘要”（同一模板）。
   - 统一模板字段：执行时间、总耗时、成功/警告/失败计数、步骤明细（状态/耗时/关键指标）。

- 使用示例（临时设置并运行连接测试）：

```powershell
$env:WECHAT_WEBHOOK = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY'
$env:ETL_CONN_TEST = '1'
$env:ETL_MAX_RETRIES = '1'  # 仅尝试一次，便于快速验证告警
python run_etl.py --conn-test
```

- 说明：当脚本检测到不可重试的确定性错误（例如认证失败 ORA-01017、MySQL 1045），会立即停止重试并发送摘要，以避免无意义的重复尝试。摘要文本会自动使用 `config.py` 中的 `TASK_DISPLAY_NAME` 将任务 ID 映射为友好中文描述。

- 调度入口说明：
   - `run_etl.py` 是统一执行入口（包含重试 + 企微摘要发送）。
   - `scheduled_etl.py` 为调度包装脚本，内部调用 `run_etl.py`；`run_scheduled_etl.bat` 调用 `scheduled_etl.py`。

 - 模块与配置位置说明：
    - 告警实现：`alerts.py`（项目根目录），替换告警渠道时可直接修改或替换此模块。
    - 告警显示名称：`config.py` 中的 `TASK_DISPLAY_NAME`，可直接在配置中修改友好名称或做国际化处理。

 - Oracle 校验 SQL（可选）：
    - 为避免测试中使用硬编码常量，可在 `config.py` 中配置 `ORACLE_VERIFY_QUERIES` 字典，示例：

```python
# config.py
ORACLE_VERIFY_QUERIES = {
      'dws_inventory_main_products': """
            SELECT COUNT(DISTINCT p.ID)
            FROM FA_STORAGE fs
            LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
            LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
            WHERE fs.ISACTIVE = 'Y'
               AND fs.M_PRODUCTALIAS_ID IS NOT NULL
               AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
               AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
      """,
      'ads_health_total': """
            SELECT COUNT(DISTINCT fs.M_PRODUCTALIAS_ID)
            FROM FA_STORAGE fs
            LEFT JOIN M_PRODUCT p ON fs.M_PRODUCT_ID = p.ID
            LEFT JOIN C_STORE s ON fs.C_STORE_ID = s.ID
            WHERE fs.ISACTIVE = 'Y'
               AND fs.M_PRODUCTALIAS_ID IS NOT NULL
               AND (s.CODE = '001' OR s.IS_ALLO2OSTORAGE = 'Y')
               AND p.M_DIM4_ID IN (134,142,139,138,141,143,133,136,140,137,144,145)
      """,
}
```

    - `test_etl_automation.py` 会优先使用上述 SQL 从 Oracle 拉取对比计数，若未配置则回退到测试常量（便于快速运行）。

 - 关于销售负数：脚本中将销售表中的负数视为退货并计入统计（属于正常业务）；仅在负数数量异常增多时才会触发告警。


### 4. 首次全量ETL

```bash
python run_etl.py
```

执行流程：
```
[1/7] dim_product (商品维度) ✅
[2/7] dim_sku (SKU维度) ✅
[3/7] dim_store (店仓维度) ✅
[4/7] dws_sales_daily (销售明细) ✅
[5/7] dws_inventory_daily (库存明细) ✅
[6/7] dabo_ready (达播数据就绪检查/回填) ✅
[7/7] ads_inventory_health (库存健康度) ✅
```

### 4.1 ODS全量同步（可选）

```bash
# 先在MySQL执行 SQL/create_ods_tables.sql 建表
python run_ods.py
```

说明：`run_ods.py` 会在抽取完成后自动执行 ODS 质量校验，并将结果写入 `logs/ods_qc_*.log`。
说明：质量校验默认使用抽取完成时刻作为 `--as-of` 截止时间，避免时间漂移。

### 4.2 ODS增量同步（推荐）

```bash
# 默认：增量模式（回刷7天），窗口由脚本按模式自动选择
python run_ods.py

# 强制全量
python run_ods.py --full

# 调整回刷天数或窗口大小
python run_ods.py --backfill-days 14 --window-days 1
```

可选参数：
- 跳过质检：`--skip-qc`
- 质检全量：`--qc-all`
- 质检回看天数：`--qc-days 7`

### 5. 验证数据

```sql
-- 在MySQL中执行
SELECT 'dim_product' AS 表名, COUNT(*) AS 记录数 FROM dim_product
UNION ALL SELECT 'dim_sku', COUNT(*) FROM dim_sku
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dws_sales_daily', COUNT(*) FROM dws_sales_daily
UNION ALL SELECT 'dws_inventory_daily', COUNT(*) FROM dws_inventory_daily
UNION ALL SELECT 'ads_inventory_health', COUNT(*) FROM ads_inventory_health;
```

---

## 📊 数据仓库表说明

### 维度表 (DIM)

#### `dim_product` - 商品维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| product_id | 商品ID | 主键 |
| product_code | 商品编码 | 13位条码 |
| product_name | 商品名称 | - |
| brand | 品牌 | - |
| category | 类别 | 耳饰/项链/戒指等 |
| series | 系列 | - |
| property | 性质 | 常规款/限定款等 |
| material | 材质成分 | 如"925银、合成立方氧化锆" |
| price_retail | 吊牌价 | - |
| price_cost | 成本价 | - |

**源表**：Oracle `M_PRODUCT`, `M_DIM`  
**更新策略**：每日全量覆盖

#### `dim_sku` - SKU维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| sku_id | SKU ID | 主键/最小库存单位 |
| product_id | 商品ID | 对应款号 |
| sku_barcode | SKU条码 | - |
| sku_color | 颜色 | - |
| sku_size | 尺寸 | - |

**源表**：Oracle `M_PRODUCT_ALIAS`, `M_ATTRIBUTESETINSTANCE`  
**更新策略**：每日全量覆盖

#### `dim_store` - 店仓维度表
| 字段 | 说明 | 备注 |
|------|------|------|
| store_id | 店仓ID | 主键 |
| store_code | 店仓编码 | 001=总仓, DS%=电商, RT%=门店 |
| store_name | 店仓名称 | - |
| is_cloud_store | 是否云仓 | Y/N |

**源表**：Oracle `C_STORE`  
**更新策略**：每日全量覆盖

### 明细层 (DWS)

#### `dws_sales_daily` - 销售明细表
按日期+店仓+SKU粒度统计销售数据

| 字段 | 说明 | 计算逻辑 |
|------|------|----------|
| date_id | 日期 | YYYYMMDD格式 |
| store_code | 店仓编码 | - |
| is_cloud_store | 云仓标识 | Y/N |
| product_id | 商品ID | - |
| m_productalias_id | SKU ID | - |
| sales_qty | 销售数量 | 正单数量 |
| return_qty | 退货数量 | 负单数量（绝对值）|
| net_qty | 净销量 | 销售-退货 |
| sales_amount | 销售金额 | 正单金额 |

**源表**：Oracle `M_RETAIL`, `M_RETAILITEM`, `C_STORE`, `M_PRODUCT`  
**更新策略**：增量更新（智能判断：凌晨查昨天，白天查今天）

#### `dws_inventory_daily` - 库存明细表
按日期+店仓+SKU粒度记录库存快照

| 字段 | 说明 | 备注 |
|------|------|------|
| date_id | 快照日期 | YYYYMMDD格式 |
| store_code | 店仓编码 | - |
| is_cloud_store | 云仓标识 | Y/N |
| product_id | 商品ID | - |
| m_productalias_id | SKU ID | - |
| qty | 库存数量 | - |
| qtypurchaserem | 采购欠数 | 在途库存（已下单未入库）|

**源表**：Oracle `FA_STORAGE`, `C_STORE`, `M_PRODUCT`  
**更新策略**：每日全量快照

### 应用层 (ADS)

#### `ads_inventory_health` - 库存健康度应用表
每个SKU的库存健康度全方位分析

| 字段分类 | 字段名 | 说明 |
|----------|--------|------|
| **基础信息** | product_id, product_code, product_name | 商品信息 |
| | sku_id, sku_barcode, color, size | SKU信息 |
| | brand, category, series, property | 分类属性 |
| **库存指标** | total_qty / warehouse_qty / cloud_qty | 总库存/总仓/云仓 |
| | purchase_rem_qty | 采购欠数（在途库存）|
| **销售指标** | sales_qty_7d / sales_qty_30d | 近7天/30天销量 |
| | sales_amt_30d | 近30天销售额 |
| | return_qty_30d / return_amount_30d | 近30天退货量/退货额 |
| | daily_avg_sales / daily_avg_sales_7d | 30天/7天日均销量 |
| **周转指标** | turnover_days | 库存周转天数 |
| | suggest_qty | 建议补货数量（可为负）|
| **分级指标** | sku_grade | SABC分级 |
| | inventory_status / status_priority | 库存状态/优先级 |
| **趋势指标** | sales_velocity / sales_trend | 销售加速度/趋势 |

**核心算法**：
```
建议补货数量 = (90天 - 当前周转天数) × 日均销量 - 近30天退货 - 采购欠数
库存周转天数 = 当前库存 / (近30天销售 / 30)
销售加速度 = (近7天日均销量) / (近30天日均销量)
```

**库存状态分级**：
| 状态 | 条件 | 补货优先级 |
|------|------|------------|
| 紧急缺货 | 有销售 且 周转<30天 | 🔴 1级 |
| 需补货 | 有销售 且 30≤周转<70天 | 🟠 2级 |
| 正常 | 有销售 且 70≤周转≤90天 | 🟢 3级 |
| 库存过高 | 有销售 且 周转>90天 | 🔵 4级 |
| 滞销 | 有库存 但 无销售 | ⚪ 5级 |
| 停售 | 无库存 且 无销售 | ⚫ 6级 |

**SABC分级**（基于销售金额累计占比）：
- **S类**（前30%）：超级爆款，最高优先级
- **A类**（30%-70%）：核心款，重点监控
- **B类**（70%-90%）：常规款，正常补货
- **C类**（90%-100%+无销售）：长尾/滞销款

**源表**：MySQL `dws_sales_daily`, `dws_inventory_daily`, `dim_product`, `dim_store`, `dim_sku`  
**更新策略**：每日全量重算

---

## 🔄 定时任务配置

### 方案一：Windows计划任务

```
任务：每日凌晨3点执行
程序：python
参数：C:\Users\tianhao\PycharmProjects\hefang_dw\run_etl.py
起始于：C:\Users\tianhao\PycharmProjects\hefang_dw
```

或使用批处理脚本：
```bash
# 运行 run_scheduled_etl.bat
```

### 方案二：Linux Crontab

```bash
# 每天凌晨3点执行
0 3 * * * cd /opt/hefang_dw && python run_etl.py >> /var/log/hefang_etl.log 2>&1
```

---

## 🛠️ 数据维护

### 回补历史数据

```bash
# 回补近90天销售数据（示例）
python -c "from etl_dws_sales import backfill; backfill(20251102, 20260130)"

# 重算库存健康度
python etl_ads_health.py
```

### 数据质量检查

```bash
# 全面数据质量检查
python check_data.py

# 库存专项检查
python check_dws_inventory.py

# ODS对账（主表/明细）
python tools/check_ods_incremental.py --days 7

# ODS明细质量对账（双通道拆分）
python tools/check_ods_retailitem_quality.py --days 7

# 使用截止时间避免时间漂移
python tools/check_ods_incremental.py --days 7 --as-of "2026-02-26 17:11:52"
python tools/check_ods_retailitem_quality.py --days 7 --as-of "2026-02-26 17:11:52"
```

### 导出应用层数据

```bash
# 导出库存健康度到Excel
python export_ads.py
```

---

## 📚 文档导航

### 核心文档（推荐阅读）

| 文档 | 内容 | 适用人群 |
|------|------|----------|
| [数据仓库与ETL手册](docs/数据仓库与ETL手册.md) | 数仓架构、ETL流程、任务调度 | 数据工程师 |
| [数据结构与映射手册](docs/数据结构与映射手册.md) | 源表结构、字段映射、取数逻辑 | 开发人员 |
| [业务逻辑与指标规范](docs/业务逻辑与指标规范.md) | 指标定义、计算公式、业务规则 | 业务分析师、产品经理 |
| [SQL开发手册](docs/SQL开发手册.md) | SQL模板、开发规范、最佳实践 | SQL开发者 |
| [问题排查手册](docs/问题排查手册.md) | 常见问题、排查步骤、解决方案 | 运维人员 |

### 扩展文档

- [mysql_data_dictionary.md](mysql_data_dictionary.md) - MySQL数据字典
- [docs/mysql_data_dictionary.md](docs/mysql_data_dictionary.md) - 详细版数据字典

---

## 🔒 数据治理

### 数据质量规则

| 规则类型 | 检查项 | 阈值 |
|----------|--------|------|
| 完整性 | 主键非空率 | 100% |
| 一致性 | 销售金额 vs 行金额合计差异 | <0.1% |
| 及时性 | 最新数据日期 | T-1日 |
| 准确性 | 库存周转天数异常值 | <0或>1000标记 |

### 字段命名规范

```
1. 表名：{层级}_{主题}_{粒度}
   示例：dws_sales_daily, ads_inventory_health

2. 字段名：小写+下划线
   示例：product_id, sales_qty, turnover_days

3. 日期字段：snapshot_date (YYYYMMDD格式)

4. 数量字段：qty / amount / count
   示例：sales_qty, total_qty, store_count

5. 标识字段：is_xxx / has_xxx
   示例：is_cloud_store, has_sales
```

---

## 🤝 团队协作

### 角色分工

| 角色 | 职责 | 联系方式 |
|------|------|----------|
| 数据工程师 | ETL开发、数据仓库维护 | tianxiaoyu911@gmail.com |
| 业务分析师 | 指标定义、报表需求 | tianxiaoyu911@gmail.com |
| 数据库管理员 | 数据库优化、权限管理 | tianxiaoyu911@gmail.com |
| 项目负责人 | 需求评审、优先级排序 | tianxiaoyu911@gmail.com |

### 开发流程

```
1. 需求评审 → 2. 技术方案设计 → 3. 开发测试 → 4. 上线部署 → 5. 监控维护
```

### 代码管理

```bash
# 分支策略
main    - 生产环境
dev     - 开发环境
feature/* - 功能分支
hotfix/* - 紧急修复
```

---

## 📈 版本记录

| 版本 | 日期 | 更新内容 | 维护人 |
|------|------|----------|--------|
| v1.0 | 2026-01-15 | 初始版本，包含DIM/DWS/ADS层 | tianxiaoyu911@gmail.com |
| v1.1 | 2026-01-19 | 新增采购欠数字段，优化建议补货算法 | tianxiaoyu911@gmail.com |
| v1.2 | 2026-01-20 | 文档重构，新增架构说明与使用指南 | tianxiaoyu911@gmail.com |
| v1.3 | 2026-01-30 | SKU维度与SKU粒度同步，销售智能判断与口径统一 | tianxiaoyu911@gmail.com |

---

## 📞 技术支持

### 常见问题

**Q1: Oracle连接失败？**
- 检查Instant Client是否安装
- 验证`config.py`中的连接配置
- 确认网络防火墙设置

**Q2: ETL执行失败？**
- 查看`logs/`目录下的日志文件
- 检查源数据库表是否正常
- 验证MySQL数据库权限

**Q3: 数据不一致？**
- 运行`check_data.py`进行质量检查
- 对比源表与目标表记录数
- 查看[问题排查手册](docs/问题排查手册.md)

### 联系方式

- **项目仓库**：（内部Git地址）
- **技术文档**：[docs/](docs/)目录
- **问题反馈**：提交Issue或联系维护人员

---

## 📄 License

本项目为广东何方珠宝有限公司内部数据仓库项目，仅供内部使用。

---

<div align="center">

**⭐ 建议优先阅读 [业务逻辑与指标规范](docs/业务逻辑与指标规范.md) 了解核心业务逻辑**

**Made with ❤️ by HEFANG Data Team**

</div>
