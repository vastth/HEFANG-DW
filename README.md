# 何方珠宝数据仓库（HEFANG DW）

<div align="center">

**基于Oracle到MySQL的珠宝电商数据仓库项目**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![Oracle](https://img.shields.io/badge/Oracle-11g-red.svg)](https://www.oracle.com/)
[![MySQL](https://img.shields.io/badge/MySQL-5.7+-blue.svg)](https://www.mysql.com/)

</div>

---

## 📖 项目简介

**何方珠宝数据仓库**是一个为广东何方珠宝有限公司打造的企业级数据仓库解决方案，旨在整合线上线下全渠道销售与库存数据，支撑库存优化、补货决策、销售分析等业务需求。

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
| **DIM** | 维度层 | `dim_*` | 商品、店仓等主数据 | 每日全量 |
| **DWS** | 汇总层 | `dws_*` | 日粒度销售、库存明细 | 销售增量/库存快照 |
| **ADS** | 应用层 | `ads_*` | 业务主题宽表（库存健康度等） | 每日全量 |

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
├── test_etl_automation.py       # ETL自动化测试
│
├── tools/                       # 辅助工具脚本（非运行链路）
│   ├── test_connection.py       # 数据库连接测试工具
│   ├── check_data.py            # 数据质量检查脚本
│   ├── check_dws_inventory.py   # 库存专项检查
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
├── README.md                    # 本文档
├── logs/                        # 日志输出目录
└── __pycache__/                 # Python缓存目录
```

---

## ⚡ 快速开始

### 1. 环境准备

**依赖安装**
```bash
pip install cx_Oracle pymysql pandas openpyxl
```

**Oracle Instant Client**（Windows系统需配置）
- 下载：https://www.oracle.com/database/technologies/instant-client/downloads.html
- 解压并配置环境变量：`PATH`添加instantclient路径

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

### 4. 首次全量ETL

```bash
python run_etl.py
```

执行流程：
```
[1/6] dim_product (商品维度) ✅
[2/6] dim_sku (SKU维度) ✅
[3/6] dim_store (店仓维度) ✅
[4/6] dws_sales_daily (销售明细) ✅
[5/6] dws_inventory_daily (库存明细) ✅
[6/6] ads_inventory_health (库存健康度) ✅
```

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
