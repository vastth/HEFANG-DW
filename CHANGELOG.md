# 更新日志（CHANGELOG）

> 说明：按 git 提交时间线整理。

## 2026-01-30

### cfeb3a1 — 补一条遗漏的推送（David）
- 更新 SQL 开发手册
  - [docs/SQL开发手册.md](docs/SQL开发手册.md)

### c9cef1d — docs: 同步SKU粒度与ETL口径说明（David）
- 同步SKU粒度口径与说明
  - [README.md](README.md)
  - [docs/mysql_data_dictionary.md](docs/mysql_data_dictionary.md)
  - [docs/业务逻辑与指标规范.md](docs/业务逻辑与指标规范.md)
  - [docs/数据仓库与ETL手册.md](docs/数据仓库与ETL手册.md)
  - [docs/数据结构与映射手册.md](docs/数据结构与映射手册.md)
  - [docs/问题排查手册.md](docs/问题排查手册.md)

### f39ce3c — feat: 销售/库存ETL切换SKU粒度并下沉口径过滤（David）
- ETL链路切换至SKU粒度，口径过滤下沉
  - [etl_ads_health.py](etl_ads_health.py)
  - [etl_dim_sku.py](etl_dim_sku.py)
  - [etl_dws_inventory.py](etl_dws_inventory.py)
  - [etl_dws_sales.py](etl_dws_sales.py)
  - [run_etl.py](run_etl.py)

## 2026-01-27

### e57eaac — 更新了README的署名（David）
- 更新署名
  - [README.md](README.md)

## 2026-01-23

### d680556 — 做了什么（David）
- 检查工具更新
  - [tools/check_data.py](tools/check_data.py)
  - [tools/check_dws_inventory.py](tools/check_dws_inventory.py)

### 6318488 — Initial commit: structure cleanup and env-based config（David）
- 初始化工程结构与配置
  - [README.md](README.md)
  - [config.py](config.py)
  - [docs/SQL开发手册.md](docs/SQL开发手册.md)
  - [docs/mysql_data_dictionary.md](docs/mysql_data_dictionary.md)
  - [docs/业务逻辑与指标规范.md](docs/业务逻辑与指标规范.md)
  - [docs/数据仓库与ETL手册.md](docs/数据仓库与ETL手册.md)
  - [docs/数据结构与映射手册.md](docs/数据结构与映射手册.md)
  - [docs/问题排查手册.md](docs/问题排查手册.md)
  - [etl_ads_health.py](etl_ads_health.py)
  - [etl_dim_product.py](etl_dim_product.py)
  - [etl_dim_store.py](etl_dim_store.py)
  - [etl_dws_inventory.py](etl_dws_inventory.py)
  - [etl_dws_sales.py](etl_dws_sales.py)
  - [run_etl.py](run_etl.py)
  - [run_scheduled_etl.bat](run_scheduled_etl.bat)
  - [scheduled_etl.py](scheduled_etl.py)
  - [test_etl_automation.py](test_etl_automation.py)
  - [tools/check_data.py](tools/check_data.py)
  - [tools/check_dws_inventory.py](tools/check_dws_inventory.py)
  - [tools/export_ads.py](tools/export_ads.py)
  - [tools/test_connection.py](tools/test_connection.py)
  - [notebooks/explore_M_IN_OUT_.ipynb](notebooks/explore_M_IN_OUT_.ipynb)
  - [notebooks/explore_M_PURCHASE.ipynb](notebooks/explore_M_PURCHASE.ipynb)
  - [notebooks/explore_M_TRANSFER.ipynb](notebooks/explore_M_TRANSFER.ipynb)
  - [notebooks/explore_RP_SIMPLESTORAGE.ipynb](notebooks/explore_RP_SIMPLESTORAGE.ipynb)
