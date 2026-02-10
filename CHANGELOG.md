# 更新日志（CHANGELOG）

> 说明：按日期与版本整理，条目按“Added / Changed / Fixed / Database / Docs”分类。


## 2026-02-06

### v0.6.1 — 告警与重试逻辑重构（2026-02-06）

#### Changed
- 将企业微信告警发送逻辑抽离为独立模块 `alerts.py`，便于替换或扩展告警渠道（例如支持邮件/钉钉等）。
- 将任务友好名称映射 `TASK_DISPLAY_NAME` 移至配置 `config.py`，便于运维调整与国际化。
- 在 `config.py` 中新增重试相关配置：`ETL_NON_RETRYABLE_ERROR_KEYWORDS`、`ETL_RETRYABLE_ERROR_KEYWORDS`、`ETL_MAX_RETRIES`（可通过环境变量覆盖）、`ETL_RETRY_SLEEP`。
- 改进 `run_etl.py` 的错误摘要提取逻辑（`_extract_error_summary`）：过滤 Help/URL 行并优先返回 ORA- 错误行，使告警内容更具可操作性。
- 新增判断逻辑 `_should_retry_based_on_details`：遇到确定性不可重试错误（例如认证/权限失败）会立即告警并放弃重试，避免无意义重复尝试。

#### Docs
- 更新 `README.md`：新增告警与测试相关环境变量说明（`WECHAT_WEBHOOK`、`ETL_CONN_TEST`、`ETL_MAX_RETRIES`、`ETL_RETRY_SLEEP`）以及 `--conn-test` 测试说明。

#### Verified
- 在本地以 `--conn-test`（故意使用错误凭据）运行验证：脚本在检测到认证失败后发出立即告警，且企业微信 webhook 返回成功。

### v0.6.0 — 达播纳入 ETL 可观测链路（2026-02-04）

#### Added
- 将外部达播（Dabo）CSV 纳入 ETL 可观测链路：新增 `dabo_ready` 就绪检查步骤，满足条件后触发回填
- ADS 宽表新增达播相关字段与“自然销量 / 自然销售额”字段：
  - `dabo_latest_date`
  - 达播 7 / 30 天销量与销售额
  - 自然销量 / 自然销售额（剔除达播影响）

#### Changed
- `etl_ads_health.py`：新增达播字段、回填逻辑，并补充自然口径计算
- `run_etl.py`：加入 `dabo_ready` 步骤与回填条件控制

#### Fixed
- 修复 ETL 中字段引用：`p.m_dim4_id` → `p.category_id`
- 解决 MySQL JOIN 字符集 / 排序规则冲突：关联字段显式使用 `COLLATE utf8mb4_unicode_ci`

#### Database / SQL
- 新增达播相关建表脚本：`ads_dabo_daily_sales`、`log_dabo_import`
- 为避免 MySQL `ADD COLUMN IF NOT EXISTS` 兼容问题，`ads_inventory_health` 改为分步 ALTER：
  - `alter_ads_inventory_health_add_dabo_latest_date.sql`
  - `alter_ads_inventory_health_add_dabo_revenue_fields.sql`
  - `alter_ads_inventory_health_add_dabo_natural_fields.sql`

#### Docs
- 同步更新达播 ETL、字段定义、回填与口径说明：
  - `docs/达播数据运营上传指南.md`
  - `docs/数据仓库与ETL手册.md`
  - `docs/数据结构与映射手册.md`
  - `docs/业务逻辑与指标规范.md`
  - `docs/SQL开发手册.md`
  - `docs/mysql_data_dictionary.md`
- `README.md` 增加 ETL 步骤说明（含 `dabo_ready`）与 CHANGELOG 链接

## 2026-01-30

### cfeb3a1 — 补一条遗漏的推送（2026-01-30）

#### Changed
- 更新 `docs/SQL开发手册.md`

### c9cef1d — docs: 同步 SKU 粒度与 ETL 口径说明（2026-01-30）

# 更新日志（CHANGELOG）

> 说明：按日期与版本整理，条目按“Added / Changed / Fixed / Database / Docs”分类。


## 2026-02-06

### v0.6.1 — 告警与重试逻辑重构（2026-02-06）

#### Changed
- 将企业微信告警发送逻辑抽离为独立模块 `alerts.py`，便于替换或扩展告警渠道（例如支持邮件/钉钉等）。
- 将任务友好名称映射 `TASK_DISPLAY_NAME` 移至配置 `config.py`，便于运维调整与国际化。
- 在 `config.py` 中新增重试相关配置：`ETL_NON_RETRYABLE_ERROR_KEYWORDS`、`ETL_RETRYABLE_ERROR_KEYWORDS`、`ETL_MAX_RETRIES`（可通过环境变量覆盖）、`ETL_RETRY_SLEEP`。
- 改进 `run_etl.py` 的错误摘要提取逻辑（`_extract_error_summary`）：过滤 Help/URL 行并优先返回 ORA- 错误行，使告警内容更具可操作性。
- 新增判断逻辑 `_should_retry_based_on_details`：遇到确定性不可重试错误（例如认证/权限失败）会立即告警并放弃重试，避免无意义重复尝试。

#### Docs
- 更新 `README.md`：新增告警与测试相关环境变量说明（`WECHAT_WEBHOOK`、`ETL_CONN_TEST`、`ETL_MAX_RETRIES`、`ETL_RETRY_SLEEP`）以及 `--conn-test` 测试说明。

#### Verified
- 在本地以 `--conn-test`（故意使用错误凭据）运行验证：脚本在检测到认证失败后发出立即告警，且企业微信 webhook 返回成功。

### v0.6.0 — 达播纳入 ETL 可观测链路（2026-02-04）

#### Added
- 将外部达播（Dabo）CSV 纳入 ETL 可观测链路：新增 `dabo_ready` 就绪检查步骤，满足条件后触发回填
- ADS 宽表新增达播相关字段与“自然销量 / 自然销售额”字段：
  - `dabo_latest_date`
  - 达播 7 / 30 天销量与销售额
  - 自然销量 / 自然销售额（剔除达播影响）

#### Changed
- `etl_ads_health.py`：新增达播字段、回填逻辑，并补充自然口径计算
- `run_etl.py`：加入 `dabo_ready` 步骤与回填条件控制

#### Fixed
- 修复 ETL 中字段引用：`p.m_dim4_id` → `p.category_id`
- 解决 MySQL JOIN 字符集 / 排序规则冲突：关联字段显式使用 `COLLATE utf8mb4_unicode_ci`

#### Database / SQL
- 新增达播相关建表脚本：`ads_dabo_daily_sales`、`log_dabo_import`
- 为避免 MySQL `ADD COLUMN IF NOT EXISTS` 兼容问题，`ads_inventory_health` 改为分步 ALTER：
  - `alter_ads_inventory_health_add_dabo_latest_date.sql`
  - `alter_ads_inventory_health_add_dabo_revenue_fields.sql`
  - `alter_ads_inventory_health_add_dabo_natural_fields.sql`

#### Docs
- 同步更新达播 ETL、字段定义、回填与口径说明：
  - `docs/达播数据运营上传指南.md`
  - `docs/数据仓库与ETL手册.md`
  - `docs/数据结构与映射手册.md`
  - `docs/业务逻辑与指标规范.md`
  - `docs/SQL开发手册.md`
  - `docs/mysql_data_dictionary.md`
- `README.md` 增加 ETL 步骤说明（含 `dabo_ready`）与 CHANGELOG 链接

## 2026-01-30

### cfeb3a1 — 补一条遗漏的推送（2026-01-30）

#### Changed
- 更新 `docs/SQL开发手册.md`

### c9cef1d — docs: 同步 SKU 粒度与 ETL 口径说明（2026-01-30）

#### Changed
- 同步 SKU 粒度口径与说明，更新：
  - `README.md`
  - `docs/mysql_data_dictionary.md`
  - `docs/业务逻辑与指标规范.md`
  - `docs/数据仓库与ETL手册.md`
  - `docs/数据结构与映射手册.md`
  - `docs/问题排查手册.md`

### f39ce3c — feat: 销售/库存 ETL 切换 SKU 粒度并下沉口径过滤（2026-01-30）

#### Added / Changed
- ETL 链路切换至 SKU 粒度，口径过滤下沉，变更文件：
  - `etl_ads_health.py`
  - `etl_dim_sku.py`
  - `etl_dws_inventory.py`
  - `etl_dws_sales.py`
  - `run_etl.py`

## 2026-01-27

### e57eaac — 更新 README 署名（2026-01-27）

#### Changed
- 更新 `README.md` 署名信息

## 2026-01-23

### d680556 — 检查工具更新（2026-01-23）

#### Changed
- 更新检查工具：
  - `tools/check_data.py`
  - `tools/check_dws_inventory.py`

### 6318488 — Initial commit: structure cleanup and env-based config（2026-01-23）

#### Added
- 初始化工程结构与配置，相关文件：
  - `README.md`
  - `config.py`
  - 文档：
    - `docs/SQL开发手册.md`
    - `docs/mysql_data_dictionary.md`
    - `docs/业务逻辑与指标规范.md`
    - `docs/数据仓库与ETL手册.md`
    - `docs/数据结构与映射手册.md`
    - `docs/问题排查手册.md`
  - ETL 脚本：
    - `etl_ads_health.py`
    - `etl_dim_product.py`
    - `etl_dim_store.py`
    - `etl_dws_inventory.py`
    - `etl_dws_sales.py`
    - `run_etl.py`
    - 计划任务脚本：`run_scheduled_etl.bat`, `scheduled_etl.py`
  - 测试与工具：
    - `test_etl_automation.py`
    - `tools/*`
  - notebooks：
    - `notebooks/explore_M_IN_OUT_.ipynb`
    - `notebooks/explore_M_PURCHASE.ipynb`
    - `notebooks/explore_M_TRANSFER.ipynb`
    - `notebooks/explore_RP_SIMPLESTORAGE.ipynb`

