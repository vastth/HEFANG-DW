# 更新日志（CHANGELOG）

> 说明：按日期与版本整理，条目按“Added / Changed / Fixed / Database / Docs”分类。


## 2026-02-24

### v0.6.2 — ETL摘要通知与调度入口统一（2026-02-24）

#### Changed
- `run_etl.py`：将 7 步 ETL 输出统一为结构化步骤报告（状态/详情/耗时），并在成功或失败场景都发送企业微信摘要。
- `run_etl.py`：统一摘要模板包含执行时间、总耗时、成功/警告/失败计数、步骤明细；失败时附加重试信息与失败原因。
- `run_etl.py`：重试等待参数改为读取 `ETL_RETRY_SLEEP`（默认回落到 `ETL_DEFAULT_RETRY_SLEEP`）。
- `scheduled_etl.py`：改为调用 `run_etl.py` 统一入口，避免多入口行为漂移；仅在 ETL 成功后继续执行 `test_etl_automation.py`。
- `run_scheduled_etl.bat`：同步说明当前链路为统一入口（含重试与摘要发送）。

#### Docs
- 更新 `README.md`：补充“成功/失败都发送企业微信统一摘要”策略与调度入口说明。
- 更新 `docs/数据仓库与ETL手册.md`：同步调度方式、异常处理与日常检查项。

#### Verified
- 本地连接测试模式（`ETL_CONN_TEST=1`、`ETL_MAX_RETRIES=1`）验证通过，企业微信成功收到摘要消息。


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