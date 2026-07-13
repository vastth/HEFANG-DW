# RUNBOOK.md — 何方珠宝数据仓库运行手册

> 适用于：数据工程师 / 运维人员 / AI 协作 Agent
>
> 前置阅读：[docs/ARCHITECTURE.md](ARCHITECTURE.md)

---

## 1. 环境准备

### 1.1 Python 环境

```powershell
# 检查 Python 版本（需要 3.10+，项目在 3.13.x 开发）
python --version

# 安装依赖（无 requirements.txt 时手动安装）
pip install python-oracledb pandas sqlalchemy pymysql requests openpyxl

# 验证关键包
python -c "import oracledb, pandas, sqlalchemy, pymysql; print('OK')"
```

> **注意**：`python-oracledb` 使用 thin 模式，**无需安装 Oracle Instant Client**。

### 1.2 环境变量配置

`.env.example` 仅作为变量清单参考；当前脚本默认不自动加载 `.env` 文件。推荐直接设置 User 级别永久环境变量：

```powershell
# Oracle（伯俊 ERP）
[Environment]::SetEnvironmentVariable('ORACLE_USER',     'your_user',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_PASSWORD', 'your_pass',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_HOST',     '10.x.x.x',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_PORT',     '1521',         'User')
[Environment]::SetEnvironmentVariable('ORACLE_SERVICE',  'your_service', 'User')

# MySQL（何方数仓）
[Environment]::SetEnvironmentVariable('MYSQL_HOST',     'localhost', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_PORT',     '3306',      'User')
[Environment]::SetEnvironmentVariable('MYSQL_USER',     'your_user', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_PASSWORD', 'your_pass', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_DB',       'hefang_dw', 'User')

# 连接池与超时（可选；未设置时使用 db_connections.py 默认值）
[Environment]::SetEnvironmentVariable('MYSQL_POOL_SIZE',      '5',    'User')
[Environment]::SetEnvironmentVariable('MYSQL_MAX_OVERFLOW',   '5',    'User')
[Environment]::SetEnvironmentVariable('MYSQL_POOL_TIMEOUT',   '30',   'User')
[Environment]::SetEnvironmentVariable('MYSQL_POOL_RECYCLE',   '1800', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_CONNECT_TIMEOUT','10',   'User')
[Environment]::SetEnvironmentVariable('MYSQL_READ_TIMEOUT',   '60',   'User')
[Environment]::SetEnvironmentVariable('MYSQL_WRITE_TIMEOUT',  '60',   'User')
[Environment]::SetEnvironmentVariable('MYSQL_ETL_READ_TIMEOUT',        '300', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_ETL_WRITE_TIMEOUT',       '300', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_LONG_RUNNING_READ_TIMEOUT','600', 'User')
[Environment]::SetEnvironmentVariable('MYSQL_LONG_RUNNING_WRITE_TIMEOUT','600', 'User')
[Environment]::SetEnvironmentVariable('ORACLE_POOL_SIZE',     '3',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_MAX_OVERFLOW',  '2',    'User')
[Environment]::SetEnvironmentVariable('ORACLE_POOL_TIMEOUT',  '30',   'User')
[Environment]::SetEnvironmentVariable('ORACLE_POOL_RECYCLE',  '1800', 'User')

# 企业微信告警（可选）
[Environment]::SetEnvironmentVariable('WECHAT_WEBHOOK', 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY', 'User')

# NAS 自动鉴权（门店日报目标目录、达播云雀目录）
[Environment]::SetEnvironmentVariable('HEFANG_NAS_USERNAME', 'your_nas_user', 'User')
[Environment]::SetEnvironmentVariable('HEFANG_NAS_PASSWORD', 'your_nas_password', 'User')
```

说明：hefang_dw 的 SQLAlchemy Engine 与 PyMySQL / Oracle 直连统一由 [db_connections.py](../db_connections.py) 创建。MySQL Engine 默认启用连接可用性探测、空闲回收、池等待超时和最大溢出控制；PyMySQL 直连默认注入连接、读取、写入超时。MySQL 超时现分三档：`default` 走 `MYSQL_READ_TIMEOUT` / `MYSQL_WRITE_TIMEOUT`，`etl` 走 `MYSQL_ETL_READ_TIMEOUT` / `MYSQL_ETL_WRITE_TIMEOUT`，`long_running` 走 `MYSQL_LONG_RUNNING_READ_TIMEOUT` / `MYSQL_LONG_RUNNING_WRITE_TIMEOUT`。当前长事务专题已由调用方显式切档，不再依赖全局一刀切超时。

### 1.2.1 HFSY 只读探查连接事实

当前已确认的数云源端连接元信息如下：

- 数据库版本：MySQL `5.7.42`
- 部署地址：`8.134.87.152:33066`
- 数据库名：`hfsy`
- 接入账号：`shuyun668`

真实密码已由用户提供，但按仓库安全约束，不写入任何被 git 跟踪的文档、脚本或模板文件。推荐在本地终端按会话临时注入：

```powershell
$env:HFSY_MYSQL_HOST='8.134.87.152'
$env:HFSY_MYSQL_PORT='33066'
$env:HFSY_MYSQL_USER='shuyun668'
$env:HFSY_MYSQL_PASSWORD='请通过本地安全方式注入真实密码'
$env:HFSY_MYSQL_DB='hfsy'
```

说明：
- 当前仓库脚本尚未正式消费 `HFSY_MYSQL_*` 变量；这些变量主要用于保存只读探查上下文，避免和数仓库 `MYSQL_*` 混用。
- 若临时复用 [tools/query_data.py](../tools/query_data.py)，应在独立终端里临时覆盖 `MYSQL_*` 到 `hfsy`，执行完成后关闭该终端，避免误连到生产数仓。

### 1.3 连通性验证

```powershell
# 自检脚本（检查 Python / 环境变量 / 包）
pwsh scripts/doctor.ps1

# 真实连通测试（需要配置环境变量）
python tools/test_connection.py
```

---

## 2. 常用命令

### 2.1 主 ETL 流水线

```powershell
# 正常执行（T-1 增量，约 10-20 分钟）
python run_etl.py

# 连通性测试模式（不执行真实 ETL，仅测试数据库连接）
python run_etl.py --conn-test

# 连通性测试 + 单次重试（用于 CI 验证）
$env:ETL_MAX_RETRIES=1; $env:ETL_CONN_TEST=1; python run_etl.py --conn-test

# 主链保持 legacy 写数，同时补做 v2 shadow ADS 对账
python run_etl.py --cutover-mode shadow_compare

# 主链显式让 ads_inventory_health 改读 DWS v2
python run_etl.py --cutover-mode v2

# 即使外部已设置 cutover，也可显式回滚到 legacy
python run_etl.py --cutover-mode v2 --rollback-to-legacy
```

说明：主链默认 `legacy`。`shadow_compare` 只追加 `ads_inventory_health` 的 `_v2` 报告型对账，不改变生产写数表；`v2` 才会显式改读 `dws_inventory_daily_v2 + dws_sales_daily_v2`；`--rollback-to-legacy` 优先级高于 `--cutover-mode`。来源：[run_etl.py](../run_etl.py#L767)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)；[run_etl.py](../run_etl.py#L998)；[cutover_controls.py](../cutover_controls.py#L29)

### 2.2 ODS 专项

```powershell
# 增量同步（生产日常，使用双水位）
python run_ods.py

# 全量覆盖（首次初始化或数据修复用；默认补一轮 full 后 catch-up）
python run_ods.py --full

# 调整或关闭 full 后补追窗口
python run_ods.py --full --full-catchup-days 2
python run_ods.py --full --full-catchup-days 0

# 跳过自动质量校验
python run_ods.py --skip-qc

# 仅执行质量校验（不触发同步）
python tools/check_ods_incremental.py
python tools/check_ods_retailitem_quality.py
```

说明：自 2026-04-07 起，`run_ods.py --full` 在 `ods_m_retail` / `ods_m_retailitem` 全量完成后，会按同一个固定 `as-of` 自动补一轮最近 1 天的增量 catch-up，并让后续 ODS 质检复用这个 `as-of`；这样可以减少长时间全量期间的在途新增漏数。`--full-catchup-days 0` 可关闭该补追。来源：[run_ods.py](../run_ods.py#L72-L125)；[etl_ods_m_retail.py](../etl_ods_m_retail.py#L91-L151)；[etl_ods_m_retailitem.py](../etl_ods_m_retailitem.py#L134-L206)

### 2.3 单步 ETL 调试

```powershell
# 单独执行某一 ETL 模块（以 dws_sales 为例，模拟主链近7天回带）
python -c "import etl_dws_sales; etl_dws_sales.run(days_back=7, include_today=True)"

# 历史回填（指定日期范围）
python -c "import etl_dws_sales; etl_dws_sales.backfill('2026-01-01', '2026-01-31')"

# 库存快照
python -c "import etl_dws_inventory; etl_dws_inventory.run()"

# DWS v2 销售并行表：dry-run 输出 SQL、写后摘要与对账 SQL，不写旧表、不进 run_etl 主链
python etl_dws_sales_v2.py --start-date 20260428 --end-date 20260430

# DWS v2 销售并行表：只读连接与结构检查
python etl_dws_sales_v2.py --conn-test

# DWS v2 销售并行表：S3 手工写入（仅用户手工执行；需确认令牌、命名锁、事务与 JSON 运行证据）
python etl_dws_sales_v2.py --execute --confirm-write WRITE_DWS_SALES_V2

# DWS v2 库存并行表：dry-run 输出 SQL、写后摘要与对账 SQL，不写旧表、不进 run_etl 主链
python etl_dws_inventory_v2.py --snapshot-date 20260507

# DWS v2 库存并行表：只读连接与结构检查，默认 timeout_profile=long_running
python etl_dws_inventory_v2.py --conn-test

# DWS v2 库存并行表：S3 手工写入（仅用户手工执行；需确认令牌、命名锁、事务与 JSON 运行证据）
python etl_dws_inventory_v2.py --execute --confirm-write WRITE_DWS_INVENTORY_V2

# DWS v2 shadow 调度：只读连接与结构检查
python scheduled_dws_v2_shadow.py --conn-test

# DWS v2 shadow 调度：独立串 raw ODS → DWD → DWS v2
python scheduled_dws_v2_shadow.py

# DWS v2 shadow 调度：如需固定 inventory same snapshot，可透传 old DWS 对齐或显式 cutoff
python scheduled_dws_v2_shadow.py --inventory-align-with-old-dws
python scheduled_dws_v2_shadow.py --inventory-source-loaded-at-cutoff "2026-05-12 09:38:18"

说明：inventory same-snapshot 当前按 `dwd_inventory_storage_snapshot.source_loaded_at` 截止；该字段来自 raw ODS 的 `etl_loaded_at`，表示 MySQL 装载时点，不是 Oracle 源端业务时点。若 cutoff 之后仍存在 `dws_inventory_scope_flag='Y'` 的库存行，`etl_dws_inventory_v2.py` 会直接拒绝继续对齐并报错，避免把 old `dws_inventory_daily.etl_time` 误当成可回放的历史 source snapshot。此时应优先检查 inventory raw 窗口与 DWD 补数批次，而不是继续使用该 cutoff 做 ADS gate 判责。

# 库存健康度重算
python -c "import etl_ads_health; etl_ads_health.run()"

# 门店经营日报：依赖检查
python etl_ads_store_daily_report.py --conn-test

# 门店经营日报统计主体层：依赖检查
python etl_ads_store_daily_subject_report.py --conn-test

# 门店日报目标导入：按月份分文件时，显式指定目标月份
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5

# 门店日报目标导入：连同门店属性一起预演
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5 --sync-store-report-attr

# 门店日报门店属性快照：首次启用前先由用户人工执行建表 SQL
Get-Content SQL/create_store_report_attr_snapshot.sql

# 门店日报目标导入：预演共同考核多 sheet 配置
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5 --sync-store-report-attr

# 门店日报渠道粗分类：查看已执行到现网的生成列 DDL
Get-Content SQL/alter_dim_store_report_attr_add_channel_type_group.sql

# 门店日报目标导入：正式写库
# 现网已于 2026-04-03 完成日志表建表；新环境首次使用前先执行 SQL/create_log_store_target_import.sql
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --created-by your_name

# 门店日报目标导入：多月份文件下，显式写入指定月份并同步门店属性
# 首次启用门店属性同步前，还需由用户人工执行 SQL/create_store_report_attr_snapshot.sql
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --sync-store-report-attr --created-by your_name

# 门店日报目标导入：正式范围扩容时，同步刷新 dim_store_report_attr
python tools/import_cfg_store_target_daily_from_nas.py --apply --sync-store-report-attr --created-by your_name

# 门店经营负责人快照：dry-run
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --preview-limit 10

# 门店经营负责人快照：显式指定文件与工作表
python tools/import_store_operation_owner_from_nas.py --file-path "\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx" --sheet-name 门店负责人映射模板 --snapshot-date 2026-04-21 --preview-limit 10

# 门店经营负责人快照：正式写库
# 新环境首次使用前先执行 SQL/create_store_operation_owner_tables.sql
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --apply --created-by your_name

# 门店经营负责人导入：最小单元测试（不写库）
python -m unittest test_store_operation_owner_import.py

# 门店属性差异清单：只读比对 April 权威快照与当前有效门店属性
python tools/diff_store_report_attr_snapshot.py --target-month 2026-04 --preview-limit 10 --output-json reports/store_attr_snapshot_diff_202604.json

# 门店属性快照登记：记录 NAS 快照、diff 摘要与当前落地状态
python tools/register_store_attr_snapshot.py --target-month 2026-04 --diff-output reports/store_attr_snapshot_diff_202604_registered.json

# 门店日报专题调度：只做文件解析、日志表检查和 dry-run
python scheduled_store_daily_report.py --conn-test

# 门店日报专题调度：自动检查 NAS 最新目标文件；若 ADS 日期已覆盖但当前 freshness 来源表 etl_time 更新更晚，则继续触发近7天 freshness 重跑
python scheduled_store_daily_report.py

# 门店日报专题调度：跟随主链 v2 / shadow_compare 使用 dws_sales_daily_v2 做 freshness
python scheduled_store_daily_report.py --cutover-mode v2

# 门店日报专题调度：显式覆盖 freshness 来源，不跟随 cutover 自动派生
python scheduled_store_daily_report.py --sales-freshness-source legacy

# 门店日报专题调度：显式指定目标月份
python scheduled_store_daily_report.py --target-month 2026-04

# 门店日报专题调度：显式指定负责人快照日期与工作表
python scheduled_store_daily_report.py --target-month 2026-04 --owner-snapshot-date 2026-04-22 --owner-sheet-name 门店负责人映射模板

# 门店日报专题调度：仅导目标，不执行负责人快照导入
python scheduled_store_daily_report.py --target-month 2026-04 --no-run-owner-import

# 门店日报专题调度：导入成功后只记录受影响日期，不自动批量重跑门店层/主体层/销售看板 ADS
python scheduled_store_daily_report.py --target-month 2026-04 --no-run-affected-ads

# 门店日报专题调度：显式按日期列表批量重跑门店层/主体层/销售看板 ADS
python scheduled_store_daily_report.py --rerun-report-date 2026-04-01 --rerun-report-date 2026-04-02 --rerun-data-version v1

# Windows 包装脚本同样支持透传显式重跑参数
run_scheduled_store_daily_report.bat --rerun-report-date 2026-04-01 --rerun-data-version v1

# 门店日报专题调度：最小单元测试（不写库）
python -m unittest test_scheduled_store_daily_report.py

# 达播统一 Excel 候选集提取：默认扫描 NAS 最新订单管理*.xlsx，不写库
python tools/extract_dabo_order_candidates_from_nas.py --preview-limit 5

# 达播统一 Excel 候选集提取：指定样本并导出 CSV / JSON
python tools/extract_dabo_order_candidates_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx" --report-json reports/dabo_yunque_candidates_summary.json --export-csv reports/dabo_yunque_candidates_selected.csv

# 达播统一 Excel 订单标签导出：指定样本并导出订单级标签 CSV
python tools/extract_dabo_order_candidates_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx" --export-order-label-csv reports/dabo_order_labels.csv

# 达播订单标签：默认 dry-run，不写库
python tools/load_dabo_order_labels_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx" --report-json reports/dabo_order_labels_dry_run.json

# 达播订单标签：用户授权后正式写入 ads_dabo_order_label
python tools/load_dabo_order_labels_from_nas.py --apply --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx"

# 达播订单标签：dry-run / apply 摘要会输出 normalization_status_distribution，用于检查 canonical_system_order_id 归一结果

# 门店经营日报：生成指定日期+版本
python etl_ads_store_daily_report.py --report-date 2026-03-23 --data-version v1

# 门店经营日报统计主体层：在门店层完成后生成指定日期+版本
python etl_ads_store_daily_subject_report.py --report-date 2026-03-23 --data-version v1

# 门店经营日报目标导入约定
# NAS 目录：\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\
# 文件命名：优先使用 YYYYMM考核数据配置表.xlsx，例如 202604考核数据配置表.xlsx；脚本同时兼容历史 YYYY年MM月日目标配置表_vN.xlsx
# 默认工作表：导入模板
# 默认模式：dry-run；若仅部分 store_name 未命中 dim_store，脚本会输出 WARNING、给出候选门店建议并跳过这些门店；若全部门店都未命中，则仍直接失败，避免空覆盖当月配置
# 若工作簿同时提供 统计主体目标 与 门店考核归属，两张 sheet 必须成对出现；只出现一张时脚本直接失败
# 两张共同考核 sheet 都存在但都为空时，表示清空当月共同考核配置
# 若 统计主体目标 / 门店考核归属 与 导入模板 的目标月份或目标版本不一致，脚本直接失败；专题调度会原样输出具体 sheet 名，不继续盲目重试
# 若 NAS 目录中同时存在多个目标月份文件，必须显式传 --target-month YYYY-MM
# 若同一目标月份下同时存在多个版本文件，需改用 --file-path 显式指定具体文件
# 若 UNC 会话因 DNS/凭证变化失效，脚本会先读取 HEFANG_NAS_USERNAME / HEFANG_NAS_PASSWORD 自动重建 \\192.168.0.151\hefang总部 连接
# 若 NAS 凭证变量未配置或配置错误，脚本会直接失败，不继续盲目重试
# 若启用 --sync-store-report-attr，模板还必须提供 门店类型 列
# 门店类型原值会直接写入 report_channel_type，支持 线上小程序 / 直营-奥莱 / 联营-免税 / 联营-奥莱 等细分类
# report_channel_type_group 为派生粗分类；SQL/alter_dim_store_report_attr_add_channel_type_group.sql 已于 2026-04-08 执行，现网表内已同时包含 report_channel_type 与 report_channel_type_group
# dim_store_report_attr 生效日默认沿用目标月内现有最新 effective_start_date；目标月无现存版本时回退到月首
# 若启用 --sync-store-report-attr，首次启用前需先由用户人工执行 SQL/create_store_report_attr_snapshot.sql
# 若启用 --sync-store-report-attr，脚本会先按 target_month + target_version 覆盖 cfg_store_report_attr_snapshot，再按 store_id 将当前有效 dim_store_report_attr 记录分类为未变化 / 变更 / 新增 / 退出；未变化不动，变更关旧开新，新增只开新，退出只关旧
# 当前只会在 cfg_store_report_attr_snapshot 出现重复 store_id，或 dim_store_report_attr 在同一生效日对同店存在多条当前有效记录时直接失败；仅因上一版历史仍有效不再单独拦截
# 若只想先登记快照而不立即同步现网细分类渠道类型，使用 tools/register_store_attr_snapshot.py；当 diff 仍存在时，台账状态会记为 pending_apply，而不是阻断登记
# tools/diff_store_report_attr_snapshot.py 为只读工具，按 store_id 输出未变化 / 变更 / 新增 / 退出四类清单
# scheduled_store_daily_report.py 是门店日报当前正式专题调度入口，默认会同步门店属性快照并承接 dim_store_report_attr；若需仅导入 cfg_store_target_daily，可追加 --no-sync-store-report-attr
# 专题调度会先解析 NAS 最新目标文件；自动模式下仅当 target_month 等于本轮自动 report_date 所在月份时才继续执行，历史或未来月份快照会直接跳过
# 默认 previous-day 模式下，6-1 00:05 会继续接受 2026-05 快照并处理 2026-05-31；若改用 current-day，则要求 target_month 与当天自然月一致
# 通过自动 report_date 月份门禁后，调度会检查 log_store_target_import；若同一 file_md5 + target_month + target_version 已成功导入，则本次直接跳过，不重复写库
# 若仅部分门店未命中 dim_store，专题调度会把这类情况记为 WARNING 并推送企微/总控摘要，列出未命中门店后跳过这些门店，继续执行其余门店的负责人导入、受影响日期判断与三张保留 ADS 重跑
# 当前专题调度会在正式 IMPORTED 且受影响日期非空时，按“门店层 -> 统计主体层 -> 销售看板月度战役”顺序触发批量重跑
# 若只想保留受影响日期集合而不自动重跑，可追加 --no-run-affected-ads
# 命中 --conn-test 时，不触发门店层、统计主体层与销售看板批量重跑
# 若目标链路命中 file_md5 + target_month + target_version 幂等跳过，且负责人链路也没有新增受影响日期，调度会继续检查三张保留 ADS 的 report_date 覆盖；若未补到统一上界，则自动按缺口日期补跑；若已覆盖，则比较近7天 freshness 来源表（默认跟随 cutover_mode：legacy=dws_sales_daily，shadow_compare/v2=dws_sales_daily_v2，也可用 --sales-freshness-source 显式覆盖）与专题 ADS etl_time，源表更新更晚时按 freshness 日期补跑
# 专题调度入口现在会先申请 hefang_dw:scheduled_store_daily_report 单实例锁；若已有另一条专题调度在跑，本次立即退出，不再把外层重试放大成多条并发链路
# 已通过 test_scheduled_store_daily_report.py 覆盖三张保留 ADS 的调用顺序、失败续跑上下文与 DWS freshness 分支；当前 2026-04/v2 文件自动模式会命中既有 SUCCESS，并在必要时继续由 freshness 判定是否重跑
# 若失败属于模板校验类问题，例如共同考核 sheet 成对约束不满足、目标月份/目标版本不一致、缺少 门店类型 列，专题调度会直接停止并输出可读错误，不进入重试等待
# 达播统一 Excel 工具默认只抽取候选集和订单标签，不做数据库写入；用于先稳定 hefang_dw 内部输入契约
# ads_dabo_order_label 是 hefang_dw 内部新的订单标签表：1 行 = 1 个 system_order_id 在某个 source_file 下的达播标签快照
# 表内保留原始 system_order_id，同时在异常组合单上补 canonical_system_order_id；后续 SQL 优先按 COALESCE(canonical_system_order_id, system_order_id) = ods_m_retail.oms_sourcecode 做筛选
# run_etl.py 的 dabo_ready 现优先检查 ads_dabo_order_label 最新批次是否存在且最近 1 天有更新，并附带输出 ads_dabo_daily_sales 当日状态
# ads_inventory_health 现优先使用最新标签批次 + ODS/缓存兜底计算达播字段；仅在标签批次不可用时回退 legacy CSV，否则达播字段按0处理
# 若刚发生手工重跑或调度重叠，先等待前一轮完成再重算，避免锁冲突
# dws_sales / dws_inventory / ads_health 现在都内置命名锁与死锁重试，但仍不建议并发重复触发
# 门店日报脚本当前独立运行，适合在配置表确认后按需手工触发；若目标库尚未补齐负责人字段，先执行 SQL/alter_ads_store_daily_report_add_owner_name.sql
# 若需要共同考核统计主体层，请在门店层后追加执行 etl_ads_store_daily_subject_report.py
# tools/import_store_operation_owner_from_nas.py 负责从 NAS 当前快照导入门店经营负责人，并在 MySQL 内维护 cfg_store_operation_owner_snapshot 当前快照、dim_store_operation_owner_assignment SCD2 历史与 log_store_operation_owner_import 导入日志
# 默认文件为 \\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx，兼容 门店负责人映射表 / 门店负责人映射模板 工作表；必填表头为 门店编码 / 门店名称 / 负责人
# 若专题调度未显式传 --owner-snapshot-date，负责人快照日默认跟随本轮专题实际处理的 report_date 上界；例如 6-1 00:05 的 previous-day 模式会默认使用 2026-05-31，而不是 2026-06-01
# 脚本会按 snapshot_date 从 dim_store_report_attr、cfg_store_assessment_assignment、cfg_store_assessment_subject_target_daily 推导当日应维护的经营实体；若经营体已存在，则快照中只能保留 SUBJECT 行，不允许再保留被吸收的 RT 成员门店
# 当前链路已接入 scheduled_store_daily_report.py：目标导入完成后会继续执行负责人导入，并按 file_md5 + snapshot_date 做独立幂等判重；可用 --no-run-owner-import 临时关闭
# 负责人链路只有发生 changed/new/exited 历史变更时，才会把受影响日期并入专题调度；起点会截断到当前 target_month 月初，避免跨月误刷
```

补充说明：`etl_dws_sales.run()` 仍保留“凌晨查昨天、白天查今天”的独立智能模式，但 `run_etl.py` 自 2026-04-23 起已把主链窗口固定对齐到 ODS 默认 7 天回刷，即先执行 `run_ods_sync(backfill_days=7)`，再执行 `etl_dws_sales.run(days_back=7, include_today=True)`。若要复现本轮主链行为，应优先使用上面的 7 天示例。来源：[run_etl.py](../run_etl.py#L59)；[run_etl.py](../run_etl.py#L526)；[run_etl.py](../run_etl.py#L544)；[etl_dws_sales.py](../etl_dws_sales.py#L178)

### 2.4 数据质检

```powershell
# 通用质检
python tools/check_data.py

# 库存质检
python tools/check_dws_inventory.py

# ODS 增量对账（检查水位与行数）
python tools/check_ods_incremental.py

# ODS 明细质检（线上/线下拆分）
python tools/check_ods_retailitem_quality.py

# ODS 对账：指定截止时间（as-of）
python tools/check_ods_incremental.py --as-of "2026-03-01 08:00:00"
```

### 2.5 快照与导出

```powershell
# 生成 MySQL 数仓 Schema 快照
python tools/snapshot_mysql_hefangdw_schema.py

# 生成 Oracle ERP Schema 快照
python tools/snapshot_oracle_bosnds3_schema.py

# 导出 ADS 层数据到 Excel
python tools/export_ads.py
```

### 2.6 MCP 与只读查数

推荐顺序：MCP 优先，只读执行；若本地未配置 MCP 或连通失败，再降级到 Python 工具。

```powershell
# 查看内置查询模板
python tools/query_data.py --list-templates

# MySQL：最近 7 天销售排行
python tools/query_data.py --template mysql_sales_rank_7d

# Oracle：最近 7 天零售单据统计
python tools/query_data.py --source oracle --template oracle_retail_docs_7d

# 自由查数并导出 JSON
python tools/query_data.py --sql "SELECT snapshot_date, product_code, total_qty FROM ads_inventory_health WHERE snapshot_date = :dt" --param dt=20260318 --output json --output-path reports/query_result_ads_sample.json

# 导出指定快照的 ADS 数据
python tools/export_ads.py --snapshot-date 20260318 --output reports/output.xlsx
```

若需要对 `hfsy` 做一次性只读探查，可在独立终端临时覆盖 `MYSQL_*` 后执行：

```powershell
$env:MYSQL_HOST='8.134.87.152'
$env:MYSQL_PORT='33066'
$env:MYSQL_USER='shuyun668'
$env:MYSQL_PASSWORD='请通过本地安全方式注入真实密码'
$env:MYSQL_DB='hfsy'
python tools/query_data.py --source mysql --sql "SELECT COUNT(*) AS row_cnt FROM t_member_info"
```

结构快照命令：

```powershell
python tools/snapshot_mysql_hefangdw_schema.py --output reports/snapshot_mysql_hefangdw_schema.json
python tools/snapshot_oracle_bosnds3_schema.py --output reports/snapshot_oracle_bosnds3_schema.json
```

MCP 推荐配置片段：

```json
{
    "mcpServers": {
        "mysql": {
            "command": "npx",
            "args": [
                "-y",
                "@benborla29/mcp-server-mysql"
            ],
            "env": {
                "MYSQL_HOST": "${MYSQL_HOST}",
                "MYSQL_PORT": "${MYSQL_PORT}",
                "MYSQL_USER": "${MYSQL_USER}",
                "MYSQL_PASS": "${MYSQL_PASSWORD}",
                "MYSQL_DB": "${MYSQL_DB}"
            }
        },
        "oracle": {
            "command": "uvx",
            "args": [
                "mcp-server-oracle"
            ],
            "env": {
                "ORACLE_CONNECTION_STRING": "${ORACLE_CONNECTION_STRING}",
                "ORACLE_SCHEMA": "BOSNDS3"
            }
        }
    }
}
```

说明：
- 当前 VS Code / Copilot 会话优先读取工作区 `.vscode/mcp.json` 与用户级 `mcp.json`；仓库根 `.mcp.json` 更适合作为 Claude/OpenCode 的兼容或本地参考配置。
- MCP 更适合交互式查数与结构探查；`tools/query_data.py` 适合作为稳定兜底和导出工具。
- 快照脚本只输出结构信息，不读取业务数据值。

### 2.6.1 ODS 高频查询接入前检查

新增 ODS 表，或首次让 ODS 直接承接 MCP 查数、排障 SQL、DWS 联表时，除字段与水位外还必须立刻检查：

1. 业务 `id` 在 MySQL 是否真的可做 `PRIMARY KEY` / `UNIQUE`；若发现重复，先确认是否为跨 `etl_batch_id` 的重复装载。
2. 目标查询是否已具备“头表过滤索引 + 明细连接索引”两类路径索引；不要只保留 `modifieddate` / `settime` 这类同步索引。
3. 建好索引后，用 `EXPLAIN ANALYZE` 固化 `r_rows`、`join_rows` 与实际耗时三项基线，再判断是否还有排序或 MCP 链路问题。
4. 若现网已经存在历史重复，先由用户手工执行 `SQL/alter_ods_m_retail_enforce_unique_id.sql` 与 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql`，再用 `python tools/check_ods_incremental.py` 复核 `duplicate_id_count` 是否归零。

`ods_m_retail` / `ods_m_retailitem` 的经验表明，缺少上述检查时，约 309 万行明细就可能退化为明细全表扫描；补齐路径索引后，同一路径可回到约 504ms 量级。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L46-L64)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L243-L331)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L47-L65)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L293-L423)；[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

### 2.7 文档审计

```powershell
# 检查代码与文档是否同步
python scripts/check_doc_sync.py
```

### 2.8 经验台帐

当一次排障形成可复用结论，或用户明确纠正业务逻辑/字段语义/SQL 口径时，追加一条经验记录：

```powershell
python scripts/log_agent_lesson.py --source task --category field-mapping --trigger "Oracle 查询字段报错" --mistake "误以为 M_PRODUCT 存在 NAME_CN" --correction "以 etl_dim_product.py 为准：NAME=product_code，VALUE=product_name" --evidence "etl_dim_product.py#L33" "etl_dim_product.py#L34" --prevention "涉及源表字段时，先对照 ETL 抽取 SQL、快照或字段映射文档"

python scripts/log_agent_lesson.py --source user-feedback --category business-rule --trigger "用户指出销售口径错误" --mistake "误把业务常量或字段语义当作既定事实" --correction "以用户确认后的业务结论为准，并同步相关文档" --evidence "docs/业务逻辑与指标规范.md#L1" --prevention "涉及业务口径变更前先确认，不凭历史经验直接改"
```

说明：
- 经验台帐文件是 `docs/AGENT_LESSONS.md`。
- 与当前仓库强相关的经验，除落盘台帐外，还应同步到 repo memory。
- `.claude/settings.json` 已增加复盘提醒 Hook，但当前仓库内没有对 GitHub Copilot 会话结束的硬触发钩子，因此仍需在任务收尾时主动判断是否要记账。

### 2.9 验收测试

```powershell
# 完整自动化验收测试（需要数据库已有数据）
python test_etl_automation.py

# Oracle 对账重点：dws_inventory / dws_sales / ads_health 默认按 0.5% 误差阈值输出结果

# 通过 pytest 运行
pytest test_etl_automation.py -v
```

---

## 3. 日志说明

| 日志文件 | 内容 | 保留策略 |
|---------|------|---------|
| `logs/etl_<日期>.log` | 主 ETL 流水线执行日志 | 不 git 追踪，本地保留 |
| `logs/ods_qc_<日期时间>.log` | ODS 质检日志 | 不 git 追踪，本地保留 |
| `logs/conn_test_<日期>.log` | 连通测试日志 | 不 git 追踪，本地保留 |

查看最新日志：
```powershell
# PowerShell 查看最新 ETL 日志（最后 100 行）
Get-Content (Get-ChildItem logs/etl_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName -Tail 100
```

---

## 4. 常见报错与处理

### 4.1 Oracle 连接失败

**报错**：`ORA-01017: invalid username/password`
```powershell
# 原因：环境变量未配置或密码错误
# 排查
[System.Environment]::GetEnvironmentVariable('ORACLE_USER', 'User')
[System.Environment]::GetEnvironmentVariable('ORACLE_PASSWORD', 'User')
# 处理：重新设置环境变量，重启终端后测试
python tools/test_connection.py
```

**报错**：`DPY-6001: cannot connect to database` / `Connection refused`
```powershell
# 原因：VPN 未连接或 Oracle 主机/端口错误
# 排查
Test-NetConnection -ComputerName $env:ORACLE_HOST -Port $env:ORACLE_PORT
```

### 4.2 MySQL 连接失败

**报错**：`Access denied for user`
```powershell
# 排查：确认 MYSQL_USER / MYSQL_PASSWORD 环境变量
python -c "import os; print(os.getenv('MYSQL_USER'), os.getenv('MYSQL_HOST'))"
```

**报错**：`Unknown database 'hefang_dw'`
```sql
-- MySQL 中手动创建数据库
CREATE DATABASE hefang_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- 然后执行建表脚本
-- MySQL Workbench 或命令行运行 SQL/create_ods_tables.sql
```

### 4.3 ODS 水位异常

**现象**：ODS 增量对账报告行数差异 > 阈值
```powershell
# 1. 查看质检日志
Get-Content logs/ods_qc_*.log -Tail 50

# 2. 手动对账（指定截止时间）
python tools/check_ods_incremental.py --as-of "2026-03-01 08:00:00"

# 3. 如确认需要重刷，执行全量
python run_ods.py --full
```

### 4.4 ADS 库存健康度异常

**现象**：`ads_inventory_health` 行数骤降或 SKU 缺失
```powershell
# 1. 检查 dws_inventory 是否正常
python tools/check_dws_inventory.py

# 2. 检查 dim_product / dim_sku 维度是否有数据
python -c "
import pymysql, os
conn = pymysql.connect(host=os.getenv('MYSQL_HOST'), user=os.getenv('MYSQL_USER'),
    password=os.getenv('MYSQL_PASSWORD'), database=os.getenv('MYSQL_DB','hefang_dw'))
c = conn.cursor()
c.execute('SELECT COUNT(*) FROM dim_product')
print('dim_product:', c.fetchone()[0])
c.execute('SELECT COUNT(*) FROM dim_sku')
print('dim_sku:', c.fetchone()[0])
conn.close()
"

# 3. 强制重算 ADS
python -c "import etl_ads_health; etl_ads_health.run()"

# 4. 如果日志出现 1213 / 1205，先确认是否有别的 ETL 会话仍在跑
python -c "import pymysql, os; conn=pymysql.connect(host=os.getenv('MYSQL_HOST'), user=os.getenv('MYSQL_USER'), password=os.getenv('MYSQL_PASSWORD'), database=os.getenv('MYSQL_DB','hefang_dw')); c=conn.cursor(); c.execute('SHOW FULL PROCESSLIST'); [print(r) for r in c.fetchall()]; conn.close()"
```

补充说明：
- `etl_dws_inventory.py` 现已在写入 `dws_inventory_daily` 前申请命名锁，并对死锁/锁等待超时做最多 3 次退避重试。
- `etl_dws_sales.py` 现已在覆盖写入 `dws_sales_daily` 前申请命名锁，并对死锁/锁等待超时做最多 3 次退避重试。
- `etl_ads_health.py` 现已将“删除当天数据 + 插入新结果”放入同一事务；若插入失败，不会留下当天 ADS 被清空的中间态。
- `run_etl.py` 现已在 `dws_sales` 或 `dws_inventory` 未成功时跳过 `ads_health`，避免下游继续放大异常。
- `test_etl_automation.py` 现已对 `dws_inventory`、`dws_sales`、`ads_health` 输出 Oracle 对账百分比；2026-03-23 复测结果分别为 0.00%、0.39%/0.11%/0.07%、0.00%。

### 4.5 企业微信告警不通

**现象**：ETL 完成但未收到消息
```powershell
# 检查 Webhook 是否配置
[System.Environment]::GetEnvironmentVariable('WECHAT_WEBHOOK', 'User')

# 手动测试（PowerShell）
$body = '{"msgtype":"text","text":{"content":"测试消息"}}'
Invoke-RestMethod -Method Post -Uri $env:WECHAT_WEBHOOK -Body $body -ContentType 'application/json'
```

### 4.6 Python 编码问题

**现象**：日志或终端输出乱码
```powershell
# 临时修复（当前会话）
$env:PYTHONIOENCODING = 'utf-8'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 永久修复
[Environment]::SetEnvironmentVariable('PYTHONIOENCODING', 'utf-8', 'User')
```

---

## 5. 数据库初始化（首次部署）

```sql
-- Step 1: 创建数据库
CREATE DATABASE hefang_dw CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Step 2: 执行 ODS 建表
SOURCE SQL/create_ods_tables.sql;

-- Step 3: 执行结构变更（按文件名顺序）
SOURCE SQL/alter_ods_incremental.sql;
-- 其余 alter_*.sql 按实际情况执行

-- Step 3a: （可选）负责人快照链路建表
SOURCE SQL/create_store_operation_owner_tables.sql;

-- Step 4: 首次全量同步 ODS
-- python run_ods.py --full

-- Step 5: 执行主 ETL 流水线
-- python run_etl.py

-- Step 6: （可选）执行门店经营日报专项 ETL
-- python etl_ads_store_daily_report.py --report-date 2026-03-23 --data-version v1
-- python etl_ads_store_daily_subject_report.py --report-date 2026-03-23 --data-version v1
```

---

## 6. 验收步骤（上线前 / 每次重大变更后）

```powershell
# Step 1: 环境自检
pwsh scripts/doctor.ps1

# Step 2: 连通测试
python tools/test_connection.py

# Step 3: ETL 连通模式（不执行真实 ETL）
$env:ETL_CONN_TEST=1; $env:ETL_MAX_RETRIES=1; python run_etl.py --conn-test

# Step 4: ODS 质检
python tools/check_ods_incremental.py
python tools/check_ods_retailitem_quality.py

# Step 5: 数据质检
python tools/check_data.py

# Step 6: 自动化验收测试
python test_etl_automation.py

# Step 7: 文档同步审计
python scripts/check_doc_sync.py

# Step 8: （可选）ODS 增量对账
python tools/check_ods_incremental.py

# Step 9: （可选）门店日报依赖检查 / 单次生成
python etl_ads_store_daily_report.py --conn-test
python etl_ads_store_daily_subject_report.py --conn-test

# Step 10: （可选）门店日报专题调度 dry-run / 正式调度
python scheduled_store_daily_report.py --conn-test
python scheduled_store_daily_report.py

# Step 10b: （可选）主链+销售专题总控调度
python scheduled_total_control.py --conn-test
python scheduled_total_control.py
python scheduled_total_control.py --cutover-mode shadow_compare
python scheduled_total_control.py --cutover-mode v2
python scheduled_total_control.py --cutover-mode v2 --rollback-to-legacy
python scheduled_total_control.py --shadow-only
.\run_scheduled_total_control.bat
.\run_scheduled_total_control.bat --cutover-mode v2
.\run_scheduled_total_control_v2.bat
.\run_scheduled_total_control_v2.bat --rollback-to-legacy

# Step 10c: （可选）DWS v2 shadow 独立调度
python scheduled_dws_v2_shadow.py --conn-test
python scheduled_dws_v2_shadow.py
python scheduled_dws_v2_shadow.py --inventory-align-with-old-dws
python scheduled_dws_v2_shadow.py --inventory-source-loaded-at-cutoff "2026-05-12 09:38:18"
.\run_scheduled_dws_v2_shadow.bat

总控模式说明：主链 `run_etl.py`、门店销售专题 `scheduled_store_daily_report.py` 与 DWS v2 读源刷新 / shadow 的子链企业微信摘要会被抑制，最终只由 `scheduled_total_control.py` 统一发送一条汇总消息；若显式传入 `--cutover-mode` / `--rollback-to-legacy`，总控会把同一组参数透传给主链与门店专题。有效模式为 `v2` 且不是 `--conn-test` / `--shadow-only` 时，总控会先执行阻断型 `DWS v2 读源预刷新`，并向 `scheduled_dws_v2_shadow.py` 追加 `--skip-ads-shadow-validation`，用于先刷新 `dws_sales_daily_v2` / `dws_inventory_daily_v2` 再让主链计算 `ads_inventory_health`；该预刷新失败时主链和专题链都会跳过。若 Windows 计划任务入口已经整体切到 V2 wrapper，则同日 old `dws_inventory_daily` 还没产出属于 pre-refresh 时序内的预期现象，此时 inventory old DWS 基线会记为 `SKIPPED`，只要 `dwd_inventory_storage_snapshot -> dws_inventory_daily_v2` 自洽通过，就不再因 `old_dws_max_etl_time=None` 阻断主链；但显式 same-snapshot 诊断仍保持阻断。非 V2 模式仍保持后置 `dws_v2_shadow` 非阻断观察语义；专题失败不阻断 shadow，shadow 失败只记 WARNING。来源：[scheduled_total_control.py](../scheduled_total_control.py#L125)；[scheduled_total_control.py](../scheduled_total_control.py#L149)；[scheduled_total_control.py](../scheduled_total_control.py#L503)；[scheduled_total_control.py](../scheduled_total_control.py#L654)；[scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L419)；[scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L500)；[scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L1093)

Windows wrapper 说明：`run_scheduled_total_control.bat` 会把追加参数原样透传给 `scheduled_total_control.py`，因此可用于 `--cutover-mode v2`、`--rollback-to-legacy`、`--conn-test` 等显式模式；`run_scheduled_total_control_v2.bat` 等价于预置 `python scheduled_total_control.py --cutover-mode v2`，用于减少人工双跑验收时误跑默认 legacy 的风险。来源：[run_scheduled_total_control.bat](../run_scheduled_total_control.bat)；[run_scheduled_total_control_v2.bat](../run_scheduled_total_control_v2.bat)

激进 V2 双跑验收建议：若生产环境允许按用户决策跳过 3 到 7 天非阻断 shadow 累计证据，则由用户手工连续执行两轮 `python scheduled_total_control.py --cutover-mode v2` 或 `run_scheduled_total_control_v2.bat`，每轮至少保留退出码、总控摘要、`dws_v2_shadow` 非阻断表现、运行耗时 / 锁证据和 `reports/context_cache/` 下的运行 JSON。若需立即回退，执行 `python scheduled_total_control.py --cutover-mode v2 --rollback-to-legacy` 或 `run_scheduled_total_control_v2.bat --rollback-to-legacy`；该回退路径只切回 legacy 读源，不删除、不改名 ADS 字段。

shadow 调度阶段键说明：`scheduled_dws_v2_shadow.py` 的运行日志、总控结构化摘要与 JSON 证据会复用一组固定阶段键，便于排查失败位置与确认写入边界。

- ODS 连通检查：`ods_retail_conn_test`、`ods_retailitem_conn_test`、`ods_fa_storage_conn_test`
- ODS 执行阶段：`ods_retail_execute_load`、`ods_retailitem_execute_load`、`ods_fa_storage_execute_load`
- DWD 连通检查：`dwd_sales_conn_test`、`dwd_inventory_conn_test`
- DWD 执行阶段：`dwd_sales_execute_load`、`dwd_inventory_execute_load`
- DWS v2 连通检查：`dws_sales_v2_conn_test`、`dws_inventory_v2_conn_test`
- DWS v2 执行阶段：`dws_sales_v2_execute_load`、`dws_inventory_v2_execute_load`
- 关键运行字段：`dws_sales_v2`、`dws_inventory_v2`、`dws_sales_mainline_days_back`、`dwd_mismatch_count`、`dws_sales_write_confirmation_token`、`dws_inventory_write_confirmation_token`

# Step 11: （可选）负责人快照链路 dry-run / 最小单测
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --preview-limit 10
python -m unittest test_store_operation_owner_import.py
```

**所有步骤无红色错误 = 可以上线**。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v2.44 | 2026-06-08 | 新增 `run_scheduled_store_daily_report.bat` 参数透传说明，可直接转发 `--rerun-report-date` / `--rerun-data-version` 做显式重跑 |
| v2.43 | 2026-05-14 | 补记 V2 pre-refresh 若 same-day old DWS 快照尚未产出时的运行语义：inventory old DWS 基线记为 `SKIPPED`，改由 DWD→v2 自洽结果决定是否放行主链 |
| v2.42 | 2026-05-13 | 补记总控 V2 模式会先执行阻断型 DWS v2 读源预刷新，再跑主链；预刷新失败时跳过主链以避免 ADS 读空 `_v2` 源 |
| v2.41 | 2026-05-12 | 新增总控 Windows wrapper 参数透传和显式 V2 wrapper 运行说明，并补记用户决策下的两次总控 V2 验收与 rollback 命令 |
| v2.40 | 2026-05-12 | 新增主链 / 总控 / 门店专题的 cutover / rollback 运行命令，并将专题 freshness 来源说明更新为 legacy / v2 可派生或显式覆盖 |
| v2.39 | 2026-05-07 | 新增 `scheduled_dws_v2_shadow.py` / `run_scheduled_dws_v2_shadow.bat` / `scheduled_total_control.py --shadow-only` 运行命令，并补记总控第三子链的非阻断语义 |
| v2.38 | 2026-05-07 | 新增 DWS v2 S3 手工写入命令说明；默认 dry-run，写入需确认令牌、命名锁、事务和 JSON 证据，仍不接总控 |
| v2.38 | 2026-06-06 | 退役 3 张销售专题 ADS，并将专题调度运行说明收口到当前保留链路 |
| v2.37 | 2026-05-07 | 新增 DWS v2 销售 / 库存 dry-run 与 conn-test 运行命令，明确当前无写库入口 |
| v2.36 | 2026-05-06 | 补记门店属性同步改为先写 cfg_store_report_attr_snapshot，再同步 dim_store_report_attr，并新增首次启用建表前置 |
| v2.35 | 2026-05-06 | 将门店未命中 dim_store 的处理更新为 WARNING + 跳过坏门店，并补记全量未命中时仍立即失败 |
| v2.34 | 2026-04-27 | 新增 hefang_dw 统一数据库连接工厂说明，并补充连接池与超时环境变量 |
| v2.33 | 2026-04-27 | 将门店日报专题调度运行说明更新为完整销售专题 ADS 链，并补充 DWS freshness 重跑规则 |
| v2.32 | 2026-04-27 | 补记 scheduled_total_control.py 已统一汇总主链与门店销售专题链的企业微信摘要，并预留后续专题接入方式 |
| v2.31 | 2026-04-24 | 新增 scheduled_total_control.py 与 run_scheduled_total_control.bat，用于主链成功后再触发销售专题链 |
| v2.30 | 2026-04-23 | 补记门店日报专题调度新增单实例锁，并说明销售主题 ADS 命名锁现为事务后显式释放 |
| v2.29 | 2026-04-23 | 补记 run_etl.py 已将 dws_sales 主链窗口固定为近7天回带，并更新单步调试示例 |
| v2.28 | 2026-04-23 | 补记销售专题 SKU 层连带贡献精度放宽前置说明，并同步 2026-04-22/v2 五层调度结果 |
| v2.27 | 2026-04-22 | 补记负责人快照已接入专题调度，并新增 ads_store_daily_report 负责人字段的执行前置说明 |
| v2.26 | 2026-04-21 | 新增门店经营负责人快照导入命令、建表步骤与最小验证说明 |
| v2.25 | 2026-04-17 | 补记销售专题 SKU 层已完成专题调度第五层显式重跑验证，并更新五层写库结果 |
| v2.24 | 2026-04-17 | 将销售专题 SKU 层接入专题调度第五层，并补记当前仅完成代码接链与单元测试验证 |
| v2.23 | 2026-04-16 | 补充销售专题月级组织层与 SKU 层的独立运行命令，并注明当前仅完成 conn-test 验证 |
| v2.22 | 2026-04-16 | 将销售专题组织日层接入专题调度第四层批量重跑，并补记四层实跑验证结果 |
| v2.21 | 2026-04-16 | 修正门店日报目标 NAS 根目录，并同步专题调度自动跳过与显式 rerun 写库验证说明 |
| v2.20 | 2026-04-16 | 将 ads_daily_sales 纳入专题调度三层批量重跑，并补充最小单元测试命令与验证边界 |
| v2.19 | 2026-04-15 | 补充专题调度对目标月份/目标版本不一致与模板校验失败的立即失败、不重试说明 |
| v2.18 | 2026-04-10 | 更新门店日报目标 NAS 命名约定为 YYYYMM考核数据配置表.xlsx，并注明脚本兼容历史旧文件名 |
| v2.17 | 2026-04-10 | 新增门店日报统计主体层运行命令，并补充共同考核多 sheet 与专题调度双层重跑说明 |
| v2.16 | 2026-04-09 | 更新 ads_inventory_health 的达播来源说明为标签主线优先、legacy 回退兜底 |
| v2.15 | 2026-04-09 | 补充 run_etl.py 的 dabo_ready 已切换为达播标签主线优先检查，并明确 legacy CSV 仅用于 ads_health 兼容回填 |
| v2.14 | 2026-04-09 | 补充 ads_dabo_order_label 的 canonical_system_order_id 归一说明，并更新 dry-run / apply 摘要解释 |
| v2.13 | 2026-04-09 | 明确专题调度只自动处理当前月份快照，并将门店属性同步语义更新为未变化/变更/新增/退出分类 |
| v2.12 | 2026-04-08 | 新增门店日报专题调度入口与按 MD5 跳过重复导入的运行说明 |
| v2.11 | 2026-04-08 | 新增门店属性快照登记命令，并补充 pending_apply 登记语义 |
| v2.10 | 2026-04-08 | 更新门店日报渠道粗分类生成列为现网已执行状态，并补充查看命令说明 |
| v2.9 | 2026-04-08 | 补充门店日报渠道细分类真值与 report_channel_type_group 生成列执行说明 |
| v2.8 | 2026-04-08 | 新增门店属性只读差异清单工具，用于 April 关口开口前识别四类差异 |
| v2.7 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充自动选档规则 |
| v2.6 | 2026-04-08 | 补充门店日报目标导入在多月份文件下使用 --target-month 的运行说明 |
| v2.5 | 2026-04-07 | 补充 run_ods --full 默认执行固定 as-of recent catch-up 与 --full-catchup-days 参数说明 |
| v2.4 | 2026-04-03 | 补充门店日报目标导入支持同步 dim_store_report_attr 的命令、生效日策略与模板约束 |
| v2.5 | 2026-04-14 | 补充 NAS 自动鉴权环境变量与 UNC 会话自动恢复说明 |
| v2.3 | 2026-04-03 | 更新门店日报目标导入命令说明为现网已建表并完成首轮 apply 验证 |
| v1.7 | 2026-03-23 | 补充 dws_sales 命名锁重试与 Oracle 对账 0.5% 阈值说明 |
| v1.6 | 2026-03-23 | 补充库存/ADS 命名锁重试、ADS 单事务覆盖与 1213/1205 排查说明 |
| v1.5 | 2026-03-20 | 补充 hfsy 源端连接事实、临时环境变量约定与只读探查示例 |
| v1.0 | 2026-03-18 | 初版运行手册 |
| v1.1 | 2026-03-18 | 新增 MCP 与只读查数说明、结构快照与导出命令 |
| v1.2 | 2026-03-18 | 新增经验台帐写入命令、复盘规则与 Hook 说明 |
| v1.3 | 2026-03-18 | 将 MCP 配置示例对齐为当前实际使用的 mcpServers 格式 |
| v1.4 | 2026-03-18 | 将查数与导出示例输出名改为通用占位，避免审计高风险误报 |
| v1.8 | 2026-04-02 | 新增 ODS 高频查询接入前检查，强调主键可行性与查询路径索引评审 |
| v1.9 | 2026-04-02 | 补充现网唯一键治理脚本与 duplicate_id_count 复核方式 |
| v2.2 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本的 dry-run / apply 命令与失败处理说明 |
| v2.1 | 2026-04-03 | 补充门店日报目标 NAS 目录与固定文件命名约定 |
| v2.0 | 2026-04-03 | 新增门店经营日报独立 ETL 入口的运行与验收命令 |
