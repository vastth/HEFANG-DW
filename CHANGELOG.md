# 更新日志（CHANGELOG）

> 说明：按日期与版本整理，条目按“Added / Changed / Fixed / Database / Docs”分类。

## 2026-07-13

### v0.8.82 — 门店日报同店资格切换为开业日期（2026-07-13）

#### Changed
- `etl_dim_store.py` 从 Oracle `C_STORE.OPENDATE` 安全抽取 `open_date`；源端空值或无法转换的日期统一落 NULL，并保留源端质量日志。
- `etl_ads_store_daily_report.py` 的同店资格改为源物理门店 `open_date <= 去年同期月份第一天`。完整源门店集合不再由任一侧销售事实驱动，本期或去年同期为 0 的合格门店仍参与辅助金额聚合。
- 空 `open_date` 判为非同店并记录日报 DQ 告警，不再回退“去年同期销售额大于 0”的旧规则；快闪排除、月中快闪合并的去年同期分母截断继续保留。

#### Added
- 新增 `SQL/alter_dim_store_add_open_date.sql`、`SQL/check_dim_store_open_date.sql` 与 `SQL/rollback_dim_store_drop_open_date.sql`，仅供用户人工执行、检查或在确认后回滚结构。
- `test_dim_store.py` 与 `test_ads_store_daily_report.py` 覆盖安全日期转换、目标缺列保护、旧版命名列插入兼容性，以及同店双侧零销售保留语义。

#### Docs
- 同步 `dim_store` 数据字典、数据契约、字段映射、ETL 说明、业务指标规范和销售部日报冻结稿。

#### Validation
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest -v test_ads_store_daily_report.py test_dim_store.py`，17 项测试通过。
- 已只读验证 Oracle 安全转换：`C_STORE` 共 231 行，`OPENDATE` 原始空值 136 行、不可转换日期 0 行。
- 用户已人工执行 `SQL/alter_dim_store_add_open_date.sql` 并刷新 `dim_store`；只读复核为 231 行、95 行有效日期、136 行不可用日期，日期范围 2018-07-25 至 2026-07-05，异常范围日期 0 行。
- 已执行 `etl_ads_store_daily_report.py --conn-test`，连接与依赖检查通过；尚未回刷 ADS 或修改 Tableau 工作簿。

## 2026-06-25

### v0.8.81 — 门店日报同店同比分母按快闪合并日前截断（2026-06-25）

#### Fixed
- 修复 `etl_ads_store_daily_report.py` 在共同考核主体门店于月中吸收 `快闪` 成员后，`same_store_last_year_mtd_sales_amt` 仍直接累计到 `report_date` 对应去年同日的问题。当前当报告月存在“月中生效”的 `快闪` 合并时，去年同期分母会截到最早 `快闪` 生效日前一天的去年同日，避免主体门店在合并后已无 ERP 销售时继续放大去年同期分母。

#### Added
- `test_ads_store_daily_report.py` 新增 SQL 生成回归断言，锁定 `flash_merge_cutoff_scope`、`effective_start_date` 透传以及去年同期截断条件必须存在。

#### Docs
- 更新 `docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md` 与 `docs/AGENT_LESSONS.md`，同步门店日报同店同比在“月中快闪合并”场景下的去年同期截断口径。

#### Validation
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest -v test_ads_store_daily_report.py`，10 项测试全部通过。
- 已执行 `D:/Anaconda/envs/pyproject/python.exe scripts/build_agent_lessons_index.py`，刷新 `docs/AGENT_LESSONS_INDEX.md`。
- 已执行 `D:/Anaconda/envs/pyproject/python.exe scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。

## 2026-06-22

### v0.8.80 — 修复 ads_daily_sales 漏计共同考核快闪源门店（2026-06-22）

#### Fixed
- 修复 `etl_ads_daily_sales.py` 的源门店作用域只取 `cfg_store_target_daily` 目标门店、未把共同考核 `快闪` 成员门店纳入 `store_entity_map` 的问题。当前已补齐 `joint_assessment_member_scope`、`joint_assessment_anchor_scope`、`store_attr_scope` 与 `source_store_scope`，使没有单店目标的快闪成员门店也会把真实流水并入对应经营体。
- 同步去掉 `assignment_candidates` 对 `target_store_scope` 的硬限制，避免 `RT014`、`RT140` 这类仅作为共同考核成员存在的门店在 `ads_daily_sales` 中丢失映射。
- `ads_daily_sales` 完成日志中的门店口径改为“生效源门店”，避免把共同考核成员门店误记为“生效目标门店”。

#### Added
- `test_ads_sales_scope_alignment.py` 新增共同考核 source scope 回归断言，锁定 `ads_daily_sales` 必须同时包含 `joint_assessment_member_scope`、`joint_assessment_anchor_scope` 与 `source_store_scope`。

#### Docs
- 更新 `docs/AGENT_LESSONS.md` 与 `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md`，沉淀“趋势图少快闪”问题的根因、验证证据与后续预防动作。

#### Validation
- 已执行 `python -m unittest -v test_ads_sales_scope_alignment.py`。
- 已用只读 SQL 核对 `2026-06-16` 到 `2026-06-21`：`ads_daily_sales` 与 KPI 缺口分别为 `12349 / 19507 / 30517 / 68475 / 36592 / 45303`，逐日精确等于 `RT014` 与 `RT140` 快闪源门店真实流水。

## 2026-06-01

### v0.8.79 — 放宽负责人共同考核月内过渡期校验（2026-06-18）

#### Changed
- 更新 `tools/import_store_operation_owner_from_nas.py`：负责人导入除按 `snapshot_date` 读取当日共同考核外，额外读取当月共同考核关系，用于识别月内生效切换过渡。
- `STORE` 被共同考核 `SUBJECT` 吸收时，若负责人文件在同月过渡期内同时保留成员 `STORE` 行与对应 `SUBJECT` 行，或在正式生效日前已提前维护 `SUBJECT` 且成员 `STORE` 仍保留，脚本会将该实体降级为 warning，不再以 `unexpected_entities` 直接阻断 `--apply`。
- 导入摘要与日志状态扩展为 `PASSED / WARNING / FAILED` 三态，并新增 `tolerated_transition_entities`、`warning_messages` 供总控链路透出提示。

#### Added
- `test_store_operation_owner_import.py` 新增共同考核过渡期回归：覆盖“`STORE + SUBJECT` 并存仅告警”和“生效日前提前维护 `SUBJECT` 且 `STORE` 仍保留仅告警”两类场景。

#### Docs
- 更新 `README.md`、`docs/DATA_CONTRACTS.md` 与 `docs/ETL业务逻辑说明.md`，同步负责人共同考核由“只能保留 `SUBJECT`”调整为“推荐保留 `SUBJECT`，同月过渡允许 `STORE + SUBJECT` 并存且仅告警”。

#### Validation
- 已执行 `python -m unittest -v test_store_operation_owner_import.py`。
- 已对当前 NAS `门店负责人映射表.xlsx` 做只读 dry-run 验证：`snapshot_date=2026-06-18` 时仍因缺少 `RT045` 且仅提前维护 `SUBJ_GZTH` 失败；`snapshot_date=2026-06-19` 时同一文件已通过，验证与共同考核生效切换一致。

### v0.8.78 — 纠正门店考核归属门店ID字段语义（2026-06-18）

#### Fixed
- 修正 `tools/import_cfg_store_target_daily_from_nas.py` 对 `门店考核归属` sheet 中 `门店ID` 的误判：该列业务实际填写的是 RT 门店编码，如 `RT050`，不应强制解析为整数。
- 当前导入逻辑改为优先按 `dim_store.store_code` 命中 `门店ID` 列，若业务填写纯数字，则继续兼容 `dim_store.store_id`；名称漂移仍只告警不阻断。

#### Added
- `test_import_cfg_store_target_daily_from_nas.py` 新增真实 Excel 语义回归：`门店ID=RT050` 可被解析，且门店名称变化时仍能按编码命中共同考核归属。

#### Docs
- 更新 `README.md`、`docs/DATA_CONTRACTS.md`、`docs/数据结构与映射手册.md` 与 `docs/ETL业务逻辑说明.md`，冻结“列名沿用 `门店ID`，业务填写 RT 门店编码，纯数字 `store_id` 仅作兼容”的规则。

#### Validation
- 已执行 `python -m unittest -v test_import_cfg_store_target_daily_from_nas.py`。
- 已对当前 NAS `202606考核数据配置表.xlsx` 做只读解析，确认 `RT050/RT014/RT045/RT140` 可正常命中共同考核归属。

### v0.8.77 — 门店考核归属按门店ID兜底导入（2026-06-18）

#### Changed
- 更新 `tools/import_cfg_store_target_daily_from_nas.py`，将 NAS `门店考核归属` sheet 的 `门店ID` 升级为必填列，共同考核归属改为优先按 `store_id` 命中 `dim_store`，不再只依赖门店名称精确匹配。
- 当 Excel `门店名称` 与 `dim_store.store_name` 不一致但 `门店ID` 能命中时，dry-run 仅输出 warning 并继续导入；当 `门店ID` 本身未命中时，才跳过对应归属配置。

#### Added
- `test_import_cfg_store_target_daily_from_nas.py` 新增“门店考核归属缺少 `门店ID` 表头直接失败”和“门店名称变更但 `门店ID` 正确时仍按 ID 命中”的回归测试。

#### Docs
- 更新 `README.md`、`docs/DATA_CONTRACTS.md`、`docs/数据结构与映射手册.md` 与 `docs/ETL业务逻辑说明.md`，同步 `门店考核归属` 必填 `门店ID` 和按 `store_id` 优先匹配的规则。

#### Validation
- 已执行 `python -m unittest -v test_import_cfg_store_target_daily_from_nas.py`。

### v0.8.76 — ads_sku_daily 月末 31 天窗口漏数修复（2026-06-01）

#### Fixed
- 修复 `etl_ads_sku_daily.py` 复用同一份明细底表同时支撑 MTD 与 30 天趋势窗口时，底表起点只取 `report_date-29`，导致 31 天月份月末遗漏当月 1 号交易的缺陷。以 `2026-05-31 / v1` 为例，校验口径按 5-01 至 5-31 期望 4243 行，但插入 SQL 仅从 5-02 起取数，最终少出 101 行。
- 当前改为让底表窗口取“月初”和“最近 30 天起点”两者中更早的日期，保证 `mtd_sales_amt` / `mtd_sales_qty` / `mtd_order_cnt` 的月累计明细不会在 31 天月末被截断，同时保留 `trend_tag` 所需的最近 30 天 / 7 天窗口。

#### Added
- `test_ads_sku_daily.py` 新增 31 天月末回归断言，锁定 `detail_base` 必须使用 `LEAST(p.month_start_id, p.rolling_30d_start_id)` 作为历史事实起点。

#### Docs
- 更新 `docs/ETL业务逻辑说明.md`，同步 `ads_sku_daily` 的明细底表窗口规则与本次漏数根因。

#### Validation
- 已执行 `python -m unittest test_ads_sku_daily`。
- 已执行只读 CTE 重算，确认 `2026-05-31 / v1` 的 `ranked` 最终结果集恢复为 4243 行。
- 已执行 `python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。

### v0.8.75 — 门店专题自动月份门禁按 report_date 对齐（2026-06-01）

#### Fixed
- 修复 `scheduled_store_daily_report.py` 自动模式把 NAS `target_month` 直接与当天自然月比较，导致 `previous-day` 模式在 `6-1 00:05` 错误跳过 `2026-05` 快照、未处理 `2026-05-31` 专题数据的问题。当前改为按本轮自动 `report_date` 所在月份校验目标文件与免税月累计文件。
- 修复负责人导入仍默认取“今天”作为 `snapshot_date`，导致 `6-1 previous-day` 处理 `2026-05-31` 专题时把负责人快照误按 `2026-06-01` 校验、将整张负责人表判成 `unexpected_entities` 的问题。当前默认改为跟随专题实际处理的 `report_date` 上界。

#### Docs
- 更新 `docs/ARCHITECTURE.md` 与 `docs/RUNBOOK.md`，同步自动月份门禁已改为跟随自动 `report_date`，并补记 `previous-day` / `current-day` 在跨月日的差异。

#### Validation
- 已执行 `python -m unittest test_scheduled_store_daily_report.py`。

#### Safety Boundary
- 本轮仅修改专题调度月份门禁、回归测试与运行文档，未执行数据库写操作、未人工重跑专题链路。

## 2026-05-26

### v0.8.74 — 门店明细总计免税口径同步（2026-05-26）

#### Changed
- `销售部自动化日报.twb` 中 `门店经营明细_门店排名` 的月客单价总计改为非免税月累计销售额 / 总实际单数。
- `销售部自动化日报.twb` 中 `门店经营明细_门店排名` 的月折扣率总计改为非免税月累计销售额 / 非免税月累计吊牌金额。

#### Docs
- 同步 `docs/业务逻辑与指标规范.md`、`docs/数据结构与映射手册.md` 与 `docs/ETL业务逻辑说明.md`：免税外部月累计销售只参与月总达成，不进入月客单价、月折扣率等其它 KPI 分子。

#### Safety Boundary
- 本轮仅修改 Tableau 工作簿展示计算与文档，未执行数据库 DDL/DML、未重跑 ETL、未回填数据。

## 2026-05-20

### v0.8.73 — 门店日报补月累计吊牌金额分母（2026-05-20）

#### Added
- `etl_ads_store_daily_report.py` 输出新增 `mtd_list_amt`，从已过滤的月累计明细 `tot_amt_list` 汇总而来，用作 `月折扣率` 与 Tableau 明细总计的聚合分母。
- 新增 `SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql`，供用户人工为目标表补 `mtd_list_amt` 物理列。
- `test_ads_store_daily_report.py` 新增断言，锁定 `mtd_list_amt` 已进入 SQL 输出字段、结构检查清单与月折扣率分母链路。

#### Docs
- 更新 `docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md` 与 `docs/业务逻辑与指标规范.md`，同步 `mtd_list_amt` 字段、DDL 边界与折扣率总计口径。

#### Safety Boundary
- 本轮未执行数据库 DDL、未重跑 ETL、未补数回填；新增 `ALTER TABLE` 脚本需由用户在 MySQL 人工执行，并建议在门店专题调度空窗执行。

## 2026-05-13

## 2026-05-14

### v0.8.72 — dim_store 改为全量抽取 C_STORE（2026-05-14）

#### Fixed
- 修复 `etl_dim_store.py` 只抽取 Oracle `C_STORE.ISACTIVE='Y'` 店仓，导致闭店/停用门店在 `dim_store` 被物理剔除的问题。当前改为全量抽取 `C_STORE`，并保留 `is_active` 状态字段，由下游口径决定是否纳入统计。
- 明确根因不在 DWS v2 shadow 的 ODS 链路。`dim_store` 当前仍属于 Oracle 直抽的 DIM 全刷对象，不存在独立的 v2 ODS `dim_store`；总控 v2 下同样复用主链 `etl_dim_store`。

#### Added
- 新增 `test_dim_store.py`，锁定 `dim_store` 抽取 SQL 不得再包含 `ISACTIVE='Y'` 过滤条件。

#### Docs
- 更新 `docs/ETL业务逻辑说明.md` 与 `docs/DATA_CONTRACTS.md`，同步 `dim_store` 现已改为全量抽取、`is_active` 仅作状态标识的规则。

#### Validation
- 已执行 `python -m unittest test_dim_store.py`。
- 已执行 `python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。

#### Safety Boundary
- 本轮只修改 `dim_store` 抽取逻辑、单测与文档，未执行任何数据库写操作。

### v0.8.71 — M6 自动批次 pre-refresh old DWS 基线误阻断修复（2026-05-14）

#### Fixed
- 修复 `scheduled_dws_v2_shadow.py` 在总控 V2 前置预刷新场景下，仍把“当天 old DWS 尚未产出快照”视为 inventory ADS gate 阻断条件的问题。Windows 计划任务入口切到 `run_scheduled_total_control_v2.bat` 后，00:05 自动批次会先执行 pre-refresh，再跑主链，因此同日 `dws_inventory_daily` 在 pre-refresh 时点为空属于预期，不应把 `old_dws_max_etl_time=None` 误判成失败。
- 当前逻辑调整为：仅在 `--skip-ads-shadow-validation` 的 pre-refresh 场景、且未请求 same-snapshot 对齐时，若 old DWS 当日快照尚未就绪，则将“库存 old DWS 可比基线检查”记为 `SKIPPED`，并改由 `dwd_inventory_storage_snapshot -> dws_inventory_daily_v2` 自洽结果决定 gate；非 pre-refresh 场景仍保留原有 WARNING / BLOCKED 语义。
- 同步补充 `test_scheduled_dws_v2_shadow.py` 回归测试，覆盖 pre-refresh 缺失 old DWS 同日快照时的 gate READY 与整体 report `SUCCESS` 退出码。

#### Evidence
- 失败证据：`logs/scheduled_total_control_20260514.log` 显示 00:05 自动批次在 `DWS v2 读源预刷新` 阶段返回 `exit_code=1`，根因是 `库存当前 ODS 基线：status=WARNING, mismatch_count=31967, old_dws_max_etl_time=None`，主链与专题链被主动跳过。
- 子链 JSON：`reports/context_cache/scheduled_dws_v2_shadow_20260514_000826.json` 显示 `dws_inventory_v2` 自身 `mismatch_count=0` 且写入 75182 行，但 `inventory_old_dws_comparable_alignment` 因 `old_dws_row_count=0` 被误记为 WARNING，进而把 `inventory_ads_gate_validation.status` 推成 `BLOCKED`。

#### Tests
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_dws_v2_shadow.py test_scheduled_total_control.py`，共 20 项通过，退出码 `0`。

#### Safety Boundary
- 本轮只修复 V2 pre-refresh 的门控语义与单测，不改任何业务口径、不改默认 `legacy`、不改 ADS 字段，也未执行任何数据库写操作。

## 2026-05-13

### v0.8.70 — M6 总控 V2 前置刷新顺序修复（2026-05-13）

#### Fixed
- 修复 `run_scheduled_total_control_v2.bat` 作为 Windows 计划任务入口后，主链 V2 模式先计算 `ads_inventory_health`、后执行 `dws_v2_shadow`，导致新日期 `_v2` DWS 读源为空、`ads_inventory_health` 写出 0 行并触发调度验证失败的问题。
- `scheduled_total_control.py` 在有效 cutover 模式为 `v2` 时，新增阻断型 `DWS v2 读源预刷新`：先运行 `scheduled_dws_v2_shadow.py --skip-ads-shadow-validation` 刷新 raw / DWD / DWS v2，再触发主链与销售专题；若预刷新失败，主链会被主动跳过，避免生产 ADS 读到空/旧 `_v2` 源。
- `scheduled_dws_v2_shadow.py` 新增 `--skip-ads-shadow-validation`，用于总控 V2 主链前置刷新场景：只刷新 `_v2` 读源，不在主链 ADS 重算前比较已持久化 `ads_inventory_health`。

#### Evidence
- 失败证据：`logs/scheduled_total_control_20260513.log` 显示主链子进程返回 1；`logs/etl_20260513.log` 显示 `ads_inventory_health` 在 V2 模式读取 `dws_inventory_daily_v2` / `dws_sales_daily_v2` 并写出 0 行。
- 只读核验：2026-05-13 运行前后查询确认 `dws_inventory_daily_v2_20260513=0`、`dws_sales_daily_v2_20260513=0`、`ads_inventory_health_20260513=0`。
- 生产重跑核验：用户 2026-05-13 09:48 前后手动重跑 `run_scheduled_total_control_v2.bat`；`logs/scheduled_total_control_20260513.log` 显示 09:47:44 ~ 09:58:35 总控整体 `SUCCESS`，链路结果为成功 3 / 失败 0 / 跳过 1，顺序为 `DWS v2 读源预刷新 -> 主链调度 -> 门店销售专题 -> DWS v2 Shadow SKIPPED`；只读查询确认 `ads_inventory_health_20260513=3088`、`dws_sales_daily_v2_20260513=177`、`dws_inventory_daily_v2_20260513=75168`。详见 `reports/m6_v2_prerefresh_production_rerun_20260513.txt`。

#### Tests
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_total_control.py test_scheduled_dws_v2_shadow.py`，共 18 项通过；输出已落盘到 `reports/m6_v2_prerefresh_tests_20260513.txt`。

#### Safety Boundary
- 本轮只修改总控顺序、shadow 前置刷新参数、单元测试和文档；生产总控重跑由用户人工执行，Agent 仅做日志与只读数据核验；未修改默认 `legacy` 模式，未改名或删除任何 ADS 字段。

## 2026-05-12

### v0.8.69 — M5 ADS V2 双跑 gate 通过与 shadow 别名修复（2026-05-12）

#### Changed
- 回填 M5 ADS V2 闭环双跑 gate：用户已手工执行两轮 `run_scheduled_total_control_v2.bat`，两轮主链与门店专题均成功，`ads_inventory_health` 两轮均以 `dws_inventory_daily_v2` / `dws_sales_daily_v2` 为读源写出 3087 行；销售日报 Tableau 与库存看板由用户确认展示正常。
- ODS-DWD-DWS-ADS 子项目状态更新为 M5 通过、可进入 M6 计划任务入口切换讨论；默认调度入口仍保持 legacy，是否切 Windows 计划任务待用户确认。

#### Fixed
- 修复 `ads_inventory_health` shadow 报告型对账 SQL 的 `color` / `size` 投影别名缺失问题，避免非阻断 shadow compare 报 `Unknown column 'ranked.color' in 'field list'`。

#### Tests
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_dws_v2_shadow.py test_scheduled_total_control.py`，共 16 项通过；输出已落盘到 `reports/m5_v2_gate_followup_tests_20260512.txt`。

#### Safety Boundary
- 本轮只修复 shadow 报告型对账 SQL、测试与文档记录；未切换默认 legacy、未修改 Windows 计划任务入口、未改名或删除任何 ADS 字段。

### v0.8.68 — 总控 V2 wrapper 与 ADS 双跑 gate 清单（2026-05-12）

#### Added
- 新增 `run_scheduled_total_control_v2.bat`，预置 `--cutover-mode v2` 并透传追加参数，供用户执行两次总控 V2 gate。
- 新增 `docs/ODS-DWD-DWS-ADS架构完善子项目/09_M5_ADS_v2闭环切换清单.md`，固化两次总控 V2 验收、ADS 依赖分类、字段兼容红线和 rollback 路径。

#### Fixed
- `run_scheduled_total_control.bat` 现会把 `%*` 参数透传给 `scheduled_total_control.py`，避免通过 Windows wrapper 传入 `--cutover-mode v2` 时被静默丢弃、实际仍跑默认 legacy。

#### Docs
- 同步更新 `docs/RUNBOOK.md` 与 ODS-DWD-DWS-ADS 子项目文档，将下一步从 3 到 7 天总控非阻断观察调整为用户确认的两次总控 V2 双跑 gate。

#### Validation
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_total_control.py test_scheduled_store_daily_report.py test_scheduled_dws_v2_shadow.py`，共 30 项通过；输出已落盘到 `reports/cutover_v2_wrapper_validation_20260512.txt`。
- 已执行 `cmd /c run_scheduled_total_control.bat --help` 与 `cmd /c run_scheduled_total_control_v2.bat --help`，验证 wrapper 可启动且不会丢弃追加参数；输出已落盘到 `reports/run_scheduled_total_control_help_20260512.txt` 与 `reports/run_scheduled_total_control_v2_help_20260512.txt`。
- 已执行 `D:/Anaconda/envs/pyproject/python.exe scripts/check_doc_sync.py --output reports/docs_code_alignment.json`；审计输出已刷新，当前报告仍包含历史 `code_only` 高风险项，未在本轮收敛。

#### Safety Boundary
- 本轮仅修改 wrapper、测试与文档，未执行生产总控 V2 写库，未修改默认 `legacy` 模式，未改名或删除任何 ADS 字段。

### v0.8.67 — 主链补显式 cutover / rollback 与专题 freshness 派生（2026-05-12）

#### Changed
- 新增 `cutover_controls.py`，统一 `legacy / shadow_compare / v2` 归一化、`rollback_to_legacy` 覆盖规则，以及门店专题 `sales_freshness_source` 的默认派生逻辑。
- `run_etl.py`、`scheduled_etl.py`、`scheduled_total_control.py` 补齐 `--cutover-mode` 与 `--rollback-to-legacy`，默认仍保持 legacy；`shadow_compare` 继续按旧 DWS 写生产 `ads_inventory_health`，仅附带 `_v2` 报告型对比，`v2` 才会显式把 `ads_inventory_health` 切到 `dws_inventory_daily_v2 + dws_sales_daily_v2`。
- `scheduled_total_control.py` 现会把 cutover / rollback 透传到主链与门店专题链，但 `dws_v2_shadow` 仍保持独立非阻断观察子链。
- `scheduled_store_daily_report.py` 不再把销售 freshness 写死为 `dws_sales_daily.etl_time`；默认会随 cutover 上下文切到对应 DWS 源，也允许显式 `--sales-freshness-source legacy|v2` 覆盖。
- `scheduled_dws_v2_shadow.py` 在写完 `_v2` 后新增 `ads_inventory_health` 报告型验证，按 `etl_ads_health.py` 同算法读取 `_v2` DWS 源与当天已落库 `ads_inventory_health` 做 compare，只产出证据不覆盖生产表。

#### Fixed
- 修复 `scheduled_store_daily_report.py` 直接调用 `run_schedule_once()` 时不会自行解析 `effective_cutover_mode` / `sales_freshness_source_mode` 的问题，避免 CLI 参数默认值只在入口层生效、函数直调路径失配。

#### Docs
- 同步更新 `docs/ARCHITECTURE.md`、`docs/ETL业务逻辑说明.md`、`docs/DATA_CONTRACTS.md`、`docs/RUNBOOK.md` 与 ODS-DWD-DWS-ADS 子项目文档，统一改正“主链完全未接 cutover”“专题 freshness 永远固定 legacy”“ADS 绝不读 v2”的过时描述。
- 文档明确保留安全边界：默认运行模式仍为 legacy，只有 `ads_inventory_health` 进入显式 cutover 范围，且未来若影子链替代旧链，不得改名或删除既有 ADS 字段。

#### Validation
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_store_daily_report.py test_scheduled_dws_v2_shadow.py test_scheduled_total_control.py`，共 28 项通过；输出已落盘到 `reports/cutover_validation_20260512.txt`。

#### Safety Boundary
- 本轮未把默认主链切到 v2，未让 `_v2` 自动替代生产 DWS / ADS，未执行任何 `INSERT` / `UPDATE` / `DELETE` / 回填；后续若需验证切换边界，仍需显式传参并由用户控制执行。

### v0.8.66 — Inventory ADS gate 验证切到 current ODS/DWD 基线（2026-05-12）

#### Changed
- `scheduled_dws_v2_shadow.py` 新增 `inventory_ads_gate_validation` 顶层结论，inventory 是否可用于 `ads_inventory_health` gate 现改为基于当前 `ods_fa_storage` 可比基线与 `dwd_inventory_storage_snapshot -> dws_inventory_daily_v2` 自洽结果判定。
- shadow 链路摘要中的 inventory 文案同步改为“库存当前 ODS 基线”与“库存 ADS 门”，不再把 old DWS same-snapshot 当作 ADS gate 的默认前提。

#### Docs
- 更新 ODS-DWD-DWS-ADS 子项目的推进看板与续接上下文，明确 `--align-with-old-dws` / `--source-loaded-at-cutoff` 仅保留为 old/v2 精确诊断入口，不再作为 inventory ADS gate 主验证口径。

#### Validation
- 已更新 `test_scheduled_dws_v2_shadow.py`，覆盖 current baseline 下的 inventory ADS gate READY / BLOCKED 判定与链路摘要输出。

#### Safety Boundary
- 本轮仅调整 shadow 验证口径、摘要与文档，不修改 `etl_ads_health.py` 生产计算逻辑，不执行任何 `INSERT` / `UPDATE` / `DELETE` / 回填。

### v0.8.65 — Inventory same-snapshot 新增 late-load 防误判保护（2026-05-12）

#### Fixed
- `etl_dws_inventory_v2.py` 在启用 `--align-with-old-dws` 或显式 `--source-loaded-at-cutoff` 时，新增 `source_loaded_at` 截止后 scope 行探针；若 cutoff 之后仍有 `dws_inventory_scope_flag='Y'` 的库存行，则直接拒绝继续 same-snapshot，对外明确提示“当前 raw/DWD 不能复原该历史快照”。
- 这样可避免把 old `dws_inventory_daily.etl_time` 直接当成可回放的 `source_loaded_at` 截止时点，进而写出一个看似已对齐、实则被 late-loaded raw 批次裁掉的 `dws_inventory_daily_v2`。

#### Docs
- 更新 `docs/RUNBOOK.md`，补记 inventory same-snapshot 的失败前提与排查方向：若 cutoff 后仍有 scope 行，应先查 inventory raw / DWD 补数批次，而不是继续拿该 cutoff 做 ADS gate 判责。

#### Validation
- 已补 `test_dws_v2_dry_run.py`，覆盖 late-loaded scope probe SQL、invalid cutoff 拒绝与零冲突放行三类回归。

#### Safety Boundary
- 本轮仅新增只读探针、失败保护、单元测试与运行手册说明，未执行任何 `INSERT` / `UPDATE` / `DELETE` / 回填；inventory same-snapshot shadow 仍由用户手工触发。

### v0.8.64 — DWS v2 Shadow 透传 inventory same-snapshot 对齐参数（2026-05-12）

#### Changed
- `scheduled_dws_v2_shadow.py` 新增 `--inventory-align-with-old-dws` 与 `--inventory-source-loaded-at-cutoff`，把库存 same-snapshot 对齐能力直接透传到 `etl_dws_inventory_v2.py`，避免 shadow 入口默认仍写出未对齐的 `dws_inventory_daily_v2`。
- shadow 运行报告新增 `inventory_alignment` 顶层记录，便于从 JSON 证据快速判断本轮库存 v2 是否启用了 old DWS 对齐或显式 cutoff。

#### Docs
- 更新 `docs/RUNBOOK.md` 的 DWS v2 shadow 命令示例，补充 inventory same-snapshot 的两种触发方式，便于后续按同一个 shadow 入口继续推进 ADS gate 验证。

#### Validation
- 已执行 `python -m unittest test_scheduled_dws_v2_shadow.py`。
- 已验证 `python scheduled_dws_v2_shadow.py --help`。

#### Safety Boundary
- 本轮仅补充 shadow 调度参数透传、回归测试与运行手册，未执行任何 `INSERT` / `UPDATE` / `DELETE` / 回填；后续 inventory same-snapshot shadow 仍由用户手工触发。

### v0.8.63 — DWS v2 Shadow 销售窗口补齐 ADS 31 天游标（2026-05-12）

#### Changed
- `scheduled_dws_v2_shadow.py` 将销售 shadow 默认回算窗口从主链 7 天提升为覆盖 `ads_inventory_health` 消费窗的 `31` 天，按 `today-30 ~ today` 包含当天补齐 `dws_sales_daily_v2` 历史覆盖。
- 当销售 shadow 窗口大于主链 `DWS_SALES_MAINLINE_DAYS_BACK=7` 时，销售 raw / DWD / DWS v2 步骤自动从 `etl` 切到 `long_running`，避免继续沿用 7 天小窗的超时档位。
- shadow 运行摘要新增销售超时档位与 ADS 销售门覆盖状态，后续可直接从 JSON / 总控摘要判断当前窗口是否足以支撑 `ads_inventory_health` 下游验证。

#### Docs
- 更新 `docs/ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md`，把 S4 销售 shadow 默认窗口、超时档位和下一步执行顺序同步到方案文档。
- 更新 `docs/ODS-DWD-DWS-ADS架构完善子项目/03_推进看板与里程碑.md` 与 `docs/ODS-DWD-DWS-ADS架构完善子项目/02_任务续接上下文.md`，把下一轮入口从“先补代码入口”调整为“由用户执行一轮新 shadow 后重做 ADS gate 验证”。

#### Validation
- 已执行 `python -m unittest test_scheduled_dws_v2_shadow.py`。

#### Safety Boundary
- 本轮仅修改 shadow 调度代码、测试与文档，未执行任何 `INSERT` / `UPDATE` / `DELETE` / 回填；新的 31 天游标 shadow 仍由用户手工触发。

## 2026-05-11

### v0.8.62 — 新增万店掌完整 ODS-DWD-DWS-DIM-ADS 链路草案（2026-05-11）

#### Added
- 新增 `ovopark_api_client.py` 与 `ovopark_etl_common.py`，统一万店掌签名、登录、请求与 MySQL 依赖检查的公共逻辑。
- 新增 `etl_ods_ovopark_shop.py`、`etl_ods_ovopark_passenger_flow.py`、`etl_dwd_ovopark_passenger_flow_daily.py`、`etl_dws_ovopark_passenger_flow.py`、`etl_ads_ovopark_store_monthly.py` 五个独立链路脚本，覆盖 Ovopark 从 ODS 到 ADS 的完整路径。
- 新增 `SQL/draft_create_dwd_ovopark_passenger_flow_daily.sql`、`SQL/draft_create_dws_ovopark_passenger_flow_daily.sql`、`SQL/draft_create_dws_ovopark_passenger_flow_monthly.sql`、`SQL/draft_create_ads_ovopark_store_monthly.sql`，补齐 DWD / DWS / ADS 草案。
- 新增 `SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql`，把 62 条 `exact_name / exact_name_area` 初配结果转成映射 seed 草案。

#### Changed
- `config.py` 与 `.env.example` 补充 Ovopark 环境变量入口，显式区分 `OVOPARK_APP_ID`、`OVOPARK_ACCESS_KEY_ID`、`OVOPARK_ACCESS_KEY_SECRET`、`OVOPARK_USERNAME`、`OVOPARK_PASSWORD`、`OVOPARK_AUTHENTICATOR`。
- `SQL/draft_create_dim_ovopark_shop_mapping.sql` 放宽 `PENDING` 行的何方门店字段为可空，并新增当前行唯一性保护辅助列，避免 1:1 映射被重复占用。
- `SQL/draft_create_ods_ovopark_tables.sql` 修正小时级客流表主键，补入 `request_object_key` 与 `is_on_business_time`，避免同小时不同请求语义发生主键碰撞。

#### Docs
- 更新 `docs/万店掌API接入-子项目资料/万店掌API探测与接入方案.md`、`docs/万店掌API接入-子项目资料/万店掌API续接上下文.md` 与 `docs/万店掌API接入-子项目资料/万店掌API阻塞演示与能力地图.html`，将项目状态从“缺密码 / 仅 ODS 建模”收口为“完整链路 draft 已落盘，待人工 apply 与首轮执行”。

#### Validation
- 已执行编辑器静态错误检查：`ovopark_api_client.py`、`ovopark_etl_common.py`、`etl_ods_ovopark_shop.py`、`etl_ods_ovopark_passenger_flow.py`、`etl_dwd_ovopark_passenger_flow_daily.py`、`etl_dws_ovopark_passenger_flow.py`、`etl_ads_ovopark_store_monthly.py` 无语法错误。

#### Safety Boundary
- 本轮仅新增或更新 draft SQL、独立 ETL 脚本与专题文档，未执行任何 CREATE / ALTER / INSERT / UPDATE / DELETE；Ovopark 链路首轮 apply 与执行仍由用户人工控制。

### v0.8.61 — 万店掌按 depId / S_门店id 收口 ODS 候选表设计（2026-05-11）

#### Added
- 新增 `SQL/draft_create_dim_ovopark_shop_mapping.sql`，输出何方门店与万店掌 `dep_id / S_门店id` 的 SCD2 映射维表草案。
- 新增 `SQL/draft_create_ods_ovopark_tables.sql`，输出 `ods_ovopark_api_raw`、`ods_ovopark_shop`、`ods_ovopark_passenger_flow_daily`、`ods_ovopark_passenger_flow_hourly` 四张候选表草案。

#### Database / SQL
- 基于本轮真实联调结果，将万店掌接入主键统一收口为内部 `depId / S_门店id`，不再把第三方 `shopId` 当作默认可用主键。
- 在草案层明确安全边界：ODS raw 不持久化 `authenticator / Ovo-Authorization` 原文，只保存脱敏请求参数与完整响应 JSON。
- 在草案层明确当前门店映射前提：`getDepartments` 全量 64 家样本中 `shopId` 与 `trilateralId` 均为空，因此 `dim_ovopark_shop_mapping` 作为接入前置表保留。

#### Docs
- 更新 `docs/万店掌API接入-子项目资料/万店掌API探测与接入方案.md` 的 MySQL 落地候选设计章节，改为引用实际 DDL 草案并明确各表粒度、主键和作用边界。

#### Validation
- 已执行 `python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。

#### Safety Boundary
- 本轮仅新增 DDL 草案与设计文档同步，未执行任何 CREATE / ALTER / INSERT / UPDATE / DELETE。

## 2026-05-06

## 2026-05-07

## 2026-05-08

### v0.8.60 — 修复门店销售专题链 ads_sales_org_daily 歧义列回归（2026-05-08）

#### Fixed
- `etl_ads_sales_org_daily.py` 将 `scope_stats` 目标统计 SQL 中未限定别名的 `COUNT(DISTINCT store_id)` 修正为 `COUNT(DISTINCT t.store_id)`，避免在 `cfg_store_target_daily` 与当前门店范围 join 后触发 `(1052, "Column 'store_id' in field list is ambiguous")`。
- `test_ads_sales_scope_alignment.py` 补充回归断言，锁定 `ads_sales_org_daily` 的目标统计 SQL 必须使用带表别名的 `t.store_id`，避免类似问题再次仅在生产重跑时暴露。

#### Validation
- 已执行 `python -m py_compile etl_ads_sales_org_daily.py test_ads_sales_scope_alignment.py`。
- 已执行 `python -m unittest test_ads_sales_scope_alignment.py`。

### v0.8.59 — 月级组织汇总同步收口到目标生效门店（2026-05-08）

#### Changed
- `etl_ads_sales_org_monthly.py` 将 `store_scope`、运行前 `scope_signature` 和 `scope_stats` 统一收口到 `report_date` 当天“组织属性有效且目标已生效”的门店，避免月级组织汇总继续沿用宽门店范围。
- `ads_sales_org_monthly` 的完成日志同步改为输出“生效目标门店”，与 `ads_store_daily_report`、`ads_sales_org_daily`、`ads_daily_sales` 的范围口径保持一致。
- `test_ads_sales_org_monthly.py` 新增 SQL 骨架、scope 签名与 scope 统计三条回归测试，锁定当天目标过滤不会在月级脚本里回退。

#### Validation
- 已执行只读 SQL 核验：`2026-05-02 ~ 2026-05-06 / v1` 的宽门店范围为 72 家、当天目标已生效门店为 71 家，差集门店固定为 `RT116 / 长沙运达汇店`。
- 已执行只读 SQL 核验：`RT116` 在 `2026-05-02 ~ 2026-05-06` 无销售事实，且现网 `ads_sales_org_monthly` 当前月汇总金额与 `ads_sales_org_daily` 的 `MTD` 仍对平，因此本次风险主要在范围定义漂移，而非已落金额偏差。

#### Safety Boundary
- 本轮未执行任何生产写库或专题重跑；`ads_daily_sales`、`ads_sales_org_daily`、`ads_sales_org_monthly` 的正式重跑仍由用户手工执行。

#### Docs
- 同步更新 `docs/ETL业务逻辑说明.md` 与 `docs/ARCHITECTURE.md`，补记月级组织汇总也已收口到 `report_date` 当天目标已生效门店，并注明 5 月上旬差集与金额边界。

### v0.8.58 — 销售主题 ADS 门店范围收口到目标生效门店（2026-05-08）

#### Changed
- `etl_ads_daily_sales.py` 将 `store_scope` 与运行前 scope 统计统一收口到 `report_date` 当天“组织属性有效且目标已生效”的门店，避免月中新店或未来生效门店在战役表中提前进入有效门店口径。
- `etl_ads_sales_org_daily.py` 采用同样的 `report_date` 当天目标生效门店交集定义，和 `ads_store_daily_report`、`ads_daily_sales` 保持一致，不再对 `target_rows != stores` 做侧向告警。
- 新增 `test_ads_sales_scope_alignment.py`，锁定两张销售主题表的 SQL 骨架与 scope 统计查询都必须带 `cfg_store_target_daily` 当天目标过滤。

#### Validation
- 已执行 `python -m unittest test_ads_sales_scope_alignment.py`。

#### Safety Boundary
- 本轮未修改 `cfg_store_target_daily`、负责人快照导入逻辑和 `ads_store_daily_report` 主体逻辑，只把两张下游销售主题 ADS 的有效门店定义收口到现有专题权威范围。

#### Docs
- 同步更新 `docs/ETL业务逻辑说明.md` 与 `docs/ARCHITECTURE.md`，明确销售主题 ADS 仅消费 `report_date` 当天目标已生效门店。

### v0.8.57 — Inventory Shadow 旧链对齐改为主链 ODS 可比基线（2026-05-08）

#### Changed
- `scheduled_dws_v2_shadow.py` 新增库存 old `dws_inventory_daily` 可比基线检查，直接用主链 `ods_fa_storage` 复刻旧库存 DWS 聚合口径，不再把刷新后的 raw / DWD 结果直接拿去和 old DWS 判责。
- shadow 内的库存 `dws_inventory_daily_v2` 写入改为只承担 `dwd_inventory_storage_snapshot -> dws_inventory_daily_v2` 自洽校验，避免刷新 raw 后 `source_loaded_at` 抬升造成 false warning。
- 新增 `test_scheduled_dws_v2_shadow.py`，覆盖库存旧链基线 SQL 与 shadow 汇总文案。

#### Validation
- 已执行 `python -m unittest test_scheduled_dws_v2_shadow.py test_scheduled_total_control.py test_dws_v2_dry_run.py`。

#### Safety Boundary
- 本轮未修改 `run_etl.py`、`scheduled_etl.py` 与生产 `dws_inventory_daily` 逻辑；旧主链库存口径不变。
- `etl_dws_inventory_v2.py` 中 `--align-with-old-dws` / `--source-loaded-at-cutoff` 仍保留，供后续需要显式构造同一 raw / DWD 时点时人工排查使用。

#### Docs
- 同步更新 `docs/ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md`，明确 shadow 内旧链检查与 post-refresh v2 校验的边界。

### v0.8.56 — 门店日报按目标生效区间收口月中新店（2026-05-07）

#### Changed
- `tools/import_cfg_store_target_daily_from_nas.py` 的 `导入模板` 新增可选列 `生效开始日` / `生效结束日`，并按门店行自己的有效区间展开 `cfg_store_target_daily`。
- `tools/import_store_operation_owner_from_nas.py` 只对 `snapshot_date` 当天已在 `cfg_store_target_daily` 生效的门店推导应维护经营实体，避免预建店或未来生效门店提前要求负责人切片。
- `etl_ads_store_daily_report.py` 仅纳入 `report_date` 当天存在目标行的门店，并把负责人缺口校验收敛到当日目标范围。

#### Validation
- 已执行 `python -m unittest -v test_import_cfg_store_target_daily_from_nas.py test_store_operation_owner_import.py test_scheduled_store_daily_report.py`。
- 已执行 `python -m py_compile tools/import_cfg_store_target_daily_from_nas.py tools/import_store_operation_owner_from_nas.py etl_ads_store_daily_report.py`。

#### Safety Boundary
- 本轮没有放宽 `ads_store_daily_report` 的负责人切片校验，而是改为用目标生效区间重新定义专题口径范围。
- 未新增任何数据库写操作；现网仍由业务只维护目标配置表与负责人映射表，负责人历史继续由库内 SCD2 自动维护。

#### Docs
- 同步更新 `docs/ETL业务逻辑说明.md` 与 `docs/数据结构与映射手册.md`，补记目标模板生效区间与目标驱动的门店/负责人范围。

### v0.8.55 — DWS v2 S4 独立 shadow 调度与总控非阻断接入（2026-05-07）

#### Added
- 新增 `scheduled_dws_v2_shadow.py`，用于串联 raw ODS → DWD → DWS v2 的独立 shadow 调度，默认只写 `_v2` 并行验证资产。
- 新增 `run_scheduled_dws_v2_shadow.bat`，作为 Windows 任务计划或人工触发的 shadow 包装入口。

#### Changed
- `scheduled_total_control.py` 新增 `dws_v2_shadow` 第三子链，主链成功后依次触发销售专题链与 DWS v2 shadow；支持 `--shadow-only`，且 shadow 失败只记 `WARNING`，不阻断旧 DWS / ADS。
- 修正总控专项开关边界：`--topic-only` 仅运行销售专题链，不再误带出 shadow 子链。
- `test_scheduled_total_control.py` 扩展覆盖第三子链、`--shadow-only` 与 `--topic-only` 行为，确保总控摘要和跳过逻辑符合当前设计。

#### Validation
- 已执行 `python -m py_compile scheduled_dws_v2_shadow.py scheduled_total_control.py test_scheduled_total_control.py`。
- 已执行 `python -m unittest -v test_scheduled_total_control.py`。
- 已验证 `python scheduled_dws_v2_shadow.py --help` 与 `python scheduled_total_control.py --help`。

#### Safety Boundary
- 本轮未修改 `run_etl.py`、`scheduled_etl.py` 与 ADS 读源；`dws_v2_shadow` 当前仅作为观察链路存在，不替换生产 DWS。
- DWS v2 仍不进入 `run_etl.py` 主链，ADS 仍不消费 `_v2` 表。

#### Docs
- 同步更新 `docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/数据仓库与ETL手册.md` 以及 ODS-DWD-DWS-ADS 子项目文档，明确 shadow 调度已落地且仅以非阻断方式接入总控。

### v0.8.54 — 库存 DWS v2 S4 对齐口径固化（2026-05-07）

#### Changed
- `etl_dws_inventory_v2.py` 新增 `--source-loaded-at-cutoff` 与 `--align-with-old-dws`，允许把库存 v2 重算固定到同一 `source_loaded_at` 截止时点。
- 库存写入分支由“纯 upsert”收敛为“同一 `date_id` 切片先删后灌 + insert/upsert”，避免使用更早 cutoff 重跑时残留上一次更晚快照才出现的 key。
- 写入运行证据新增 old DWS 对齐上下文，可记录 old DWS 基线、resolved cutoff 和 aligned old-v2 mismatch 样本。

#### Validation
- `SQL/check_dws_v2_parallel_reconciliation.sql` 新增库存 old DWS 基线探针、`old DWS vs DWD aligned` 对账段和仅限 aligned reload 后使用的 `old DWS vs v2` 对账段。
- `test_dws_v2_dry_run.py` 已补 source cutoff、old DWS probe、同日删除切片和非法 old DWS 表名校验用例。

#### Safety Boundary
- 本轮仍未修改 `run_etl.py`、`scheduled_etl.py`、`scheduled_total_control.py`；DWS v2 仍不接总控，ADS 仍不消费 v2 表。
- 库存 old DWS 精确对比现在要求先固定旧表 `MAX(etl_time)` 或显式传入同值 cutoff，再重载同一天 `dws_inventory_daily_v2`。

#### Docs
- 同步更新 M4 方案、推进看板、续接上下文、`docs/MYSQL数据字典.md` 与 `docs/DATA_CONTRACTS.md`，把库存 S4 对齐步骤写成显式执行口径。

### v0.8.53 — DWS v2 S3 实跑验收收口（2026-05-07）

#### Changed
- 在用户明确授权下完成 `etl_dws_sales_v2.py --execute --confirm-write WRITE_DWS_SALES_V2` 与 `etl_dws_inventory_v2.py --execute --confirm-write WRITE_DWS_INVENTORY_V2` 的 S3 实跑验收。
- 销售 `dws_sales_daily_v2` 在 `20260428-20260430` 写入 3417 行，`source_dwd_row_count=5103`，DWD-v2 mismatch 为 0。
- 库存 `dws_inventory_daily_v2` 在 `20260507` 写入 75104 行，DWD-v2 mismatch 为 0。

#### Validation
- 运行证据已落 `reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json` 与 `reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json`。
- 只读验收复核显示：销售 v2 与旧 `dws_sales_daily` 在验收窗口 0 差异；库存 v2 与旧 `dws_inventory_daily` 存在 200 条同 key `qty` 差异，`qty_total_diff=99`、`qtypurchaserem_total_diff=0`，当前按快照时点不同记录，不视为 DWD→v2 转换错误。

#### Safety Boundary
- 本轮未修改 `run_etl.py`、`scheduled_etl.py`、`scheduled_total_control.py`；DWS v2 仍不接总控，ADS 仍不消费 v2 表。
- S4 之前仍需固定库存 shadow 对比所使用的 source snapshot timepoint，避免同日不同时点差异误判。

#### Docs
- 同步更新 DWS v2 SQL 头部、M4 方案、推进看板、续接上下文、设计基线、子项目 README、M3 续接说明与共享交接文档，补齐“已完成 S3 实跑验收”的当前状态描述。

### v0.8.52 — DWS v2 S3 手工写入分支（2026-05-07）

#### Added
- 新增 `dws_v2_write_utils.py`，统一封装 DWS v2 S3 写入确认令牌、MySQL 命名锁、显式事务前隐式事务清理、JSON 安全序列化和运行证据输出。
- `etl_dws_sales_v2.py` 新增 S3 手工写入分支：默认仍 dry-run；只有显式传入 `--execute --confirm-write WRITE_DWS_SALES_V2` 才会写入 `dws_sales_daily_v2`。
- `etl_dws_inventory_v2.py` 新增 S3 手工写入分支：默认仍 dry-run；只有显式传入 `--execute --confirm-write WRITE_DWS_INVENTORY_V2` 才会写入 `dws_inventory_daily_v2`。
- 两个 v2 脚本的 dry-run 输出补充写后目标摘要 SQL 与 DWD-v2 对账 SQL；写入分支会输出 `reports/context_cache/dws_sales_v2_s3_load_*.json` 或 `reports/context_cache/dws_inventory_v2_s3_load_*.json` 运行证据。

#### Safety Boundary
- 本轮未执行真实 `--execute --confirm-write` 写入，未修改 `run_etl.py`、`scheduled_etl.py`、`scheduled_total_control.py`；DWS v2 仍不接总控，ADS 仍不消费 v2 表。
- 写入分支必须经过确认令牌、表结构检查、MySQL 命名锁、显式事务、失败回滚、锁释放和写后 DWD-v2 对账。
- 真实 v2 小窗口写入仍由用户手工执行并复核 JSON 运行证据；Agent 不代执行落库写操作。

#### Validation
- 已执行 `python -m py_compile dws_v2_write_utils.py etl_dws_sales_v2.py etl_dws_inventory_v2.py test_dws_v2_dry_run.py`。
- 已执行 `python -m unittest -v test_dws_v2_dry_run.py`，4 个测试通过。
- 已验证两个脚本 `--help`、默认 dry-run 输出、`--execute` 无确认令牌拒绝写入（退出码 1）和 `--conn-test` 只读结构检查通过。

#### Docs
- 同步更新 DWS v2 SQL 头部、M4 方案、推进看板、续接上下文、设计基线、子项目 README、M3 交接文档、`docs/ETL业务逻辑说明.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`、`docs/ARCHITECTURE.md` 与 `docs/RUNBOOK.md`，明确 S3 手工写入分支已实现但本轮未执行真实写入、未接调度。

### v0.8.51 — DWS v2 dry-run / conn-test 脚本设计（2026-05-07）

#### Added
- 新增 `etl_dws_sales_v2.py`，用于 `dws_sales_daily_v2` 并行验证表的 dry-run / conn-test。默认仅输出源摘要 SQL、候选 `INSERT ... SELECT` SQL与参数；`--conn-test` 只读校验 MySQL 连接、`dwd_sales_retail_item` / `dws_sales_daily_v2` 字段和粒度唯一键。
- 新增 `etl_dws_inventory_v2.py`，用于 `dws_inventory_daily_v2` 并行验证表的 dry-run / conn-test。默认仅输出源摘要 SQL、候选 `INSERT ... SELECT` SQL与参数；`--conn-test` 只读校验 MySQL 连接、`dwd_inventory_storage_snapshot` / `dws_inventory_daily_v2` 字段和粒度唯一键。
- 新增 `test_dws_v2_dry_run.py`，覆盖两个 v2 dry-run SQL 生成、默认窗口 / 快照参数和非法标识符拦截。

#### Safety Boundary
- 两个新增脚本当前均无 `--execute` / `--apply` 写库入口，不执行 DDL / DML，不删除或插入 `dws_sales_daily_v2` / `dws_inventory_daily_v2`。
- 未修改 `run_etl.py`、`scheduled_etl.py`、`scheduled_total_control.py`；DWS v2 仍不接总控，ADS 仍不消费 v2 表。

#### Validation
- 已执行 `python -m py_compile etl_dws_sales_v2.py etl_dws_inventory_v2.py test_dws_v2_dry_run.py`。
- 已执行 `python -m unittest -v test_dws_v2_dry_run.py`，3 个测试通过。
- 已执行两个脚本的默认 dry-run 输出验证，以及 `etl_dws_sales_v2.py --conn-test`、`etl_dws_inventory_v2.py --conn-test` 只读结构检查。

#### Docs
- 同步更新 M4 方案、推进看板、续接上下文、设计基线、子项目 README、M3 交接文档、`docs/ETL业务逻辑说明.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`、`docs/ARCHITECTURE.md` 与 `docs/RUNBOOK.md`，明确 DWS v2 进入 dry-run / conn-test 阶段但仍未写库、未接调度。

### v0.8.50 — DWS v2 人工建表状态核验（2026-05-07）

#### Added
- 新增 `reports/context_cache/dws_v2_manual_ddl_verification_20260507.json`，记录用户人工执行 DWS v2 DDL 后的只读结构核验证据。

#### Database / SQL
- 用户已人工建 `dws_sales_daily_v2` 与 `dws_inventory_daily_v2`；Copilot 未执行 DDL / DML，仅通过 `INFORMATION_SCHEMA` 与行数查询核验。
- 核验结果：`dws_sales_daily_v2` 为 33 列 0 行，`dws_inventory_daily_v2` 为 31 列 0 行，两表均具备 `date_id + store_id + product_id + m_productalias_id` 粒度唯一键、`validation_status` 与 `etl_time`。
- 更新三份 DWS v2 SQL 文件头部状态说明，明确两张 v2 表当前为空表，仍需完成候选装载后才能执行并行对账 SQL。

#### Docs
- 同步更新 ODS-DWD-DWS-ADS 架构完善子项目入口、设计基线、续接上下文、推进看板、M4 方案、`docs/MYSQL数据字典.md` 与 `docs/DATA_CONTRACTS.md`，明确 DWS v2 已人工建表但未写数据、未接总控、未切 ADS。

### v0.8.49 — M4 DWS v2 并行表与调度回滚方案设计（2026-05-07）

#### Added
- 新增 `docs/ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md`，设计 `dws_sales_daily_v2`、`dws_inventory_daily_v2` 并行表、shadow 调度接入、验收矩阵与分阶段回滚路径。
- 新增 `SQL/draft_create_dws_sales_daily_v2.sql` 与 `SQL/draft_create_dws_inventory_daily_v2.sql`，仅作为 DDL 草案；未执行 DDL、未写库、未建索引。
- 新增 `SQL/check_dws_v2_parallel_reconciliation.sql`，提供只读对账 SQL，用于后续比较 DWD→DWS v2 自洽和 DWS v2→旧 DWS 差异。
- 新增 `reports/context_cache/dws_v2_parallel_design_evidence_20260507.json`，记录只读 schema 探查、M3 验证结论和 DWS v2 超时 / 锁设计输入。

#### Docs
- 同步更新 ODS-DWD-DWS-ADS 架构完善子项目入口、设计基线、续接上下文、推进看板和 M3 文档，明确 M4 仍为设计草案，当前未接总控，现有 DWS / ADS 不消费 v2 表。

### v0.8.48 — M3 销售完整业务日期与库存 full raw 初始化（2026-05-07）

#### Added
- 新增 `reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json`，记录销售完整业务日期 raw 补齐、库存 full raw 初始化、DWD 重算、最小对账与库存时间点差异证据。

#### Changed
- `etl_ods_m_retail_raw.py` 新增 `business-date` 模式，可按 `M_RETAIL.BILLDATE` 补齐完整业务日期单头 raw，仍默认 dry-run，显式 `--execute` 才写库。
- `etl_ods_m_retailitem_raw.py` 新增 `business-date` 模式，可关联 `M_RETAIL.BILLDATE` 补齐完整业务日期明细 raw，仍默认 dry-run，显式 `--execute` 才写库。

#### Operations
- 已按用户授权补齐销售 20260428-20260430 完整业务日期 raw：`ods_m_retail_raw` 2861 行、`ods_m_retailitem_raw` 5103 行。
- 已按用户授权执行库存 `FA_STORAGE` full raw 初始化：`ods_fa_storage_raw` 201946 行，`timeout_profile=long_running`，未执行 `TRUNCATE`，按主键 upsert。
- 已重算旁路 DWD：`dwd_sales_retail_item` 20260428-20260430 为 5103 行，`dwd_inventory_storage_snapshot` 20260507 为 201946 行。
- 最小对账结果：销售 DWD 与 `dws_sales_daily` 日级汇总对齐；库存 raw→DWD 自洽，库存 DWD 与 `dws_inventory_daily` 的 `qty` 差 337，原因是生产 ODS/DWS 快照时间点早于本次 Oracle full raw 初始化。

#### Docs
- 同步更新 ODS-DWD-DWS-ADS 架构完善子项目、`docs/MYSQL数据字典.md`、`docs/DATA_CONTRACTS.md` 与 `docs/ETL业务逻辑说明.md`，明确 M3 仍为旁路验证链路，未接总控，当前 DWS / ADS 不消费。

### v0.8.47 — 门店属性同步支持从 Excel 等级列写入 store_grade（2026-05-07）

#### Changed
- 更新 `tools/import_cfg_store_target_daily_from_nas.py`，当启用 `--sync-store-report-attr` 时，现支持从 `导入模板` 的 `等级 / 店铺等级 / 门店等级` 列读取门店等级，并同步写入 `cfg_store_report_attr_snapshot.store_grade` 与 `dim_store_report_attr.store_grade`；若模板未提供等级列或单元格为空，则保持沿用当前有效历史值的兼容行为。
- `etl_ads_store_daily_report.py` 继续直接读取 `dim_store_report_attr.store_grade` 并写入 `ads_store_daily_report.store_grade`，因此在门店属性同步后无需额外改门店日报 ETL。

#### Test
- 新增 `test_import_cfg_store_target_daily_from_nas.py` 覆盖等级列解析、Excel 等级优先写入以及等级列为空时沿用历史值的场景。

#### Docs
- 同步更新 `docs/ETL业务逻辑说明.md` 与 `docs/数据结构与映射手册.md`，补记门店属性同步时的等级列来源与兼容边界。

### v0.8.46 — 门店属性导入切换为快照输入 + 历史承接（2026-05-06）

#### Changed
- 更新 `tools/import_cfg_store_target_daily_from_nas.py`，当启用 `--sync-store-report-attr` 时，现改为先按 `target_month + target_version` 覆盖写入 `cfg_store_report_attr_snapshot`，再从快照表同步 `dim_store_report_attr` 历史版本，继续沿用未变化 / 变更 / 新增 / 退出分类与自动关旧开新逻辑。
- 调整门店属性重叠保护：不再把“上一版历史仍处于有效区间”单独视作致命覆盖冲突；当前仅在 `cfg_store_report_attr_snapshot` 出现重复 `store_id`，或 `dim_store_report_attr` 在同一生效日对同店命中多条当前有效记录时直接失败。

#### Database / SQL
- 新增 `SQL/create_store_report_attr_snapshot.sql`，提供 `cfg_store_report_attr_snapshot` 建表 DDL；该表用于承接业务当前快照，首次启用前仍需由用户人工执行建表。

#### Test
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest -v test_scheduled_store_daily_report.py test_import_cfg_store_target_daily_from_nas.py`，当前 18 个相关单元测试全部通过，覆盖“旧版本仍有效但不再误拦截”与“真实当前重叠仍直接失败”场景。

#### Docs
- 同步更新 `docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/ETL业务逻辑说明.md`、`docs/RUNBOOK.md` 与 `docs/数据仓库与ETL手册.md`，补记门店属性改为“快照输入 + 历史承接”、首次启用建表前置，以及专题调度继续只读历史表的边界。

### v0.8.45 — 门店目标未命中 dim_store 改为 warning 跳过（2026-05-06）

#### Fixed
- 更新 `tools/import_cfg_store_target_daily_from_nas.py`，将“部分门店未命中 dim_store”从致命校验改为 `WARNING`，写出候选建议并跳过这些门店相关的目标、门店属性和共同考核归属配置；同时新增安全阀，若导入模板全部门店都未命中，或共同考核归属行会被整体跳空，仍立即失败，避免空覆盖当月配置。
- 更新 `scheduled_store_daily_report.py`，当目标导入仅命中未建店门店时，专题调度现改为输出 `WARNING` 到企微与总控摘要，继续执行负责人导入、受影响日期判断与六层 ADS 重跑，不再把整条链路标记为 `FAILED`。

#### Test
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest -v test_scheduled_store_daily_report.py test_import_cfg_store_target_daily_from_nas.py`，当前 16 个相关单元测试全部通过，覆盖 WARNING 摘要、未命中门店跳过与主店缺失连带跳过场景。

#### Docs
- 同步更新 `docs/ARCHITECTURE.md`、`docs/RUNBOOK.md` 与 `docs/数据仓库与ETL手册.md`，补记门店未命中 `dim_store` 的 WARNING + skip 行为、企微/总控摘要语义，以及全量未命中时的安全失败边界。

## 2026-04-30

### v0.8.44 — M3 raw / DWD 小窗口真实装载（2026-04-30）

#### Added
- 新增 `etl_m3_load_utils.py`，封装 M3 raw / DWD 小窗口 upsert、标识符校验与 DataFrame 清洗公共逻辑。
- 新增 `reports/context_cache/m3_raw_load_window_probe_20260430.json`，记录 raw ODS 装载前 Oracle 近 1 天只读行数探查。
- 新增 `reports/context_cache/m3_raw_dwd_small_window_load_20260430.json`，记录 raw ODS 小窗口装载、DWD upsert、主键重复检查、raw→DWD 行数与 DWS 差异边界。

#### Changed
- 将 `etl_ods_m_retail_raw.py`、`etl_ods_m_retailitem_raw.py`、`etl_ods_fa_storage_raw.py` 从只输出 SQL 的骨架升级为默认 dry-run、显式 `--execute` 才 upsert 的旁路装载脚本；full 模式仍需 `--confirm-full-load`。
- 将 `etl_dwd_sales_retail_item.py`、`etl_dwd_inventory_storage_snapshot.py` 从只输出 SQL 的骨架升级为默认 dry-run、显式 `--execute` 才 upsert 的 DWD 小窗口装载脚本，并在写入前检查 raw 源非空。
- 更新 `SQL/check_dwd_sales_retail_item_min.sql` 与 `SQL/check_dwd_inventory_storage_snapshot_min.sql` 默认对账窗口，适配近 1 天 / 当天快照验证。

#### Operations
- 用户已人工执行 `SQL/alter_m3_raw_dwd_update_table_comments_after_create.sql` 修正线上表注释旧字样；Agent 仅做只读核验。
- 已按用户授权执行 raw ODS 近 1 天小窗口装载：`ods_m_retail_raw` 962 行、`ods_m_retailitem_raw` 2103 行、`ods_fa_storage_raw` 5741 行。
- 已按用户授权执行 DWD 小窗口写入：`dwd_sales_retail_item` 2103 行、`dwd_inventory_storage_snapshot` 5741 行；主键重复检查均为 0。

#### Docs
- 同步更新 ODS-DWD-DWS-ADS 架构完善子项目文档、`docs/MYSQL数据字典.md`、`docs/DATA_CONTRACTS.md` 与 `docs/ETL业务逻辑说明.md`，明确 M3 仅为旁路验证数据，未接总控，当前 DWS / ADS 不消费。

### v0.8.43 — M3 raw / DWD 建表状态校准（2026-04-30）

#### Added
- 新增 `reports/context_cache/m3_manual_ddl_verification_20260430.json`，记录用户人工完成 5 张 M3 raw / DWD 表建表后的只读核验结果：5 表均存在、当前为空表、剔除字段未残留。
- 新增 `SQL/alter_m3_raw_dwd_update_table_comments_after_create.sql`，仅作为用户人工修正线上表注释的可选 SQL；Agent 未执行 ALTER。

#### Changed
- 更新 5 份 M3 raw / DWD DDL 文件头部状态与表注释，将“未执行草案”校准为“用户已人工建空表、未装载、未接调度”。

#### Docs
- 同步更新 ODS-DWD-DWS-ADS 架构完善子项目文档、`docs/MYSQL数据字典.md`、`docs/DATA_CONTRACTS.md` 与 `docs/ETL业务逻辑说明.md`，明确 5 表已存在但生产数据契约尚未生效，当前 DWS / ADS 仍不消费这些表。

## 2026-04-28

### v0.8.42 — 将数据库超时测试纳入项目级约束（2026-04-28）

#### Changed
- 更新 `.github/copilot-instructions.md`、`.claude/CLAUDE.md` 与 `AGENTS.md`，要求后续新增或修改任何涉及数据库读写的 ETL、调度、工具脚本或 SQL 时，必须显式评估 `db_connections.py` 的 `timeout_profile`，并在开发/排障阶段保留超时测试证据。

#### Docs
- 同步更新 `docs/ARCHITECTURE.md`，补记统一连接工厂的 `default / etl / long_running` 三档超时治理要求，以及 sales_org 日/月汇总已作为长跑写库样板接入 `long_running`。

## 2026-04-27

### v0.8.41 — hefang_dw 统一数据库连接工厂（2026-04-27）

#### Added
- 新增 `db_connections.py`，统一 MySQL SQLAlchemy Engine、PyMySQL 直连、Oracle SQLAlchemy Engine 与 Oracle 直连创建入口，集中配置连接池大小、最大溢出、等待超时、空闲回收与 MySQL 连接/读/写超时。

#### Changed
- 更新 hefang_dw 的 ODS / DIM / DWS / ADS / 调度 / 测试 / 工具脚本，改为通过 `create_mysql_engine()`、`connect_mysql()`、`create_oracle_engine()` 与 `connect_oracle()` 获取连接；保留原有事务、`cursorclass`、`autocommit`、`dispose()` 与 `close()` 调用语义。
- 本轮按用户要求不修改 `dabo_etl`。

#### Docs
- 同步更新 `.env.example`、`README.md`、`docs/ARCHITECTURE.md` 与 `docs/RUNBOOK.md`，补充连接工厂说明和连接池 / 超时环境变量。

#### Test
- 已执行排除 `.conda`、`.venv`、`example_repos` 与 `logs` 后的项目 Python 编译检查，当前 115 个 Python 文件通过语法检查。
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_ads_sku_daily.py test_ods_incremental_utils.py test_scheduled_store_daily_report.py test_scheduled_total_control.py test_store_operation_owner_import.py`，当前 22 个单元测试全部通过。
- 已执行 `D:/Anaconda/envs/pyproject/python.exe scripts/check_doc_sync.py --output reports/docs_code_alignment.json`，完成文档同步审计 JSON 刷新。

### v0.8.40 — 门店销售专题同日多次总控 freshness 修复（2026-04-27）

#### Changed
- 更新 `scheduled_store_daily_report.py`，在自然日覆盖已到统一上界时继续比较近 7 天 `dws_sales_daily.etl_time` 与六张专题 ADS 的 `etl_time`；若主链 DWS 更新晚于专题 ADS，则按 freshness 命中日期触发重跑，避免同一天第 2/第 3 次总控后专题 ADS 误判 `SKIPPED`。
- 将 `ads_sales_org_monthly` 接入门店销售专题批量重跑链，专题调度顺序扩展为“门店层 -> 主体层 -> 销售看板月度战役 -> SKU 汇总 -> 销售组织日汇总 -> 销售组织月汇总”。

#### Database / SQL
- 更新 `SQL/create_ads_sales_org_monthly.sql` 的表备注，注明该表已接入 `scheduled_store_daily_report.py` 专题第六层，但仍未接入 `run_etl.py` 主链。

#### Operations
- 已按用户授权完成 `2026-04-20` ~ `2026-04-26`、`data_version=v2` 的五张日级专题 ADS 显式补跑，旧进程日志显示 `requested=7, completed=7, failed=0`；本轮后续单独补齐 `ads_sales_org_monthly` 在同一日期范围内的月级组织 ADS，7 个 `report_date` 均回查输出 72 行，`target_month` 覆盖 2026 年 1 月到 4 月。

#### Test
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_store_daily_report.py test_scheduled_total_control.py`，当前 15 个相关单测全部通过，覆盖本轮新增 freshness 与六层调用顺序。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/DATA_CONTRACTS.md` 与 `docs/数据结构与映射手册.md`，补记六层专题 ADS 与 DWS freshness 规则。

### v0.8.39 — 总控统一汇总主链与门店销售专题链执行摘要（2026-04-27）

#### Added
- 新增 `control_chain_summary.py`，沉淀总控与子链之间的结构化摘要协议，统一约定摘要输出路径与“抑制子链企业微信”环境变量，供后续新增专题复用接入。
- 新增 `test_scheduled_total_control.py`，覆盖“总控统一摘要包含主链与专题链 section”“双链成功时只发送一条统一企业微信摘要”“主链失败时专题链标记 `SKIPPED`”三类场景。

#### Changed
- 更新 `run_etl.py`，主链现支持在总控模式下输出结构化摘要并抑制自身企业微信；直接单独运行时仍保持原有主链摘要发送行为。
- 更新 `scheduled_store_daily_report.py`，专题调度现支持把 SUCCESS / SKIPPED / FAILED / 显式批量重跑结果输出为结构化摘要；若由总控触发，则抑制子链单独企业微信并回传摘要给总控。
- 更新 `scheduled_total_control.py`，总控现通过链路注册表串联主链与门店销售专题链，统一读取子链摘要并发送唯一企业微信出口，后续新增专题可按同一协议接入。

#### Test
- 已执行 `D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_store_daily_report.py test_scheduled_total_control.py`，当前 14 个相关单测全部通过。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md` 与 `docs/数据仓库与ETL手册.md`，补记总控统一企业微信出口、结构化摘要协议与后续专题接入方式。

## 2026-04-23

### v0.8.38 — 扩面治理 ads/dim/cfg comment 漂移并修复 dim_product_attr 回退根因（2026-04-24）

#### Fixed
- 更新 `etl_dim_product.py`，`dim_product_attr` 现改为先执行固定 DDL，再 `TRUNCATE + append` 写入，不再使用 `to_sql(if_exists='replace')` 每次重建整表冲掉表备注与列注释。

#### Database
- 新增 `SQL/create_dim_product_attr.sql`，沉淀 `dim_product_attr` 的固定建表结构与注释。
- 新增 `SQL/alter_ads_dim_cfg_comment_alignment.sql`，统一补齐 `ads_dabo_daily_sales`、`ads_dabo_order_bridge`、`ads_dabo_order_label`、`ads_dabo_order_retail_bridge`、`dim_product_attr` 与部分 `ads_inventory_health` / `cfg_*` / `dim_*` 的现网 comment。
- 更新 `SQL/create_ads_dabo_order_label.sql`、`SQL/create_ads_dabo_order_retail_bridge.sql`、`SQL/达播数据ETL建表.sql` 与 `dabo_etl/sql/create_tables_mysql.sql`，补齐达播相关表的建表 comment，避免后续重建再次回退。

#### Docs
- 同步更新 `docs/MYSQL数据字典.md`，补记 `ads_dabo_order_bridge` 字典，并刷新达播兼容表、`dim_product_attr` 与 `ads_inventory_health` 的字段说明。

### v0.8.37 — 根治门店日报专题调度锁竞争放大（2026-04-23）

#### Fixed
- 更新 `scheduled_store_daily_report.py`，在进入专题调度主循环前新增顶层命名锁 `hefang_dw:scheduled_store_daily_report`；若已有另一条专题调度在跑，本次立即退出，不再把外层重试放大成多条并发包装层实例。
- 更新 `etl_ads_daily_sales.py`、`etl_ads_sku_daily.py`、`etl_ads_sales_org_daily.py` 与 `etl_ads_sales_org_monthly.py`，销售主题 ADS 仍保持各表独立命名锁串行化，但现在会在事务结束后显式执行 `RELEASE_LOCK`，减少连接异常或手工中断后的灰色持锁窗口。

#### Test
- 扩充 `test_scheduled_store_daily_report.py`，新增专题调度单实例锁的最小单元测试，覆盖“显式重跑路径会先申请顶层锁”和“锁已被占用时直接返回错误”两类场景；`python -m unittest test_scheduled_store_daily_report.py` 当前已通过 7 个测试。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md` 与 `docs/数据仓库与ETL手册.md`，明确锁问题根因在于“包装层重复触发 + 子任务表锁串行化”的叠加，并补记新的单实例锁防重入策略。

### v0.8.36 — 销售主题 ADS 统一切换为 report_channel_type 明细口径（2026-04-23）

#### Changed
- 更新 `etl_ads_daily_sales.py`、`etl_ads_sales_org_daily.py`、`etl_ads_sales_org_monthly.py`、`etl_ads_sku_daily.py` 与 `etl_ads_store_daily_subject_report.py`，销售主题 ADS 主输出统一改为 `report_channel_type` 细分类，不再使用 `report_channel_type_group` 作为主口径，也不再物化 `area_name='全国'` / `report_channel_type='全部'` 类物理汇总行。
- 同步调整 `SQL/create_ads_daily_sales.sql`、`SQL/create_ads_sales_org_daily.sql`、`SQL/create_ads_sales_org_monthly.sql`、`SQL/create_ads_sku_daily.sql`、`SQL/create_store_daily_assessment_tables.sql` 及对应 alter 脚本，确保目标表注释、唯一键和粒度说明与新口径一致。
- 更新 `SQL/check_ads_daily_sales_min.sql`、`SQL/check_ads_sales_org_daily_min.sql`、`SQL/check_ads_sales_org_monthly_min.sql`、`SQL/check_ads_sku_daily_min.sql`，最小对账由依赖 `全国/全部` 物理总盘行改为按全部 `area_name + report_channel_type` 明细切片聚合核对。

#### Docs
- 同步更新 `README.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/ARCHITECTURE.md`、`docs/ETL业务逻辑说明.md`、`docs/数据结构与映射手册.md`、`docs/SQL开发手册.md`，明确销售主题 ADS 现口径为 `report_channel_type` 明细切片，物理层不再生成 `全国/全部` 汇总行。
- 同步更新 `docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md` 与 `docs/销售部数据治理-子项目/销售看板Tableau实施与追踪.md`，将消费层方案收敛为“由 Tableau 聚合全部明细切片”，不再依赖物理总盘成员。

### v0.8.35 — 对齐主链 dws_sales 回带窗口并完成 2026-04-21/22 复对账（2026-04-23）

#### Fixed
- 更新 `run_etl.py`，新增 `ODS_INCREMENTAL_BACKFILL_DAYS` 与 `DWS_SALES_MAINLINE_DAYS_BACK`，主链在 `ods_sync` 回刷最近 7 天后，固定执行 `etl_dws_sales.run(days_back=7, include_today=True)`，避免 ODS 晚到补数只停留在 ODS 层。

#### Database
- 已执行 `python run_etl.py`，确认 `dws_sales_daily` 的 `20260421` / `20260422` 已按主链近 7 天窗口重刷成功，其中净额分别恢复为 `547226.41` / `616700.65`。
- 已执行 `scheduled_store_daily_report.py --rerun-report-date 2026-04-21 --rerun-report-date 2026-04-22 --rerun-data-version v2`，完成两天五层 ADS 重跑；`2026-04-21` 输出 `73/73/567/8561/54`，`2026-04-22` 输出 `73/73/594/8722/54`。

#### Test
- 已完成 Oracle → DWS 日级复对账：`20260421` / `20260422` 的净额分别为 `547226.41` / `616700.65`，与 Oracle 同口径聚合一致；净销量 `720285` / `762` 也与 Oracle 一致。
- 已完成门店日报范围 DWS → `ads_daily_sales` / `ads_sku_daily` / `ads_sales_org_daily` 复对账，两天的金额与 `ads_sku_daily.mtd_sales_qty` 差异均为 `0`。

#### Docs
- 基于 `reports/docs_code_alignment.json`（2026-04-23 11:36:02）同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`，补记主链近 7 天回带窗口、两天专题重跑结果与复对账结论。

### v0.8.34 — 修复 ads_sku_daily 连带贡献精度并跑通 2026-04-22/v2 五层调度（2026-04-23）

#### Changed
- 更新 `etl_ads_sku_daily.py` 的目标列校验，当前会在 `--conn-test` 和正式运行前强制检查 `attach_contribution` 至少为 `DECIMAL(14,2)`，避免旧版物理表在专题调度第五层再次因精度不足失败。
- 显式执行 `scheduled_store_daily_report.py --rerun-report-date 2026-04-22 --rerun-data-version v2`，已完成 `ads_store_daily_report=73`、`ads_store_daily_subject_report=73`、`ads_daily_sales=594`、`ads_sku_daily=8600`、`ads_sales_org_daily=54` 的五层写库验证；期间 `ads_sales_org_daily` 曾因命名锁 `hefang_dw:ads_sales_org_daily` 触发可重试等待，但目标日最终已补齐落库。

#### Database
- 补充 `SQL/alter_ads_sku_daily_widen_attach_contribution_precision.sql` 作为旧版 `ads_sku_daily` 的精度修复脚本，用于把 `attach_contribution` 从 `DECIMAL(7,2)` 放宽到 `DECIMAL(14,2)`。

#### Test
- 新增 `test_ads_sku_daily.py` 最小单测，覆盖“旧精度会被识别并提示 widen 脚本”与“`DECIMAL(14,2)` 可通过校验”两类场景；`python -m unittest test_ads_sku_daily.py` 当前已通过 2 个测试。
- 已执行 `python etl_ads_sku_daily.py --conn-test`，确认当前源依赖与目标列精度检查均可通过。

#### Docs
- 基于 `reports/docs_code_alignment.json`（2026-04-22 17:08:49）同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/RUNBOOK.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/数据结构与映射手册.md`，补记 `attach_contribution` 精度要求与 2026-04-22/v2 五层调度结果。

## 2026-04-22

### v0.8.33 — 负责人接入门店日报专题调度并下沉日报字段（2026-04-22）

#### Changed
- 更新 `etl_ads_store_daily_report.py`，在最终经营实体粒度新增 `owner_name` 下沉逻辑，并把负责人历史切片纳入运行前校验；当负责人历史为空、当前日期命中切片重叠，或经营实体缺少有效切片时，门店日报 ETL 直接失败。
- 更新 `scheduled_store_daily_report.py`，将负责人快照导入正式接入专题调度主流程；当前实际顺序变为“目标导入/幂等跳过 -> 负责人导入/幂等跳过或禁用 -> 合并受影响日期 -> 五层 ADS 批量重跑”，并新增 `--owner-file-path`、`--owner-sheet-name`、`--owner-snapshot-date`、`--no-run-owner-import` 参数。

#### Database
- 新增 `SQL/alter_ads_store_daily_report_add_owner_name.sql`，用于在 `ads_store_daily_report.store_name` 后补 `owner_name` 字段；该 SQL 已落仓，但现网是否执行仍以用户当轮授权为准。

#### Test
- 扩充 `test_scheduled_store_daily_report.py`，新增负责人受影响日期起点截断与目标链路/负责人链路日期并集合并的最小单元测试；`python -m unittest test_scheduled_store_daily_report.py` 当前已通过 5 个测试。

#### Docs
- 基于 `reports/docs_code_alignment.json`（2026-04-22）同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md` 与 `docs/ETL业务逻辑说明.md`，补记负责人快照已接入专题调度、`ads_store_daily_report` 的 `owner_name` 字段来源，以及待执行 alter 的边界。

### v0.8.32 — 冻结负责人映射工作簿录入口径（2026-04-22）

#### Changed
- 更新 NAS 正式文件 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx`，新增 `填写说明` sheet，并在数据表头补充批注，把“当前真值快照、共同考核只维护 SUBJECT、负责人可为空、不维护 Excel 历史区间、同一实体只保留一行”的业务录入口径直接冻结在工作簿内。

#### Docs
- 同步更新 `README.md`、`docs/业务逻辑与指标规范.md`、`docs/数据仓库与ETL手册.md` 与 `docs/ETL业务逻辑说明.md`，明确正式 NAS 文件内置说明页与表头批注，且说明页不参与导入。

## 2026-04-21

### v0.8.31 — 新增门店经营负责人快照导入与 SCD2 承接链路（2026-04-21）

#### Added
- 新增 `SQL/create_store_operation_owner_tables.sql`，创建 `cfg_store_operation_owner_snapshot`、`dim_store_operation_owner_assignment` 与 `log_store_operation_owner_import` 三张负责人链路表。
- 新增 `tools/import_store_operation_owner_from_nas.py`，从 NAS 当前快照读取负责人映射，校验经营实体清单，并在 `--apply` 时同步维护当前快照、SCD2 历史与导入日志。
- 新增 `test_store_operation_owner_import.py`，覆盖“共同考核经营体吸收成员店后只保留主体行”与“历史切片 changed/new/exited 分类”两类最小单测。

#### Changed
- 冻结负责人映射业务语义为“业务只维护当前快照，历史由 MySQL 内部 SCD2 维护”；共同考核存在时，负责人快照仅维护经营体行，不再同时维护被吸收的 RT 成员门店。

#### Test
- 已完成新脚本语法/问题检查，并通过 `python -m unittest test_store_operation_owner_import.py` 最小单元测试。

#### Docs
- 基于 `reports/docs_code_alignment.json`（2026-04-21 17:11:26）同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md` 与 `docs/业务逻辑与指标规范.md` 的负责人快照链路说明；数据契约、字典与映射文档在同轮继续补齐。

## 2026-04-17

### v0.8.30 — 完成 ads_sku_daily 第五层专题调度实跑验证（2026-04-17）

#### Changed
- 显式执行 `scheduled_store_daily_report.py --rerun-report-date 2026-04-15 --rerun-data-version v2`，完成门店层、主体层、月度战役、SKU 汇总和销售组织汇总五层 ADS 写库验证；其中 `ads_sku_daily` 输出 `7168` 行。

#### Test
- 复用 `SQL/check_ads_sku_daily_min.sql` 的最小对账逻辑完成只读复核，`row_count_and_unique_key`、`grand_total_compare`、`top20_sku_rank_compare`、`derived_field_coverage` 均返回 `OK`。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md` 与销售看板专项清单，将第五层状态统一修正为“已实跑验证”，并保留 `category_health_tag` 继续预留的业务边界。

### v0.8.29 — 将 ads_sku_daily 接入专题调度第五层（2026-04-17）

#### Changed
- `scheduled_store_daily_report.py` 在受影响日期批量重跑时，当前会按“门店层 -> 主体层 -> 销售看板月度战役 -> SKU 汇总 -> 销售组织汇总”顺序触发五层 ADS；摘要、失败告警与 CLI 文案同步更新为五层语义。
- `ads_sku_daily` 当前数据库状态同步更新为“已完成 2026-04-15 / v1 正式写库与最小对账”，但接入专题调度后的显式数据库重跑仍待用户按需授权。

#### Test
- 扩充 `test_scheduled_store_daily_report.py`，覆盖五层 ADS 调用顺序，以及新增 `ads_sku_daily` 层失败时的剩余日期续跑上下文。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md` 与 `docs/MYSQL数据字典.md`，将 `ads_sku_daily` 从“未接调度/未正式写库”的旧表述修正为“已正式写库、已接专题调度代码链、接链后显式重跑待授权”。

### v0.8.28 — 固化 ads_sku_daily 连带贡献口径并补 ODS 订单级派生（2026-04-17）

#### Added
- 新增 `SQL/alter_ads_sku_daily_add_attach_contribution.sql`，用于现网已补三项派生字段后的增量补列。

#### Changed
- `ads_sku_daily` 新增 `attach_contribution` 字段，口径冻结为“含A订单中非A商品销售额 / 含A订单总金额 * 100%”，并在 ODS `ods_m_retail + ods_m_retailitem` 上按订单级共购关系计算。
- `etl_ads_sku_daily.py --conn-test` 若发现目标表已存在但字段缺失，会输出旧结构告警；正式 `run` 会在目标表缺字段时直接失败，避免旧表结构静默跑错。
- `category_health_tag` 继续不物化，消费侧暂结合销售占比与库存侧占比粗看。
- `SQL/check_ads_sku_daily_min.sql` 新增 `attach_contribution` 非空覆盖检查。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/业务逻辑与指标规范.md`、`docs/SQL开发手册.md` 与销售看板专题文档，明确连带贡献已冻结并入表，品类健康标签仍后置。

### v0.8.27 — 扩展 ads_sku_daily 三项二期派生字段（2026-04-17）

#### Added
- 新增 `SQL/alter_ads_sku_daily_add_phase2_derived_fields.sql`，用于现网已建 `ads_sku_daily` 的手工增量结构变更。

#### Changed
- `ads_sku_daily` 在事实字段基础上新增 `sales_mix_pct`、`rank_no`、`trend_tag` 三项二期派生；其中销售占比按同切片月累计销售额占比计算，排名按 `mtd_sales_amt` 降序稳定排序，趋势标签按近 7 天 / 30 天净销量加速度输出。
- `attach_contribution` 与 `category_health_tag` 继续保持后置，不在本轮 DDL 和 ETL 中物化。
- `SQL/check_ads_sku_daily_min.sql` 新增派生字段覆盖检查，验证销售占比、排名和趋势标签已写出且排名序列连续。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md` 与销售看板专题文档，明确 ads_sku_daily 当前已补三项二期派生，但仍未接入专题调度。

## 2026-04-16

### v0.8.26 — 新增 ads_sales_org_monthly 与 ads_sku_daily 两张销售看板仓库样板（2026-04-16）

#### Added
- 新增 `SQL/create_ads_sales_org_monthly.sql`、`etl_ads_sales_org_monthly.py` 与 `SQL/check_ads_sales_org_monthly_min.sql`，补齐销售看板“年度 KPI 与 12 个月节奏 + 渠道结构”样板表的 DDL、独立 ETL 与最小对账 SQL。
- 新增 `SQL/create_ads_sku_daily.sql`、`etl_ads_sku_daily.py` 与 `SQL/check_ads_sku_daily_min.sql`，补齐销售看板“商品品类结构 + SKU Top20”样板表的 DDL、独立 ETL 与最小对账 SQL。

#### Changed
- `ads_sales_org_monthly` 首版冻结为“月度与 YTD 实际链路先落地、总年标字段暂按空值策略保留”的样板方案；`annual_target_amt` 与 `ytd_target_amt` 当前不向组织明细下沉。
- `ads_sku_daily` 首版冻结为事实层样板，只落 `day_sales_amt`、`mtd_sales_amt`、`mtd_sales_qty`、`mtd_order_cnt` 四类核心字段，不物化连带贡献与品类健康诊断标签。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md` 与销售看板专题文档，明确这两张表已完成仓内落包、`--conn-test` 已通过，但仍未接入 `run_etl.py` 主链，且未获授权建表与写库。

### v0.8.25 — 将 ads_sales_org_daily 接入门店日报专题调度第四层并完成实跑验证（2026-04-16）

#### Changed
- `scheduled_store_daily_report.py` 当前在受影响日期批量重跑时，会按顺序触发 `ads_store_daily_report`、`ads_store_daily_subject_report`、`ads_daily_sales` 与 `ads_sales_org_daily` 四张 ADS，并同步更新摘要、失败告警与 CLI 文案。

#### Added
- 扩充 `test_scheduled_store_daily_report.py`，将最小单元测试升级为覆盖四层 ADS 调用顺序，以及第四层 `ads_sales_org_daily` 失败时的续跑上下文保留。

#### Database
- 已执行 `scheduled_store_daily_report.py --rerun-report-date 2026-04-15 --rerun-data-version v2`，完成 `ads_store_daily_report=73`、`ads_store_daily_subject_report=73`、`ads_daily_sales=405`、`ads_sales_org_daily=54` 的四层 ADS 写库验证。
- 已回查 `ads_sales_org_daily` 的 `row_count_and_unique_key`、`mtd_total_compare`、`ytd_total_compare`，结果均为 `OK`。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/DATA_CONTRACTS.md` 与销售看板专题文档，将 `ads_sales_org_daily` 从“建议接链”更新为“已接入专题调度并完成四层实跑验证”。

### v0.8.24 — 修正门店日报目标 NAS 根目录并完成专题调度实跑验证（2026-04-16）

#### Fixed
- `tools/import_cfg_store_target_daily_from_nas.py` 的默认 NAS 根目录从历史 `月度日目标配置表` 修正为当前实际使用的 `目标配置表`，恢复专题调度与导入工具在现网目录下的自动找档能力。

#### Database
- 已执行 `scheduled_store_daily_report.py --target-month 2026-04`，确认当前 `202604考核数据配置表.xlsx` 因 `file_md5 + target_month + target_version` 已存在 `SUCCESS` 记录而按设计 `SKIPPED`。
- 已执行 `scheduled_store_daily_report.py --rerun-report-date 2026-04-15 --rerun-data-version v2`，完成 `ads_store_daily_report`、`ads_store_daily_subject_report`、`ads_daily_sales` 三层 ADS 写库验证。
- 已执行 `etl_ads_sales_org_daily.py --report-date 2026-04-15 --data-version v2`，并完成 `SQL/check_ads_sales_org_daily_min.sql` 的 `row_count_and_unique_key`、`mtd_total_compare`、`ytd_total_compare` 最小对账，结果均为 `OK`。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/DATA_CONTRACTS.md` 与销售看板专题文档，改写“尚未验证”的旧表述，补充幂等跳过、显式 rerun 写库验证，以及 `ads_sales_org_daily` 建议纳入专题调度的结论。

### v0.8.23 — 将 ads_daily_sales 接入门店日报专题调度并补最小单元测试（2026-04-16）

#### Changed
- `scheduled_store_daily_report.py` 当前在受影响日期批量重跑时，会按顺序触发 `ads_store_daily_report`、`ads_store_daily_subject_report` 与 `ads_daily_sales` 三张 ADS，并同步更新摘要、失败告警与 CLI 文案。

#### Added
- 新增 `test_scheduled_store_daily_report.py`，用最小单元测试覆盖三层 ADS 调用顺序、失败续跑上下文保留，以及跳过说明文案包含销售看板的信息。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/DATA_CONTRACTS.md` 与销售看板专题文档，明确当时仅完成 `ads_daily_sales` 的专题调度代码接入与最小单元测试；后续真实写库验证已在 `v0.8.24` 补记。

### v0.8.22 — 执行 ads_daily_sales 首轮样本并修复最小对账 SQL 排序规则冲突（2026-04-16）

#### Fixed
- 修复 `SQL/check_ads_daily_sales_min.sql` 在当前 MySQL 库上比较 `area_name='全国'` / `report_channel_type_group='全部'` 时的排序规则冲突，改为显式指定 `utf8mb4_0900_ai_ci`，避免 `Illegal mix of collations` 报错。

#### Database
- 已执行 `etl_ads_daily_sales.py --report-date 2026-04-15 --data-version v1`，首轮样本写入 `405` 行。
- 已完成 `SQL/check_ads_daily_sales_min.sql` 最小对账，其中 `row_count_and_unique_key=OK`、`grand_total_series_compare=OK`。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/数据仓库与ETL手册.md`、`docs/数据结构与映射手册.md` 与销售看板专题文档，将 `ads_daily_sales` 状态更新为已完成首轮样本验证。

## 2026-04-15

### v0.8.21 — 新增 ads_daily_sales 仓库样板并回写销售看板文档真值（2026-04-15）

#### Added
- 新增 `SQL/create_ads_daily_sales.sql`，定义销售看板“月度战役”样板表 `ads_daily_sales` 的首版结构。
- 新增 `etl_ads_daily_sales.py`，按 `battle_month = report_date` 所在自然月月初、`sales_date = 月初至 report_date` 的规则产出日目标、日实际与累计字段。
- 新增 `SQL/check_ads_daily_sales_min.sql`，最小对账覆盖行数、唯一键与全国总盘整段日序列核对。

#### Changed
- 将 `ads_sales_org_daily` 的文档状态从“待数据库验证”更新为“当前数据库已完成 `2026-04-14 / v1` 单日建表与两轮最小对账验证，但仍未接入主调度”。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`，补充 `ads_daily_sales` 独立入口、执行命令与 `ads_sales_org_daily` 的最新验证状态。

## 2026-04-10

### v0.8.20 — 新增门店日报共同考核统计主体层（2026-04-10）

#### Added
- 新增 `SQL/create_store_daily_assessment_tables.sql`，创建 `cfg_store_assessment_subject_target_daily`、`cfg_store_assessment_assignment` 与 `ads_store_daily_subject_report`。
- 新增 `etl_ads_store_daily_subject_report.py`，在门店原子层日报基础上生成统计主体层日报。

#### Changed
- `tools/import_cfg_store_target_daily_from_nas.py` 扩展为兼容四 sheet 工作簿，在原有 `导入模板` 基础上可选同步 `统计主体目标` 与 `门店考核归属` 两张 sheet，并保留旧单 sheet 模式兼容。
- `scheduled_store_daily_report.py` 更新为在受影响日期上按“门店层 -> 统计主体层”顺序重跑日报，并把共同考核配置同步计入调度摘要。

#### Docs
- 同步更新 `README.md`、`docs/MYSQL数据字典.md`、`docs/DATA_CONTRACTS.md`、`docs/数据结构与映射手册.md`、`docs/ETL业务逻辑说明.md`、`docs/数据仓库与ETL手册.md`、`docs/ARCHITECTURE.md`、`docs/业务逻辑与指标规范.md`、`docs/SQL开发手册.md` 与 `docs/RUNBOOK.md`，明确共同考核多 sheet 导入、统计主体层口径与专题调度双层重跑行为。

## 2026-04-09

### v0.8.19 — 为达播订单标签新增 canonical 桥接层（2026-04-09）

#### Added
- 新增 `SQL/alter_ads_dabo_order_label_add_normalization_fields.sql`，为现网 `ads_dabo_order_label` 补 canonical 与归一审计字段。

#### Changed
- `SQL/create_ads_dabo_order_label.sql` 增加 `canonical_system_order_id`、`normalization_status`、`normalization_rule`、`normalization_evidence` 字段，并补充 canonical 索引。
- `tools/load_dabo_order_labels_from_nas.py` 在装载阶段新增“精确命中优先 + 同文件唯一 superset 候选”归一逻辑；当前样本 `订单管理20260402093825.xlsx` 的 2 条未命中小红书组合单已自动归一并重装成功。
- `tools/query_data.py` 的 `mysql_dabo_tagged_daily_by_billdate` 模板改为优先使用 `COALESCE(canonical_system_order_id, system_order_id)` 与 `ods_m_retail.oms_sourcecode` 做桥接。

#### Database
- 已通过 `tools/load_dabo_order_labels_from_nas.py --apply` 对 `订单管理20260402093825.xlsx` 重装标签表，现网结果为 `exact_hit=484`、`auto_alias=2`、`normalization_unresolved_count=0`。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/SQL开发手册.md`、`docs/ETL业务逻辑说明.md`、`docs/数据仓库与ETL手册.md`、`docs/ARCHITECTURE.md` 与 `docs/达播数据运营上传指南.md`，明确原始 `system_order_id` 保留追溯、下游桥接优先使用 canonical 值。

## 2026-04-07

### v0.8.18 — 新增统一 Excel 达播订单标签主线（2026-04-08）

#### Added
- 新增 `SQL/create_ads_dabo_order_label.sql`，定义内部订单标签表 `ads_dabo_order_label`，用于按 `system_order_id` 给 ODS 订单打上“是否达播 / 达播渠道”标签。
- 新增 `tools/load_dabo_order_labels_from_nas.py`，支持从 `订单管理*.xlsx` 生成标签导入摘要，并在用户授权后用 `--apply` 正式写库。

#### Changed
- `tools/extract_dabo_order_candidates_from_nas.py` 新增订单标签导出能力，可在候选集基础上输出去重后的订单标签 CSV。
- `tools/query_data.py` 新增 `mysql_dabo_tagged_daily_by_billdate` 模板，支持基于 `ads_dabo_order_label.system_order_id = ods_m_retail.oms_sourcecode` 按渠道汇总 ODS 日实收、退款和净额。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/SQL开发手册.md` 与达播专题文档，明确当前主线先做订单打标，再在 ODS / SQL 层按标签计算达播指标。

### v0.8.17 — 为 run_ods 全量增加固定 as-of 的尾部补追（2026-04-07）

#### Changed
- `run_ods.py` 在 `--full` 模式下新增 recent catch-up 阶段：`ods_m_retail` 与 `ods_m_retailitem` 全量完成后，会按同一个固定 `as-of` 再跑一轮最近窗口的增量补追，默认回刷 1 天，可用 `--full-catchup-days` 调整或设为 `0` 关闭。
- `etl_ods_m_retail.py` 与 `etl_ods_m_retailitem.py` 的增量模式新增 `as_of` 截止时间支持，使 full 后补追与后续质检能够共享同一个时间截面。

#### Docs
- 同步更新 `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md`、`docs/数据仓库与ETL手册.md` 与 `docs/ETL业务逻辑说明.md`，说明 full 后 recent catch-up 的默认行为与参数。

## 2026-04-03

### v0.8.16 — 新增门店日报目标 NAS 导入脚本（2026-04-03）

#### Added
- 新增 `tools/import_cfg_store_target_daily_from_nas.py`，默认从固定 NAS 文件读取 `导入模板` sheet，支持只读 dry-run 与显式 `--apply` 写库两种模式。
- 新增 `SQL/create_log_store_target_import.sql`，为门店日报目标导入补齐执行日志表 DDL。

#### Changed
- 门店日报目标导入改为按首行表头月宽表读取，并按 `store_name` 做大小写不敏感匹配后展开为 `cfg_store_target_daily` 日粒度记录。
- `--apply` 模式按目标月份 + 目标版本先删后插 `cfg_store_target_daily`，并在成功或失败时写入 `log_store_target_import`。

#### Docs
- 同步更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/数据仓库与ETL手册.md`、`docs/数据结构与映射手册.md`、`docs/MYSQL数据字典.md`、`docs/RUNBOOK.md` 与 `docs/TODO_ISSUES.md`，说明导入脚本、日志表 DDL、dry-run / apply 用法与当前剩余阻塞项。

## 2026-04-02

### v0.8.15 — 治理 ODS 重复装载与唯一键（2026-04-02）

#### Changed
- `etl_ods_m_retail.py` 与 `etl_ods_m_retailitem.py` 的增量写入改为“窗口清理 + 分块按源 id 替换写入”，降低源记录跨时间窗移动时留下旧副本的风险。
- 两个 ODS 零售同步模块在 `run()` 上增加 MySQL 命名锁与锁冲突重试，减少同表并发重跑导致的重复装载风险。
- `tools/check_ods_incremental.py` 新增 `duplicate_id_count` 输出，便于治理前后直接复核 ODS 重复业务键是否清零。

#### Database
- `SQL/create_ods_tables.sql` 为 `ods_m_retail.id` 与 `ods_m_retailitem.id` 增加 fresh install 场景下的唯一键定义。
- 新增 `SQL/alter_ods_m_retail_enforce_unique_id.sql` 与 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql`，用于现网先清理历史重复装载，再补唯一键。

#### Docs
- 同步更新 `README.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/RUNBOOK.md`、`docs/MYSQL数据字典.md`、`docs/数据仓库与ETL手册.md` 与 `docs/TODO_ISSUES.md`，明确代码治理、DDL 治理与现网手工执行边界。

## 2026-04-01

### v0.8.14 — 修复日报 SQL 月初累计窗口（2026-04-01）

#### Fixed
- 修复 `SQL/==日报数据SQL.sql` 在每月 1 日把月累计窗口算成“本月 1 日到昨天”而出现反向日期区间的问题。
- 将月累计与同期累计统一改为按动态窗口计算：非月初取本月 1 日到昨天；每月 1 日回退为上一个完整自然月，去年同期窗口整体回退 12 个月。

#### Docs
- 同步更新 `docs/SQL开发手册.md`、`docs/业务逻辑与指标规范.md`、`docs/ETL业务逻辑说明.md`，补充日报模板的月初边界口径说明。

## 2026-03-31

### v0.8.13 — oms_sourcecode 历史回填改为分批 apply（2026-03-31）

#### Changed
- `tools/backfill_ods_m_retail_oms_sourcecode.py` 的全量历史回填从单条大 UPDATE 改为按 `id` 范围分批 apply，降低长事务与锁等待风险。
- 新增 `--apply-batch-size` 参数，支持手工控制每批处理的暂存行数；`--apply-only` 继续保留，便于暂存已装载后恢复 apply 阶段。

#### Docs
- 同步更新 `README.md` 与 `docs/ETL业务逻辑说明.md`，说明 `oms_sourcecode` 历史补齐的推荐手工执行方式已改为“先装载暂存，再分批 apply”。

### v0.8.12 — 达播订单改走 ODS 内桥接（2026-03-31）

#### Changed
- `etl_ods_m_retail.py` 新增同步 `OMS_SOURCECODE`，为达播主订单桥接到 ODS 零售头表提供字段基础。
- `tools/query_data.py` 新增 MySQL 模板 `mysql_dabo_actual_daily_by_billdate`，可直接按 `source_file` 统计每日达播实收、退款和净额。
- `mysql_dabo_actual_daily_by_billdate` 现支持自动回退 `ads_dabo_order_retail_bridge`，在 `ods_m_retail.oms_sourcecode` 历史回填未完成时仍可在 MySQL 内出数。

#### Database
- `SQL/create_ods_tables.sql` 为 `ods_m_retail` 增加 `oms_sourcecode` 字段与索引，并将字段长度定为 `VARCHAR(512)`，兼容 Oracle 实际超长来源订单号。
- 为 `ods_m_retail.id` 补充普通索引，避免按 `id` 回填 `oms_sourcecode` 时扫描全表并放大锁冲突。
- 新增 `SQL/alter_ods_m_retail_add_oms_sourcecode.sql`，用于现网增量补列。
- 新增 `SQL/create_ads_dabo_order_retail_bridge.sql`，用于创建达播订单到零售单头的运行层桥接缓存。

#### Docs
- 同步更新 `README.md`、`docs/MYSQL数据字典.md`、`docs/DATA_CONTRACTS.md`、`docs/数据结构与映射手册.md`、`docs/ETL业务逻辑说明.md`，明确达播主订单通过 `OMS_SOURCECODE` 在 MySQL ODS 内桥接的路径。


## 2026-03-18

### v0.8.11 — 收敛第二阶段 agent 描述（2026-03-23）

#### Changed
- 收敛 `.github/agents/*.agent.md` 的 description，补齐更贴近真实提问方式的触发词，减少 agent picker 与自然语言发现时的歧义。
- 保持 5 个 agent 的职责边界不变，本轮重点放在“更容易被找到和看懂”，而不是继续扩张工具或职责范围。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 更新为“hooks 已按逻辑正常执行，本轮推进重心切回第二阶段 agents 可发现性与 description 收敛”。

### v0.8.10 — 新增 Stop 收口提醒试点（2026-03-23）

#### Added
- 扩展 `.github/hooks/post-edit-reminder-hefang.json`，新增 `Stop` 事件，接入 `scripts/copilot_session_close_reminder.ps1`。
- 新增 `scripts/copilot_session_close_reminder.ps1`，基于 `PostToolUse` 日志中的最近命中类型，在会话收口时输出非阻断提醒。

#### Changed
- `Stop` 提醒不直接依赖当前工作树脏状态，而是复用 `logs/copilot_post_edit_reminder.log` 作为最近编辑证据，降低历史未提交改动带来的误报。
- 为避免同一组命中类型在短时间内重复刷屏，新增最近签名去重状态文件 `logs/copilot_session_close_reminder_state.json`。
- 在真实 Copilot 会话中已确认 `Warning from Stop hook` 会显示；同时发现 PowerShell 非零 stderr 的中文文案在宿主 UI 中存在乱码，因此将 Stop 提示文案收敛为 ASCII，优先保证可读性。
- 为进一步降低 Stop warning 卡片中的 PowerShell 错误格式噪音，将 `Stop` 事件的顶层调用从 `pwsh` 切为 `cmd` 包装脚本 `scripts/copilot_session_close_reminder.cmd`，尽量收敛额外的宿主错误元信息。
- 在继续复测后，确认 `cmd` 包装层仍不足以消除宿主中的 `NativeCommandError` 风格噪音，因此将 `Stop` 实现切换为 `python` 脚本，并改走标准输出 + 非零退出码链路，进一步绕开 PowerShell 错误包装。
- 将 `PostToolUse` 事件也切换为 `python` 脚本 `scripts/copilot_post_edit_reminder.py`，并把旧的 `pwsh` 与 `cmd` 入口保留为兼容包装层，减少宿主未热更新配置时继续报旧路径错误的概率。
- 进一步复测发现：当前宿主下 `stdout + exit 1` 会落日志但不稳定展示 warning 卡片，因此将 Python 版 `PostToolUse` 与 `Stop` 的提示输出切回 `stderr`，继续保留 Python 实现以避免 PowerShell 编码与包装噪音。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 补充第二个提醒型 hook 试点，并将当前版本推进到 `v0.17`。

### v0.8.9 — 继续细分 PostToolUse docs 规则（2026-03-23）

#### Changed
- 继续扩展 `scripts/copilot_post_edit_reminder.ps1` 的 docs 匹配规则，在原有会议纪要类、运行文档类、README 类基础上，新增 `data-dictionary` 与 `governance-docs` 两类。
- 将 `MYSQL数据字典.md`、`HFSY数据字典.md` 从运行文档中拆出，将 `AGENT_HANDOFF.md`、`AGENT_LESSONS.md`、`TODO_ISSUES.md` 从运行文档中拆出，使提醒动作更贴近真实收口差异。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 补充 docs 二次细分结论，并将当前版本推进到 `v0.16`。

### v0.8.8 — 细分 PostToolUse docs 提醒规则（2026-03-23）

#### Changed
- 扩展 `scripts/copilot_post_edit_reminder.ps1` 的 docs 匹配优先级，将原先统一的 `doc` 提醒细分为 `meeting-minutes`、`runbook-docs`、`readme` 与兜底 `doc` 四类。
- 为会议纪要类、运行文档类和 README 分别提供更贴近收口动作的提醒文案，降低文档提醒过粗带来的噪音。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 补充 PostToolUse docs 细粒度规则扩展，并将当前版本推进到 `v0.15`。

### v0.8.7 — 调整 PostToolUse warning 返回策略（2026-03-23）

#### Changed
- 将 `scripts/copilot_post_edit_reminder.ps1` 在命中提醒场景下的返回方式从“退出码 0 + JSON `systemMessage`”调整为“非阻断 warning 退出码 + stderr 文案”，以提高 GitHub Copilot UI warning 的展示概率。
- 保留未命中场景的 `{"continue":true}` JSON 成功返回，避免无关编辑被误判为 warning。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 补充 PostToolUse warning 展示排障结论，明确 `systemMessage` 不等同于稳定的 UI warning 卡片，并记录新的试验策略。

### v0.8.6 — 扩展 PostToolUse 提醒粒度（2026-03-23）

#### Changed
- 扩展 `scripts/copilot_post_edit_reminder.ps1` 的 `PostToolUse` 提醒规则，新增 Copilot 自定义能力文件修改场景。
- ETL 提醒补充“最小验证”提示，SQL 提醒补充 `doc-sync`，docs 提醒补充“必要复扫”提示。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 补充 `PostToolUse` 第一轮扩展范围，并明确当前 UI 展示不稳定时应以日志作为执行真值。

### v0.8.5 — 最小提醒型 hook 试点与阶段收口 prompt（2026-03-20）

#### Added
- 新增 `.github/hooks/post-edit-reminder-hefang.json`，作为第三阶段首个 `PostToolUse` 提醒型 hook 试点。
- 新增 `scripts/copilot_post_edit_reminder.ps1`，对 ETL、SQL、docs 和 README 编辑输出非阻断收口提醒。
- 新增 `.github/prompts/stage-close-hefang.prompt.md`，为阶段收口检查提供 prompt 入口。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 记录首个提醒型 hook 试点的行为边界，并补充阶段收口检查 prompt 的定位。

### v0.8.4 — 第三阶段 hooks 设计稿与会议纪要 prompt（2026-03-20）

#### Added
- 新增 `.github/prompts/meeting-minutes-hefang.prompt.md`，将 superpowers / Copilot 能力设计讨论后的会议纪要更新沉淀为单任务 prompt。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 新增第三阶段 hooks 设计稿，明确提醒型、守门型、自动执行型的分层推进建议与当前不启用边界。
- `docs/子项目资料/superpowers内化会议纪要.md` 记录 `meeting-minutes-hefang` prompt 的定位与用途。

### v0.8.3 — 新增运行时验收 prompt（2026-03-20）

#### Added
- 新增 `.github/prompts/runtime-acceptance-hefang.prompt.md`，将 Copilot 自定义能力的运行时验收步骤沉淀为可复用 prompt。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 补充运行时验收 prompt 的定位、边界与当前用途。

### v0.8.2 — Copilot 第二阶段启动（2026-03-20）

#### Added
- 新增 `.github/agents/planner-hefang.agent.md`，为需求澄清、范围界定与实施顺序规划提供角色化入口。
- 新增 `.github/agents/etl-auditor-hefang.agent.md`，为 ETL、调度与测试审计提供只读代理入口。
- 新增 `.github/agents/doc-syncer-hefang.agent.md`，为文档差异归类与修订执行提供角色化入口。
- 新增 `.github/agents/db-inspector-hefang.agent.md`，为快照、结构文档与数据库证据核对提供结构探查入口。
- 新增 `.github/agents/reviewer-hefang.agent.md`，为交付前 review、风险复查与收口检查提供评审入口。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 将第一阶段验收收口为“按用户判定通过、自动触发稳定性保留观察项”，并记录第二阶段 custom agents 已启动。

### v0.7.12 — 重命名 CRM 上下文主文档（2026-03-20）

#### Docs
- 将 `docs/子项目资料/数云CRM数据接入实施计划.md` 重命名为 `docs/子项目资料/数云CRM实施上下文与下一步执行入口.md`，作为切换对话窗口时的统一上下文入口文件。
- 在该文件新增“当前阶段快照”“当前推进进度”“下一步执行入口”“新对话承接方式”，用于后续直接衔接实现阶段。

### v0.7.11 — 补证 HFSY 实表空值与 copy 表重叠（2026-03-20）

#### Docs
- `docs/子项目资料/数云CRM实施上下文与下一步执行入口.md` 纳入三项全表补证结果：`t_member_bind_info` 的 `*1` 列与 `DecryptionTags` 当前全空、`modified` 字符串时间列无空值和异常格式、`t_order_copy*` 与 `t_order` 按 `order_item_id` 100% 重叠。
- `docs/HFSY数据字典.md` 更新使用说明，明确 `*1` 列当前不可依赖，`t_order_copy` 与 `t_order_copy1` 当前应排除出正式消费链路。

### v0.8.0 — Copilot 第一阶段启动（2026-03-20）

#### Added
- 新增 `.github/instructions/python-etl.instructions.md`，将 ETL / 调度 / ETL 自动化测试的领域规则从全局总指令中拆出。
- 新增 `.github/skills/planning-hefang/SKILL.md`，为复杂 ETL / 审计 / 文档同步任务提供“先规划、后实施”的统一入口。
- 新增 `.github/skills/etl-audit-hefang/SKILL.md`，为字段映射、增量逻辑、幂等性和文档同步风险提供只读审计入口。
- 新增 `.github/skills/doc-sync-hefang/SKILL.md`，为代码与文档一致性检查提供统一入口。
- 新增 `.github/skills/completion-check-hefang/SKILL.md`，为任务结束前的验证、交接与经验沉淀检查提供统一入口。

#### Changed
- `.github/copilot-instructions.md` 明确“全局常驻规则”与“ETL 专用 file instructions”的分层关系。

#### Docs
- `docs/子项目资料/superpowers内化会议纪要.md` 由“讨论中”更新为“第一阶段实施中”，并记录首个落地点与下一滚动项。
- `docs/子项目资料/superpowers内化会议纪要.md` 新增第一阶段静态验收结果、运行时人工验收步骤与判定标准。

### v0.7.10 — 同步 HFSY 连接上下文（2026-03-20）

#### Docs
- `docs/HFSY数据字典.md` 补充 `hfsy` 实库连接元信息，明确当前版本为 MySQL `5.7.42`、地址为 `8.134.87.152:33066`、数据库名为 `hfsy`、接入账号为 `shuyun668`。
- `docs/子项目资料/数云CRM实施上下文与下一步执行入口.md` 新增源端连接事实章节，并明确真实密码只作为会话事实存在，不落盘到 git 跟踪文档。
- `docs/RUNBOOK.md` 新增 `hfsy` 只读探查的临时环境变量约定与查询示例。

### v0.7.9 — 数云 CRM 审计发现清单（2026-03-20）

#### Docs
- `docs/子项目资料/数云CRM实施上下文与下一步执行入口.md` 新增“当前审计发现清单”，按 High / Medium / Low 与待补证项固化当前实现边界。
- 将实施计划版本推进到 `v2.5`，明确第一阶段可直接开工、必须延后和仍需补证的对象范围。

### v0.7.8 — 新增 HFSY 数据字典（2026-03-20）

#### Docs
- 新增 `docs/HFSY数据字典.md`，基于 `reports/snapshot_mysql_hfsy_schema.json` 记录数云 `hfsy` 实库的表、字段、注释、键与当前行数。
- `docs/子项目资料/数云CRM实施上下文与下一步执行入口.md` 补充 `hfsy` 快照与数据字典产物，明确后续字段映射和实施设计应直接引用这两份审计产物。

### v0.7.7 — 数云 CRM 实表证据校正（2026-03-20）

#### Docs
- `docs/子项目资料/数云CRM实施上下文与下一步执行入口.md` 纳入数云 xlsx 与 `hfsy` 实表证据，确认当前真实源表为 `t_member_info`、`t_member_bind_info`、`t_trade`、`t_order`、`t_pin_xid_rel`、`sys_area`。
- 将数云侧 MySQL 版本前提从“建议 8.0+”纠偏为“当前实表运行在 5.7.42，实施必须保持 5.7 兼容”。

### v0.7.6 — 单人数据库环境约束（2026-03-19）

#### Changed
- `.github/copilot-instructions.md`、`AGENTS.md`、`.claude/CLAUDE.md` 增加当前开发环境的现实约束：用户为唯一数据库负责人，禁止默认假设存在内部 DBA / 运维协同。

#### Docs
- `docs/ARCHITECTURE.md` 补充 Oracle 位于阿里云、MySQL 与 `hefang_dw` 运行在公司服务器虚拟机的部署边界，以及 CRM 实证材料索取路径。

### v0.7.5 — 经验台帐与复盘机制（2026-03-18）

#### Added
- 新增 `scripts/log_agent_lesson.py`，用于将排障结论、业务纠错与字段语义修正写入经验台帐。
- 新增 `docs/AGENT_LESSONS.md`，作为共享的 Agent 经验台帐。
- 新增 `.opencode/commands/lesson.md`，为 OpenCode 提供手动经验落盘入口。

#### Changed
- `.claude/settings.json` 增加经验复盘提示型 Hook，要求在形成可复用经验后落盘台帐。
- `.github/copilot-instructions.md`、`AGENTS.md`、`README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md` 同步经验台帐机制、命令入口与限制说明。

### v0.7.4 — 只读查数工具与 tools 直跑修复（2026-03-18）

#### Added
- 新增 `tools/query_data.py`，支持 MySQL / Oracle 只读查询、模板查数、参数注入与导出。
- 新增 `.claude/skills/data-query/SKILL.md`，统一“结构查询 / 固定对账 / 自由查数”路由规则。
- 新增 `.claude/agents/data-query-agent.md`，补充 MCP 优先、Python 工具兜底的数据查询工作流。

#### Changed
- `tools/export_ads.py` 增加 `argparse`，支持 `--snapshot-date` 与 `--output`，仍保持 `ads_inventory_health` 只读导出。
- `README.md`、`docs/RUNBOOK.md`、`docs/ARCHITECTURE.md` 补充只读查数、结构快照与 MCP 降级说明。

#### Fixed
- `tools/snapshot_mysql_hefangdw_schema.py`、`tools/snapshot_oracle_bosnds3_schema.py`、`tools/test_connection.py`、`tools/export_ads.py` 统一改为基于 `REPO_ROOT` 解析 `config.py`、`docs/` 与 `reports/` 路径，支持任意工作目录直接运行。

### v0.7.3 — dim_channel 店仓字段更名（2026-03-18）

#### Changed
- `etl_dim_channel.py` 将 `dim_channel` 目标字段更名为 `WING_CODE`，并直接映射 Oracle `O2O_RETAIL_CHANNEL.WING_CODE`。
- `test_etl_automation.py` 将 `dim_channel` 自动化校验改为核对 `WING_CODE='DS001'`。
- `SQL/create_dim_channel.sql` 修正目标字段名为 `WING_CODE`，并新增现网迁移脚本 `SQL/alter_dim_channel_rename_store_code_to_wing_code.sql`。

#### Docs
- 更新 `README.md`、`docs/DATA_CONTRACTS.md`、`docs/ETL业务逻辑说明.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`，统一 `dim_channel` 目标字段名为 `WING_CODE`。

### v0.7.2 — dim_channel 血缘补齐（2026-03-18）

#### Added
- 新增 `etl_dim_channel.py`，将 Oracle `O2O_RETAIL_CHANNEL` 全量同步到 MySQL `dim_channel`。
- 新增 `SQL/create_dim_channel.sql`，补齐 `dim_channel` 建表脚本。

#### Changed
- `run_etl.py` 主流水线由 7 步扩展为 8 步，新增 `dim_channel` 同步步骤。
- `config.py` 新增 `dim_channel` 任务显示名。
- `test_etl_automation.py` 新增 `dim_channel` 自动化校验。

#### Docs
- 更新 `README.md`、`docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/TODO_ISSUES.md`，关闭 P1-001。


## 2026-03-04

### v0.7.0 — everything-claude-code 四层架构扩展（2026-03-04）

#### Added
- 新增 3 个 ETL/数仓专属 Subagent（`.claude/agents/`）：
  - `etl-auditor`：ETL代码审计专家（只读，sonnet）
  - `doc-syncer`：文档同步执行者（读写，haiku）
  - `db-inspector`：数据库结构探查专家（只读 + MySQL MCP，haiku）
- 新增 5 个项目专属 Skill（`.claude/skills/`）：
  - `/handoff`：一键写入 AGENT_HANDOFF.md 交接记录
  - `/quality-check`：全套质检（连通性+ETL空跑+数据质量+文档同步）
  - `/doc-sync`：文档同步检查与自动修复
  - `/etl-audit`：ETL 完整审计，输出带优先级的发现清单
  - `/schema-snap`：数据库结构快照 + 字典漂移检测
- 新增本地 MCP 配置约定（`.mcp.json`，已忽略不提交）：MySQL + Oracle 双向 MCP（只读，env var 引用）
  - MySQL：`@benborla29/mcp-server-mysql`，直连何方数仓
  - Oracle：`mcp-server-oracle`，直连伯俊 ERP
- 新增 PostToolUse Hook：修改 `etl_*.py` 或 `SQL/*.sql` 后自动提醒同步文档

#### Changed
- `.claude/settings.json`：新增 `hooks.PostToolUse` 节点
- `.claude/CLAUDE.md`：新增第 8 章「Agent 与 Skill 快速索引」
- `.gitignore`：追加 `.mcp.json`（含 env var 引用，不提交）

#### Docs
- `CLAUDE.md` 第 8 章新增完整的 Subagents/Skills/Hooks/MCP 索引表

参考：架构模式来源 [affaan-m/everything-claude-code](https://github.com/affaan-m/everything-claude-code)


## 2026-03-03

### v0.6.4 — 文档审计与交接工具补齐（2026-03-03）

#### Added
- 新增文档审计脚本与术语过滤规则。来源：[scripts/check_doc_sync.py](scripts/check_doc_sync.py#L1-L80)
- 新增环境自检脚本（Doctor）。来源：[scripts/doctor.ps1](scripts/doctor.ps1#L1-L72)
- 新增交接日志写入脚本。来源：[scripts/log_agent_action.py](scripts/log_agent_action.py#L1-L76)
- 新增 MySQL/Oracle 结构快照导出脚本。来源：[tools/snapshot_mysql_hefangdw_schema.py](tools/snapshot_mysql_hefangdw_schema.py#L1-L86)、[tools/snapshot_oracle_bosnds3_schema.py](tools/snapshot_oracle_bosnds3_schema.py#L1-L78)

#### Changed
- dws_sales 抽取与清洗补充门店编码、云仓标识与别名字段处理。来源：[etl_dws_sales.py](etl_dws_sales.py#L28-L100)

#### Docs
- 新增 Agent 交接日志与待办追踪文档。来源：[docs/AGENT_HANDOFF.md](docs/AGENT_HANDOFF.md#L1-L44)、[docs/TODO_ISSUES.md](docs/TODO_ISSUES.md#L1-L45)


## 2026-02-26

### v0.6.3 — ODS双水位与质检链路（2026-02-26）

#### Added
- 新增 ODS 抽取链路：`etl_ods_fa_storage.py`、`etl_ods_m_retail.py`、`etl_ods_m_retailitem.py`、`run_ods.py`。
- 新增 ODS 质量校验工具：`tools/check_ods_incremental.py`、`tools/check_ods_retailitem_quality.py`。
- 明细双通道增量：`MODIFIEDDATE`（线上）与 `SETTIME`（线下）双水位对账与拆分校验。

#### Changed
- `run_ods.py` 集成质量校验并输出日志 `logs/ods_qc_*.log`。
- 明细增量逻辑改为双水位并记录 `ods_m_retailitem_settime` 水位。

#### Fixed
- 对账工具 `--as-of` 截止时间过滤兼容 `M_RETAIL` 查询别名。
- 全量完成后同步写入 `settime` 水位，避免后续增量再次全量回刷。

#### Database / SQL
- 新增 ODS 建表与增量迁移脚本：`SQL/create_ods_tables.sql`、`SQL/alter_ods_incremental.sql`。
- `ods_m_retailitem` 增加 `settime` 字段及索引。

#### Docs
- 更新 `README.md`：ODS 双水位说明、质量校验入口与日志说明。
- 更新 `docs/数据仓库与ETL手册.md`、`docs/数据结构与映射手册.md`、`docs/mysql_data_dictionary.md` 同步 ODS 口径与字段。


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

