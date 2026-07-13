# ODS-DWD-DWS-ADS 架构完善子项目

> 本目录是“逐步完善 ODS-DWD-DWS-ADS 分层架构”的长期续接入口。后续无论上下文压缩、新开窗口，均应优先阅读本目录文档恢复权威事实。

---

## 1. 子项目目标

在不影响现有每日总控自动调度的前提下，逐步补齐当前项目缺失的 DWD 明细事实层，并规范后续 DWS 主题汇总层建设，降低新增主题时重复口径、重复过滤、跨层直连 ODS 带来的维护风险。

---

## 2. 文档入口

| 文档 | 用途 | 更新时机 |
|------|------|----------|
| `00_项目目标与背景.md` | 记录项目目标、现状、约束、已知证据 | 背景、范围或事实证据变化时 |
| `01_设计基线.md` | 记录分层原则、目标架构、实施边界、风险 | 架构决策、命名规则、主题优先级变化时 |
| `02_任务续接上下文.md` | 作为新窗口或上下文压缩后的接棒总入口 | 每次完成实质推进后必须更新 |
| `03_推进看板与里程碑.md` | 记录阶段、里程碑、下一步、推进日志 | 每次完成任务、变更阶段或新增阻塞时必须更新 |
| `04_M1只读审计报告.md` | 记录销售 DWS、库存 DWS 与 ADS 依赖的只读审计结论 | M1 审计复盘或后续补证据时 |
| `05_M2第一批DWD主题设计冻结草案.md` | 记录第一批销售明细与库存快照 DWD 候选粒度、主键、字段、增量、验证、风险和用户确认的长期设计决策 | M2 人工复核、进入 M3 前 |
| `06_M2_5_ORACLE源库画像与ODS_DWD规划.md` | 记录 Oracle BOSNDS3 源库结构、核心表字段启用率、当前 ODS 覆盖缺口和 ODS / DWD 长期规划 | M2.5 源库画像复盘、M3 字段血缘设计前 |
| `07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md` | 冻结 `M_RETAIL` / `M_RETAILITEM` / `FA_STORAGE` ODS 扩展字段白名单，比较 ODS 扩展方案，登记 raw ODS DDL、旁路 ETL、销售完整业务日期 / 库存 full raw 初始化验证与 DWD 对账结论 | M3 人工复核、raw ODS 方案确认、旁路装载验证和 DWS v2 接入设计前 |
| `08_M4_DWS_v2并行表_调度接入与回滚方案.md` | 设计 `dws_sales_daily_v2`、`dws_inventory_daily_v2` 并行表、调度 shadow 接入、验收矩阵与回滚方案；用户已人工建表并完成空表核验，且已完成 dry-run / conn-test / S3 手工写入分支、S4 独立 shadow run 与总控非阻断接入；仍未切生产消费 | M5 总控 V2 双跑 gate 或 S6 计划任务入口切换前 |
| `09_M5_ADS_v2闭环切换清单.md` | 固化用户 2026-05-12 激进推进决策下的两次总控 V2 gate、ADS 依赖分类、字段兼容红线和 rollback 路径 | 执行两轮总控 V2 前后、评估是否进入 M6 切计划任务前 |

---

## 3. 当前权威事实摘要

- 当前主链调度顺序包含 DIM、ODS、DWS、ADS，但未包含 DWD 步骤。证据：`run_etl.py#L52-L61`。
- 当前项目文档已把理想链路描述为 ODS → DIM → DWD → DWS → ADS，但同时标注 DWD 层“暂无（DWD层未在代码实现）”。证据：`docs/数据仓库与ETL手册.md#L52-L56`。
- 当前 DWS 销售脚本直接从 ODS 零售主/明细表聚合到 `dws_sales_daily`。证据：`etl_dws_sales.py#L1-L4`、`etl_dws_sales.py#L56-L57`。
- 当前 DWS 库存脚本直接从 ODS 库存表聚合到 `dws_inventory_daily`。证据：`etl_dws_inventory.py#L1-L4`、`etl_dws_inventory.py#L55-L55`。
- 当前总控由 `scheduled_total_control.py` 调度主链、门店销售专题与 `dws_v2_shadow` 非阻断子链，主链入口为 `scheduled_etl.py`，而 `scheduled_etl.py` 统一调用 `run_etl.py`。证据：`scheduled_total_control.py`、`scheduled_dws_v2_shadow.py`、`scheduled_etl.py#L48-L52`。
- M1 只读审计确认：库存健康 ADS 真实依赖 `dws_inventory_daily` 与 `dws_sales_daily`；当前保留的门店销售专题链路中，`ads_store_daily_report` 与 `ads_daily_sales` 的事实口径已改为 ODS / 配置直读，但专题调度 freshness 仍以 `dws_sales_daily.etl_time` 作为刷新代理。证据：`etl_ads_health.py`、`etl_ads_store_daily_report.py`、`etl_ads_daily_sales.py`、`scheduled_store_daily_report.py`。
- 2026-04-29 用户从首席数据官与数据架构师长期最优解视角确认两条 M2 决策：销售 DWD 不只保留核算事实，应承接关键业务上下文；库存 DWD 不只覆盖当前库存健康链路，应保留全店仓快照事实。证据：`05_M2第一批DWD主题设计冻结草案.md`。
- 2026-04-29 M2.5 只读画像确认：Oracle `BOSNDS3` 全库约 2705 张表，当前核心链路先聚焦 `M_RETAIL`、`M_RETAILITEM`、`FA_STORAGE`、商品 / SKU / 店仓 / 渠道维表；后续补证确认 `FA_STORAGE` 存在全量为 0 的模板化冗余字段，非空不等同于字段有真实业务值。证据：`06_M2_5_ORACLE源库画像与ODS_DWD规划.md`、`reports/oracle_bosnds3_core_field_profile_202604.json`、`reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json`。
- 2026-04-29 用户已确认 M3 优先采用旁路 `ods_*_raw` 方案；2026-04-30 用户已人工完成 `ods_m_retail_raw`、`ods_m_retailitem_raw`、`ods_fa_storage_raw`、`dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` 建表并人工修正表注释；随后在用户授权下完成 raw ODS 近 1 天小窗口真实装载与 DWD 小窗口 upsert。2026-05-07 已在用户授权下补齐 20260428-20260430 销售完整业务日期 raw / DWD（`ods_m_retail_raw` 2861 行、`ods_m_retailitem_raw` 5103 行、`dwd_sales_retail_item` 5103 行）并完成 20260507 库存 full raw / DWD 初始化（`ods_fa_storage_raw` 201946 行、`dwd_inventory_storage_snapshot` 201946 行）；当前 5 表已有旁路验证数据，但未接入总控，当前 DWS / ADS 仍不消费。证据：`07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md`、`SQL/draft_create_ods_m_retail_raw.sql`、`SQL/draft_create_ods_m_retailitem_raw.sql`、`SQL/draft_create_ods_fa_storage_raw.sql`、`etl_ods_m_retail_raw.py`、`etl_ods_m_retailitem_raw.py`、`etl_ods_fa_storage_raw.py`、`SQL/check_dwd_sales_retail_item_min.sql`、`SQL/check_dwd_inventory_storage_snapshot_min.sql`、`reports/context_cache/m3_manual_ddl_verification_20260430.json`、`reports/context_cache/m3_raw_dwd_small_window_load_20260430.json`、`reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json`。
- 2026-05-07 已输出 M4 DWS v2 并行表、调度接入与回滚方案：规划 `dws_sales_daily_v2`、`dws_inventory_daily_v2` 两张并行表，落草案 DDL、只读对账 SQL和设计证据缓存；用户随后已人工建两张 v2 表，Copilot 先只读核验其结构，再在用户明确授权下完成 S3 实跑验收。销售 `20260428-20260430` 已写入 3417 行且 DWD-v2 mismatch 为 0；库存 `20260507` 已写入 75104 行且 DWD-v2 mismatch 为 0。额外只读复核显示销售 v2 与旧 DWS 0 差异；库存 v2 与旧 DWS 的 200 条同 key `qty` 差异当前按快照时点不同记录。当前已新增 `scheduled_dws_v2_shadow.py`、`run_scheduled_dws_v2_shadow.bat`，并把 `dws_v2_shadow` 以非阻断子链接入 `scheduled_total_control.py`；现有 DWS / ADS 仍不消费 v2 表，也未切 `run_etl.py` 主链。证据：`08_M4_DWS_v2并行表_调度接入与回滚方案.md`、`SQL/draft_create_dws_sales_daily_v2.sql`、`SQL/draft_create_dws_inventory_daily_v2.sql`、`SQL/check_dws_v2_parallel_reconciliation.sql`、`dws_v2_write_utils.py`、`etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py`、`scheduled_dws_v2_shadow.py`、`run_scheduled_dws_v2_shadow.bat`、`scheduled_total_control.py`、`test_dws_v2_dry_run.py`、`test_scheduled_total_control.py`、`reports/context_cache/dws_v2_parallel_design_evidence_20260507.json`、`reports/context_cache/dws_v2_manual_ddl_verification_20260507.json`、`reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json`、`reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json`。
- 2026-04-30 已按 ERP AD_COLUMN 字典、用户提供的 `FA_STORAGE` 开发平台截图、Oracle 全量非零值扫描和用户确认的真实字段原则完成 raw ODS / DWD 草案字段校准；`DATEOUT` / `DATEIN` 已收敛为出库 / 入库日期，`QTYPURCHASEREM`、`QTYOMSTRANSLATE`、`QTYPREOUT1` 保留为真实有非零值库存信号；`RETURNQTY`、`ORG_M_RETAILITEM_ID`、`QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 已从草案 DDL 剔除。证据：`data/AD_COLUMN04301009.xlsx`、`reports/context_cache/ad_column_retail_raw_semantics_20260430.csv`、`reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json`、`07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md`。

---

## 4. 本目录维护规则

1. 后续每次推进 DWD / DWS / ADS 分层建设后，必须同步更新 `02_任务续接上下文.md` 与 `03_推进看板与里程碑.md`。
2. 若冻结新的分层原则、主题优先级、命名规则、表粒度或切换策略，必须同步更新 `01_设计基线.md`。
3. 若产生新的事实证据、已验证命令、调度边界或数据库快照，必须同步写入对应文档的“证据”或“推进日志”。
4. 若新增或修改 ETL / SQL / 表结构，必须同步项目根文档：`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/DATA_CONTRACTS.md`、`docs/数据仓库与ETL手册.md`、`docs/ETL业务逻辑说明.md`、`docs/SQL开发手册.md`、`README.md` 中受影响部分。
5. 任何 DDL、建索引、补数回填、增删改数据操作仅输出 SQL / 脚本 / 执行顺序，由用户人工执行；Agent 不代执行落库写操作。

---

## 5. 当前状态

| 项目项 | 状态 | 说明 |
|--------|------|------|
| 子项目文档目录 | 已建立 | 本轮仅新增文档，不修改 ETL / SQL / 调度 |
| DWD 实现 | 已完成旁路销售完整业务日期与库存 full raw 初始化 / 未接调度 | 用户已人工建 `dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` 两张表；DWD ETL 已在授权下完成小窗口、20260428-20260430 销售完整业务日期和 20260507 库存 full raw 快照 upsert 验证，但仍未接入总控，生产 DWS / ADS 不消费 |
| M1 只读审计 | 已完成 | 已形成 `04_M1只读审计报告.md`，覆盖 DWS 血缘与 ADS 依赖 |
| M2 设计冻结草案 | 已输出 / 关键长期决策已确认 | 已形成 `05_M2第一批DWD主题设计冻结草案.md`，并写回用户确认的销售与库存 DWD 长期边界；不代表已建表或已实现 |
| M2.5 Oracle 源库画像 | 已完成初版 | 已形成 `06_M2_5_ORACLE源库画像与ODS_DWD规划.md`，仅做只读结构与字段启用率画像，不代表已确认废字段或已进入实现 |
| M3 ODS 白名单与 DWD 草案 | raw 方案已确认 / 已完成旁路销售完整业务日期与库存 full raw 初始化 / 未接调度 | 已形成 `07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md`、3 份 raw ODS DDL、3 个 raw ODS 装载脚本、2 份 DWD DDL、2 个 DWD 旁路装载脚本和 2 份对账 SQL；2026-04-30 已根据 ERP 字典、截图与 Oracle 全量非零值扫描更新字段注释并剔除全零 / 全空模板字段；用户已人工建 5 张表并修正表注释，Copilot 已按授权完成近 1 天小窗口、20260428-20260430 销售完整业务日期 raw / DWD 和 20260507 库存 full raw / DWD 初始化验证，但当前 DWS / ADS 不消费这些表 |
| M4 DWS v2 并行方案 | 已输出设计 / 用户已人工建表 / 空表已核验 / dry-run、conn-test、S3 实跑验收与 S4 shadow 调度接入已完成 / 未切生产消费 | 已形成 `08_M4_DWS_v2并行表_调度接入与回滚方案.md`、两份 DWS v2 DDL 草案、一份只读并行对账 SQL、两个 v2 脚本、`scheduled_dws_v2_shadow.py` 与 `run_scheduled_dws_v2_shadow.bat`；已在用户明确授权下完成一次 S3 受控写入验收，且当前以 `scheduled_total_control.py` 的非阻断子链观察运行 |
| 总控调度影响 | 非阻断接入 | 已修改 `scheduled_total_control.py`，新增 `dws_v2_shadow` 子链与 `--shadow-only`；未修改 `scheduled_etl.py`、`run_etl.py` 主链，shadow 失败只记 WARNING，不影响旧 DWS / ADS |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.10 | 2026-06-10 | 将子项目首页的销售专题现行说明收口到当前保留链路，并移除对退役三表脚本的当前态引用 |
| v1.9 | 2026-05-12 | 新增 M5 ADS v2 闭环切换清单入口，记录两次总控 V2 gate、ADS 字段兼容红线与 rollback 路径 |
| v1.8 | 2026-05-07 | 记录 S4 独立 shadow 调度与总控非阻断接入已完成，当前入口状态更新为“未切生产消费” |
| v1.7 | 2026-05-07 | 记录 DWS v2 已完成 S3 实跑验收：销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0；销售与旧 DWS 0 差异，库存与旧 DWS 的 200 条同 key `qty` 差异当前按快照时点不同记录 |
| v1.6 | 2026-05-07 | 记录 DWS v2 S3 手工写入分支已新增：默认 dry-run，写入需确认令牌、命名锁、事务和 DWD-v2 对账；本轮未执行真实写入 / 未接调度 |
| v1.5 | 2026-05-07 | 记录 DWS v2 dry-run / conn-test 脚本已新增且无写库入口，仍未写 v2 数据 / 未接调度 |
| v1.4 | 2026-05-07 | 记录 DWS v2 两表已由用户人工建表并完成空表核验，仍未写 v2 数据 / 未接调度 |
| v1.3 | 2026-05-07 | 新增 M4 DWS v2 并行表、调度接入与回滚方案入口；明确仅设计草案，未执行 DDL / 写库 / 调度修改 |
| v1.2 | 2026-05-07 | 记录 M3 已完成销售完整业务日期 raw / DWD 补齐与库存 full raw / DWD 初始化验证；仍未接调度 |
| v1.1 | 2026-04-30 | 记录用户已人工修正表注释，并完成 M3 raw / DWD 近 1 天小窗口真实装载与最小对账；仍未接调度 |
| v1.0 | 2026-04-30 | 记录用户已人工完成 5 张 M3 raw / DWD 表建表，并校准为已建空表、未装载、未接调度状态 |
| v0.9 | 2026-04-30 | 记录 M3 raw / DWD 草案按真实有数据且语义明确原则剔除 Oracle 模板冗余字段 |
| v0.8 | 2026-04-30 | 记录 M3 raw / DWD 草案字段语义第一轮校准状态与仍待补证字段 |
| v0.7 | 2026-04-29 | 记录用户已确认 M3 raw 旁路方案，并补充 raw ODS DDL、抽取骨架和 DWD 小窗口对账 SQL 当前状态 |
| v0.6 | 2026-04-29 | 增加 M3 ODS 字段白名单、DWD DDL 草案和旁路 ETL 骨架入口与当前状态 |
| v0.5 | 2026-04-29 | 增加 M2.5 Oracle 源库画像与 ODS / DWD 规划入口、事实摘要和当前状态 |
| v0.4 | 2026-04-29 | 写回 M2 用户确认的两条长期设计决策与当前状态 |
| v0.3 | 2026-04-29 | 增加 M2 第一批 DWD 主题设计冻结草案入口与当前状态 |
| v0.2 | 2026-04-28 | 增加 M1 只读审计报告入口与 ADS 依赖结论摘要 |
| v0.1 | 2026-04-28 | 新增 ODS-DWD-DWS-ADS 架构完善子项目目录入口 |
