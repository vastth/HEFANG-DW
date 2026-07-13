# ARCHITECTURE.md — 何方珠宝数据仓库项目地图

> 本文档是仓库架构的**唯一权威来源**。修改数据层结构、调度顺序或关键配置后，必须同步更新本文件。
>
> 最后更新：2026-05-14（v0.7.63 对齐）

---

## 0. 实施边界

- 2026-05-13 用户已将 Windows 计划任务入口切到 `run_scheduled_total_control_v2.bat`，M6 进入真实调度入口观察；09:09 首轮失败暴露 V2 总控顺序问题：主链先读 `_v2` DWS 计算 `ads_inventory_health`，而 `_v2` DWS 当日读源尚未刷新。
- 当前已修正为：有效模式为 `v2` 且不是 `--conn-test` / `--shadow-only` 时，`scheduled_total_control.py` 会先执行阻断型 `DWS v2 读源预刷新`，并向 `scheduled_dws_v2_shadow.py` 追加 `--skip-ads-shadow-validation`；预刷新成功后再触发主链和销售专题，预刷新失败则跳过主链以避免 ADS 读空 `_v2` 源。2026-05-14 又补一层时序保护：若当前批次本身就是最早生成同日 `_v2` 读源的 pre-refresh，则允许 same-day `dws_inventory_daily` 尚未产出，此时 inventory old DWS 基线会记为 `SKIPPED`，不再把 `old_dws_max_etl_time=None` 误判成失败；若显式请求 same-snapshot 对齐，仍保持阻断。未显式传参时默认仍是 `legacy`。来源：[scheduled_total_control.py](../scheduled_total_control.py#L125)；[scheduled_total_control.py](../scheduled_total_control.py#L149)；[scheduled_total_control.py](../scheduled_total_control.py#L503)；[scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L419)；[scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L500)；[scheduled_dws_v2_shadow.py](../scheduled_dws_v2_shadow.py#L1041)
- 当前只有 `ads_inventory_health` 纳入主链 cutover 范围：`shadow_compare` 仍按旧 DWS 写生产 ADS，只追加 `_v2` 影子对账；`v2` 才会显式改读 `dws_inventory_daily_v2 + dws_sales_daily_v2`。来源：[run_etl.py](../run_etl.py#L767)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)；[etl_ads_health.py](../etl_ads_health.py#L523)
- ADS 层 MySQL 表已被 Tableau 和其他下游直接消费；后续若由影子链替代旧链，只允许新增字段，不允许改名或删除既有 ADS 字段。

---

## 1. 系统全景

```
┌─────────────────────────────────────────────────────────────────┐
│                        数据源（Source）                          │
│  Oracle 19c — 伯俊 ERP (BOSNDS3)                                │
│  表：FA_STORAGE / M_RETAIL / M_RETAILITEM / M_PRODUCT /         │
│       M_PRODUCTALIAS / C_STORE / M_PURCHASEITEM / ...           │
└────────────────────────┬────────────────────────────────────────┘
                         │ python-oracledb (thin mode)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ODS 层（Operational Data Store）              │
│  MySQL: ods_fa_storage / ods_m_retail / ods_m_retailitem        │
│  策略：增量（双水位）或全量覆盖                                  │
│  水位字段：MODIFIEDDATE（线上）/ SETTIME（线下）                  │
└────────────────────────┬────────────────────────────────────────┘
                         │ Pandas + SQLAlchemy
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    DIM 层（维度层，每日全刷）                      │
│  MySQL: dim_product / dim_sku / dim_store / dim_channel          │
│  来源：直接读 Oracle，每日全量覆盖                                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    DWS 层（明细汇总层）                           │
│  MySQL: dws_sales_daily / dws_inventory_daily                    │
│  销售：增量（按日期窗口从 ODS 聚合）                              │
│  库存：每日快照（全量覆盖当日数据）                               │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    ADS 层（应用层）                               │
│  MySQL: ads_inventory_health（库存健康度，SKU 粒度）              │
│         ads_daily_sales（销售看板月度战役，仓库样板）             │
│         ads_store_daily_report（门店经营日报，最终经营实体粒度）  │
│         ads_store_daily_subject_report（门店经营日报，统计主体粒度）│
│         ads_dabo_order_label（达播订单标签，订单粒度）            │
│         ads_dabo_daily_sales（达播数据，外部 CSV 导入）           │
│  配置依赖：dim_store_report_attr / cfg_store_target_daily /       │
│           cfg_store_assessment_subject_target_daily /             │
│           cfg_store_assessment_assignment                         │
│  每日全量重算，含 SABC 分级与健康度评分                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. 目录结构

```
hefang_dw/
│
├── 【调度入口】
│   ├── run_etl.py              主调度：9步流水线（dim→ods→dws→dabo→ads，支持 legacy / shadow_compare / v2）
│   ├── run_ods.py              ODS 专项调度（增量/全量 + 自动质检）
│   ├── scheduled_etl.py        任务计划包装（调 run_etl.py → test_etl_automation.py，透传 cutover / rollback）
│   ├── run_scheduled_etl.bat   Windows 任务计划触发脚本
│   ├── scheduled_total_control.py 主链成功后串联销售专题与 DWS v2 shadow 的总控包装，并向主链/专题链透传 cutover / rollback
│   ├── run_scheduled_total_control.bat 主链+销售专题+shadow 总控 Windows 触发脚本
│   ├── scheduled_store_daily_report.py 门店日报专题调度（调目标导入专题，freshness 来源可按 cutover_mode 派生 legacy / v2）
│   ├── run_scheduled_store_daily_report.bat 门店日报专题 Windows 触发脚本
│   ├── scheduled_dws_v2_shadow.py DWS v2 shadow 调度（raw ODS→DWD→_v2，并附带 ads_inventory_health 报告型对账）
│   └── run_scheduled_dws_v2_shadow.bat DWS v2 shadow Windows 触发脚本
│
├── 【ETL 模块】
│   ├── etl_ods_fa_storage.py   ODS: 库存主档（FA_STORAGE）
│   ├── etl_ods_m_retail.py     ODS: 零售单据头（M_RETAIL，增量）
│   ├── etl_ods_m_retailitem.py ODS: 零售单据明细（M_RETAILITEM，双水位）
│   ├── etl_dim_product.py      DIM: 商品维度（全刷）
│   ├── etl_dim_sku.py          DIM: SKU 维度（全刷）
│   ├── etl_dim_store.py        DIM: 店仓维度（全刷）
│   ├── etl_dim_channel.py      DIM: 渠道维度（全刷）
│   ├── etl_dws_sales.py        DWS: 销售日报（增量）
│   ├── etl_dws_inventory.py    DWS: 库存快照（每日）
│   ├── dws_v2_write_utils.py   DWS v2: S3 手工写入分支的确认令牌、命名锁、事务与运行证据工具
│   ├── etl_dws_sales_v2.py     DWS v2: 销售并行表 dry-run / conn-test / S3 手工写入（默认不写库）
│   ├── etl_dws_inventory_v2.py DWS v2: 库存并行表 dry-run / conn-test / S3 手工写入（默认不写库）
│   ├── etl_ads_health.py       ADS: 库存健康度（全量重算）
│   ├── etl_ads_daily_sales.py  ADS: 销售看板月度战役日节奏（仓库样板）
│   ├── etl_ads_store_daily_report.py  ADS: 门店经营日报（最终经营实体层）
│   └── etl_ads_store_daily_subject_report.py ADS: 门店经营日报（统计主体兼容层）
│
├── 【核心配置】
│   ├── config.py               唯一配置中心（连接参数 + 业务常量）
│   ├── db_connections.py       统一数据库连接工厂（连接池 + 超时分层）
│   └── alerts.py               企业微信告警模块
│
├── 【测试与质检】
│   ├── test_etl_automation.py  自动化验收测试
│   ├── test_dws_v2_dry_run.py  DWS v2 dry-run SQL 生成单元测试
│   ├── test_store_operation_owner_import.py 门店经营负责人导入最小单元测试
│   ├── tools/test_connection.py        连通性测试
│   ├── tools/check_data.py             通用数据质检
│   ├── tools/check_dws_inventory.py    库存质检
│   ├── tools/check_ods_incremental.py  ODS 增量对账
│   └── tools/check_ods_retailitem_quality.py  ODS 明细质检
│
├── 【数据库脚本】
│   └── SQL/
│       ├── create_ods_tables.sql       ODS 建表
│       ├── create_store_operation_owner_tables.sql 门店经营负责人快照与SCD2建表
│       ├── alter_ods_incremental.sql   双水位字段迁移
│       ├── alter_*.sql                 其他结构变更（13个）
│       ├── 库存健康度_SKU粒度_v5.0.sql  健康度计算 SQL（参考口径）
│       ├── ==日报数据SQL.sql            日报模板
│       └── ==线上销售月报SQL 2.0.sql    月报模板
│
├── 【工具】
│   ├── tools/export_ads.py                   导出 ads_inventory_health 快照
│   ├── tools/query_data.py                   通用只读查数与导出工具
│   ├── tools/import_store_operation_owner_from_nas.py 门店经营负责人快照 dry-run / 导入工具
│   ├── tools/extract_dabo_order_candidates_from_nas.py  达播统一 Excel 候选集/标签导出
│   ├── tools/load_dabo_order_labels_from_nas.py  达播订单标签 dry-run / 导入工具
│   ├── tools/snapshot_mysql_hefangdw_schema.py  MySQL 结构快照
│   ├── tools/snapshot_oracle_bosnds3_schema.py  Oracle 结构快照
│   ├── scripts/check_doc_sync.py              文档代码同步审计
│   └── scripts/log_agent_lesson.py            Agent 经验台帐写入
│
├── 【文档】
│   └── docs/（含 AGENT_HANDOFF / AGENT_LESSONS 等协作文档）
│
├── 【配置与模板】
│   ├── .env.example            环境变量模板
│   ├── .claude/settings.json   Agent 默认设置（可提交）
│   └── .claude/CLAUDE.md       Agent 协作规范（本项目）
│   ├── .claude/agents/          Agent 子代理定义（ETL/文档/结构）
│   ├── .claude/agents/data-query-agent.md  数据查询与对账专家
│   ├── .claude/skills/          Skills 定义（/handoff 等）
│   ├── .claude/skills/data-query/SKILL.md  data-query 查询路由工作流
│   └── .mcp.json                本地 MCP 兼容配置（主要供 Claude/OpenCode 参考，不作为 VS Code 会话主入口）
│
└── 【数据与输出】
    ├── data/                   测试参考数据（不提交）
    ├── logs/                   ETL 运行日志（不提交）
    ├── reports/                导出报表（不提交）
    └── notebooks/              Jupyter 探索（不提交规则变更）
```

---

## 3. ETL 执行流水线

### 3.1 主流水线（run_etl.py）

执行顺序固定（`STEP_ORDER`，见 `run_etl.py:43`）：

```
步骤  模块                   说明                        失败策略
─────────────────────────────────────────────────────────────────
1    etl_dim_product        商品维度全刷                 重试3次→告警继续
2    etl_dim_sku            SKU 维度全刷                 重试3次→告警继续
3    etl_dim_store          店仓维度全刷                 重试3次→告警继续
4    etl_dim_channel        渠道维度全刷                 重试3次→告警继续
5    ods_sync               ODS 增量同步 + 自动质检       重试3次→告警继续
6    etl_dws_sales          销售增量（主链近7天回带，已消费ODS）        重试3次→告警停止
7    etl_dws_inventory      库存快照（已消费ODS）        重试3次→告警停止
8    dabo_ready             达播主线就绪检查             优先检查标签主线并上报 legacy CSV 状态
9    etl_ads_health         库存健康度全量重算           重试3次→告警继续
─────────────────────────────────────────────────────────────────
```

说明：自 2026-04-23 起，`run_etl.py` 用同一个 `ODS_INCREMENTAL_BACKFILL_DAYS=7` 常量驱动 `ods_sync` 与 `dws_sales` 主链，先回刷 ODS 最近 7 天，再执行 `etl_dws_sales.run(days_back=7, include_today=True)`；这样 ODS 晚到补齐的数据会在同轮主链内继续下沉到 DWS，不再停留在 ODS 层。来源：[run_etl.py](../run_etl.py#L59)；[run_etl.py](../run_etl.py#L526)；[run_etl.py](../run_etl.py#L544)；[run_etl.py](../run_etl.py#L570)

cutover 说明：主链默认 `legacy`。`--cutover-mode shadow_compare` 时，`ads_inventory_health` 仍按旧 DWS 写生产表，但会额外对 `dws_inventory_daily_v2 + dws_sales_daily_v2` 运行报告型对账；`--cutover-mode v2` 时，`ads_inventory_health` 改读 `_v2` DWS。总控 V2 模式会先执行阻断型 DWS v2 读源预刷新，保证主链 ADS 计算前当日 `_v2` 源已刷新；`--rollback-to-legacy` 优先级高于前者。不显式传参时仍回到 `legacy`。来源：[cutover_controls.py](../cutover_controls.py#L29)；[cutover_controls.py](../cutover_controls.py#L55)；[run_etl.py](../run_etl.py#L767)；[run_etl.py](../run_etl.py#L782)；[run_etl.py](../run_etl.py#L803)；[run_etl.py](../run_etl.py#L998)；[scheduled_total_control.py](../scheduled_total_control.py#L503)

触发方式：
```bash
# 手动触发（每日正常）
python run_etl.py

# 连通性测试（不执行真实 ETL）
python run_etl.py --conn-test

# 任务计划（通过 scheduled_etl.py 包装）
python scheduled_etl.py
```

### 3.2 ODS 专项流水线（run_ods.py）

```bash
python run_ods.py                # 增量（默认，使用双水位）
python run_ods.py --full         # 全量覆盖 + retail/retailitem recent catch-up
python run_ods.py --full --full-catchup-days 0   # 关闭 full 后补追
python run_ods.py --skip-qc      # 跳过自动质检
```

如仅执行质检，请直接运行 `tools/check_ods_incremental.py` 与 `tools/check_ods_retailitem_quality.py`；
ODS 质检日志输出到 `logs/ods_qc_<日期时间>.log`。自 2026-04-07 起，`run_ods.py --full` 默认会在 `ods_m_retail` / `ods_m_retailitem` 全量结束后，按同一个固定 `as-of` 自动补一轮最近 1 天的增量 catch-up，并让 ODS 质检复用该 `as-of`。来源：[run_ods.py](../run_ods.py#L72-L125)；[etl_ods_m_retail.py](../etl_ods_m_retail.py#L91-L151)；[etl_ods_m_retailitem.py](../etl_ods_m_retailitem.py#L134-L206)

### 3.2c 达播订单标签工具链

```bash
python tools/extract_dabo_order_candidates_from_nas.py --preview-limit 5
python tools/load_dabo_order_labels_from_nas.py --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx"
python tools/load_dabo_order_labels_from_nas.py --apply --file "docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx"
```

说明：
- `ads_dabo_order_label` 是 hefang_dw 当前内部达播主线的订单标签表，作用是为 ODS 订单打上“是否达播 / 达播渠道”标签。
- `tools/extract_dabo_order_candidates_from_nas.py` 负责从 `订单管理*.xlsx` 提取候选集并导出订单标签 CSV。
- `tools/load_dabo_order_labels_from_nas.py` 默认只做 dry-run；只有用户在当轮明确授权后，才允许用 `--apply` 正式写入 `ads_dabo_order_label`。自 2026-04-09 起，装载阶段会为少量异常组合单补 `canonical_system_order_id`，但保留原始 `system_order_id` 不变。
- 该工具链现已接入 `run_etl.py` 主流水线：`dabo_ready` 优先检查 `ads_dabo_order_label` 最新批次是否存在且最近 1 天有更新，同时继续上报 `ads_dabo_daily_sales` 的 legacy 状态。
- `etl_ads_health.py` 自 2026-04-09 起优先消费最新 `ads_dabo_order_label` 批次，在 ODS 内按订单标签汇总达播 SKU 指标，并以 `ads_dabo_order_retail_bridge` 作为缓存兜底；只有标签批次不可用时，才回退 `ads_dabo_daily_sales`。来源：[etl_ads_health.py](../etl_ads_health.py#L195)；[etl_ads_health.py](../etl_ads_health.py#L270)；[run_etl.py](../run_etl.py#L649)

### 3.2a 门店日报专项入口（etl_ads_store_daily_report.py）

```bash
python etl_ads_store_daily_report.py --conn-test
python etl_ads_store_daily_report.py --report-date 2026-03-23 --data-version v1
```

说明：
- 当前脚本为独立运行入口，尚未接入 `run_etl.py` 主流水线。
- 运行前会校验 `dim_store_report_attr`、`cfg_store_target_daily` 的当日生效配置是否存在重叠，并校验负责人切片是否唯一有效。
- `ads_store_daily_report` 当前按“最终经营实体”出数：未配置共同考核时保持一店一行；命中共同考核配置时，物理门店会在本表直接合并为经营体行。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L8)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L109)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L148)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L184)
- 当前脚本还会按最终经营实体粒度左联 `dim_store_operation_owner_assignment`，把负责人下沉到 `owner_name`；仓库已新增 `SQL/alter_ads_store_daily_report_add_owner_name.sql`，目标库执行前该字段仍属于未实现状态，ETL 会在写数前直接提示缺列。来源：[../etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L370)；[../etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L526)；[../etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L629)；[../SQL/alter_ads_store_daily_report_add_owner_name.sql](../SQL/alter_ads_store_daily_report_add_owner_name.sql#L1)
- `cfg_store_target_daily` 的正式交付路径已确认采用“业务投递 Excel 到 NAS 指定目录，由独立 Python 任务定时扫描并导入”；当前已冻结 NAS 目录为 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，并按月份分文件管理，当前推荐命名规则已切换为 `YYYYMM考核数据配置表.xlsx`；导入脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。当前仓库已提供 `tools/import_cfg_store_target_daily_from_nas.py`；现网已于 2026-04-03 完成 `log_store_target_import` 建表、首轮 `--apply` 写库与专项消费验证，新环境首次写库前仍需先执行 `SQL/create_log_store_target_import.sql`，再使用 `--apply` 写入 `cfg_store_target_daily`。若 NAS 目录内同时存在多个月份文件，脚本要求显式传入 `--target-month YYYY-MM` 选择本次导入月份；若同月同时存在多个版本文件，则需改用 `--file-path` 显式指定。若模板显式提供 `门店类型` 列，可追加 `--sync-store-report-attr` 同步刷新 `dim_store_report_attr`；当前 `report_channel_type_group` 已作为生成列生效于现网表结构。若工作簿同时提供 `统计主体目标` 与 `门店考核归属` 两张可选 sheet，脚本还会同步刷新共同考核配置表。
- 正式写数逻辑已内置在 `etl_ads_store_daily_report.py`；`docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql` 当前仅保留为设计参考，不再作为运行时依赖。

### 3.2aa 门店日报统计主体层入口（etl_ads_store_daily_subject_report.py）

```bash
python etl_ads_store_daily_subject_report.py --conn-test
python etl_ads_store_daily_subject_report.py --report-date 2026-03-23 --data-version v1
```

说明：
- 该脚本消费最终经营实体层 `ads_store_daily_report`，不直接读取 ODS。
- 若共同考核配置存在，则在最终经营实体结果上补齐 `subject_code`、主店锚点与成员门店数；未配置门店自动回退为一店一主体。
- 主体层事实值不再重复汇总物理门店，而是直接复用 `ads_store_daily_report` 已合并后的销售额、销量、订单数与目标值；因此主体层的 `day_order_cnt` / `mtd_order_cnt` 会自动继承门店层“按过滤后商品范围单号净额判 `1 / 0 / -1`、近零值按 0 处理”的订单数口径。来源：[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L95)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L139)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L154)

### 3.2ac 销售看板月度战役入口（etl_ads_daily_sales.py）

```bash
python etl_ads_daily_sales.py --conn-test
python etl_ads_daily_sales.py --report-date 2026-04-14 --data-version v1
```

说明：
- 该脚本当前仍是独立样板脚本入口，未接入 `run_etl.py` 主流水线；但专题调度 `scheduled_store_daily_report.py` 已可在受影响日期批量重跑时触发 `ads_daily_sales`。历史 `2026-04-15 / v1` 与 `2026-04 / v2` 的写库与最小对账记录形成于旧版销售主题逻辑，本轮统一到 `ads_store_daily_report` 权威口径后不能直接视为新逻辑验证结果。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L456)；[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)；[AGENT_HANDOFF_archive.md](AGENT_HANDOFF_archive.md#L460)
- `battle_month` 当前固定为 `report_date` 所在自然月月初，`sales_date` 只覆盖月初到 `report_date`；首版不预展开未来日期，也不物化预测字段。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L1)
- 当前首版只落物理字段：`report_date`、`battle_month`、`sales_date`、`area_name`、`report_channel_type`、`day_target_amt`、`day_actual_amt`、`cum_target_amt`、`cum_actual_amt`、`last_year_cum_actual_amt`、`data_version`、`etl_time`。来源：[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)
- `day_target_amt` 已统一为“共同考核主体日目标优先，否则回退经营实体内门店日目标求和”；`day_actual_amt` 改为基于 `ods_m_retail + ods_m_retailitem`、并按门店日报商品范围过滤后的净额口径；累计字段统一按 `area_name + report_channel_type` 日序列窗口累加。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L118)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L175)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L186)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L302)
- 门店范围已与 `ads_store_daily_report` 对齐，固定收口到 `report_date` 当天“组织属性有效且目标已生效”的门店，再对这批门店展开整段 `battle_month` 日历。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L63)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L175)
- 仓库已同步补齐最小对账 SQL，用于核对行数、唯一键，以及按全部明细切片聚合后的整段日序列日目标、日实际和累计口径。来源：[SQL/check_ads_daily_sales_min.sql](../SQL/check_ads_daily_sales_min.sql#L1)


### 3.2b 门店日报目标导入工具（tools/import_cfg_store_target_daily_from_nas.py）

```bash
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --preview-limit 5 --sync-store-report-attr
python tools/register_store_attr_snapshot.py --target-month 2026-04 --diff-output reports/store_attr_snapshot_diff_202604_registered.json
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --created-by your_name
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --sync-store-report-attr --created-by your_name
python tools/import_cfg_store_target_daily_from_nas.py --target-month 2026-04 --apply --sync-store-report-attr --created-by your_name
```

说明：
- 默认按 NAS 目录 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\` 扫描 `YYYYMM考核数据配置表.xlsx`，并兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。
- 默认工作表为 `导入模板`，按首行表头读取 `目标月份`、`目标版本`、`门店名称`、`月目标` 与 `1日目标` 到 `31日目标`；若启用 `--sync-store-report-attr`，还会读取 `门店类型` 列。
- 若工作簿同时包含 `统计主体目标` 与 `门店考核归属` 两张可选 sheet，则会同步解析共同考核主体目标与门店归属；两张 sheet 只提供一张时直接失败。
- 门店类型当前支持 `小程序 / 线上小程序 / 直营 / 直营-奥莱 / 联营 / 联营-免税 / 联营-奥莱`；导入后细分类直接写入 `report_channel_type`，粗分类由 `report_channel_type_group` 生成列自动承接；若 `门店类型` 文本包含 `免税`，同步门店属性时 `is_duty_free` 判为 `Y`，避免 `联营-免税` 门店沿用旧属性中的 `N`。来源：[../tools/import_cfg_store_target_daily_from_nas.py](../tools/import_cfg_store_target_daily_from_nas.py#L1711-L1716)
- 若 NAS 目录同批存在多个 `目标月份` 文件，必须显式传入 `--target-month YYYY-MM`；未传时脚本直接失败并提示当前可选文件，避免误选月份。
- 若同一 `目标月份` 存在多个版本文件，脚本不会自动猜测版本，需改用 `--file-path` 显式指定具体文件。
- 门店匹配按 `store_name` 语义进行，当前实现为大小写不敏感精确匹配；若仍未命中，会直接报错并给出候选门店建议，避免静默误配。
- `tools/register_store_attr_snapshot.py` 会先复用只读 diff 逻辑，再把 `file_md5 / compare_date / diff_counts / status` 记录到 `reports/store_attr_snapshot_registry.json`；若最新 NAS 细分类尚未正式 apply，状态登记为 `pending_apply`。
- 只有 `--apply` 才会按“目标月份 + 目标版本”先删后插 `cfg_store_target_daily`，并把执行摘要写入 `log_store_target_import`。
- 启用 `--sync-store-report-attr` 时，脚本会在同一事务中按 `store_id` 对当前有效 `dim_store_report_attr` 记录做未变化 / 变更 / 新增 / 退出分类；未变化不动，变更执行关旧开新，新增只开新，退出只关旧。默认沿用目标月内现有最新 `effective_start_date`，目标月无现存版本时回退到月首，也可用 `--attr-effective-start-date` 显式覆盖。
- 写入 `dim_store_report_attr` 前，脚本会检查所选生效日是否存在其他不同起始日的有效配置；若存在重叠，直接失败，不做静默覆盖。

### 3.2bb 门店经营负责人快照导入工具（tools/import_store_operation_owner_from_nas.py）

```bash
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --preview-limit 10
python tools/import_store_operation_owner_from_nas.py --file-path "\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx" --sheet-name 门店负责人映射模板 --snapshot-date 2026-04-21 --preview-limit 10
python tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-04-21 --apply --created-by your_name
python -m unittest test_store_operation_owner_import.py
```

说明：
- 默认读取 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店负责人映射表.xlsx`，兼容 `门店负责人映射表 / 门店负责人映射模板` 两个工作表名；必填表头为 `门店编码 / 门店名称 / 负责人`，`备注`、`生效日期`、`失效日期` 可选。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L27)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L29)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L40)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L142)
- 若专题调度未显式传 `--owner-snapshot-date`，当前默认不再取“今天”，而是跟随本轮专题实际处理的 `report_date` 上界；默认 `previous-day` 模式下，`6-1 00:05` 会使用 `2026-05-31` 作为负责人快照日，避免把整张负责人表误判成 `unexpected_entities`。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L396)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L406)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L1970)
- 脚本仍按 `snapshot_date` 读取 `dim_store_report_attr` 当前有效且纳入口径的门店，再叠加 `cfg_store_assessment_assignment` 与 `cfg_store_assessment_subject_target_daily` 推导当日应维护的经营实体清单；独立门店维护 `STORE`，共同考核维护 `SUBJECT`。若 Excel 未填写日期列，则当前行默认从 `snapshot_date` 起生效；若显式填写 `生效日期 / 失效日期`，则该区间必须覆盖 `snapshot_date`。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L256)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L320)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L561)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L878)
- 若某共同考核经营体存在，负责人快照中只能保留经营体行，不能再同时保留被吸收的 RT 成员门店；被吸收成员行会进入 `unexpected_entities` 并阻断 `--apply`。当前最小单测已覆盖 `RT007 -> SUBJ_SZ_WXTD` 的实体吸收场景。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L437)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L728)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L16)
- 默认模式为 dry-run；只有 `--apply` 才会按快照日覆盖写入 `cfg_store_operation_owner_snapshot`，再把 `dim_store_operation_owner_assignment` 按 `unchanged / changed / new / exited` 维护历史切片。当前历史切换点改为 `snapshot.effective_start_date`，若旧切片起始日不早于新起始日则直接删除旧切片，避免反向区间；若新快照与上一版历史切片完全一致，则直接重开旧版本，不新增重复切片。导入摘要会额外输出 `earliest_history_effective_start_date`，供专题调度计算受影响日期起点。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L794)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L842)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L967)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1104)
- 首次启用前需先执行 `SQL/create_store_operation_owner_tables.sql`；若快照表、历史表或日志表缺失，脚本会直接失败并提示先补 DDL。当前该链路已接入 `scheduled_store_daily_report.py` 自动调度：目标导入完成后会继续执行负责人导入，并按 `file_md5 + snapshot_date` 做独立幂等判重。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L37)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L223)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L991)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L1223)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L1)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L23)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L48)；[../test_store_operation_owner_import.py](../test_store_operation_owner_import.py#L66)

专题调度补充：负责人链路发生 `changed/new/exited` 时，`scheduled_store_daily_report.py` 会优先使用导入摘要里的 `earliest_history_effective_start_date` 作为负责人链路受影响日期起点，再与目标月月初取较大值生成回刷窗口，而不再固定从 `owner_snapshot_date` 起算。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L765)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L822)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L833)

### 3.2bc 免税月累计导入工具（tools/import_duty_free_store_mtd_sales_from_nas.py）

```bash
python tools/import_duty_free_store_mtd_sales_from_nas.py --preview-limit 10
python tools/import_duty_free_store_mtd_sales_from_nas.py --file-path "\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\免税门店月累计销售.xlsx" --sheet-name 免税月累计 --preview-limit 10
python tools/import_duty_free_store_mtd_sales_from_nas.py --apply --created-by your_name
python -m unittest test_import_duty_free_store_mtd_sales_from_nas.py
```

说明：
- 默认读取 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\免税门店月累计销售.xlsx` 的 `免税月累计` sheet；模板固定表头为 `目标月份 / 数据版本 / 门店ID / 门店名称 / 渠道类型 / 月累计`。来源：[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L27)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L34)
- 同一份文件只允许一个 `目标月份` 和一个 `数据版本`；`门店ID` 列兼容数值型 `dim_store.store_id` 与 `RTxxx` 这类 `dim_store.store_code`，导入前会统一解析为 `store_id`，并校验当前有效 `dim_store_report_attr.is_duty_free='Y'`。文件中的 `门店名称 / 渠道类型` 只用于对齐真值，不会反写维表；`月累计` 空白按 `0.00` 解析。来源：[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L88)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L101)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L196)；[../tools/import_duty_free_store_mtd_sales_from_nas.py](../tools/import_duty_free_store_mtd_sales_from_nas.py#L351)
- 默认模式为 dry-run；只有 `--apply` 才会按 `target_month + data_version` 先删后插 `cfg_duty_free_store_mtd_sales`，并把 `changed / new / exited` 摘要写入 `log_duty_free_store_mtd_sales_import`。该摘要仅用于专题调度判断是否新增免税受影响日期。来源：[../SQL/create_cfg_duty_free_store_mtd_sales.sql](../SQL/create_cfg_duty_free_store_mtd_sales.sql#L3)；[../SQL/create_cfg_duty_free_store_mtd_sales.sql](../SQL/create_cfg_duty_free_store_mtd_sales.sql#L16)
- 当前链路只承接外部 `月累计销售额` 单指标，不补日销、销量、订单数或折扣等其它事实；后续 `ads_store_daily_report` 只会据此覆盖免税实体的 `mtd_sales_amt / month_ach_rate / mtd_rank`。来源：[../etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L589)；[../etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L632)

### 3.2c 门店日报专题调度入口（scheduled_store_daily_report.py）

```bash
python scheduled_store_daily_report.py --conn-test
python scheduled_store_daily_report.py
python scheduled_store_daily_report.py --target-month 2026-04
python scheduled_store_daily_report.py --target-month 2026-04 --owner-snapshot-date 2026-04-22 --owner-sheet-name 门店负责人映射模板
python scheduled_store_daily_report.py --target-month 2026-04 --no-run-owner-import
python scheduled_store_daily_report.py --target-month 2026-04 --no-run-affected-ads
python scheduled_store_daily_report.py --rerun-report-date 2026-04-01 --rerun-report-date 2026-04-02 --rerun-data-version v1
```

说明：
- 该入口是门店日报当前正式专题调度脚本，定位为“调度包装层”，不直接改写目标导入工具的手工契约。
- 自动模式会先选择 NAS 目录中最后修改的目标文件，当前推荐命名为 `YYYYMM考核数据配置表.xlsx`，并兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`；脚本随后校验解析出的 `target_month` 是否等于本轮自动 `report_date` 所在月份。默认 `previous-day` 模式下，`6-1 00:05` 会继续接受 `2026-05` 快照并补跑 `2026-05-31`；若切到 `current-day`，则要求快照目标月与当天自然月一致。若不是本轮自动 `report_date` 所在月份，则本轮记录跳过。若需处理历史或未来月份，必须显式传入 `--target-month` 或 `--file-path`。
- 默认会同步刷新 `dim_store_report_attr`；若只想写 `cfg_store_target_daily`，可追加 `--no-sync-store-report-attr`。
- 在正式 apply 前，会先检查 `log_store_target_import`；若相同 `file_md5 + target_month + target_version` 已存在最近一次 `SUCCESS` 记录，则当前调度直接跳过，不重复写库。
- 当前专题调度已负责 NAS 目标导入、负责人快照导入、免税月累计导入、门店属性同步、共同考核配置同步、受影响日期判断，以及按日期列表顺序触发 `ads_store_daily_report`、`ads_store_daily_subject_report` 与 `ads_daily_sales` 批量重跑。
- 负责人导入发生在目标导入之后；若相同 `file_md5 + snapshot_date` 已存在最近一次 `SUCCESS` 记录，则当前负责人链路直接跳过，不重复写库。只有 `changed/new/exited` 发生时，负责人链路才会为专题调度补充新的受影响日期。
- 免税月累计导入同样在专题调度内独立执行；若相同 `file_md5 + target_month + data_version` 已存在最近一次 `SUCCESS` 记录，则当前免税链路直接跳过，不重复写库。只有 `changed/new/exited` 发生时，免税链路才会把专题调度统一上界对应的当天 `report_date` 加入受影响日期集合；可用 `--no-run-duty-free-import` 临时关闭。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2026)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2133)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2136)
- 若命中 `--conn-test`，则不触发批量重跑。若目标链路幂等跳过、负责人链路也没有新增受影响日期，专题调度会先检查当前 `data_version` 下三张保留 ADS 的 `report_date` 是否已补到统一上界；若仍存在缺口，则自动按“最落后 ADS + 1 天到统一上界”补跑，若有任一 ADS 在当月仍无数据，则按“月初到统一上界”整段补跑。若日期已覆盖到统一上界，则继续比较近 7 天专题 freshness 来源表的 `etl_time` 与三张保留 ADS 的 `etl_time`；该来源默认按 `cutover_mode` 派生：`legacy` 读 `dws_sales_daily`，`shadow_compare` / `v2` 读 `dws_sales_daily_v2`，也可用 `--sales-freshness-source legacy|v2` 显式覆盖；源表更新更晚时按 freshness 命中日期重跑。可用 `--no-run-owner-import` 临时关闭负责人链路；如需覆盖默认负责人文件或快照日，可追加 `--owner-file-path`、`--owner-sheet-name`、`--owner-snapshot-date`。来源：[../cutover_controls.py](../cutover_controls.py#L55)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L474)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L545)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2076)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2226)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L2575)
- 若正式 IMPORTED 后受影响日期非空，默认自动按“门店层 -> 统计主体层 -> 销售看板月度战役”顺序触发批量重跑；如只想保留日期判断结果，可追加 `--no-run-affected-ads`。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L984)
- 若自动链路中断或需要手工补跑，可通过 `--rerun-report-date YYYY-MM-DD` 多次传入显式日期列表，直接重跑当前三张保留 ADS。来源：[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L49)；[../scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L984)

### 3.3 查询与审计执行面

- 结构探查：优先使用 MySQL / Oracle MCP 或 `db-inspector`，仅查看表、字段、索引与注释。
- 固定对账：优先使用 `tools/check_ods_incremental.py` 与 `tools/check_ods_retailitem_quality.py`，避免重复实现既有口径。
- 自由查数：通过 `tools/query_data.py` 统一承接 MySQL / Oracle 只读查询，并支持导出 `table`、`json`、`csv`、`excel`。
- ADS 固定导出：通过 `tools/export_ads.py` 导出 `ads_inventory_health`；结构快照由 `tools/snapshot_mysql_hefangdw_schema.py` 与 `tools/snapshot_oracle_bosnds3_schema.py` 生成。
- 经验复盘：通用经验沉淀到 `docs/AGENT_LESSONS.md`，由 `scripts/log_agent_lesson.py` 负责结构化写入；用户明确纠错的业务结论也必须进入该台帐。
- Hook 边界：当前仓库内已确认可用的是 `.claude/settings.json` 的 `PostToolUse` 提示型 Hook；GitHub Copilot 当前未暴露可在仓库本地强制执行的“会话结束自动写台帐”钩子，因此需要保留收尾自检与命令兜底。

---

## 4. 数据库连接

### 4.0 环境边界

- 当前公司开发环境由用户单人维护数据库与数仓工程，不存在可默认协同的内部 DBA / 运维角色。
- Oracle 源库运行在阿里云；MySQL 目标库与 `hefang_dw` 项目运行在公司服务器虚拟机。
- 因此，涉及真实 CRM 落库结构时，不应默认从当前 `hefang_dw` MySQL 中取得 `shuyun_ods` 实证；若本地未落表，需改为索取外部对接材料或未来联调环境证据。

### 4.1 数据源（Oracle）

| 参数 | 环境变量 | 默认值（`.env.example`）|
|------|----------|------------------------|
| 主机 | `ORACLE_HOST` | localhost |
| 端口 | `ORACLE_PORT` | 1521 |
| 服务名 | `ORACLE_SERVICE` | orcl |
| 用户名 | `ORACLE_USER` | change_me |
| 密码 | `ORACLE_PASSWORD` | change_me |

驱动：`python-oracledb`（thin 模式，**无需安装 Oracle Instant Client**）

### 4.2 数据目标（MySQL）

| 参数 | 环境变量 | 默认值 |
|------|----------|--------|
| 主机 | `MYSQL_HOST` | localhost |
| 端口 | `MYSQL_PORT` | 3306 |
| 数据库 | `MYSQL_DB` | hefang_dw |
| 用户名 | `MYSQL_USER` | change_me |
| 密码 | `MYSQL_PASSWORD` | change_me |

驱动：`SQLAlchemy + PyMySQL`

### 4.3 统一连接工厂

hefang_dw 运行链路内的 SQLAlchemy Engine、PyMySQL 直连与 Oracle 直连统一通过 [../db_connections.py](../db_connections.py#L40-L106) 创建；基础账号、主机、库名仍由 [../config.py](../config.py) 读取环境变量并作为唯一配置来源。统一工厂只封装连接池、连接可用性探测、超时与空闲回收参数，不改变任何 ETL 业务 SQL 或指标口径。

自 2026-04-28 起，凡新增或修改涉及数据库读写的 ETL、调度、工具脚本或 SQL，在接入调度前都必须显式评估超时风险、选择匹配的 `timeout_profile`，并保留至少一类超时验证证据（命令、日志、报错或耗时）；不得把“未验证超时边界”的链路直接接入主链或总控。

| 工厂函数 | 适用场景 | 关键控制 |
|----------|----------|----------|
| `create_mysql_engine()` | pandas / SQLAlchemy 写入与只读查询 | `timeout_profile`、`pool_pre_ping`、`pool_size`、`max_overflow`、`pool_timeout`、`pool_recycle`、MySQL 连接/读/写超时 |
| `connect_mysql()` | 命名锁、事务控制、DictCursor 工具脚本 | `timeout_profile`、`connect_timeout`、`read_timeout`、`write_timeout`，调用方继续负责 `close()` |
| `create_oracle_engine()` | ODS / 对账工具的 Oracle SQLAlchemy 查询 | `pool_pre_ping`、`pool_size`、`max_overflow`、`pool_timeout`、`pool_recycle` |
| `connect_oracle()` | DIM 抽取、连通性验证与自动化对账 | 调用方继续负责 `close()` |

MySQL 当前内置 `default` / `etl` / `long_running` 三档超时配置，定义见 [../db_connections.py](../db_connections.py#L47-L105)。默认连接池与超时参数集中定义在 [../db_connections.py](../db_connections.py#L25-L37)，可通过 `MYSQL_POOL_SIZE`、`MYSQL_MAX_OVERFLOW`、`MYSQL_POOL_TIMEOUT`、`MYSQL_POOL_RECYCLE`、`MYSQL_CONNECT_TIMEOUT`、`MYSQL_READ_TIMEOUT`、`MYSQL_WRITE_TIMEOUT`、`MYSQL_ETL_READ_TIMEOUT`、`MYSQL_ETL_WRITE_TIMEOUT`、`MYSQL_LONG_RUNNING_READ_TIMEOUT`、`MYSQL_LONG_RUNNING_WRITE_TIMEOUT`、`ORACLE_POOL_SIZE`、`ORACLE_MAX_OVERFLOW`、`ORACLE_POOL_TIMEOUT`、`ORACLE_POOL_RECYCLE` 覆盖。

---

## 5. 关键业务常量（config.py）

> 以下常量由业务确认，**修改前必须获得业务确认**：

| 常量 | 值（ID列表）| 含义 |
|------|------------|------|
| `MAIN_CATEGORY_IDS` | (134,142,139,138,141,143,133,136,140,137,144,145) | 主销品类别（12个）|
| `PROPERTY_ONSALE` | (224,296,297) | 在售款性质 ID |
| `PROPERTY_NEW` | (225,298,299) | 新品性质 ID |
| `PROPERTY_DISCONTINUED` | (127,126,152) | 绝版款性质 ID |

库存状态判断逻辑（见 `etl_ads_health.py`）：
- 总仓：`C_STORE.CODE = '001'`
- 云仓：`C_STORE.IS_ALLO2OSTORAGE = 'Y'`

---

## 6. 调度依赖图

```
Windows 任务计划（每日 xx:xx）
    └─▶ run_scheduled_etl.bat
            └─▶ scheduled_etl.py
                    ├─▶ run_etl.py（9步流水线）
                    │       ├─▶ etl_dim_product.run()
                    │       ├─▶ etl_dim_sku.run()
                    │       ├─▶ etl_dim_store.run()
                    │       ├─▶ etl_dim_channel.run()
            │       ├─▶ run_ods.run(backfill_days=7)
            │       ├─▶ etl_dws_sales.run(days_back=7, include_today=True)
                    │       ├─▶ etl_dws_inventory.run()
                    │       ├─▶ dabo_ready（标签主线就绪检查）
                    │       └─▶ etl_ads_health.run()
                    └─▶ test_etl_automation.py（仅在 ETL 成功后执行）

Windows 任务计划（推荐总控入口，例如 00:05 / 12:30）
    └─▶ run_scheduled_total_control.bat
            └─▶ scheduled_total_control.py
                    ├─▶ cutover_mode=v2 时先执行 DWS v2 读源预刷新（阻断）
                    ├─▶ scheduled_etl.py
                    │   └─▶ run_etl.py（9步主链）
                    ├─▶ scheduled_store_daily_report.py
                    │   └─▶ 仅在主链 exit_code=0 时才继续执行专题链
                    └─▶ scheduled_dws_v2_shadow.py
                        └─▶ legacy / shadow_compare 后置非阻断观察；V2 已前置刷新时本轮标记 SKIPPED
```

ODS 流水线当前已纳入主流水线，也保留独立手动执行入口：
```
Windows 任务计划 / 手动触发
    └─▶ run_etl.py
            ├─▶ ods_sync（内部调用 run_ods.run）
            │       ├─▶ etl_ods_fa_storage.run()
            │       ├─▶ etl_ods_m_retail.run()
            │       └─▶ etl_ods_m_retailitem.run()
            └─▶ 后续 DWS / ADS 主链

手动独立执行：
    └─▶ python run_ods.py
            ├─▶ etl_ods_fa_storage.run()
            ├─▶ etl_ods_m_retail.run()
            └─▶ etl_ods_m_retailitem.run()

销售看板月度战役入口：
    └─▶ python etl_ads_daily_sales.py --report-date YYYY-MM-DD --data-version v1
            ├─▶ SQL/create_ads_daily_sales.sql（用户手工执行建表）
            └─▶ SQL/check_ads_daily_sales_min.sql（最小对账）

门店日报专项入口：
    └─▶ python etl_ads_store_daily_report.py --report-date YYYY-MM-DD --data-version v1
            └─▶ 脚本内置 SQL 模板（不依赖外部 .sql 文件）

门店日报统计主体层入口：
    └─▶ python etl_ads_store_daily_subject_report.py --report-date YYYY-MM-DD --data-version v1
            └─▶ ads_store_daily_report + assessment cfg tables

门店日报专题调度：
    └─▶ run_scheduled_store_daily_report.bat
        └─▶ scheduled_store_daily_report.py
            └─▶ tools/import_cfg_store_target_daily_from_nas.py --apply --sync-store-report-attr
                └─▶ log_store_target_import / cfg_store_target_daily / dim_store_report_attr / cfg_store_assessment_*
            └─▶ etl_ads_store_daily_report.py
            └─▶ etl_ads_store_daily_subject_report.py
            └─▶ etl_ads_daily_sales.py

DWS v2 shadow 独立入口：
    └─▶ run_scheduled_dws_v2_shadow.bat
        └─▶ scheduled_dws_v2_shadow.py
            ├─▶ raw ODS shadow
            ├─▶ DWD shadow
            └─▶ DWS v2 shadow

总控包装说明：
    └─▶ scheduled_total_control.py
            ├─▶ 默认不改写 run_etl.py 与 scheduled_store_daily_report.py 的 legacy 边界；若显式传入 `--cutover-mode` / `--rollback-to-legacy`，仅向主链与专题链透传相同参数
            ├─▶ 主链失败时直接短路，不继续触发销售专题链与 DWS v2 shadow
            ├─▶ 销售专题失败不阻断 shadow，shadow 失败只记 WARNING，不影响旧 DWS / ADS
            ├─▶ 总控统一收集主链、专题链与 shadow 子链结构化摘要，并只发送一条企业微信汇总消息
            └─▶ 适合把“主链固定时点 + 成功后再跑专题 + 非阻断 shadow 观察”合并成一个 Windows 计划任务入口

门店经营负责人快照导入：
    └─▶ python tools/import_store_operation_owner_from_nas.py --apply --created-by your_name
            ├─▶ SQL/create_store_operation_owner_tables.sql
            ├─▶ dim_store_report_attr / cfg_store_assessment_assignment / cfg_store_assessment_subject_target_daily
            └─▶ cfg_store_operation_owner_snapshot / dim_store_operation_owner_assignment / log_store_operation_owner_import
```

---

## 7. 告警机制

- 模块：`alerts.py`，通过 `WECHAT_WEBHOOK` 环境变量配置
- 触发时机：每步 ETL 失败且重试耗尽后，以及整体流水线完成后
- 摘要格式：含执行时间、总耗时、成功/警告/失败计数、步骤明细
- 总控模式：`scheduled_total_control.py` 会抑制子链各自的企业微信摘要，改为在总控层统一汇总 DWS v2 前置刷新、主链、专题链与后置 `dws_v2_shadow` 执行结果；V2 前置刷新失败会把主链和专题链标记为 `SKIPPED`，后续新专题只需输出相同结构化摘要即可接入总控统一出口
- 不可重试错误（立即告警，不等待）：ORA-01017 / invalid username / access denied 等（见 `config.py:ETL_NON_RETRYABLE_ERROR_KEYWORDS`）

---

## 8. 技术栈

| 组件 | 版本/说明 |
|------|----------|
| Python | 3.13.x |
| python-oracledb | thin 模式（无需 Instant Client）|
| SQLAlchemy | ORM + 原生 SQL |
| PyMySQL | MySQL 驱动 |
| Pandas | 数据转换 |
| Oracle DB | 19c EE（伯俊 ERP 数据源）|
| MySQL | 8.0.x（何方数仓目标）|
| Windows 任务计划 | 生产调度 |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.7.68 | 2026-06-08 | 将门店日报与 ads_daily_sales 的商品范围架构切换为固定排除 `147/149/150`，移除对 `dim_report_product_rule` 的运行时依赖 |
| v0.7.67 | 2026-05-26 | 补充门店目标导入规则：`门店类型` 包含 `免税` 时同步 `dim_store_report_attr.is_duty_free='Y'` |
| v0.7.66 | 2026-05-26 | 免税月累计模板的 `门店ID` 列兼容 `store_code`，并记录空白 `月累计` 按 `0.00` 解析；上线前仍需校验 `is_duty_free='Y'` |
| v0.7.65 | 2026-05-26 | 将免税月累计外部文件语义从 `reportdate` 更正为 `目标月份`，并补记专题调度按统一上界回刷当天 `report_date` |
| v0.7.64 | 2026-05-25 | 新增免税月累计 NAS 导入工具与专题调度分支，并明确 `ads_store_daily_report` 只对免税实体覆盖 `mtd_sales_amt / month_ach_rate / mtd_rank` |
| v0.7.64 | 2026-06-06 | 退役 3 张销售专题 ADS，并将专题调度架构说明收口到当前保留链路 |
| v0.7.63 | 2026-05-14 | 补记 V2 pre-refresh 场景下若 same-day old DWS 快照尚未产出，则 inventory old DWS 基线记为 `SKIPPED`、不再误阻断主链 |
| v0.7.62 | 2026-05-13 | 补记 M6 Windows 计划任务已切到 V2 wrapper 后的总控顺序修复：V2 模式先阻断刷新 DWS v2 读源，再运行主链 ADS |
| v0.7.61 | 2026-05-12 | 补记主链 / 总控 / 门店专题已新增 cutover / rollback 开关，默认仍为 legacy，`shadow_compare` 仅做 v2 报告型对账 |
| v0.7.60 | 2026-05-12 | 补记 DWS v2 最新无参数 shadow 已给出 ADS gate READY、下一步仅进入总控非阻断观察，并冻结 ADS 既有字段名不可改的实施边界 |
| v0.7.59 | 2026-05-12 | 将负责人导入架构说明更新为兼容 Excel 显式生效/失效日期，并补记专题调度按 earliest_history_effective_start_date 起算回刷窗口 |
| v0.7.58 | 2026-05-08 | 将销售专题月级组织层的门店范围说明收口到 `report_date` 当天目标已生效门店，并补记 RT116 在 5 月上旬仅造成范围漂移 |
| v0.7.57 | 2026-05-08 | 将销售专题日层对象的门店范围说明收口到 `report_date` 当天目标已生效门店，和门店日报专题保持一致 |
| v0.7.56 | 2026-05-07 | 补记 `scheduled_dws_v2_shadow.py` / `run_scheduled_dws_v2_shadow.bat` 已落地，并将 `dws_v2_shadow` 作为非阻断子链接入 `scheduled_total_control.py` |
| v0.7.55 | 2026-05-07 | 补记 DWS v2 S3 手工写入分支和 `dws_v2_write_utils.py`；默认 dry-run，写入需确认令牌，仍不接总控 |
| v0.7.54 | 2026-05-07 | 补记 DWS v2 dry-run / conn-test 脚本和测试入口，明确当前无写库入口、不接总控 |
| v0.7.53 | 2026-04-29 | 补记销售专题月级组织层的 month_order_cnt 已改为汇总 ads_store_daily_report.day_order_cnt，并校准销售专题 SKU 层的判单规则为过滤后净额与近零容差 |
| v0.7.52 | 2026-04-29 | 补记 ads_store_daily_subject_report 订单数直接承接门店层，因此自动继承过滤后金额与近零容差口径 |
| v0.7.51 | 2026-04-28 | 补充数据库读写链路的超时治理要求，并记录 timeout_profile 分层与长跑 ETL 样板 |
| v0.7.50 | 2026-04-27 | 将销售专题日层对象的架构说明统一到 ads_store_daily_report 权威口径，并修复章节中误插的版本记录块 |
| v0.7.49 | 2026-04-27 | 新增 db_connections.py 统一连接工厂架构说明，并补充连接池与超时参数边界 |
| v0.7.48 | 2026-04-27 | 将 scheduled_store_daily_report.py 架构说明更新为六层 ADS，并新增 DWS freshness 触发规则 |
| v0.7.47 | 2026-04-27 | 为 scheduled_total_control.py 增加子链结构化摘要聚合，统一企业微信出口并支持后续专题复用接入 |
| v0.7.46 | 2026-04-27 | 为 scheduled_store_daily_report.py 补充自然日推进兜底，按五张 ADS 的 report_date 覆盖缺口自动补跑到统一上界 |
| v0.7.45 | 2026-04-24 | 新增 scheduled_total_control.py 与 run_scheduled_total_control.bat，总控层串联主链成功后再触发销售专题链 |
| v0.7.44 | 2026-04-23 | 同步销售主题 ADS 改为 report_channel_type 明细口径，并移除 全国/全部 物理汇总行架构描述 |
| v0.7.43 | 2026-04-23 | 补记 run_etl.py 已将 dws_sales 主链窗口对齐到 ODS 7 天回刷，并同步 2026-04-21/22 五层重跑与复对账结果 |
| v0.7.42 | 2026-04-23 | 补记销售专题 SKU 层连带贡献精度要求提升到 DECIMAL(14,2)，并同步 2026-04-22/v2 五层调度实跑结果 |
| v0.7.41 | 2026-04-22 | 补记负责人快照已接入专题调度，并新增 ads_store_daily_report 负责人字段与待执行 alter 说明 |
| v0.7.40 | 2026-04-21 | 新增门店经营负责人快照导入链路、依赖图与最小单测入口说明 |
| v0.7.39 | 2026-04-17 | 补记销售专题 SKU 层已完成专题调度第五层显式重跑验证，并更新五层写库状态 |
| v0.7.38 | 2026-04-17 | 将销售专题 SKU 层接入专题调度第五层，并补记当前仅完成代码接链与单元测试验证 |
| v0.7.37 | 2026-04-17 | 更新销售专题 SKU 层为含 attach_contribution 的二期样板，并补记 ODS 订单级口径与目标旧结构告警 |
| v0.7.36 | 2026-04-17 | 更新销售专题 SKU 层为已补 sales_mix_pct、rank_no、trend_tag 的二期样板，并注明本轮仍未接入专题调度 |
| v0.7.35 | 2026-04-16 | 新增销售专题月级组织层与 SKU 层的架构入口、独立运行说明与最小对账路径 |
| v0.7.34 | 2026-04-16 | 将销售专题组织日层接入门店日报专题调度第四层，并补记四层实跑验证结果 |
| v0.7.33 | 2026-04-16 | 同步专题调度自动跳过与显式 rerun 写库验证状态，并补记销售专题组织日层的 v2 复验与接链建议 |
| v0.7.32 | 2026-04-16 | 将 ads_daily_sales 纳入门店日报专题调度三层批量重跑，并注明当前仅完成单元测试验证 |
| v0.7.31 | 2026-04-16 | 更新 ads_daily_sales 为已完成 2026-04-15/v1 首轮样本与最小对账验证状态 |
| v0.7.30 | 2026-04-16 | 更新 ads_daily_sales 为当前库已建表但空表待样本验证状态 |
| v0.7.29 | 2026-04-15 | 新增销售看板月度战役仓库样板入口，并将销售专题组织日层状态更新为已完成单日验证 |
| v0.7.28 | 2026-04-15 | 新增销售专题组织日层仓库样板入口、建表脚本与最小对账路径说明 |
| v0.7.27 | 2026-04-15 | 将门店日报目标导入 NAS 根目录从 月度日目标配置表 更新为 目标配置表 |
| v0.7.26 | 2026-04-10 | 将 ads_store_daily_report 调整为最终经营实体粒度，并将主体层更新为基于最终结果补主体编码 |
| v0.7.25 | 2026-04-10 | 更新门店日报目标 NAS 命名约定为 YYYYMM考核数据配置表.xlsx，并注明自动扫描兼容历史旧文件名 |
| v0.7.24 | 2026-04-10 | 新增门店日报统计主体层与共同考核配置架构，并同步专题调度双层重跑链路 |
| v0.7.23 | 2026-04-09 | 将 ads_inventory_health 的达播来源更新为标签主线优先、legacy 回退兜底，并同步主调度语义 |
| v0.7.22 | 2026-04-09 | 将主流水线 dabo_ready 更新为达播标签主线优先检查，并明确 legacy CSV 仅作为 ads_health 兼容回填开关 |
| v0.7.21 | 2026-04-09 | 为达播订单标签工具链补充 canonical_system_order_id 归一桥接层说明 |
| v0.7.20 | 2026-04-09 | 明确专题调度只自动处理当前月份快照，并将门店属性同步语义更新为未变化/变更/新增/退出分类 |
| v0.7.19 | 2026-04-08 | 新增门店日报专题调度入口，明确 MD5 判重与当前只负责目标导入链路 |
| v0.7.18 | 2026-04-08 | 新增门店属性快照登记工具与 pending_apply 台账语义 |
| v0.7.17 | 2026-04-08 | 更新门店日报渠道粗分类生成列为现网已执行状态，并补充细分类支持范围 |
| v0.7.16 | 2026-04-08 | 新增 ads_dabo_order_label 内部标签主线与统一 Excel 工具链架构说明 |
| v0.7.15 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充目录选档规则 |
| v0.7.14 | 2026-04-08 | 补充门店日报目标导入在多月份 NAS 文件下需显式传入 --target-month 的架构说明 |
| v0.7.13 | 2026-04-07 | 补充 run_ods --full 默认追加固定 as-of recent catch-up 的时序说明 |
| v0.7.12 | 2026-04-03 | 补充门店日报目标导入支持基于门店类型同步 dim_store_report_attr 的参数与生效日策略 |
| v0.7.11 | 2026-04-03 | 更新门店日报目标 NAS 导入为现网已建表、已首轮 apply、已完成专项消费验证 |
| v0.7.10 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本与日志表 DDL 说明 |
| v0.7.9 | 2026-04-03 | 冻结门店日报目标 NAS 目录与固定文件命名约定 |
| v0.7.8 | 2026-04-03 | 明确门店日报目标配置采用 NAS 投递目录加 Python 定时扫描导入的正式路径 |
| v0.6.3 | 2026-03-01 | 对齐 v0.6.3 目录与调度描述 |
| v0.7.0 | 2026-03-04 | 补充 Agent/Skills 目录与 MCP 本地配置 |
| v0.7.1 | 2026-03-16 | 同步 run_ods 参数与 ODS 质检说明 |
| v0.7.2 | 2026-03-18 | 增加 dim_channel 维度实现并将主流水线更新为 8 步 |
| v0.7.4 | 2026-03-18 | 新增只读查数工具、data-query skill/agent 与 MCP 降级说明 |
| v0.7.5 | 2026-03-18 | 新增经验台帐、复盘脚本与 Hook 边界说明 |
| v0.7.7 | 2026-04-03 | 新增门店经营日报独立 ETL 入口与配置依赖说明 |
