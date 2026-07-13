# M4 DWS v2 并行表、调度接入与回滚方案

---

## 1. 文档用途

本文承接 M3 raw / DWD 旁路验证结果，先设计 DWS v2 并行表、并行调度接入、对账验收与回滚路径。M4 初始交付输出设计、DDL 草案和只读对账 SQL；2026-05-07 用户说明两份 DWS v2 DDL 草案已人工建表，Copilot 仅通过只读 `INFORMATION_SCHEMA` 与行数查询完成空表核验。随后已新增 `etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py` 的 dry-run / conn-test 模式；本轮按用户“进入 S3 写入分支”和“授权你补完 S3 实跑验收关口”的明确授权，完成 S3 手工写入入口与实跑验收：销售 `20260428-20260430` 写入 3417 行至 `dws_sales_daily_v2`，库存 `20260507` 写入 75104 行至 `dws_inventory_daily_v2`，两者 DWD-v2 mismatch 均为 0。额外只读复核显示销售 v2 与旧 `dws_sales_daily` 在验收窗口 0 差异；库存 v2 与旧 `dws_inventory_daily` 存在 200 条同 key `qty` 差异、`qty_total_diff=99`、`qtypurchaserem_total_diff=0`，结合旧表 `MAX(etl_time)=2026-05-07T04:31:36Z` 与 v2 `source_max_loaded_at=2026-05-07 09:50:24`，当前按快照时点差异记录，不视为 DWD→v2 转换错误。随后已新增 `scheduled_dws_v2_shadow.py`、`run_scheduled_dws_v2_shadow.bat`，并把 `dws_v2_shadow` 以非阻断子链接入 `scheduled_total_control.py`；当前仍未修改 `run_etl.py` / `scheduled_etl.py`，也未切 ADS 读源。

---

## 2. 本轮设计边界

| 项 | 结论 |
|----|------|
| 本轮性质 | M4 设计稿 + S1 用户人工建表后的只读核验 + S2 dry-run / conn-test 脚本 + S3 受控手工写入分支与实跑验收 + S4 独立 shadow 调度与总控非阻断接入已完成，仍不是生产切换 |
| 目标对象 | `dws_sales_daily_v2`、`dws_inventory_daily_v2` |
| 新增 SQL 草案 | `SQL/draft_create_dws_sales_daily_v2.sql`、`SQL/draft_create_dws_inventory_daily_v2.sql`、`SQL/check_dws_v2_parallel_reconciliation.sql` |
| 证据缓存 | `reports/context_cache/dws_v2_parallel_design_evidence_20260507.json`、`reports/context_cache/dws_v2_manual_ddl_verification_20260507.json`、`reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json`、`reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json` |
| DDL / 索引 | 两份 DDL 草案已由用户人工执行；Copilot 先只读确认两张表存在且具备粒度唯一键，再在用户明确授权下完成一次 S3 实跑验收 |
| ETL / 调度 | 已新增 `etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py`、`dws_v2_write_utils.py`、`scheduled_dws_v2_shadow.py` 和 `run_scheduled_dws_v2_shadow.bat`；默认 dry-run / conn-test，S3 写入需显式追加 `--execute --confirm-write WRITE_DWS_SALES_V2` 或 `--execute --confirm-write WRITE_DWS_INVENTORY_V2`。其中销售 shadow 默认回算窗口已改为 `31` 天，用于覆盖 `ads_inventory_health` 的 `today-30 ~ today` 包含当天消费窗；当销售窗口大于主链 `7` 天回刷时，shadow 内销售 raw / DWD / DWS v2 步骤自动切到 `long_running`。库存脚本支持 `--source-loaded-at-cutoff` / `--align-with-old-dws`，并在写入时按 `date_id` 先删后灌，确保 shadow compare 固定到同一 source snapshot timepoint；`scheduled_total_control.py` 当前已把 `dws_v2_shadow` 作为非阻断子链接入，支持 `--shadow-only`，仍不改 `run_etl.py` 主链 |
| 下游切换 | 不切换 ADS，不改现有 `dws_sales_daily` / `dws_inventory_daily` |

---

## 3. 已确认事实与证据

| 事实 | 证据 |
|------|------|
| 当前 `run_etl.py` 主链包含 `dws_sales`、`dws_inventory`，但没有 DWD 步骤 | `run_etl.py#L44-L55` |
| 当前销售 DWS 仍从 `ods_m_retailitem` + `ods_m_retail` 聚合并写入 `dws_sales_daily` | `etl_dws_sales.py#L1-L4`、`etl_dws_sales.py#L44-L64`、`etl_dws_sales.py#L147-L162` |
| 当前库存 DWS 仍从 `ods_fa_storage` 聚合并写入 `dws_inventory_daily` | `etl_dws_inventory.py#L1-L4`、`etl_dws_inventory.py#L48-L60`、`etl_dws_inventory.py#L153-L168` |
| M3 销售 DWD 已完成 20260428-20260430 完整业务日期验证，DWD 5103 行并与当前 DWS 日级汇总对齐 | `reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json` |
| M3 库存 DWD 已完成 20260507 full raw 快照，raw→DWD 自洽；与当前 DWS `qty` 差 337 的原因是快照时间点不同 | `reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json` |
| 本轮已用只读 `INFORMATION_SCHEMA` 核实现有 DWS / DWD 字段与索引，用于 v2 草案列与索引设计 | `reports/context_cache/dws_v2_parallel_design_evidence_20260507.json` |
| 用户已人工建 `dws_sales_daily_v2`、`dws_inventory_daily_v2`；Copilot 只读核验显示销售 v2 33 列、库存 v2 31 列，两表均为 0 行，均具备 `date_id + store_id + product_id + m_productalias_id` 唯一键、`validation_status` 与 `etl_time` | `reports/context_cache/dws_v2_manual_ddl_verification_20260507.json` |
| 已新增 DWS v2 dry-run / conn-test / S3 手工写入脚本；销售脚本默认窗口为 20260428-20260430、`timeout_profile='etl'`，库存脚本默认快照为 20260507、`timeout_profile='long_running'`；写入分支需确认令牌、命名锁、显式事务和写后 DWD-v2 对账；本轮已完成 CLI、dry-run、无令牌拒绝、只读 conn-test 和 S3 实跑验收，销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0 | `etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py`、`dws_v2_write_utils.py`、`test_dws_v2_dry_run.py`、`reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json`、`reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json` |
| 已新增 `scheduled_dws_v2_shadow.py` 独立 shadow 调度与 `run_scheduled_dws_v2_shadow.bat` Windows wrapper；`scheduled_total_control.py` 当前已接入 `dws_v2_shadow` 非阻断子链，支持 `--shadow-only`，topic 失败不阻断 shadow，shadow 失败只记 WARNING | `scheduled_dws_v2_shadow.py`、`run_scheduled_dws_v2_shadow.bat`、`scheduled_total_control.py`、`test_scheduled_total_control.py` |

---

## 4. DWS v2 总体设计原则

1. **并行优先**：新增 `_v2` 表，不原地改造或替换 `dws_sales_daily`、`dws_inventory_daily`。
2. **兼容下游优先**：v2 表保留现有 DWS 核心字段，便于与旧表一键对账；新增字段只作为审计、血缘和长期扩展，不直接影响现有 ADS。
3. **DWD 来源优先**：v2 表第一阶段只消费已验证的 `dwd_sales_retail_item`、`dwd_inventory_storage_snapshot`，不再直接从生产 ODS 聚合。
4. **先自洽后替换**：验收顺序为 DWD→DWS v2 自洽、DWS v2→现有 DWS 对齐、ADS shadow 对比，全部通过且用户确认后才允许讨论切换。
5. **可回滚**：任何阶段都能通过停用 v2 脚本、撤出总控步骤或把下游读源切回旧 DWS 回滚；旧 DWS 在切换完成前持续保留。
6. **超时与锁显式化**：销售小窗口默认 `timeout_profile='etl'`，历史重算用 `long_running`；库存 full snapshot 聚合默认 `long_running`。S3 手工写入默认锁名分别为 `hefang_dw:dws_sales_daily_v2:s3`、`hefang_dw:dws_inventory_daily_v2:s3`，命名锁必须与写入事务在同一个 MySQL 连接中获取和释放。

---

## 5. DWS v2 并行表设计

### 5.1 `dws_sales_daily_v2`

| 设计项 | 方案 |
|--------|------|
| 来源表 | `dwd_sales_retail_item` |
| 粒度 | `date_id + store_id + product_id + m_productalias_id` |
| 唯一键 | `uk_dws_sales_daily_v2_date_store_product_sku(date_id, store_id, product_id, m_productalias_id)` |
| 过滤范围 | `dws_sales_scope_flag = 'Y'` |
| 正向销售 | `is_positive_sale_flag = 'Y'` 时汇总 `qty`、`line_actual_amt`、`line_list_amt` |
| 退货 | `is_return_flag = 'Y'` 时对 `qty`、`line_actual_amt` 取绝对值汇总 |
| 订单数 | `COUNT(DISTINCT CASE WHEN is_positive_sale_flag = 'Y' THEN retail_id END)` |
| 兼容字段 | `date_id`、`store_id`、`store_code`、`is_cloud_store`、`product_id`、`m_productalias_id`、`sales_qty`、`sales_amount`、`sales_amount_list`、`return_qty`、`return_amount`、`order_count`、`etl_time` |
| 增强字段 | `source_dwd_row_count`、`positive_line_count`、`return_line_count`、水位字段、`load_batch_id`、`validation_status`、`validation_note` |
| 净额字段 | v2 草案可生成 `net_qty = sales_qty - return_qty`、`net_amount = sales_amount - return_amount`；因现有 `dws_sales_daily.net_qty/net_amount` 当前 ETL 不填充，切换前必须确认是否对下游暴露非零净值 |

### 5.2 `dws_inventory_daily_v2`

| 设计项 | 方案 |
|--------|------|
| 来源表 | `dwd_inventory_storage_snapshot` |
| 粒度 | `date_id(snapshot_date) + store_id + product_id + m_productalias_id` |
| 唯一键 | `uk_dws_inventory_daily_v2_date_store_product_sku(date_id, store_id, product_id, m_productalias_id)` |
| 过滤范围 | `dws_inventory_scope_flag = 'Y'` |
| 兼容字段 | `date_id`、`store_id`、`store_code`、`is_cloud_store`、`product_id`、`m_productalias_id`、`qty`、`qty_valid`、`qty_occupy`、`qtypurchaserem`、`etl_time` |
| 第一阶段等价 | `qty_valid` 先沿用 `qty`；`qty_occupy` 先保持 0；`qtypurchaserem` 来自 `qty_purchase_rem` 汇总 |
| 增强字段 | `qty_preout`、`qty_prein`、`qty_freeze`、`qty_oms`、`qty_oms_translate`、`qty_preout1`、`source_dwd_row_count`、0 库存 / 负库存行数、水位字段、`load_batch_id`、`validation_status`、`validation_note` |
| 对账边界 | 与现有 `dws_inventory_daily` 对齐时必须保证同一 source snapshot timepoint；S4 推荐先读取旧表当日 `MAX(etl_time)`，再以 `--align-with-old-dws` 或显式 `--source-loaded-at-cutoff` 重载同一天 v2 切片；否则只能把差异记录为时间点差异，不当作转换错误 |

---

## 6. DWD → DWS v2 聚合口径草案

### 6.1 销售聚合

```sql
SELECT
    date_id,
    store_id,
    COALESCE(store_code, '') AS store_code,
    COALESCE(is_cloud_store, 'N') AS is_cloud_store,
    product_id,
    m_productalias_id,
    SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(qty, 0) ELSE 0 END) AS sales_qty,
    SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_actual_amt, 0) ELSE 0 END) AS sales_amount,
    SUM(CASE WHEN is_positive_sale_flag = 'Y' THEN COALESCE(line_list_amt, 0) ELSE 0 END) AS sales_amount_list,
    SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(qty, 0)) ELSE 0 END) AS return_qty,
    SUM(CASE WHEN is_return_flag = 'Y' THEN ABS(COALESCE(line_actual_amt, 0)) ELSE 0 END) AS return_amount,
    COUNT(DISTINCT CASE WHEN is_positive_sale_flag = 'Y' THEN retail_id END) AS order_count
FROM dwd_sales_retail_item
WHERE date_id BETWEEN :start_date AND :end_date
  AND dws_sales_scope_flag = 'Y'
GROUP BY date_id, store_id, COALESCE(store_code, ''), COALESCE(is_cloud_store, 'N'), product_id, m_productalias_id;
```

### 6.2 库存聚合

```sql
SELECT
    snapshot_date AS date_id,
    store_id,
    COALESCE(store_code, '') AS store_code,
    COALESCE(is_cloud_store, 'N') AS is_cloud_store,
    product_id,
    m_productalias_id,
    SUM(COALESCE(qty, 0)) AS qty,
    SUM(COALESCE(qty, 0)) AS qty_valid,
    0 AS qty_occupy,
    SUM(COALESCE(qty_purchase_rem, 0)) AS qtypurchaserem
FROM dwd_inventory_storage_snapshot
WHERE snapshot_date = :snapshot_date
  AND dws_inventory_scope_flag = 'Y'
GROUP BY snapshot_date, store_id, COALESCE(store_code, ''), COALESCE(is_cloud_store, 'N'), product_id, m_productalias_id;
```

---

## 7. 调度接入方案

### 7.1 阶段化接入路径

| 阶段 | 名称 | 动作 | 总控影响 | 退出条件 |
|------|------|------|----------|----------|
| S0 | 设计冻结 | 本文 + SQL 草案完成；不建表、不写库 | 无 | 用户确认 v2 表、字段、口径、回滚策略 |
| S1 | 用户人工建表 | 用户已人工执行两份 `draft_create_dws_*_v2.sql`；Copilot 仅只读核验表结构和空表行数 | 无 | 已确认表和索引存在、两表均为 0 行；S2 dry-run / conn-test 脚本已完成 |
| S2 | 旁路脚本 dry-run | 已新增 `etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py`，默认输出 SQL / conn-test；不传 `--execute` 时不写库 | 无 | 已完成 py_compile、单元测试、dry-run 输出和只读 conn-test；用户已授权进入 S3 写入分支实现 |
| S3 | 手工小窗口写入 | 已在用户明确授权下完成：销售跑 `20260428-20260430`、库存跑固定 `snapshot_date=20260507`，均通过确认令牌进入写入分支 | 无 | 已生成脚本运行证据 JSON；销售 DWD→v2 mismatch 0 且与旧 DWS 0 差异，库存 DWD→v2 mismatch 0，v2→旧 DWS 的 200 条同 key `qty` 差异当前按快照时点差异记录 |
| S4 | 独立计划任务 shadow run | 已新增 `scheduled_dws_v2_shadow.py` 与 `run_scheduled_dws_v2_shadow.bat`，不挂 `run_etl.py` 主链；仅写 `_v2` 表。销售步骤默认按 `ads_inventory_health` 所需的 `today-30 ~ today` 包含当天窗口重算，并在窗口大于主链 7 天时自动抬到 `long_running`；库存步骤先用主链 `ods_fa_storage` 做 old `dws_inventory_daily` 可比基线检查，再执行 raw / DWD 刷新与 `dws_inventory_daily_v2` 重算；`scheduled_total_control.py` 当前以 `dws_v2_shadow` 非阻断子链串行触发 shadow，支持 `--shadow-only` | 不影响旧主链；shadow 失败只记 WARNING | 已完成一轮 READY；用户已决定不再等待 3-7 天观察，改用两次总控 V2 gate |
| S5 | 总控 V2 双跑 gate | 用户手工执行两轮 `scheduled_total_control.py --cutover-mode v2` 或 `run_scheduled_total_control_v2.bat`；主链与专题链显式接收 V2 cutover，shadow 仍非阻断 | 有影响：`ads_inventory_health` 在 V2 模式改读 `_v2`；默认模式仍不变 | 两轮退出码、总控摘要、运行 JSON、耗时 / 锁证据和 ADS 字段兼容观察齐全 |
| S6 | 计划任务入口与下游逐步切换 | 双跑通过并由用户确认后，才讨论是否把 Windows 任务计划入口改为 V2 wrapper 或显式 V2 命令；其余 ADS 按依赖分类逐项确认 | 有影响，需变更窗口 | 用户确认、回滚命令齐全、ADS 字段未改删、主要差异有证据解释 |

### 7.2 建议任务顺序

1. DIM / ODS 现有主链继续照常运行。
2. M3 raw / DWD 旁路链路在 shadow 任务中先运行，确保 DWD 最新。
3. `dws_sales_daily_v2` 在 shadow 内默认按 `31` 天窗口重算，覆盖 `ads_inventory_health` 的 `today-30 ~ today` 包含当天销售输入；若只做主链对齐类最小验证，可显式回退到近 7 天窗口。
4. `dws_inventory_daily_v2` 按当天快照重算；shadow 调度内的旧链检查应先基于主链 `ods_fa_storage` 对旧 `dws_inventory_daily` 做可比基线核对，再执行 raw / DWD 刷新与 v2 写入，避免把刷新后的 `source_loaded_at` 误当作旧链时点。
5. `SQL/check_dws_v2_parallel_reconciliation.sql` 只读对账并写出报告：先做 DWD→v2 自洽；若需要追查旧链差异，再做 6B 的 `old DWS vs DWD aligned` 与 6C 的 `old DWS vs v2`，但这两步只在已构造出真正同一 source snapshot timepoint 时才可判责。
6. 仅当对账通过，才允许后续 ADS shadow 或生产切换讨论。

### 7.3 主链接入开关建议

后续若需要修改代码，建议采用显式环境变量或配置开关，默认关闭：

| 开关 | 默认 | 作用 |
|------|------|------|
| `ENABLE_DWS_V2_SHADOW` | `0` | 是否在主链中运行 v2 shadow step |
| `DWS_V2_BLOCK_ON_FAILURE` | `0` | v2 shadow 失败是否阻断下游；第一阶段必须为 0 |
| `DWS_V2_SALES_DAYS_BACK` | `31` | 销售 v2 回算窗口，默认覆盖 `ads_inventory_health` 的 `today-30 ~ today` 包含当天消费窗；若仅做主链 7 天对齐，可显式传参缩小 |
| `DWS_V2_INVENTORY_SNAPSHOT_MODE` | `today` | 库存 v2 快照模式；切换前需固定时间点规则 |

---

## 8. 回滚方案

### 8.1 分阶段回滚

| 阶段 | 回滚动作 | 是否影响旧主链 |
|------|----------|----------------|
| S0 / S1 | 不执行 v2 脚本；如用户已建表，可选择保留空表或人工 DROP / RENAME | 不影响 |
| S2 / S3 | S2 停止运行 dry-run / conn-test 即可；S3 手工写入分支若已由用户执行，保留 `_v2` 表和运行证据作为对账材料，或由用户人工清理 / 回滚候选数据。库存 aligned rerun 因为是“删当日切片 + 重灌”，若需回退应回到上一轮完整 shadow 结果而不是只补单条 key | 不影响 |
| S4 | 停用独立计划任务 / bat；旧 `run_etl.py` 不变 | 不影响 |
| S5 | 将 `ENABLE_DWS_V2_SHADOW=0` 或移除 shadow step；v2 失败不阻断旧 DWS / ADS | 不影响 |
| S6 | 下游读源切回旧 `dws_sales_daily` / `dws_inventory_daily`；必要时恢复旧配置和 ADS 重跑 | 影响下游，需变更窗口 |

### 8.2 禁止的回滚方式

1. 不直接 `TRUNCATE` 旧 `dws_sales_daily` 或 `dws_inventory_daily`。
2. 不把旧表 `RENAME` 为 v2 表来完成切换。
3. 不在没有同快照证据时用库存 DWS v2 覆盖旧 DWS。
4. 不让 v2 shadow 失败阻断现有 `ads_health`，直到用户明确确认切换窗口。

---

## 9. 超时、锁与事务风险评估

| 对象 | 数据量 / 历史证据 | 推荐 `timeout_profile` | 锁与事务建议 |
|------|------------------|------------------------|--------------|
| 销售 DWS v2 小窗口 | M3 20260428-20260430 DWD 5103 行；现有 ODS 主链近 7 天回刷 | `etl` | 命名锁 `hefang_dw:dws_sales_daily_v2:s3`；按日期窗口单事务 upsert，写后输出目标摘要、DWD-v2 差异数和样本，先验证小窗口耗时 |
| 销售 DWS v2 历史回填 | 历史可能覆盖百万级 DWD 明细 | `long_running` | 分日期批次；每批落对账报告；不与总控同窗口抢锁 |
| 库存 DWS v2 full snapshot | M3 `dwd_inventory_storage_snapshot` 20260507 为 201946 行 | `long_running` | 命名锁 `hefang_dw:dws_inventory_daily_v2:s3`；按 `snapshot_date` 单事务“DELETE 当日切片 + INSERT 重灌”，避免 aligned rerun 残留更晚快照 key，写后输出 DWD-v2 自洽证据与 old DWS cutoff 对齐结果 |
| 对账 SQL | 销售按窗口、库存按 snapshot_date | `etl` / `long_running` 视窗口 | 全只读；大窗口结果落 `reports/context_cache/`，聊天只摘要 |

S2 默认路径仍只完成 dry-run / conn-test，不持有命名锁、不打开写事务、不删除或插入 `dws_sales_daily_v2` / `dws_inventory_daily_v2`。S3 手工写入路径仅在用户运行 `--execute --confirm-write WRITE_DWS_SALES_V2` 或 `--execute --confirm-write WRITE_DWS_INVENTORY_V2` 时触发；脚本会先校验确认令牌和表结构，再获取命名锁、开启显式事务执行 `ON DUPLICATE KEY UPDATE`，失败时回滚并在 JSON 证据中记录清理状态。

---

## 10. 验收矩阵

| 验收层级 | 销售 | 库存 | 通过标准 |
|----------|------|------|----------|
| DWD→DWS v2 自洽 | v2 与 `dwd_sales_retail_item` 同 key 聚合差异为 0；金额容差 0.01 | v2 与 `dwd_inventory_storage_snapshot` 同 key 聚合差异为 0 | `SQL/check_dws_v2_parallel_reconciliation.sql` 差异段返回 0 行或有明确豁免 |
| DWS v2→旧 DWS 对齐 | 20260428-20260430 先复用 M3 验证窗口；再扩到近 7 天 | 必须同一 source snapshot timepoint；库存需先取旧表 `MAX(etl_time)`，再以同一 cutoff 重载 v2 切片；否则只记录差异原因 | 销售日级 / key 级对齐；库存差异不得无证解释 |
| 调度 / 总控 gate | 用户已决策由两次总控 V2 双跑替代 3-7 天连续 shadow 观察；shadow 仍非阻断 | 同左 | 两轮总控摘要或独立日志可追溯退出码、耗时、行数、对账状态和 ADS 字段兼容观察 |
| 下游 shadow | 先只读比对 ADS 输入，不写生产 ADS | `ads_inventory_health` 最后切，因为真实依赖 DWS | 用户确认后逐步扩大范围 |

---

## 11. 待确认问题

| 编号 | 问题 | 当前建议 |
|------|------|----------|
| M4-Q1 | 是否接受 `_v2` 表名 | 建议接受，避免破坏旧表和现有下游 |
| M4-Q2 | v2 是否填充 `net_qty` / `net_amount` | 建议 v2 填充候选净值，但下游切换前需确认是否允许从旧表默认 0 变为计算值 |
| M4-Q3 | 库存 v2 是否暴露 `qty_preout`、`qty_prein`、`qty_freeze` 等增强字段 | 建议先保留在 v2，不接旧 ADS；后续按业务场景逐项启用 |
| M4-Q4 | DWS v2 是否进入 `run_etl.py` 主链 | 第一阶段不进入；先独立 shadow run，连续稳定后再由用户确认 |
| M4-Q5 | 库存精确对齐旧 DWS 的快照时间点 | 必须先固定同一 source snapshot timepoint；当前 shadow 调度已把“旧链基线”改为主链 `ods_fa_storage` 可比检查，`--align-with-old-dws` / `--source-loaded-at-cutoff` 保留给显式构造同一 raw / DWD 时点的人工排查 |

---

## 12. 下一步执行建议

1. 两份 DWS v2 DDL 已由用户人工执行，当前结构核验结论见 `reports/context_cache/dws_v2_manual_ddl_verification_20260507.json`。
2. S3 实跑验收已完成：销售证据见 `reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json`，库存证据见 `reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json`。
3. S4 调度代码已完成：`scheduled_dws_v2_shadow.py`、`run_scheduled_dws_v2_shadow.bat` 已落地，`scheduled_total_control.py` 当前已把 `dws_v2_shadow` 作为非阻断子链接入，支持 `--shadow-only`。
4. 用户已决定跳过 3 到 7 天 shadow 观察，下一步改为手工执行两轮 `scheduled_total_control.py --cutover-mode v2` 或 `run_scheduled_total_control_v2.bat`。
5. 每轮保留退出码、总控摘要、运行 JSON、耗时 / 锁证据、`dws_v2_shadow` 非阻断表现和 ADS 字段兼容观察；若 shadow 失败但未阻断，也要记录失败阶段与后续处理，不把数据异常视为已完成切换。
6. 两轮通过后，再由用户决定是否进入 S6：修改 Windows 任务计划入口为 V2 wrapper 或显式 V2 命令；若失败或需保守回退，使用 `--rollback-to-legacy`，不删除、不改名 ADS 字段。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-05-12 | 同步用户决策：S5 改为两次总控 V2 双跑 gate，S4 不再要求 3-7 天连续 shadow 观察，并补记 wrapper 与 rollback 路径 |
| v0.9 | 2026-05-12 | 将 `scheduled_dws_v2_shadow.py` 的销售 shadow 默认窗口调整为覆盖 `ads_inventory_health` 的 31 天游标，并在超出主链 7 天时自动切到 `long_running` |
| v0.8 | 2026-05-08 | 调整库存 S4 旧链对齐口径：shadow 内不再把刷新后的 raw / DWD 直接与 old DWS 绑定比较，改为先用主链 `ods_fa_storage` 做可比基线检查，再单独保留 DWD→v2 自洽结果 |
| v0.7 | 2026-05-07 | 记录 S4 独立 shadow 调度已落地：新增 `scheduled_dws_v2_shadow.py` / `run_scheduled_dws_v2_shadow.bat`，并将 `dws_v2_shadow` 以非阻断子链接入 `scheduled_total_control.py` |
| v0.6 | 2026-05-07 | 固化库存 S4 对齐口径：`etl_dws_inventory_v2.py` 新增 `--source-loaded-at-cutoff` / `--align-with-old-dws`，写入改为同日切片删后重灌；`SQL/check_dws_v2_parallel_reconciliation.sql` 新增 old DWS 基线探针与 aligned 对账步骤 |
| v0.5 | 2026-05-07 | 记录 DWS v2 已在用户明确授权下完成 S3 实跑验收：销售写入 3417 行、库存写入 75104 行，DWD-v2 mismatch 均为 0；销售与旧 DWS 0 差异，库存对旧 DWS 的 200 条同 key `qty` 差异当前按快照时点不同记录，仍未接总控 |
| v0.4 | 2026-05-07 | 用户授权进入 S3 后，补记 DWS v2 受控手工写入分支：默认仍 dry-run，`--execute` 需确认令牌、命名锁、显式事务、失败回滚和写后 DWD-v2 对账；本轮未执行真实写入、未接总控 |
| v0.3 | 2026-05-07 | 新增 `etl_dws_sales_v2.py`、`etl_dws_inventory_v2.py` dry-run / conn-test 脚本；当前无写库入口，已完成 dry-run、conn-test 与单元测试 |
| v0.2 | 2026-05-07 | 记录用户已人工执行两份 DWS v2 DDL，Copilot 已完成空表与唯一键只读核验；仍未写 v2 数据、未新增 ETL、未改调度 |
| v0.1 | 2026-05-07 | 新增 DWS v2 并行表、调度接入、对账验收与回滚方案；落 DDL 草案和只读对账 SQL，未执行 DDL / 写库 / 调度修改 |
