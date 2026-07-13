# M2 第一批 DWD 主题设计冻结草案

---

## 1. 文档状态与边界

| 项 | 说明 |
|----|------|
| 文档状态 | 草案已输出；2026-04-29 用户已确认两条长期设计决策；2026-04-30 M3 已输出 DDL / ETL 旁路产物且用户已人工建 DWD 空表 |
| 设计对象 | `dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` |
| 实现状态 | 已由用户人工建 DWD 表；已按授权完成旁路小窗口装载验证；未接入调度；未成为生产数据契约 |
| 本轮范围 | 只冻结候选粒度、主键、来源字段、过滤标识、增量策略、验证方式和风险，不输出最终 DDL |
| 明确不做 | 不修改 `run_etl.py`、`scheduled_etl.py`、`scheduled_total_control.py`；不执行数据库写操作；不把 DWD 写成当前生产来源 |

本文件用于 M2 人工复核。以下表名、字段名、刷新方式和切换策略在 M2 阶段为候选设计；截至 2026-04-30，两个 DWD 表已由用户人工建为空表，但不代表已装载、已接调度或已成为当前生产来源。

2026-04-29 用户已确认以下长期设计方向：

1. 销售 DWD 不应只保留核算事实，应沉淀“零售明细原子事实 + 可复现业务销售底表的关键业务上下文”；但核心 DWD 不一比一照搬 ERP 导出 Excel 模板，展示字段可通过维表或业务宽表视图承接。
2. 库存 DWD 第一阶段不应只覆盖当前库存健康链路，应保留全店仓库存快照事实；库存健康只是第一批验证和消费场景。

---

## 2. 当前事实基线

| 事实 | 证据 |
|------|------|
| 主链 `STEP_ORDER` 当前包含 DIM、ODS、DWS、ADS，不包含 DWD | `run_etl.py#L52-L61` |
| 主链 ODS 增量默认回刷 7 天，DWS 销售主链回带天数与 ODS 回刷窗口一致 | `run_etl.py#L64-L65`、`run_etl.py#L633-L652` |
| 当前销售 DWS 从 `ods_m_retailitem`、`ods_m_retail` 和 `dim_store` 聚合，并过滤有效单据、已完成状态、日期窗口、SKU 非空 | `etl_dws_sales.py#L34-L64` |
| 当前销售 DWS 写入策略是按日期范围删除 `dws_sales_daily` 后追加写入 | `etl_dws_sales.py#L147-L162` |
| 当前库存 DWS 从 `ods_fa_storage` 和 `dim_store` 抽取，并过滤有效库存、SKU 非空、总仓或云仓 | `etl_dws_inventory.py#L35-L58` |
| 当前库存 DWS 会把同一 `store_id + product_id + m_productalias_id` 的多行合并 | `etl_dws_inventory.py#L102-L127` |
| 库存健康 ADS 以 `dws_inventory_daily` 为库存主表，并从 `dws_sales_daily` 取近 30 / 7 天销售 | `etl_ads_health.py#L437-L463` |
| 门店销售专题调度 freshness 当前仍以 `dws_sales_daily.etl_time` 晚于专题 ADS 为重跑触发依据 | `scheduled_store_daily_report.py#L394-L505` |
| MySQL 连接工厂默认 `timeout_profile='default'`，同时提供 `etl` 与 `long_running` 档位 | `db_connections.py#L29-L63`、`db_connections.py#L85-L88` |
| ODS 零售头 / 明细唯一键治理仍是 P1 人工项，未完成前不能把现网历史库唯一约束视为已落实 | `docs/TODO_ISSUES.md#L21-L30`、`SQL/alter_ods_m_retail_enforce_unique_id.sql#L1-L40`、`SQL/alter_ods_m_retailitem_enforce_unique_id.sql#L1-L39` |

---

## 3. M2 冻结结论摘要

| 主题 | 候选对象 | M2 建议结论 | 用户复核重点 |
|------|----------|--------------|--------------|
| 销售明细事实 | `dwd_sales_retail_item` | 以源零售明细行为事实粒度，保留单头状态、明细金额、SKU、门店、双水位、下游过滤标识，并承接会员、营业员、购物券、商品归因等关键业务上下文 | 已确认长期方向：DWD 不只保留核算事实，但不一比一复制 ERP 销售底表展示模板 |
| 库存快照事实 | `dwd_inventory_storage_snapshot` | 以快照日期 + 源库存行为候选粒度，保留全店仓库存事实、SKU 标识、总仓 / 云仓标识和 DWS 过滤标识；DWS 再汇总总仓 / 云仓范围 | 已确认长期方向：DWD 不只覆盖当前库存健康链路，库存健康只是第一批验证与消费范围 |
| 销售事实 freshness | 候选 `sales_fact_freshness` / 等效元数据 | M2 只冻结原则：短期不改调度，仍兼容 `dws_sales_daily.etl_time`；DWD 旁路验证后再设计专用事实水位 | 是否需要单独落表，还是先用 DWD 按 `date_id` 的 `MAX(etl_time)` 视图替代 |

---

## 4. 候选一：`dwd_sales_retail_item`

### 4.1 设计目的

把销售明细中目前散落在 ODS、DWS、ADS 的清洗与过滤依据前移到统一明细事实层，但不在 DWD 层直接丢弃 DWS 当前不需要的事实范围。该层不应只服务财务或核算汇总，还应承接销售部日常业务底表中反复用于指标归因的关键上下文。现有 `dws_sales_daily` 仍保持生产主链不动；未来可新增并行 DWS v2 从本 DWD 聚合后与现表对账。

### 4.2 候选粒度与主键

| 项 | 候选设计 |
|----|----------|
| 业务粒度 | 一行代表一条 Oracle `M_RETAILITEM` 零售明细，并带上对应 `M_RETAIL` 单头上下文 |
| 候选主键 | `retail_item_id`，映射 `ods_m_retailitem.id` |
| 关联键 | `retail_id`，映射 `ods_m_retailitem.m_retail_id` / `ods_m_retail.id` |
| 日期分区候选 | `date_id`，映射 `ods_m_retail.billdate` |
| 幂等约束前提 | 现网历史 ODS 明细 `id` 唯一键治理完成后，才可把 `retail_item_id` 视为稳定唯一键 |

说明：若复核发现源明细 `id` 在历史库存在重复且短期无法治理，则 M3 需改为临时去重视图 / staging 表方案，不能直接依赖 `retail_item_id` 作为唯一键。

### 4.3 候选来源字段

| 字段组 | 候选字段 | 来源 |
|--------|----------|------|
| 源主键 | `retail_item_id`、`retail_id` | `ods_m_retailitem.id`、`ods_m_retailitem.m_retail_id` |
| 单头上下文 | `docno`、`date_id`、`store_id`、`oms_sourcecode` | `ods_m_retail.docno`、`billdate`、`c_store_id`、`oms_sourcecode` |
| 商品与 SKU | `product_id`、`m_productalias_id` | `ods_m_retailitem.m_product_id`、`m_productalias_id` |
| 数量与金额 | `qty`、`price_list`、`price_actual`、`line_actual_amt`、`line_list_amt` | `ods_m_retailitem.qty`、`pricelist`、`priceactual`、`tot_amt_actual`、`tot_amt_list` |
| 单头金额 | `retail_actual_amt`、`retail_list_amt`、`retail_total_qty` | `ods_m_retail.tot_amt_actual`、`tot_amt_list`、`tot_qty` |
| 状态字段 | `retail_status`、`retail_isactive` | `ods_m_retail.status`、`ods_m_retail.isactive` |
| 水位字段 | `retail_modifieddate`、`item_modifieddate`、`item_settime` | `ods_m_retail.modifieddate`、`ods_m_retailitem.modifieddate`、`ods_m_retailitem.settime` |
| 审计字段 | `etl_time`、`source_loaded_at`、`source_batch_id` | DWD 装载时间、ODS `etl_loaded_at`、ODS `etl_batch_id` |

2026-04-29 参照销售部 ERP 固定导出底表后，用户确认 DWD 长期最优解不应只保留核算事实。M3 字段设计需补充以下“关键业务上下文”审计，但字段能否落在 DWD 主表、维表或业务宽表视图中，仍以源表字段可得性和稳定性为准：

| 上下文类型 | 业务用途 | M3 设计边界 |
|------------|----------|-------------|
| 会员上下文 | 支撑会员销售、VIP 类型、开卡时间等销售分析 | 优先保留稳定会员 key；展示名和类型可由维表或宽表视图补齐 |
| 人员上下文 | 支撑营业员归因、门店人员绩效与人工核对 | 优先确认源侧人员 ID / 名称是否稳定；避免只依赖 Excel 展示文本 |
| 营销上下文 | 支撑购物券、活动、优惠类分析 | 先保留可追溯券或活动标识；复杂券规则不在 DWD 层直接计算 |
| 商品归因上下文 | 支撑货号、条码、尺寸、系列、类别、材质、上市日等销售底表分析 | 稳定编码进入事实或维表；展示属性优先由 DIM / 宽表视图承接 |

来源证据：销售业务底表 `data/4月截止28日原始数据.xlsx` 包含单据、店仓、商品、条码、VIP、营业员、购物券、数量与金额等字段；该文件仅作为业务使用场景证据，不代表这些字段已全部在 ODS 中完成血缘确认。

来源字段以 ODS 建表与抽取脚本为依据：`SQL/create_ods_tables.sql#L16-L49`、`etl_ods_m_retail.py#L107-L127`、`etl_ods_m_retailitem.py#L119-L139`。

### 4.4 候选过滤标识

DWD 不直接继承 DWS 的汇总过滤为唯一事实范围，建议只沉淀标识，过滤留给 DWS / ADS：

| 标识 | 候选逻辑 | 目的 |
|------|----------|------|
| `has_retail_header_flag` | 明细能命中 `ods_m_retail.id` | 暴露孤儿明细质量问题 |
| `is_valid_retail_flag` | `retail_isactive='Y'` 且 `retail_status=2` | 对齐现有销售 DWS 的有效单据范围 |
| `has_sku_flag` | `m_productalias_id IS NOT NULL` | 暴露现有 DWS 被过滤掉的无 SKU 明细 |
| `is_positive_sale_flag` | `retail_actual_amt > 0`，或 `retail_actual_amt = 0` 且 `qty > 0` | 对齐现有 DWS 正向销售数量 / 金额判断 |
| `is_return_flag` | `retail_actual_amt < 0`，或 `retail_actual_amt = 0` 且 `qty < 0` | 对齐现有 DWS 退货数量 / 金额判断 |
| `dws_sales_scope_flag` | 有效单据 + SKU 非空 + `date_id` 非空 | 未来 DWS v2 聚合过滤入口 |

以上标识复用现有 DWS 事实：`etl_dws_sales.py#L44-L64`。标识名与最终字段名仍待用户确认。

### 4.5 候选增量与幂等策略

| 项 | 候选策略 |
|----|----------|
| 影响日期识别 | 从 `ods_m_retail.modifieddate`、`ods_m_retailitem.modifieddate`、`ods_m_retailitem.settime` 三类变化中收集受影响 `date_id`；默认与 ODS 7 天回刷窗口保持一致 |
| 装载方式 | 旁路 DWD 先按受影响日期窗口重算，再按 `retail_item_id` 幂等 upsert；若后续选择按日期删除重写，必须同时处理跨日期变更导致的旧日期残留 |
| 重跑能力 | 支持 `--start-date / --end-date` 小窗口重算；历史回填只输出命令与 SQL，由用户人工执行 |
| 锁策略 | 使用独立命名锁，例如 `hefang_dw:dwd_sales_retail_item`，不能与现有 DWS 锁混用 |
| 超时档位 | 小窗口默认建议 `timeout_profile='etl'`；历史大窗口 / 全量回填建议 `long_running`，需保留耗时证据后再确认 |

设计理由：当前主链 ODS 默认回刷 7 天，销售 DWS 主链也回带 7 天；若 DWD 后续介入，也必须逐层对齐晚到数据窗口。证据：`run_etl.py#L64-L65`、`run_etl.py#L633-L652`。

### 4.6 已确认的长期设计决策

销售 DWD 的长期定位为“零售明细原子事实 + 关键业务上下文”，而不是只保留核算事实的窄表。核心 DWD 主表应优先稳定事实主键、交易金额数量、状态标识、水位和可追溯业务 key；会员、营业员、购物券、商品展示属性等字段应在 M3 做源字段血缘审计后，按稳定性分别进入 DWD 主表、DIM 维表或 DWD / DWS 业务宽表视图。

该决策不改变当前生产链路，也不表示 DWD 已实现。进入 M3 后，仍需逐字段核对 ODS / DIM 是否存在对应来源，不能直接把 Excel 展示列写成目标表字段。

---

## 5. 候选二：`dwd_inventory_storage_snapshot`

### 5.1 设计目的

把库存当前快照先沉淀为可追溯的 DWD 快照事实，保留源库存行、店仓范围、SKU 状态和采购欠数。该层不应只服务当前库存健康 ADS，而应作为未来全店仓库存分析的底层事实。现有 `dws_inventory_daily` 仍保持生产主链不动；未来 DWS v2 再从 DWD 按总仓 / 云仓规则聚合。

### 5.2 候选粒度与主键

| 项 | 候选设计 |
|----|----------|
| 业务粒度 | 一行代表某个快照日的一条源库存记录，优先保留 `ods_fa_storage.id` 级别事实 |
| 候选主键 | `snapshot_date + storage_id`，其中 `storage_id` 映射 `ods_fa_storage.id` |
| 日期分区候选 | `snapshot_date`，取 DWD 装载日 / 指定快照日 |
| 聚合备用键 | `snapshot_date + store_id + product_id + m_productalias_id`，仅用于 DWS 汇总和重复分析，不建议作为 DWD 唯一事实键 |
| 历史限制 | 若 Oracle / ODS 只提供当前库存状态，则 DWD 只能从启用日起沉淀每日快照，不能凭当前 ODS 反推历史每日库存 |

### 5.3 候选来源字段

| 字段组 | 候选字段 | 来源 |
|--------|----------|------|
| 源主键 | `storage_id` | `ods_fa_storage.id` |
| 快照日期 | `snapshot_date` | DWD 装载参数或运行日期 |
| 店仓与商品 | `store_id`、`product_id`、`m_productalias_id` | `ods_fa_storage.c_store_id`、`m_product_id`、`m_productalias_id` |
| 库存数量 | `qty`、`qtypurchaserem`；`qty_valid` 仅作为对现有 DWS 的等价对账口径 | `ods_fa_storage.qty`、`qtypurchaserem`；2026-04-30 全量非零值补证确认源侧 `QTYVALID` 全量为 0，新 DWD 不再保留该物理字段 |
| 状态字段 | `isactive` | `ods_fa_storage.isactive` |
| 店仓标识 | `store_code`、`is_cloud_store` | `dim_store.store_code`、`dim_store.is_cloud_store`，作为候选冗余字段 |
| 审计字段 | `etl_time`、`source_loaded_at`、`source_batch_id` | DWD 装载时间、ODS `etl_loaded_at`、ODS `etl_batch_id` |

来源字段以 ODS DDL 和现有 DWS 抽取为依据：`SQL/create_ods_tables.sql#L3-L13`、`etl_dws_inventory.py#L35-L58`。

### 5.4 候选过滤标识

| 标识 | 候选逻辑 | 目的 |
|------|----------|------|
| `is_active_storage_flag` | `isactive='Y'` | 对齐当前 DWS 的有效库存范围 |
| `has_sku_flag` | `m_productalias_id IS NOT NULL` | 暴露无 SKU 库存记录 |
| `is_total_warehouse_flag` | `store_code='001'` | 标识总仓范围 |
| `is_cloud_store_flag` | `is_cloud_store='Y'` | 标识云仓范围 |
| `dws_inventory_scope_flag` | 有效库存 + SKU 非空 + 总仓或云仓 | 未来 DWS v2 聚合过滤入口 |
| `zero_qty_kept_flag` | `qty=0` 时仍保留 | 防止误把 0 库存从事实层删除 |

当前库存 DWS 已明确不应过滤 `qty=0`，且会按店仓商品 SKU 合并重复组合。证据：`etl_dws_inventory.py#L35-L58`、`etl_dws_inventory.py#L102-L127`。

参照业务库存底表 `data/4月28日库存底表.xlsx` 后，库存 DWD 的长期范围应保留全店仓快照事实，而不是只保留当前库存健康链路需要的总仓 / 云仓范围。该业务底表包含大量店仓、SKU、0 库存以及在单、在途、采购未回货等库存信号；这些字段证明业务库存分析需求宽于当前 `ads_inventory_health`，但不代表当前 `ods_fa_storage` 已具备全部字段来源。2026-04-30 补证确认 `FA_STORAGE.QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 全量为 0，新架构不再为了追溯完整性保留这些模板字段。

### 5.5 候选增量与幂等策略

| 项 | 候选策略 |
|----|----------|
| 日常装载 | 按 `snapshot_date` 删除重写 DWD 当日快照，再写入当日 ODS 当前库存状态 |
| 历史回填 | 仅在存在历史库存快照来源时设计；若只有当前 `ods_fa_storage`，不声称可回填历史每日库存 |
| 重跑能力 | 支持指定 `--snapshot-date` 旁路重算；默认不接入总控 |
| 锁策略 | 使用独立命名锁，例如 `hefang_dw:dwd_inventory_storage_snapshot` |
| 超时档位 | 当日快照建议从 `timeout_profile='etl'` 起步；全量历史快照或大范围重建建议 `long_running`，必须保留耗时证据 |

### 5.6 已确认的长期设计决策

库存 DWD 第一阶段的装载范围不应裁剪为当前库存健康链路范围。长期最优解是保留全店仓、全 SKU、0 库存、负库存和源侧可得库存信号，再通过 `dws_inventory_scope_flag` 标识当前 DWS / ADS 是否消费。

第一阶段验证可以优先对齐现有 `dws_inventory_daily` 与 `ads_inventory_health`，但这只是第一批消费场景，不是 DWD 的事实边界。业务库存底表中的在单、在途、标准金额等字段，需要在 M3 / M4 补做源字段审计；若当前 ODS 不具备对应字段，不能在第一阶段承诺完整替代业务库存底表。对于已确认全量为 0 的 Oracle 模板字段，M3 raw / DWD 草案不再纳入。

---

## 6. DWD → DWS → ADS 切换边界

| 阶段 | 允许动作 | 禁止动作 | 验收证据 |
|------|----------|----------|----------|
| M2 设计复核 | 用户确认候选表名、粒度、字段、标识、水位、验证矩阵 | 不写 DDL，不写 ETL，不接总控 | 本文件经用户确认 |
| M3 旁路小窗口验证 | 新增独立 DWD 脚本与 `--conn-test` / dry-run；显式 `--execute` 做旁路 upsert；不改现有主链 | 不替换 `dws_sales_daily` / `dws_inventory_daily` | 脚本空跑、单日小窗口日志、超时档位说明、最小对账证据 |
| M4 DWS v2 方案 | 设计并行 DWS v2 从 DWD 聚合，与现有 DWS 对账 | 不原地破坏生产 DWS | 近 1 天 / 7 天金额、数量、行数对账 |
| M5 小窗口验证 | 用户人工执行 DDL / 小窗口写入后对账 | 不在未验证超时边界前接入总控 | 行数、金额、库存数量、ADS 关键指标对账 |
| M6 主链接入 | 用户明确授权后再考虑 `run_etl.py` / 总控接入 | 不绕过回滚方案 | 用户确认、回滚方案、文档同步完成 |

短期结论：M2 结束后仍不修改当前调度，销售专题 freshness 仍兼容 `dws_sales_daily.etl_time`。只有当 DWD + DWS v2 验证稳定后，才讨论 freshness 来源切换。

---

## 7. 验证矩阵草案

### 7.1 销售 DWD 验证

| 验证项 | 草案口径 | 通过标准 |
|--------|----------|----------|
| 唯一性 | `retail_item_id` 重复检查 | 重复数为 0，或重复治理方案已被用户确认 |
| 明细覆盖 | DWD 行数 vs ODS 明细行数，按 `date_id` 和 `has_retail_header_flag` 分层统计 | 不因 DWS 过滤而提前丢明细；差异有质量标签解释 |
| DWS 对账 | `dwd_sales_retail_item` 中 `dws_sales_scope_flag='Y'` 的行按现有 DWS 粒度聚合，对比 `dws_sales_daily` | 近 1 天 / 7 天销售数量、销售金额、退货数量、退货金额、正向单数一致或差异有解释 |
| 晚到数据 | ODS 7 天回刷窗口内改动后，DWD 与 DWS v2 逐层覆盖同一业务日期 | 不出现 ODS 已补齐但 DWD / DWS 未刷新的日期 |
| ADS 影响 | 库存健康或销售专题关键金额 / 数量在切换前后对账 | 切换前不影响现有 ADS；切换后差异需逐项解释 |

### 7.2 库存 DWD 验证

| 验证项 | 草案口径 | 通过标准 |
|--------|----------|----------|
| 唯一性 | `snapshot_date + storage_id` 重复检查 | 重复数为 0；若源 `storage_id` 不稳定，必须改键或保留重复分析 |
| 0 库存保留 | `qty=0` 记录在 DWD 中可查询 | 不因 DWD 装载丢弃 0 库存事实 |
| DWS 对账 | DWD 中 `dws_inventory_scope_flag='Y'` 行按当前 DWS 粒度聚合，对比 `dws_inventory_daily` | 当日 `qty`、由 DWD `qty` 生成的 `qty_valid` 等价值、`qtypurchaserem` 汇总一致或差异有解释 |
| 店仓范围 | 总仓 `001` 与云仓标识统计 | DWD 保留全店仓事实，DWS 仅消费总仓 / 云仓范围 |
| ADS 影响 | `ads_inventory_health` 库存主表相关 SKU 数、总库存、采购欠数 | 切换前不影响现有 ADS；切换后差异需逐项解释 |

---

## 8. 超时与锁风险评估

| 对象 | 数据量 / 事务范围 | 候选 `timeout_profile` | 风险 | M3 前置验证 |
|------|------------------|--------------------------|------|--------------|
| `dwd_sales_retail_item` 单日 / 7 天窗口 | 读取 ODS 头表 + 明细表 + 可选维表，写入明细级事实 | `etl` | 细粒度行数可能远高于 DWS 聚合行；若按日期删除重写，锁持有时间可能较长 | 单日 dry-run 行数、单日写入耗时、7 天读写耗时、锁等待日志 |
| `dwd_sales_retail_item` 历史回填 | 大范围明细级重建 | `long_running` | 大事务、长锁、索引维护成本高 | 只能由用户人工小批次执行；先按月 / 周分片压测 |
| `dwd_inventory_storage_snapshot` 当日快照 | 全量当前库存快照写入 | `etl` | 库存源全量快照可能大于当前 DWS 过滤后结果 | 当日快照行数、写入耗时、重复组合统计 |
| `dwd_inventory_storage_snapshot` 历史重建 | 多日快照堆叠 | `long_running` | 若无历史源快照，不允许声称可回填；若有历史源，表规模线性增长 | 先确认历史源，再按单日 / 7 日压测 |

当前现有 DWS 仍调用 `create_mysql_engine()` 默认档，M3 若新增 DWD 脚本，必须显式传入 `timeout_profile`，不能继续隐式依赖 `default`。证据：`etl_dws_sales.py#L68`、`etl_dws_sales.py#L133`、`etl_dws_inventory.py#L63`、`etl_dws_inventory.py#L144`、`db_connections.py#L85-L88`。

---

## 9. 用户人工复核清单

| 编号 | 复核项 | 当前建议 | 需用户确认 |
|------|--------|----------|------------|
| R1 | 第一批 DWD 表名 | `dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` | 长期方向已认可；最终 DDL 命名仍在 M3 草案中确认 |
| R2 | 销售 DWD 粒度 | 一行一条 `M_RETAILITEM` 明细，保留单头上下文与关键业务上下文 | 已确认：销售 DWD 不只保留核算事实 |
| R3 | 销售 DWD 过滤策略 | DWD 保留事实 + 标识，DWS 再过滤有效单据 / SKU 非空 | 已确认：DWD 不提前丢弃无 SKU / 非有效事实，过滤在 DWS / ADS 显式表达 |
| R4 | 库存 DWD 粒度 | 快照日期 + 源库存行 | 长期方向已认可；M3 需补源 `storage_id` 稳定性与重复组合验证 |
| R5 | 库存范围 | DWD 保留全店仓有效库存，DWS 再过滤总仓 / 云仓 | 已确认：库存 DWD 不只覆盖当前库存健康链路 |
| R6 | freshness | 短期兼容 `dws_sales_daily.etl_time`，后续设计 DWD 事实水位 | 原则认可；是否单独落元数据表留到 M3 方案比较 |
| R7 | ODS 唯一键治理 | DWD 正式依赖源 `id` 幂等前，先由用户人工治理现网重复 | 保留为前置风险项，仍不由 Agent 执行写库治理 |
| R8 | M3 进入条件 | 用户确认本草案后，才进入 DDL 草案与旁路 ETL 骨架 | 本轮仅写回确认决策；是否正式启动 M3 仍待下一轮明确授权 |

---

## 10. M2 后续建议

1. 用户先按第 9 节复核粒度、过滤边界和 freshness 策略。
2. 若用户确认，M3 再输出 DDL 草案与旁路 ETL 骨架；DDL 仍只由用户人工执行。
3. M3 脚本必须包含 `--conn-test` / dry-run、小窗口参数、显式 `timeout_profile`、独立命名锁和最小对账输出。
4. 在 DWD 真实装载并完成小窗口验证前，不改 `run_etl.py` 的 `STEP_ORDER`，不影响每日总控。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.4 | 2026-04-30 | 补记两个 DWD 表已由用户人工建为空表，仍未装载、未接调度 |
| v0.3 | 2026-04-30 | 补充 `FA_STORAGE.QTYVALID` 等全零模板字段不进入新 DWD 物理字段，库存对账使用 `qty` 生成 `qty_valid` 等价值 |
| v0.2 | 2026-04-29 | 写回用户确认的两条长期设计决策：销售 DWD 承接关键业务上下文，库存 DWD 保留全店仓快照事实 |
| v0.1 | 2026-04-29 | 新增 M2 第一批 DWD 主题设计冻结草案，覆盖销售明细与库存快照候选对象 |