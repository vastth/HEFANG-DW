# ODS 打通自动化链路计划与续接入口

> 状态：第一阶段核心链路已打通，17:05 主链实跑验证通过
> 创建日期：2026-03-23
> 当前版本：v1.5
> 用途：本文件是“将 ODS 打通到现有自动化主链”主题的跨对话上下文主文件；当前窗口上下文接近上限或切换到新窗口时，优先提供本文件即可快速恢复目标、现状、决策、进度与下一步执行入口。
> 说明：本文件只记录当前已确认事实、实施计划、待确认项与续接提示，不替代正式交接日志；每完成一个阶段后，应同步更新本文件与 docs/AGENT_HANDOFF.md。

## 目标

将当前独立执行的 ODS 链路接入现有自动化主链，形成至少可稳定运行的最小闭环：

- ODS 纳入自动化调度
- DWS 改为消费 ODS，而不再直接读取 Oracle 业务表
- ADS 继续消费 DWS 与 DIM
- 文档最终与新链路保持一致

当前优先目标不是一次性重构整个数仓，而是先完成“ODS -> DWS -> ADS 自动化打通”的第一阶段闭环。DIM 是否也要改为经 ODS 中转，当前暂不作为第一阶段强制目标。

## 当前阶段快照

- 已确认 ODS 入口已接入 run_etl.py 主链，当前既可由主链自动触发，也保留 run_ods.py 独立执行入口。
- 已确认 scheduled_etl.py 继续只调用 run_etl.py，因此自动化层已经间接覆盖 ODS。
- 已确认 dws_sales_daily 与 dws_inventory_daily 均已切换为消费 ODS。
- 已确认 ads_inventory_health 当前消费的是 DWS 与 DIM，因此主链内部 DIM -> DWS -> ADS 是连通的。
- 已完成一次只扫描的文档对齐审计，并刷新 reports/docs_code_alignment.json。
- 已确认 docs/ARCHITECTURE.md 与 docs/DATA_CONTRACTS.md 已出现“目标态先写进文档，但代码尚未实现”的漂移。
- 当前已完成三步代码改动：主链已纳入 ODS 同步与质检步骤，dws_sales 与 dws_inventory 均已切换为消费 ODS。
- 已确认 2026-03-23 17:05 触发的 `run_etl.py` 9 步主链完整成功，结果为成功 8 / 警告 1 / 失败 0；唯一警告为 `dabo_ready` 当日无记录。

## 当前推进进度

- 已完成：链路事实核对、主链与 ODS 入口审计、文档只扫描审计、实施顺序决策。
- 已完成：第一阶段文件级改造顺序、调度接入方案倾向、ODS 字段缺口核对。
- 当前阶段：第一阶段核心链路已打通，已完成主链接入 ODS、dws_sales ODS 化、库存链字段补齐与 dws_inventory ODS 化。
- 已完成：重构后的真实链路验证。
- 尚待增强：必要的快照/对账验证，以及 DIM 是否需要后续 ODS 化评估。

## 已确认事实

### 1. 入口与调度

- 主自动化链步骤当前已包含 `ods_sync`，顺序为 `dim_product / dim_sku / dim_store / dim_channel / ods_sync / dws_sales / dws_inventory / dabo_ready / ads_health`。来源：[run_etl.py](../../run_etl.py#L47-L56)
- 定时包装脚本当前仍只调用 `run_etl.py`，因此自动化入口未增加新的独立调度点。来源：[scheduled_etl.py](../../scheduled_etl.py#L40-L49)
- ODS 独立入口仍执行 `etl_ods_fa_storage / etl_ods_m_retail / etl_ods_m_retailitem`，并可选触发 ODS 质量校验；主链内部当前通过复用 `run_ods.run(...)` 调用这套逻辑。来源：[run_ods.py](../../run_ods.py#L81-L102)；[run_etl.py](../../run_etl.py#L381-L391)

### 2. 当前数据依赖关系

- `etl_dws_sales.py` 当前从 MySQL `ods_m_retail / ods_m_retailitem / dim_store` 聚合销售数据。来源：[etl_dws_sales.py](../../etl_dws_sales.py#L17-L48)
- `etl_dws_inventory.py` 当前从 MySQL `ods_fa_storage / dim_store` 提取库存数据。来源：[etl_dws_inventory.py](../../etl_dws_inventory.py#L17-L38)
- `etl_ads_health.py` 当前消费 `dws_inventory_daily / dws_sales_daily / dim_* / ads_dabo_daily_sales`。来源：[etl_ads_health.py](../../etl_ads_health.py#L230-L275)

### 3. 当前文档状态

- `docs/ARCHITECTURE.md`、`docs/DATA_CONTRACTS.md` 与运行说明文档中的 dws_sales / dws_inventory 描述已回到与代码一致的状态；后续主要剩真实验证与快照证据补强。来源：[docs/ARCHITECTURE.md](../ARCHITECTURE.md)；[docs/DATA_CONTRACTS.md](../DATA_CONTRACTS.md)
- `docs/ETL业务逻辑说明.md` 与 `docs/数据仓库与ETL手册.md` 已同步到“主链含 ODS，dws_sales / dws_inventory 已消费 ODS”的现状。来源：[docs/ETL业务逻辑说明.md](../ETL业务逻辑说明.md)；[docs/数据仓库与ETL手册.md](../数据仓库与ETL手册.md)

## 当前关键决策

### 决策 0：第一阶段以主链真实跑通作为完成标志

当前已确认：

- `run_etl.py` 于 2026-03-23 17:05 启动的 9 步主链已完整执行结束。
- `ods_sync`、`dws_sales`、`dws_inventory`、`ads_health` 均为 `SUCCESS`。
- `dabo_ready` 为 `WARNING`，原因是当日无达播记录，这不再阻断主链完成。
- 库存与 ADS 在本轮修复后未再出现“先删当天数据，再因死锁失败”的中间态。

因此，第一阶段当前可以定义为：代码改造完成，主链真实验证通过，并发重跑导致的 `1213/1205` 风险已完成第一轮修复。

### 决策 1：先重构 ETL，再同步核心文档

当前已决定：

- 不先把 `docs/ARCHITECTURE.md` 与 `docs/DATA_CONTRACTS.md` 回写成旧现状
- 先进入 ETL 重构设计与落地
- 待第一阶段代码闭环成形并完成最小验证后，再在同一轮同步更新文档

原因：

- 这两份文档目前已部分写成目标态
- 若先回退文档，再马上做链路打通，会产生双重返工
- 当前高风险点是链路行为，不是文案本身

### 决策 2：第一阶段优先做 ODS -> DWS -> ADS

当前建议的第一阶段最小闭环：

- ODS 接入自动化调度
- `etl_dws_sales.py` 改为从 `ods_m_retail / ods_m_retailitem` 聚合
- `etl_dws_inventory.py` 评估从 `ods_fa_storage` 聚合的最小改法
- ADS 尽量不改业务口径，只复用新的 DWS 输出

DIM 当前暂不强制改为依赖 ODS，因为现有 ODS 并未覆盖商品、SKU、店仓、渠道主档。

### 决策 3：按已实现层级逐步同步文档

当前已落地：

- `run_etl.py` 已纳入 `ods_sync` 步骤
- `run_ods.py` 仍保留为独立手动入口

- `etl_dws_sales.py` 已改读 ODS
- `etl_dws_inventory.py` 已改读 ODS
- `etl_ods_fa_storage.py` 已补齐 `qtypurchaserem`

因此，当前文档可以写为“主链已接入 ODS，dws_sales 与 dws_inventory 已消费 ODS”；但 DIM 仍未 ODS 化，不能写成“全链路所有层都已改由 ODS 驱动”。

## 第一阶段实施计划

### Phase 0：重构前冻结

目标：锁定范围，避免把问题做大。

1. 明确本轮第一阶段目标仅为 `ODS -> DWS -> ADS` 自动化打通。
2. 明确 DIM 暂不纳入 ODS 化改造。
3. 明确 `ads_inventory_health` 的业务口径不在本轮主动改动范围内。

### Phase 1：调度链路接入 ODS

目标：让 ODS 正式进入自动化链，而不再完全独立运行。

候选改法：

1. 在 `run_etl.py` 中新增 ODS 步骤，由主链统一调度。
2. `scheduled_etl.py` 继续只调 `run_etl.py`，不额外双调 `run_ods.py`。

当前倾向：采用方案 1，避免双入口并行维护两套失败策略和摘要逻辑。

### Phase 2：DWS 销售改为消费 ODS

目标：将 `etl_dws_sales.py` 的上游从 Oracle 改为 ODS。

关键点：

1. 保持当前目标表结构与增量写入方式尽量不变。
2. 保持 `date_id` 窗口删除再插入的幂等模型。
3. 先对齐 `ods_m_retail` 与 `ods_m_retailitem` 的可用字段，不额外扩业务口径。

### Phase 3：DWS 库存改为消费 ODS

目标：将 `etl_dws_inventory.py` 的上游从 Oracle 改为 `ods_fa_storage`。

关键点：

1. 校对 `ods_fa_storage` 当前字段是否足够支撑现有 `qty / qty_valid / qtypurchaserem` 口径。
2. 若 ODS 当前字段不足，先补最小必要字段，不顺手扩大改造范围。
3. 保持当日快照覆盖事务模型不变。

### Phase 4：最小验证

目标：确认重构后主链可运行，且结果不出现明显回归。

已完成的最小验证：

1. 连接测试通过，Oracle 与 MySQL 均可访问。
2. `etl_dws_inventory.py` 与 `etl_ads_health.py` 的模块级回归已通过。
3. `run_etl.py` 于 2026-03-23 17:05 的 9 步主链完整成功，结果汇总为：
   - `ods_sync: SUCCESS`
   - `dws_sales: SUCCESS`
   - `dws_inventory: SUCCESS`
   - `ads_health: SUCCESS`
   - `dabo_ready: WARNING (今日无记录，latest_date=None)`
4. ODS 质量校验日志已生成：`logs/ods_qc_20260323_170528.log`。

后续若继续增强，可补：

1. ODS 与 Oracle 的差异对账解释。
2. `dws_sales_daily` / `dws_inventory_daily` / `ads_inventory_health` 的抽样业务核对。
3. DIM 是否继续 ODS 化评估。

### Phase 5：文档同步

目标：将“目标态已经实现的部分”同步成正式现状。

优先顺序：

1. `docs/ARCHITECTURE.md`
2. `docs/DATA_CONTRACTS.md`
3. `docs/数据仓库与ETL手册.md`
4. `README.md`
5. `docs/ETL业务逻辑说明.md`

## 第一阶段文件级改造清单

### A. 调度层

优先改动文件：

1. `run_etl.py`
2. `config.py`
3. `scheduled_etl.py`

建议改法：

1. 在 `run_etl.py` 的 `STEP_ORDER` 中新增 `ods_sync` 步骤，并在 `run_all()` 中显式调用 ODS 同步。
2. 复用 `run_ods.run(...)` 作为主链内的 ODS 执行函数，而不是复制一套 ODS 调度逻辑。
3. `scheduled_etl.py` 继续只调用 `run_etl.py`，避免自动化层同时维护两套入口。
4. `config.py` 的 `TASK_DISPLAY_NAME` 需要新增 `ods_sync` 友好名称，否则主链摘要与告警显示不完整。

当前倾向的步骤顺序：

1. `dim_product`
2. `dim_sku`
3. `dim_store`
4. `dim_channel`
5. `ods_sync`
6. `dws_sales`
7. `dws_inventory`
8. `dabo_ready`
9. `ads_health`

这样做的原因是：DIM 当前仍独立直连 Oracle，不依赖 ODS；ODS 接到 DWS 前面即可满足依赖关系，且对现有主链改动最小。

### B. DWS 销售层

优先改动文件：

1. `etl_dws_sales.py`
2. 视需要补充 `test_etl_automation.py`

当前判断：销售链是第一阶段最适合先切到 ODS 的部分。

原因：

1. `ods_m_retail` 已提供 `id / billdate / c_store_id / tot_amt_actual / tot_amt_list / status / isactive / modifieddate`。来源：[etl_ods_m_retail.py](../../etl_ods_m_retail.py#L101-L113)
2. `ods_m_retailitem` 已提供 `m_retail_id / m_product_id / m_productalias_id / qty / tot_amt_actual / tot_amt_list / modifieddate / settime`。来源：[etl_ods_m_retailitem.py](../../etl_ods_m_retailitem.py#L100-L113)
3. 当前 `dws_sales_daily` 所需的 `store_code / is_cloud_store` 可以改为通过 `dim_store` 按 `c_store_id` 关联获取，而不必继续直连 Oracle `C_STORE`。

因此，销售链的第一阶段改造可以保持：

1. 目标表结构不变
2. 日期窗口删除再插入模型不变
3. 聚合口径先不动
4. 只是把上游切成 `ods_m_retail + ods_m_retailitem + dim_store`

### C. DWS 库存层

优先改动文件：

1. `etl_ods_fa_storage.py`
2. `SQL/create_ods_tables.sql`
3. `etl_dws_inventory.py`

当前判断：库存链阻塞已解除，并已完成 ODS 化。

已完成项：

1. 在 `etl_ods_fa_storage.py` 中补抽 `QTYPURCHASEREM`
2. 在 `SQL/create_ods_tables.sql` 中补 ODS 对应字段，并新增结构变更 SQL
3. `etl_dws_inventory.py` 已切到 `ods_fa_storage + dim_store`

## 当前已确认技术阻塞与实施判断

### 1. 已确认可先实施的部分

1. `run_etl.py` 接入 ODS 步骤
2. `etl_dws_sales.py` 改读 ODS

这两项当前都没有明显的结构级阻塞，属于第一阶段最适合先动手的部分。

### 2. 已完成库存字段补齐与 ODS 化的部分

1. `etl_ods_fa_storage.py` 已补齐 `qtypurchaserem`
2. `etl_dws_inventory.py` 已改读 ODS

### 3. 当前不建议在第一阶段一起做的部分

1. DIM 改为消费 ODS
2. ADS 业务口径重写
3. 文档全面回写

这些动作要么超出当前 ODS 覆盖范围，要么会把当前第一阶段最小闭环做大。

## 推荐开工顺序

建议直接按下面顺序开工：

1. 修改 `config.py`，为 `ods_sync` 补任务展示名。
2. 修改 `run_etl.py`，把 ODS 接入主链步骤与执行摘要。
3. 先不改 `scheduled_etl.py` 的入口，只验证主链接入后是否已自动覆盖 ODS。
4. 重构 `etl_dws_sales.py`，改为消费 `ods_m_retail + ods_m_retailitem + dim_store`。
5. 为库存链补齐 `ods_fa_storage.qtypurchaserem`，同步调整 ODS 建表 SQL。
6. 再重构 `etl_dws_inventory.py`，改为消费 `ods_fa_storage + dim_store`。
7. 做最小验证。
8. 验证通过后，再同步核心文档。

## 当前待确认项

1. `run_etl.py` 是否直接纳入 ODS 步骤，还是通过包装方式复用 `run_ods.py`。
2. 新增的 `qtypurchaserem` 字段在现网 MySQL 是否已执行结构变更 SQL。
3. 第一阶段收口是否需要补做 ODS 与 DWS 的抽样对账。
4. 文档最终采用“只写当前现状”还是“现状 + 未实现目标”双层结构。

当前倾向答案：

1. 倾向在 `run_etl.py` 中直接纳入 ODS 步骤，但执行层复用 `run_ods.run(...)`。
2. `qtypurchaserem` 已在代码与建表 SQL 中落地，但数据库实例仍需执行结构变更 SQL。
3. 第一阶段已达到“主链已纳入 ODS，且 DWS 销售/库存都改读 ODS”的目标，下一步重心是验证。
4. 文档在代码落地后优先写成当前现状；若保留目标态，必须显式标注“未实现”。

## 当前风险清单

### High

1. 若直接先改文档而不改链路，会造成两次回写与双重返工。
2. 若把 DIM 也一起纳入 ODS 化，而 ODS 又未覆盖主档，会导致范围失控。
3. 若在调度层同时保留 `run_etl.py` 和 `run_ods.py` 两套主入口并都参与自动化，失败策略、重试策略和摘要告警会分叉。

### Medium

1. `docs/DATA_CONTRACTS.md` 当前不仅有来源漂移，还有 ODS 库存字段旧口径残留，后续修文必须以代码和快照双证据处理。
2. 文档审计脚本当前是词项级模型，不能替代人工判断“来源语义是否一致”。
3. dws_sales / dws_inventory 改为消费 ODS 后，字段类型或空值处理若与旧链路不同，可能影响 ADS 计算稳定性。

### Low

1. `docs/ETL业务逻辑说明.md` 当前较贴近现状，后续应优先作为校验锚点，而不是第一批重写对象。
2. `docs/数据仓库与ETL手册.md` 当前对 ODS 独立执行的描述基本正确，后续大概率只需局部调整。

## 直接执行入口

当准备继续本主题时，优先按以下顺序推进：

1. 先读取本文件和 `docs/AGENT_HANDOFF.md` 最新一条记录。
2. 先确认当前阶段是在做：
   - 方案细化
   - ETL 重构
   - 验证
   - 文档同步
3. 再进入对应动作。

## 新窗口承接方式

切换到新对话窗口时，建议直接提供本文件，并附上一句：

- “以此文件为完整上下文，继续推进 ODS 打通自动化链路工作，先读取当前阶段与下一步执行入口。”

如果你只想推进某一小步，也可以直接说：

- “以此文件为上下文，只先设计 run_etl.py 如何接入 ODS 步骤，不改代码。”
- “以此文件为上下文，只先重构 etl_dws_sales.py，使其改为消费 ods_m_retail / ods_m_retailitem。”
- “以此文件为上下文，只先整理第一阶段最小验证方案。”
- “以此文件为上下文，只先同步 ARCHITECTURE 和 DATA_CONTRACTS。”

## 下次进来先读这些文件

1. [docs/AGENT_HANDOFF.md](../AGENT_HANDOFF.md)
2. [run_etl.py](../../run_etl.py)
3. [run_ods.py](../../run_ods.py)
4. [scheduled_etl.py](../../scheduled_etl.py)
5. [etl_dws_sales.py](../../etl_dws_sales.py)
6. [etl_dws_inventory.py](../../etl_dws_inventory.py)
7. [etl_ads_health.py](../../etl_ads_health.py)
8. [docs/ARCHITECTURE.md](../ARCHITECTURE.md)
9. [docs/DATA_CONTRACTS.md](../DATA_CONTRACTS.md)
10. [reports/docs_code_alignment.json](../../reports/docs_code_alignment.json)

## 更新规则

每完成以下任一动作，都应更新本文件的“当前阶段快照 / 推进进度 / 关键决策 / 风险清单 / 版本记录”：

- 调整实施顺序
- 开始修改主入口或 DWS 逻辑
- 验证结论发生变化
- 文档同步策略发生变化
- 新增明确证据，推翻旧假设

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.5 | 2026-03-23 | 补充 17:05 主链实跑成功证据，标记第一阶段真实验证通过 |
| v1.0 | 2026-03-23 | 新增 ODS 打通自动化链路计划与续接入口，沉淀当前事实、阶段计划与新窗口承接方式 |
| v1.1 | 2026-03-23 | 补充第一阶段文件级改造清单、实施阻塞与推荐开工顺序 |
| v1.2 | 2026-03-23 | 更新为第一阶段已开工，记录主链已纳入 ODS 同步与质检步骤 |
| v1.3 | 2026-03-23 | 记录 dws_sales 已切换为消费 ODS，推进重心转向库存链字段补齐与 ODS 化 |
| v1.4 | 2026-03-23 | 记录 qtypurchaserem 已落入 ods_fa_storage，且 dws_inventory 已切换为消费 ODS |