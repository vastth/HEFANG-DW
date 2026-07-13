# 达播订单桥接 Oracle 实收实施说明

> 文档目的：为 `vastth/dabo_etl` 项目侧 AI / Agent 提供完整上下文、边界约束、改造目标与实施方案，使其能够在**不修改 Oracle** 的前提下，完成“达播订单桥接 Oracle 生意额”的外部项目改造。

## 0. 配套文档

- 当前实际落地状态与数据库复核结果，请看 [达播数据同步任务续接上下文.md](达播数据同步任务续接上下文.md)
- 当前阶段、下一步与推进日志，请看 [达播数据同步任务推进看板.md](达播数据同步任务推进看板.md)

## 1. 需求目标

目标不是继续统计旧 CSV 自身的“商家收入金额”，也不是先纠结 Excel 金额字段兼容，而是要先识别出：

- 哪些 Oracle 平台订单属于达播订单
- 它们分别属于哪个达播渠道
- 再基于 Oracle / ODS 订单口径汇总每日实收 / 生意额

本需求的最终口径应以 Oracle 为准，外部达播 CSV 的作用是提供“达播订单集合”，而不是直接作为最终销售额口径。

## 2. 关键约束

### 2.1 Oracle 只允许只读

- 不允许在 Oracle 新增表
- 不允许在 Oracle 新增字段
- 不允许在 Oracle 回写标记
- 不允许在 Oracle 执行任何增删改

因此，所有实现动作都必须落在以下两侧：

- 外部项目 `vastth/dabo_etl`
- MySQL / HEFANG-DW 消费侧

### 2.2 不能破坏现有库存健康链路

当前达播链路已经用于库存健康度与自然销量剔除，现有按“日期 + SKU”聚合的 `ads_dabo_daily_sales` 仍然要保留，不能用新需求直接替换掉旧表。

来源：

- `ads_dabo_daily_sales` 当前粒度为“1 行 = 1 个 SKU 在 1 天的达播销售记录”。来源：[docs/DATA_CONTRACTS.md](../docs/DATA_CONTRACTS.md)
- 当前建表脚本仅定义了 `sale_date + product_alias_code` 主键，没有订单号字段。来源：[SQL/达播数据ETL建表.sql](../SQL/%E8%BE%BE%E6%92%AD%E6%95%B0%E6%8D%AEETL%E5%BB%BA%E8%A1%A8.sql)
- `run_etl.py` 只检查 `ads_dabo_daily_sales` 是否就绪，然后驱动健康度回填。来源：[run_etl.py](../run_etl.py)
- `etl_ads_health.py` 也是按 SKU 条码汇总达播销量与达播销售额。来源：[etl_ads_health.py](../etl_ads_health.py)

## 3. 已确认事实

### 3.1 统一 Excel 天然带有“双订单号 + 行级筛选字段”

当前业务已明确：达播筛选的新入口不是旧 CSV，而是云雀导出的 `订单管理*.xlsx`。

已确认的关键字段包括：

- 系统单号
- 平台单号
- 平台
- 状态
- 主播名称
- 平台发货时间
- 商品编码

这说明统一 Excel 已经同时具备：

- 行级筛选达播订单所需的业务字段
- 桥接 Oracle / ODS 所需的主键候选字段
- 平台母单追溯所需的辅助字段

来源：

- [docs/达播数据运营上传指南.md](../docs/%E8%BE%BE%E6%92%AD%E6%95%B0%E6%8D%AE%E8%BF%90%E8%90%A5%E4%B8%8A%E4%BC%A0%E6%8C%87%E5%8D%97.md)
- `docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx`
- [docs/AGENT_LESSONS.md](../docs/AGENT_LESSONS.md)

### 3.2 当前 MySQL 落表粒度过粗，桥接键在写库前被丢失

当前建表脚本中：

- `ads_dabo_daily_sales` 只有 `sale_date`、`product_alias_code`、`dabo_sales_qty`、`dabo_order_count`、`dabo_revenue`
- `log_dabo_import` 只有导入日志信息

没有任何一张表保留：

- `main_order_id`
- `sub_order_id`
- 平台/渠道信息
- 达人信息

来源：[SQL/达播数据ETL建表.sql](../SQL/%E8%BE%BE%E6%92%AD%E6%95%B0%E6%8D%AEETL%E5%BB%BA%E8%A1%A8.sql)

### 3.3 HEFANG-DW 当前只消费聚合结果，不消费订单明细

当前主调度在 `dabo_ready` 步骤只检查 `ads_dabo_daily_sales` 当日是否有数据；之后 `ads_health` 调用 `backfill_dabo_fields()` 回填库存健康度字段。

来源：[run_etl.py](../run_etl.py)

库存健康度回填逻辑中，达播相关字段来自：

- 近 30 天达播销量汇总
- 近 7 天达播销量汇总
- 近 30 天达播销售额汇总
- 近 7 天达播销售额汇总
- 达播最新日期

这些都按 `product_alias_code / sku_barcode` 聚合，没有订单粒度。

来源：[etl_ads_health.py](../etl_ads_health.py)

### 3.4 Oracle 无法仅靠现有业务字段直接筛出“达播”

已做过 Oracle 只读核查，结论是：

- 线上渠道门店编码只能识别平台总渠道
- 未发现稳定的“达人 / 达播 / 直播”原生标记字段可直接筛达播

因此不能依赖 Oracle 自身字段直接把抖音总销售拆成“自然 + 达播”。

来源：[docs/AGENT_HANDOFF.md](../docs/AGENT_HANDOFF.md)

### 3.5 已验证：统一 Excel 应使用“系统单号 -> Oracle M_RETAIL.OMS_SOURCECODE”桥接

2026-04-08 的真实样本核查已确认：

- 整份云雀 Excel 中扫描出 `268` 行 `平台单号` 与 `系统单号` 不一致
- 这些 `系统单号` 普遍带有 `-3051286`、`-3051291`、`-C1`、`-C2` 等后缀
- 拿这些差异样本去 Oracle `BOSNDS3.M_RETAIL` 做只读核查时，`系统单号` 能稳定命中当前业务单据
- 裸 `平台单号` 要么命中更早历史单据，要么完全不命中

代表性样本：

- `6924639914841439798-3051286` 命中 2026-03-27 单据 `RE2603270000141`，而裸 `6924639914841439798` 只命中更早的 2026-03-05 单据 `RE2603050001866`
- `6951493917365049300-3051291` 命中 2026-03-27 单据 `RE2603270000142`，而裸 `6951493917365049300` 命中的是更早的 2026-03-22 单据
- 淘宝样本 `2701769666144032793` 裸平台单号完全不命中，只有 `2701769666144032793-C1` 能命中 Oracle

因此，当前最稳妥的桥接主键已经冻结为：

`系统单号 -> M_RETAIL.OMS_SOURCECODE`

同时：

- `平台单号` 只保留为辅助字段，用于平台母单追溯、排障或回查
- Oracle 全流程保持只读，不允许新增对象或回写达播标记

来源：

- `docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx`
- [docs/AGENT_HANDOFF.md](../docs/AGENT_HANDOFF.md)
- [docs/AGENT_LESSONS.md](../docs/AGENT_LESSONS.md)

### 3.6 当前缺口不在桥接键结论，而在统一 Excel 主线接入

桥接主键本身已经不再是开放问题，当前真正未收口的是：

- dabo_etl / MySQL 侧是否已经显式保留 `system_order_id + platform_order_id`
- 统一 Excel 的行级筛选逻辑是否已经完全替换旧 CSV 主路径
- hefang_dw 是否把 ODS 主线桥接统一切换到 `system_order_id = oms_sourcecode`

换言之，下一阶段重点应从“继续猜桥接键”转为“把已经确认的桥接键落成稳定实现”。

## 4. 外部项目 dabo_etl 的当前问题

对 `vastth/dabo_etl` 的代码和文档核查后，当前行为如下：

- 读取 CSV 时会把中文列映射成 `main_order_id`、`sub_order_id`、`product_alias_code`、`qty`、`order_status`、`ship_time`、`revenue`
- 之后按 `sale_date + product_alias_code` 聚合
- 写入 MySQL 时也只写入 `ads_dabo_daily_sales`

关键问题在于：

**外部项目虽然读取了订单号，但在聚合后没有把订单号持久化保存。**

外部仓库证据：

- `src/etl_processor.py` 中 `rename_map` 已包含 `主订单编号 -> main_order_id`、`子订单编号 -> sub_order_id`
- `aggregate()` 按 `sale_date`、`product_alias_code` 分组，并以 `sub_order_id` 计数订单数
- `src/db_handler.py` 的 `required_cols` 仅允许写入 `sale_date`、`product_alias_code`、`dabo_sales_qty`、`dabo_order_count`、`dabo_revenue`
- `README.md` 与 `REQUIREMENTS.md` 也都说明当前只落 `ads_dabo_daily_sales` 与 `log_dabo_import`

外部仓库来源：

- `https://github.com/vastth/dabo_etl/tree/main/src/etl_processor.py#L65-L89`
- `https://github.com/vastth/dabo_etl/tree/main/src/etl_processor.py#L151-L167`
- `https://github.com/vastth/dabo_etl/tree/main/src/db_handler.py#L145-L183`
- `https://github.com/vastth/dabo_etl/tree/main/README.md#L53-L74`
- `https://github.com/vastth/dabo_etl/tree/main/REQUIREMENTS.md#L32-L54`

## 5. 为什么现状无法满足新需求

现状表 `ads_dabo_daily_sales` 的主键是：

- `sale_date`
- `product_alias_code`

这意味着一旦多笔订单在同一天卖出同一 SKU，它们在写库时就已经合并为一条汇总记录。此时再去追问：

- 这条 SKU 汇总里包含了哪些具体订单？
- 哪些 Oracle 零售单属于达播？

答案已经无法从 MySQL 现状中恢复出来。

因此，**不能指望在 HEFANG-DW 侧基于当前 `ads_dabo_daily_sales` 反推订单集合**。

## 6. 推荐目标方案

### 6.1 总体思路

保留现有聚合表，但当前优先在 hefang_dw 内先落一层“统一 Excel 候选集提取工具”，把输入契约稳定下来，再决定旧桥接表如何迁移。

推荐采用“三层推进”设计：

1. 候选集轨：先在 hefang_dw 内从 `订单管理*.xlsx` 提取统一 Excel 候选集
2. 标签轨：将去重后的 `system_order_id` 落到内部订单标签表，为 ODS 订单打上“是否达播 / 达播渠道”标签
3. 聚合轨：后续指标统一在 ODS / SQL 层基于标签筛选计算；旧 `ads_dabo_daily_sales` 仅保留兼容

当前已落地的第一阶段入口为 [tools/extract_dabo_order_candidates_from_nas.py](../../tools/extract_dabo_order_candidates_from_nas.py)。来源：[tools/extract_dabo_order_candidates_from_nas.py](../../tools/extract_dabo_order_candidates_from_nas.py)

### 6.2 推荐新增表

建议在 hefang_dw / MySQL 优先新增：

- `ads_dabo_order_label`

该对象是当前内部主承接表，优先解决“订单打标”。

若后续仍需承接旧外部链路或兼容订单级明细，再评估：

- `ads_dabo_order_bridge`

如果你更偏好分层命名，也可以叫：

- `dwd_dabo_order_detail`

本文统一将 `ads_dabo_order_label` 视为 hefang_dw 内部主标签对象；`ads_dabo_order_bridge` 继续视作旧兼容链路对象。

### 6.3 推荐字段

最小可用字段如下：

| 字段名 | 说明 | 是否必须 |
|--------|------|----------|
| `source_file` | 来源 Excel 文件名 | 是 |
| `source_file_mtime` | 来源 Excel 修改时间 | 是 |
| `system_order_id` | 系统单号，后续桥接 Oracle / ODS 主键 | 是 |
| `platform_order_id` | 平台单号，仅用于辅助追溯 | 是 |
| `is_dabo_order` | 是否达播 | 是 |
| `dabo_channel_code` | 达播渠道代码，如 dy/tm/xhs/sph | 是 |
| `dabo_channel_name` | 达播渠道名称 | 是 |
| `order_status` | 已完成/已发货 等 | 是 |
| `influencer_id` | 达人 ID | 否 |
| `influencer_name` | 达人昵称 | 否 |
| `platform_ship_time` | 平台发货时间 | 否 |
| `source_row_count` | 当前 system_order_id 在候选集中的行数 | 是 |
| `created_at` | 创建时间 | 是 |
| `updated_at` | 更新时间 | 是 |

### 6.4 推荐主键 / 唯一键

优先建议使用唯一键，而不是只靠自增主键：

- 唯一键一：`(source_file, system_order_id)`

这样做的原因是：当前标签表是“某个 Excel 快照对 ODS 订单的打标结果”，不是 SKU 明细事实表；同一 `system_order_id` 在一个文件中只需要保留 1 条订单级标签。

## 7. dabo_etl 项目最小改造方案

### 7.1 不要替换现有 ads_dabo_daily_sales

保留现有表、现有聚合、现有导入日志逻辑，避免影响库存健康链路。

### 7.2 在 ETL 处理器中保留“清洗后的订单明细 DataFrame”

当前 `process_file()` 的流程是：

1. `read_csv`
2. `clean_and_filter`
3. `aggregate`
4. `validate_sku`
5. 返回聚合结果

建议改为：

1. `read_csv`
2. `clean_and_filter`
3. 基于明细生成 `detail_df`
4. 基于 `detail_df` 再做 `aggregate`
5. `validate_sku` 同时作用于明细与聚合，或至少先校验明细中的 SKU 再聚合
6. 同时返回：
   - `detail_df`
   - `agg_df`
   - `meta`

### 7.3 新增明细写库方法

在 `src/db_handler.py` 中新增类似方法：

- `insert_dabo_order_bridge(detail_df, source_file)`

要求：

- 使用事务
- 使用 `ON DUPLICATE KEY UPDATE` 保证幂等
- 不因为重复导入造成重复明细

### 7.4 监听主流程改成“先写明细，再写聚合”

当前 `file_watcher.py` / `main.py` 的主流程是：

- ETL 返回聚合 DataFrame
- 删除近 N 天旧聚合数据
- 写入 `ads_dabo_daily_sales`

建议改为：

1. ETL 返回 `detail_df + agg_df`
2. 写入 / upsert `ads_dabo_order_bridge`
3. 再写入 / upsert `ads_dabo_daily_sales`
4. 最后写入 `log_dabo_import`

说明：

- 若你仍保留“删除近 N 天重导”的策略，需分别评估明细表与聚合表的删除策略
- 更推荐明细表以唯一键 UPSERT 为主，避免粗暴删除近 60 天导致审计链条不清晰

### 7.5 平台识别建议

当前已冻结的优先方案，不再是“文件名前缀驱动”，而是“统一 Excel 行级字段驱动”。

- 平台识别以 Excel 行内 `平台` 字段为准
- 达播筛选以 `状态 = 平台发货`、`主播名称` 非空且不以 `HEFANG` 开头为准
- 文件名只承担来源文件标识，不再承担主平台识别职责
- 若旧 CSV / 前缀链路仍需保留，应明确标注为历史兼容方案，不能再把它写成当前主路径

这样做的好处是：

- 不再依赖文件名约定去猜业务渠道
- 平台和达人判断都能在行级审计和回溯
- 后续即使同一份 Excel 混入多平台数据，也能稳定按行划分渠道

## 8. 推荐建表草案

以下为建议示意，不要求逐字照搬，但字段意图应完整保留。

```sql
CREATE TABLE IF NOT EXISTS ads_dabo_order_bridge (
  id BIGINT NOT NULL AUTO_INCREMENT,
  platform_code VARCHAR(32) NOT NULL,
  platform_name VARCHAR(64) NOT NULL,
  system_order_id VARCHAR(128) NOT NULL,
  platform_order_id VARCHAR(128) NOT NULL,
  sale_date DATE NOT NULL,
  product_alias_code VARCHAR(80) NOT NULL,
  qty INT NOT NULL DEFAULT 0,
  revenue_csv DECIMAL(14,2) NULL,
  order_status VARCHAR(32) NOT NULL,
  influencer_name VARCHAR(128) NULL,
  source_file VARCHAR(255) NOT NULL,
  import_batch_id VARCHAR(64) NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_dabo_order_bridge (
    platform_code,
    system_order_id,
    platform_order_id,
    product_alias_code
  ),
  KEY idx_dabo_order_bridge_sale_date (sale_date),
  KEY idx_dabo_order_bridge_system_order (system_order_id),
  KEY idx_dabo_order_bridge_platform_order (platform_order_id),
  KEY idx_dabo_order_bridge_sku (product_alias_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

## 9. 改造后的职责边界

### 9.1 hefang_dw 当前先负责什么

- 读取 NAS / 指定路径下的 `订单管理*.xlsx`
- 按统一 Excel 规则提取达播候选集
- 导出候选集摘要 / CSV，供后续落表与桥接验证使用
- 暂不直接改写旧 `ads_dabo_daily_sales` 兼容表

### 9.2 后续若继续落表，承接对象负责什么

- 接收候选集并落到订单桥接明细表
- 显式保留 `system_order_id + platform_order_id` 两套字段语义
- 在金额字段确认后，继续输出旧的 SKU 日聚合表 `ads_dabo_daily_sales`

### 9.3 HEFANG-DW 后续负责什么

- 读取 `ads_dabo_order_bridge`
- 在 **只读 Oracle** 前提下，用 `system_order_id -> M_RETAIL.OMS_SOURCECODE` 做桥接
- 汇总 Oracle 口径的每日达播实收

### 9.4 Oracle 负责什么

- 仅被查询
- 不做任何结构变更或数据写回

## 10. HEFANG-DW 后续实现思路（给外部项目侧 AI 了解上下文）

外部项目改造完成后，HEFANG-DW 侧可采用如下只读方案：

1. 从 MySQL `ads_dabo_order_bridge` 取一段时间内的 `system_order_id`
2. 在 Oracle 只读查询 `M_RETAIL / M_RETAILITEM / C_STORE`
3. 按 `M_RETAIL.OMS_SOURCECODE IN (:system_order_ids)` 识别达播订单
4. 按 `BILLDATE + 平台渠道` 汇总 `TOT_AMT_ACTUAL`
5. 按最终业务口径选择：
   - 仅统计正单实收
   - 或统计含退款净额

注意：

- 当前桥接结论已由统一 Excel 的真实差异样本进一步固化为“系统单号优先”
- 平台单号只保留为追溯字段，不再建议默认映射到 `OMS_SOURCECODE`

## 11. 建议实施顺序

建议按以下顺序推进，避免一次性把所有问题混在一起：

### 阶段 A：先让 hefang_dw 内部候选集跑通

- 用 [tools/extract_dabo_order_candidates_from_nas.py](../../tools/extract_dabo_order_candidates_from_nas.py) 读取 NAS 最新 `订单管理*.xlsx`
- 固化统一 Excel 的筛选逻辑与输出字段
- 抽样核对 `system_order_id / platform_order_id / product_alias_code` 是否稳定导出
- 明确当前仍不直接改写旧兼容聚合表

### 阶段 B：先把订单标签表落库

- 新增表 `ads_dabo_order_label`
- 先验证同一文件重复导入不会产生重复标签
- 再验证 `system_order_id -> ODS / Oracle` 的主桥接链路
- 旧 `ads_dabo_order_bridge` 是否迁移，放到标签主线稳定后再决定

### 阶段 C：再让统一 Excel 标签主线桥接 ODS / Oracle 跑通

- 用云雀 `订单管理*.xlsx` 样本导入订单标签
- 抽样核对 `system_order_id / platform_order_id / dabo_channel_code` 是否完整落库
- 为 HEFANG-DW 提供标签样本，验证 `system_order_id -> ODS / Oracle` 的只读桥接

### 阶段 D：再扩展其他平台

- 天猫样本验证
- 视频号样本验证
- 小红书样本验证

若平台订单号语义不一致，则按平台分别维护桥接规则，不要强行假设“一套规则通吃所有平台”。

## 12. 外部项目侧验收标准

当 `dabo_etl` 改造完成时，至少应满足以下条件：

1. 同一 Excel dry-run 后，`ads_dabo_order_label` 的预期订单标签数可稳定输出。
2. 同一 Excel 重复导入，不会产生重复标签。
3. 至少对当前统一 Excel 样本，可抽出一批 `system_order_id + dabo_channel_code` 供 HEFANG-DW 去桥接 ODS / Oracle。
4. 旧 `ads_dabo_daily_sales` 兼容链路不被当前内部标签表改造破坏。
5. Oracle 全流程保持只读，不需要新增任何对象。

## 13. 对外部项目 AI 的明确实现指令

如果你是 `dabo_etl` 项目侧 AI，请按以下原则实施：

- 不要删除现有 `ads_dabo_daily_sales` 逻辑
- 不要把“订单桥接需求”混成“替换库存健康口径”
- 优先新增内部标签表，而不是扩宽旧聚合表强行塞金额或订单字段
- 主桥接键优先保留 `system_order_id`
- `platform_order_id` 继续保留，但只作为平台母单 / 排障辅助字段
- 所有改造都应兼容“Oracle 只读”边界

## 14. 证据索引

### HEFANG-DW 仓库内证据

- 统一 Excel 字段要求：`系统单号 / 平台单号 / 平台 / 状态 / 主播名称 / 平台发货时间 / 商品编码`。来源：[docs/达播数据运营上传指南.md](../docs/%E8%BE%BE%E6%92%AD%E6%95%B0%E6%8D%AE%E8%BF%90%E8%90%A5%E4%B8%8A%E4%BC%A0%E6%8C%87%E5%8D%97.md)
- 当前聚合表建表定义。来源：[SQL/达播数据ETL建表.sql](../SQL/%E8%BE%BE%E6%92%AD%E6%95%B0%E6%8D%AEETL%E5%BB%BA%E8%A1%A8.sql)
- 当前数据契约说明 `ads_dabo_daily_sales` 为 SKU 日粒度。来源：[docs/DATA_CONTRACTS.md](../docs/DATA_CONTRACTS.md)
- 当前调度只检查聚合表是否就绪。来源：[run_etl.py](../run_etl.py)
- 当前健康度回填按 SKU 汇总达播数据。来源：[etl_ads_health.py](../etl_ads_health.py)
- 抖音主订单号桥接 Oracle 的结论摘要。来源：[docs/AGENT_HANDOFF.md](../docs/AGENT_HANDOFF.md)

### dabo_etl 外部仓库证据

- 中文字段映射为 `main_order_id` / `sub_order_id`：
  - `https://github.com/vastth/dabo_etl/tree/main/src/etl_processor.py#L65-L89`
- 聚合阶段按 `sale_date + product_alias_code` 汇总：
  - `https://github.com/vastth/dabo_etl/tree/main/src/etl_processor.py#L151-L167`
- 写库只允许聚合字段：
  - `https://github.com/vastth/dabo_etl/tree/main/src/db_handler.py#L145-L183`
- 当前 README 只声明落 `ads_dabo_daily_sales` 与 `log_dabo_import`：
  - `https://github.com/vastth/dabo_etl/tree/main/README.md#L53-L74`
- 当前需求文档也只定义聚合表结构：
  - `https://github.com/vastth/dabo_etl/tree/main/REQUIREMENTS.md#L32-L54`

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.6 | 2026-04-08 | 主线进一步收口为 ads_dabo_order_label 订单打标，强调先打标后在 ODS/SQL 层按渠道计算指标 |
| v1.5 | 2026-04-08 | 调整为 hefang_dw 内部候选集先落地，再决定旧桥接表迁移与兼容金额字段策略 |
| v1.4 | 2026-04-08 | 切换到统一 Excel 行级筛选主线，冻结系统单号为 Oracle / ODS 主桥接键，并强化 Oracle 只读边界 |
| v1.3 | 2026-03-31 | 增补视频号前缀 sph，平台识别约定更新为 dy/tm/xhs/sph |
| v1.2 | 2026-03-31 | 冻结平台识别方案为文件名前缀驱动，约定 dy/tm/xhs 并取消旧 dabo 前缀兼容 |
| v1.1 | 2026-03-31 | 补充同目录续接上下文与推进看板入口 |
| v1.0 | 2026-03-31 | 新增达播订单桥接 Oracle 实收实施说明，供 dabo_etl 项目侧 AI 接棒实施 |