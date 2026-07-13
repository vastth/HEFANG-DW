# 数据字典（hefang_dw）

> 说明：本字典以当前 MySQL 已存在对象与仓内已落地 DDL 为主。`ads_daily_report`、`ads_sales_summary` 仅保留为历史规划占位，不属于当前现网表结构；销售专题当前保留的现网对象为 `ads_store_daily_report`、`ads_store_daily_subject_report` 与 `ads_daily_sales`，三者当前都由 `scheduled_store_daily_report.py` 按受影响日期、缺口日期与 freshness 日期批量重跑，且均未接入 `run_etl.py` 主链；`ads_sales_org_daily`、`ads_sales_org_monthly`、`ads_sku_daily` 已退役，不再列为当前现网字典对象。来源：[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L49)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L450)；[scheduled_store_daily_report.py](../scheduled_store_daily_report.py#L984)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L122)

> M3 架构完善中的 `ods_m_retail_raw`、`ods_m_retailitem_raw`、`ods_fa_storage_raw`、`dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` 已由用户人工建表并由 Copilot 按授权完成旁路验证装载：20260428-20260430 销售 raw 为 2861 / 5103 行、销售 DWD 为 5103 行；20260507 库存 raw / DWD 均为 201946 行。上述对象仍未接入 `run_etl.py` / 总控，当前 DWS / ADS 不消费，不代表生产数据契约已切换。来源：[ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md](ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md)；[../reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json](../reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json)

> M4 架构完善中的 `dws_sales_daily_v2`、`dws_inventory_daily_v2` 已由用户人工执行 DDL 草案建表；Copilot 先只读核验，确认销售 v2 为 33 列、库存 v2 为 31 列，两表均具备 `date_id + store_id + product_id + m_productalias_id` 粒度唯一键、`validation_status` 与 `etl_time`。随后已在用户明确授权下完成一次 S3 实跑验收，确认销售 v2 当前 3417 行、库存 v2 当前 75104 行，DWD-v2 mismatch 均为 0。销售 v2 与旧 `dws_sales_daily` 在验收窗口 0 差异；库存 v2 与旧 `dws_inventory_daily` 的 200 条同 key `qty` 差异当前按快照时点不同记录。两张表仍未接入 `run_etl.py` / 总控，当前 ADS 不消费，不代表生产数据契约已切换。来源：[ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md](ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md)；[../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json](../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json)；[../reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json](../reports/context_cache/dws_sales_v2_s3_acceptance_20260507_1339.json)；[../reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json](../reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json)

## ads_daily_report（历史规划对象）
- 描述: 电商日报应用表历史规划草案
- 状态: 历史规划/未实现（当前 MySQL 库、代码、DDL 与结构快照均无对应对象）
- 说明: 以下字段仅保留早期规划占位，不代表当前现网结构；如需查询当前销售日报相关现网表，请查看 `ads_store_daily_report` 与 `ads_store_daily_subject_report`。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | report_date | date | NO |  | 报告日期 |
| 3 | channel_id | int | YES |  | 渠道ID |
| 4 | channel_name | varchar(50) | YES |  | 渠道名称 |
| 5 | sales_amount | decimal(14,2) | YES | 0.00 | 销售额 |
| 6 | sales_qty | int | YES | 0 | 销量 |
| 7 | return_amount | decimal(14,2) | YES | 0.00 | 退货额 |
| 8 | return_qty | int | YES | 0 | 退货量 |
| 9 | net_amount | decimal(14,2) | YES | 0.00 | 净销售额 |
| 10 | net_qty | int | YES | 0 | 净销量 |
| 11 | order_count | int | YES | 0 | 订单数 |
| 12 | avg_price | decimal(10,2) | YES |  | 客单价 |
| 13 | return_rate | decimal(5,2) | YES |  | 退货率 |
| 14 | discount_rate | decimal(5,2) | YES |  | 折扣率 |
| 15 | sales_amount_yoy | decimal(14,2) | YES |  | 去年同期销售额 |
| 16 | yoy_growth | decimal(5,2) | YES |  | 同比增长率 |
| 17 | sales_amount_mom | decimal(14,2) | YES |  | 上期销售额 |
| 18 | mom_growth | decimal(5,2) | YES |  | 环比增长率 |
| 19 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## ads_daily_sales
- 描述: 销售看板月度战役日节奏表
- 状态: 已接专题调度但未接 run_etl 主链
- 说明: 首版固定 `battle_month = report_date` 所在自然月月初，`sales_date` 只覆盖月初到 `report_date`；物理层只落日目标、日实际、累计目标、累计实际与去年同期累计实际，不物化预测字段。当前逻辑已统一到门店日报权威口径：共同考核经营体按 `sales_date` 优先取 `cfg_store_assessment_subject_target_daily.day_target`，未命中时才回退经营实体内门店日目标求和；当日实际和去年同期实际改为在 `ods_m_retail + ods_m_retailitem` 上按门店日报门店范围、商品范围汇总净额；累计字段按 `area_name + report_channel_type` 的日序列累计。历史 `2026-04-15 / v1` 与 `2026-04 / v2` 的验证结论对应旧版销售主题逻辑，本轮统一口径后需重新验证。来源：[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L122)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L141)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L175)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L189)；[etl_ads_daily_sales.py](../etl_ads_daily_sales.py#L311)；[SQL/create_ads_daily_sales.sql](../SQL/create_ads_daily_sales.sql#L1)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | report_date | date | NO |  | 报告日期 |
| 3 | battle_month | date | NO |  | 战役月份首日 |
| 4 | sales_date | date | NO |  | 销售日期 |
| 5 | area_name | varchar(50) | NO |  | 战区；物理层不再生成 全国 汇总行 |
| 6 | report_channel_type | varchar(32) | NO |  | 经营渠道细分类；物理层不再生成 全部 汇总行 |
| 7 | day_target_amt | decimal(18,2) | NO | 0.00 | 当日节奏目标；共同考核时优先主体日目标，否则回退经营实体门店日目标求和 |
| 8 | day_actual_amt | decimal(18,2) | NO | 0.00 | 当日实际，按门店日报商品范围的 ODS 净额汇总 |
| 9 | cum_target_amt | decimal(18,2) | NO | 0.00 | 月累计目标，按 `area_name + report_channel_type` 日序列累加 |
| 10 | cum_actual_amt | decimal(18,2) | NO | 0.00 | 月累计实际，按 `area_name + report_channel_type` 日序列累加 |
| 11 | last_year_cum_actual_amt | decimal(18,2) | NO | 0.00 | 去年同期累计实际，按 `area_name + report_channel_type` 日序列累加 |
| 12 | data_version | varchar(32) | NO | v1 | 数据版本号 |
| 13 | etl_time | datetime | NO |  | ETL生成时间 |
| 14 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 15 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |


## ads_inventory_health
- 描述: 库存健康度应用表（达播字段优先取标签主线，ODS/缓存兜底）

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | snapshot_date | date | YES |  | 快照日期 |
| 3 | product_id | bigint | YES |  | 商品ID |
| 4 | product_code | varchar(80) | YES |  | 商品编码 |
| 5 | product_name | varchar(200) | YES |  | 商品名称 |
| 6 | category_id | int | YES |  | 类别ID |
| 7 | category_name | varchar(50) | YES |  | 类别 |
| 8 | property_id | int | YES |  | 性质ID |
| 9 | property_name | varchar(50) | YES |  | 性质 |
| 10 | series_id | int | YES |  | 系列ID |
| 11 | series_name | varchar(100) | YES |  | 系列名称 |
| 12 | price_list | decimal(12,2) | YES |  | 吊牌价 |
| 13 | total_qty | int | YES |  | 总库存 |
| 14 | warehouse_qty | int | YES |  | 总仓库存 |
| 15 | cloud_qty | int | YES |  | 云仓库存 |
| 16 | purchase_rem_qty | int | YES |  | 采购欠数/在途库存 |
| 17 | sales_qty_30d | int | YES |  | 近30天销量（全量） |
| 18 | sales_amt_30d | decimal(14,2) | YES |  | 近30天销售额（全量） |
| 19 | sales_qty_7d | int | YES |  | 近7天销量（全量） |
| 20 | dabo_sales_qty_30d | int | NO | 0 | 近30天达播销量 |
| 21 | dabo_sales_qty_7d | int | NO | 0 | 近7天达播销量 |
| 22 | dabo_latest_date | date | YES |  | 达播最新日期（按SKU） |
| 23 | dabo_revenue_30d | decimal(14,2) | NO | 0.00 | 近30天达播销售额 |
| 24 | dabo_revenue_7d | decimal(14,2) | NO | 0.00 | 近7天达播销售额 |
| 25 | natural_sales_qty_30d | int | NO | 0 | 近30天自然销量（全量-达播） |
| 26 | natural_sales_qty_7d | int | NO | 0 | 近7天自然销量（全量-达播） |
| 27 | natural_revenue_30d | decimal(14,2) | NO | 0.00 | 近30天自然销售额（全量-达播） |
| 28 | natural_revenue_7d | decimal(14,2) | NO | 0.00 | 近7天自然销售额（全量-达播） |
| 29 | return_qty_30d | int | YES |  | 近30天退货量 |
| 30 | return_amount_30d | decimal(14,2) | YES |  | 近30天退货金额 |
| 31 | daily_avg_sales | decimal(10,2) | YES |  | 近30天日均销量（全量） |
| 32 | daily_avg_sales_7d | decimal(10,2) | YES |  | 近7天日均销量（全量） |
| 33 | natural_daily_avg_sales | decimal(10,2) | NO | 0.00 | 近30天自然日均销量 |
| 34 | natural_daily_avg_sales_7d | decimal(10,2) | NO | 0.00 | 近7天自然日均销量 |
| 35 | sales_velocity | decimal(5,2) | YES |  | 销售加速度（全量） |
| 36 | natural_sales_velocity | decimal(5,2) | YES |  | 自然销售加速度 |
| 37 | turnover_days | decimal(10,1) | YES |  | 周转天数 |
| 38 | inventory_status | varchar(20) | YES |  | 库存状态 |
| 39 | sku_grade | char(1) | YES |  | SABC分级 |
| 40 | suggest_qty | int | YES |  | 建议补货数量 |
| 41 | etl_time | datetime | YES |  | ETL时间戳 |
| 42 | sales_trend | varchar(20) | YES |  | 销售趋势（全量） |
| 43 | status_priority | int | YES |  | 状态优先级 |
| 44 | sales_rank | int | YES |  | 销售排名 |
| 45 | sales_ratio | decimal(5,2) | YES |  | 销售占比 |
| 46 | cumulative_ratio | decimal(5,2) | YES |  | 累计占比 |
| 47 | created_at | datetime | YES |  | 创建时间 |
| 48 | sku_id | bigint | YES |  | SKU主键（M_PRODUCT_ALIAS.ID） |
| 49 | sku_barcode | varchar(80) | YES |  | 条码（M_PRODUCT_ALIAS.NO） |
| 50 | color | varchar(50) | YES |  | SKU颜色 |
| 51 | size | varchar(50) | YES |  | SKU尺寸 |

## ads_store_daily_report
- 描述: 门店经营日报应用表
- 状态: 已实现（独立 ETL 入口 `etl_ads_store_daily_report.py` 已接管写数）
- 说明: 当前表已按最终经营实体出数；未配置共同考核时保持原门店行，已配置共同考核时直接输出经营体行，不再保留被合并的物理门店。若共同考核成员门店当月没有单店目标，但 `cfg_store_assessment_assignment.subject_code` 已配置，其销售额仍会并入经营体；成员门店属性优先命中当日切片，若当日缺失则回退最近历史切片，共同考核成员源门店仍无切片时再回退挂靠主店属性。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L8)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L103)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L128)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L178)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L276)
- 补充: `owner_name` 已在现网落地，当前 ETL 会将其作为必需列写入并在产出后做空值校验；新环境若仍是旧表，可执行 `SQL/alter_ads_store_daily_report_add_owner_name.sql` 补列。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L42)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L527)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L881)
- 补充: 仓库已新增 `SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql`，用于补 `mtd_list_amt` 月累计吊牌金额分母；目标库执行前该字段仍属于待落地物理列，更新后的 ETL 会在写数前检查缺列状态，避免 `月折扣率` 继续只能平均行级折扣率。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L59)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L436)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L607)；[SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql](../SQL/alter_ads_store_daily_report_add_mtd_list_amt.sql#L1)
- 补充: 仓库已新增 `SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql`，用于补 `same_store_mtd_sales_amt` 与 `same_store_last_year_mtd_sales_amt` 两个同店同比辅助金额字段；当前 `yoy_rate` / `yoy_amt_diff` 已改为基于这两列重算。目标库执行前，这两列仍属于待落地物理列，更新后的 ETL 会在写数前检查缺列状态。来源：[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L59)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L607)；[etl_ads_store_daily_report.py](../etl_ads_store_daily_report.py#L711)；[SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql](../SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql#L1)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | report_date | date | NO |  | 报告日期 |
| 3 | store_id | bigint | NO |  | 经营实体ID；共同考核时取挂靠主店ID |
| 4 | store_code | varchar(40) | NO |  | 经营实体编码；共同考核时写入主体编码前40位 |
| 5 | store_name | varchar(255) | NO |  | 经营实体名称；共同考核时写入主体名称 |
| 6 | owner_name | varchar(100) | YES |  | 负责人名称，可为空 |
| 7 | area_name | varchar(100) | YES |  | 区域名称 |
| 8 | report_channel_type | varchar(20) | NO |  | 日报渠道类型 |
| 9 | store_grade | varchar(20) | YES |  | 店铺等级 |
| 10 | is_duty_free | char(1) | NO | N | 是否免税(Y/N) |
| 11 | day_sales_amt | decimal(18,2) | NO | 0.00 | 日销售额 |
| 12 | day_sales_qty | int | NO | 0 | 日销量 |
| 13 | day_order_cnt | int | NO | 0 | 日订单数 |
| 14 | day_attach_rate | decimal(18,4) | YES |  | 日连带 |
| 15 | day_avg_ticket | decimal(18,2) | YES |  | 日客单价 |
| 16 | day_discount_rate | decimal(18,4) | YES |  | 日折扣率 |
| 17 | day_target | decimal(18,2) | NO | 0.00 | 日目标 |
| 18 | day_ach_rate | decimal(18,4) | YES |  | 日达成率 |
| 19 | mtd_sales_amt | decimal(18,2) | NO | 0.00 | 月累计销售额 |
| 20 | mtd_list_amt | decimal(18,2) | NO | 0.00 | 月累计吊牌金额，月累计折扣率分母 |
| 21 | mtd_sales_qty | int | NO | 0 | 月累计销量 |
| 22 | mtd_order_cnt | int | NO | 0 | 月累计订单数 |
| 23 | mtd_attach_rate | decimal(18,4) | YES |  | 月累计连带 |
| 24 | mtd_avg_ticket | decimal(18,2) | YES |  | 月累计客单价 |
| 25 | mtd_discount_rate | decimal(18,4) | YES |  | 月累计折扣率 |
| 26 | month_target | decimal(18,2) | NO | 0.00 | 月目标 |
| 27 | month_ach_rate | decimal(18,4) | YES |  | 月完成率 |
| 28 | last_month_mtd_sales_amt | decimal(18,2) | YES |  | 上月同期累计销售额 |
| 29 | last_month_mtd_sales_qty | int | YES |  | 上月同期累计销量 |
| 30 | last_year_mtd_sales_amt | decimal(18,2) | YES |  | 去年同期累计销售额 |
| 31 | same_store_mtd_sales_amt | decimal(18,2) | NO | 0.00 | 同店本期累计销售额，仅纳入去年同期有销售且 assignment_role 不为快闪的源门店 |
| 32 | same_store_last_year_mtd_sales_amt | decimal(18,2) | NO | 0.00 | 同店去年同期累计销售额，仅纳入去年同期有销售且 assignment_role 不为快闪的源门店 |
| 33 | yoy_rate | decimal(18,4) | YES |  | 销售额同比率，按同店辅助金额重算 |
| 34 | yoy_amt_diff | decimal(18,2) | YES |  | 销售额同比差额，按同店辅助金额重算 |
| 35 | last_year_mtd_sales_qty | int | YES |  | 去年同期累计销量 |
| 36 | yoy_qty_rate | decimal(18,4) | YES |  | 销量同比率 |
| 37 | yoy_qty_diff | decimal(18,4) | YES |  | 销量同比差额 |
| 38 | day_rank | int | YES |  | 日销排名 |
| 39 | mtd_rank | int | YES |  | 月销排名 |
| 40 | time_progress | decimal(18,4) | YES |  | 时间进度 |
| 41 | data_version | varchar(32) | NO | v1 | 数据版本号 |
| 42 | etl_time | datetime | NO |  | ETL生成时间 |
| 43 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 44 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## ads_store_daily_subject_report
- 描述: 门店经营日报统计主体层应用表
- 状态: 已实现（独立 ETL 入口 `etl_ads_store_daily_subject_report.py` 已接管写数）
- 说明: 以最终经营实体层 `ads_store_daily_report` 为输入，再叠加 `cfg_store_assessment_subject_target_daily` 与 `cfg_store_assessment_assignment` 回填主体编码、主店锚点、渠道细分类与成员数；若当月未配置共同考核，则自动回退为“每店一个主体”；当前目标表缺少 `report_channel_type` 时会直接报结构过旧。`day_order_cnt` 与 `mtd_order_cnt` 不在主体层重算，而是直接承接 `ads_store_daily_report` 已修正后的订单数，因此会自动继承门店层“按过滤后商品范围单号净额判 `1 / 0 / -1`、近零值按 0 处理”的口径。来源：[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L31)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L77)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L276)；[etl_ads_store_daily_subject_report.py](../etl_ads_store_daily_subject_report.py#L553)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | report_date | date | NO |  | 报告日期 |
| 3 | subject_code | varchar(64) | NO |  | 统计主体编码 |
| 4 | subject_name | varchar(255) | NO |  | 统计主体名称 |
| 5 | subject_source | varchar(32) | NO |  | 主体来源：default_independent/configured_subject |
| 6 | assessment_mode | varchar(20) | NO |  | 考核模式：独立/合并 |
| 7 | anchor_store_id | bigint | YES |  | 挂靠主店ID |
| 8 | anchor_store_name | varchar(255) | YES |  | 挂靠主店名称 |
| 9 | report_channel_type | varchar(32) | NO |  | 经营渠道细分类 |
| 10 | member_store_count | int | NO | 0 | 主体内门店数 |
| 11 | day_sales_amt | decimal(18,2) | NO | 0.00 | 日销售额 |
| 12 | day_sales_qty | int | NO | 0 | 日销量 |
| 13 | day_order_cnt | int | NO | 0 | 日订单数 |
| 14 | day_attach_rate | decimal(18,4) | YES |  | 日连带 |
| 15 | day_avg_ticket | decimal(18,2) | YES |  | 日客单价 |
| 16 | day_target | decimal(18,2) | NO | 0.00 | 日目标 |
| 17 | day_ach_rate | decimal(18,4) | YES |  | 日达成率 |
| 18 | mtd_sales_amt | decimal(18,2) | NO | 0.00 | 月累计销售额 |
| 19 | mtd_sales_qty | int | NO | 0 | 月累计销量 |
| 20 | mtd_order_cnt | int | NO | 0 | 月累计订单数 |
| 21 | mtd_attach_rate | decimal(18,4) | YES |  | 月累计连带 |
| 22 | mtd_avg_ticket | decimal(18,2) | YES |  | 月累计客单价 |
| 23 | month_target | decimal(18,2) | NO | 0.00 | 月目标 |
| 24 | month_ach_rate | decimal(18,4) | YES |  | 月达成率 |
| 25 | last_month_mtd_sales_amt | decimal(18,2) | NO | 0.00 | 上月同期累计销售额 |
| 26 | last_month_mtd_sales_qty | int | NO | 0 | 上月同期累计销量 |
| 27 | last_year_mtd_sales_amt | decimal(18,2) | NO | 0.00 | 去年同期累计销售额 |
| 28 | yoy_rate | decimal(18,4) | YES |  | 销售额同比率 |
| 29 | yoy_amt_diff | decimal(18,2) | NO | 0.00 | 销售额同比差额 |
| 30 | last_year_mtd_sales_qty | int | NO | 0 | 去年同期累计销量 |
| 31 | yoy_qty_rate | decimal(18,4) | YES |  | 销量同比率 |
| 32 | yoy_qty_diff | int | NO | 0 | 销量同比差额 |
| 33 | day_rank | int | YES |  | 日销排名 |
| 34 | mtd_rank | int | YES |  | 月销排名 |
| 35 | time_progress | decimal(18,4) | NO | 0.0000 | 时间进度 |
| 36 | data_version | varchar(32) | NO | v1 | 数据版本号 |
| 37 | etl_time | datetime | NO |  | ETL生成时间 |
| 38 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 39 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## ads_sales_summary（历史规划对象）
- 描述: 销售汇总应用表历史规划草案
- 状态: 历史规划/未实现（当前 MySQL 库、代码、DDL 与结构快照均无对应对象）
- 说明: 以下字段仅保留早期规划占位，不代表当前现网结构；当前仓库没有对应 ETL 或建表脚本。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | report_date | date | NO |  | 报告日期 |
| 3 | granularity | varchar(20) | NO |  | 粒度 |
| 4 | channel_id | int | YES |  | 渠道ID |
| 5 | channel_name | varchar(50) | YES |  | 渠道名称 |
| 6 | category_id | int | YES |  | 类别ID |
| 7 | category_name | varchar(50) | YES |  | 类别名称 |
| 8 | sales_amount | decimal(14,2) | YES | 0.00 | 销售额 |
| 9 | sales_qty | int | YES | 0 | 销量 |
| 10 | return_amount | decimal(14,2) | YES | 0.00 | 退货额 |
| 11 | return_qty | int | YES | 0 | 退货量 |
| 12 | net_amount | decimal(14,2) | YES | 0.00 | 净销售额 |
| 13 | order_count | int | YES | 0 | 订单数 |
| 14 | sku_count | int | YES | 0 | 动销SKU数 |
| 15 | avg_price | decimal(10,2) | YES |  | 客单价 |
| 16 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## ads_dabo_daily_sales
- 描述: 达播日销售兼容汇总表
- 说明: 当前 `ads_inventory_health` 的达播字段优先使用 `ads_dabo_order_label` 最新标签批次桥接 ODS 与 `ads_dabo_order_retail_bridge` 缓存；仅当标签批次不可用时，才回退本表。来源：[etl_ads_health.py](../etl_ads_health.py#L195)；[etl_ads_health.py](../etl_ads_health.py#L270)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | sale_date | date | NO |  | 发货日期 |
| 2 | product_alias_code | varchar(80) | NO |  | SKU条码 |
| 3 | dabo_sales_qty | int | NO | 0 | 达播销量 |
| 4 | dabo_order_count | int | NO | 0 | 达播订单数 |
| 5 | dabo_revenue | decimal(14,2) | NO | 0.00 | 达播实收金额 |
| 6 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 7 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## ads_dabo_order_bridge
- 描述: 达播订单明细桥接表（原始 CSV 明细）

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | platform_code | varchar(32) | NO |  | 平台代码 |
| 3 | platform_name | varchar(64) | NO |  | 平台名称 |
| 4 | main_order_id | varchar(64) | NO |  | 主订单编号 |
| 5 | sub_order_id | varchar(64) | NO |  | 子订单编号 |
| 6 | sale_date | date | NO |  | 发货日期 |
| 7 | product_alias_code | varchar(80) | NO |  | SKU条码 |
| 8 | qty | int | NO | 0 | 销量 |
| 9 | revenue_csv | decimal(14,2) | NO | 0.00 | CSV实收金额 |
| 10 | order_status | varchar(32) | NO |  | 订单状态 |
| 11 | influencer_id | varchar(64) | YES |  | 主播ID |
| 12 | influencer_name | varchar(128) | YES |  | 主播名称 |
| 13 | ad_channel | varchar(128) | YES |  | 广告渠道 |
| 14 | traffic_channel | varchar(128) | YES |  | 流量渠道 |
| 15 | source_file | varchar(255) | NO |  | 来源文件名 |
| 16 | source_file_date | date | YES |  | 来源文件日期 |
| 17 | import_batch_id | varchar(64) | NO |  | 导入批次ID |
| 18 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 19 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## ads_dabo_order_label
- 描述: 达播订单标签表（统一 Excel 内部主线）
- 状态: 已提供 DDL 与导入脚本，正式写库需用户授权
- 证据: SQL/create_ads_dabo_order_label.sql；tools/load_dabo_order_labels_from_nas.py

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | source_file | varchar(255) | NO |  | 来源 Excel 文件名 |
| 2 | source_sheet | varchar(128) | NO |  | 来源工作表 |
| 3 | source_file_mtime | datetime | NO |  | 来源文件修改时间 |
| 4 | first_source_row_number | int | NO |  | 首个来源行号 |
| 5 | source_row_count | int | NO | 1 | 同 system_order_id 命中的来源行数 |
| 6 | system_order_id | varchar(512) | NO |  | 原始系统单号 |
| 7 | canonical_system_order_id | varchar(512) | YES |  | 归一后的优先桥接键 |
| 8 | normalization_status | varchar(32) | NO | unreviewed | 归一状态 |
| 9 | normalization_rule | varchar(64) | YES |  | 归一规则名 |
| 10 | normalization_evidence | text | YES |  | 归一证据 JSON |
| 11 | platform_order_id | varchar(128) | YES |  | 平台单号 |
| 12 | is_dabo_order | tinyint | NO | 1 | 是否达播 |
| 13 | dabo_source | varchar(64) | NO | yunque_order_management | 标签来源 |
| 14 | dabo_channel_code | varchar(32) | NO |  | 达播渠道代码 |
| 15 | dabo_channel_name | varchar(64) | NO |  | 达播渠道名称 |
| 16 | influencer_id | varchar(128) | YES |  | 主播 ID |
| 17 | influencer_name | varchar(128) | NO |  | 主播名称 |
| 18 | order_status | varchar(64) | NO |  | 订单状态 |
| 19 | platform_ship_time | varchar(32) | YES |  | 平台发货时间原始文本 |
| 20 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 21 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## log_dabo_import
- 描述: 达播CSV导入日志

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | file_name | varchar(255) | NO |  | 文件名 |
| 3 | file_path | varchar(500) | YES |  | 文件路径 |
| 4 | records_total | int | NO | 0 | 原始行数 |
| 5 | records_after_filter | int | NO | 0 | 过滤后行数 |
| 6 | records_inserted | int | NO | 0 | 写入行数 |
| 7 | sku_match_rate | decimal(5,4) | YES |  | SKU匹配率 |
| 8 | status | varchar(20) | NO |  | 状态 |
| 9 | message | varchar(1000) | YES |  | 错误信息 |
| 10 | started_at | datetime | YES |  | 开始时间 |
| 11 | finished_at | datetime | YES |  | 结束时间 |
| 12 | created_at | datetime | NO | CURRENT_TIMESTAMP |  |

## dim_category
- 描述: 类别维度表
- 状态: 已实现（数据库结构快照）
- 证据: reports/snapshot_mysql_hefangdw_schema.json（2026-03-01 01:41:36）

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | category_id | int | NO |  | 类别ID |
| 2 | category_name | varchar(50) | NO |  | 类别名称 |
| 3 | is_main_product | char(1) | YES | Y | 是否主销品类别 |
| 4 | sort_order | int | YES | 0 | 排序 |
| 5 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dim_channel
- 描述: 电商渠道维度表
- 状态: 已实现（ETL + 建表脚本）；目标库现存数据已验证
- 证据: reports/snapshot_mysql_hefangdw_schema.json（2026-03-01 01:41:36）
- 说明: 仓库内已提供 [etl_dim_channel.py](etl_dim_channel.py#L1-L131) 与 [SQL/create_dim_channel.sql](SQL/create_dim_channel.sql#L1-L11)，来源为 Oracle `O2O_RETAIL_CHANNEL`；当前目标字段名为 `WING_CODE`，直接映射源表 `WING_CODE`，`CODE` 仅保留为渠道编码。2026-03-23 已实查 Oracle 与 MySQL，确认两边均为 87 条记录且 `WING_CODE` 全部非空。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | channel_id | int | NO |  | 渠道ID |
| 2 | channel_name | varchar(50) | NO |  | 渠道名称 |
| 3 | channel_code | varchar(20) | YES |  | 渠道编码 |
| 4 | WING_CODE | varchar(40) | YES |  | 渠道挂接码（保留 Oracle 原值） |
| 5 | is_main | tinyint | YES | 0 | 是否主要渠道 |
| 6 | platform_type | varchar(20) | YES |  | 平台类型 |
| 7 | is_active | char(1) | YES | Y | 是否有效 |
| 8 | created_at | datetime | YES | CURRENT_TIMESTAMP | 创建时间 |

## dim_date
- 描述: 日期维度表
- 说明: dim_date 为静态维度表，当前未在代码实现自动生成。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | date_id | int | NO |  | 日期ID格式YYYYMMDD |
| 2 | date_value | date | NO |  | 日期 |
| 3 | date_year | int | NO |  | 年 |
| 4 | date_month | int | NO |  | 月 |
| 5 | date_day | int | NO |  | 日 |
| 6 | date_quarter | int | NO |  | 季度 |
| 7 | week_of_year | int | NO |  | 年周数 |
| 8 | day_of_week | int | NO |  | 周几1到7 |
| 9 | day_name_cn | varchar(10) | YES |  | 周几中文 |
| 10 | month_name_cn | varchar(10) | YES |  | 月份中文 |
| 11 | is_weekend | tinyint | YES | 0 | 是否周末 |
| 12 | is_holiday | tinyint | YES | 0 | 是否节假日 |
| 13 | holiday_name | varchar(50) | YES |  | 节假日名称 |
| 14 | year_month | varchar(7) | YES |  | 年月格式YYYY-MM |
| 15 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dim_product
- 描述: 商品维度表
- 说明: year_id/year_name 字段当前未在代码实现写入（对应 M_DIM2_ID 维度）。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | product_id | bigint | NO |  | 商品ID |
| 2 | product_code | varchar(80) | NO |  | 商品编码款号 |
| 3 | product_name | varchar(200) | YES |  | 商品名称 |
| 4 | category_id | int | YES |  | 类别ID |
| 5 | category_name | varchar(50) | YES |  | 类别名称 |
| 6 | property_id | int | YES |  | 性质ID |
| 7 | property_name | varchar(50) | YES |  | 性质名称 |
| 8 | series_id | int | YES |  | 系列ID |
| 9 | series_name | varchar(100) | YES |  | 系列名称 |
| 10 | brand_id | int | YES |  | 品牌ID |
| 11 | brand_name | varchar(50) | YES |  | 品牌名称 |
| 12 | year_id | int | YES |  | 年份ID |
| 13 | year_name | varchar(20) | YES |  | 年份 |
| 14 | price_list | decimal(12,2) | YES |  | 吊牌价 |
| 15 | price_cost | decimal(12,2) | YES |  | 成本价 |
| 16 | is_main_product | char(1) | YES | Y | 是否主销品 |
| 17 | is_active | char(1) | YES | Y | 是否有效 |
| 18 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 19 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 20 | material | text | YES |  |  |

## dim_product_attr
- 描述: 商品属性表（颜色/尺寸，取每个货号的第一个SKU）
- 说明: 当前 `etl_dim_product.py` 会先执行固定 DDL，再对 `dim_product_attr` 做 `TRUNCATE + append`，不再用 `replace` 重建整表冲掉注释。来源：[etl_dim_product.py](../etl_dim_product.py#L145)；[SQL/create_dim_product_attr.sql](../SQL/create_dim_product_attr.sql#L1)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | product_id | bigint | YES |  | 商品ID(dim_product.product_id) |
| 2 | color | text | YES |  | 颜色 |
| 3 | size | text | YES |  | 尺寸 |

## dim_sku
- 描述: SKU维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | sku_id | bigint | NO |  | SKU主键 |
| 2 | sku_barcode | varchar(80) | YES |  | 条码 |
| 3 | product_id | bigint | YES |  | 货号ID |
| 4 | sku_color | varchar(50) | YES |  | 颜色 |
| 5 | sku_size | varchar(50) | YES |  | 尺寸 |
| 6 | is_active | char(1) | YES | Y | 是否有效 |
| 7 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 8 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dim_store
- 描述: 店仓维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | store_id | bigint | NO |  | 店仓ID |
| 2 | store_code | varchar(40) | NO |  | 店仓编码 |
| 3 | store_name | varchar(255) | YES |  | 店仓名称 |
| 4 | area_id | int | YES |  | 区域ID |
| 5 | area_name | varchar(100) | YES |  | 区域名称 |
| 6 | is_warehouse | tinyint | YES | 0 | 是否仓库 |
| 7 | is_store | tinyint | YES | 0 | 是否门店 |
| 8 | is_cloud_store | char(1) | YES | N | 是否云仓 |
| 9 | is_center | char(1) | YES | N | 是否物流中心 |
| 10 | store_type | varchar(20) | YES |  | 类型 |
| 11 | is_active | char(1) | YES | Y | 是否有效 |
| 12 | open_date | date | YES |  | 门店开业日期，来源 Oracle `C_STORE.OPENDATE`；源值为空或无法安全转换时为 NULL |
| 13 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 14 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |

说明：`open_date` 对应 DDL 为 `SQL/alter_dim_store_add_open_date.sql`，已于 2026-07-13 由用户人工执行并完成 `dim_store` 全量刷新。只读核对结果为 231 行、有效开业日期 95 行、不可用开业日期 136 行，日期范围为 2018-07-25 至 2026-07-05。`etl_dim_store.py` 会先校验字段存在再执行 `TRUNCATE`，因此不会在缺列时先清空维表；旧版按列名写入的 ETL 回滚后可保留该可空列。

## dim_store_report_attr
- 描述: 门店日报业务属性配置表
- 状态: 已实现（配置表，供 `ads_store_daily_report` 读取）
- 说明: 默认可人工维护；正式扩范围时，也可通过 `tools/import_cfg_store_target_daily_from_nas.py --sync-store-report-attr` 基于 NAS 模板 `门店类型` 列，按 `store_id` 对当前有效记录做未变化 / 变更 / 新增 / 退出分类。未变化不动，变更执行关旧开新，新增只开新，退出只关旧。脚本默认沿用目标月内现有最新 `effective_start_date`，目标月无现存版本时回退到月首，并在写库前检查是否存在其他不同起始日的有效配置重叠。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | store_id | bigint | NO |  | 店仓ID(dim_store.store_id) |
| 3 | store_code | varchar(40) | NO |  | 店仓编码(dim_store.store_code) |
| 4 | store_name | varchar(255) | NO |  | 店仓名称(dim_store.store_name) |
| 5 | report_channel_type | varchar(20) | NO |  | 日报渠道类型 |
| 6 | report_channel_type_group | varchar(20) | YES |  | 日报渠道粗分类生成列（由 report_channel_type 自动派生） |
| 7 | store_grade | varchar(20) | YES |  | 店铺等级 |
| 8 | is_duty_free | char(1) | NO | N | 是否免税(Y/N) |
| 9 | is_include_in_daily_report | char(1) | NO | Y | 是否纳入日报(Y/N) |
| 10 | remark | varchar(500) | YES |  | 业务备注 |
| 11 | effective_start_date | date | NO |  | 生效开始日期 |
| 12 | effective_end_date | date | NO | 9999-12-31 | 生效结束日期 |
| 13 | updated_by | varchar(50) | YES |  | 最近更新人 |
| 14 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 15 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

- 现网状态：`SQL/alter_dim_store_report_attr_add_channel_type_group.sql` 已于 2026-04-08 执行，当前字典以 15 列为准；`report_channel_type_group` 为 STORED 生成列，用于按 `report_channel_type` 自动派生 `小程序 / 直营 / 联营` 三类粗分类。
- 结构证据：`reports/snapshot_mysql_hefangdw_schema.json` 于 2026-04-08 14:21:42 生成，快照已包含 `report_channel_type_group` 与 `extra = STORED GENERATED`。

## dim_report_product_rule
- 描述: 门店日报商品纳入口径规则表
- 状态: 已实现（配置表，当前保留为历史配置与人工分析参考）
- 说明: 自 2026-06-08 起，`ads_store_daily_report` 与 `ads_daily_sales` 不再直接读取该表圈定商品范围，改为固定排除 `147=辅料`、`149=办公用品`、`150=道具`，其余 `dim_product.category_id` 默认纳入。该表现存记录仍可用于历史回溯与人工分析；若后续业务要调整固定排除集合，应修改 ETL / 对账 SQL 并人工重跑受影响日期。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | category_id | int | NO |  | 商品类别ID(dim_product.category_id) |
| 3 | category_name | varchar(100) | NO |  | 商品类别名称 |
| 4 | include_in_store_daily_report | char(1) | NO | Y | 是否纳入门店日报(Y/N) |
| 5 | rule_note | varchar(500) | YES |  | 规则说明 |
| 6 | effective_start_date | date | NO |  | 生效开始日期 |
| 7 | effective_end_date | date | NO | 9999-12-31 | 生效结束日期 |
| 8 | updated_by | varchar(50) | YES |  | 最近更新人 |
| 9 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 10 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## cfg_store_target_daily
- 描述: 门店日报目标配置表
- 状态: 已实现（配置表，供 `ads_store_daily_report` 读取）
- 说明: 正式交付方案已确认采用“业务投递 Excel 到 NAS 指定目录，由 Python 定时扫描导入 `cfg_store_target_daily`”；当前已冻结 NAS 目录为 `\\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\`，并按月份分文件管理，当前推荐命名规则已切换为 `YYYYMM考核数据配置表.xlsx`；导入脚本同时兼容历史 `YYYY年MM月日目标配置表_vN.xlsx`。当前仓库已提供导入脚本 `tools/import_cfg_store_target_daily_from_nas.py`；现网已于 2026-04-03 完成 `log_store_target_import` 建表、首轮 `--apply` 写库与专项消费验证，新环境首次写库前仍需先执行 `SQL/create_log_store_target_import.sql`。若 NAS 目录内同时存在多个目标月份文件，需显式传入 `--target-month` 选择月份；若同月同时存在多个版本文件，则需改用 `--file-path` 显式指定。若模板显式提供 `门店类型` 列，可追加 `--sync-store-report-attr` 同步刷新 `dim_store_report_attr`。若工作簿同时提供 `统计主体目标` 与 `门店考核归属`，同一次导入也会同步刷新共同考核配置表；若两张 sheet 同时存在但无有效数据，则表示清空当月共同考核配置。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | target_date | date | NO |  | 目标日期 |
| 3 | store_id | bigint | NO |  | 店仓ID(dim_store.store_id) |
| 4 | month_target | decimal(18,2) | NO | 0.00 | 门店月目标 |
| 5 | day_target | decimal(18,2) | NO | 0.00 | 门店日目标 |
| 6 | target_version | varchar(32) | NO | v1 | 目标版本号 |
| 7 | created_by | varchar(50) | YES |  | 创建人/导入人 |
| 8 | updated_by | varchar(50) | YES |  | 最近更新人 |
| 9 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 10 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## cfg_store_assessment_subject_target_daily
- 描述: 门店日报统计主体日目标配置表
- 状态: 已实现（配置表，供 `etl_ads_store_daily_subject_report.py` 读取）
- 说明: 来自 NAS 工作簿 `统计主体目标` sheet；以“主体编码 + 目标日期 + 目标版本”维护主体日目标，供统计主体层优先取值。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | target_date | date | NO |  | 目标日期 |
| 3 | target_month | date | NO |  | 目标月份首日 |
| 4 | subject_code | varchar(64) | NO |  | 统计主体编码 |
| 5 | subject_name | varchar(255) | NO |  | 统计主体名称 |
| 6 | assessment_mode | varchar(20) | NO |  | 考核模式：独立/合并 |
| 7 | month_target | decimal(18,2) | NO | 0.00 | 月目标 |
| 8 | day_target | decimal(18,2) | NO | 0.00 | 日目标 |
| 9 | target_version | varchar(32) | NO | v1 | 目标版本号 |
| 10 | remark | varchar(500) | YES |  | 备注 |
| 11 | created_by | varchar(64) | YES |  | 创建人 |
| 12 | updated_by | varchar(64) | YES |  | 最近更新人 |
| 13 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 14 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## cfg_store_assessment_assignment
- 描述: 门店日报统计主体归属配置表
- 状态: 已实现（配置表，供 `etl_ads_store_daily_subject_report.py` 读取）
- 说明: 来自 NAS 工作簿 `门店考核归属` sheet；只以显式配置决定“共同考核”，不依据商场、城市或 RT 编码自动推断。未配置时，主体层默认回退为独立门店主体。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | target_month | date | NO |  | 目标月份首日 |
| 3 | target_version | varchar(32) | NO | v1 | 目标版本号 |
| 4 | store_id | bigint | NO |  | 门店ID |
| 5 | store_code | varchar(40) | NO |  | 门店编码 |
| 6 | store_name | varchar(255) | NO |  | 门店名称 |
| 7 | subject_code | varchar(64) | NO |  | 统计主体编码 |
| 8 | assignment_role | varchar(20) | NO |  | 归属角色：主店/快闪/独立 |
| 9 | is_joint_assessment | char(1) | NO | N | 是否共同考核(Y/N) |
| 10 | anchor_store_id | bigint | YES |  | 挂靠主店ID |
| 11 | anchor_store_name | varchar(255) | YES |  | 挂靠主店名称 |
| 12 | effective_start_date | date | NO |  | 生效开始日 |
| 13 | effective_end_date | date | NO |  | 生效结束日 |
| 14 | remark | varchar(500) | YES |  | 备注 |
| 15 | created_by | varchar(64) | YES |  | 创建人 |
| 16 | updated_by | varchar(64) | YES |  | 最近更新人 |
| 17 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 18 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## log_store_target_import
- 描述: 门店日报目标导入日志表
- 状态: 已实现（2026-04-03 已建表，并完成首条 SUCCESS 导入日志验证）
- 说明: 由 `tools/import_cfg_store_target_daily_from_nas.py --apply` 追加写入，记录文件指纹、目标月份、门店数、展开行数、写入结果与错误信息；新环境首次使用前仍需先执行 `SQL/create_log_store_target_import.sql`。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | file_name | varchar(255) | NO |  | 源文件名 |
| 3 | file_path | varchar(500) | YES |  | 源文件路径 |
| 4 | file_md5 | char(32) | YES |  | 源文件MD5 |
| 5 | source_sheet | varchar(100) | NO | 导入模板 | 来源工作表 |
| 6 | target_month | date | YES |  | 目标月份首日 |
| 7 | target_version | varchar(32) | YES |  | 目标版本号 |
| 8 | store_count | int | NO | 0 | 命中的门店数 |
| 9 | records_total | int | NO | 0 | 源门店行数 |
| 10 | records_after_filter | int | NO | 0 | 展开后的日粒度行数 |
| 11 | records_inserted | int | NO | 0 | 实际写入行数 |
| 12 | status | varchar(20) | NO |  | 执行状态 |
| 13 | message | varchar(1000) | YES |  | 执行摘要或错误信息 |
| 14 | started_at | datetime | YES |  | 开始时间 |
| 15 | finished_at | datetime | YES |  | 结束时间 |
| 16 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |

## cfg_store_operation_owner_snapshot
- 描述: 门店经营负责人当前快照表
- 状态: 已实现（建表 SQL 已落盘，供 `tools/import_store_operation_owner_from_nas.py` 写入）
- 说明: 承接业务在 NAS 维护的当前负责人真值；脚本按 `snapshot_date` 推导当日应维护的经营实体清单，独立门店维护 `STORE`，共同考核经营体维护 `SUBJECT`。若经营体已存在，则快照中只允许保留经营体行，不允许再保留被吸收的 RT 成员门店。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L256)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L320)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L403)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L437)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L1)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | snapshot_date | date | NO |  | 快照日期 |
| 3 | entity_type | varchar(20) | NO |  | 经营实体类型：STORE/SUBJECT |
| 4 | entity_id | bigint | YES |  | 经营实体ID；普通门店=store_id，共同考核主体=挂靠主店store_id |
| 5 | entity_code | varchar(64) | NO |  | 经营实体编码；普通门店=store_code，共同考核主体=subject_code |
| 6 | entity_name | varchar(255) | NO |  | 经营实体名称 |
| 7 | owner_name | varchar(100) | YES |  | 负责人名称，可为空 |
| 8 | remark | varchar(500) | YES |  | 备注 |
| 9 | source_file_name | varchar(255) | YES |  | 来源文件名 |
| 10 | source_file_md5 | char(32) | YES |  | 来源文件MD5 |
| 11 | created_by | varchar(64) | YES |  | 创建人 |
| 12 | updated_by | varchar(64) | YES |  | 最近更新人 |
| 13 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 14 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## dim_store_operation_owner_assignment
- 描述: 门店经营负责人 SCD2 历史表
- 状态: 已实现（建表 SQL 已落盘，供 `tools/import_store_operation_owner_from_nas.py --apply` 维护）
- 说明: 由负责人快照与当前有效历史切片对比后维护；`unchanged` 不动，`changed/new` 开新，`changed/exited` 关旧。若新快照与紧邻上一版历史切片完全一致，则直接重开旧版本，不新增重复切片。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L584)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L830)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L876)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L917)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L23)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | entity_type | varchar(20) | NO |  | 经营实体类型：STORE/SUBJECT |
| 3 | entity_id | bigint | YES |  | 经营实体ID；普通门店=store_id，共同考核主体=挂靠主店store_id |
| 4 | entity_code | varchar(64) | NO |  | 经营实体编码 |
| 5 | entity_name | varchar(255) | NO |  | 经营实体名称 |
| 6 | owner_name | varchar(100) | YES |  | 负责人名称，可为空 |
| 7 | source_snapshot_date | date | NO |  | 触发当前版本生效的快照日期 |
| 8 | source_file_name | varchar(255) | YES |  | 触发当前版本生效的来源文件名 |
| 9 | source_file_md5 | char(32) | YES |  | 触发当前版本生效的来源文件MD5 |
| 10 | effective_start_date | date | NO |  | 生效开始日 |
| 11 | effective_end_date | date | NO |  | 生效结束日 |
| 12 | is_current | char(1) | NO | Y | 是否当前有效（Y/N） |
| 13 | created_by | varchar(64) | YES |  | 创建人 |
| 14 | updated_by | varchar(64) | YES |  | 最近更新人 |
| 15 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 16 | updated_at | datetime | NO | CURRENT_TIMESTAMP | 更新时间 |

## log_store_operation_owner_import
- 描述: 门店经营负责人导入日志表
- 状态: 已实现（建表 SQL 已落盘；是否写库取决于用户是否执行 `--apply`）
- 说明: 由 `tools/import_store_operation_owner_from_nas.py --apply` 追加写入，记录文件指纹、快照日期、预期经营实体数、命中数、缺失/异常数以及快照/历史写入结果。dry-run 不写该表。来源：[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L775)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L997)；[../tools/import_store_operation_owner_from_nas.py](../tools/import_store_operation_owner_from_nas.py#L1164)；[../SQL/create_store_operation_owner_tables.sql](../SQL/create_store_operation_owner_tables.sql#L48)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 主键ID |
| 2 | file_name | varchar(255) | NO |  | 源文件名 |
| 3 | file_path | varchar(500) | YES |  | 源文件路径 |
| 4 | file_md5 | char(32) | YES |  | 源文件MD5 |
| 5 | source_sheet | varchar(100) | NO |  | 来源工作表 |
| 6 | snapshot_date | date | NO |  | 快照日期 |
| 7 | records_total | int | NO | 0 | 源数据行数 |
| 8 | expected_entity_count | int | NO | 0 | 预期经营实体数 |
| 9 | matched_entity_count | int | NO | 0 | 成功匹配的经营实体数 |
| 10 | missing_entity_count | int | NO | 0 | 缺失经营实体数 |
| 11 | unexpected_entity_count | int | NO | 0 | 异常经营实体数 |
| 12 | snapshot_rows_inserted | int | NO | 0 | 快照表写入行数 |
| 13 | history_rows_opened | int | NO | 0 | 历史表开新或重开行数 |
| 14 | history_rows_closed | int | NO | 0 | 历史表关旧或同日替换行数 |
| 15 | status | varchar(20) | NO |  | 执行状态 |
| 16 | message | varchar(1000) | YES |  | 执行摘要或错误信息 |
| 17 | started_at | datetime | YES |  | 开始时间 |
| 18 | finished_at | datetime | YES |  | 结束时间 |
| 19 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |

## dws_inventory_daily
- 描述: 日库存快照表（全量SKU，不做主销品类别过滤）

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | date_id | int | NO |  | 日期ID |
| 3 | store_id | bigint | NO |  | 店仓ID |
| 4 | store_code | varchar(40) | YES |  | 店仓编码 |
| 5 | is_cloud_store | char(1) | YES | N | 是否云仓(Y/N) |
| 6 | product_id | bigint | NO |  | 商品ID |
| 7 | m_productalias_id | bigint | YES |  | SKU ID（条码） |
| 8 | qty | int | YES | 0 | 库存数量 |
| 9 | qty_valid | int | YES | 0 | 可用库存 |
| 10 | qty_occupy | int | YES | 0 | 占用数量 |
| 11 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 12 | etl_time | datetime | YES |  | ETL时间戳 |
| 13 | qtypurchaserem | bigint | YES | 0 | 采购欠数/在途 |

## dws_sales_daily
- 描述: 日销售汇总表
- 说明: net_qty/net_amount 字段当前未在代码实现写入（默认值为0）。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | date_id | int | NO |  | 日期ID |
| 3 | store_id | bigint | NO |  | 店仓ID |
| 4 | product_id | bigint | NO |  | 商品ID |
| 5 | m_productalias_id | bigint | YES |  | SKU ID（条码） |
| 6 | sales_qty | int | YES | 0 | 销售数量 |
| 7 | sales_amount | decimal(14,2) | YES | 0.00 | 销售金额 |
| 8 | sales_amount_list | decimal(14,2) | YES | 0.00 | 吊牌金额 |
| 9 | return_qty | int | YES | 0 | 退货数量 |
| 10 | return_amount | decimal(14,2) | YES | 0.00 | 退货金额 |
| 11 | net_qty | int | YES | 0 | 净销量（字段存在但当前ETL不填充，默认0） |
| 12 | net_amount | decimal(14,2) | YES | 0.00 | 净销售额（字段存在但当前ETL不填充，默认0） |
| 13 | order_count | int | YES | 0 | 订单数 |
| 14 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 15 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 16 | etl_time | datetime | YES |  | ETL时间戳 |
| 17 | store_code | varchar(32) | YES |  | 源店仓编码（如 DS001） |
| 18 | is_cloud_store | char(1) | YES | N | 是否云仓(Y/N) |

## dws_sales_daily_v2
- 描述: DWS v2 销售日汇总并行表，来源 `dwd_sales_retail_item`
- 状态: 用户已人工建表；已新增 `etl_dws_sales_v2.py` dry-run / conn-test / S3 手工写入分支。当前已在用户明确授权下完成一次 S3 实跑验收：`20260428-20260430` 写入 3417 行，DWD-v2 mismatch 为 0，且与旧 `dws_sales_daily` 在验收窗口 0 差异；未接入 `run_etl.py` / 总控，当前 ADS 不消费。
- 粒度: `date_id + store_id + product_id + m_productalias_id`；唯一键 `uk_dws_sales_daily_v2_date_store_product_sku`。
- 证据: [../SQL/draft_create_dws_sales_daily_v2.sql](../SQL/draft_create_dws_sales_daily_v2.sql)；[../dws_v2_write_utils.py](../dws_v2_write_utils.py)；[../etl_dws_sales_v2.py](../etl_dws_sales_v2.py)；[../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json](../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 自增主键 |
| 2 | date_id | int | NO |  | 业务日期，YYYYMMDD，来源 dwd_sales_retail_item.date_id |
| 3 | store_id | bigint | NO |  | 店仓 ID，来源 dwd_sales_retail_item.store_id |
| 4 | store_code | varchar(40) | NO |  | 店仓编码，来源 dwd_sales_retail_item.store_code |
| 5 | is_cloud_store | char(1) | NO | N | 是否云仓门店，来源 dwd_sales_retail_item.is_cloud_store |
| 6 | product_id | bigint | NO |  | 商品 ID，来源 dwd_sales_retail_item.product_id |
| 7 | m_productalias_id | bigint | NO |  | SKU / 条码 ID，来源 dwd_sales_retail_item.m_productalias_id |
| 8 | sales_qty | decimal(18,4) | NO | 0 | 销售数量，按当前 DWS 正向销售行口径汇总 |
| 9 | sales_amount | decimal(18,4) | NO | 0 | 销售金额，按当前 DWS 正向销售行口径汇总 |
| 10 | sales_amount_list | decimal(18,4) | NO | 0 | 吊牌金额，按当前 DWS 正向销售行口径汇总 |
| 11 | return_qty | decimal(18,4) | NO | 0 | 退货数量，按当前 DWS 退货行口径取绝对值汇总 |
| 12 | return_amount | decimal(18,4) | NO | 0 | 退货金额，按当前 DWS 退货行口径取绝对值汇总 |
| 13 | net_qty | decimal(18,4) | NO | 0 | 净销量候选值，建议由 sales_qty - return_qty 生成；切换前需确认是否暴露给下游 |
| 14 | net_amount | decimal(18,4) | NO | 0 | 净销售额候选值，建议由 sales_amount - return_amount 生成；切换前需确认是否暴露给下游 |
| 15 | order_count | bigint | NO | 0 | 订单数，COUNT(DISTINCT 正向销售 retail_id) |
| 16 | source_dwd_row_count | bigint | NO | 0 | 参与聚合的 DWD 明细行数 |
| 17 | positive_line_count | bigint | NO | 0 | 正向销售明细行数 |
| 18 | return_line_count | bigint | NO | 0 | 退货明细行数 |
| 19 | min_retail_modified_at | datetime | YES |  | 参与聚合单头最小修改时间 |
| 20 | max_retail_modified_at | datetime | YES |  | 参与聚合单头最大修改时间 |
| 21 | min_item_modified_at | datetime | YES |  | 参与聚合明细最小修改时间 |
| 22 | max_item_modified_at | datetime | YES |  | 参与聚合明细最大修改时间 |
| 23 | min_item_set_time | datetime | YES |  | 参与聚合明细最小 SETTIME |
| 24 | max_item_set_time | datetime | YES |  | 参与聚合明细最大 SETTIME |
| 25 | source_min_loaded_at | datetime | YES |  | 参与聚合 raw ODS 最早装载时间 |
| 26 | source_max_loaded_at | datetime | YES |  | 参与聚合 raw ODS 最晚装载时间 |
| 27 | load_batch_id | varchar(64) | YES |  | DWS v2 装载批次 ID |
| 28 | source_layer_version | varchar(32) | NO | M3_DWD_V1 | 来源层版本标识 |
| 29 | validation_status | varchar(20) | NO | PENDING | 并行对账状态：PENDING / PASSED / FAILED / WAIVED |
| 30 | validation_note | varchar(512) | YES |  | 并行对账说明或豁免原因 |
| 31 | etl_time | datetime | NO | CURRENT_TIMESTAMP | DWS v2 装载时间 |
| 32 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 33 | updated_at | datetime | NO | CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

## dws_inventory_daily_v2
- 描述: DWS v2 库存日汇总并行表，来源 `dwd_inventory_storage_snapshot`
- 状态: 用户已人工建表；已新增 `etl_dws_inventory_v2.py` dry-run / conn-test / S3 手工写入分支。当前已在用户明确授权下完成一次 S3 实跑验收：`20260507` 写入 75104 行，DWD-v2 mismatch 为 0；与旧 `dws_inventory_daily` 的 200 条同 key `qty` 差异当前按快照时点不同记录。现脚本已支持 `--source-loaded-at-cutoff` / `--align-with-old-dws`，库存 S4 对比时可先读取旧 `dws_inventory_daily.MAX(etl_time)` 作为 cutoff，再按 `date_id` 切片先删后灌重载 v2，避免更晚快照残留 key 误判为转换问题；未接入 `run_etl.py` / 总控，当前 ADS 不消费。
- 粒度: `date_id(snapshot_date) + store_id + product_id + m_productalias_id`；唯一键 `uk_dws_inventory_daily_v2_date_store_product_sku`。
- 证据: [../SQL/draft_create_dws_inventory_daily_v2.sql](../SQL/draft_create_dws_inventory_daily_v2.sql)；[../dws_v2_write_utils.py](../dws_v2_write_utils.py)；[../etl_dws_inventory_v2.py](../etl_dws_inventory_v2.py)；[../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json](../reports/context_cache/dws_v2_manual_ddl_verification_20260507.json)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 自增主键 |
| 2 | date_id | int | NO |  | 库存快照日期，YYYYMMDD，来源 dwd_inventory_storage_snapshot.snapshot_date |
| 3 | store_id | bigint | NO |  | 店仓 ID，来源 dwd_inventory_storage_snapshot.store_id |
| 4 | store_code | varchar(40) | NO |  | 店仓编码，来源 dwd_inventory_storage_snapshot.store_code |
| 5 | is_cloud_store | char(1) | NO | N | 是否云仓门店，来源 dwd_inventory_storage_snapshot.is_cloud_store |
| 6 | product_id | bigint | NO |  | 商品 ID，来源 dwd_inventory_storage_snapshot.product_id |
| 7 | m_productalias_id | bigint | NO |  | SKU / 条码 ID，来源 dwd_inventory_storage_snapshot.m_productalias_id |
| 8 | qty | decimal(18,4) | NO | 0 | 库存数量，按当前 DWS 范围标识汇总 dwd_inventory_storage_snapshot.qty |
| 9 | qty_valid | decimal(18,4) | NO | 0 | 可用库存等价候选值；M3 已剔除源侧全量无业务值 QTYVALID，第一阶段沿用 qty |
| 10 | qty_occupy | decimal(18,4) | NO | 0 | 占用数量候选值；当前生产 DWS 无源字段，第一阶段保持 0 |
| 11 | qtypurchaserem | decimal(18,4) | NO | 0 | 采购欠数 / 在途，来源 qty_purchase_rem 汇总 |
| 12 | qty_preout | decimal(18,4) | NO | 0 | 在单数量候选汇总，来源 qty_preout |
| 13 | qty_prein | decimal(18,4) | NO | 0 | 在途数量候选汇总，来源 qty_prein |
| 14 | qty_freeze | decimal(18,4) | NO | 0 | 冻结数量候选汇总，来源 qty_freeze |
| 15 | qty_oms | decimal(18,4) | NO | 0 | OMS 冻结量候选汇总，来源 qty_oms |
| 16 | qty_oms_translate | decimal(18,4) | NO | 0 | OMS 转换占用 / 调整候选汇总，来源 qty_oms_translate |
| 17 | qty_preout1 | decimal(18,4) | NO | 0 | 备用预出调整候选汇总，来源 qty_preout1 |
| 18 | source_dwd_row_count | bigint | NO | 0 | 参与聚合的 DWD 库存源行数 |
| 19 | zero_qty_row_count | bigint | NO | 0 | 参与聚合的 0 库存源行数 |
| 20 | negative_qty_row_count | bigint | NO | 0 | 参与聚合的负库存源行数 |
| 21 | min_storage_modified_at | datetime | YES |  | 参与聚合库存源行最小修改时间 |
| 22 | max_storage_modified_at | datetime | YES |  | 参与聚合库存源行最大修改时间 |
| 23 | source_min_loaded_at | datetime | YES |  | 参与聚合 raw ODS 最早装载时间 |
| 24 | source_max_loaded_at | datetime | YES |  | 参与聚合 raw ODS 最晚装载时间 |
| 25 | load_batch_id | varchar(64) | YES |  | DWS v2 装载批次 ID |
| 26 | source_layer_version | varchar(32) | NO | M3_DWD_V1 | 来源层版本标识 |
| 27 | validation_status | varchar(20) | NO | PENDING | 并行对账状态：PENDING / PASSED / FAILED / WAIVED |
| 28 | validation_note | varchar(512) | YES |  | 并行对账说明或豁免原因 |
| 29 | etl_time | datetime | NO | CURRENT_TIMESTAMP | DWS v2 装载时间 |
| 30 | created_at | datetime | NO | CURRENT_TIMESTAMP | 创建时间 |
| 31 | updated_at | datetime | NO | CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

## etl_log
- 描述: ETL执行日志表
- 说明: 日志表结构预留，当前未在代码实现写入。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | job_name | varchar(100) | NO |  | 任务名称 |
| 3 | job_type | varchar(50) | YES |  | 任务类型 |
| 4 | source_table | varchar(100) | YES |  | 源表 |
| 5 | target_table | varchar(100) | YES |  | 目标表 |
| 6 | start_time | datetime | YES |  | 开始时间 |
| 7 | end_time | datetime | YES |  | 结束时间 |
| 8 | rows_read | int | YES | 0 | 读取行数 |
| 9 | rows_written | int | YES | 0 | 写入行数 |
| 10 | status | varchar(20) | YES |  | 状态 |
| 11 | error_message | text | YES |  | 错误信息 |
| 12 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## ods_fa_storage
- 描述: ODS-实时库存表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 库存记录ID |
| 2 | c_store_id | bigint | YES |  |  |
| 3 | m_product_id | bigint | YES |  |  |
| 4 | m_productalias_id | bigint | YES |  |  |
| 5 | qty | decimal(18,4) | YES |  |  |
| 6 | qtyvalid | decimal(18,4) | YES |  |  |
| 7 | qtypurchaserem | decimal(18,4) | YES |  | 采购欠数/在途 |
| 8 | isactive | char(1) | YES |  |  |
| 9 | etl_batch_id | varchar(32) | NO |  |  |
| 10 | etl_loaded_at | datetime | YES | CURRENT_TIMESTAMP |  |

## ods_m_retail
- 描述: ODS-零售单主表
- 约束提醒: `id` 为源端业务键；新建环境执行 `SQL/create_ods_tables.sql` 时会直接创建 `uk_ods_m_retail_id`，但现网历史库若还未手工执行 `SQL/alter_ods_m_retail_enforce_unique_id.sql`，就不能默认视为已落实唯一约束。
- 装载提醒: 增量写入当前已改为“窗口清理 + 按源 `id` 替换写入”，并在模块运行时增加 MySQL 命名锁，优先从代码层降低重复装载风险。来源：[etl_ods_m_retail.py](etl_ods_m_retail.py#L46-L64)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L243-L270)；[etl_ods_m_retail.py](etl_ods_m_retail.py#L293-L331)
- 查询提醒: 若直接承接 MCP / 销售联表查询，需同步评估覆盖 `billdate`、`c_store_id`、`status`、`isactive`、`id` 的头表路径索引，而不是只保留同步相关索引。来源：[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 零售单ID |
| 2 | docno | varchar(40) | YES |  | 订单编号 |
| 3 | billdate | int | YES |  | 单据日期（YYYYMMDD） |
| 4 | c_store_id | bigint | YES |  | 店仓ID |
| 5 | oms_sourcecode | varchar(512) | YES |  | 外部来源订单号，用于达播主订单桥接 |
| 6 | tot_amt_actual | decimal(18,2) | YES |  | 实收金额 |
| 7 | tot_amt_list | decimal(18,2) | YES |  | 吊牌金额 |
| 8 | tot_qty | decimal(18,4) | YES |  | 总数量 |
| 9 | status | int | YES |  | 单据状态 |
| 10 | isactive | char(1) | YES |  | 是否有效 |
| 11 | modifieddate | datetime | YES |  | 修改时间 |
| 12 | etl_batch_id | bigint | NO | 0 | ETL批次号 |
| 13 | etl_loaded_at | datetime | YES | CURRENT_TIMESTAMP | ETL加载时间 |

## ods_m_retailitem
- 描述: ODS-零售单明细表
- 约束提醒: `id` 为源端业务键；新建环境执行 `SQL/create_ods_tables.sql` 时会直接创建 `uk_ods_m_retailitem_id`，但现网历史库若还未手工执行 `SQL/alter_ods_m_retailitem_enforce_unique_id.sql`，就不能默认视为已落实唯一约束。
- 装载提醒: 双水位增量写入当前已改为“窗口清理 + 按源 `id` 替换写入”，并在模块运行时增加 MySQL 命名锁，优先从代码层降低重复装载风险。来源：[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L47-L65)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L293-L354)；[etl_ods_m_retailitem.py](etl_ods_m_retailitem.py#L385-L423)
- 查询提醒: 若直接承接 ODS 销售联表查询，需同步评估 `m_retail_id` 连接索引与 `m_productalias_id` 过滤能力，不能只保留 `modifieddate` / `settime` 同步索引。来源：[etl_dws_sales.py](etl_dws_sales.py#L56-L63)

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  | 明细ID |
| 2 | m_retail_id | bigint | YES |  | 主表ID |
| 3 | m_product_id | bigint | YES |  | 商品ID |
| 4 | m_productalias_id | bigint | YES |  | SKU ID |
| 5 | qty | decimal(18,4) | YES |  | 数量 |
| 6 | pricelist | decimal(18,2) | YES |  | 吊牌价 |
| 7 | priceactual | decimal(18,2) | YES |  | 实收单价 |
| 8 | tot_amt_actual | decimal(18,2) | YES |  | 实收金额 |
| 9 | tot_amt_list | decimal(18,2) | YES |  | 吊牌金额 |
| 10 | modifieddate | datetime | YES |  | 修改时间（线上通道） |
| 11 | settime | datetime | YES |  | 设置时间（线下通道） |
| 12 | etl_batch_id | varchar(32) | NO |  | ETL批次号 |
| 13 | etl_loaded_at | datetime | YES | CURRENT_TIMESTAMP | ETL加载时间 |

## ods_sync_state
- 描述: ODS增量同步水位表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | table_name | varchar(64) | NO |  | 表名（如 ods_m_retailitem、ods_m_retailitem_settime） |
| 2 | last_sync | datetime | YES |  | 水位时间（按不同通道分别记录） |
| 3 | current_window_start | datetime | YES |  | 当前窗口起点 |
| 4 | current_window_end | datetime | YES |  | 当前窗口终点 |
| 5 | status | varchar(20) | YES |  | 状态（running/pending/success） |
| 6 | updated_at | datetime | YES | CURRENT_TIMESTAMP | 最近更新时间 |
| 7 | rows_written | int | YES | 0 | 最近一次写入行数 |

## ads_dabo_order_retail_bridge
- 描述: 达播订单到零售单头桥接缓存表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | source_file | varchar(255) | NO |  | 达播样本文件名 |
| 2 | main_order_id | varchar(512) | NO |  | 达播主订单编号 |
| 3 | retail_id | bigint | NO |  | 零售单ID |
| 4 | billdate | int | NO |  | 单据日期（YYYYMMDD） |
| 5 | retail_tot_amt_actual | decimal(18,2) | YES |  | 零售单头实收金额 |
| 6 | retail_status | int | YES |  | 零售单状态 |
| 7 | retail_isactive | char(1) | YES |  | 是否有效 |
| 8 | synced_at | datetime | NO | CURRENT_TIMESTAMP | 最近同步时间 |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v2.56 | 2026-07-13 | 记录 `dim_store.open_date` DDL 已由用户人工执行并完成维表刷新；同步 231 行、95 行有效日期、136 行不可用日期的只读核对结果 |
| v2.55 | 2026-07-13 | 新增 `dim_store.open_date` 字典定义，说明其来源、不可用值语义与待人工执行 DDL 边界；同步同店资格改由开业日期判定 |
| v2.54 | 2026-06-10 | 校准首页现行说明为仅保留 `ads_store_daily_report`、`ads_store_daily_subject_report`、`ads_daily_sales`，并补记退役三表已不再列入当前现网字典对象 |
| v2.53 | 2026-06-08 | 补记 `dim_report_product_rule` 已从门店日报与 ads_daily_sales 运行时商品范围中退役，当前固定排除 `147/149/150` |
| v2.52 | 2026-05-22 | 明确 `ads_store_daily_report.same_store_*` 仅纳入去年同期有销售且 `assignment_role` 不为快闪的源门店，避免 RT014 误入同店同比 |
| v2.51 | 2026-05-22 | 新增 `ads_store_daily_report` 同店同比辅助金额字段说明，并将 `yoy_rate / yoy_amt_diff` 更新为按同店辅助金额重算 |
| v2.50 | 2026-05-20 | 新增 `ads_store_daily_report.mtd_list_amt` 月累计吊牌金额字段说明，并补记对应待执行 alter 脚本与 ETL 缺列检查 |
| v2.49 | 2026-05-19 | 补记 `ads_store_daily_report` 的门店属性回退已放宽为“当日优先、否则最近历史切片（可跨月）” |
| v2.48 | 2026-05-18 | 补记 `ads_store_daily_report` 会纳入当月已配置共同考核的成员门店，并在成员门店缺少当月属性切片时回退挂靠主店属性 |
| v2.47 | 2026-05-07 | 补记 `etl_dws_inventory_v2.py` 已支持 `source_loaded_at cutoff` 自动/显式对齐与同日切片删后重灌，用于库存 S4 shadow compare 固定到旧 DWS 同一快照时点 |
| v2.46 | 2026-05-07 | 补记 DWS v2 已完成一次 S3 实跑验收：销售当前 3417 行、库存当前 75104 行，DWD-v2 mismatch 均为 0；仍未接生产调度 |
| v2.46 | 2026-06-06 | 退役 3 张销售专题 ADS，并将销售专题现行字典说明收口到保留对象 |
| v2.45 | 2026-05-07 | 补记 DWS v2 S3 手工写入分支已新增：默认 dry-run，写入需确认令牌、命名锁、事务和 DWD-v2 对账；本轮未执行真实写入且未接生产调度 |
| v2.44 | 2026-05-07 | 补记 DWS v2 dry-run / conn-test 脚本已新增但无写库入口，v2 表仍为空表且未接生产调度 |
| v2.43 | 2026-05-07 | 新增 DWS v2 两张并行表字典，记录用户已人工建表并完成空表核验，但仍未写 v2 数据、未接生产调度 |
| v2.42 | 2026-05-07 | 同步 M3 raw / DWD 旁路表已完成销售完整业务日期和库存 full raw 初始化验证，但仍未接生产调度 |
| v2.41 | 2026-04-29 | 补记 ads_sales_org_monthly.month_order_cnt 直接汇总 ads_store_daily_report.day_order_cnt，并校准 ads_sku_daily.mtd_order_cnt 为按 SKU 过滤后净额与近零容差判单 |
| v2.40 | 2026-04-29 | 补记 ads_store_daily_subject_report 的订单数字段直接承接 ads_store_daily_report，自动继承过滤后金额与近零容差口径 |
| v2.39 | 2026-04-29 | 补记 dim_report_product_rule 当前未纳入 459=餐具，并新增对应人工执行 SQL 说明 |
| v2.38 | 2026-04-27 | 将 ads_sales_org_daily、ads_daily_sales、ads_sku_daily 的口径统一到 ads_store_daily_report 权威事实，并补记历史验证不覆盖本轮新逻辑 |
| v2.37 | 2026-04-27 | 补记 ads_sales_org_monthly 已接入门店销售专题调度第六层，并同步专题调度 freshness 相关状态 |
| v2.36 | 2026-04-24 | 扩面同步达播 ADS、dim_product_attr 与部分 dim/cfg 表 comment 语义，新增 ads_dabo_order_bridge 字典并修复 dim_product_attr 注释回退根因 |
| v2.35 | 2026-04-24 | 按当前 ETL 语义同步销售主题 ADS 表备注与 area_name 注释，并修正 ads_store_daily_report.owner_name、ads_store_daily_subject_report.report_channel_type、ads_sales_org_monthly.target_year 的现网落地说明 |
| v2.34 | 2026-04-23 | 将 ads_sku_daily.attach_contribution 字段类型更新为 decimal(14,2)，并补记 2026-04-22/v2 实跑验证 |
| v2.33 | 2026-04-22 | 补记 ads_store_daily_report 待执行负责人字段 owner_name 与对应 alter 说明 |
| v2.32 | 2026-04-21 | 新增门店经营负责人当前快照、SCD2 历史与导入日志三张表字典说明 |
| v2.31 | 2026-04-17 | 补记 ads_sku_daily 已完成专题调度第五层显式重跑验证，并更新调度状态 |
| v2.30 | 2026-04-17 | 同步 ads_sku_daily 已正式写库并接入专题调度第五层，更新三张销售看板 ADS 的调度状态 |
| v2.29 | 2026-04-17 | 将 ads_sku_daily 字典更新为含 attach_contribution 的二期样板，并补记增量 alter 脚本 |
| v2.28 | 2026-04-17 | 将 ads_sku_daily 更新为含 sales_mix_pct、rank_no、trend_tag 的二期样板字典，并补记 alter 脚本 |
| v2.27 | 2026-04-16 | 新增 ads_sales_org_monthly 与 ads_sku_daily 字典，并注明当前仅完成 conn-test 验证 |
| v2.26 | 2026-04-16 | 更新 ads_daily_sales 为已完成 2026-04-15/v1 首轮样本与最小对账验证状态 |
| v2.25 | 2026-04-16 | 更新 ads_daily_sales 为当前库已建表但空表待样本验证状态 |
| v2.24 | 2026-04-15 | 新增 ads_daily_sales 字典，并将 ads_sales_org_daily 状态更新为已完成单日验证 |
| v2.23 | 2026-04-15 | 新增 ads_sales_org_daily 仓库样板字典，并标注未默认建表状态 |
| v2.22 | 2026-04-15 | 将门店日报目标导入 NAS 根目录从 月度日目标配置表 更新为 目标配置表 |
| v2.21 | 2026-04-15 | 明确 ads_daily_report 与 ads_sales_summary 为历史规划对象，并补充现网门店日报表指引 |
| v2.23 | 2026-04-29 | 补记 M3 raw ODS 与 DWD 草案对象均未落库，不纳入当前现网数据字典 |
| v2.22 | 2026-04-23 | 同步销售主题 ADS 改为 report_channel_type 明细口径，并移除 全国/全部 物理汇总行字段说明 |
| v2.20 | 2026-04-10 | 将 ads_store_daily_report 字段语义更新为最终经营实体口径，并同步主体层输入说明 |
| v2.19 | 2026-04-10 | 更新门店日报目标 NAS 命名约定为 YYYYMM考核数据配置表.xlsx，并注明导入脚本兼容历史旧文件名 |
| v2.18 | 2026-04-10 | 新增门店日报统计主体层与共同考核两张配置表字典，并补充四 sheet 模板导入说明 |
| v2.17 | 2026-04-09 | 为 ads_dabo_order_label 增加 canonical_system_order_id 与归一审计字段，并将 system_order_id 说明调整为原始值保留 |
| v2.16 | 2026-04-09 | 更新 dim_store_report_attr 说明为未变化/变更/新增/退出分类同步，而非整片切片刷新 |
| v2.15 | 2026-04-08 | 更新 dim_store_report_attr 为 15 列现网结构，并确认 report_channel_type_group 生成列已执行 |
| v2.14 | 2026-04-08 | 补充门店日报渠道细分类真值口径，并登记 report_channel_type_group 生成列为待执行 DDL |
| v2.13 | 2026-04-08 | 新增 ads_dabo_order_label 字段字典，明确统一 Excel 主线用于订单打标 |
| v2.12 | 2026-04-08 | 调整门店日报目标 NAS 文件约定为按月份分文件，并补充目录选档说明 |
| v2.11 | 2026-04-03 | 补充门店日报目标导入支持同步 dim_store_report_attr 的字典说明与默认生效日策略 |
| v2.10 | 2026-04-03 | 更新门店日报目标导入为已建表并完成首轮正式导入验证 |
| v2.9 | 2026-04-03 | 新增门店日报目标 NAS 导入脚本与日志表字典说明 |
| v2.8 | 2026-04-03 | 冻结门店日报目标 NAS 目录与固定文件命名约定 |
| v2.7 | 2026-04-03 | 明确门店日报目标配置采用 NAS 投递目录加 Python 定时扫描导入的正式路径 |
| v1.0 | 2026-02-27 | 初版数据字典 |
| v1.1 | 2026-02-28 | 标注规划表未实现状态 |
| v1.2 | 2026-02-28 | 标注dim_date未在代码实现自动生成 |
| v1.3 | 2026-02-28 | 标注dim_category/dim_channel为已实现 |
| v1.4 | 2026-02-28 | 标注year/net字段与etl_log未在代码实现写入 |
| v1.5 | 2026-03-01 | 更新dim_channel快照证据并标注未在代码实现 |
| v1.6 | 2026-03-16 | 修正 dim_category 快照证据路径 |
| v1.7 | 2026-03-18 | 补充 dim_channel ETL 来源，并标注目标库现存数据待验证 |
| v1.8 | 2026-03-18 | 将 dim_channel 店仓字段重命名为 WING_CODE 并对齐 Oracle 来源 |
| v1.9 | 2026-03-18 | 按最新快照修正 ads_inventory_health 字段顺序、可空性与默认值 |
| v2.0 | 2026-03-23 | 确认 dim_channel 目标库现存数据已与 Oracle 对齐 |
| v2.1 | 2026-03-31 | 为 ods_m_retail 补充 oms_sourcecode 字段，用于达播主订单桥接 |
| v2.2 | 2026-03-31 | 将 ods_m_retail.oms_sourcecode 扩容为 varchar(512)，兼容 Oracle 侧超长来源订单号 |
| v2.3 | 2026-03-31 | 新增 ads_dabo_order_retail_bridge，用作达播日报的 MySQL 桥接缓存 |
| v2.4 | 2026-04-02 | 为 ods_m_retail 与 ods_m_retailitem 补充主键治理与查询路径索引提醒 |
| v2.5 | 2026-04-02 | 补充 ODS 重复装载代码治理、fresh install 唯一键与现网手工治理脚本说明 |
| v2.6 | 2026-04-03 | 新增门店日报配置表与 ads_store_daily_report 字段字典 |
