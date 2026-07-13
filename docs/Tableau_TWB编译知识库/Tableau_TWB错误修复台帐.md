# Tableau TWB 错误修复台帐

> 版本：v0.1
> 日期：2026-05-08
> 适用范围：HEFANG 项目内所有通过直接编辑 / 编译 `.twb` 产生的 Tableau 重开验证报错、字段失效、渲染异常、加载阻塞问题。

## 1. 台帐定位

- 本台帐用于沉淀 Tableau `.twb` 编译后的真实报错与修复经验，尤其是“用户重开工作簿做渲染测试”阶段暴露出来的阻塞问题。
- 默认记录顺序为“最新在前”；每次遇到新的报错或阻塞，Agent 需先尝试修复，再把结果写入本台帐。
- 若问题尚未修复，也要记录“现象 + 当前假设 + 未完成项”，避免后续重复排查。

## 2. 强制记录规则

遇到以下情况时，必须追加一条记录：

1. 用户重开 `.twb` 后出现“无法完成操作”“加载无法完成”“字段无效”“工作表空白”“dashboard 不显示”等阻塞问题。
2. Agent 为修复 Tableau 报错修改了 datasource、worksheet、dashboard、window、filter、calculation、style、zone 或其它 XML 结构。
3. 同一问题虽未完全解决，但已明确排除一批错误方向或拿到新的根因证据。

每条记录至少包含：

- 触发场景
- 报错 / 现象
- 根因判断
- 修复动作
- 验证状态
- 预防动作

## 3. 滚动记录

### [2026-07-01] 销售部自动化日报旧版 若负责人月度汇总的同店同比明显高于顶部 KPI，优先检查 owner datasource 是否额外用 `same_store_mtd_sales_amt > 0` 缩窄了同店母集

- 触发场景：用户在 `销售部自动化日报-Old.twb` 中发现，顶部 `KPI05_同店同比` 为 `6.14%`，但“区域负责人月度汇总”总计行的同店同比却达到 `28.3%`，要求把旧版负责人汇总改成和 KPI 一致的口径。
- 报错 / 现象：
  1. 顶部 KPI datasource `ds_kpi_same_store_yoy_physical_live` 直接按最新报告日汇总 `same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt` 后计算同比。
  2. 门店经营明细总计行同样使用 `SUM([same_store_mtd_sales_amt]) / SUM([same_store_last_year_mtd_sales_amt]) - 1`，与 KPI 的 helper 汇总方式一致。
  3. 只有负责人月度汇总 datasource `ds_owner_monthly_yoy_live` 在 `operating_owner` CTE 中额外写了 `CASE WHEN COALESCE(a.same_store_mtd_sales_amt, 0) > 0 THEN ... ELSE 0 END`，导致“本期为 0 或负数、但去年同期有销售”的同店门店被排除在负责人辅助分子分母之外。
- 根因判断：问题不在 Tableau 总计行二次汇总，也不在 worksheet 层公式；真正的漂移来自负责人月度汇总 datasource 的 SQL。它先用 `same_store_mtd_sales_amt > 0` 缩窄了每个负责人的同店母集，再把这个已经被过滤过的 `same_store_current_amt / same_store_last_year_amt` 暴露给总计行，最终会系统性抬高同比。
- 修复动作：
  1. 先创建备份：`D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报-Old.backup_owner_summary_align_kpi_20260701_1145.twb`。
  2. 将 `销售部自动化日报-Old.twb` 中 `ds_owner_monthly_yoy_live` 的两处重复 SQL（datasource 根 relation 与 `object-graph` 副本）统一从“`CASE WHEN same_store_mtd_sales_amt > 0` 后再汇总”改为“直接 `SUM(COALESCE(a.same_store_mtd_sales_amt, 0))` 与 `SUM(COALESCE(a.same_store_last_year_mtd_sales_amt, 0))` 汇总”。
  3. 保留 worksheet 层负责人汇总公式不变，因为它本身已经是按 helper 辅助字段汇总后再计算同比；只需要把底层辅助字段恢复到与 KPI 一致的母集即可。
- 验证状态：已完成最小结构验证。修改后的 `销售部自动化日报-Old.twb` 通过 Python `ElementTree.parse()` 校验，返回 `XML_OK`；并确认旧的 `same_store_mtd_sales_amt > 0` 过滤已从负责人 datasource 中移除。尚未由用户重开 Tableau 验证前端渲染值。
- 预防动作：
  1. 以后若同屏 KPI、负责人汇总、门店明细都声称展示“同店同比”，必须同时核对三处是否共用同一批 helper 分子分母，而不是只看 worksheet 最后一层公式长得像不像。
  2. 若负责人汇总需要和总盘 KPI 保持一致，不要在 owner datasource 中追加 `same_store_mtd_sales_amt > 0` 之类的二次筛选；这会排掉本期为 0 或负数但去年同期有效的门店，造成总计同比虚高。

### [2026-06-22] 销售部自动化日报 若日销售趋势图从某天开始持续低于 KPI，且缺口集中在共同考核快闪门店，优先排查 `ads_daily_sales` 是否遗漏 `joint_assessment_member_scope`，不要先改 worksheet 过滤

- 触发场景：用户在 `销售部自动化日报.twb` 重开渲染测试中指出，`销售趋势分析_日销售趋势` 从 `2026-06-16` 起缺少北京国贸快闪、从 `2026-06-19` 起再缺少广州天环快闪；但顶部 `KPI01_日销售额` 正常。
- 报错 / 现象：
  1. 趋势图 6/21 柱体显示 `568,403`，而 KPI 同日报显示 `613,706`。
  2. 只读核对 `ads_daily_sales` 后，趋势图展示值与表内 `day_actual_amt` 完全一致，说明不是 Tableau 柱图渲染丢值。
  3. 继续对比 `ads_store_daily_report`，`2026-06-16` 到 `2026-06-21` 的日销售缺口分别为 `12349 / 19507 / 30517 / 68475 / 36592 / 45303`。
  4. 用 `ods_m_retail + ods_m_retailitem` 只读汇总 `RT014`、`RT140` 后，逐日金额精确等于上述缺口，证明漏的是快闪源门店流水，不是经营体主店流水。
- 根因判断：根因不在 `.twb` 的 worksheet filter，而在 `etl_ads_daily_sales.py` 的 source scope。修复前该脚本只从 `cfg_store_target_daily` 提取 `target_store_scope`，再直接基于该范围构建 `store_entity_map`；共同考核快闪成员门店没有单店目标时不会进入该范围，因此 RT014、RT140 的真实流水在 `ads_daily_sales` 中整段缺失。相比之下，`ads_store_daily_report` 已经显式把 `joint_assessment_member_scope` 纳入 `source_store_scope`，所以 KPI 仍然正确。
- 修复动作：
  1. 在 `etl_ads_daily_sales.py` 中新增 `joint_assessment_member_scope`、`joint_assessment_anchor_scope`、`store_attr_scope` 与 `source_store_scope`。
  2. 将 `store_entity_map` 的输入从 `store_scope` 改为 `source_store_scope`，并保留“成员门店属性缺失时回退挂靠主店属性”的取值方式。
  3. 去掉 `assignment_candidates` 对 `target_store_scope` 的硬限制，确保没有单店目标的共同考核成员门店也能建立主体映射。
  4. 新增 `test_ads_sales_scope_alignment.py` 回归断言，锁定上述四个 scope 片段和 `FROM source_store_scope sss` 不能回退。
- 验证状态：已完成代码级最小验证。`python -m unittest -v test_ads_sales_scope_alignment.py` 通过；只读 SQL 已确认趋势图差额逐日精确等于 `RT014/RT140` 实际流水。尚未做真实写库重跑，仍需用户人工执行 ETL 后重开 Tableau 验证前端恢复。
- 预防动作：
  1. 后续若同屏 KPI 与趋势图不一致，先确认两者读的是不是同一张 ADS；若不是，优先对比 ADS 粒度与 source scope，而不是先改 Tableau 公式。
  2. 涉及共同考核的 ADS，只要消费事实流水，就要显式检查是否把 `joint_assessment_member_scope` 纳入源门店范围；只看目标门店范围通常会漏快闪成员店。

### [2026-06-16] 伯俊Oracle数据建模 若 sales.csv 与 calendar.csv 已改成 `BILL_DATE_ID = date_id` 仍报关系类型不一致，需同时检查 workbook 内嵌 SQL 运算符污染和 `date_id` metadata 是否被 Tableau 推断成 `real`

- 触发场景：用户在 `工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb` 中已经把日期关系改成 `sales.csv.BILL_DATE_ID = calendar.csv.date_id`，但 Tableau 数据模型页仍提示日期关系有问题，并在关系编辑器中显示类型不一致。
- 报错 / 现象：
  1. relationship XML 已经是 `[BILL_DATE_ID] = [date_id]`，说明关系键本身没有再连回旧的 `[BILL_DATE] = [date]`。
  2. workbook 内嵌 `calendar.csv` Custom SQL 仍残留 `<<`、`>>`、`<<=`，`sales.csv` 仍残留 `<<>>`，说明用户同步外部 SQL 后，`.twb` 内实际加载的两份 relation 副本并没有完全收口。
  3. `metadata-records` 中 `[sales.csv].[BILL_DATE_ID]` 的 `local-type` 是 `integer`，但 `[calendar.csv].[date_id]` 被 Tableau 记录成 `real`，即使两边都来自 Oracle number，也会直接触发关系类型不一致。
  4. 即使把 `date_id` 的 `local-type` 修到 integer，如果 datasource 根列定义里的 `[date_id]` 仍然是 `role='measure' type='quantitative'`，Tableau 关系编辑器仍可能把它当成无效输入，而不是关系键维度。
- 根因判断：这类报错通常不是单一关系表达式写错，而是两层问题叠加：第一层是 `.twb` 内嵌 Custom SQL 的 relation collection 和 `object-graph` 副本没有同步修干净；第二层是 `calendar.csv` 用 `TO_NUMBER(TO_CHAR(calendar_date,'YYYYMMDD')) AS date_id` 这类无精度 number 表达式时，Tableau 可能把 surrogate key 推断成 decimal/real，而 `BILL_DATE_ID` 来自源表整数列，会被识别成 integer。
- 根因判断：这类报错通常不是单一关系表达式写错，而是三层问题叠加：第一层是 `.twb` 内嵌 Custom SQL 的 relation collection 和 `object-graph` 副本没有同步修干净；第二层是 `calendar.csv` 用 `TO_NUMBER(TO_CHAR(calendar_date,'YYYYMMDD')) AS date_id` 这类无精度 number 表达式时，Tableau 可能把 surrogate key 推断成 decimal/real，而 `BILL_DATE_ID` 来自源表整数列，会被识别成 integer；第三层是即使数值类型已一致，若根列定义仍把 `date_id` 标成 measure，Tableau 关系编辑器也可能继续报输入错误。
- 修复动作：
  1. 先创建备份：`工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.backup_date_relation_fix_20260616_*.twb`。
  2. 将 `.twb` 中两份 `calendar.csv` relation 的 `date_id`、`last_year_same_date_id`、`prev_month_same_date_id` 全部改成 `CAST(TO_NUMBER(... ) AS NUMBER(8,0))`，从 SQL 源头把日期代理键收口为整数。
  3. 将 `.twb` 中 `sales.csv` / `calendar.csv` 残留的 `<<>>`、`<<`、`>>`、`<<=` 分别修回 `<>`、`<`、`>`、`<=`，避免 Tableau 重开或刷新时继续命中坏 SQL。
  4. 将 `.twb` 的 `metadata-records` 中 `[calendar.csv].[date_id]` 的 `local-type` 从 `real` 改为 `integer`，让当前 workbook 立刻与 `[sales.csv].[BILL_DATE_ID]` 对齐。
  5. 将 datasource 根列定义中的 `[date_id]` 从 `role='measure' type='quantitative'` 改为 `role='dimension' type='ordinal'`，与已正常工作的 `store_id (stores.csv)` / `sku_id (products.csv)` 键字段保持一致。
  6. 同步修改外部文件 `SQL/【伯俊建模】日期维表SQL.sql`，避免用户下次再次把外部 SQL 同步进 `.twb` 时把问题重新带回。
- 验证状态：已完成最小结构与只读验证。`.twb` 经 PowerShell XML 解析返回 `XML_PARSE_OK`；检索确认 workbook 内不再残留 `<<>>` / `<<` / `>>` / `<<=`；`[calendar.csv].[date_id]` 的 metadata 已改为 `integer`；Oracle 只读执行新的 `CAST(... AS NUMBER(8,0))` 日期键写法返回样本 `20180101 / 20170101 / 20171201`。仍需用户重开 Tableau，确认关系红叹号消失。
- 验证状态：已完成最小结构与只读验证。`.twb` 经 PowerShell XML 解析返回 `XML_PARSE_OK`；检索确认 workbook 内不再残留 `<<>>` / `<<` / `>>` / `<<=`；`[calendar.csv].[date_id]` 的 metadata 已改为 `integer`；根列定义中的 `[date_id]` 已改为 `role='dimension' type='ordinal'`；Oracle 只读执行新的 `CAST(... AS NUMBER(8,0))` 日期键写法返回样本 `20180101 / 20170101 / 20171201`。仍需用户重开 Tableau，确认关系红叹号消失。
- 预防动作：
  1. Tableau 中若用户反馈“关系表达式已经改对，但仍提示类型不一致”，先同时检查 relation XML、Custom SQL 实体文本和 metadata-record 的 `local-type`，不要只看关系线。
  2. Oracle 自定义 SQL 中凡是要作为关系键使用的 surrogate key，优先显式 `CAST(... AS NUMBER(8,0))`，不要直接裸用 `TO_NUMBER(...)`。
  3. 对带 `object-graph` 副本的 `.twb` datasource 改 Custom SQL 时，根 relation 与副本 relation 必须双写一致；否则外部 `.sql` 文件是对的，Tableau 实际加载的内嵌 SQL 仍可能是旧版本。
  4. 关系键除了数值类型一致外，datasource 根列定义也应尽量保持 dimension/ordinal 语义；若某个键字段仍被注册成 measure，Tableau 关系编辑器可能继续报输入错误。

### [2026-06-12] HEFANG门店实时销售战情看板若门店明细页“同店本期月销”仍显示快照值，且顶部 KPI 与负责人/明细的滞后门店数不一致，优先检查 Measure Names 实例链路与 time_progress 时间源是否统一

- 触发场景：用户要求继续修复 `HEFANG门店实时销售战情看板.twb`，明确要把门店明细页的“同店本期月销”显示列从旧快照实例切到实时辅助字段，并把顶部 KPI 与负责人/明细的时间进度统一成同一来源。
- 报错 / 现象：
  1. `实时战情_门店实时销售明细` 中，`月达成率` 和 `同店同比` 已经走实时辅助字段，但“同店本期月销”显示列仍挂在旧的 `[sum:mtd_sales_amt:qk]` 上，导致界面继续展示快照月累计。
  2. 顶部 `进度落后门店数` 使用 Oracle `LAST_STATUSTIME` 计算营业时间进度，而负责人汇总与门店明细使用 `NOW()`，同一时刻会出现顶部与下方门店计数不一致。
- 根因判断：
  1. 只改 Tableau calculation 还不够；在 `Measure Names / Multiple Values` 结构下，真正控制显示列的是 alias、column-instance、groupfilter、manual-sort 这整条实例链路。
  2. owner realtime datasource 没有把 `LAST_STATUSTIME` 暴露到 worksheet，本轮之前负责人汇总与门店明细只能退回到 `NOW()` 口径。
- 修复动作：
  1. 在 `ds_owner_realtime_summary_live` 的 Oracle `Realtime Sales` SQL 中补出 `LAST_STATUSTIME`，并同步补齐 cols map、metadata-record、datasource 根字段定义。
  2. 将 owner datasource、负责人汇总 worksheet、门店明细 worksheet 中的 `time_progress` 公式统一改成基于 `{ FIXED : MAX([LAST_STATUSTIME]) }` 的营业时间进度。
  3. 将门店明细页的“同店本期月销”从旧快照实例 `[sum:mtd_sales_amt:qk]` 切到实时辅助实例 `[usr:Calculation_202606121801:qk]`，并同步更新 Measure Names alias、groupfilter、manual-sort 与 sheet 内 column-instance。
- 验证状态：已完成最小结构与只读结果验证。`xml.etree.ElementTree.parse()` 返回 `XML_PARSE_OK`；检索确认门店明细页 Measure Names 已切到 `[usr:Calculation_202606121801:qk]`；用修改后的 twb SQL 只读复算，`LATEST_STATUS=2026-06-12 16:41:37` 下顶部 `KPI_LAGGING=44`、负责人口径 `OWNER_LAGGING=44`，计数已对齐。仍需用户重开 Tableau，确认前端渲染与显示文本符合预期。
- 预防动作：
  1. 以后修 Tableau 明细表里的某个显示列时，不能只看 calculation，要同时核对 `Measure Names` 的 alias、column-instance、filter、manual-sort 是否还指向旧实例。
  2. 同屏若同时展示 KPI、负责人汇总、门店明细的“达成率 / 线性进度 / 领先滞后门店数”，必须先统一 `time_progress` 的时间源，否则即使数据源相同也会出现肉眼可见的计数漂移。

### [2026-06-12] HEFANG门店实时销售战情看板若门店实时销售明细页的“度量名称”筛选器出现红感叹并提示无效，优先检查是否同时保留了同名的快照字段和实时替代字段

- 触发场景：用户重开 `HEFANG门店实时销售战情看板.twb` 后，在 `实时战情_门店实时销售明细` 页看到 `度量名称` 筛选器红感叹，悬浮提示“错误: 度量名称 上的筛选器无效”。
- 报错 / 现象：
  1. `实时战情_门店实时销售明细` 页的 `Measure Names` 筛选器失效，导致整张表无法正常展示。
  2. 左侧数据窗格中同时存在两组 caption 相同的字段：底层快照字段 `[mtd_sales_amt] / [last_year_mtd_sales_amt]` 与新的实时替代字段 `Calculation_202606121801 / Calculation_202606121802` 都显示为“同店本期月销_辅助 / 同店去年同期月销_辅助”。
  3. 本轮实时口径 patch 虽然已经把公式切到实时辅助字段，但 Tableau 仍可能把 `Measure Names` 成员解析到重复 caption 的旧快照字段上。
- 根因判断：根因不是实时口径公式再次写坏，而是 Tableau 的 `Measure Names / Measure Values` 视图中同时暴露了“同名快照字段”和“同名实时替代字段”，导致 `度量名称` 筛选器成员解析出现字段歧义。
- 修复动作：
  1. 保留实时替代字段 `Calculation_202606121801 / Calculation_202606121802` 的 caption 为业务展示名“同店本期月销_辅助 / 同店去年同期月销_辅助”。
  2. 将底层快照字段 `[mtd_sales_amt] / [last_year_mtd_sales_amt]` 在 datasource 根定义、区域负责人汇总 worksheet 和门店实时销售明细 worksheet 中统一改名为“同店本期月销_快照辅助 / 同店去年同期月销_快照辅助”。
  3. 不改动实时口径公式本身，只通过 caption 去歧义，避免 `Measure Names` 再次混淆旧快照字段与新实时字段。
- 验证状态：已完成最小结构验证。Python `xml.etree.ElementTree.parse()` 解析工作簿返回 `XML_PARSE_OK`；并检索确认当前 `.twb` 中实时辅助字段 caption 与快照辅助字段 caption 已区分。仍需用户重开 Tableau，确认 `实时战情_门店实时销售明细` 页的 `度量名称` 不再红感叹。
- 预防动作：
  1. 以后在 Tableau 的 `Measure Names / Measure Values` 视图里新增替代字段时，不要让底层快照字段与替代展示字段共用同一 caption。
  2. 如果必须保留原始字段供公式引用，优先把原始字段显式标成“快照 / 原始 / 底稿”，再让新字段承接业务展示名。
  3. 用户重开后若只出现 `Measure Names` 红感叹而非整库报错，先查字段 caption 歧义，再查公式本身。

### [2026-06-12] HEFANG门店实时销售战情看板若门店实时销售明细表的“同店本期月销_辅助 / 月达成率 / 同店同比”仍停在 D-1，不要只改 worksheet 公式；要把 owner realtime datasource 改成“快照月累计基线 + 当日实时增量 + 去年同日同刻实时增量”

- 触发场景：用户在修完免税 / 小程序问题后，继续指出 `HEFANG门店实时销售战情看板.twb` 的 `实时战情_门店实时销售明细` 中，`同店本期月销_辅助`、`月达成率`、`同店同比` 仍然不是实时口径，而是“月累计截至最新实时数据，而不是 D-1；同店同比也是”。
- 报错 / 现象：
  1. 明细表 caption 明写“月累计与月目标取最新日报快照”，与用户要求的实时口径相冲突。
  2. `同店同比` 与 `月达成率` 虽然挂在 Tableau calculation 上，但底层都吃的是 `[mtd_sales_amt]` / `[last_year_mtd_sales_amt]` 这组快照字段。
  3. `ds_owner_realtime_summary_live` 的 `Realtime Sales` relation 只返回当日 `DAY_SALES_AMT`，没有去年同日同刻的实时增量，导致同比分子分母无法完整对齐。
- 根因判断：根因不是 worksheet 表层公式写错，而是 federated datasource `ds_owner_realtime_summary_live` 混用了两套时间语义：MySQL `Owner Scope` 从 `ads_store_daily_report` 最新快照直接取 `mtd_sales_amt / last_year_mtd_sales_amt / same_store_yoy`，Oracle `Realtime Sales` 只补了“今天实时销售额”。结果就是“日销售实时、月累计和同比仍停在 D-1”。
- 修复动作：
  1. 先创建备份：`工作簿/HEFANG门店实时销售战情看板.backup_realtime_mtd_alignment_20260612_01.twb`。
  2. 在 `ds_owner_realtime_summary_live` 的 Oracle `Realtime Sales` SQL 中新增 `latest_status` 与 `last_year_hourly_sales` CTE，补出 `LAST_YEAR_DAY_SALES_AMT`，即“去年同月同日截至当前最新实时点”的销售额。
  3. 保留 MySQL 快照月累计作为基线，不直接改 `Owner Scope`；新增 Tableau 计算字段 `Calculation_202606121801/202606121802`，分别把“本期月累计 / 去年同期月累计”改成“快照累计 + 实时当日增量”。
  4. 将 `实时战情_区域负责人实时汇总` 和 `实时战情_门店实时销售明细` 中的 `同店同比`、`月达成率` 与明细表月累计列切换到新的实时辅助字段；同步把明细表 caption 改成“月累计按最新日报快照 + 当日实时流水补齐至当前最新数据，同店同比按去年同月同日截至同一实时点重算”。
- 验证状态：已完成最小结构验证。Python `xml.etree.ElementTree.parse()` 解析工作簿返回 `XML_PARSE_OK`；并检索确认 `.twb` 中已不再残留明细表旧的 `sum:mtd_sales_amt:qk` 引用、旧版 `IF SUM(IF [last_year_mtd_sales_amt] > 0 THEN [mtd_sales_amt] ...` 同比公式，以及“月累计与月目标取最新日报快照”旧说明文案。仍需用户重开 Tableau，确认明细表与 owner 汇总表的月累计 / 月达成率 / 同店同比都已按实时口径展示。
- 预防动作：
  1. 以后凡是“实时看板里某些字段看着像实时、其实还是 D-1”这类问题，默认同时检查 worksheet caption、worksheet local calculation、federated MySQL scope SQL 和 Oracle realtime relation，不能只看表层公式。
  2. 如果实时同比要对齐到“截至当前最新数据”，不要只给分子补当日实时额；还要同步补去年同月同日截至同一实时点的增量，否则会出现“分子实时、分母停在昨天”的半对齐口径。
  3. 对带 `object-graph` 副本的 `.twb` datasource 改 SQL 时，根 relation 与副本 relation 必须双写一致；否则 Tableau 可能继续加载旧 SQL。

### [2026-06-12] HEFANG门店实时销售战情看板若“今日0销售门店数 / 进度落后门店数”同时多出 8 家免税且“小程序”仍出现在渠道图，不能只修顶部 KPI 公式；还要同步清理 owner 计数、Oracle 实时 SQL 的 `store_id=96` 硬编码和 worksheet 显式成员过滤

- 触发场景：用户重开前先按截图反馈，指出 `HEFANG门店实时销售战情看板.twb` 顶部两张红框卡片都多算了 8 家免税门店，同时说明“小程序现在不进入这个看板”。
- 报错 / 现象：
  1. “今日0销售门店数”“进度落后门店数”都把 8 家 `联营-免税` 门店算进去了。
  2. 负责人表“领先门店数 / 滞后门店数”与顶部 KPI 同源，若不同步修复会出现同屏口径不一致。
  3. 当前有效日报范围里并没有小程序门店，但渠道销售贡献图和实时 datasource 仍残留“小程序”入口。
- 根因判断：根因不是单一公式错误，而是 `.twb` 内同时存在三层旧口径残留：
  1. 顶部 KPI 与 owner 汇总计数字段没有按 `report_channel_type` 排除“免税”。
  2. 多个 Oracle 实时 SQL 仍把 `store_id=96` 硬编码映射为“小程序”，导致看板继续吞入已退出范围的门店。
  3. `实时战情_渠道销售贡献图` worksheet 里还保留了显式 `member='小程序'` 和 manual sort bucket。
- 修复动作：
  1. 先创建备份：`工作簿/HEFANG门店实时销售战情看板.backup_exclude_duty_free_and_mini_program_20260612_01.twb`。
  2. 在 `ds_oracle_realtime_store_kpi_live` 的 target scope query 中补出 `report_channel_type` 字段，并将顶部“今日0销售门店数 / 进度落后门店数”改为仅统计 `report_channel_type` 不含“免税”的门店。
  3. 将 `ds_owner_realtime_summary_live` 的“领先门店数 / 滞后门店数”同步改为排除“免税”，避免 owner 表和顶部 KPI 口径漂移。
  4. 从 `ds_realtime_cum_progress_target_live`、`ds_owner_realtime_summary_live`、`ds_oracle_realtime_store_kpi_live`、`ds_oracle_realtime_store_hourly_live` 的 Oracle 实时 SQL 硬编码列表中移除 `store_id=96`，并删掉 `WHEN r.C_STORE_ID IN (96) THEN '小程序'`。
  5. 从 `实时战情_渠道销售贡献图` 的 categorical filter 和 manual sort bucket 中移除“小程序”显式成员。
- 验证状态：已做最小结构验证，两次使用 Python `xml.etree.ElementTree.parse()` 解析工作簿均返回 `XML_OK`；并检索确认 `.twb` 中不再残留 `WHEN r.C_STORE_ID IN (96)`、旧 `store_id=96` 实时门店清单，以及渠道贡献图中的“小程序”显式 member / bucket。仍需用户重开 Tableau，确认顶部两个门店计数各减少 8，且渠道贡献图不再出现“小程序”。
- 预防动作：
  1. 以后凡是实时看板出现“门店数多 / 少一批固定渠道门店”的问题，先用只读 SQL 确认 `dim_store_report_attr` 当前有效范围真值，不要先信 `.twb` 里的硬编码列表。
  2. 修这类范围问题时，固定联查四层：维表真值、federated scope query、Oracle 实时 SQL 硬编码、worksheet 显式 member / sort；任意一层没清理干净，用户重开后都可能看到旧渠道残留。
  3. 若同屏存在顶部 KPI 与 owner 汇总表，默认把两者视为联动口径，不能只修上面不修下面。

### [2026-06-11] SKU 生命周期看板若将事实层做成聚合表（按 date/store/sku group by）再去连维表，关系编辑器容易出现输入异常或后续口径漂移；应改回明细事实并补齐 stores 维关系

- 触发场景：用户要求“先完全复刻 HEFANG经营数据看板-全域版 的数据源建模”，并指出当前建模思路需要回到最细粒度 fact + conformed dimensions。
- 报错 / 现象：
  1. 先前 `sales_sku_daily` 使用聚合 SQL（含 `COUNT/SUM/GROUP BY`），与 `calendar_dim/sku_dim` 建关系后稳定性差，且难以与全域版口径对齐。
  2. 当前模型缺少 `stores` 维表节点，`store_id` 只能停留在事实表字段，无法形成与参考模型一致的星型关系。
- 根因判断：根因是逻辑模型层级与参考工作簿不一致：参考模型是“明细事实 sales + calendar + products + stores（再可扩展 cfg snapshot）”，而目标模型把事实先聚合后再关联维表，导致关系与口径都更脆弱。
- 修复动作：
  1. 先创建备份：`工作簿/SKU生命周期看板项目/SKU生命周期分析看板.backup_replicate_datasource_model_20260611_155458.twb`。
  2. 将 `自定义 SQL 查询1`（datasource 根 relation + object-graph 副本）从聚合 SQL 改为明细事实 SQL（`sale_id/retail_item_id/date/date_id/store_id/sku_id/product_id/units/line_actual_amt/...`）。
  3. 新增 `自定义 SQL 查询3` 作为 stores 维表 SQL，并在 datasource 与 object-graph 同步落盘。
  4. 补齐 `stores.csv` 逻辑表节点、`[store_id (自定义 SQL 查询3)]` 字段映射与 metadata-record。
  5. 新增关系：`[store_id] = [store_id (自定义 SQL 查询3)]`，形成 sales->stores 关系链。
- 验证状态：已做最小结构验证：PowerShell XML 解析返回 `XML_OK`；仍需用户重开 Tableau 验证模型页无红叹号且关系可编辑。
- 预防动作：
  1. 复刻参考 workbook 建模时，优先先对齐“逻辑层拓扑”（事实/维度节点与关系），不要先在事实层做预聚合。
  2. 所有 datasource SQL 改动必须双写到 datasource 根 relation 与 object-graph 副本，避免“看似已改、实际加载旧 SQL”的漂移。
  3. 每次改关系后固定执行两步最小验证：关系表达式检索 + XML parse 校验。

### [2026-06-11] SKU 生命周期看板中若 `sales_sku_daily -> calendar_dim` 使用日期键关系出现“关系的某个输入中存在错误”，可改为 `date_id` 关系键规避日期字段关系解析异常

- 触发场景：用户重开 `工作簿/SKU生命周期看板项目/SKU生命周期分析看板.twb` 后，在数据模型页看到 `sales_sku_daily -> calendar_dim` 关系红叹号，关系编辑器提示“关系的某个输入中存在错误”。
- 报错 / 现象：
  1. 关系线可见，但 `sales_sku_daily` 与 `calendar_dim` 的日期键关系在编辑器中报输入错误。
  2. 关系表达式原本为 `[date (自定义 SQL 查询1)] = [date]`。
- 根因判断：在该 workbook 的当前状态下，`date` 字段关系键存在关系解析不稳定问题（字段可见但关系输入报错）；同源的整数键 `date_id` 元数据完整且与业务日期同粒度，可作为更稳的关系键。
- 修复动作：
  1. 先创建备份：`工作簿/SKU生命周期看板项目/SKU生命周期分析看板.backup_calendar_relation_fix_20260611_153350.twb`。
  2. 将 relationship 表达式从 `[date (自定义 SQL 查询1)] = [date]` 改为 `[date_id (自定义 SQL 查询1)] = [date_id]`。
  3. 保持 `sales_sku_daily -> sku_dim` 的 `sku_id` 关系不变，避免扩大改动面。
- 验证状态：已用 Python `ElementTree.parse()` 校验 XML，结果 `XML_OK`；仍需用户重开 Tableau 确认红叹号消失。
- 预防动作：
  1. 遇到关系编辑器“输入错误”且日期键疑似不稳定时，优先检查是否存在同粒度稳定 surrogate key（如 `date_id`）可替代关系键。
  2. 修改 relationship 后至少做两步最小验证：目标表达式检索确认 + `ElementTree.parse()` 结构校验。

### [2026-06-11] SKU 生命周期看板若 workbook 内嵌 Custom SQL 的比较运算符被写坏成 `>>` / `<<` / `<<=`，或 relationship 表达式落成空 `[] = []`，Tableau 数据模型页会出现红叹号，关系字段下拉框也会空白

- 触发场景：用户在 `工作簿/SKU生命周期看板项目/SKU生命周期分析看板.twb` 中按 3 张 Oracle Custom SQL 逻辑表建模后，反馈 `sales_sku_daily -> sku_dim` 关系无法匹配，Tableau 关系编辑器里左右字段下拉为空，并出现红色感叹号。
- 报错 / 现象：
  1. 工作簿磁盘上的 `sql/01_calendar_dim.sql` 与 `sql/04_sales_sku_daily.sql` 本身是合法 Oracle SQL，但 `.twb` 内嵌的同名 Custom SQL 副本出现 `>>`、`<<`、`<<=`。
  2. `object-graph` 下 `sales_sku_daily -> sku_dim` 的 relationship XML 被写成 `<expression op='[]' /> = <expression op='[]' />`，导致关系键实际为空。
  3. 这两类问题叠加后，Tableau 虽然还能显示逻辑表节点，但无法正常解析字段元数据，关系编辑器中的匹配字段列表会空白。
- 根因判断：根因不在 `calendar_dim` 的建模思路，而在 `.twb` 内部存在两层漂移：一层是 datasource / object-graph 中重复保存的 Custom SQL 文本被错误替换了比较运算符；另一层是 `sales_sku_daily` 与 `sku_dim` 的 relationship 表达式没有真正落下 `[sku_id] = [sku_id (自定义 SQL 查询2)]`，而是空表达式。
- 修复动作：
  1. 先创建备份：`工作簿/SKU生命周期看板项目/SKU生命周期分析看板.backup_relationship_fix_20260611_150907.twb`。
  2. 将 `.twb` 中 datasource 根 relation 与 `object-graph` 副本里的错误比较运算符统一修回合法 Oracle 写法：`>`、`<`、`<=`。
  3. 将 `sales_sku_daily -> sku_dim` 的 relationship 表达式补回 `[sku_id] = [sku_id (自定义 SQL 查询2)]`。
  4. 修复后用 Python `ElementTree.parse()` 验证工作簿 XML，结果 `XML_OK`。
- 验证状态：已检索确认 `.twb` 内不再残留 `>>` / `<<` / `<<=` 和空 `[]` relationship；并已完成 XML 解析校验，结果 `XML_OK`。仍需用户重开 Tableau，确认关系编辑器可以正常选到 `sku_id` 字段且红叹号消失。
- 预防动作：
  1. 以后只要用户反馈“关系字段下拉空白 / 红叹号 / 逻辑表能看到但字段选不到”，先同时检查 `.twb` 的 datasource relation、副本 `object-graph` relation 和 relationship 表达式，不要只盯着外部 `.sql` 文件。
  2. 对含 Custom SQL 的 `.twb` 做人工 patch 后，必须至少做两步最小验证：全文检索非法比较运算符与空 relationship，再做一次 `ElementTree.parse()`。
  3. 若同一逻辑表的 SQL 在外部 `.sql` 文件和 `.twb` 内嵌 XML 中同时存在，以 Tableau 实际加载的 `.twb` 内嵌 SQL 为准排错，因为字段元数据读取依赖的是后者。

### [2026-06-10] 9.门店对比表 若把黑色“上月销售额”定位点误接成 `diff:usr:Calculation_0225680113684482:qk` 差分实例，会把原本的双轴上月销售额标记变成按行差分轴，进而出现 90+ NULL 提示与右侧渲染异常

- 触发场景：用户重开 `HEFANG经营数据看板-全域版.twb` 时反馈 `9.门店对比表` 中本应展示上月销售额的黑色三角标记异常，右下角出现 `90+ null` 提示后视图表现失真。
- 报错 / 现象：
  1. 列架顶部对应黑色三角的 pill 不再是单纯的 `聚合(1.上月销售额)` 语义，而是挂到了 `diff:usr:Calculation_0225680113684482:qk`。
  2. `pane id='5'` 的 `x-axis-name`、对应 axis `space` 编码以及 `<cols>` 中的第五列，都引用了这个 Difference table calculation 实例。
  3. 该实例会沿 `Rows` 对 `1.上月销售额` 做相邻门店差分，而不是把每家门店自己的上月销售额当成定位点，因此会引入大量无意义空值，并把黑色定位三角从“上月销售额双轴标记”扭曲成“门店间差分标记”。
- 根因判断：问题不在 datasource 缺数，也不在 `1.上月销售额` 公式本身，而在 worksheet XML 被误接到了差分 column-instance。`9.门店对比表` 的正确结构应直接使用 `usr:Calculation_0225680113684482:qk` 作为黑色三角轴；一旦切成 `diff:` 派生实例，Tableau 会把该列按表计算结果解释，导致空值激增和标记位置异常。
- 修复动作：
  1. 删除该 worksheet local `datasource-dependencies` 中的 `diff:usr:Calculation_0225680113684482:qk` column-instance 定义。
  2. 将 axis style 中针对该列的 `display` / `space` 编码从 `diff:` 改回 `usr:Calculation_0225680113684482:qk`。
  3. 将 `pane id='5'` 的 `x-axis-name` 从差分实例改回原始 `usr:Calculation_0225680113684482:qk`。
  4. 将 `<cols>` 中第五列的列架表达式同步改回 `usr:Calculation_0225680113684482:qk`。
  5. 修复前已按规则创建备份：`HEFANG经营数据看板-全域版.backup_fix_prev_month_marker_20260610_01.twb`。
- 验证状态：已完成 Python `ElementTree.parse()` 语法校验，结果 `XML_OK`；并检索确认目标工作簿内已不存在 `diff:usr:Calculation_0225680113684482:qk`。仍需用户重开 Tableau，确认 `9.门店对比表` 中黑色上月销售额标记恢复，右下角不再提示 `90+ null`。
- 预防动作：
  1. 对双轴“定位点 / 参考点”类标记做 XML patch 时，优先核对 `pane x-axis-name` 是否仍直接指向原始 measure instance，不要误接到 `Difference`、`Running Sum` 之类的 table calculation 派生实例。
  2. 如果用户反馈“某个点位列本来是参考值标记，但突然出现大量 null”，优先搜索该 worksheet 是否新增了 `diff:`、`running:` 等 column-instance，而不是先怀疑源数据断档。
  3. 修改 `<cols>` 时要和 pane、axis style 一起联动检查；同一列若三处引用不一致，很容易出现 Tableau 能打开但视图标记逻辑错位的情况。

### [2026-05-22] 给 Parameters datasource 增加 `default-value-field` 时，若引用的 calculation 只存在于 worksheet local `datasource-dependencies` 而不存在于 datasource 根定义，Tableau 重开可能直接把 worksheet 判成“没有有效数据源”

- 触发场景：用户要求把 `销售趋势分析_日销售趋势` 改成参数窗口方案后，重开 `销售部自动化日报.twb` 时弹出警告，提示“工作表包含错误。会删除以下内容：工作表没有有效数据源”。
- 报错 / 现象：
  1. 出错 worksheet 是 `销售趋势分析_日销售趋势`。
  2. workbook XML 级别仍能正常解析，但 Tableau 客户端在打开 worksheet 时认为整张图没有有效数据源。
  3. 本轮改动里，`AxisStart / AxisEnd` 参数都使用了 `default-value-field='[federated.1eddfye0w1v4zc1ffmccc01ghpvs].[Calculation_1730010000000303/0304]'`，而这两个 calculation 当时只补在了该 worksheet 的 local `datasource-dependencies` 中。
- 根因判断：Tableau 在解析 Parameters datasource 的 `default-value-field` 时，会去对应 datasource 的根字段注册表中找目标 field。若该 field 仅存在于某个 worksheet 的 local `datasource-dependencies`，而不在 datasource 根定义中，Tableau 会把参数默认值引用判成失效，从而连带把依赖该参数的 worksheet 判成“无有效数据源”。
- 修复动作：
  1. 保留 `AxisStart / AxisEnd` 参数方案不回退。
  2. 在 `ds_ads_daily_sales` 根 datasource 中补齐 `Calculation_1730010000000303`（参数默认轴起点_首页日销趋势）和 `Calculation_1730010000000304`（参数默认轴终点_首页日销趋势）。
  3. 使 Parameters datasource 与 worksheet local `datasource-dependencies` 的 `default-value-field` 都指向同一套 root-level calculation，避免参数默认值“有引用、无根注册”。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；并检索确认 `Calculation_1730010000000303/0304` 已同时存在于 datasource 根定义与 `销售趋势分析_日销售趋势` 本地依赖中。仍需用户继续重开 Tableau，确认该 worksheet 不再弹出“没有有效数据源”。
- 预防动作：
  1. 以后给 Parameters datasource 写 `default-value-field` 时，先确认目标 calculation 已注册在对应 datasource 根定义里，不要只写 worksheet local 副本。
  2. 如果 Tableau 报“worksheet 没有有效数据源”，但 XML parse 正常，优先检查参数默认值、字段注册表和 datasource 根/本地副本是否漂移，而不要先怀疑连接本身。
  3. 对 workbook 做“参数化改造”时，新增 calculation 应遵循“先 root，后 local”的顺序，避免 local 能看到、Parameters 看不到。

### [2026-05-22] 销售日报趋势图若继续沿用连续日期轴 `none:sales_date:qk`，即使已用 calc 过滤到当月，也仍可能自动补出 4/29、4/30、5/22、5/23 这类边界日期；更稳的修法是“参数窗口 + 离散日期列架”

- 触发场景：用户重开 `销售部自动化日报.twb` 时，`销售趋势分析_日销售趋势` 明明已经限定为当前月且不超过最新报告日，但横轴仍反复出现 `4/29 4/30`、`5/22 5/23` 这类边界标签；前一轮尝试通过 fixed range 和半天留白修补，用户多次重开后仍不满意，因此明确要求“换个方法，用参数来实现”。
- 报错 / 现象：
  1. 即使 `sales_date` 已通过 calc 过滤为“当前月且不超过最新报告日”，连续日期轴仍会按自己的刻度策略在首尾自动留白，并显示轴外日期标签。
  2. 直接改 `major-origin`、`min/max`、半天内边距虽然能局部缓解，但容易在“去掉边界标签”和“保留首尾空隙”之间反复拉扯，重开后观感不稳定。
  3. 对这张销售趋势图来说，真正的业务需求是“只显示参数窗口内的日期头”，并不是必须坚持连续日期轴。
- 根因判断：问题根因不在筛选逻辑，而在 Tableau 对连续日期轴 `none:sales_date:qk` 的自动补边界刻度机制。只要横轴还是 continuous exact date，首尾就可能出现超出数据窗口的日期标签；fixed range 只能做静态折中，不能从结构上阻断这个行为。
- 修复动作：
  1. 保留并升级 Parameters datasource：新增 `AxisStart` 参数，`AxisEnd` 改为动态默认值参数；两者分别绑定 `ds_ads_daily_sales` 内新增的“参数默认轴起点/终点” calculation。
  2. 将 `销售趋势分析_日销售趋势` 的窗口过滤 calc 从“当前月且不超过最新报告日”改为 ` [sales_date] >= [Parameters].[AxisStart] AND [sales_date] <= [Parameters].[AxisEnd] `，让日期窗口明确由参数驱动。
  3. 为 `sales_date` 补离散 exact date 实例 `none:sales_date:ok`，并将 worksheet 的 `<cols>` 从 `none:sales_date:qk` 切换为 `none:sales_date:ok`。
  4. 同步把日期标签格式和 tooltip 中的销售日期 token 改为离散日期实例，移除这张图上针对 continuous date 轴的 `space` 编码；并清空之前临时加上的 on-select 参数动作，避免点击图表后误改参数窗口。
- 验证状态：已完成 workbook XML 解析校验，结果 `XML_OK`；并已确认 `AxisStart` / `AxisEnd` 参数、参数窗口过滤公式、`none:sales_date:ok` 列架都已写入 `销售部自动化日报.twb`。仍需用户继续重开 Tableau，确认横轴只显示参数窗口内的离散日期，不再自动补边界日期标签。
- 预防动作：
  1. 后续若用户的真正诉求是“只显示某个日期窗口内的日级标签”，优先考虑“参数窗口 + 离散 exact date 列架”，不要先陷入 continuous axis 的 min/max 微调。
  2. 只有在确实需要连续时间尺度、缩放或不规则时间间距时，才继续用 `none:*:qk`；否则 exact date discrete 更稳定，也更符合日报看板的阅读习惯。
  3. 若 workbook 中已经存在参数 datasource，不要额外硬编码 fixed range；优先把窗口边界收敛为参数默认值或参数动作，避免同一张图同时存在“calc 过滤、参数窗口、静态 axis range”三套边界来源。

### [2026-05-21] 当中文学习数据已经把 `product_name` / `product_category` 值汉化后，若 `[Parameter 5]` / `[Parameter 6]` 仍保留英文默认值与成员列表，`PRODUCT PERFORMANCE` 右侧产品对比模块会整体失效

- 触发场景：用户重开 `Retail Toy Store 学习版.twb` 后，`OVERVIEW` 已恢复，`STORE ANALYTICS` 与 `PRODUCT PERFORMANCE` 左半部分也能显示，但右侧产品对比区域仍不工作。
- 报错 / 现象：
  1. `12. 产品销售趋势` 顶部标题仍显示英文产品名，折线区域为空。
  2. `14. 品类内产品趋势` 的品类标题仍显示英文 `Games`，图层无法按中文品类正确筛选。
  3. `15. 卡片-*` 四张卡片里的“所选产品销售额/销量”持续为 0，表现为 `0 / 共 35`。
- 根因判断：当前中文学习数据已经把 `product_name`、`product_category` 的值层汉化为 `魔方`、`游戏` 等中文，但 workbook 内 `[Parameter 5]` 仍默认 `Rubik's Cube`，`[Parameter 6]` 仍默认 `Games`，且各自 `<members><member value=...>` 列表仍保留英文。相关 worksheet 的核心公式都依赖 `[product_name] = [Parameters].[Parameter 5]`、`[product_category] = [Parameters].[Parameter 6]`，因此参数语言与数据值不一致时，会把整个产品对比链路比较成 false / 0。
- 修复动作：
  1. 修改 `tools/rewire_toys_town_twb_to_chinese_csv.py`，新增参数字符串编解码与成员本地化逻辑，统一扫描整棵 workbook 中所有 `[Parameter 5]` 与 `[Parameter 6]` 节点。
  2. 将产品参数默认值与成员从英文产品名批量映射到中文值，例如 `Rubik's Cube -> 魔方`；将品类参数默认值与成员从英文品类批量映射到中文值，例如 `Games -> 游戏`。
  3. 同步把 `[none:product_category:nk]` 色板中的英文 bucket 改成中文 bucket，避免中文数据命中后颜色映射退回默认配色。
  4. 重新执行改线脚本覆盖 `Retail Toy Store 学习版.twb`。
- 验证状态：已检索确认 workbook 内 `[Parameter 5]` 默认值与多处 worksheet-local 副本都已变为 `&quot;魔方&quot;`，`Rubik's Cube` / `Games` / `Art & Crafts` / `Electronics` / `Sports & Outdoors` / `Toys` 等影响中文值匹配的主要英文字面量已清空；XML 解析校验结果为 `XML_OK`。仍需用户继续重开 Tableau，确认右侧三组产品对比模块恢复出数。
- 预防动作：
  1. Tableau 中文学习版如果把数据值域汉化，除了固定成员筛选外，参数默认值、参数 member 列表、色板 bucket 也都属于“值域契约”的一部分，必须同步迁移。
  2. 若页面标题仍显示英文参数值，而数据表内容已经是中文，优先检查参数节点本身，不要先怀疑 datasource 失效。
  3. 用脚本批量改 `.twb` 参数时，要兼容 ElementTree 解析后的带双引号字符串，不要只匹配字面量 `&quot;...&quot;`。

### [2026-05-21] 当中文学习数据把 `store_location` 值汉化后，Overview 里仍写死 `Airport` / `Residential` 成员筛选会把整组 KPI 与趋势图整体筛空

- 触发场景：用户重开 `Retail Toy Store 学习版.twb` 后，`STORE ANALYTICS` 与 `PRODUCT PERFORMANCE` 已恢复，但 `OVERVIEW` 页顶部 KPI、趋势图和下方模块仍大面积空白。
- 报错 / 现象：
  1. `1. 销售额KPI`、`1. 销售额趋势`、`3. 利润KPI`、`4. 利润率KPI`、`5. 日销售趋势` 等 Overview worksheet 均仍存在固定的 `store_location` 成员筛选。
  2. 这些筛选在 workbook XML 中写死为 `Airport`、`Residential`，而当前中文学习数据 `stores_zh.csv` 中 `store_location` 实际值已变成 `机场店`、`居民区店`。
  3. 结果是数据源可连通、字段也有效，但 Overview 相关 worksheet 被统一过滤成 0 行，所以 dashboard 看起来像“连接正常但页面空白”。
- 根因判断：这次问题不在 datasource connection，也不在字段契约，而在 workbook 内部固定筛选值没有随中文数据值一起迁移。Overview 大量 worksheet 复用了同一组 `store_location` 成员过滤器，因此一处值域漂移会导致整页模块同步空白。
- 修复动作：将 `Retail Toy Store 学习版.twb` 中所有针对 `[none:store_location:nk]` 的固定成员筛选，从 `Airport` / `Residential` 批量替换为 `机场店` / `居民区店`。
- 验证状态：已检索确认 workbook 内不再残留 `member="&quot;Airport&quot;"` / `member="&quot;Residential&quot;"`；中文成员筛选已落盘 28 处，且 XML 解析校验结果为 `XML_OK`。仍需用户继续重开 Tableau，确认 `OVERVIEW` 页恢复出数。
- 预防动作：
  1. 除了 field contract 外，任何 workbook 内写死的成员筛选值也属于“数据契约”的一部分；做值汉化时必须同步排查这些固定 member。
  2. 如果出现“其它页正常，只有某个 dashboard 整组模块空白”，优先搜索该页所有 worksheet 的 `filter class="categorical"` 和 `member=`，判断是不是值域被整体筛空。

### [2026-05-21] 将已经绑定英文 field contract 的 Tableau workbook 直接改连“中文表头 CSV”，会触发辅助表红叹号、逻辑表字段缺失和 dashboard 局部空白；正确做法是保留英文物理键列，只汉化数据值与展示 caption

- 触发场景：用户在重开 `Retail Toy Store 学习版.twb` 做 CSV 脱离 Hyper 验证时，发现辅助表 datasource 出现红色感叹号，主销售源的 `Sales` 物理表只剩少量字段，`OVERVIEW` 页大面积空白，而 `PRODUCT PERFORMANCE`、`STORE ANALYTICS` 仅部分模块能出数。
- 报错 / 现象：
  1. 辅助表 `auxiliar_buttons_zh.csv` 在 Tableau 数据源页里同时出现中文物理字段和英文失效字段，`Button Text`、`Switch`、`Value` 标红。
  2. 主 datasource 虽然已经切到 CSV live connection，但 `cols/map` 被改写为 `[sales].[日期]`、`[products].[产品名称]` 这类中文 remote 字段路径，与 workbook 内原有 `[date]`、`[product_name]`、`[store_id]` 的 field contract 不一致。
  3. 中文 mock 数据文件 `sales_zh.csv`、`products_zh.csv`、`stores_zh.csv`、`auxiliar_buttons_zh.csv` 的表头被翻成了中文，导致 Tableau workbook 里依赖英文 remote-name 的逻辑层无法稳定匹配，最终表现为字段缺失、物理表字段变少、页面局部空白。
- 根因判断：这次问题不是 `textscan` 连接本身，也不是 CSV 行数不足，而是把“中文化”做到了物理表头层。该 workbook 的 datasource、`metadata-records`、`cols/map`、worksheet 本地依赖都已经固化为英文 field contract；一旦直接把 CSV header 改成中文，再把 `cols/map` 一并改成中文 remote 字段名，Tableau 就会把同一 datasource 同时看成“旧英文字段失效 + 新中文物理字段孤立存在”，从而出现红叹号与空白页。
- 修复动作：
  1. 修改 `tools/generate_toys_town_mock_source_data.py`，将 `mock_source_data_zh-CN` 调整为“英文表头 + 中文值”模式：保留 `sale_id/date/store_id/product_id/units`、`product_name/product_category`、`store_name/store_city/store_location`、`Button Text/Value/Switch` 等英文键列，只把产品名、门店名、城市、位置类型和按钮文本值汉化。
  2. 修改 `tools/rewire_toys_town_twb_to_chinese_csv.py`，将主 datasource 的 `cols/map` 恢复为 `[sales].[date]`、`[products].[product_name]`、`[Custom SQL Query].[store_name]` 等英文 remote 字段路径，同时把辅助表 `<relation><columns>` 恢复为 `Button Text / Value / Switch`。
  3. 重新生成 `mock_source_data_zh-CN/*.csv`，并重新执行改线脚本覆盖 `Retail Toy Store 学习版.twb`。
- 验证状态：已完成脚本 `py_compile` 校验、中文 CSV 重生成、workbook 重刷；抽样确认 `auxiliar_buttons_zh.csv` 表头已恢复为 `Button Text,Value,Switch`，`sales_zh.csv` 表头已恢复为 `sale_id,date,store_id,product_id,units`，且 `Retail Toy Store 学习版.twb` 中辅助表列定义与主 datasource `cols/map` 已同步恢复英文 field contract；XML 解析校验结果为 `XML_OK`。仍需用户继续重开 Tableau，确认红叹号消失且 `OVERVIEW` 页恢复出数。
- 预防动作：
  1. 对已有 Tableau workbook 做“中文 CSV 替换”时，默认只汉化数据值和展示 caption，不要改物理表头，除非准备同步重写整套 `metadata-records`、`cols/map`、worksheet 本地依赖和所有 remote-name 引用。
  2. 若重开后出现“辅助表红叹号 + 主逻辑表字段突然变少 + dashboard 局部空白”三联症，优先检查 CSV header 是否被翻译，以及 `cols/map` 是否仍指向 workbook 原始英文 remote 字段。
  3. 中文目录、中文文件名可以保留；真正需要稳定的，是 CSV 内部 header 与 workbook field contract 的一致性。

### [2026-05-21] 同一张 KPI dashboard 若 `线性进度偏差` 文本字段在 relationship 模型里重新用 `day_sales_amt / day_target` 裸算达成率，而不是复用旁边已验证正确的 `日达成率` measure，会出现卡片间数值自相矛盾

- 触发场景：用户在重开 `HEFANG门店实时销售战情看板.twb` 后反馈，顶部 `今日达成率` 卡片显示 `5.8%`、`营业时间进度` 显示 `14.44%`，但 `线性进度偏差` 却显示 `-4.30pp`，与“今日达成率 - 营业时间进度”的文案不一致。
- 报错 / 现象：
  1. `KPI02_日达成率` 与 `KPI04_月累计达成率` 都绑定 `ds_oracle_realtime_store_kpi_live`，但同屏数值互相对不上。
  2. `KPI04_月累计达成率` 的本地 `线性进度偏差文本_实时战情` 直接在 Text calc 内写 `ROUND(((SUM([day_sales_amt]) / SUM([day_target])) - MIN([time_progress])) * 100, 2)`。
  3. 在 Tableau relationship 模型下，这种“在文本字段里重新裸算跨逻辑表比值”的写法，可能与单独展示的 `日达成率_实时战情` 产生不同的聚合语义，导致用户看到 `5.8%` 与 `-4.30pp` 这种明显冲突的组合。
- 根因判断：本次问题不在 Oracle 实时数据、也不在营业时间进度来源，而在 workbook 内部 calculation 复用策略。`线性进度偏差文本_实时战情` 没有复用已经显示正确的 `日达成率_实时战情`，而是在 Text calc 中重新用 `day_sales_amt / day_target` 裸算一次，触发 relationship 聚合漂移。
- 修复动作：
  1. 将 datasource 根 calculation `Calculation_202605141701` 改为基于 `[Calculation_202605140512] - [Calculation_202605141551]` 生成 pp 文本，不再直接写 `SUM([day_sales_amt]) / SUM([day_target])`。
  2. 将 `KPI04_月累计达成率` worksheet local `Calculation_202605141701` 改为基于本地 `日达成率_实时战情` 与 `time_progress` 生成文本。
  3. 在 `KPI04_月累计达成率` 的本地 `datasource-dependencies` 中补入 `日达成率_实时战情` 字段依赖，强制该卡片复用与 `KPI02_日达成率` 相同的达成率 measure。
- 验证状态：已完成 PowerShell XML 解析校验，结果 `XML_OK`；仍需用户继续重开 Tableau 或手动刷新，确认 `线性进度偏差` 已回到与 `今日达成率 - 营业时间进度` 一致的数值区间。
- 预防动作：
  1. 在 Tableau relationship 模型里，如果某个 KPI 文案本质上是“已展示 measure 之间的差值 / 拼接文本”，优先直接复用已验证正确的 measure，不要在 Text calc 中重新裸算底层分子分母。
  2. 遇到“同一 datasource、同屏 KPI 互相对不上”的问题时，优先检查是不是某张卡片在本地 `datasource-dependencies` 里重新展开了跨逻辑表计算，而不要先怀疑源库数据错。
  3. Text worksheet 的字段链排查要同时看根 calculation 和 worksheet local `datasource-dependencies`，避免只改 root 后遗漏本地副本。

### [2026-05-21] 左上角实时额与达成率 / 明细若来自两条不同的 realtime SQL 实现路径，即使都标记为 Live，也可能在同屏出现“小时累计正确、日达成率和门店明细严重放大”的口径漂移

- 触发场景：用户在重开 `HEFANG门店实时销售战情看板.twb` 后反馈，10:45 左右左上角 `今日实时销售额` 只有几千元，但 `今日达成率` 却显示 `60.8%`，底部 `门店实时销售明细` 中还出现多家门店单店实时销售过万，明显与当前时点不符。
- 报错 / 现象：
  1. `KPI01_日销售额` 与 `实时战情_分时销售` 使用的 `ds_oracle_realtime_store_hourly_live` 给出的当日实时累计值，与 Oracle 直查一致。
  2. 同一时点下，`KPI02_日达成率` 使用的 `ds_oracle_realtime_store_kpi_live` 与 `实时战情_门店实时销售明细` 使用的 `ds_owner_realtime_summary_live` 却显示出远高于实际的日销售与达成率。
  3. 只读 Oracle 复算证据显示：2026-05-21 10:51 时，当日小时销售仅 `09 点 = 553`、`10 点 = 4684`，累计 `5237`；同日目标总额为 `461640.48`，正确实时达成率约为 `1.13%`。同一批 Oracle 实时流水里，单店最高实时销售也仅约 `2680`，与明细表里动辄过万的显示不一致。
- 根因判断：本次问题已可确认不在源库真实实时流水，而在 workbook 内部 realtime datasource 实现路径分叉。左上角实时额 / 分时图走的是按小时累计的 hourly live SQL，KPI 与门店明细走的是另一条独立的日汇总 realtime SQL；当这两条路径在 Tableau 客户端出现结果漂移时，用户就会看到“左上角几千元，但达成率和明细像全天数据”的同屏矛盾。
- 修复动作：
  1. 将 `ds_oracle_realtime_store_kpi_live` 的 Oracle `Realtime Sales` SQL 改写为与 hourly live 同口径的 `hourly_sales` 累计结构：先按小时聚合当天流水，再按门店汇总成当日累计，并保留 `LAST_STATUSTIME`。
  2. 将 `ds_owner_realtime_summary_live` 的 Oracle `Realtime Sales` SQL 同步改为相同的 `hourly_sales -> realtime_sales` 结构，避免门店明细继续走另一条日汇总路径。
  3. 两套 datasource 均把时间过滤统一为 `STATUSTIME BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE + 1) - (1 / 86400)`，并同步修改 datasource 根 relation 与 `object-graph` 副本，强制 Tableau 重走新的 realtime SQL 文本。
- 验证状态：已完成 PowerShell 原生 XML 解析校验，结果 `XML_OK`；并完成 Oracle / MySQL 只读复算，确认 2026-05-21 10:51 时正确口径应为 `日销售=5237`、`日目标=461640.48`、`日达成率≈1.13%`。仍需用户继续重开 Tableau 或手动刷新，确认 KPI02 与门店明细已回到与左上角一致的实时口径。
- 预防动作：
  1. 同一张 realtime dashboard 内，若用户会拿 `今日实时销售额`、`日达成率`、`门店实时销售明细` 互相肉眼对账，就不要让它们分别依赖不同实现路径的 realtime SQL；优先复用同一套 hourly cumulative 逻辑。
  2. 看到“小时趋势正确，但日达成率 / 门店明细明显像全天数据”时，应先直接 Oracle 只读复算小时销售与当日累计，先判断是源库异常还是 workbook 内部 datasource 漂移。
  3. 对 `.twb` 中的实时 datasource 做修复时，不只改一处根 relation；必须同步修改 `object-graph` 副本，并在修复后做 XML 校验与源库复算对照。

### [2026-05-19] 若实时 KPI / 明细直接消费“只有当日 Oracle 流水的门店集合”，而不先用权威门店清单做脚手架左联，则无流水门店会被漏算成缺行，顶部 0 销售 / 落后门店数会明显偏小，底部实时销售额则表现为空白

- 触发场景：用户在重开 `HEFANG门店实时销售战情看板.twb` 验收“门店清单统一到 dim_store_report_attr”后，发现顶部 `今日0销售门店数` 仅为 `1`、`进度落后门店数` 仅为 `15`，但底部 `实时战情_门店实时销售明细` 中 `实时销售额` 为空白的门店明显远多于 1 家。
- 报错 / 现象：
  1. `KPI06_今日0销售门店数` 与 `KPI08_总月标` 使用的 `ds_oracle_realtime_store_kpi_live` 明显少算无成交门店。
  2. `实时战情_门店实时销售明细` 使用的 `ds_owner_realtime_summary_live` 中，多家门店 `实时销售额` 为空白而不是 `0`。
  3. 只读复算证据显示：2026-05-19 14:23:20 时权威门店清单共 73 家，但 Oracle 当日实时聚合仅返回 41 家门店，缺失 32 家无流水门店；同时仅有 1 家门店 `RT098 重庆时代天街店` 在实时聚合结果内显式出现 `DAY_SALES_AMT = 0`。
- 根因判断：问题不在 KPI 公式本身，而在 datasource 的 Oracle Custom SQL 只返回“当天有流水行的门店”。在 Tableau relationship 语义下，`ZN([day_sales_amt_raw])` 只能把“已存在但为空”的度量补成 0，不能把“压根没有实时行”的门店补出来，因此：
  1. 顶部 KPI 只把实时聚合里显式存在的 `0` 额门店算进 `今日0销售门店数`；
  2. 完全没有实时行的门店不会进入 KPI 计数；
  3. 在明细表里同一批门店会表现成 `实时销售额` 空白，而不是可参与汇总的 `0`。
- 修复动作：
  1. 将 `ds_oracle_realtime_store_kpi_live` 的 Oracle `Realtime Sales` SQL 改为：先用 73 家权威门店 `store_scope` 脚手架，再左联实时聚合结果，并对 `DAY_SALES_AMT` 使用 `NVL(..., 0)`，保留 `LAST_STATUSTIME`。
  2. 将 `ds_owner_realtime_summary_live` 的 Oracle `Realtime Sales` SQL 同步改为相同模式，确保明细表中的无流水门店也能落成 `0`，不再只是空白缺行。
  3. 同步修改 datasource 根 relation 与 `object-graph` 中的重复 relation 副本，避免 Tableau 读取不同副本时再次漂移。证据位于主工作簿 `HEFANG门店实时销售战情看板.twb` 中 owner summary datasource 的 Oracle SQL 与 KPI datasource 的 Oracle SQL，两处都已改成 `store_scope + LEFT JOIN realtime_sales` 结构。
- 验证状态：已完成 PowerShell 原生 XML 解析校验，结果 `XML_OK`；并完成只读跨库复算，确认旧口径下 `ACTIVE=73 / SALES_ROWS=41 / ABSENT_SALES_ROWS=32 / PRESENT_ZERO_COUNT=1`。仍需用户继续重开 Tableau，确认顶部 KPI 与底部明细已刷新为“权威门店清单口径”。
- 预防动作：
  1. 后续只要实时看板需要统计“0 销售门店数 / 落后门店数 / 无成交门店明细”，就不能直接消费 fact-only 的 Oracle 实时聚合结果，必须先构造权威 `store_scope` 脚手架，再左联实时事实。
  2. 在 Tableau relationship 模型中，`ZN()` 只能补空值，不能补缺行；看到“明细空白远大于 KPI 计数”时，应优先怀疑 datasource 没把无流水门店物化出来。
  3. 修改 datasource relation 后，必须同步检查根 relation 与 `object-graph` 副本，并至少做一次 XML 校验与一轮只读复算，避免“结构合法但业务口径仍缺行”。

### [2026-05-15] 若同一张实时战情 dashboard 的页头时间卡仍绑定 `ds_oracle_realtime_store_kpi_live`，而核心 `今日实时销售额` KPI 已切到 `ds_oracle_realtime_store_hourly_live`，用户会看到“数据截至”明显落后于同屏实时销售额

- 触发场景：用户在 Tableau 客户端继续验证 `HEFANG门店实时销售战情看板.twb` 的实时性时，发现同一屏里 `今日实时销售额` 已刷新到更高金额，但页头 `数据截至` 仍停在较早时间点，例如截图中金额已更新而 `数据截至` 仍显示 `2026-05-15 14:46`。
- 报错 / 现象：
  1. `页头_信息摘要` 显示的 `数据截至` 不随同屏 `KPI01_日销售额` 一起更新，肉眼可见滞后。
  2. `页头_时间进度卡` 的营业时间进度也会跟着停在旧时点，因为它和 `数据截至` 共用同一条 header 时间链路。
  3. 直接查 Oracle 实时流水可拿到更晚的 `LAST_STATUSTIME`，说明不是源库没有新数据，而是 worksheet 绑定链路分叉。
- 根因判断：这次问题不在累计 SQL，也不在 Oracle 实时流水本身，而在 workbook 内部 datasource 绑定不一致。`KPI01_日销售额` 实际已改用 `ds_oracle_realtime_store_hourly_live`，但 `页头_信息摘要` 与 `页头_时间进度卡` 仍停留在 `ds_oracle_realtime_store_kpi_live`。结果同一张 dashboard 上的“金额”和“数据截至/营业时间进度”来自两条不同的 realtime datasource，用户就会看到 header 时间比核心 KPI 更旧。
- 修复动作：
  1. 将 `页头_信息摘要` 的 worksheet datasource 从 `ds_oracle_realtime_store_kpi_live` 切到 `ds_oracle_realtime_store_hourly_live`。
  2. 将 `页头_时间进度卡` 的 worksheet datasource 同步切到 `ds_oracle_realtime_store_hourly_live`。
  3. 保留两张 Text worksheet 现有的 aggregate helper 形态不变，只替换 `datasource`、本地 `datasource-dependencies`、`encodings` 与 `customized-label` 中的 federated datasource 前缀，避免再次触发 “用户定义聚合需要再聚合” 的 header 空白问题。
- 验证状态：已完成 PowerShell 原生 XML 解析校验，结果 `XML_OK`；仍需用户继续重开 Tableau，确认 `数据截至` 与 `营业时间进度` 已跟随 hourly live 链路刷新到最新交易时间。
- 预防动作：
  1. 后续在同一张 realtime dashboard 中，只要用户会拿“页头数据截至”和“核心实时 KPI”做肉眼对照，就必须先核对两者是否绑定同一条 datasource，而不要只看 calculation 名称都叫 `LAST_STATUSTIME`。
  2. 若某个 KPI 已被验证为更实时的权威链路，header 时间卡与营业进度卡应默认跟随同一条链路，避免 dashboard 内部出现“金额刷新了，但数据截至没刷新”的感知分叉。
  3. 修改 Text worksheet 的 datasource 时，不只要改 `<datasource ...>`，还要同步检查本地 `datasource-dependencies`、`encodings` 和 `customized-label` token，否则客户端仍可能继续引用旧 federated 前缀。

### [2026-05-15] 在 Text crosstab 中若把 `FIXED` / LOD helper 直接作为 `:Measure Names` 成员暴露，并再用它驱动行维度排序，Tableau 重开后会同时报“度量名称筛选器无效”和“门店编码排序无效”

- 触发场景：为 `HEFANG门店实时销售战情看板.twb` 新增底部 `实时战情_门店实时销售明细` 模块后，用户在 Tableau 客户端打开 worksheet，发现 `度量名称` pill 变红并提示“度量名称上的筛选器无效”，同时 `门店编码` pill 也变红并提示“门店编码上的排序无效”。
- 报错 / 现象：
  1. `实时战情_门店实时销售明细` worksheet 中，`cols` 上的 `:Measure Names` 过滤器失效，整张表无法正常出数。
  2. 行维度 `门店编码` 的排序配置失效，Tableau 直接把该 pill 标红。
  3. 这两个报错都指向同一条展示 measure：直接暴露在视图里的 `usr:Calculation_202605151302:qk`。
- 根因判断：`Calculation_202605151302` 本质上是 `FIXED [store_id], [owner_name] : MIN(ZN([day_sales_amt_raw]))` 的 LOD helper。把这类 helper 直接注册为 `Measure Names` 成员，并再拿它做 text crosstab 的 measure filter 与 computed-sort 依赖时，当前这本 HEFANG 实时战情 workbook 的 Tableau 客户端会把它判成无效的展示 measure，最终导致同一张表上的 `:Measure Names` 过滤器和行排序一起失效。
- 修复动作：
  1. 保留 `Calculation_202605151302` 作为辅助字段，但将 caption 改为 `实时销售额_辅助`，不再直接把它暴露到 `Measure Names`。
  2. 新增聚合展示字段 `Calculation_202605151321 = SUM([Calculation_202605151302])`，由它承接 `实时销售额` 的列展示。
  3. 将 `实时战情_门店实时销售明细` 中 `Measure Names` 的 filter、manual-sort、列宽引用统一从 `usr:Calculation_202605151302:qk` 切换到 `usr:Calculation_202605151321:qk`。
  4. 移除该 worksheet 上对 `门店编码` 的 computed-sort，先恢复稳定打开和渲染。
- 验证状态：已完成 PowerShell 原生 XML 解析校验，结果 `XML_OK`；仍需用户继续重开 Tableau，确认 `实时战情_门店实时销售明细` 已恢复渲染且不再出现红色 pill。
- 预防动作：
  1. 后续在 HEFANG 这本实时战情 `.twb` 里做 Text crosstab 时，若某个 measure 来自 `FIXED` / LOD helper，优先拆成“辅助 helper + 聚合展示字段”两层，不要直接把 helper 本身挂进 `:Measure Names`。
  2. 若新 worksheet 同时出现“度量名称筛选器无效”和“行维度排序无效”，优先检查两者是否共同依赖了同一个 user measure instance，而不要先怀疑数据源断连。
  3. 在新表首次重开验证通过前，先避免给多层 rows 的明细 crosstab 叠加 computed-sort；优先保证 view 能稳定加载，再补排序策略。

### [2026-05-15] 页头“信息摘要 / 时间进度卡”若继续使用 `aggregation=true + usr:Calculation` 挂载方式，则切到 `LAST_STATUSTIME` 后的 header 计算必须保持 aggregate 形态，否则整张 worksheet 会因“用户定义聚合需要再聚合”而空白

- 触发场景：修复 `ds_realtime_cum_progress_target_live` 后，用户重开 `HEFANG门店实时销售战情看板.twb`，发现页头信息摘要和时间进度卡整块不显示；Tableau 在 `刷新时间文本_实时战情` 与 `营业时间进度_实时战情` 上提示“需要对非聚合公式的用户定义聚合”。
- 报错 / 现象：
  1. `页头_信息摘要` worksheet 中 `刷新时间文本_实时战情` 变红，整张摘要卡空白。
  2. `页头_时间进度卡` worksheet 中 `营业时间进度_实时战情` 以及依赖它的进度条字段一起变红，整张时间卡空白。
  3. 两张 worksheet 都保留 `aggregation value='true'`，并通过 `usr:Calculation_*` 实例直接挂到 Text mark 上。
- 根因判断：上一轮把 header 两个 calculation 从原先可聚合的写法，改成了直接返回 `{ FIXED : MAX([LAST_STATUSTIME]) }` 的非聚合 LOD 形态；但这两张 Text worksheet 仍然沿用 `aggregation=true + usr:Calculation` 的挂载方式，导致 Tableau 在渲染时把它们视为“需要进一步聚合的用户定义聚合”，从而整张 worksheet 失效。
- 修复动作：
  1. 将 `刷新时间文本_实时战情` 改为显式聚合写法：所有 `LAST_STATUSTIME` 引用统一包成 `MIN({ FIXED : MAX([LAST_STATUSTIME]) })`。
  2. 将 `营业时间进度_实时战情` 同步改为显式聚合写法：用 `MIN({ FIXED : MAX([LAST_STATUSTIME]) })` 参与 `IF / ELSEIF / DATEDIFF` 计算。
  3. 同步修正 datasource 根部定义和 `页头_信息摘要`、`页头_时间进度卡` 两张 worksheet 的本地 `datasource-dependencies` 副本，保证公式一致。
- 验证状态：已完成 Python XML 解析校验，结果 `XML_OK`；仍需用户继续重开 Tableau，确认两张页头 worksheet 已恢复渲染。
- 预防动作：
  1. 后续凡是 HEFANG 当前这本实时战情工作簿里采用 `aggregation=true + usr:Calculation` 的 Text worksheet，若 root calculation 切到 LOD / FIXED 逻辑，必须同时检查该 calculation 是否仍保持 aggregate 形态。
  2. 不要把原本可渲染的聚合 helper 直接改成裸 `FIXED` 结果；若需要引用 LOD，优先在外层显式包 `MIN(...)` 或其它合法聚合。
  3. 修改 datasource 根 calculation 后，务必同时搜索 worksheet 本地 `datasource-dependencies` 中的同名副本，避免根定义与本地定义漂移。

### [2026-05-15] `ds_realtime_cum_progress_target_live` 的 Oracle Custom SQL 若同时出现 `&lt;=` 和后续 `&gt;=`，Tableau 会把两者之间整段误识别成参数名，并报 `F024F6FE`

- 触发场景：用户在重开 `HEFANG门店实时销售战情看板.twb` 时，Tableau 报“无法完成操作”，提示连接到数据源 `ds_realtime_cum_progress_target_live` 出现问题，错误码 `F024F6FE`，并显示“自定义 SQL 关系引用了不存在的参数: = 15 ... AND ABS(ri.TOT_AMT_ACTUAL) ...”；同时伴随“无法连接到 Oracle 服务器 8.134.9.203”的通用连接失败提示。
- 报错 / 现象：
  1. 报错文本并不是指向真实参数名，而是把 `= 15` 到后续 `ABS(ri.TOT_AMT_ACTUAL)` 之间的大段 SQL 当成了“参数”。
  2. 出错数据源是 `ds_realtime_cum_progress_target_live` 的 Oracle Custom SQL 关系 `Hourly Sales`。
  3. Tableau 同时抛出 Oracle 连接失败提示，但首要阻塞点其实是 Custom SQL 在 Tableau 侧预解析阶段就已经失败。
- 根因判断：Tableau Custom SQL 会把尖括号包裹的内容按参数占位处理。该数据源 SQL 中先出现 `CONNECT BY LEVEL &lt;= 15`，后面又出现 `ABS(ri.TOT_AMT_ACTUAL) &gt;= 1` 与 `r.STATUSTIME &gt;= TRUNC(SYSDATE)`；Tableau 在解析时会把第一处 `<` 到后续某个 `>` 之间的整段文本误当成参数名，因此报“引用了不存在的参数”。
- 修复动作：
  1. 将 `CONNECT BY LEVEL &lt;= 15` 改写为 `CONNECT BY LEVEL BETWEEN 1 AND 15`。
  2. 将 `ABS(ri.TOT_AMT_ACTUAL) &gt;= 1` 改写为 `SIGN(ABS(ri.TOT_AMT_ACTUAL) - 1) IN (0, 1)`。
  3. 将 `r.STATUSTIME &gt;= TRUNC(SYSDATE)` 改写为 `r.STATUSTIME BETWEEN TRUNC(SYSDATE) AND TRUNC(SYSDATE + 1) - (1 / 86400)`。
  4. 同步修正 datasource 根节点 relation 与 `object-graph` 中的重复 `Hourly Sales` relation，避免 Tableau 读取不同副本时状态不一致。
- 验证状态：已对修复后的 `HEFANG门店实时销售战情看板.twb` 执行 Python XML 解析校验，结果 `XML_OK`；Tableau 客户端重开后的真实连接恢复情况仍需用户继续确认。
- 预防动作：
  1. 后续在 Tableau `.twb` 的 Custom SQL relation 中，默认避免直接写 `&lt;` / `&gt;` 比较符组合，优先改写为 `BETWEEN`、`IN`、`SIGN(...)`、`=` 等无尖括号写法。
  2. 一旦 Custom SQL 报“引用了不存在的参数”，优先检查是否存在 `<...>` 误触发，而不要先把问题误判成数据库真有缺失参数。
  3. 修改内嵌 Custom SQL 时，必须同时检索 datasource relation 与 `object-graph` 中的重复 relation 副本，保证两处文本一致。

### [2026-05-15] 实时可信度问题若继续沿用 `NOW()` 伪刷新、总粒度门店计数和视图层 `RUNNING_SUM/WINDOW_MAX`，会同时导致页头时间不可信、门店数 KPI 漂移、累计趋势末点无法对齐实时 KPI

- 触发场景：用户重开 `HEFANG门店实时销售战情看板.twb` 后，连续反馈三类问题：页头“刷新时间”并没有跟随看板刷新到最新；`0销售门店数` 与 `进度落后门店数` 看起来没有实时同步；`今日累计销售进度` 在 8~14 点之间不递增，且最新时点不等于顶部 `今日实时销售额` KPI。
- 报错 / 现象：
  1. 页头显示的是看板打开时刻式样的“刷新时间”，而不是 Oracle 实时交易的真实截至时间。
  2. 两张门店数 KPI 在 federated / relationship 粒度下直接 `SUM(IF ...)` 时，会受总粒度聚合影响，结果偏大或偏小。
  3. 累计趋势继续依赖 worksheet 侧 `RUNNING_SUM/WINDOW_MAX` 时，容易因小时缺口、视图粒度和筛选顺序导致曲线不单调，末点也可能和实时销售 KPI 脱节。
- 根因判断：
  1. `NOW()` 只能表达 Tableau 当前计算时刻，不能代表实时交易已到哪个业务时点；对实时战情更正确的语义应是“数据截至”。
  2. 门店数 KPI 本质是“每店是否命中条件”的计数，必须先在 `store_id` 粒度打标，再汇总；不能在总粒度直接判断。
  3. 累计趋势若要求“按小时单调递增，且最新点与实时 KPI 对齐”，更稳定的做法是在 SQL 侧先产出逐店逐小时累计值，并单独暴露最新销售小时，再让视图层只做显示裁剪。
- 修复动作：
  1. 在 `ds_oracle_realtime_store_kpi_live` 的 Oracle SQL 中补 `MAX(r.STATUSTIME) AS LAST_STATUSTIME`，并将页头文案从“刷新时间”改为“数据截至”。
  2. 将页头时间文本、营业时间进度、`time_progress` 全部切到 `{ FIXED : MAX([LAST_STATUSTIME]) }` 驱动，不再伪装成 `NOW()`。
  3. 将 `今日0销售门店数` 与 `进度落后门店数` 改为 `SUM({ FIXED [store_id] : MIN(IF ... THEN 1 ELSE 0 END) })` 的门店级标志汇总。
  4. 重写 `ds_realtime_cum_progress_target_live` 的 Oracle SQL：补门店范围、小时时间骨架、逐店逐小时累计与 `LATEST_HOUR`，再把累计图 calculation 改为直接消费 SQL 预累计结果。
- 验证状态：已用 Python 对主工作簿执行 XML 解析校验，结果 `XML_OK`；Tableau 客户端重开后的真实渲染与口径对账仍需用户继续确认。
- 预防动作：
  1. 后续实时看板若没有 Tableau 原生 refresh timestamp，就不要继续把 `NOW()` 写成“刷新时间”；优先找真实业务水位字段，并按“数据截至”语义展示。
  2. 对“门店数 / 门店达标数 / 门店落后数”这类布尔计数指标，默认先做 `FIXED [store_id]` 标志再汇总，避免 relationship 聚合漂移。
  3. 对要求“最新点必须对齐单值 KPI”的累计趋势，优先考虑 SQL 预累计 + 最新时点字段，不要默认继续堆 `RUNNING_SUM/WINDOW_MAX` 表计算。

### [2026-05-15] 把 `:Measure Names` 直接挂到 `实时战情_今日累计销售进度` 的 `cols` 上，虽然能把两根柱体拆开，但会同步把目标线按 `(小时, 度量名称)` 粒度打散成点，并触发 X 轴标签爆炸

- 触发场景：用户要求“今日累计销售额 / 昨日同小时累计销售额”从重叠柱体改为并排双柱体；尝试将主 worksheet 的 `cols` 从 `[none:SALE_HOUR:nk]` 改为 `([none:SALE_HOUR:nk] / [:Measure Names])`。
- 报错 / 现象：Tableau 能打开工作簿，但视图退化为两类明显问题：
  1. X 轴下方重复出现长字符串的 `Measure Names` 标签，严重挤占可读空间。
  2. 原本应连续的“今日目标进度线”不再连线，而是被打散成每小时一个离散点。
- 根因判断：在同一张 worksheet 中，`Line` mark 也会受到 `cols` 上全部离散维度的切分影响。将 `:Measure Names` 上列后，Tableau 会按 `(SALE_HOUR, Measure Name)` 的组合拆分 pane / mark 粒度；这能形成 grouped bars，但同时破坏目标线跨小时的连续性。
- 修复动作：
  1. 不再继续在主文件里硬推 `SALE_HOUR / Measure Names` 的单-sheet 方案。
  2. 先把主工作簿回滚到稳定可解析版本。
  3. 另外产出 `HEFANG门店实时销售战情看板.overlay_trial_20260515_115700.twb`，尝试改用“bar 底图 + 透明目标线 worksheet”的 dashboard 叠层方案。
- 验证状态：主文件与 `overlay_trial` 试验版均已完成 XML 解析校验，结果 `XML_OK`；是否满足最终视觉要求仍待 Tableau 客户端重开确认。
- 预防动作：后续如果同类需求同时要求“每个离散桶内 grouped bars + 一条连续折线”，优先评估多 worksheet / dashboard overlay 方案；不要默认把 `:Measure Names` 直接上 `cols` 作为单-sheet 通解。

### [2026-05-15] 当前 HEFANG 实时战情工作簿版本不接受 `mark-line-pattern` 这个 mark 样式枚举；即使值写成 `solid`，也会在 Tableau 重开时直接触发 `D2E8DA72`

- 触发场景：用户要求把 `实时战情_今日累计销售进度` 中的 `今日目标进度线` 从柱体改为折线后，重开 `HEFANG门店实时销售战情看板.twb`，Tableau 立即报“加载无法成功完成”。
- 报错 / 现象：错误码 `D2E8DA72`，详情为 `Error(2067,66): value 'mark-line-pattern' not in enumeration`。
- 根因判断：当前这本 `HEFANG门店实时销售战情看板.twb` 所对应的 Tableau 文档模型不接受 `mark-line-pattern` 这一 style 枚举。也就是说，虽然其它样板 workbook 里可能存在类似属性，但对当前目标 workbook 版本，哪怕写成 `solid` 也会在 schema 校验阶段被直接拒绝。
- 修复动作：
  1. 保留双轴结构与 `Line` mark，不回退折线方案。
  2. 仅删除目标线 pane 中的 `<format attr='mark-line-pattern' value='solid' />`。
  3. 让折线依赖默认 line pattern 渲染，后续若需虚线或其它线型，必须先在同版本 workbook 中确认存在合法枚举写法。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续为 HEFANG 当前这本实时战情工作簿增加折线样式时，默认不要主动写 `mark-line-pattern`；优先只改 `mark class='Line'`、`color`、`size` 这类已在当前 workbook 中验证通过的属性。

### [2026-05-14] 当前 HEFANG 实时战情工作簿版本不接受 `manual-sort` 节点，且 `horizontal-align` 不是合法 style 枚举值；这两类 schema 级错误会直接导致工作簿加载失败

- 触发场景：用户要求废弃旧文本版渠道模块并改成“当天销售额柱形图”后，重开 `HEFANG门店实时销售战情看板.twb` 直接弹出 `D2E8DA72`，提示 `manual-sort` 无声明、内容模型不允许，且 `horizontal-align` 取值非法。
- 报错 / 现象：Tableau 客户端无法完成工作簿加载，错误详情包括：
  1. `no declaration found for element 'manual-sort'`
  2. `element 'manual-sort' is not allowed for content model '(datasources?,mapsources?,datasource-dependencies*,filter,sort,perspectives,slices?,aggregation)'`
  3. `value 'horizontal-align' not in enumeration`
- 根因判断：虽然参考工作簿中存在 `manual-sort` 片段，但当前这本 `HEFANG门店实时销售战情看板.twb` 所对应的 Tableau 文档模型不接受该节点；同时 `datalabel` style 中写入了非法的 `horizontal-align` 属性值，导致 schema 级校验直接阻断加载。
- 修复动作：
  1. 从 `实时战情_渠道销售贡献图` worksheet 的 `<view>` 中移除 `manual-sort` 整段节点。
  2. 从 `datalabel` style 中删除 `horizontal-align` 这一非法格式项。
  3. 先以“恢复可加载”为优先，暂时接受默认排序；若后续用户确认排序不符合预期，再用当前 workbook 版本兼容的排序写法补回。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续为 HEFANG 当前这本实时战情工作簿编译 worksheet 时，不要直接复制其它工作簿里的 `manual-sort` 和未经验证的样式属性；新增排序或 label 对齐前，先确认目标 workbook 版本已存在同类合法写法。

### [2026-05-14] 渠道销售贡献模块若把裸 `FIXED` 结果继续传给下游比例 / 文本 helper，Tableau 会将整组字段标红并导致 worksheet 空白

- 触发场景：`HEFANG门店实时销售战情看板.twb` 新增 `实时战情_渠道销售贡献` 模块后，用户在 Tableau 客户端打开 worksheet，看到工作表空白；计算编辑器中 `渠道销售贡献率_实时战情` 报“无法将聚合和非聚合参数与此函数混合”。
- 报错 / 现象：`渠道销售贡献率_实时战情` 与后续一串 `直营/联营/小程序贡献率文本`、`条形` helper 变红，导致整张 Text worksheet 不渲染。
- 根因判断：
  1. datasource 根部的 `渠道销售贡献率_实时战情` 公式写成了 `SUM([mtd_sales_amt]) / { FIXED : SUM(...) }`，分子是显式聚合，分母是裸 `FIXED` 结果，Tableau 将其判为聚合 / 非聚合混用。
  2. 新模块内三个渠道贡献率和文本 helper 又继续直接引用这些 LOD 结果，没有显式再包 `MIN(...)`，于是错误沿着下游链路扩散到整张 worksheet。
- 修复动作：
  1. 将 `渠道销售贡献率_实时战情` 改为 `SUM([mtd_sales_amt]) / MIN({ FIXED : SUM(...) })`。
  2. 将 `直营/联营/小程序贡献率_实时战情` 统一改为 `MIN([渠道销售额]) / MIN([渠道总销售额])`。
  3. 将金额文本、贡献率文本、条形文本所有下游 helper 全部改为读取 `MIN([Calculation_xxx])`，避免再次把裸 LOD 结果混入字符串 calculation。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续凡是 Tableau 中 `FIXED` 结果要继续参与除法、比较、字符串格式化或阶梯分段时，默认先显式包一层 `MIN(...)` 或其它聚合后再传给下游 helper，不要假设“LOD 本身已经算聚合”。

### [2026-05-14] Text worksheet 中若字符串字段写成 `AVG([另一个非聚合计算字段])` 这类嵌套聚合 helper，Tableau 仍可能把该字段标红并提示“需要对非聚合公式的用户定义聚合”

- 触发场景：`HEFANG门店实时销售战情看板.twb` 的 `页头_时间进度卡` 已经简化成“两行版”，但用户在 Tableau 客户端截图中看到 `进度条已完成_实时战情`、`进度条未完成_实时战情` 两个 pill 仍是红色 `聚合(...)`。
- 报错 / 现象：Marks 卡里 `营业时间进度_实时战情` 以 `AVG(...)` 显示正常，但两个字符串进度条 pill 变红，鼠标提示“错误: 计算‘进度条已完成_实时战情’需要对非聚合公式的用户定义聚合”。
- 根因判断：上一轮虽然把字符串进度条改写成了 `IF AVG([营业时间进度_实时战情]) ...`，但 `营业时间进度_实时战情` 本身仍是非聚合 calculation。也就是说，字符串字段是在自己的公式里再包一层 `AVG([另一个计算字段])`。对这类 Text worksheet 场景，Tableau 仍会把底层 helper 视为非聚合来源，导致字符串字段整条 calculation 被判为非法聚合链路。
- 修复动作：
  1. 将 `营业时间进度_实时战情` 本身改成聚合数值字段：`MIN(IF NOW() &lt;= DATEADD('hour', 10, TODAY()) ... END)`。
  2. 将 `进度条已完成_实时战情`、`进度条未完成_实时战情` 改为直接引用聚合后的 `[Calculation_202605141551]`，不再在字符串公式里额外包 `AVG(...)`。
  3. 将 `页头_时间进度卡` 的数值实例从 `[avg:Calculation_202605141551:qk]` 切换为 `[usr:Calculation_202605141551:qk]`，并同步更新 `encodings` 与 `customized-label` 引用。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续凡是 Text worksheet 里“字符串文案 / 进度条”依赖动态数值 helper 时，优先采用“数值 helper 自身就是聚合 calculation + 字符串字段直接引用它 + 视图实例用 `usr:`”的模式；不要在字符串 calculation 中再写 `AVG([另一个 calculation])` 这种嵌套用户定义聚合。

### [2026-05-14] Text worksheet 中若时间文本计算使用 `NOW()` / `TODAY()` 原始非聚合公式，而该字段在 Marks 卡上以 `聚合(...)` 方式使用，Tableau 会直接报“需要对非聚合公式的用户定义聚合”

- 触发场景：`HEFANG门店实时销售战情看板.twb` 的 `页头_信息摘要` 已修复“刷新时间字段缺少作用域”后，用户再次在 Tableau 客户端打开 worksheet，仍看到 `刷新时间文本_实时战情` 这个 pill 变红，并提示计算需要对非聚合公式做用户定义聚合。
- 报错 / 现象：Marks 卡中 `统计日期文本_实时战情` 能正常显示为蓝色 `聚合(...)`，但 `刷新时间文本_实时战情` 变成红色 `聚合(...)`，鼠标提示“错误: 计算‘刷新时间文本_实时战情’需要对非聚合公式的用户定义聚合”。
- 根因判断：前一轮虽然已经把刷新时间字段补进了 `datasource-dependencies`、`column-instance` 和 `encodings`，但该 calculation 公式本身仍写成 `NOW()` 的非聚合版本。因为这张 Text worksheet 在 Marks 卡上是按 `聚合(...)` 使用文本字段，Tableau 会要求 calculation 自身也满足聚合规则；`NOW()` 原始公式不满足，于是直接报聚合错误。
- 修复动作：
  1. 将 `刷新时间文本_实时战情` 从 `NOW()` 版本改为 `MIN(NOW())` 驱动的聚合版时间公式。
  2. 将 `当前时间文本_实时战情` 同步改为 `MIN(NOW())` 驱动，避免 `页头_时间进度卡` 后续踩同类问题。
  3. 保留前一轮已补好的字段声明、`column-instance` 与 `encodings` 不变，只修 calculation 的聚合层级。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续凡是在 Text worksheet / KPI 卡中展示 `NOW()`、`TODAY()`、`DATEPART(..., NOW())` 这类时间函数，只要最终会以 `聚合(...)` 形式挂到 Marks 卡，就默认优先写成 `MIN(NOW())`、`MIN(TODAY())` 或其它聚合版表达式；不要等 Tableau 客户端报错后再补聚合。

### [2026-05-14] Text worksheet 的 customized-label 若引用多个动态字段，但 `encodings` 里只挂了其中一部分 `<text column>`，Tableau 标签编辑器会继续把缺的字段标成“缺少字段”

- 触发场景：`HEFANG门店实时销售战情看板.twb` 的 `页头_信息摘要` 为了贴近原型，改成同一行同时显示 `统计日期`、`数据源`、`刷新时间`、`连接方式`。用户重开后在 Tableau 标签编辑器里看到 `刷新时间` 位置仍是红色 `<缺少字段>`。
- 报错 / 现象：即使 Agent 已经为 `刷新时间文本_实时战情` 补了 `<column ...>` 和 `<column-instance ...>`，Tableau 客户端里 `刷新时间` 仍不显示，标签编辑器继续把对应 token 标红。
- 根因判断：对于 Text worksheet，动态字段不只要在 `datasource-dependencies` 里声明，还必须真正进入视图的 `encodings`。当前 `页头_信息摘要` 的 `customized-label` 用了两个动态字段：`统计日期` 和 `刷新时间`，但 `encodings` 一开始只挂了第一条 `<text column='...[usr:Calculation_202605141541:nk]' />`，没有把刷新时间对应的第二条 `<text column='...[usr:Calculation_202605141542:nk]' />` 一起挂进去，所以 Tableau 仍把它当成“未进入视图的字段”。
- 修复动作：
  1. 先补 `刷新时间文本_实时战情` 的 `<column ...>` 与 `<column-instance ...>`。
  2. 再在 `页头_信息摘要` 的 `<encodings>` 下追加第二条 `<text column='[federated...].[usr:Calculation_202605141542:nk]' />`。
  3. 保持其它布局和 `NOW()` 公式不变，只修字段进入视图的链路。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续凡是 Text worksheet 的 `customized-label` 同时引用多个动态字段，都要逐个检查这三层是否齐全：`<column ...>`、`<column-instance ...>`、`encodings` 下对应的 `<text column ...>`。只补前两层仍可能在客户端显示“缺少字段”。

### [2026-05-14] 在固定高度较小的 dashboard 卡片里同时保留 Text worksheet 内置 title 和 customized-label，会让正文被挤没，并造成 KPI 标题重复

- 触发场景：`HEFANG门店实时销售战情看板.twb` 完成首屏 KPI 与摘要层后，用户首次重开 Tableau 截图显示工作簿已能正常打开，但顶部 `页头_信息摘要` 整块空白、`页头_时间进度卡` 只剩标题，且 6 张 KPI 卡都出现“卡片顶端标题 + 正文第一行重复标题”的双重标题现象。
- 报错 / 现象：这次不是弹窗报错，而是典型渲染异常。顶部摘要行的两个 Text 卡片位于较小固定高度 zone 中，用户截图里可以看到卡片边框正常、底部图表也正常，说明 datasource 与 dashboard zone 都已加载成功，但正文 mark label 没有出现在卡片里；KPI 卡虽然出值，但每张卡都重复显示一次标题。
- 根因判断：问题不在 calculation 或 datasource，而在 Text worksheet 的显示层结构。当前 8 张文本卡片都同时保留了 worksheet 自带 `<layout-options><title>...</title></layout-options>` 与 pane `customized-label`。在小高度 dashboard zone 中，内置 title 会先占掉可用垂直空间，导致顶部两张卡正文被裁掉；KPI 卡高度略大，所以正文还能显示，但会形成“worksheet title + customized-label 第一行”两套标题并存。
- 修复动作：
  1. 先创建备份 `HEFANG门店实时销售战情看板.text_card_title_cleanup_20260514_131034.twb`。
  2. 从 `页头_信息摘要`、`页头_时间进度卡`、`KPI07_总日标`、`KPI01_日销售额`、`KPI02_日达成率`、`KPI08_总月标`、`KPI03_月累计销售额`、`KPI04_月累计达成率` 这 8 张 Text worksheet 中删除内置 title。
  3. 保留原有 customized-label、dashboard zone 和数据字段链路不变，只修显示层冲突。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续凡是用 Text worksheet 做 dashboard 卡片时，如果卡片正文的 `customized-label` 第一行已经承担标题语义，就不要再保留 worksheet 内置 title；尤其在固定高度较小的 zone 里，先默认关掉内置 title，再考虑正文样式。

### [2026-05-14] Oracle live Custom SQL 若手工写入小写字段名，但 Tableau 实际按大写列名刷新 metadata，中文 caption 字段会整体报“字段在数据库中不存在”

- 触发场景：`HEFANG门店实时销售战情看板.twb` 已能打开后，用户进入 `实时战情_分时销售` worksheet，发现 `实时销售额`、`实时单数`、`最新交易时间`、`小时` 等字段出现红色感叹号，并提示“字段在数据库中不存在”。
- 报错 / 现象：Tableau 数据窗格中同时出现 Oracle datasource 的英文原始字段 `SALES_AMT`、`ORDER_CNT`、`LAST_STATUSTIME`、`SALE_HOUR`，以及带中文 caption 的 `实时销售额`、`实时单数`、`最新交易时间`、`小时`；后者全部变成无效字段。
- 根因判断：这不是数据库少字段，而是 Agent 在 XML 中手工注入 Oracle Custom SQL datasource 时，把语义层字段名写成了小写链路：`[sales_amt]`、`[order_cnt]`、`[last_statustime]`、`[sale_hour]`。但 Oracle 对未加双引号的别名默认回传大写列名，Tableau 客户端实际刷新后识别的是 `SALES_AMT`、`ORDER_CNT`、`LAST_STATUSTIME`、`SALE_HOUR`。结果导致中文 caption 字段、worksheet `datasource-dependencies`、`column-instance`、tooltip / rows / cols 仍指向小写名字，整条引用链一起悬空。
- 修复动作：
  1. 先创建备份 `HEFANG门店实时销售战情看板.oracle_fieldcase_fix_20260514_1210.twb`。
  2. 将 Oracle datasource 两处 Custom SQL 中的 alias 全部改为大写：`REPORT_DATE`、`SALE_HOUR`、`STORE_ID`、`STORE_CODE`、`STORE_NAME`、`SALES_AMT`、`ORDER_CNT`、`LAST_STATUSTIME`。
  3. 同步把 datasource `metadata-records`、根 `<column ... name='[...]' />`、`实时战情_分时销售` 的 `datasource-dependencies`、`column-instance`、tooltip、`rows`、`cols` 全部切换到对应大写字段名，同时保留中文 caption 不变。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开 / 刷新字段验证待继续执行。
- 预防动作：后续手工编译 Oracle live Custom SQL datasource 时，字段链路不能只看 SQL 文本里写了什么别名，还要对齐 Tableau 客户端真实识别的列名大小写；默认优先直接使用 Oracle 最终返回的大写列名，caption 再写中文，不要在 XML 里自造一套小写根字段名。

### [2026-05-14] 当前 HEFANG 实时战情 workbook 即使已改成 `computed-sort`，若 manifest 缺少排序特性开关，重开仍会报 D2E8DA72 / `no declaration found for element 'computed-sort'`

- 触发场景：在 `HEFANG门店实时销售战情看板.twb` 中把 `实时战情_门店月达成排行` 的排序从 `shelf-sorts` 改成 `computed-sort` 后，用户再次在 Tableau 客户端重开工作簿做验证。
- 报错 / 现象：Tableau 继续弹出 `D2E8DA72`，错误文本显示 `Error(812,177): no declaration found for element 'computed-sort'`，并进一步提示 `element 'computed-sort' is not allowed for content model '(datasources?,mapsources?,datasource-dependencies*,filter,sort,perspectives,slices?,aggregation)'`。
- 根因判断：问题不在 `computed-sort` 写法本身，而在这份目标 workbook 顶部 `document-format-change-manifest` 仍停留在极简能力集，缺少支持新排序标签的 workbook 级 feature flag。对照同版本且可正常打开的 `销售驾驶舱_第一批_20260420.twb`、`invertory_DashBoard_main.twb` 可确认，它们都显式包含 `IntuitiveSorting`、`IntuitiveSorting_SP2`、`SortTagCleanup`；目标文件缺少这些声明时，Tableau 会把 `computed-sort` 当成未声明节点。
- 修复动作：
  1. 先创建备份 `HEFANG门店实时销售战情看板.sort_manifest_fix_20260514_1146.twb`。
  2. 在目标 workbook 的 `<document-format-change-manifest>` 中补入 `<IntuitiveSorting />`、`<IntuitiveSorting_SP2 />`、`<SortTagCleanup />`。
  3. 保留原先已经改好的 `<computed-sort column='[...].[none:store_name:nk]' direction='DESC' using='[...].[sum:month_ach_rate:qk]' />`，不回退为无排序状态。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续从空白或极简 `.twb` 起步时，只要要注入 `computed-sort`、`shelf-sorts` 等排序结构，必须先对照已验证工作簿补齐排序相关 manifest 开关，不要只替换 view 内节点而忽略 workbook 级能力声明。

### [2026-05-14] 在当前 HEFANG 实时战情 workbook 中把 `shelf-sorts` 直接写进 `view`，会触发 D2E8DA72 / `no declaration found for element 'shelf-sorts'`

- 触发场景：为 `HEFANG门店实时销售战情看板.twb` 新增 `实时战情_门店月达成排行` worksheet 后，用户在 Tableau 客户端再次重开工作簿做渲染验证。
- 报错 / 现象：Tableau 弹出 `D2E8DA72`，错误文本显示 `Error(812,24): no declaration found for element 'shelf-sorts'`，并进一步提示 `element 'shelf-sorts' is not allowed for content model ...`。
- 根因判断：当前这份实时战情 workbook 的 schema 兼容层并不接受 `shelf-sorts` / `shelf-sort-v2` 这套排序结构；虽然某些参考 workbook 中存在该节点，但在本工作簿当前能力集下，`view` 内应使用旧版兼容的 `computed-sort` 或 `manual-sort`，不能直接照搬 `shelf-sorts`。
- 修复动作：
  1. 删除 `实时战情_门店月达成排行` worksheet `view` 内的 `<shelf-sorts>` 与 `<shelf-sort-v2 .../>`。
  2. 改写为单行兼容排序：`<computed-sort column='[...].[none:store_name:nk]' direction='DESC' using='[...].[sum:month_ach_rate:qk]' />`。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 再次重开验证待继续执行。
- 预防动作：后续若目标 `.twb` 不是从同一 schema / manifest 家族复制出来的，不要直接照搬 `shelf-sorts`；优先使用 `computed-sort` / `manual-sort` 这类兼容度更高的排序节点，除非已在目标工作簿中确认 `shelf-sorts` 可以被 Tableau 正常接受。

### [2026-05-14] 从空白 workbook 注入带 `datatype='table'` 的 datasource 后，若 manifest 缺少 `ObjectModelTableType`，重开会报 D2E8DA72 / `value 'table' not in enumeration`

- 触发场景：为 `HEFANG门店实时销售战情看板.twb` 从空白工作簿骨架注入 Oracle / MySQL datasource 后，用户关闭并重开 Tableau 工作簿做首次渲染验证。
- 报错 / 现象：Tableau 弹出“无法完成操作”，错误码 `D2E8DA72`；报错定位到 `Error(193,186): value 'table' not in enumeration`，指向 `Custom SQL Query` 的内部对象列 `<column ... datatype='table' .../>`。
- 根因判断：问题不在 datasource SQL 本身，而在 workbook 顶部 `document-format-change-manifest` 缺少 `ObjectModelTableType`。当前这份实时战情 `.twb` 是从极简空白 workbook 起步，manifest 比 `销售部自动化日报.twb` 等已验证工作簿少了该开关；当 datasource 语义层中出现 `datatype='table'` 的内部对象列时，Tableau 会因为未启用 `ObjectModelTableType` 而把 `table` 视为非法枚举值。
- 修复动作：
  1. 先创建报错前备份 `HEFANG门店实时销售战情看板.error_fix_20260514_110630.twb`。
  2. 在目标 workbook 的 `<document-format-change-manifest>` 中补入 `<ObjectModelTableType />`。
  3. 不删除 datasource 中的 `datatype='table'` 内部对象列，保留 Tableau 正常 datasource 语义层结构。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续凡是从空白 workbook 起步，再注入已验证 `.twb` 中的 datasource / Custom SQL 结构时，必须先比对并补齐目标 workbook 的 feature manifest，尤其检查是否包含 `ObjectModelTableType`；不要只复制 datasource 片段而忽略 workbook 级能力开关。

### [2026-05-13] 销售日报新建 MySQL Custom SQL datasource 首次重开时报“不存在参数”与 `; ) TableauSQL LIMIT 0`，根因是 Tableau 对自定义 SQL 的二次解析

- 触发场景：为 `销售部自动化日报.twb` 的 `KPI05_同店同比` 新增 `ds_kpi_same_store_yoy_physical_live` live Custom SQL datasource 后，用户首次在 Tableau 客户端重开工作簿并进入 Custom SQL 编辑/连接验证页面。
- 报错 / 现象：先报“自定义 SQL 关系引用了不存在的参数”，报错文本直接截出 `0.0005 THEN '→ 较昨日 持平' ...`；随后又报 MySQL 语法错误，关键片段为 `near '; ) TableauSQL LIMIT 0' at line 169`。
- 根因判断：这不是 MySQL 服务异常，而是 Tableau 对 Custom SQL 做了两层额外处理。其一，Tableau 会把原始 Custom SQL 包成 `SELECT * FROM (<sql>) TableauSQL LIMIT 0` 做探测，因此 SQL 末尾如果保留分号，会在包裹后形成 `; ) TableauSQL LIMIT 0` 并直接触发语法错误。其二，Tableau 会把原始 SQL 里的 `<...>` 片段误当成参数占位符，所以像 `rk <= 2`、`ABS(diff) < 0.0005` 这类包含 `<` 的比较表达式，会被误判为“引用了不存在的参数”。
- 修复动作：
  1. 去掉 `ds_kpi_same_store_yoy_physical_live` 两处 Custom SQL XML 片段尾部的分号。
  2. 将 `rk <= 2` 改写为 `rk IN (1, 2)`。
  3. 将 `ABS(diff) < 0.0005` 改写为 `ROUND(ABS(diff) * 100, 1) = 0`，并把正负方向判断改写为 `SIGN(diff) = 1`，确保 Custom SQL 文本中不再出现会触发 Tableau 参数解析的 `<`。
- 验证状态：`.twb` 编辑器 XML 检查无错误；外部工作簿已完成上述修复，但尚待用户在 Tableau 客户端再次重开验证 datasource 是否可正常连接与渲染。
- 预防动作：以后凡是在 Tableau 中写 MySQL Custom SQL，都默认遵守两条红线：1）不要保留 SQL 结尾分号；2）尽量避免在原始 SQL 文本中直接出现 `<` 比较运算，优先改写为 `IN`、`=`、`SIGN()`、`ROUND()` 等等价表达式。

### [2026-05-12] 若只清理单张 KPI 的 Text 颜色编码，剩余 KPI 会在负值日继续整卡翻橙

- 触发场景：上一轮已修正 `去年同期同比` 的橙色问题，但用户再次重开日报后反馈前三张 KPI 卡在当天为负值时又整体变橙。
- 报错 / 现象：`总目标`、`日销售额`、`日达成率` 三张卡的标题、主值与副文案再次一起变成橙色，说明问题并非只存在于 `KPI05_去年同期同比` 单张卡。
- 根因判断：此前虽然已经总结出“Text KPI 需要移除 color shelf 和 mark color encoding 才能固定配色”，但实际只对 `KPI05` 做了彻底清理；`KPI01-04/07/08` 仍保留 `<encodings><color ... /></encodings>` 和 `<style-rule element='mark'><encoding attr='color' .../></style-rule>`，因此在趋势方向为负时仍会整张卡翻橙。
- 修复动作：对当前 7 张现用 KPI（`KPI01-05/07/08`）统一批量删除 Text marks 的 color shelf 与 mark color palette 编码，而不是继续逐张修。修改前备份为 `销售部自动化日报.kpi_text_color_cleanup_20260512_102107.twb`。
- 验证状态：已通过终端统计确认 `Calculation_1730010000000611/0621/0631/0641/0651/0671/0681` 对应的 `colorShelf=0`、`markEncoding=0`；XML 解析结果 `XML_OK`。
- 预防动作：只要目标是“顶部 KPI 卡完全固定配色”，就必须一次性扫描所有现用 KPI worksheet，把 Text mark 的 color shelf 和 mark color encoding 全部清零；不要根据用户截图只修一张当前出问题的卡。

### [2026-05-12] `去年同期同比` KPI 卡仍保留 Text 颜色编码会继续显示橙色；用户确认停用后需同步删除 KPI06 残留 worksheet/window/thumbnail 元数据

- 触发场景：上一轮已把 7 张 KPI 卡的字体统一成固定蓝色，并把趋势文案改为“较昨日”，但用户重开后反馈 `去年同期同比` 卡仍显示橙色；同时要求顺手清理 `KPI06_目标缺口` 的残留元数据。
- 报错 / 现象：顶部最后一张 `去年同期同比` 卡片主值和副文案仍沿用同比方向色，没有跟其余 6 张卡保持统一；工作簿内仍残留不再使用的 `KPI06_目标缺口` worksheet、worksheet window 和 thumbnail 节点。
- 根因判断：虽然上一轮把 `KPI05_去年同期同比` 的 `customized-label` 字体色改成了固定蓝色，也把 `datalabel color-mode` 从 `match` 改成了 `automatic`，但该 worksheet 仍保留了 `<encodings><color ... /></encodings>` 和 `<style-rule element='mark'><encoding attr='color' .../></style-rule>` 两层颜色编码，Tableau 仍会按 mark color 渲染 Text。另一方面，用户明确停用 KPI06 后，如果只把 dashboard zone 去掉而不清理底层 worksheet/window/thumbnail，工作簿里仍会残留无用元数据。
- 修复动作：
  1. 从 `KPI05_去年同期同比` 中删除 Text marks 的 `<color column='...[usr:Calculation_1730010000000651:qk]' />`，并同时删除 `style-rule element='mark'` 下的整段颜色 palette 编码，只保留固定字体色与标签显示规则。
  2. 从 `销售部自动化日报.twb` 中彻底删除 `KPI06_目标缺口` 的 `<worksheet ...>`、`<window class='worksheet' ...>` 和 `<thumbnail ...>` 三块残留节点。
  3. 修改前先落盘备份 `销售部自动化日报.kpi05_kpi06_cleanup_20260512_095627.twb`。
- 验证状态：已再次执行 XML 解析校验，结果 `XML_OK`；并用字符串检索确认当前 `.twb` 中 `KPI06_目标缺口` 剩余命中数为 `0`。
- 预防动作：
  1. Text KPI 卡如果要求完全固定颜色，不能只改 `customized-label` 字体色；必须同时检查并移除 worksheet 的 `color` shelf 与 mark color palette 编码。
  2. 当某张 worksheet 被用户明确废弃时，除 dashboard zone 外，还要同步核对并清理 worksheet、window、thumbnail 等残留节点，避免工作簿持续膨胀和后续误判。

### [2026-05-11] `KPI06_目标缺口` 残留悬空标签字段会阻塞打开；KPI 趋势文案需挂 Text，日/月对比图已按用户要求回退三色柱图

- 触发场景：上一轮修完 AGG 报错后，用户重开 workbook 时仍收到 `KPI06_目标缺口` 警告：“不存在名为 `Calculation_1730010000000017` 的字段”；同时用户在 Tableau 界面中确认，趋势文案只有拖到 Text 才会稳定显示，并明确要求“子弹图不要了，回归一开始的三色柱型图”。
- 报错 / 现象：`KPI06_目标缺口` 打开即弹字段缺失告警；KPI 趋势文案如果只挂在细节上下文，客户端里不稳定；左侧日/月达成率对比不再需要 bullet 目标带，而要回到原始单层三色横向柱图。
- 根因判断：`KPI06` 的自定义标签还残留了 `[usr:Calculation_1730010000000017:nk]` 的旧引用，但该 worksheet 本地 `datasource-dependencies` 并没有注册这个 column-instance，因此 Tableau 打开时直接报缺字段。另一方面，Text KPI 的趋势文案在当前工作簿里只有真正挂到 Text shelf 才会稳定参与 label 渲染；仅放 `lod/detail` 不符合用户当前验证结果。bullet 图虽然技术上可用，但用户最终确认不要该形态。
- 修复动作：
  1. 将 KPI01-KPI08 的趋势文案实例统一从 `lod` 改为第二个 `text` 编码；保留主值仍为第一个 `text`，颜色继续由趋势方向字段控制。
  2. 对 `KPI06_目标缺口` 删除悬空的 `[usr:Calculation_1730010000000017:nk]` 标签行，不再展示旧的“目标缺口判断”副说明，直接保留“标题 + 主值 + 趋势文案”三段式结构。
  3. 将 `渠道达成概览_日达成率对比` 与 `渠道达成概览_月达成率对比` 从 bullet 双层条回退为单层横向 Bar：`rows=渠道组`、`cols=达成率`、`color=渠道组`、`text=达成率`，并为 `直营/联营/小程序` 固定三色映射。
- 验证状态：已对修复后的 `销售部自动化日报.twb` 再次执行 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：
  1. 后续凡是 Text KPI 的副文案，默认直接挂到 Text，不要再只放 `lod/detail` 猜 Tableau 是否能认。
  2. 修改 `customized-label` 前，先核对对应 worksheet 本地 `datasource-dependencies` 是否真的存在被引用的 `usr:` 实例，避免 root 层有字段但本地没注册的半连接状态。
  3. 当用户明确否决某种图形形态时，优先直接回退到用户认可的稳定结构，不在被否决的样式上继续做视觉微调。

### [2026-05-11] KPI 自定义标签如果直接引用未挂到 marks 上下文的趋势文案字段，会显示 `<缺少字段!>`；bullet 目标带配色过近会让实际条不明显

- 触发场景：修完 AGG 报错后，用户再次重开 `销售部自动化日报.twb`，发现 KPI 箭头文案仍未成功显示；在 `总日标` 标签编辑器中能看到主值正常，但第二行变成红色 `<缺少字段!>`。同时用户反馈左侧 `日达成率对比` / `月达成率对比` 的 bullet 图里，100% 目标带与实际完成条看起来几乎同色，阅读不清。
- 报错 / 现象：KPI 卡片主数字正常，但箭头趋势文案缺失；bullet 图虽然能显示双层条带，但视觉上目标带与实际条分离度不足。
- 根因判断：当前 KPI 采用 `customized-label` 直接插入趋势文案字段，但这些 `趋势文案_*` 实例没有挂到 marks 上下文里，Tableau 重开后会把标签中的该占位符解析成 `<缺少字段!>`。另一方面，bullet 图的背景目标带和实际条虽然技术上是两层 bar，但原先配色过近、实际条过细，导致在当前画布缩放下几乎看成一条色带。
- 修复动作：为 KPI01-KPI08 的每张 Text worksheet，把对应 `趋势文案_*` 字段补进 pane `encodings` 的 `lod` 上下文；`KPI06_目标缺口` 额外把副说明字段也补进 `lod`，避免再次丢字段。对两个 bullet worksheet，把 100% 目标带颜色从 `#E6EBF2` 调浅到 `#DCE6F2`，并把实际条改成更明显的 `#4E79A7`，同时把背景条和实际条的 size 从 `0.78/0.20` 调整为 `0.82/0.28`，增强中心实绩线的可见性。
- 验证状态：已对修复后的 `销售部自动化日报.twb` 再次执行 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：
  1. 以后凡是 Text worksheet 依赖 `customized-label` 插入第二、第三个字段，不要只改标签 XML；至少把这些字段挂到 `lod`、`detail` 或其它稳定的 marks 上下文里。
  2. bullet 图在 XML 上双层成立，不代表缩放后可读；目标带和实际条必须同时检查颜色对比和 size 差异，避免“技术正确、视觉上仍像同一条”。

### [2026-05-11] KPI 趋势箭头与 bullet 目标带如果以非聚合公式直接挂 `User` 聚合，会在 Tableau 重开后报 AGG 错并导致工作表空白

- 触发场景：在 `销售部自动化日报.twb` 中新增 KPI01-KPI08 趋势箭头，以及 `渠道达成概览_日达成率对比` / `渠道达成概览_月达成率对比` 的 bullet 目标带后，用户重开 Tableau 进行真实渲染测试。
- 报错 / 现象：KPI 卡片与两个达成率对比工作表变空白；截图中的 Tableau 报错明确指向 `子弹图目标带`、`趋势方向_总日标` 等新字段，提示“需要对非聚合公式的用户定义聚合”。
- 根因判断：本轮新增字段沿用了 `column-instance derivation='User'` 的 AGG 用法，但实际公式里有两类不满足条件的写法：① bullet 目标带直接写成常量 `1`；② 趋势方向字段虽然内部用了 `FIXED` / `SUM`，但整体公式顶层仍是非聚合表达式，趋势文案又继续直接引用这些非聚合结果。Tableau 重开后会把这类字段判定为“非聚合公式却被当成用户定义聚合”，从而整张 worksheet 失效。
- 修复动作：对 `Calculation_1730010000000214`、`Calculation_1730010000000224` 将公式从 `1` 改为 `MIN(1)`；对 `Calculation_1730010000000611` 到 `Calculation_1730010000000681` 的趋势方向字段，把整体公式统一包成 `MIN(IF ... END)`；对 `Calculation_1730010000000612` 到 `Calculation_1730010000000682` 的趋势文案字段，将“上一报告日为空”判断改成 `ISNULL(MIN([上一报告日期_v1]))`，使整条字符串公式也与聚合层级一致。未改 dashboard 布局、颜色区间和字段命名。
- 验证状态：已用 Python 对修复后的 `销售部自动化日报.twb` 执行 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：
  1. 只要某个 calculation 会以 `usr:` 实例被当成 AGG 使用，就不要把顶层公式写成裸常量或裸 IF；至少保证顶层是 `MIN(...)`、`MAX(...)`、`SUM(...)`、`ATTR(...)` 等 Tableau 可识别的聚合表达式。
  2. 如果趋势值本质上是 `FIXED` LOD 在当前视图上的常量回填，优先把整个趋势方向公式包成 `MIN(IF ... END)`，再让趋势文案只引用这个已聚合结果。
  3. `.twb` 直改完成后，不能只停留在 `XML_OK`；凡是新增 calculation 并进入 Tableau 重开测试阶段，都要把客户端真实渲染视为必做验证。

### [2026-05-11] `DashboardRoundedCorners` 在当前 Tableau 2025.03 构建上触发高版本特性警告，且圆角不生效

- 触发场景：为 `销售部自动化日报.twb` 复刻 PNG 样例中的圆角卡片效果，参考 `Uber Dashboard.twb` 的 XML 写法，在 workbook 的 `<document-format-change-manifest>` 中新增 `<_.fcp.DashboardRoundedCorners.true...DashboardRoundedCorners />`，并仅对 `日销售趋势图` 对应的 dashboard zone 试点注入 `<_.fcp.DashboardRoundedCorners.true...format attr='corner-radius' value='16' />`。
- 报错 / 现象：用户重开工作簿时弹出兼容警告：“此工作簿是在较新版本的 Tableau 中创建的。当前版本的 Tableau 可打开工作簿，但某些特性和功能会丢失。(DashboardRoundedCorners)”；关闭警告后页面无圆角视觉变化。
- 根因判断：虽然知识库样例 `Uber Dashboard.twb` 含有 `DashboardRoundedCorners`，但当前用户的实际构建 `2025.3.1 (20253.25.1210.1815)` 并未把该特性视为本版本稳定能力。直接把 manifest feature flag 和 `corner-radius` XML 注入现有工作簿，会被 Tableau 判定为“较新版本特性”，因此只会弹兼容警告并忽略渲染。
- 修复动作：立即从当前 `销售部自动化日报.twb` 中回滚两处试点内容：①删除 manifest 中的 `<_.fcp.DashboardRoundedCorners.true...DashboardRoundedCorners />`；②删除 `日销售趋势图` 模块 zone-style 中的 `<_.fcp.DashboardRoundedCorners.true...format attr='corner-radius' value='16' />`。同时保留备份 `销售部自动化日报.backup_corner_radius_trial_20260511_1.twb` 作为试点留档。
- 验证状态：当前 `.twb` 已确认不再包含 `DashboardRoundedCorners|corner-radius` 字样，XML 解析结果 `XML_OK`；用户侧兼容警告应在重开后消失。
- 预防动作：
  1. 对当前 Tableau 2025.03 环境，不再直接向生产中的 `.twb` 注入 `DashboardRoundedCorners` / `corner-radius` XML。
  2. 若仍要接近 PNG 圆角卡片效果，优先改走两条替代路线：`bitmap/shape` 圆角背景图方案，或“白色容器 + 更大留白 + 细边框”的近似方案。
  3. 后续如需再次试圆角，必须先在独立样板 `.twb` 上验证当前构建是否真正支持，再决定是否迁移到 HEFANG 正式工作簿。

### [2026-05-09] 美化阶段写入 `element='view'` / `element='body'` / `font-color` 导致工作簿加载失败（D2E8DA72）

- 触发场景：美化 Step2 为 11 个 worksheet 添加视图背景色时使用了 `<style-rule element='view'>`；Step3b 为门店明细表格添加交替行色时使用了 `<style-rule element='body'>` 及 `band-color-one`/`band-color-two` 属性；Step3a 在 `element='header'` 中写入了 `<format attr='font-color' ...>`。
- 报错 / 现象：工作簿加载失败，Tableau 报 `D2E8DA72`，16 条错误，分5类：`value 'view' not in enumeration`（12处）、`value 'body' not in enumeration`（1处）、`value 'font-color' not in enumeration`（1处）、`value 'band-color-one' not in enumeration`（1处）、`value 'band-color-two' not in enumeration`（1处）。
- 根因判断：`element` 属性的合法枚举值不包含 `view` 和 `body`；`pane` 才是控制图表/视图背景色的合法元素。`band-color-one`/`band-color-two` 是 `element='body'` 的配套属性，随之一起非法。`header` 元素的合法 format attr 不包含 `font-color`（只支持 `background-color`、`font-weight`、`font-size`、`font-family` 等）。
- 修复动作：①将所有 12 处 `element='view'` 替换为 `element='pane'`（`pane` 可合法设置 `background-color`）；②移除整个 `element='body'` style-rule 块（含 band-color 配置）；③移除 header style-rule 中唯一的 `<format attr='font-color' .../>` 行。执行 `fix_enumeration_errors.py`，长度差 −396。
- 验证状态：XML_OK，剩余 element=view/body 均为 0，剩余 attr=font-color 为 0；可重新打开工作簿验证渲染。
- 预防动作：
  1. `style-rule element` 只使用已知合法值：`cell`、`pane`、`mark`、`axis`、`label`、`datalabel`、`header`、`table`、`table-div`、`refline`、`dropline`、`zeroline`、`gridline`、`trendline`、`dash-title`；**禁止**使用 `view`、`body`、`worksheet`。
  2. 视图/图表背景色统一走 `element='pane'` + `<format attr='background-color' .../>`。
  3. 交替行颜色（`band-color-one/two`）不能通过 `style-rule` XML 注入，需在 Tableau UI 中手动设置；不要在 TWB 编译时尝试写入。
  4. `element='header'` 中只写 `background-color`、`font-weight`、`font-size`、`font-family`；文字颜色用 `font-color` 可能在部分版本不认，移除更安全。

### [2026-05-09] `element='pane'` 写入 background-color 导致图表区出现可见底色（非白色背景块）

- 触发场景：Step2 将 `element='view'` 改为 `element='pane'` 后保留了 `background-color='#F7F8F6'`，Tableau 渲染时在每张 worksheet 的图表/数据区域都绘制了浅灰白底色，用户重开后看到明显的有色块状底色（截图圈出）。
- 报错 / 现象：图表区显示可见底色（#F7F8F6 浅灰白），用户要求改为白色/透明默认。
- 根因判断：`element='pane'` 的 `background-color` 会直接应用到数据图表区，`#F7F8F6` 虽然接近白色但与纯白明显可见差异，且与 Tableau 默认透明状态不同；而 `element='view'`（非法）本意是控制整体视图背景色，对应的合法选项实际上是 dashboard 层的 zone 背景，worksheet 级无法直接设置。
- 修复动作：移除全部 12 处 `<style-rule element='pane'><format attr='background-color' value='#F7F8F6'/></style-rule>` 块，让图表区恢复 Tableau 默认透明白色。保留原 Tableau 原生的 `element='pane'`+`minheight='-1'` 规则（共 9 处）不动。长度差 -1500，XML_OK。
- 验证状态：XML_OK，12 处 pane background-color 全部清除；可重开工作簿验证图表区为白色。
- 预防动作：
  1. Worksheet 级别无法通过 XML style-rule 可靠地设置图表区背景色，不要在 `element='pane'` 或 `element='view'` 上写 `background-color`。
  2. 若要让工作表区域有色背景，应在 dashboard zone 层设置容器背景色（`<zone-style><format attr='background-color'.../>`），而不是在 worksheet 的 `<style>` 块内操作。
  3. 今后美化工作表背景一律走 dashboard zone 容器颜色，worksheet 内只写字体、边框、列宽等结构性属性。

### [2026-05-09] `style-rule element='table'` 中使用 `format-string` 属性导致工作簿加载失败（D2E8DA72）

- 触发场景：为门店经营明细表格中比率字段（日达成率、日连带、月完成率等）设置数字格式，在 worksheet 的 `<style>` 块末尾追加了 `<style-rule element='table'>` 块，并使用 `<format attr='format-string' .../>` 规则。
- 报错 / 现象：工作簿加载失败，Tableau 报 `D2E8DA72`，错误内容为 `value 'format-string' not in enumeration`，共 11 行报错（对应 11 条 format 规则）。
- 根因判断：`format-string` 不是 Tableau `style-rule` 中 `format attr` 的合法枚举值。`<style-rule element='table'>` 下的 `attr` 只接受 `background-color`、`width`、`height` 等特定值，不接受 `format-string`。Tableau 的数字格式只能通过 `<column>` 上的 `default-format` 属性声明，不能通过 worksheet style 规则指定。
- 修复动作：移除整个非法的 `<style-rule element='table'>` 块（11 条 format-string 规则）；同时在全局 datasource 和 worksheet datasource-dependencies 两处的 `<column>` 定义上添加 `default-format` 属性：达成率/折扣率/同比率用 `p0.00%`，连带率用 `f0.0`，客单价用 `#,##0`。
- 验证状态：XML_OK；全局和 worksheet 两处共 14 个 column 定义均已加 `default-format`，`format-string` style 块已清除，工作簿可正常打开。
- 预防动作：后续设置比率/金额字段格式时，一律在 `<column>` 上加 `default-format='p0.00%'` 等属性，不要在 worksheet `<style-rule>` 中使用 `attr='format-string'`。

### [2026-05-09] 累计达成趋势图的销售日期如果直接以 `none:sales_date:qk` 挂到 tooltip，会触发 ATTR 转换报错并导致图表空白

- 触发场景：在上一轮为 `销售趋势分析_累计达成趋势` 补 tooltip 上下文后，用户重开 Tableau 看到整张图变空白；Marks 卡中的 `销售日期` 变成红色 tooltip pill，并提示“由于无法使用 ATTR() 将字段 销售日期 转换为度量，因此无法在工具提示中显示该字段”。
- 报错 / 现象：worksheet 无法正常渲染，图表区域变空白；tooltip 相关错误明确指向 `销售日期`。
- 根因判断：上一轮虽然方向正确，知道 `Measure Names / Multiple Values` 结构需要显式 `<tooltip column='...'>` 才能让自定义 tooltip 令牌生效，但对日期维度用了错误实例 `none:sales_date:qk`。这是列架上的连续 / 定量实例，不是 tooltip 里可聚合显示的 Attribute 实例。Tableau 在 tooltip 上尝试把它包成 `ATTR()` 时失败，于是把 pill 标红并让整张图进入失效状态。
- 修复动作：先创建备份 `销售部自动化日报.backup_tooltip_blank_fix_20260509_152242.twb`；随后在 `销售趋势分析_累计达成趋势` 的 local `datasource-dependencies` 中新增 `column-instance column='[sales_date]' derivation='Attribute' name='[attr:sales_date:ok]' type='ordinal'`，并把 pane `<encodings>` 和 `customized-tooltip` 中的日期引用统一从 `[none:sales_date:qk]` 改为 `[attr:sales_date:ok]`。累计实际、累计目标、去年同期累计实际的 tooltip 编码保持不变。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续凡是给 `Measure Names / Multiple Values` 图补 tooltip 上下文，维度类字段不能直接拿列架上的 `none:...:qk` 或其它连续实例复用；如果要在 tooltip 中显示日期 / 分类维度，优先补对应的 `Attribute` column-instance，再让 tooltip 引用 `attr:...` 版本。

### [2026-05-09] 累计达成趋势图如果使用 Measure Names / Multiple Values，自定义 tooltip 中额外指标必须显式挂到 pane 的 tooltip 编码

- 触发场景：用户在 `销售趋势分析_累计达成趋势` 中看到 tooltip 编辑器里的 `累计实际`、`累计目标`、`去年同期累计实际` 全部变成红色占位符，悬浮时只显示字面量 `<累计实际>`、`<累计目标>`、`<去年同期累计实际>`，不显示对应数值。
- 报错 / 现象：`销售日期` 仍能正常显示，但三个累计指标在 tooltip 里都没有被替换成数值；Tableau 编辑器将这些字段标红，说明当前 mark 上下文里它们并不是可解析的 tooltip 字段。
- 根因判断：这张图的 pane 采用的是 `rows=[Multiple Values] + color=[Measure Names]` 的多指标折线结构。此前 XML 只在 `<encodings>` 里保留了 `color=[Measure Names]`，没有把 `sales_date`、`sum:cum_actual_amt:qk`、`sum:cum_target_amt:qk`、`sum:last_year_cum_actual_amt:qk` 显式挂到 tooltip 上下文；因此 `customized-tooltip` 里的令牌虽然还在 XML 中，但对当前 pane 来说属于失效引用，Tableau 会在编辑器中标红，并在悬浮时退化成字面文本。
- 修复动作：先创建备份 `销售部自动化日报.backup_tooltip_fix_20260509_151814.twb`；随后仅修改 `销售趋势分析_累计达成趋势` 对应 pane 的 `<encodings>`，在 `color=[Measure Names]` 之后追加 4 个显式 tooltip 编码：`[none:sales_date:qk]`、`[sum:cum_actual_amt:qk]`、`[sum:cum_target_amt:qk]`、`[sum:last_year_cum_actual_amt:qk]`。未改动图形结构、筛选器、计算字段和 tooltip 文案本身。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；用户侧 Tableau 重开与悬浮验证待继续执行。
- 预防动作：后续凡是 `Measure Names / Multiple Values` 结构的图，如果自定义 tooltip 需要显示“除当前色彩编码之外的额外指标”，必须先把这些字段显式挂到 pane 的 `<encodings><tooltip column='...' /></encodings>` 中；不能只在 `customized-tooltip` 文案里写令牌，否则很容易出现“编辑器里字段标红、悬浮时显示字面量占位符”的假成功状态。

### [2026-05-09] 是否最新报告日第二轮收口后，root 层只保留 1 个共享字段和 1 个门店明细专用字段

- 触发场景：用户明确确认“最多保留 1 个共享的 `是否最新报告日`，门店明细保留 1 个独立 caption 版本，其余 8 个都在改写引用后删除”，要求在完成备份后直接落修。
- 报错 / 现象：即便完成第一轮零引用去重后，`ds_ads_store_daily_report_basic` 的字段面板里仍然有多份 `是否最新报告日`，因为 KPI02~KPI06、渠道达成概览 3 张图和销售贡献占比仍各自保留了一份 root calculation。
- 根因判断：这组重复项并不是 8 个不同业务字段，而是 8 个“按 worksheet 复制出来的同一布尔过滤器”；它们的公式完全一致，差异只在 `Calculation_...` 名称。真正需要保留的 root 语义只有两个：全页共享的最新报告日，以及门店明细专用 caption 的 `是否最新报告日_门店明细`。
- 修复动作：先创建备份 `销售部自动化日报.backup_latest_report_dedupe_20260509_145544.twb`；随后将 KPI02~KPI06、`渠道达成概览_日达成率对比`、`渠道达成概览_月达成率对比`、`渠道达成概览_销售贡献占比` 这 7 张 sheet 的 local 过滤链统一改写到共享字段 `Calculation_1730010000000001`；同时把 `直营贡献占比` 根级公式里对 `Calculation_1730010000000201` 的引用改成 `Calculation_1730010000000001`；最后删除 8 个重复 root 字段：`0002`、`0003`、`0004`、`0005`、`0006`、`0201`、`0211`、`0221`。门店经营明细继续保留 `Calculation_1730010000000401` 不动。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；重建后的盘点报告显示 root calculation 从 25 降到 17，总 calculation 从 49 降到 41，且“是否最新报告日”语义簇当前 root 只剩 `Calculation_1730010000000001` 与 `Calculation_1730010000000401` 两个。
- 预防动作：后续遇到“所有 sheet 都需要 latest-report 过滤”时，默认先复用共享 root 字段，再按确有展示语义差异的模块决定是否保留第二个 caption 版本；禁止再按 sheet 批量新建 `Calculation_...0002/0003/...` 这种同公式 latest-report 字段。

### [2026-05-09] ds_ads_store_daily_report_basic 第一轮安全去重只删除 6 个零引用 root calculation，root 条目数从 31 降到 25

- 触发场景：用户确认要“基于当前 twb 先做一版带备份的去重清理，只动确定无引用风险的重复项”，要求不是继续分析，而是对当前工作簿执行最小风险落地。
- 报错 / 现象：上一轮盘点已确认 `ds_ads_store_daily_report_basic` 有 31 个 root calculation、24 个 worksheet-local 副本；其中部分 root 字段不仅公式重复，而且在整个 twb 中除定义本身外没有任何外部引用，属于纯粹的数据源字段污染。
- 根因判断：`直营日达成率 / 直营月达成率 / 联营日达成率 / 联营月达成率 / 小程序日达成率 / 小程序月达成率` 这 6 个 root calculation 只是早期为渠道模块逐张复制出来的同公式副本；当前真正被 worksheet 使用的是 `Calculation_1730010000000213`、`Calculation_1730010000000223` 等通用字段或 worksheet-local 副本，因此这 6 个字段已经沦为零引用垃圾字段。
- 修复动作：先在用户工作簿目录创建带时间戳备份，再只删除上述 6 个 root calculation 定义，不动任何被引用字段，也不动任何 worksheet-local `datasource-dependencies`；随后重建 `reports/context_cache/ds_ads_store_daily_report_basic_calc_inventory_20260509.md`。清理后统计为：root calculation 从 31 降到 25，总 calculation 从 55 降到 49，唯一公式数仍为 9。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；去重后报告已更新，确认被删 6 个字段不再出现在当前 twb 中。
- 预防动作：后续若发现某 calculation 在全工作簿中 `refs=0` 且公式簇内已有其它可复用 root 字段，优先走“备份后直接删 root 定义”的最小清理路径；而涉及仍有引用的 root 字段、或任何 worksheet-local calculation 的合并 / 删除，必须先补引用关系盘点，不能沿用本轮这种直接删法。

### [2026-05-09] ds_ads_store_daily_report_basic 当前存在明显 calculation 污染，后续编译 twb 必须先复用语义字段再谈新增

- 触发场景：用户在 Tableau 字段面板中查看 `ds_ads_store_daily_report_basic` 时，明确指出“计算字段有很多重复项、冗余项”，要求先整理当前 twb 的 calculation 列表，并要求后续编译 `.twb` 时避免继续制造这类无意义计算字段污染。
- 报错 / 现象：同一个数据源下能看到大量 `Calculation_...` 字段，caption 和公式重复度很高；例如 `是否最新报告日`、`渠道组`、`日达成率`、`月达成率`、`同比率` 都存在多份 root 级副本，同时部分 worksheet 里还有同公式的 local 副本。
- 根因判断：当前 `销售部自动化日报.twb` 在多轮手工 XML / 试错编译过程中，多次采用“为每个 worksheet 单独新建 calculation”的方式推进，导致 `ds_ads_store_daily_report_basic` 出现 root 级重复字段；与此同时，多个 worksheet 的 `datasource-dependencies` 又落了同公式 local 副本，形成 root + local 双层冗余。经本轮盘点，当前该数据源共有 55 个 calculation 条目，但按公式归并后只有 9 个唯一语义，其中 24 个 local 条目与 root 完全同公式重复。
- 修复动作：本轮未直接删字段，而是先完成结构化盘点并落报告到 `reports/context_cache/ds_ads_store_daily_report_basic_calc_inventory_20260509.md`，明确当前 8 个主要重复簇：`是否最新报告日`、`日达成率`、`月达成率`、`渠道组`、`同比率`、`排名`、`目标缺口`、`负责人_展示`；同时将“先复用、后新增”的编译约束回灌到交接与 repo memory，避免后续继续放大污染面。
- 验证状态：已完成 XML 只读盘点与明细报告生成；本轮未做删除式清理，因此不存在新增渲染风险。
- 预防动作：后续对 HEFANG `.twb` 做编译 / 修补时，必须先按“公式归一化 + caption 归类”扫描现有 datasource 字段；若 root 级已有同语义 calculation，则直接复用已有 `name`，不要再生成新的 `Calculation_...`。除非公式、粒度或显示语义确实不同，否则禁止因为 worksheet 名不同就复制 `是否最新报告日`、`渠道组`、`日达成率`、`月达成率` 一整套字段。worksheet-local `datasource-dependencies` 也应只保留目标 sheet 真正新增的局部语义，不能机械复制 root 级 calculation 进来。

### [2026-05-09] 渠道达成概览的日达成率对比、月达成率对比如果误绑到贡献占比公式，会表现成两张图数值完全一样

- 触发场景：用户重开首页第二行“渠道达成概览”后反馈“这两个模块的计算逻辑有错，数值相同”，截图中 `日达成率对比` 与 `月达成率对比` 三个渠道的百分比完全一致。
- 报错 / 现象：两张图标题不同，但三条横条都显示成同一组数值；视觉上像是“日达成”和“月达成”都被算成了同一个指标。
- 根因判断：`渠道达成概览_日达成率对比` 当前绑定的 `Calculation_1730010000000213`、以及 `渠道达成概览_月达成率对比` 绑定的 `Calculation_1730010000000223`，都被误写成了 `SUM([mtd_sales_amt]) / FIXED 总 mtd_sales_amt` 的贡献占比公式，因此两张图实际都在画月累计贡献占比，而不是日达成率 / 月达成率。
- 修复动作：将 `Calculation_1730010000000213` 改回 `IF SUM([day_target]) = 0 THEN NULL ELSE SUM([day_sales_amt]) / SUM([day_target]) END`，专门服务 `日达成率对比`；将 `Calculation_1730010000000223` 改回 `IF SUM([month_target]) = 0 THEN NULL ELSE SUM([mtd_sales_amt]) / SUM([month_target]) END`，专门服务 `月达成率对比`；并同步把两个字段 caption 改回 `日达成率`、`月达成率`，避免后续在字段面板里继续混淆。
- 验证状态：已完成 XML 文法校验；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续在当前 workbook 里复用“渠道组 + 最新报告日”模板时，不能只复制 `caption + 过滤器 + 颜色`，必须逐个复核 measure 公式是否仍对应当前图的业务语义；若出现“不同图标题但数值完全相同”，优先检查是否误复用了上一张图的 calculation 节点。

### [2026-05-09] 用户只要日销售趋势图图例时，静态 legend worksheet 也必须只描述日图两条指标

- 触发场景：用户明确指出“累计达成趋势图之前的图例就 OK，很好了；我要的是日销售趋势图的图例”，说明此前把累计图和日图说明混在同一个 legend worksheet 里，属于理解偏差。
- 报错 / 现象：虽然 static legend worksheet 已经能稳定挂进 dashboard，但文字内容同时写了累计图和日图，导致用户仍觉得“要的日图图例没有被正确表达”。
- 根因判断：此前把“静态图例 worksheet 稳定显示”当成了主要目标，却没有把 legend 的作用域限制在用户当前真正关注的 `日销售趋势图`，结果图例内容方向错了。
- 修复动作：将 `销售趋势分析_趋势图例` 收口为只服务日销售趋势图：使用稳定的 `data_version` 单值 mark 承载文本，只保留两条说明 `柱：当日实际`、`线：当日目标`；不再混入累计图的目标或去年同期累计说明。
- 验证状态：已完成 XML 解析校验；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续做静态 legend worksheet 时，先明确“图例服务哪一张图、哪几个指标”；不要把多个图的说明合并到一块 legend 里，以免结构稳定了但语义仍然跑偏。

### [2026-05-09] 自动 color legend zone 在当前 dashboard 中不稳定时，改用静态 legend worksheet 更稳

- 触发场景：用户持续反馈“日销售趋势图还是没有图例”，说明此前插入的 `type-v2='color'` legend zone 即使 XML 合法，也没有稳定转化成用户可见的图例。
- 报错 / 现象：dashboard 中曾存在自动 color legend zone，但用户侧实际视觉仍接近“没有图例”；反复调整 zone 高度和布局后，问题依旧不稳定。
- 根因判断：当前旧工作簿里，自动 color legend zone 的可见性高度依赖 Tableau 运行时对 `pane-specification-id`、布局和 worksheet 关联的回写，结构层面即使合法，也不保证每次都按预期展示。
- 修复动作：放弃继续依赖自动 color legend，新增一个独立的 `销售趋势分析_趋势图例` worksheet，用文本标记直接写死颜色说明：当日实际 / 累计实际、当日目标、累计目标、去年同期累计；再把 dashboard 中原 legend zone 替换成这个稳定的 legend worksheet 区块。
- 验证状态：已完成 XML 解析校验；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续在 HEFANG 当前这本旧 `.twb` 中，如果自动 color legend 多次合法但仍不稳定显示，优先切换到“静态 legend worksheet”方案，不再在 `type-v2='color'` 上继续消耗时间。

### [2026-05-09] 折线 pane 中 `customized-label` 写在 `customized-tooltip` 前面，会直接触发 D2E8DA72

- 触发场景：用户重开工作簿时报错 `D2E8DA72`，错误消息明确指出 `element 'customized-tooltip' is not allowed`，定位到 `销售部自动化日报.twb` 第 2225 行附近。
- 报错 / 现象：Tableau 无法完成工作簿加载，整本 `.twb` 被 schema 拦截，无法进入可视化验证阶段。
- 根因判断：在 `日销售趋势图` 第二层折线 pane 里，为了强制显示目标线数值，新增了 `customized-label`；但节点顺序写成了 `encodings -> customized-label -> customized-tooltip -> style`。当前 Tableau XSD 要求这里必须是 `encodings -> customized-tooltip -> customized-label -> style`。
- 修复动作：保持显式 `text` 编码与自定义标签不变，只把 `customized-label` 移到 `customized-tooltip` 后面，恢复为 Tableau 接受的节点顺序。
- 验证状态：已完成 XML 解析校验，当前 `.twb` 结构重新合法；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续在 pane 内同时使用 `customized-tooltip` 与 `customized-label` 时，严格按 Tableau 内容模型顺序落位，不要只看语义是否完整；否则即使字段都合法，也会在加载期被 XSD 直接拦截。

### [2026-05-09] 日趋势折线标签需要显式 text 绑定，legend zone 过矮也会表现成“像没图例”

- 触发场景：用户再次反馈 `日销售趋势图` 仍想看到折线数据，同时 dashboard 里仍感觉“缺少图例”。
- 报错 / 现象：左图主轴已经出现，但目标折线的数据标签仍不稳定；右图 dashboard 虽然已有 color legend zone，但高度过矮、布局过扁时，实际表现接近“图例不存在”。
- 根因判断：仅靠 `mark-labels-show=true` 仍可能被 Tableau 默认标签策略吞掉；而 dashboard color legend zone 如果高度太小、横向排布太挤，也会让 legend 实际不可读。
- 修复动作：为左图第二层 `Line` pane 补显式 `text` 编码和 `customized-label`，强制目标线显示数值；同时把 dashboard 中累计趋势图的 legend zone 从低矮横排改成更高的纵向区块，给图例留出稳定显示空间。
- 验证状态：待本轮 XML 解析校验与用户侧 Tableau 重开验证。
- 预防动作：后续遇到 Tableau 线图标签“偶尔显示、偶尔不显示”时，不只调 `mark-labels-show`；优先加显式 `text` 编码。dashboard 图例如果已有结构却看不见，也优先检查 zone 尺寸与排布，而不是立即判定 legend 绑定失败。

### [2026-05-09] 当前 ds_ads_daily_sales 只有“去年同期累计实际”，没有现成“去年同期日销”字段

- 触发场景：用户继续要求趋势区补“去年同期”折线并区分颜色。
- 报错 / 现象：当前工作簿 datasource 明确存在 `last_year_cum_actual_amt`，但不存在 `last_year_day_actual_amt` 或等价的“去年同期日销”物理字段，因此左图无法像右图那样直接挂一条现成的去年同期折线。
- 根因判断：`ds_ads_daily_sales` 当前字段边界只覆盖 `day_actual_amt`、`day_target_amt`、`cum_actual_amt`、`cum_target_amt`、`last_year_cum_actual_amt`；右图的去年同期线来自累计字段，左图若要补去年同期日销，必须额外派生表计算字段或等待上游提供物理字段。
- 修复动作：本轮先确保右图已有的“去年同期累计实际”继续使用更深的冷灰色，与累计实际深蓝、累计目标金橙做清晰区分；左图则先不伪造不存在的去年同期日销线。
- 验证状态：字段存在性已通过当前目标 `.twb` datasource 片段核对确认。
- 预防动作：后续遇到“目标图里有上一年日销线”这类诉求时，先核对 datasource 是否真有对应字段；如果只有累计字段，不要把累计字段误挂到日销图里充当去年同期日销。

### [2026-05-09] Tableau 重开后把日趋势第二层回写成 Automatic pane，导致折线标签丢失且主轴继续隐藏

- 触发场景：用户继续重开 `日销售趋势图` 后反馈需要“纵轴要出现，折线也要显示数据”。
- 报错 / 现象：虽然折线还能画出来，但左侧主轴仍被隐藏；同时第二层 pane 被 Tableau 回写成 `mark class='Automatic'`，导致此前手工加的折线 tooltip 与数据标签配置丢失。
- 根因判断：当前工作簿在 Tableau 重开保存后，会把不够稳定的第二层 line pane 简化回 `Automatic`，而我们此前又把主轴 display 设成了隐藏，因此用户看到的是“有线但没轴、也没线标签”的半成品状态。
- 修复动作：把左图主轴 `day_actual_amt` 的 `display` 恢复为 `true`，并将第二层 pane 明确改回 `mark class='Line' + y-index='1'`，同时补齐颜色、tooltip、线宽和 `mark-labels-show=true` 的数据标签样式。
- 验证状态：待本轮 XML 解析校验与用户侧 Tableau 重开验证。
- 预防动作：后续如果 Tableau 重开后出现 pane 被回写成 `Automatic`，优先检查是否因此吞掉了 line pane 的显式样式；涉及“要显示线标签”的需求时，不能只依赖 Automatic mark 的默认行为。

### [2026-05-09] 累计达成趋势图三线对比不明显且 dashboard 无图例，需同时调整色板与补 legend zone

- 触发场景：用户重开工作簿后反馈 `累计达成趋势图` 三条折线颜色不明显，且 dashboard 上没有图例，难以快速区分“累计实际 / 累计目标 / 去年同期累计实际”。
- 报错 / 现象：工作表能正常显示三条线，但原来的蓝灰色差距太小，尤其目标线与去年同期线对比度不足；同时 dashboard 只嵌了 worksheet zone，没有附带任何颜色图例区。
- 根因判断：此前为了先完成第一层结构，只统一了趋势图区样式，没有继续拉开 measure palette，也没有在 dashboard XML 中补 `type-v2='color'` 的 legend zone，因此用户在 dashboard 场景下看不到 measure legend。
- 修复动作：将 `cum_actual_amt` 保持深蓝，将 `cum_target_amt` 改为更醒目的金橙色，将 `last_year_cum_actual_amt` 改为更深的冷灰色；同时把累计趋势图线宽从较细值上调，并在首页 dashboard 上新增一个绑定 `[:Measure Names]` 的颜色图例 zone。
- 验证状态：已完成 XML 解析校验；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续凡是 dashboard 内使用 `Measure Names` 驱动多线图，不能只停留在 worksheet 可显示；还要检查 measure palette 是否足够分离，以及 dashboard 是否显式挂上图例 zone，否则用户在整页浏览时会丢失读图锚点。

### [2026-05-09] 日销售趋势图仍未真正叠轴时，需改用样板级单 class0 轴同步写法

- 触发场景：第一次补 `y-axis-name` 与 `y-index` 后，用户再次截图反馈 `日销售趋势图` 仍然没有真正叠到同一坐标系里。
- 报错 / 现象：柱图与目标线依旧被 Tableau 拆成两块纵向 pane，而不是“柱 + 线”重叠；说明仅补 pane 元数据还不足以驱动当前工作簿里的 dual-axis 同步。
- 根因判断：此前 left chart 的 `axis` 规则仍沿用了 `class='1' + class='0'` 的混合写法，且没有像样板那样同时把两条 rows 轴都 `display=false`；在当前工作簿里，这会让 Tableau 继续把两个 measure 当成上下分层而不是同步双轴。
- 修复动作：将 `日销售趋势图` 的 axis 规则改成样板里的保守版本：只保留 `class='0'` 的 synchronized `space` encoding，删除混合 class 写法，并同时隐藏 `day_actual_amt` 与 `day_target_amt` 两条 rows 轴显示。
- 验证状态：已完成 XML 解析校验；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续手工拼双轴图时，如果第一次补 pane 元数据后仍出现上下分层，不要继续盲调 pane；优先回到 `style-rule element='axis'`，直接复用已验证样板的 synchronized 轴规则。

### [2026-05-09] 日销售趋势图首次手工改成 Bar + Line 后仍上下分层，根因是 dual-axis pane 缺少完整轴元数据

- 触发场景：用户重开工作簿查看第一版 `日销售趋势图` 后，截图显示上半部分只有 `当日实际` 柱，下半部分单独出现 `当日目标` 纵轴，说明目标线没有与柱图叠到同一视图区。
- 报错 / 现象：工作表未报字段失效，但 Tableau 将 `Bar + Line` 渲染成上下两个 pane，而不是同图双轴叠加；同时目标线几乎不可见。
- 根因判断：首次手工 XML 收口时，左图第二层 pane 虽然已写 `y-axis-name`，但第一层 pane 未显式写主轴 `y-axis-name`，且第二层缺 `y-index='1'`；在当前工作簿里，这组 dual-axis 元数据不足以让 Tableau 识别为真正叠轴。
- 修复动作：为柱图 pane 补写主轴 `y-axis-name='[sum:day_actual_amt:qk]'`，为目标线 pane 补 `y-index='1'`，同时补一层 `encodings/color` 并把目标线颜色和粗细从极浅细线调成更可见的蓝灰色线。
- 验证状态：已完成 XML 解析校验；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续手工拼 Tableau 双轴图时，不只检查 `rows=(measure1 + measure2)` 和第二层 pane；必须同时核对第一层 `y-axis-name`、第二层 `y-axis-name`、必要时的 `y-index='1'` 是否齐全，否则 Tableau 可能退化成上下分层的两块 pane。

### [2026-05-09] 首页两张趋势图无法继续依赖 cwtwb 直改，已回退为手工 XML 第一版收口

- 触发场景：用户要求继续按第一层目标效果改进 `销售趋势分析_日销售趋势` 与 `销售趋势分析_累计达成趋势`；前序 MCP 探针已确认当前旧多数据源工作簿里，`cwtwb configure_chart` 无法注册 `ads_daily_sales` 的趋势字段，用户随后明确要求“改走手工 XML 收口两张趋势图”。
- 报错 / 现象：`cwtwb configure_chart` 对两张趋势图持续报 `Unknown field`，已知字段列表只包含 `ads_store_daily_report` 一侧字段，未注册 `sales_date`、`day_actual_amt`、`cum_actual_amt` 等 `ads_daily_sales` 字段，因此无法直接通过 MCP 重配趋势图。
- 根因判断：当前目标 `.twb` 属于多数据源旧工作簿，`cwtwb` 在该工作簿上的字段注册存在缺口；趋势图区实际字段虽已存在于 datasource XML 中，但 MCP 层拿不到对应 field registry，只能作为结构探针，不适合作为最终落盘路径。
- 修复动作：回退到手工 XML 收口。将 `销售趋势分析_日销售趋势` 从单 pane 多度量折线改为“当日实际柱 + 当日目标线”的双轴过渡版，补齐同步轴、透明背景、去网格、tooltip 与数据标签；`销售趋势分析_累计达成趋势` 保留三线结构，统一颜色、背景、网格、tooltip 与标签样式；同时把 `ads_daily_sales` 全局 measure 色板调成更接近目标模板的蓝灰系。
- 验证状态：已完成 XML 解析校验，目标 `.twb` 结构合法；本轮未完成用户侧 Tableau 重开渲染验证，且当前会话中的 `cwtwb open_workbook` 被用户禁用，无法追加 MCP 打开验证。
- 预防动作：后续在 HEFANG 这类旧多数据源 `.twb` 上，只要 `cwtwb configure_chart` 的已知字段列表未覆盖目标 datasource，就不要继续盲试 MCP 落盘；先确认字段注册缺口，再直接走“备份 -> 手工 XML 最小补丁 -> XML 解析验证”的保守路线。

### [2026-05-09] 销售贡献占比基础 Pie 在 Tableau 里只显示局部，根因是 mark 与 pane 调得过大，导致视图区裁切

- 触发场景：用户重开 `销售部自动化日报.twb` 后，在 worksheet 视图里看到 `销售贡献占比` 只显示一小部分颜色块，无法看到完整饼图，并反馈“饼图只能看到小部分”。
- 报错 / 现象：画布本身并不小，但 Pie 显示成被截断的局部色块，说明不是“图太小”，而是 Pie 本体超出当前视图区后被裁切。
- 根因判断：上一轮为了放大基础 Pie，把 `mark size` 提高到 `3.6`、pane 高度提到 `180`，dashboard zone 也一起放大；在当前 worksheet / dashboard 组合下，这组参数过大，反而让 Pie 超出可见区域，只剩局部落在画布里。
- 修复动作：把 `渠道达成概览_销售贡献占比` 的 `mark size` 从 `3.6` 收回到 `2.2`，把 pane 高度从 `180` 收回到 `132`，并把 dashboard 对应 zone 从 `11000` / `7000` / `7000` 调整为更均衡的 `8400` / `8300` / `8300`，优先保证完整可见而不是继续极限放大。
- 验证状态：已完成 XML 解析校验，并确认修复后的目标 `.twb` 能再次被 `cwtwb open_workbook` 正常打开；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续调单层 Pie 时，不把“图偏小”直接等同于继续放大；若 Tableau 已经出现“只看到局部色块”的现象，应优先判断为 mark / pane 过大导致的裁切，再往回收而不是继续加码。

### [2026-05-09] 用 cwtwb 重搭销售贡献占比基础 Pie 时，save_workbook 生成了非法 worksheet 结构，需回退到手工收口

- 触发场景：用户要求用 `cwtwb` 重搭 `渠道达成概览_销售贡献占比` 的基础 Pie 模块，先执行了 `open_workbook -> configure_chart(mark_type=Pie) -> save_workbook`。
- 报错 / 现象：`save_workbook` 在写出临时 `.twb` 时触发 Tableau XSD 校验失败，报错明确指向目标 worksheet 中 `datasource-dependencies` 节点位置非法，且 `encodings` 节点也出现在当前 schema 不接受的位置，导致 cwtwb 结果无法直接落盘。
- 根因判断：当前目标工作簿属于已有结构较重的旧 `.twb`，`cwtwb configure_chart` 在该 worksheet 上重写 Pie 编码时，没有完整继承原始 worksheet 的内容模型顺序，导致保存阶段生成的临时 XML 不满足 Tableau XSD。
- 修复动作：保留 cwtwb 探出的目标形态，只把“基础 Pie”需要的最小变更手工回填到原工作簿：为目标 worksheet 补 `mtd_sales_amt` measure 实例，移除旧的自定义标签引用，改成合法的 `Pie + color(渠道组) + wedge-size(月累计销售额) + text(渠道组)` encodings，同时把 mark size、pane height 和 dashboard 对应 zone 高度一并调大。
- 验证状态：已完成 XML 解析校验，并确认修复后的目标 `.twb` 能再次被 `cwtwb open_workbook` 正常打开；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续在已有 HEFANG `.twb` 上使用 `cwtwb configure_chart` 时，不能默认认为 `save_workbook` 一定能直接落盘成功；若命中 XSD 失败，应优先读取报错 worksheet 的临时结构，提炼出 cwtwb 期望编码后再做最小手工收口，而不是反复盲试保存。

### [2026-05-09] 销售贡献占比切到 LaDataViz Donut 扩展后只显示英文占位提示，根因是扩展已挂载但编码未绑定

- 触发场景：用户在 Tableau 中把 `渠道达成概览_销售贡献占比` 切换到 Donut 拓展后，画面只显示 `Donut Chart` 帮助文案，没有真实图形。
- 报错 / 现象：工作表未报 XML 或字段失效错误，但扩展画面提示需要把字段拖到 `Sections` 和 `Angle` 等编码槽位，说明当前只是扩展壳体已加载，业务字段还没有真正绑定进去。
- 根因判断：目标 `.twb` 中已经存在 `com.ladataviz.extension.donut` 的 `add-in` 和 `referenced-extension` 注册信息，但 `instance-settings` 为空，导致 Donut 扩展启动后拿不到 `Sections`、`Angle(values)`、`Color`、`KPI` 的字段映射。
- 修复动作：为 `渠道达成概览_销售贡献占比` 的 Donut 扩展补写 `instance-settings`，绑定 `sections=渠道组`、`values=SUM(mtd_sales_amt)`、`color=渠道组`、`kpi=贡献占比`，并补充 `dialog-status` 与 `setting-key-version` 通用设置。
- 验证状态：已完成 XML 解析验证，并确认目标 worksheet 片段中存在 4 个 Donut 编码键；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续把 worksheet 切换到 VizExtension 时，不能只保留 `mark class='VizExtension'` 和 `add-in` 注册，还要同步检查 `instance-settings` 是否已完成最小字段绑定；否则 Tableau 只会显示扩展自带的引导占位页。

### [2026-05-09] 销售贡献占比的 donut 结构在当前工作簿中渲染不稳定，已按用户要求回退为基础 Pie

- 触发场景：补齐双轴同步与隐藏纵轴后，用户重开工作簿仍反馈 `销售贡献占比` 的 donut 效果不可用，并明确要求“不要环形图了，改回基础 pie 饼图”。
- 报错 / 现象：当前工作簿里的 donut 方案即使 XML 结构完整，用户侧实际渲染仍未达到预期，继续在单 worksheet 上追加 donut 细节的收益已经低于直接回退稳定方案。
- 根因判断：本次问题不再是 XML 解析错误，而是当前目标 `.twb` 对这张图的双层 Pie / 双轴 donut 渲染兼容性不足；在首页左侧当前容器与工作簿上下文下，继续强推 donut 会放大反复调试成本。
- 修复动作：按用户要求，将 `渠道达成概览_销售贡献占比` 从 donut 结构回退为单层基础 `Pie`：移除双轴同步配置、移除内圈白色 Pie 挖空层、删除 duplicated rows 表达式，只保留单 pane 的 `Pie + color + wedge-size + label` 基础编码。
- 验证状态：已完成 XML 解析验证，并确认当前 worksheet 只保留单层 Pie pane；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续如果用户优先要求“稳定可用”而非强制环形图，应优先回退到单层 Pie 或其它稳定图形，不在单 worksheet donut 路线上无限追加局部修补。

### [2026-05-09] 销售贡献占比已改成双层 Pie 仍出现“三个 pie + 纵轴”，根因是缺少双轴同步与轴隐藏配置

- 触发场景：用户重开 `销售部自动化日报.twb` 后反馈 `渠道达成概览_销售贡献占比` 仍“没有变成环形图”，截图中同时出现多个 Pie 和一条可见纵轴。
- 报错 / 现象：工作表并未报字段失效，但画面不是一个居中的 donut，而是上下拆开的多个 Pie，且右侧残留连续轴，视觉上明显偏离图2目标。
- 根因判断：此前虽然已经把 `rows` 改成同一 measure 的双轴表达，也补了内层白色 Pie，但缺少工作双轴 donut 所需的完整轴元数据，尤其是 `axis` 的 `synchronized='true'`、两条 rows 轴 `display='false'` 以及 tick / table 背景隐藏设置；Tableau 因此仍把两层 Pie 当成未完全同步的独立 pane 渲染。
- 修复动作：参考本机样板 `Uber Dashboard.twb`，为 `渠道达成概览_销售贡献占比` 补齐 `style-rule element='axis'` 下的双轴同步编码、两条 rows 轴隐藏、透明 tick 颜色，并补充透明 table 背景；同时把内圈恢复为白色 `Pie` 挖空层，保留 `rows=(measure + measure)` 的双轴结构。
- 验证状态：已完成 XML 解析验证，并确认目标 worksheet 片段中已写入同步双轴与隐藏纵轴配置；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续在 Tableau `.twb` 中实现 donut 时，不能只复制“外层 Pie + 内层白色 mark + duplicated rows”这三步，必须把完整的双轴同步与轴隐藏样式一起迁入，优先对照已验证样板逐项核对。

### [2026-05-09] 销售贡献占比从普通 Pie 升级为 donut，需要双层 Pie 叠加，而不是只调单层 Pie 大小

- 触发场景：用户确认单层 Pie 无论放大还是缩小都难兼容当前首页左侧容器，并明确要求把 `销售贡献占比` 升级成图2那种环形图。
- 报错 / 现象：单层 Pie 会在“太小看不清”和“太大挤爆容器”之间反复摆动，无法稳定落到图2那种环形主视觉。
- 根因判断：当前 Tableau `.twb` 里的 `销售贡献占比` 最初只是单层 `Pie` mark；这种写法只能调整外圈整体大小，不能形成真正的中间留白，因此难以在有限容器内兼顾可读性和视觉平衡。
- 修复动作：参考本机样板 `Uber Dashboard.twb` 的双层 Pie 模式，把 `渠道达成概览_销售贡献占比` 改成 donut 结构：外层 pane 使用 `Pie + wedge-size + 渠道组着色`，内层 pane 再叠一层更小的白色 `Pie` 形成挖空效果，并把 `rows` 改成同一 measure 的双轴表达。
- 验证状态：已完成 XML 解析验证；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续用户要求环形图时，不再通过单层 Pie 反复调 `mark size` 试错；优先直接按“双层 Pie / 双轴 Pie”模式实现。

### [2026-05-09] 渠道达成概览的 Pie 首次落地后过小，原因是 mark size 与容器高度都偏保守

- 触发场景：用户重开 `销售部自动化日报.twb` 后反馈左上 `销售贡献占比` 虽然已切成 Pie，但图形本体太小，视觉上只剩中间一个很小的圆点。
- 报错 / 现象：Pie 没有加载失败，但有效可视面积过小，无法承担图2里“渠道概览主视觉”的作用。
- 根因判断：首次把 `销售贡献占比` 从 `Bar` 改成 `Pie` 时，保留了较保守的 `mark size=1.35`，同时 dashboard 左侧三块图仍按近乎均分高度排布，导致 Pie 实际占用空间过小。
- 修复动作：将 `渠道达成概览_销售贡献占比` 的 `mark size` 提高到 `3.6`，pane 高度固定到 `180`，并把 dashboard 左侧容器中该图的 zone 高度从 `8300` 提高到 `11000`，为 Pie 释放更多垂直空间；对应地将下方两张对比条形图高度压缩到 `7000`。
- 验证状态：已完成 XML 解析验证；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续把 `Bar` 改造为首页主视觉 `Pie` 时，不能只替换 mark 类型，必须同时检查 `mark size`、pane 高度和 dashboard zone 分配，至少改其中两层。

### [2026-05-08] KPI06“目标缺口”只改了 worksheet 局部定义，未同步 datasource 全局 calculation，导致 Tableau 仍按时间进度口径取值

- 触发场景：用户重开 `销售部自动化日报.twb` 后检查首页第 6 张 KPI，发现“目标缺口”显示为 `0.2258`，且 Tableau worksheet 左侧标记卡显示为“聚合(时间进度)”。
- 报错 / 现象：首页第 6 张 KPI 虽然标题已改成“目标缺口”，但数值仍等于时间进度 `22.58%` 对应的小数值，而不是“月目标 - 月累计销售额”的缺口金额。
- 根因判断：此前只修改了 `KPI06_目标缺口` worksheet 内部 `datasource-dependencies` 的局部 calculation 定义，没有同步修改 datasource 根级 `<column name='[Calculation_1730010000000016]'>` 的全局定义；Tableau 重开后仍按全局的旧公式 `MAX([time_progress])` 解析该 calculation id。
- 修复动作：将 datasource 根级 `Calculation_1730010000000016` 从“时间进度”改为“目标缺口”，公式改为 `IF SUM([month_target]) - SUM([mtd_sales_amt]) > 0 THEN SUM([month_target]) - SUM([mtd_sales_amt]) ELSE 0 END`，并把默认格式改为金额样式 `n#,##0;-#,##0`。
- 验证状态：已完成 XML 解析验证，并确认全局旧定义已被新定义替换；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续直接编辑 `.twb` 修改已有 calculation id 时，必须同时检查 datasource 根级定义和 worksheet 局部 `datasource-dependencies` 定义，不能只改其中一层。

### [2026-05-08] 门店经营明细把“排名”当连续轴，导致负责人右侧出现“月100”样式并把指标表头压到表格底部

- 触发场景：用户重开 `销售部自动化日报.twb` 后检查首页第四行 `门店经营明细_门店排名`，发现 `负责人_展示` 右侧不是“排名”列，而是一条类似“月 100”的异常轴样式；同时 `同比率`、`日达成率`、`日销售额`、`月累计销售额` 等指标表头落在表格底部。
- 报错 / 现象：工作簿可以打开，但门店明细表结构错误，阅读顺序被破坏；业务上应显示为离散“排名”列的位置，被 Tableau 渲染成连续刻度列。
- 根因判断：当前 worksheet 的 `<rows>` 结构把 `[none:mtd_rank:qk]` 作为定量字段乘进了离散维度链，导致 Tableau 把排名当连续轴处理；连续轴夹在维度列与 Measure Names 之间时，会把该列渲染成刻度，并连带影响指标表头的正常顶部显示。
- 修复动作：新增离散计算字段 `Calculation_1730010000000406` 作为“排名”展示列，公式按 `mtd_rank` 输出字符串；将门店明细 worksheet 的 `<rows>` 改为纯离散维度链：店铺等级 -> 日报渠道类型 -> 经营实体名称 -> 负责人_展示 -> 排名；同时按业务阅读顺序重排 `:Measure Names` 过滤成员。
- 验证状态：已完成 XML 解析验证，并确认目标 `.twb` 中已引用新的离散排名列；用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续在 Tableau crosstab 中展示“排名”“序号”“TopN 位次”时，不直接把数值 measure 混入 rows 连续轴；如需要展示为表格列，优先转成离散显示字段，再挂到 rows 维度链中。

### [2026-05-08] 门店经营明细 worksheet 的 `manual-sort` 导致工作簿加载失败

- 触发场景：为首页第四行新增 `门店经营明细_门店排名` 表后，用户重开目标工作簿时报错，Tableau 提示 `manual-sort` 元素不被当前内容模型接受。
- 报错 / 现象：工作簿无法打开；报错中明确指出 `manual-sort` 没有声明，且 `datasources, datasource-dependencies, filter, sort, perspectives, slices, aggregation` 的内容模型里不允许该元素。
- 根因判断：当前目标工作簿虽然能接受其它排序节点，但在新增门店明细 worksheet 的当前位置不接受 `manual-sort` 这种写法；直接复制参考表格的排序节点会触发 schema 拦截。
- 修复动作：先删除门店经营明细 worksheet 中的 `manual-sort` 节点，优先恢复工作簿可打开状态；字段顺序问题留待后续基于真实渲染再收口。
- 验证状态：已完成 XML 解析验证；用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续为当前目标工作簿新增 crosstab 时，不默认复制参考 workbook 的 `manual-sort`；先保证能打开，再用更保守的排序方式或 Tableau UI 二次收口列顺序。

### [2026-05-08] 复制 `ds_ads_daily_sales` 时把 `extract` 一起带入，导致工作簿无法打开

- 触发场景：为首页第三行新增“销售趋势分析”双图时，从参考工作簿复制 `ds_ads_daily_sales` 数据源结构到目标工作簿。
- 报错 / 现象：用户重开工作簿时报 `D2E8DA72`，错误消息指向 `extract` 节点的 `user-specific` 与 `object-id` 属性未声明。
- 根因判断：复制参考 workbook 的 datasource 时把 `extract` 节点、Hyper 路径和刷新元数据一起带入，而目标工作簿当前 schema 不接受这些属性。
- 修复动作：删除该 datasource 下复制过来的 `extract` 节点，只保留 live datasource 结构。
- 验证状态：已完成 XML 解析验证；用户后续已确认工作簿可再次打开。
- 预防动作：后续从其它 `.twb` 借 datasource 结构时，默认不复制 `extract`、Hyper 路径、refresh 元数据和本机特定缓存信息，优先保留 live connection + metadata-records + column / column-instance。

### [2026-05-08] 渠道达成概览“贡献占比”报聚合 / 非聚合混用错误

- 触发场景：用户重开工作簿验证首页第二行“渠道达成概览”三张卡时，`贡献占比` 计算字段报错。
- 报错 / 现象：Tableau 提示“无法将聚合和非聚合参数与此函数混合”，`贡献占比` 字段无效，导致渠道卡无法正常显示。
- 根因判断：`SUM([mtd_sales_amt])` 与裸 `FIXED` 分母直接参与除法，Tableau 把该分母判为非聚合量；同时三张卡最初共用了相同 caption，增加了排障成本。
- 修复动作：将分母改为显式聚合表达式，例如 `MIN({ FIXED : SUM(IF ... THEN [mtd_sales_amt] END) })`，并将三组字段 caption 改为带渠道前缀，避免同名混淆。
- 验证状态：已完成用户侧 Tableau 实际渲染验证，三张渠道卡正常显示。
- 预防动作：后续凡是 Tableau 比例口径，都必须遵循“汇总后计算”；若 `FIXED` 结果要参与四则运算，默认再包一层显式聚合后再使用。

### [2026-05-21] `实时战情_今日累计销售进度` 没值时，要优先核对当前 worksheet 绑定的 datasource alias 和本地 `datasource-dependencies`，不能只改 workbook 里同名 root calculation

- 触发场景：用户在 `HEFANG门店实时销售战情看板.twb` 中明确指出，`实时战情_今日累计销售进度` 不是样式问题，而是 `今日累计销售额_实时累计趋势` 到 11 点仍然接近 0，柱体整体扁平。
- 报错 / 现象：当前工作表实际使用的是 `federated.3cumprogresstargetlive` 这套 datasource，本地 `Measure Names / Multiple Values` 绑定到 `usr:Calculation_202605142004:qk`；如果只按 caption 在整个 workbook 搜索，同名 calculation 还会在别的 realtime datasource 中出现，容易误改到不影响当前 sheet 的残留副本。
- 根因判断：`ds_realtime_cum_progress_target_live` 这条链里，`Calculation_202605142004` 原先间接依赖 `[SALES_AMT]`，而 `[SALES_AMT]` 的 LOD 键写成了 `[store_id]`。在当前 logical table / relationship 语义下，这个键来自 target scope 侧，不稳定地压扁了 Hourly Sales 侧的实时累计值；与此同时，真正驱动当前 worksheet 的是该 sheet 自己的 `datasource-dependencies` 副本，而不是 workbook 里所有同名 root calculation 的任意一处。
- 修复动作：把 `ds_realtime_cum_progress_target_live` 的 root 定义和当前 worksheet 本地副本同时改成统一口径：`Calculation_202605142004` / `Calculation_202605142005` 直接聚合 `ZN([SALES_AMT_RAW])`；`SALES_AMT` 的 LOD 键改为 `[STORE_ID (Hourly Sales)]`；保留 `今日目标进度线` 全天虚线定义不变，不再触碰 `Calculation_202605151040`。
- 验证状态：已完成 PowerShell XML 解析校验，结果 `XML_OK`；并确认当前 workbook 中 `federated.3cumprogresstargetlive` 的 root 与 worksheet local 副本都已切到 `SALES_AMT_RAW + STORE_ID (Hourly Sales)` 口径。用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续修复 Tableau `.twb` 中“同 caption、同 calculation id 在多套 datasource 并存”的问题时，必须先锁定当前 worksheet 的 datasource alias、`rows/cols` 和 `:Measure Names` 绑定，再定向核对该 worksheet 的本地 `datasource-dependencies`；不能仅凭全局 grep 命中就认定改到了真正生效的字段。

### [2026-06-01] `ds_kpi_same_store_yoy_physical_live` 把免税门店混入 same-store KPI，导致同比被大幅拉低

- 触发场景：用户核对 `销售部自动化日报.twb` 的 2026-05-31 顶部 KPI，指出业务口径应为“同店同比 2.2% / 同店+快闪同比 4.5%”，但 Tableau 当前显示 `-14.09% / -12.15%`。
- 报错 / 现象：工作簿可以正常打开，但 `KPI05_同店同比` 与 `KPI09_同店+当期快闪同比` 的数值明显偏低，与业务核算不一致。
- 根因判断：当前 `ds_kpi_same_store_yoy_physical_live` 的 Custom SQL 直接汇总 `ads_store_daily_report.same_store_*`，没有排除 `is_duty_free='Y'`。结果 6 家免税门店在 2026-05-31 带入了 `2,377,107.50` 的去年同期分母，但当前同店分子为 `0`；这些门店又只在 ADS 月累计达成链路承接 `external_mtd_sales_amt`，并未进入 same-store 辅助金额，最终把 KPI 从业务口径 `2.20% / 4.51%` 拉成 `-14.09% / -12.15%`。
- 修复动作：对 `销售部自动化日报.twb` 中 `ds_kpi_same_store_yoy_physical_live` 的两份 Custom SQL 副本同时做最小 XML patch：
  1. 在 `store_scope` 中增加 `COALESCE(sra.is_duty_free, 'N') <> 'Y'`，将免税门店排除出 same-store 基础集合。
  2. 在 `popup_scope` 中补 `dim_store_report_attr` 关联，并增加同样的 `is_duty_free` 排除条件，避免免税门店进入 popup uplift。
  3. 在 `same_store_daily` 上再加一层 `WHERE COALESCE(a.is_duty_free, 'N') <> 'Y'` 防御式过滤，确保直接汇总 `ads_store_daily_report` 时不再混入免税门店。
  4. 先创建备份 `销售部自动化日报.backup_exclude_duty_free_same_store_20260601_121338.twb`，再覆盖主工作簿。
- 验证状态：已完成 Python `ElementTree.parse()` 解析校验，结果 `XML_OK`；并确认 twb 中两份 SQL 副本都已命中 `store_scope`、`popup_scope`、`same_store_daily` 的免税过滤条件。用户侧 Tableau 重开渲染验证待继续执行。
- 预防动作：后续凡是销售日报 KPI 同时承接 same-store、popup uplift、免税月累计三套口径，必须逐项确认 `is_duty_free` 的参与边界；若免税销售只作为月累计外部快照接入，就不要默认它能直接进入同比类 KPI。

### [2026-06-01] 顶部 KPI 修正后，区域负责人表与门店明细总计未自动跟随 same-store 口径

- 触发场景：用户重开 `销售部自动化日报.twb` 后，顶部 KPI 已恢复为 `同店同比 2.2% / 同店+当期快闪同比 4.51%`，但继续要求“区域负责人表的两个同比指标也用同一个口径”“门店销售明细工作表的同比率总计行用 KPI 的同店同比口径”。
- 报错 / 现象：顶部 KPI 正常，但负责人表与门店明细总计仍各自保留独立公式，存在与顶部 KPI 口径再次漂移的风险。
- 根因判断：
  1. `ds_owner_monthly_yoy_live` 是独立 datasource，不会自动继承 `ds_kpi_same_store_yoy_physical_live` 的免税过滤和 popup 左连接修复。
  2. 门店明细 worksheet 的 `Calculation_1730010000000405` 在总计层级会重新汇总所有可见门店，包括免税门店，天然可能偏离顶部 same-store KPI。
- 修复动作：
  1. 对 `ds_owner_monthly_yoy_live` 的两份 Custom SQL 副本同步补丁：`store_scope` 排除 `is_duty_free='Y'`，`popup_scope` 改为 `LEFT JOIN dim_store_report_attr + COALESCE(is_duty_free,'N') <> 'Y'`，使负责人表的 same-store / same-store+popup 与顶部 KPI 使用同一事实边界。
  2. 将门店明细 worksheet 的 `Calculation_1730010000000405` 改成双分支公式：`COUNTD([store_name]) = 1` 时保留单店 same-store 逻辑；总计层级则改按 `is_duty_free <> 'Y'` 的 same-store helper 重新汇总，确保总计行收敛到顶部 KPI 同店同比。
  3. 修改前另存备份 `销售部自动化日报.backup_owner_and_detail_yoy_20260601_134541.twb`。
- 验证状态：已完成 XML 解析校验，结果 `XML_OK`；只读重算负责人表合计结果为 `same_store_yoy=2.20%`、`same_store_popup_yoy=4.51%`。用户侧 Tableau 重开验证待继续执行。
- 预防动作：后续凡是调整顶部 KPI 的业务口径，都必须同步核对“顶部 KPI / 区域负责人表 / 门店明细总计”三处是否仍残留独立公式副本；不要假设只修一处 datasource 就能全局收敛。

### [2026-07-01] `ds_kpi_same_store_yoy_physical_live` 的 same_store_daily 绕过 physical store_scope，直接汇总 ADS 实体辅助字段，导致 2026-06-30 KPI05 显示 6.14%

- 触发场景：用户在手工重跑 `report_date=2026-06-30` 后重开 `销售部自动化日报.twb`，发现顶部 KPI05 `同店同比` 显示 `6.14%`，与业务核算明显不符，并要求先把这个明显错误值修掉。
- 报错 / 现象：当前工作簿中 `KPI05_同店同比` 可以正常渲染，但数值由 6 月 29 日的约 `29.09%` 在 6 月 30 日骤降为 `6.14%`；只读对账显示并不是分子下降，而是分母突然多出 8 家免税店的 `1,773,040.00` 去年同期辅助金额。
- 根因判断：`ds_kpi_same_store_yoy_physical_live` 的 Custom SQL 前半段虽然已定义 `store_scope` 并排除 `is_duty_free='Y'`，但 `same_store_daily` 实际没有汇总 `same_store_store_set`，而是直接对 `ads_store_daily_report.same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt` 做全表聚合。这样一来，physical scope 与最终 same-store 聚合口径脱节，免税门店和经营实体级辅助字段都会重新混入顶部 KPI。
- 修复动作：对 `销售部自动化日报.twb` 中 `ds_kpi_same_store_yoy_physical_live` 的两份 SQL 副本同时做最小 XML patch，把 `same_store_daily` 从“直接汇总 `ads_store_daily_report`”改为“汇总前文已定义好的 `same_store_store_set`”，让 KPI05/09 使用与 `store_scope + last_year_store_mtd + popup_scope` 同一套 physical same-store 集合；修改前先备份 `销售部自动化日报.backup_same_store_scope_fix_20260701_1025.twb`。
- 验证状态：已完成 XML 结构补丁，并通过 Python `ElementTree.parse()` 校验，结果 `XML_OK`；根据修补前的只读 SQL 对账，切回 `same_store_store_set` 后，`report_date=2026-06-30 / v1` 的 physical same-store 汇总预计不再是 `6.14%`，而会回到约 `18.91%` 的 physical store 集合口径。仍需用户重开 Tableau 确认渲染结果。
- 预防动作：后续凡是 `.twb` 中先定义 `store_scope / popup_scope / same_store_store_set` 再生成 KPI 的 datasource，都必须核对最终 `same_store_daily` 是否真的消费了该集合；不要因为 `ads_store_daily_report` 已有辅助金额列，就直接跳过 scope 层再做二次聚合。

### [2026-07-01] 业务口径更正：销售日报同店同比需统一恢复为“含免税冻结口径”

- 触发场景：用户在看到顶部 KPI 被修到 `18.91%` 后，明确纠正业务口径：销售日报这套工作簿里的 `同店同比`，无论是顶部 KPI 卡、`区域负责人月度汇总`，还是 `门店经营明细` 总计，都应该走“含免税冻结口径”。
- 报错 / 现象：上一轮把顶部 KPI 改成了 `same_store_store_set` 的实体门店集合口径，而负责人汇总和门店明细仍保留历史 helper 聚合公式，结果工作簿内同时出现 `18.91%`、`28.3%` 等多套总计值，和用户确认后的目标口径不一致。
- 根因判断：上一轮默认把“技术上更一致的实体门店集合口径”当成目标，却没有优先遵循用户刚刚确认的业务真值。历史交接也已明确，这张工作簿的 same-store 口径曾被冻结为“含免税版本”，后续若调整必须先经业务确认。
- 修复动作：将 `ds_kpi_same_store_yoy_physical_live` 的 `same_store_daily` 从 `same_store_store_set` 聚合恢复为直接汇总 `ads_store_daily_report.same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt`，同时移除同轮新增的免税过滤；再把 `ds_owner_monthly_yoy_live` 中负责人汇总的 `same_store_yoy / same_store_current_amt / same_store_last_year_amt` 恢复为直接汇总含免税 helper 金额，使顶部 KPI、区域负责人汇总、门店经营明细重新收敛到同一套含免税冻结口径。
- 验证状态：已完成 twb XML 解析校验，结果 `XML_OK`；按现有探针文件 `reports/context_cache/same_store_yoy_20260630_probe.txt`，统一回含免税 helper 口径后，三处总计预期会回到约 `6.14%`。仍需用户重开 Tableau 确认实际渲染值。
- 预防动作：凡是用户直接更正“这张表的业务口径应该是什么”时，必须先把这条更正当成新的业务真值，再判断是延续上轮修复还是回滚；不要把技术推导出的新口径覆盖业务确认口径。

## 4. 后续维护约定

- 每次新增记录后，如涉及项目级规则变化，还需同步更新 `.github/skills/tableau-twb-compiler-hefang/SKILL.md` 或知识库入口文档。
- 若某类问题重复出现至少 2 次，应把“预防动作”上升为 Skill 检查项或项目级长期指令。
- 若用户已明确让其本人手工完成 datasource 导入、重命名或 Tableau UI 调整，台帐仍需记录“Agent 接手前提”和“交接边界”。

## 5. 版本记录

| 版本 | 日期 | 变更内容 |
|---|---|---|
| v2.22 | 2026-07-01 | 新增“用户更正销售日报 same-store 业务口径后，KPI/负责人/明细需统一恢复为含免税冻结版本”的修复记录 |
| v2.21 | 2026-07-01 | 新增“sales 同店同比 KPI 若在 `same_store_daily` 直接汇总 ADS 辅助字段，会绕过 physical store_scope 并重混免税/实体范围”的修复记录 |
| v2.20 | 2026-06-16 | 新增“伯俊Oracle数据建模中，日期关系已改成 `BILL_DATE_ID = date_id` 仍报类型不一致时，需同时修复内嵌 SQL 运算符污染与 `date_id` integer metadata”的修复记录 |
| v2.19 | 2026-06-01 | 新增“顶部 KPI 修正后，区域负责人表与门店明细总计不会自动继承 same-store 口径，需联动修复”的记录 |
| v2.18 | 2026-06-01 | 新增“销售日报 same-store / same-store+popup KPI 必须排除免税门店，否则去年同期分母会压低同比”的修复记录 |
| v2.17 | 2026-05-22 | 新增“Parameters 的 default-value-field 若只指向 worksheet local calculation，会导致 Tableau 把 worksheet 判成没有有效数据源”的修复记录 |
| v2.16 | 2026-05-22 | 新增“销售日报趋势图若继续沿用连续日期轴会自动补边界日期标签，参数窗口 + 离散日期列架是更稳修法”的修复记录 |
| v2.15 | 2026-05-21 | 新增“relationship 模型下线性进度偏差 Text calc 不要重新裸算 day_sales_amt / day_target，应复用已验证正确的日达成率 measure”的修复记录 |
| v2.14 | 2026-05-21 | 新增“实时战情_今日累计销售进度应先检查当前 worksheet 的 datasource alias 与本地 datasource-dependencies，且累计值需改走 SALES_AMT_RAW + STORE_ID (Hourly Sales)”的修复记录 |
| v2.13 | 2026-05-15 | 新增“页头时间卡与核心 KPI 若绑定不同 realtime datasource，会导致数据截至滞后”的修复记录 |
| v2.12 | 2026-05-15 | 新增页头信息摘要与时间进度卡在 `aggregation=true + usr:Calculation` 下必须保持 aggregate helper 的修复记录 |
| v2.11 | 2026-05-15 | 新增 Oracle Custom SQL 中 `&lt;=` / `&gt;=` 组合会被 Tableau 误识别为参数占位的修复记录 |
| v2.10 | 2026-05-14 | 新增“Oracle live Custom SQL 字段大小写与 Tableau 实际识别列名不一致，导致中文 caption 字段整体失效”的修复记录 |
| v2.9 | 2026-05-14 | 新增“`computed-sort` 仍报错时，根因是 workbook 缺少排序相关 manifest 特性开关”的修复记录 |
| v2.8 | 2026-05-14 | 新增“当前实时战情 workbook 不接受 `shelf-sorts`，需改回 `computed-sort`”的修复记录 |
| v2.7 | 2026-05-14 | 新增“空白 workbook 缺少 `ObjectModelTableType` 导致 `datatype='table'` 枚举报错”的修复记录 |
| v2.6 | 2026-05-09 | 新增“累计达成趋势图把 `none:sales_date:qk` 直接挂到 tooltip 导致空白图”的根因与修复记录 |
| v2.5 | 2026-05-09 | 新增“累计达成趋势图在 Measure Names / Multiple Values 结构下 tooltip 失效”的根因与修复记录 |
| v2.4 | 2026-05-09 | 新增“是否最新报告日”第二轮收口，仅保留 1 个共享 root 字段和 1 个门店明细专用 root 字段的执行记录 |
| v2.3 | 2026-05-09 | 新增 ds_ads_store_daily_report_basic 第一轮安全去重，仅删除 6 个零引用 root calculation 的执行记录 |
| v2.2 | 2026-05-09 | 新增 ds_ads_store_daily_report_basic 存在大量重复 calculation、后续 twb 编译必须先复用语义字段的盘点记录 |
| v2.1 | 2026-05-09 | 新增渠道达成概览的日达成率 / 月达成率误绑到贡献占比公式、导致两张图数值完全一样的修复记录 |
| v2.1 | 2026-05-09 | 新增 `style-rule element='table'` 中使用 `format-string` attr 触发 D2E8DA72 的修复记录 |
| v2.0 | 2026-05-09 | 新增静态 legend worksheet 也必须限定为日图两条指标，不能混入累计图说明的记录 |
| v1.9 | 2026-05-09 | 新增自动 color legend zone 不稳定时，改用静态 legend worksheet 的修复记录 |
| v1.8 | 2026-05-09 | 新增 `customized-label` 位于 `customized-tooltip` 之前会触发 D2E8DA72 的修复记录 |
| v1.7 | 2026-05-09 | 新增日趋势折线需要显式 text 绑定、legend zone 过矮会导致“像没图例”的修复记录 |
| v1.6 | 2026-05-09 | 新增 ds_ads_daily_sales 仅有“去年同期累计实际”而无“去年同期日销”字段的边界记录 |
| v1.5 | 2026-05-09 | 新增日趋势图在 Tableau 重开后第二层被回写成 Automatic pane，需恢复主轴与折线标签的修复记录 |
| v1.4 | 2026-05-09 | 新增累计趋势图颜色不明显且 dashboard 缺图例时，需要同时调整色板和补 legend zone 的修复记录 |
| v1.3 | 2026-05-09 | 新增日销售趋势图在补 pane 元数据后仍未真正叠轴，需回退到样板级单 class0 轴同步写法的记录 |
| v1.2 | 2026-05-09 | 新增日销售趋势图因 dual-axis pane 轴元数据不完整而上下分层的修复记录 |
| v1.1 | 2026-05-09 | 新增首页两张趋势图因 cwtwb 字段注册缺口而回退到手工 XML 第一版收口的记录 |
| v1.0 | 2026-05-09 | 新增销售贡献占比基础 Pie 因 mark 与 pane 过大而被视图区裁切的修复记录 |
| v0.9 | 2026-05-09 | 新增 cwtwb 重搭销售贡献占比基础 Pie 时 save_workbook 产出非法 worksheet 结构，需要手工收口的修复记录 |
| v0.8 | 2026-05-09 | 新增 LaDataViz Donut 扩展已挂载但未绑定编码，导致只显示英文占位提示的修复记录 |
| v0.7 | 2026-05-09 | 新增销售贡献占比放弃 donut 并回退为基础 Pie 的收口记录 |
| v0.6 | 2026-05-09 | 新增销售贡献占比出现“三个 pie + 纵轴”时，需要补齐双轴同步与隐藏配置的修复记录 |
| v0.5 | 2026-05-09 | 新增销售贡献占比从普通 Pie 升级为 donut 的结构化修复记录 |
| v0.4 | 2026-05-09 | 新增渠道概览 Pie 首次落地后过小，通过放大 mark 与容器高度修复的记录 |
| v0.3 | 2026-05-08 | 新增 KPI06“目标缺口”因未同步全局 calculation 而仍按时间进度取值的修复记录 |
| v0.2 | 2026-05-08 | 新增门店经营明细“排名被当连续轴，导致异常列与表头下沉”的修复记录 |
| v0.1 | 2026-05-08 | 新增 Tableau TWB 错误修复台帐，并补记渠道贡献、数据源 extract、门店明细 manual-sort 三类真实报错经验 |