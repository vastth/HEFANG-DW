# AGENT_LESSONS.md — Agent 经验台帐

> 用途：沉淀可复用的排障结论、字段语义修正、业务逻辑纠错与查询踩坑，避免同类错误重复发生。
>
> 适用对象：GitHub Copilot / Claude Code / OpenCode 子代理。

---

## 记录规则

以下场景必须记入经验台帐：

- 一次排障后得到可复用结论，例如字段不存在、真实来源表与文档不一致、某命令在本仓库有固定坑。
- 用户明确指出“业务逻辑不对”“字段语义不对”“SQL 口径不对”，且已确认修正方向。
- 同类问题预计会在后续任务中反复出现。

推荐写入命令：

```powershell
python scripts/log_agent_lesson.py \
  --source task \
  --category field-mapping \
  --trigger "Oracle 查询字段报错" \
  --mistake "误以为 M_PRODUCT 存在 NAME_CN" \
  --correction "以 etl_dim_product.py 为准：NAME=product_code，VALUE=product_name" \
  --evidence "etl_dim_product.py#L33" "etl_dim_product.py#L34" \
  --prevention "涉及源表字段时，先对照 ETL 抽取 SQL 或快照后再写查询"
```

若经验是用户业务纠错，应将 `--source` 设为 `user-feedback`。

---

## 经验记录

### [2026-07-13 12:04] · task · business-rule

**触发场景**：门店日报同店资格从去年同期销售额改为开业日期

**错误假设**：用任一侧销售事实或去年同期销售额大于0筛选同店门店，会遗漏本期或同期为0的合格门店。

**修正结论**：同店资格必须由完整源物理门店集合按 dim_store.open_date 截止日判定；两侧销售事实左连接，空开业日期判非同店且不回退旧规则。

**证据**：
- Oracle只读查询：C_STORE共231行，OPENDATE原始空值136行、不可转换日期0行
- 专项单元测试17项通过

**预防动作**：后续修改同店SQL时，锁定完整门店母集、双侧LEFT JOIN、快闪排除、空日期DQ和0/正数两类同比边界。

---

### [2026-07-05 11:51] · task · etl-operations

**触发场景**：NAS 已修正门店生效开始日，但总控仍因负责人快照 unexpected entity 失败

**错误假设**：把 NAS 当前生效日期修正误判为足以自动回拨现网 dim_store_report_attr 的月内错误 effective_start_date。

**修正结论**：确认 import_cfg_store_target_daily_from_nas 会优先沿用 existing_latest_in_target_month，且门店属性差异判断不比较 effective_start_date；因此仅修改 NAS 不能自动修复现网错误切片，需先人工修复 dim_store_report_attr，再重新验证负责人导入与总控链路。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py:_resolve_store_attr_effective_start_date
- tools/import_cfg_store_target_daily_from_nas.py:_detect_store_attr_changed_fields
- tools/import_cfg_store_target_daily_from_nas.py:_build_store_attr_rows
- tools/import_store_operation_owner_from_nas.py:_load_active_store_rows
- tools/import_store_operation_owner_from_nas.py:_build_validation_error_message
- logs/store_daily_report_schedule_20260705.log

**预防动作**：遇到 NAS 已修正但总控仍报 unexpected entity 时，先只读核对 dim_store_report_attr 是否残留月内更晚有效期切片；若存在，先由用户人工执行维表修复 SQL，再做 owner import dry-run 与 scheduled_store_daily_report.py --conn-test。

---

### [2026-07-02 13:12] · user-feedback · tableau-rt119-store-code-fx

**触发场景**：销售部自动化日报.twb 需要把澳门伦敦人店 RT119 的销售额按固定汇率 0.84 折算为 RMB，且不改 worksheet 公式。

**修正结论**：这份 workbook 里基础日报 datasource 对应的是 `store_code = 'RT119'`，不是数仓摘要里常见的 `store_id`；而 ODS 明细 datasource 对应的是 `r.c_store_id = 'RT119'`。后续做同类门店外币折算时，必须按 datasource 的真实字段名落条件，不能把两个字段名混为一谈。折算逻辑优先下沉到 datasource / Custom SQL 层，让现有 worksheet 自动继承新口径。

**证据**：
- [销售部自动化日报.twb](D:/tianhao/Documents/我的%20Tableau%20存储库/工作簿/销售部自动化日报.twb#L556)
- [销售部自动化日报.twb](D:/tianhao/Documents/我的%20Tableau%20存储库/工作簿/销售部自动化日报.twb#L1841)

**预防动作**：以后遇到跨 datasource 的门店别名 / 编码 / ID 口径，先核对每个 datasource 实际落字段，再决定是用 `store_code`、`store_id` 还是 `c_store_id`，不要直接沿用用户口头简称。

---

### [2026-07-01 11:51] · task · tableau-sql-scope

**触发场景**：修复 销售部自动化日报-Old.twb 负责人汇总同店同比与 KPI 漂移

**错误假设**：负责人月度汇总 datasource 在 operating_owner CTE 里额外用 same_store_mtd_sales_amt > 0 过滤同店辅助分子分母，误以为这样不会影响总计口径。

**修正结论**：若负责人汇总需要与顶部 KPI 和门店经营明细总计一致，owner datasource 必须直接汇总 same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt helper，不能再按本期是否大于 0 缩窄同店母集。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报-Old.twb#L74-L87
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报-Old.twb#L4336-L4346
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报-Old.twb#L4513
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb#L74-L87

**预防动作**：以后凡是同屏 KPI、负责人汇总、门店明细都展示同店同比，先核对三处是否复用同一批 helper 分子分母，不要只看 worksheet 最后一层公式。

---

### [2026-07-01 10:40] · user-feedback · business-rule

**触发场景**：用户更正销售日报同店同比应统一走含免税冻结口径

**错误假设**：默认把去免税的实体门店集合口径当成销售日报工作簿的目标口径，没有优先服从用户刚刚确认的业务真值。

**修正结论**：销售日报这套工作簿里的同店同比，顶部 KPI、区域负责人汇总、门店经营明细总计都应统一回含免税冻结口径；技术上更一致的实体门店集合口径不能覆盖业务确认口径。

**证据**：
- 用户当轮明确说明：KPI卡、区域负责人、门店经营明细都应该走含免税口径
- docs/AGENT_HANDOFF_archive.md 已记录该工作簿历史上存在含免税冻结版本
- reports/context_cache/same_store_yoy_20260630_probe.txt 已保留含免税 helper 总计 6.14% 的只读探针

**预防动作**：后续修 Tableau 口径时，若用户直接更正业务口径，必须先以该更正为新的业务真值，再决定是延续还是回滚上一轮技术修复。

---

### [2026-07-01 10:11] · task · tableau-sql-scope

**触发场景**：修复 销售部自动化日报 KPI05 同店同比 6.14% 异常

**错误假设**：误以为 datasource 前半段已限定 physical store_scope 后，same_store_daily 直接汇总 ads_store_daily_report.same_store_* 仍会保持相同门店范围。

**修正结论**：same_store_daily 若绕过 same_store_store_set 直接聚合 ADS 实体辅助字段，会重新混入免税门店与经营实体范围；顶部同店同比 KPI 必须汇总前文定义的 physical same-store 集合。

**证据**：
- 销售部自动化日报.twb 的 ds_kpi_same_store_yoy_physical_live 在修复前 same_store_daily 直接 FROM ads_store_daily_report a
- 2026-06-30 / v1 只读对账显示全量 helper 汇总为 6.14%，而 physical same_store_store_set 汇总约为 18.91%
- docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md 已记录本轮 root cause 与修复动作

**预防动作**：凡是 .twb 中先定义 store_scope/popup_scope/same_store_store_set 再出 KPI 的 datasource，都必须核对最终 same_store_daily 是否真的消费该集合，并对异常日期做一次分子分母只读复算。

---

### [2026-06-25 00:00] · user-feedback · business-rule

**触发场景**：用户指出北京国贸店经营体在 `2026-06-15` 起由主体店与快闪店合并后，ERP 销售终端转移到快闪店，主体店不再继续产生日销；因此 `2026-06-22` 的同店同比不能再拿去年累计 22 天当分母。

**错误假设**：沿用“同店去年同期永远累计到 `report_date` 对应去年同日”的固定规则，忽略了月中快闪合并后主体店 ERP 流水会在合并日起中断。

**修正结论**：若当前经营实体命中“月中才生效”的 `快闪` 合并，则 `same_store_last_year_mtd_sales_amt` 的去年同期累计上界应截到最早 `快闪` 生效日前一天的去年同日；月初即生效的共同考核主体不适用这条截断规则。

**证据**：
- 用户 2026-06-25 明确反馈：主体店与快闪合并后，主体门店不再产生销售数据，去年同期分母只能取到合并日前一天。
- `etl_ads_store_daily_report.py` 已新增 `flash_merge_cutoff_scope`，并在 `same_store_last_year_fact` 按最早 `快闪` 生效日前一天回退去年同期上界。
- `test_ads_store_daily_report.py` 已新增月中快闪合并回归，锁定 `effective_start_date` 透传与 `DATE_SUB(fmcs.merge_before_date, INTERVAL 1 YEAR)` 截断条件。

**预防动作**：后续凡是共同考核主体在月中吸收 `快闪`、联营或其它“终端切换”成员时，不要默认沿用完整同期累计；先核对合并生效日后主体店 ERP 是否还继续产生日销，再决定去年同期分母是否需要按切换日前截断。

---

### [2026-06-22 17:53] · task · tableau-xml

**触发场景**：修复 伯俊Oracle数据建模.twb 剩余英文库存字段与语义文件夹归类时，批量脚本首次运行失败且库存字段仍未显示中文

**错误假设**：误以为 update_bojun_twb_labels.py 只要补充字段映射就能直接复用，忽略了 datasource caption 已改名，以及缺失顶层 column 的插入范围仍依赖当前 workbook 旧 folders-common

**修正结论**：处理 Tableau 批量字段汉化时，目标 datasource 不能只靠固定 caption 定位；应优先支持按关键 parent-name 或结构特征回退识别。同时，给新增语义文件夹补 root column 时，字段来源必须取自脚本定义的目标分组全集，而不是工作簿当前 folders-common，否则新分组字段永远不会进入待补列集合。

**证据**：
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb:190 datasource caption 已为 伯俊数据模型_Full
- reports/context_cache/update_bojun_twb_labels.py 首次报错 RuntimeError: 未找到目标 datasource: DEV_HEFANG_销售数据集_近1年
- reports/context_cache/update_bojun_twb_labels.py 二次报错 RuntimeError: 未找到顶层字段定义: [inventory_snapshot_id]
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb:5830-5864 已新增 34 个库存字段 root column 并写入中文 caption
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb:6144-6182 已新增 3-1/3-2/3-3 库存事实文件夹

**预防动作**：后续凡是复用 Tableau XML 批量修补脚本，先核对 datasource caption 是否漂移，再检查待补字段集合是不是来自目标配置而不是当前 workbook 现状；脚本收口至少保留三项验证：退出码为 0、XML parse 成功、关键 caption/folder grep 命中。

---

### [2026-06-22 13:41] · task · sales-scope

**触发场景**：用户重开 `销售部自动化日报.twb` 后指出，`销售趋势分析_日销售趋势` 从 `2026-06-16` 起少了北京国贸快闪和广州天环快闪，但顶部 `KPI01_日销售额` 正常。

**错误假设**：先怀疑是 Tableau worksheet 过滤、离散日期列架或 `.twb` 渲染问题，准备直接改工作簿 XML。

**修正结论**：若销售日报趋势图读的是 `ads_daily_sales`，而 KPI 读的是 `ads_store_daily_report`，且缺口刚好集中在共同考核快闪成员门店，优先检查 `ads_daily_sales` 的 source scope 是否只取了 `cfg_store_target_daily` 目标门店。`ads_daily_sales` 必须像 `ads_store_daily_report` 一样，把 `joint_assessment_member_scope` 纳入源门店范围，再用挂靠主店属性回填 `area_name/report_channel_type`，否则没有单店目标的快闪成员店会在趋势表里整段缺失。

**证据**：
- 只读 SQL 核对 `2026-06-16` 到 `2026-06-21` 的趋势图缺口为 `12349 / 19507 / 30517 / 68475 / 36592 / 45303`。
- 同期 `ods_m_retail + ods_m_retailitem` 只读汇总显示，`RT014` 与 `RT140` 快闪源门店真实流水逐日精确等于上述缺口。
- `etl_ads_daily_sales.py` 修复前只有 `target_store_scope`，而 `etl_ads_store_daily_report.py` 已显式包含 `joint_assessment_member_scope` 与 `source_store_scope`。

**预防动作**：后续凡是 Tableau 趋势图与 KPI 同屏不一致，且差额集中在共同考核快闪/成员门店时，不要先改 `.twb`；先用只读 SQL 对比 `ads_daily_sales` 与 `ads_store_daily_report`，再检查 ADS ETL 是否漏纳入共同考核成员源门店。

### [2026-06-19 01:00] · user-feedback · business-rule

**触发场景**：用户指出负责人快照在共同考核 2026-06-18/19 生效切换窗口内，不应因 STORE 与 SUBJECT 同月并存而阻断。

**错误假设**：沿用旧规则，认为共同考核一出现就必须立即删除被吸收成员门店的 STORE 行，否则负责人导入应按 unexpected_entities 失败。

**修正结论**：负责人共同考核的最终真值仍推荐维护 SUBJECT，但在同一目标月的生效切换过渡期内，若被吸收成员 STORE 行与对应 SUBJECT 行并存，或在正式生效日前已提前维护 SUBJECT 且成员 STORE 仍保留，应降级为 warning 而不是阻断；只有缺少当前应维护实体，或仅提前维护 SUBJECT 但未同时保留成员 STORE 时，才继续失败。

**证据**：
- 用户明确要求：程序识别到 RT045 已纳入统计主体合并后，不要报错或阻断，只要提示。
- test_store_operation_owner_import.py 新增 STORE+SUBJECT 并存与提前 SUBJECT 场景回归并通过。
- 真实 dry-run 结果：snapshot_date=2026-06-18 时缺 RT045 且仅有 SUBJ_GZTH 仍失败；snapshot_date=2026-06-19 时同一文件已 PASSED。

**预防动作**：后续遇到共同考核切换或主体合并类负责人规则时，不要直接沿用历史 memory 里的硬失败结论；先核对当月生效边界、用户当前业务口径，以及负责人文件是否处于同月过渡窗口。

---

### [2026-06-18 16:35] · user-feedback · field-semantics

**触发场景**：门店销售专题报错 `门店ID 不是合法整数：RT050`，用户明确指出 `门店考核归属` sheet 里的 `门店ID` 列业务实际填写的是门店编码，对应 `RT050` 这类值。

**错误假设**：把 `门店考核归属` 的 `门店ID` 固定理解为数值 `store_id`，只做整数校验，导致真实 NAS 文件中的 RT 门店编码被直接判错。

**修正结论**：`门店考核归属` 的 `门店ID` 列名虽沿用 ID，但业务真值应按 `dim_store.store_code` 维护；导入脚本应优先按 `store_code` 命中，共同考核配置若填写纯数字时再兼容 `store_id`。`门店名称` 仅作展示和名称漂移告警，不能再承担主匹配键。

**证据**：
- 用户 2026-06-18 明确反馈：`门店ID 其实是 门店编码 对应 就是 RT050 这种`
- `test_import_cfg_store_target_daily_from_nas.py` 已新增 `RT050` 解析与共同考核命中回归
- 只读解析当前 NAS `202606考核数据配置表.xlsx`，前 4 行共同考核门店标识分别为 `RT050 / RT014 / RT045 / RT140`
- 继续映射 `dim_store` 后，4 条共同考核归属全部命中，validation 为空

**预防动作**：后续凡是业务外部 Excel 出现 `门店ID`、`商品ID` 这类列名，先用真实文件样本或用户确认字段语义，再决定是按数值主键、业务编码还是双兼容解析；不要仅凭表头命名直接收口为整数。

### [2026-06-17 11:20] · task · tableau-xml

**触发场景**：Tableau 字段文件夹已生效但字段窗格仍显示英文内部名

**错误假设**：误以为只要更新 metadata-record 的 remote-alias，并把字段放入 folders-common，Tableau 就会自动按中文显示。

**修正结论**：对于这类自定义 SQL 建模的 twb，若字段仅存在于 cols/metadata-records/folder-item 而没有主数据源顶层 column 注册，Tableau 字段窗格仍会回退显示内部字段名；必须为缺失字段补建顶层 column，并在该层写 caption。

**证据**：
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb:4596
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb:4729
- reports/context_cache/update_bojun_twb_labels.py

**预防动作**：后续处理 Tableau 字段汉化时，固定按 root column -> cols map -> metadata-record -> folder-item 四层检查；若字段在文件夹里仍显示英文，优先核对是否缺少顶层 column 注册。

---

### [2026-06-17 11:05] · task · tableau-xml

**触发场景**：批量给 Tableau twb 顶层 column 标签补写 caption 属性

**错误假设**：初版脚本把 caption 追加到了自闭合 <column ... /> 标签的斜杠后面，形成 / caption='...' > 的非法结构。

**修正结论**：更新标签改写规则：匹配 <column ... /> 或 <column ...> 的闭合段时，新增 caption 必须插到 /> 或 > 之前，并在落盘后立即做 XML 解析校验。

**证据**：
- reports/context_cache/update_bojun_twb_labels.py: update_column_caption 改为拆分标签主体与闭合段再注入 caption
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb: 重新执行脚本后 XML_OK

**预防动作**：后续所有 twb/XML 批量属性注入任务，先用闭合段捕获正则处理自闭合标签，再以 ET.parse 或等价 XML 校验作为强制收口步骤。

---

### [2026-06-16 15:54] · task · tableau-twb

**触发场景**：复制旧伯俊建模 workbook 生成 clean 新建版时，需要把旧的辅助 datasource 和 cfg 快照残留一并清掉

**错误假设**：只删除 datasource relationship 或只替换 SQL，就足够得到一个干净的新 TWB

**修正结论**：若旧 workbook 带有辅助 datasource、cfg 快照或额外连接，必须同步清理 root column 注册、extract relation 与 metadata-records、object-graph、relationships、named-connection 五层残留，否则新 TWB 仍会带坏引用或脏模型

**证据**：
- 工作簿/SKU生命周期看板项目/伯俊Oracle数据建模_新建版.twb 清理后仅剩 1 datasource、4 object、3 relationship
- PowerShell XML 解析返回 XML_PARSE_OK

**预防动作**：以后基于旧 TWB 克隆新建版时，按 datasource -> named-connection -> root columns -> extract metadata -> object-graph -> relationships 的顺序裁剪，并对 helper/cfg/mysql 关键字做全文复扫

---

### [2026-06-16 14:12] · task · tableau-twb

**触发场景**：用户在 `伯俊Oracle数据建模.twb` 中已经把 sales 和 calendar 的关系改成 `BILL_DATE_ID = date_id`，但 Tableau 仍提示关系类型不一致

**错误假设**：只要 relationship XML 已从 `[BILL_DATE] = [date]` 改成 `[BILL_DATE_ID] = [date_id]`，关系错误就会自然消失；同时默认 Oracle `TO_NUMBER(TO_CHAR(...))` 产出的日期代理键会被 Tableau 稳定识别成整数

**修正结论**：Tableau 日期关系报“类型不一致”或“关系的某个输入中存在错误”时，必须同时检查四层：1）relationship XML 是否已经换成正确 surrogate key；2）workbook 内嵌 Custom SQL 的 datasource 根 relation 和 `object-graph` 副本是否仍残留坏运算符或旧 SQL；3）relationship 两端 `metadata-record` 的 `local-type` 是否一致；4）datasource 根列定义里关系键是否仍被注册成 `measure`。对 Oracle 日期代理键，优先显式写成 `CAST(TO_NUMBER(TO_CHAR(...)) AS NUMBER(8,0))`，并确保 workbook metadata 中 `date_id` 是 `integer`，根列定义中 `date_id` 是 `dimension/ordinal`

**证据**：
- `工作簿/SKU生命周期看板项目/伯俊Oracle数据建模.twb` 中 `relationship` 已是 `[BILL_DATE_ID] = [date_id]`
- 同文件 `sales.csv` / `calendar.csv` 两份内嵌 SQL 曾残留 `<<>>` / `<<` / `>>` / `<<=`
- 同文件 `[sales.csv].[BILL_DATE_ID]` metadata 为 `integer`，而 `[calendar.csv].[date_id]` 在修复前为 `real`，修复后已改为 `integer`
- 同文件根列定义中的 `[date_id]` 在修复前为 `role='measure' type='quantitative'`，修复后已改为 `role='dimension' type='ordinal'`
- `SQL/【伯俊建模】日期维表SQL.sql` 已将 `date_id`、`last_year_same_date_id`、`prev_month_same_date_id` 改为 `NUMBER(8,0)`

**预防动作**：以后凡是 Tableau 关系线已经改成 surrogate key 仍报错，先排“关系表达式正确但 SQL/metadata 不一致”的方向；涉及 Oracle Custom SQL 关系键时，默认显式收口数值精度，并双写 datasource 根 relation 与 `object-graph` 副本，避免下次同步外部 SQL 后旧问题复发

### [2026-06-12 16:45] · task · tableau-twb

**触发场景**：修复 HEFANG门店实时销售战情看板时，门店明细页的同店本期月销列虽然公式已切到实时口径，但显示值仍停在快照，且顶部 KPI 与负责人/明细的滞后门店数存在时间进度漂移

**错误假设**：只改 Calculation_202606121801/202606121802 与月达成率/同比公式，就认为 Measure Names 表格会自动改显示列；同时默认允许顶部 KPI 用 LAST_STATUSTIME、owner/明细用 NOW()

**修正结论**：在 Tableau 的 Measure Names / Multiple Values 视图里，显示列切换必须同步改 alias、column-instance、groupfilter、manual-sort；若同屏存在顶部 KPI 与 owner/明细的进度计数，time_progress 必须统一到同一时间源，实时战情优先统一到 Oracle 最新交易时间 LAST_STATUSTIME

**证据**：
- 工作簿/HEFANG门店实时销售战情看板.twb#L1789
- 工作簿/HEFANG门店实时销售战情看板.twb#L1847
- 工作簿/HEFANG门店实时销售战情看板.twb#L3192
- 工作簿/HEFANG门店实时销售战情看板.twb#L3503

**预防动作**：以后修 Tableau 明细表时，先同时检查字段公式和 Measure Names 成员链路；凡同屏比较达成率、线性进度和领先/滞后门店数时，先确认全部 sheet 的 time_progress 是否来自同一 timestamp

---

### [2026-06-12 16:26] · user-feedback · tableau-sql-modeling

**触发场景**：用户明确要求门店维/商品维/销售事实以 Tableau 星型模型落地，销售事实默认全量并通过 Extract 控制性能

**错误假设**：把销售事实 SQL 同时做成宽表，又在 Tableau 中继续关联独立门店维和商品维，导致事实重复展开维度字段且刷新代价偏高

**修正结论**：若 Tableau 数据源采用关系模型，销售事实 SQL 只保留 store_id、sku_id、product_id_from_item、单据字段、状态字段和金额数量度量；门店与商品描述字段留在独立维表；默认全量历史由 Tableau Extract 或增量刷新策略承接性能控制

**证据**：
- SQL/【伯俊建模】销售订单SQL.sql
- SQL/【伯俊建模】门店维表SQL.sql
- SQL/【伯俊建模】商品维表SQL.sql

**预防动作**：后续新增 Tableau 自定义 SQL 前先二选一：要么单宽表，要么星型关系模型；若选星型模型，事实 SQL 禁止重复展开维度描述字段，并避免额外 DISTINCT/ROW_NUMBER 兜底排序。

---

### [2026-06-12 15:55] · task · tableau-twb

**触发场景**：用户重开 HEFANG门店实时销售战情看板 后，实时战情_门店实时销售明细 的 度量名称 筛选器出现红感叹并提示筛选器无效

**错误假设**：在 Measure Names 视图里直接保留快照字段与实时辅助字段相同 caption，导致 Tableau 同时暴露两组同名度量

**修正结论**：保留实时辅助字段 caption 为业务展示名，同时把底层快照字段 caption 明确改成 快照辅助，降低 Tableau 对 Measure Names 筛选成员的字段歧义

**证据**：
- 工作簿/HEFANG门店实时销售战情看板.twb 中 Calculation_202606121801/202606121802 负责实时月累计与实时去年同期月累计
- 同文件中 last_year_mtd_sales_amt / mtd_sales_amt caption 已改为 同店去年同期月销_快照辅助 / 同店本期月销_快照辅助
- Python xml.etree.ElementTree.parse 校验返回 XML_PARSE_OK

**预防动作**：以后在 Tableau 的 Measure Names / Measure Values 视图里新增替代字段时，不要让底层快照字段与替代展示字段共用同一 caption；优先把原始字段显式标成 快照/原始，再让新字段承接业务展示名

---

### [2026-06-12 15:03] · user-feedback · tableau-twb

**触发场景**：用户指出 HEFANG门店实时销售战情看板 的 门店实时销售明细表 中 同店本期月销_辅助、月达成率、同店同比 仍停在 D-1 快照

**错误假设**：把 ds_owner_realtime_summary_live 中基于 ads_store_daily_report latest snapshot 的 mtd_sales_amt / last_year_mtd_sales_amt 直接当成实时月累计与同比口径

**修正结论**：实时月累计应采用 快照月累计基线 + Oracle 当日实时增量；实时同比还需同步补 去年同月同日截至同一实时点 的实时增量，再在 Tableau 层重算 month ach 与 same-store yoy

**证据**：
- 工作簿/HEFANG门店实时销售战情看板.twb 中 ds_owner_realtime_summary_live 的 Owner Scope SQL 直接读取 ads_store_daily_report latest snapshot
- 同文件的 Realtime Sales SQL 已新增 LAST_YEAR_DAY_SALES_AMT，并新增 Calculation_202606121801/202606121802 重算实时月累计
- Python xml.etree.ElementTree.parse 校验返回 XML_PARSE_OK

**预防动作**：遇到实时看板月累计/同比异常时，固定联查 worksheet caption、worksheet local calc、MySQL scope SQL、Oracle realtime relation；若要做实时同比，不能只补分子，还要同步补去年同日同刻分母增量

---

### [2026-06-12 14:36] · user-feedback · business-rule

**触发场景**：用户核对 `HEFANG门店实时销售战情看板.twb` 时指出，顶部“今日0销售门店数”“进度落后门店数”都多算了 8 家免税门店，且当前小程序不应继续进入这张看板

**错误假设**：默认把异常只归因于顶部 KPI 公式本身，认为改掉两张卡片的计数公式即可；没有继续核对 owner 汇总公式、实时 Oracle SQL 的硬编码门店列表，以及 worksheet 对渠道成员的显式过滤/排序是否仍保留“小程序”

**修正结论**：HEFANG 实时战情看板当前范围必须以 `dim_store_report_attr` 有效 `report_channel_type / is_duty_free` 真值为准。2026-06-12 只读核验显示当前日报范围 72 家，其中免税 8 家、小程序 0 家，且 `store_id=96` 当前有效记录为 0 行。因此修复这类问题时，必须同时做四件事：1）顶部“0 销售/落后门店数”按 `report_channel_type` 排除“免税”；2）负责人表领先/滞后门店数同步排除“免税”；3）清理 `.twb` 中把 `store_id=96` 硬编码为“小程序”的实时 Oracle SQL；4）清理 worksheet 显式 `member/bucket='小程序'` 残留

**证据**：
- 2026-06-12 只读核验：`dim_store_report_attr` 当前有效范围 `current_scope_cnt=72`、`duty_free_cnt=8`、`mini_program_cnt=0`，`store_id=96` 当前有效记录为 0 行
- 工作簿/HEFANG门店实时销售战情看板.twb#L405
- 工作簿/HEFANG门店实时销售战情看板.twb#L821
- 工作簿/HEFANG门店实时销售战情看板.twb#L824
- 工作簿/HEFANG门店实时销售战情看板.twb#L2133
- 工作簿/HEFANG门店实时销售战情看板.twb#L2145

**预防动作**：后续凡是实时看板出现“渠道/门店数不对但数据源似乎没变”的问题，固定按“当前维表真值 -> federated target/owner scope -> Oracle store_scope 硬编码 -> worksheet 显式 member/sort”四层排查，不能只改某个 KPI 公式副本

---

### [2026-06-11 15:09] · task · tableau-twb

**触发场景**：用户在 SKU 生命周期看板的 Tableau 数据模型页反馈 `sales_sku_daily` 与 `sku_dim` 关系字段下拉为空并出现红叹号

**错误假设**：误以为关系失效主要由 `calendar_dim` 外部 SQL 改动导致，优先怀疑日期维字段过多、日期范围过宽或外部 `.sql` 文件本身有语法问题

**修正结论**：真正根因是 `.twb` 内嵌 Custom SQL 的比较运算符被写坏成 `>>` / `<<` / `<<=`，且 `sales_sku_daily -> sku_dim` 的 relationship XML 落成了空 `[] = []`；这种情况下应先修 workbook 内嵌 SQL 与关系表达式，而不是先回改外部 SQL 草案

**证据**：
- 工作簿/SKU生命周期看板项目/SKU生命周期分析看板.twb#L35
- 工作簿/SKU生命周期看板项目/SKU生命周期分析看板.twb#L168
- 工作簿/SKU生命周期看板项目/SKU生命周期分析看板.twb#L2105
- 工作簿/SKU生命周期看板项目/SKU生命周期分析看板.twb#L2113

**预防动作**：后续凡是 Tableau 关系字段下拉为空时，优先检查 `.twb` 的 datasource relation、副本 `object-graph` relation 和 relationship XML，确认没有非法比较运算符、没有空 `[]` 关系，再决定是否回头改外部 SQL 草案

---

### [2026-06-08 10:31] · user-feedback · business-rule

**触发场景**：北京国贸 2026-06-07 月累计销售额差额追查时，用户要求从根上避免新品类因漏配被排除

**错误假设**：默认把 dim_report_product_rule 当作 ads_store_daily_report 和 ads_daily_sales 的运行时白名单，认为新增品类后补配置即可

**修正结论**：当前门店日报与 ads_daily_sales 的商品范围应固定排除 147=辅料、149=办公用品、150=道具，其余 dim_product.category_id 默认纳入；dim_report_product_rule 只保留为历史配置与人工分析参考，不再作为运行时依赖

**证据**：
- etl_ads_store_daily_report.py#L343
- etl_ads_store_daily_report.py#L394
- etl_ads_daily_sales.py#L242
- etl_ads_daily_sales.py#L279
- SQL/check_ads_daily_sales_min.sql#L340
- docs/业务逻辑与指标规范.md#L147

**预防动作**：后续凡是门店日报或 ads_daily_sales 对账差额集中在新品类时，先检查是否误把显式纳入表当运行时白名单；若业务规则仍是固定排除三类，则只能改排除集合，不能再要求补 dim_report_product_rule 才生效

---

### [2026-06-06 09:15] · task · retirement-governance

**触发场景**：退役销售专题 ADS 并收口专题调度/文档/SQL

**错误假设**：以为删除 ETL、测试和建表脚本就完成退役，忽略了专题调度监控表清单、活动 SQL 注释/证据引用、活动文档版本记录里的精确退役表名。

**修正结论**：退役对象必须同时清理运行代码、专题调度链路、监控表清单、活动 SQL 资产与注释、活动文档正文，以及会触发 doc-sync 高风险命中的版本记录词面；否则仓库会继续暴露已退役对象。

**证据**：
- scheduled_store_daily_report.py 已收口为 3 张保留 ADS
- reports/docs_code_alignment.json 二次复跑后不再把 3 张退役 ADS 名称列为高风险 docs-only 词项
- README.md/docs/ARCHITECTURE.md/docs/DATA_CONTRACTS.md 等活动文档已移除当前运行描述

**预防动作**：后续退役表/链路时，固定按 运行代码 -> 调度与监控清单 -> 测试 -> SQL 资产与注释 -> 活动文档正文 -> 版本记录词面 六层做全文检索，并在最小测试 + doc-sync 复跑通过后再写 handoff。

---

### [2026-06-03 09:43] · user-feedback · business-rule

**触发场景**：用户要求按线上销售月报SQL3.0口径扩展门店范围并保留全部RT门店

**错误假设**：将线上门店口径泛化为 DS% 或继续只保留 RT%

**修正结论**：线上门店应按 SQL/==线上销售月报SQL_3_0.sql 的渠道白名单纳入，并在 stores.csv 将这些渠道标记为 线上门店；线下侧维持所有 RT 门店，不引入 cfg_store_report_attr_snapshot 收窄过滤

**证据**：
- SQL/==线上销售月报SQL_3_0.sql
- 工作簿/#VOTD Sales Dashboard (Retail Toy Store)_v2025.3/HEFANG经营数据看板-全域版.twb

**预防动作**：后续改 Tableau 门店范围时先固定权威渠道白名单并同步修改两套重复 Custom SQL（relation collection + object-graph）

---

### [2026-06-01 13:45] · task · tableau-kpi

**触发场景**：顶部 KPI 的 `同店同比 / 同店+当期快闪同比` 已修正为 `2.2% / 4.5%` 后，用户继续要求区域负责人表两列同比和门店明细表“同比率”总计行也必须使用同一口径

**错误假设**：默认认为只要顶部 KPI datasource 修正完毕，负责人表和明细表会自然同步；没有继续核对它们是否仍在使用各自独立的 helper 聚合公式或包含免税门店的总计逻辑

**修正结论**：销售日报里同店同比相关口径不能只修顶部 KPI，还要同步检查至少两处下游展示：
1. `ds_owner_monthly_yoy_live` 的 `store_scope / popup_scope` 是否也按 same-store KPI 一样排除免税并保留属性缺失快闪店。
2. 门店明细 worksheet 的“同比率”总计行是否在原生总计层级重新汇总了免税门店；若是，必须显式写成“单店行保持原逻辑、总计行按非免税 same-store helper 重算”的双分支公式。

**证据**：
- 负责人表只读重算：修复后 `TOTALS = same_store_yoy 0.0220 / same_store_popup_yoy 0.0451`
- `Amor` 负责人行因 RT014 popup uplift，`same_store_popup_yoy` 与 `same_store_yoy` 不再相同
- 门店明细 `Calculation_1730010000000405` 已改为 `COUNTD([store_name]) = 1` 走单店逻辑、否则总计层级排除 `is_duty_free='Y'`

**预防动作**：后续凡是 Tableau 顶部 KPI 的业务口径发生变更，必须把“顶部 KPI / owner 汇总表 / 门店明细总计”视为一个联动检查单元，逐一核对是否仍存在独立公式副本或总计层级重算偏差

### [2026-06-01 12:25] · task · tableau-kpi

**触发场景**：在 `销售部自动化日报.twb` 中为 same-store KPI 排除免税门店后，`同店同比` 回到 `2.2%`，但 `同店+当期快闪同比` 也错误地变成了同样的 `2.2%`

**错误假设**：为 popup_scope 排除免税门店时，默认认为快闪店都能在 `dim_store_report_attr` 找到当前有效记录，因此直接写成 `INNER JOIN dim_store_report_attr`

**修正结论**：popup uplift 口径不能强依赖当前有效 `dim_store_report_attr` 记录；像 RT014 这类快闪专用店可能在当前日报维表里没有有效行，但仍应保留在 popup_scope 中。对 popup_scope 做免税过滤时，应使用 `LEFT JOIN dim_store_report_attr`，再以 `COALESCE(is_duty_free, 'N') <> 'Y'` 过滤，避免把属性缺失的合法快闪店一并吞掉

**证据**：
- 2026-05-31 只读核验：`cfg_store_assessment_assignment` 中 popup 门店仅 `store_id=27 / RT014`，但当前有效 `dim_store_report_attr` 为空
- 使用 `INNER JOIN dim_store_report_attr` 时，popup_scope 为空，`popup_current_amt=0`，`same_store_popup_yoy=2.20%`
- 改为 `LEFT JOIN dim_store_report_attr` 后，popup_scope 恢复为 RT014，`popup_current_amt=289437.60`，`same_store_popup_yoy=4.51%`

**预防动作**：后续凡是 Tableau / SQL 里对 popup_scope、快闪映射或并店映射追加维表属性过滤时，先验证这些店是否一定存在当前有效维表记录；如果存在“事实有效但维表缺行”的场景，默认优先 `LEFT JOIN + 防御式过滤`

### [2026-06-01 12:13] · user-feedback · business-rule

**触发场景**：用户核对 `销售部自动化日报.twb` 的 2026-05-31 顶部 KPI，指出业务口径应为“同店同比 2.2% / 同店+快闪同比 4.5%”，而当前 Tableau 显示 `-14.09% / -12.15%`

**错误假设**：默认认为 `ads_store_daily_report.same_store_*` 已经是最终同店口径，只要直接汇总就能得到正确 KPI，没有继续核对免税门店是否应被排除在 same-store / popup 口径之外

**修正结论**：门店日报顶部 `同店同比` 与 `同店+当期快闪同比` 必须彻底排除免税门店；免税销售当前只参加月总达成，不应进入 same-store 分子分母，也不应进入 popup uplift 范围。2026-05-31 若排除 `is_duty_free='Y'` 后，KPI 会从 `-14.09% / -12.15%` 回到 `2.20% / 4.51%`

**证据**：
- `销售部自动化日报.twb` 中 `ds_kpi_same_store_yoy_physical_live` 的原 SQL 会直接汇总 `ads_store_daily_report.same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt`
- 只读门店差异清单显示 6 家免税门店在 2026-05-31 合计带入 `2,377,107.50` 去年同期分母，但当前 same-store 分子为 `0`
- 只读重算：仅保留 `is_duty_free='N'` 后，`same_store_yoy=0.0220`、`same_store_popup_yoy=0.0451`

**预防动作**：后续凡是销售日报 / Tableau KPI 同时承接 same-store、popup uplift、免税月累计三套口径时，必须逐项确认 `is_duty_free` 是否应参与；不能因为免税月累计已接入 `mtd_sales_amt`，就默认它也应进入同比 KPI

### [2026-06-01 10:43] · task · etl-window

**触发场景**：ads_sku_daily 同时承接 MTD 与滚动30天窗口，且 report_date 落在 31 天月份月末

**错误假设**：把 detail_base 的历史起点固定写成 report_date-29，默认认为既能覆盖滚动30天也能覆盖月累计

**修正结论**：当同一份底表同时服务 MTD 与滚动窗口时，历史起点必须取 month_start 与 rolling_window_start 中更早者；否则 31 天月末会漏掉当月 1 号只出现过的组合

**证据**：
- etl_ads_sku_daily.py:detail_base 原先使用 p.rolling_30d_start_id
- 2026-05-31/v1 只读重算：修复前 ranked=4142，修复后 ranked=4243
- 差集核实：仅在 2026-05-01 出现、5-02 至 5-31 不再出现的 SKU+战区+渠道组合共 101 个

**预防动作**：后续新增或改造 ETL 时，只要同一份明细底表同时给 MTD / rolling-N-day / trend 复用，就必须显式检查时间窗是否取最早起点，并补 31 天月末回归测试

---

### [2026-06-01 10:13] · task · reporting-ops

**触发场景**：2026-06-01 电商销售日报表『管道输出』在 6-1 跨月时，绿色月累计区多列直接变成 0。

**错误假设**：把月累计公式的月初边界绑定到 TODAY() 所在月份，而不是统一绑定到报表日=昨天；6-1 时区间会变成 >=6-1 且 <6-1。

**修正结论**：凡按昨天出报的绿色月累计公式，月初边界必须改为 EOMONTH(TODAY()-1,-1)+1；去年同期月初同理改为 EDATE(EOMONTH(TODAY()-1,-1)+1,-12)。

**证据**：
- 工作簿/〓电商销售日报表 (5).xlsx 管道输出!H3:L13、C5:D5、C7:D8、C13 原公式均含 EOMONTH(TODAY(),-1)+1。
- 工作簿/〓电商销售日报表 (5)_管道输出跨月修复建议.xlsx 对应单元格已改为 EOMONTH(TODAY()-1,-1)+1，共 54 处。

**预防动作**：以后凡日报表声明『报表日=昨天』，必须逐列检查月累计和去年同期的起始边界是否跟随昨天而不是今天；同时单独排查绿色区域里是否存在写死 0 的常量单元格。

---

### [2026-06-01 09:24] · user-feedback · etl-operations

**触发场景**：2026-06-01 09:19 总控 V2 中，门店销售专题在修复 target_month 跨月门禁后，仍在 owner_import 阶段把整张负责人表判成 unexpected_entities。

**错误假设**：只把目标文件 target_month 门禁对齐到 previous-day 的 report_date，却遗漏了负责人导入默认 snapshot_date 仍然取 date.today()。

**修正结论**：门店专题凡是 previous-day/current-day 会改变实际处理日期的场景，负责人默认 snapshot_date 也必须跟随专题本轮实际处理的 report_date 上界，而不能继续取今天日期。否则 expected_entities 可能为空，进而把整张负责人表误判成 unexpected_entities。

**证据**：
- logs/scheduled_total_control_20260601.log#L160-L165
- logs/store_daily_report_schedule_20260601.log#L45-L49
- scheduled_store_daily_report.py#L406
- scheduled_store_daily_report.py#L1970

**预防动作**：后续凡是专题调度新增或修改了自动 report_date 逻辑，必须同时检查目标文件门禁、负责人 snapshot_date、免税快照月份和 affected_dates 上界这四类时间锚点是否统一跟随实际处理日期。

---

### [2026-06-01 09:07] · user-feedback · etl-operations

**触发场景**：2026-06-01 00:05 总控 V2 中，门店销售专题在 previous-day 自动模式下没有处理 2026-05-31，而是因为跨月被直接跳过。

**错误假设**：把专题自动模式的 target_month 门禁固定理解成必须等于当天自然月，忽略了 previous-day 模式实际处理的是前一天 report_date。

**修正结论**：专题自动模式的月份校验必须对齐本轮自动 report_date 所在月份。previous-day 在 6-1 00:05 应接受 2026-05 快照并处理 2026-05-31；只有 current-day 才要求 target_month 等于当天自然月。

**证据**：
- logs/scheduled_total_control_20260601.log#L64-L67
- logs/store_daily_report_schedule_20260601.log#L2-L4
- scheduled_store_daily_report.py#L1319
- test_scheduled_store_daily_report.py#L99

**预防动作**：后续凡是专题调度同时支持 previous-day/current-day 两种自动 report_date 模式时，所有 target_month 门禁、快照月份门禁和跨月跳过文案都要统一跟随自动 report_date，而不是直接使用 date.today()。

---

### [2026-05-27 13:20] · user-feedback · business-rule

**触发场景**：用户同步门店负责人真实业务变化：原负责人 Gloria 已离职，当前暂未任命新负责人，业务先用 NEW 作为临时负责人占位。

**错误假设**：看到负责人从 Gloria 改成 NEW / New 时，默认按录入错误或缺负责人异常处理，或只关注字段非空而忽略生效日期是否会回溯历史。

**修正结论**：Gloria 离职后的临时负责人占位属于业务真值，负责人导入链路可以承接该占位值；但 Excel 中负责人大小写会按原样入库，显式 `生效日期` 会驱动负责人 SCD2 回刷窗口，需核对是否符合业务期望。

**证据**：
- 用户 2026-05-27 确认：原 Gloria 离职，当前暂未有新负责人，用 NEW 暂时代替。
- NAS `门店负责人映射表.xlsx` 最后修改时间为 2026-05-27 13:08:59，负责人分布中 New=17、Gloria=0。
- `tools/import_store_operation_owner_from_nas.py --snapshot-date 2026-05-27 --preview-limit 20` dry-run 返回 validation_status=PASSED，matched=73，changed=17，new=0，exited=0，earliest_history_effective_start_date=2026-05-09。
- `cmd /c run_scheduled_total_control_v2.bat --conn-test --topic-only` 返回 exit_code=0，门店销售专题 SUCCESS。

**预防动作**：后续负责人临时占位变更先核对三点：是否仍有旧负责人残留、占位值大小写是否符合展示口径、是否存在显式生效日期导致历史回刷；再判断是否会阻塞总控 V2。

---

### [2026-05-26 15:21] · user-feedback · business-rule

**触发场景**：用户明确同步门店销售明细总和的免税 KPI 口径

**错误假设**：把免税外部月累计销售额纳入 Tableau 明细总计的月客单价、月折扣率分子，导致月客单价偏高、月折扣率超过 100%

**修正结论**：免税销售只参加月总达成计算；门店明细总计月客单价=非免税月累计销售额/总实际单数，月折扣率=非免税月累计销售额/非免税月累计吊牌金额

**证据**：
- 用户 2026-05-26 口径确认
- 销售部自动化日报.twb 门店经营明细_门店排名 Calculation_1730010000000410/0411
- 只读 MySQL 查询 2026-05-25 v1：非免税月销 13,546,668，总实际单数 7,317，非免税吊牌 14,670,301

**预防动作**：后续凡是把免税外部月累计销售接入 Tableau 或 ADS 派生 KPI，除月总达成外，先检查是否需要按 is_duty_free 排除免税销售分子

---

### [2026-05-26 10:34] · task · etl-diagnostics

**触发场景**：总控 V2 手动运行后，`test_etl_automation.py` 的 `dws_sales_daily` 近30天 Oracle/MySQL 对账超过 0.5% 阈值

**错误假设**：看到 Oracle/MySQL 对账异常时，优先怀疑 Oracle 或 ODS 同步漏数；但本次 MySQL ODS 按同口径聚合后与截图 Oracle 值完全一致

**修正结论**：本次异常定位在 ODS 到 legacy `dws_sales_daily` 之间，不在 Oracle/ODS；主链 legacy DWS 只回带近7天且覆盖校验只看近30天日期是否存在，历史业务日期的迟到 ODS 行可能已入 ODS，但未触发旧日期 DWS 重算。V2 预刷新链路按 20260426~20260526 窗口重算，销售对账为 SUCCESS

**证据**：
- `test_etl_automation.py` 使用 `date_id >= now-30` 查询 legacy `dws_sales_daily`，并与 `config.ORACLE_VERIFY_QUERIES['dws_sales_30d_summary']` 对账
- 2026-05-26 10:05 主链日志显示 legacy `dws_sales_daily` 仅同步 20260520~20260526
- 只读 MySQL 查询显示同口径 ODS 聚合为 rows=25313、sales=37269825.45、returns=10016339.8544，与截图 Oracle 值一致；legacy `dws_sales_daily` 为 rows=25151、sales=36172865.63、returns=9718526.88
- 差异集中在 20260426~20260519，20260520~20260526 与 ODS 对齐；20260430 的 DS030 京东自营销售在 ODS 有 sales=1027589.90、returns=283020.38，但 legacy DWS 为 0

**预防动作**：后续遇到 legacy `dws_sales_daily` 近30天对账异常时，先按 Oracle 同口径在 MySQL ODS 侧重算并与 legacy DWS 分日比对；若 ODS 对齐而 DWS 不齐，优先判断为历史业务日期迟到数据未重算，处理方向是受控回补 legacy DWS 对应业务日期窗口，而不是重跑 Oracle/ODS 或修改业务口径

---

### [2026-05-26 09:57] · user-feedback · business-rule

**触发场景**：用户指出 is_duty_free 判断还应看门店渠道类型是否包含免税，例如 联营-免税

**错误假设**：仅按当前已有 is_duty_free 或门店名称包含免税判断，忽略了 NAS 目标导入模板的 门店类型/report_channel_type 本身就是免税业务真值

**修正结论**：同步 dim_store_report_attr 时，NAS xlsx 的 门店类型/report_channel_type 是权威真值；若文本包含 免税，则 `is_duty_free='Y'`，否则必须落成 `N`，不再沿用历史有效属性中的旧值，也不再按门店名称兜底

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py 已将 _derive_duty_free_flag 改为优先检查 report_channel_type 包含免税
- test_import_cfg_store_target_daily_from_nas.py 新增 RT110/联营-免税 覆盖用例
- run_scheduled_total_control_v2.bat --conn-test --topic-only 已通过

**预防动作**：后续维护门店属性同步逻辑时，门店类型/渠道类型包含免税必须作为 is_duty_free=Y 的显式规则；非免税门店必须显式落成 `N`，不得继承旧有效属性或依赖门店名称兜底

---

### [2026-05-26 09:47] · task · schema-migration

**触发场景**：免税月累计 dry-run 在 RT110 修正后报 Unknown column target_month

**错误假设**：以为重新执行 CREATE TABLE IF NOT EXISTS DDL 就会把已存在的 report_date 旧表结构改成 target_month

**修正结论**：CREATE TABLE IF NOT EXISTS 不会迁移既有表字段；已存在旧表时必须单独执行 ALTER，将 cfg/log 两张免税表的 report_date 改为 target_month，并同步日志索引名

**证据**：
- 只读 information_schema 查询显示 cfg_duty_free_store_mtd_sales 与 log_duty_free_store_mtd_sales_import 仍只有 report_date、没有 target_month
- dry-run 报错：Unknown column 'target_month' in 'where clause'
- SQL/alter_duty_free_store_mtd_sales_target_month_20260526.sql 已提供人工 ALTER 脚本

**预防动作**：后续语义字段从 report_date 改 target_month 时，若表已存在，不要只给 create 脚本；必须先查 information_schema 并提供 ALTER 迁移脚本

---

### [2026-05-26 09:43] · user-feedback · business-truth

**触发场景**：用户确认 RT110 / 杭州萧山国际机场店确实属于免税门店

**错误假设**：只凭当前 dim_store_report_attr.is_duty_free=N 判断 RT110 不是免税门店，可能把维表真值漂移当成业务事实

**修正结论**：RT110 / store_id=708 / 杭州萧山国际机场店应按免税门店处理；正式跑免税月累计前，需要用户人工把当前有效 dim_store_report_attr.is_duty_free 修正为 Y，再重新 dry-run

**证据**：
- 只读查询显示 RT110 当前有效记录 id=219, store_id=708, report_channel_type=联营-免税, is_duty_free=N
- 用户 2026-05-26 明确确认 RT110 / 杭州萧山国际机场店 确实属于免税门店
- SQL/update_dim_store_report_attr_rt110_duty_free_20260526.sql 已提供人工修正脚本

**预防动作**：后续遇到 report_channel_type=联营-免税 但 is_duty_free=N 的门店，不能直接按非免税定论；应向用户确认业务真值，并由用户人工修正维表后再跑写库链路

---

### [2026-05-26 09:40] · task · field-semantics

**触发场景**：免税月累计 Excel 业务真值中 门店ID 列实际填写 RT 门店编码，且存在维表免税标记不一致

**错误假设**：把 门店ID 固定假设为数值 store_id，且只检查 Excel 表头是否正确，未先用真实 Excel dry-run 校验 store_code、空白月累计与 is_duty_free 前置条件

**修正结论**：免税月累计导入应兼容 dim_store.store_code，并在写库前校验 dim_store_report_attr.is_duty_free='Y'；若 RT 编码命中但免税标记为 N，应先由用户人工修正维表真值或确认剔除该门店，再跑总控 V2

**证据**：
- tools/import_duty_free_store_mtd_sales_from_nas.py 支持 store_code 解析
- dry-run 输出：以下门店当前 is_duty_free 不是 Y: [708]
- 只读查询：RT110 -> store_id=708, report_channel_type=联营-免税, is_duty_free=N

**预防动作**：免税专题上线前必须先跑导入工具 dry-run；遇到 RT 编码或空白月累计先确认解析规则，遇到 is_duty_free=N 不要直接跑总控写库

---

### [2026-05-26 09:17] · user-feedback · field-semantics

**触发场景**：用户指出免税月累计 Excel 首列 reportdate 应改为 目标月份

**错误假设**：把免税月累计外部文件首列先按日粒度 reportdate/report_date 设计，容易只改 Excel 表头而遗漏快照表、日志幂等键、调度受影响日期和 ADS join 的语义联动

**修正结论**：免税月累计外部快照应以 目标月份/target_month 为业务键；Excel 只提供月份，实际需要回刷的 report_date 由专题调度按 auto-report-date-mode 统一上界推导

**证据**：
- tools/import_duty_free_store_mtd_sales_from_nas.py 已改为 REQUIRED_HEADERS 包含 目标月份 并输出 target_month/target_month_start
- SQL/create_cfg_duty_free_store_mtd_sales.sql 与 SQL/create_log_duty_free_store_mtd_sales_import.sql 已改为 target_month 字段和索引
- scheduled_store_daily_report.py 已按 target_month 做免税链路幂等判重，并用调度统一上界生成受影响日期
- test_import_duty_free_store_mtd_sales_from_nas.py、test_scheduled_store_daily_report.py、test_ads_store_daily_report.py 已覆盖 target_month 语义

**预防动作**：后续凡是用户纠正外部 Excel 字段语义，必须按端到端契约变更处理：表头、解析对象、DDL、日志幂等键、调度日期推导、ADS join、单测、模板和文档一起核对。

---

### [2026-05-26 00:12] · task · tableau-twb

**触发场景**：用户在 Tableau 数据模型页看到 `sales.csv -> calendar.csv` relationship 报“类型不匹配”，虽然两边看上去都是日期字段

**错误假设**：默认认为只要 Oracle Custom SQL 两边都返回 `DATE`，Tableau relationship 就一定会自动识别为相同类型，不会再出现顶层字段类型漂移

**修正结论**：在 Tableau workbook XML 中，关系键是否兼容不只取决于底层 Oracle metadata；即使底层两边都还是 `SQLT_DAT`，只要顶层 `<column ... datatype=...>` 声明一个被定制成 `date`、另一个仍保留 `datetime`，Tableau 关系视图就可能直接报类型不匹配。遇到这种问题，先对照顶层字段声明，而不是急着重写 SQL 或 relationship 表达式

**证据**：
- `HEFANG复刻.twb` metadata：`sales.csv.[date]` 与 `calendar.csv.[date]` 都是 `remote-type=7 / DebugRemoteType=SQLT_DAT / local-type=datetime`
- `HEFANG复刻.twb` 顶层字段声明：`[date]` 已是 `datatype='date' datatype-customized='true'`，而 `[date (calendar.csv)]` 原先仍为 `datatype='datetime'`
- 2026-05-26 修复：将 `[date (calendar.csv)]` 改为 `datatype='date' datatype-customized='true'` 后，工作簿 XML 解析 `XML_OK`

**预防动作**：后续每次改 Tableau Oracle datasource 后，如果关系页出现“类型不匹配”，优先同时检查 1）底层 metadata `local-type`，2）顶层字段声明 `datatype` 是否被单边定制；不要只盯着 SQL 返回类型

### [2026-05-25 18:05] · task · tableau-twb

**触发场景**：用户在 `HEFANG复刻.twb` 中发现 `99.月份标签 = LEFT(DATENAME('month',[date]),1)` 不再显示英文月份首字母，而是显示 `6/7/8/9/1...` 这类数字月份标签

**错误假设**：默认认为 `DATENAME('month', [date])` 无论 Tableau 客户端是什么语言环境，都会稳定返回英文月份名，因此直接 `LEFT()` 就能得到模板里的英文首字母月份标签

**修正结论**：`DATENAME('month', [date])` 会受 Tableau 当前语言环境影响；在中文界面下，它返回的是本地化月份文本，`LEFT()` 取得的首字符不再等于英文月份首字母。若看板样式要求固定英文月份缩写或首字母，应改为 `DATEPART('month', [date])` + 手工映射，避免依赖本地化字符串

**证据**：
- `HEFANG复刻.twb` 中原公式：`LEFT(DATENAME('month',[date]),1)`
- 2026-05-25 用户截图：趋势图列头显示 `6/7/8/9/1...`
- 2026-05-25 修复后 `HEFANG复刻.twb` 已改为 `CASE DATEPART('month',[date]) ... END`

**预防动作**：后续凡是在 Tableau workbook 中需要“固定语言的月份/星期标签”，不要直接依赖 `DATENAME()` 的输出文本；优先用 `DATEPART()` 配合显式映射，保证在中文/英文客户端、不同 locale 设置下表现一致

### [2026-05-25 17:51] · task · business-rule

**触发场景**：免税门店只有外部月累计销售额，需要接入门店日报专题调度与 Tableau 总盘

**错误假设**：把免税外部快照当作完整销售事实，连同日销、销量、订单数、连带率、客单价、折扣率一起回填到 ADS。

**修正结论**：当外部来源只提供月累计销售额时，只能覆盖被该证据直接支持的下游字段；本仓库当前只允许免税链路覆盖 ads_store_daily_report 的 mtd_sales_amt、month_ach_rate 和 mtd_rank，其余指标继续沿用原交易事实或保持空值。

**证据**：
- 用户明确说明免税侧仅提供一个指标：月累计销售额
- etl_ads_store_daily_report.py 已仅对 mtd_sales_amt、month_ach_rate、mtd_rank 接入 cfg_duty_free_store_mtd_sales
- docs/DATA_CONTRACTS.md 已同步写明免税快照不反推日销、订单数、连带率、客单价或折扣率

**预防动作**：后续遇到业务外部 Excel/快照只提供聚合指标时，先冻结可安全覆盖的字段清单，再实施导入与 ADS 覆盖；未被原始证据支持的派生指标不得一并改写。

---

### [2026-05-25 17:18] · task · field-mapping

**触发场景**：用户指出 `HEFANG复刻.twb` 当前 `product_code` 仍是 SPU 条码粒度，要求改成 SKU 粒度，并补齐 `sku_barcode`、`color`、`size`，让商品行级展示能落到“商品名称 + color + size”

**错误假设**：默认认为只要把 Tableau `products.csv` 的 Custom SQL 从 `M_PRODUCT` 改成 `M_PRODUCT_ALIAS`，并把 `product_code / product_name` 替换成 SKU 字段，workbook 就会自然变成 SKU 粒度，不需要同步修改事实表键和 relationship

**修正结论**：当 Tableau workbook 的商品维度从 SPU 升到 SKU 时，必须同步让事实侧暴露 `M_PRODUCTALIAS_ID`，并把 relationship 从 `sales.product_id = products.product_id` 改成 `sales.sku_id = products.sku_id`；否则一个 SPU 会对应多个 SKU，关系模型会在分析时产生一对多膨胀，导致行级明细和聚合 KPI 失真。`color / size` 可取自 `M_ATTRIBUTESETINSTANCE.VALUE1 / VALUE2`，`sku_barcode` 取自 `M_PRODUCT_ALIAS.NO`

**证据**：
- etl_dim_sku.py
- 外部参考工作簿 `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/invertory_DashBoard_main.twb`
- 2026-05-25 Oracle 只读核验：近 24 个月 RT 门店销售明细 `M_PRODUCTALIAS_ID` 空值行数 = `0`
- 2026-05-25 `HEFANG复刻.twb` XML 修复：`sales.csv` 增加 `sku_id`，`products.csv` 改为 SKU 粒度 SQL，relationship 改为 `sku_id`

**预防动作**：后续凡是改 Tableau 维表粒度，不要只盯着维表 SQL；必须同时核对 1）事实侧对应键是否存在且覆盖完整，2）relationship 是否跟着切换，3）字段面板是否已注册新键和新增维度字段，否则很容易做出“字段看起来更细了、关系实际上还是旧粒度”的假修复

### [2026-05-22 16:00] · user-feedback · business-rule

**触发场景**：用户指出 2026-05-21 销售日报 KPI05 业务上应为 `-1.89%`，但 `ads_store_daily_report` 重跑后明细表同比总计却显示 `+0.59%`

**错误假设**：默认认为只要 `ads_store_daily_report` 已把 `yoy_rate` 切成 same-store 辅助分子分母，就必然与 KPI05 的同店同比一致，没有继续核对 same-store 集合里是否误纳了 `assignment_role='快闪'` 的快闪店

**修正结论**：门店日报 same-store 口径必须排除 `assignment_role='快闪'` 的源门店；快闪金额只能进入“同店+当期快闪同比”，不能混入 `same_store_mtd_sales_amt / same_store_last_year_mtd_sales_amt`。2026-05-21 的漂移就是 RT014 `快闪店专用` 被误纳入 same-store，额外带入 `289437.60 / 65062.00`，把整体同比从 `-1.89%` 拉成了 `+0.59%`

**证据**：
- etl_ads_store_daily_report.py
- test_ads_store_daily_report.py
- 2026-05-22 只读 SQL 对账：`ads_store_daily_report` 汇总同店辅助金额 = `9132756.06 / 9078911.96 => +0.59%`
- 2026-05-22 只读 SQL 对账：KPI05 原 SQL 复算 = `8843318.46 / 9013849.96 => -1.89%`
- 2026-05-22 差集定位：RT014 `快闪店专用` assignment_role=`快闪`

**预防动作**：后续凡是 same-store 与“同店+当期快闪”并存的日报/看板，必须先把快闪门店从 same-store 集合里剔除，再单独做 popup uplift；不要只凭“去年同期有销售”判断是否纳入 same-store

### [2026-05-22 13:58] · user-feedback · field-mapping

**触发场景**：用户明确否定“直接改销售日报 workbook XML 让同比率对齐”的路线，要求先回退 workbook，再把 `ads_store_daily_report` 的同比列改成同店同比

**错误假设**：默认认为只要把 ADS 字段口径改掉，当前 Tableau 明细表的同比率与总计就会自动跟着变成同店同比，没有先确认 worksheet 是直接读取 ADS 字段还是在本地用旧分子分母公式重算

**修正结论**：凡是从 Tableau 展示问题回切到 ETL 口径修复时，必须先核对当前 worksheet 是否直接消费目标字段；如果工作簿仍在本地用 `SUM([mtd_sales_amt]) / SUM([last_year_mtd_sales_amt]) - 1` 一类公式重算，单改 ADS 不会自动改变现有展示总计，需把“数据层口径修复”和“展示层字段绑定修复”拆开管理

**证据**：
- etl_ads_store_daily_report.py
- test_ads_store_daily_report.py
- SQL/alter_ads_store_daily_report_add_same_store_yoy_helpers.sql
- 外部 Tableau 工作簿 `销售部自动化日报.twb` 当前明细表同比率公式 = `IF SUM([last_year_mtd_sales_amt]) = 0 THEN NULL ELSE SUM([mtd_sales_amt]) / SUM([last_year_mtd_sales_amt]) - 1 END`

**预防动作**：后续凡是用户要求“改 ETL 口径就行”时，先检查 Tableau/报表是否真的消费该字段；若展示层仍有本地公式或自定义 SQL 派生，必须在实施前明确告知“只改 ETL 能改数据语义，但未必能改当前页面展示结果”

---

### [2026-05-21 12:00] · task · tableau-twb

**触发场景**：用户指出 HEFANG实时销售战情看板 中 今日达成率 与 线性进度偏差 同屏不一致，截图里 5.8% 与 14.44% 无法推出 -4.30pp

**错误假设**：默认认为线性进度偏差与日达成率既然来自同一 datasource，就可以在 Text calc 内再次直接写 SUM(day_sales_amt) / SUM(day_target) 后再减时间进度，不会影响结果

**修正结论**：在 Tableau relationship 模型里，若某个 KPI 文本本质上只是已展示 measure 的派生值，应直接复用已验证正确的 measure；不要在 Text calc 中重新裸算跨逻辑表分子分母，否则可能与旁边的 KPI 卡片产生不同聚合语义

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2421
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2426
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2292
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2293

**预防动作**：后续在 Tableau Text worksheet 排查同屏 KPI 不一致时，先检查本地 datasource-dependencies 是否复用了同一 measure；如果只是要展示 差值 / 标签 / 文案，优先基于现成 measure 做派生，不要重复写底层比值公式

---

### [2026-05-21 11:44] · task · tableau-twb

**触发场景**：用户指出 HEFANG 实时战情看板的 实时战情_今日累计销售进度 到 11 点仍接近 0，问题集中在 今日累计销售额_实时累计趋势 取值，而不是累计目标虚线样式。

**错误假设**：只按 caption / calculation id 在整个 workbook 全局搜索并修改字段，默认认为改到任意同名 root calculation 就会影响当前 worksheet 渲染。

**修正结论**：修复 Tableau 同名 calculation 问题时，必须先锁定当前 worksheet 的 datasource alias、Measure Names / Multiple Values 绑定和本地 datasource-dependencies；本次真正生效的是 federated.3cumprogresstargetlive，本地副本需要与 root 一起切到 SALES_AMT_RAW 聚合，并把 SALES_AMT 的 LOD 键改为 STORE_ID (Hourly Sales)。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L734
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L745
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2750
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2758
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L2874

**预防动作**：后续处理 Tableau .twb 中的字段失效或取值异常时，先按当前 worksheet 引用的 datasource alias 定位，再同步检查该 worksheet 的本地 datasource-dependencies 和 Multiple Values 绑定；不要再基于全局 grep 命中直接批量改所有同名 calculation。

---

### [2026-05-21 11:25] · user-feedback · tableau-twb

**触发场景**：用户在回看 `HEFANG门店实时销售战情看板.twb` 的 `实时战情_今日累计销售进度` 图后，明确指出上一轮“把累计目标虚线截断到 `LATEST_HOUR`”的修改完全错误，要求立即回退。

**错误假设**：把累计目标虚线当成可随显示效果自由调整的视觉辅助线，误以为截断到 `LATEST_HOUR` 能修复图表问题。

**修正结论**：这张图的累计目标虚线本身就是全天目标进度线，不应因当前时点显示效果而改成只画到 `LATEST_HOUR`。若图表观感异常，应优先检查累计销售序列、mark 类型、双轴配置或其它展示层问题，不能直接改累计目标虚线定义。

**证据**：
- 工作簿/HEFANG门店实时销售战情看板.twb#L740
- 工作簿/HEFANG门店实时销售战情看板.twb#L2753
- 用户反馈：`回退，这次改动完全错了，你把 累计目标虚线调整了`

**预防动作**：后续处理 Tableau 实时图的“显示不对”问题时，先区分“展示层问题”与“业务/指标定义问题”；只要用户未明确要求，就不要改单张图里承担业务语义的目标线、基准线或阈值线定义。

### [2026-05-21 11:07] · task · tableau-twb

**触发场景**：用户在 `HEFANG门店实时销售战情看板.twb` 中发现，10:45 左右左上角 `今日实时销售额` 只有几千元，但 `今日达成率` 却显示 `60.8%`，底部 `门店实时销售明细` 还出现多家过万金额。

**错误假设**：默认把左上角 hourly live 卡片、日达成率 KPI 和门店明细都当成同一条 realtime 口径，只在单个 datasource 上做局部修补。

**修正结论**：遇到“小时趋势 / 左上角实时额看起来正确，但日达成率和门店明细像全天数据”的矛盾画面时，先直接 Oracle 只读复算小时销售与当日累计，判断源库事实。若 hourly live 与源库一致，而 KPI / 明细明显偏大，应把这些 sheet 统一到同一套 `hourly_sales` 累计 SQL 路径，并同步修改 datasource 根 relation 与 `object-graph` 副本，不要继续并存两条不同实现路径的 realtime SQL。

**证据**：
- 工作簿/HEFANG门店实时销售战情看板.twb#L1387
- 工作簿/HEFANG门店实时销售战情看板.twb#L1938
- docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md#L32

**预防动作**：后续凡是同一张 realtime dashboard 上会被用户肉眼互相对账的实时额、达成率和门店明细，优先复用同一套 hourly cumulative SQL 逻辑；排障时先做源库只读复算，再决定是改 SQL 口径还是查客户端刷新 / 缓存问题。

### [2026-05-20 13:51] · task · tableau-twb

**触发场景**：销售日报 Tableau 门店经营明细启用底部总计后，用户发现 KPI 卡片与明细总计比例不一致

**错误假设**：默认认为 Tableau 开启总计行后会自动把日达成率、月达成率等比例字段按 KPI 口径重算，忽略了视图仍使用 AVG 行级 rate 字段

**修正结论**：Tableau 明细表总计行若要和 KPI 卡片对齐，比例字段必须切到聚合后计算字段，例如 SUM(day_sales_amt)/SUM(day_target)、SUM(mtd_sales_amt)/SUM(month_target)，不能继续使用 AVG(day_ach_rate) 或 AVG(month_ach_rate)

**证据**：
- 销售部自动化日报.twb: 门店经营明细_门店排名 原使用 avg:day_ach_rate/qk、avg:month_ach_rate/qk、avg:yoy_rate/qk
- XML validation: XML_OK after switching to usr:Calculation_1730010000000403/0404/0405

**预防动作**：后续凡是 Tableau 明细表或排名表启用总计行，必须逐列检查比例/均值类指标总计口径；需与 KPI 对齐时优先复用聚合后 calculation，而不是暴露行级 rate 的 AVG 聚合。

---

### [2026-05-20 13:15] · user-feedback · tableau-twb

**触发场景**：用户在核对 `销售部自动化日报.twb` 的“销售贡献占比”饼图时，发现勾选“其他”后会额外冒出 6.7%，并进一步确认 `直营-奥莱`、`联营-奥莱`、`联营-免税` 等渠道类型被错误归入“其他”。

**错误假设**：默认认为 Tableau 渠道组计算只要覆盖标准 `直营/联营/小程序` 文本即可，不需要对带后缀的渠道类型变体做防御式匹配。

**修正结论**：`report_channel_type` 的 Tableau 渠道组计算必须统一为 `TRIM + CONTAINS` 的防御式分类：空值归“其他”，包含“小程序”归“小程序”，包含“直营”归“直营”，包含“联营”归“联营”。这样才能稳定覆盖 `直营-奥莱`、`联营-奥莱`、`联营-免税` 等变体，避免它们被错误落入“其他”，并在排除“其他”的 LOD 分母下放大占比偏差。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb#L1525
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb#L1539
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb#L1548

**预防动作**：后续凡是 Tableau 按渠道、店型等业务字符串做分组时，不要只按标准值或前缀想当然判断；先枚举已落库的实际变体值，再在 calculation 中用 `TRIM + CONTAINS` 或显式映射覆盖，尤其要在占比 / LOD 图表上额外验证勾选“其他”后的残余占比是否合理。

---

### [2026-05-14 15:00] · user-feedback · etl-architecture

**触发场景**：用户明确指出，`dim_store` 不应再因为 Oracle `C_STORE.ISACTIVE='N'` 就被主链物理剔除，而应保留全量店仓并把有效状态交给下游判断。

**错误假设**：默认把 `dim_store` 当成“当前活跃门店清单”，认为只抽 `ISACTIVE='Y'` 足以支撑门店主题 ETL，停用/闭店门店应在维表层直接消失。

**修正结论**：`dim_store` 属于基础店仓维表，应全量抽取 Oracle `C_STORE` 并保留 `is_active` 状态；是否继续参与日报、目标、负责人或历史口径，应由下游 ETL 与配置表生效区间决定。当前问题根因也不在 v2 ODS 缺 `dim_store`，因为 `dim_store` 仍是 Oracle 直抽 DIM，全链路都复用 `etl_dim_store.py`。

**证据**：
- etl_dim_store.py#L17
- etl_dim_store.py#L38
- run_etl.py#L52
- docs/ETL业务逻辑说明.md#L201

**预防动作**：后续设计 DIM 主数据对象时，不要把源系统的运行态有效标记直接当成“物理删行条件”；若下游仍有历史配置、目标快照或专题 ADS 依赖该实体，应优先全量保留并在消费层显式过滤。

### [2026-05-14 13:40] · task · tableau-schema

**触发场景**：吸收 `tableau/tableau-document-schemas` 官方 XSD 时，`xmlschema` 直接加载 `twb_2026.1.0.xsd` 报 `unknown attribute group 'user:UserAttributes-AG'`，且 HEFANG 存量工作簿根节点版本为 `18.1`。

**错误假设**：默认把官方 TWB XSD 当作可直接加载且可覆盖所有 Tableau 2025.x 生成工作簿的通用校验器。

**修正结论**：官方公开 schema 当前实测从 2026.1 / workbook version `26.1` 起；主 XSD 还需要本地 `user` namespace companion schema 才能加载。HEFANG `18.1` 存量 workbook 应返回 `skipped`，不得为通过 XSD 擅自升 `version` / `original-version`。

**证据**：
- mcp_servers/tableau_worksheet_mcp/src/tableau_worksheet_mcp/official_schema.py#L60
- mcp_servers/tableau_worksheet_mcp/src/tableau_worksheet_mcp/official_schema.py#L90
- docs/Tableau_TWB编译知识库/官方Schema吸收指南_20260514.md#L9
- docs/Tableau_TWB编译知识库/官方Schema吸收指南_20260514.md#L26

**预防动作**：后续做 Tableau TWB schema 校验时先读取 workbook root version；只有官方覆盖版本才执行 XSD 校验，旧版记录 `skipped` 并继续字段引用与 Tableau 重开渲染验证；加载官方 XSD 时保留 `user` namespace adapter。

---

### [2026-05-14 13:13] · task · tableau-twb

**触发场景**：用户首次重开 HEFANG 门店实时销售战情看板后回传截图，顶部摘要卡空白、时间进度只剩标题，6 张 KPI 卡出现标题重复。

**错误假设**：为 dashboard 文本卡片同时保留了 worksheet 内置 title 和 customized-label，默认认为小高度固定 zone 里这两层标题可以共存。

**修正结论**：当 Text worksheet 已用 customized-label 承担卡片标题和正文时，固定高度较小的 dashboard zone 不应再保留 worksheet 内置 title；否则 title 会先占可用高度，导致正文被裁掉，或形成 KPI 双重标题。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L889
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L955
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L1063
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L1378

**预防动作**：后续凡是用 Text worksheet 做 dashboard 卡片，先判断标题语义由谁承担；如果 customized-label 第一行已经是卡片标题，就默认移除 worksheet title，尤其在固定高度较小的 zone 中。

---

### [2026-05-14 13:01] · task · tableau-twb

**触发场景**：为 HEFANG 门店实时销售战情看板补页头与 KPI 首屏时，准备复用 销售部自动化日报.twb 的 KPI 文案与公式模式

**错误假设**：默认打算把参考日报里依赖前一期或较上期数据的 KPI 趋势公式直接搬到当前实时战情 workbook，没有先核对目标 MySQL datasource 的 SQL 是否只保留最新 report_date 与 data_version='v1' 快照。

**修正结论**：当 Tableau datasource 只返回最新快照时，首版 KPI 卡应先改写为当前状态汇总公式，例如 SUM(day_sales_amt)、SUM(day_target)、SUM(mtd_sales_amt) 以及汇总后达成率；前期、环比、较上期文案必须等引入历史快照或独立历史 datasource 后再接。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L257
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L258
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L1160
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L1350

**预防动作**：后续凡是从参考 Tableau 工作簿借 KPI 模式时，先读目标 datasource 的实际 SQL 与时间粒度，先判断它是最新快照型还是历史序列型，再决定能否复用趋势文案与对比公式。

---

### [2026-05-14 11:47] · task · tableau-twb

**触发场景**：HEFANG门店实时销售战情看板.twb 可打开后，实时战情_分时销售 中的中文字段全部出现红色感叹号，Tableau 提示字段在数据库中不存在

**错误假设**：在手工编译 Oracle live Custom SQL datasource 时，只按 XML 里自定义的小写 alias 构建 metadata、根字段和 worksheet 引用，没有继续核对 Tableau 客户端对 Oracle 未加双引号 alias 的实际列名解析结果。

**修正结论**：Oracle live Custom SQL 若使用未加双引号的 alias，Tableau 客户端实际会按大写列名识别字段；因此 datasource 的 SQL alias、metadata-record local-name、根 column name、datasource-dependencies、column-instance、tooltip、rows/cols 必须统一到同一套大写列名，caption 再写中文。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L35
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L203
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L707
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L773

**预防动作**：后续凡是给 Oracle live datasource 手工补字段中文 caption 时，先在 Tableau 客户端或已验证样板里确认最终列名大小写，再统一整条引用链；不要在 XML 里自造一套小写根字段名。

---

### [2026-05-14 11:39] · task · tableau-twb

**触发场景**：在 HEFANG门店实时销售战情看板.twb 中，把 shelf-sorts 改成 computed-sort 后，用户再次用 Tableau 客户端重开验证

**错误假设**：默认认为只要把不兼容的 shelf-sorts 替换为 computed-sort 就足够，没有继续核对目标 workbook 顶部 document-format-change-manifest 是否包含排序相关 feature flag。

**修正结论**：在从空白或极简 .twb 起步的 workbook 中，computed-sort 是否可用不仅取决于 view 内 XML 写法，还取决于 workbook 级 manifest 是否补齐 IntuitiveSorting、IntuitiveSorting_SP2、SortTagCleanup；缺少这些开关时，Tableau 会直接把 computed-sort 视为未声明节点。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L7
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L815
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售驾驶舱_第一批_20260420.twb#L9

**预防动作**：后续凡是给空白/极简 workbook 注入 computed-sort、shelf-sorts、customized-tooltip 等依赖 feature gate 的结构时，先做两层比对：一层比 view 片段，一层比 document-format-change-manifest；不要只复制局部 XML。

---

### [2026-05-14 11:33] · task · tableau-twb

**触发场景**：在 HEFANG门店实时销售战情看板.twb 中新增排行 worksheet 后，为 rows 排序直接照搬其它 workbook 的 shelf-sorts / shelf-sort-v2 结构，随后用户在 Tableau 客户端重开验证

**错误假设**：默认认为参考工作簿里可用的 shelf-sorts 节点可以直接跨 workbook 复制，不需要核对目标 workbook 当前 schema / manifest 是否接受这套排序结构。

**修正结论**：在当前这份实时战情 workbook 中，view 内不接受 shelf-sorts，必须改用兼容度更高的 computed-sort 或 manual-sort；否则会在重开时触发 D2E8DA72 和 no declaration found for element shelf-sorts。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L812
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售驾驶舱_第一批_20260420.twb#L4473

**预防动作**：后续从参考 .twb 借排序 XML 时，先用目标工作簿当前已存在的合法排序节点做基线；若目标文件里从未出现 shelf-sorts，就默认先用 computed-sort / manual-sort，不要把高阶排序结构直接塞进新 view 里。

---

### [2026-05-14 11:08] · task · tableau-twb

**触发场景**：为 HEFANG门店实时销售战情看板.twb 从空白 workbook 骨架注入 Oracle/MySQL datasource 后，用户首次关闭并重开 Tableau 工作簿进行渲染验证

**错误假设**：默认认为只要直接复用已验证 .twb 里的 datasource / Custom SQL XML 片段即可，不需要先比对目标空白 workbook 的 document-format-change-manifest 是否具备相同 feature flag。

**修正结论**：当 datasource 语义层包含 datatype='table' 这类对象模型枚举时，目标 workbook 必须先具备对应的 manifest 开关；本次空白骨架缺少 ObjectModelTableType，导致 Tableau 在重开时把内部对象列的 table 判成非法枚举并报 D2E8DA72。

**证据**：
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L5-L13
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/HEFANG门店实时销售战情看板.twb#L193
- D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb#L11

**预防动作**：后续凡是从空白 .twb 起步再注入 datasource、Custom SQL、table-type 内部对象列时，先对齐参考 workbook 的 document-format-change-manifest，至少检查 ObjectModelTableType 等对象模型相关 feature flag；不要只复制 datasource 块后再靠重开报错反推缺项。

---

### [2026-05-14 09:30] · task · cutover-runtime

**触发场景**：2026-05-14 00:05 Windows 自动触发 `run_scheduled_total_control_v2.bat` 时，总控在 `DWS v2 读源预刷新` 阶段失败，主链和专题链被主动跳过

**错误假设**：默认认为 inventory ADS gate 使用的“当前 ODS 基线”在切到 V2 wrapper 后仍可以继续依赖 same-day old `dws_inventory_daily`；于是把 `old_dws_max_etl_time=None` 直接当成 pre-refresh 失败

**修正结论**：当 Windows 计划任务入口已经整体切到 V2 wrapper 时，00:05 pre-refresh 本身就是生成 same-day `_v2` 读源的最早步骤，same-day old `dws_inventory_daily` 尚未产出属于预期。此时若未请求 same-snapshot 诊断，应把 inventory old DWS 基线记为 `SKIPPED`，并改由 `dwd_inventory_storage_snapshot -> dws_inventory_daily_v2` 自洽结果决定是否放行主链；只有显式 same-snapshot 对齐场景才应继续阻断。

**证据**：
- logs/scheduled_total_control_20260514.log
- reports/context_cache/scheduled_dws_v2_shadow_20260514_000826.json
- scheduled_dws_v2_shadow.py
- test_scheduled_dws_v2_shadow.py
- test_scheduled_total_control.py

**预防动作**：后续凡是把原本“旁路 / 非阻断”的 shadow 链路前置成生产 pre-refresh，都要重新检查 gate 是否还依赖旧主链同日产物；如果总控已经改成“先 pre-refresh 再主链”，就不能再把 same-day old 产物缺失当成自动失败。

---

### [2026-05-13 09:42] · task · cutover-runtime

**触发场景**：2026-05-13 用户将 Windows 计划任务入口切到 run_scheduled_total_control_v2.bat 后，09:09 总控失败且 ads_inventory_health 写出 0 行

**错误假设**：默认沿用主链 -> 专题 -> 后置 DWS v2 shadow 的总控顺序，忽略了生产 ADS 在 v2 模式下已经先读取 dws_inventory_daily_v2 / dws_sales_daily_v2；后置 shadow 对 fresh date 来说太晚

**修正结论**：一旦生产 ADS 切到 V2 DWS 读源，DWS v2 刷新必须成为主链前置阻断步骤；预刷新阶段应跳过持久化 ADS compare，等主链 ADS 写出后再做可比性判断

**证据**：
- logs/scheduled_total_control_20260513.log
- logs/etl_20260513.log
- scheduled_total_control.py
- scheduled_dws_v2_shadow.py
- reports/m6_v2_prerefresh_tests_20260513.txt

**预防动作**：后续任何 cutover 从 report-only shadow 转为生产读源时，必须把依赖图重排为 source refresh -> consumer ADS，并用调度单元测试覆盖顺序与前置失败阻断行为

---

### [2026-05-12 17:50] · task · etl-architecture

**触发场景**：总控 V2 双跑 gate 中 DWS v2 shadow 主体对账为 0 mismatch，但 ads_inventory_health 报告型 compare 仍 WARNING

**错误假设**：只核对生产 ADS 写入 SQL 是否成功，容易忽略 shadow 报告型 compare 会复用同一投影 SQL 并要求内层列别名与外层 ranked.* 字段完全一致

**修正结论**：ads_inventory_health 的 shadow 投影需要显式保留 color/size 别名：内层 SELECT 使用 sku.sku_color AS color、sku.sku_size AS size，外层才能稳定读取 ranked.color、ranked.size；修复后必须加单元测试覆盖 SQL 字符串中的别名链路

**证据**：
- etl_ads_health.py
- test_scheduled_dws_v2_shadow.py
- reports/context_cache/scheduled_dws_v2_shadow_20260512_165855.json
- reports/context_cache/scheduled_dws_v2_shadow_20260512_172320.json
- reports/m5_v2_gate_followup_tests_20260512.txt

**预防动作**：后续新增 ADS shadow/report-only compare 时，必须同时检查内层投影别名、外层 ranked.* 引用和持久化 ADS 字段名；不要只以生产写入成功判断 shadow compare SQL 可执行。

---

### [2026-05-12 16:43] · user-feedback · tableau-twb

**触发场景**：用户追问销售日报‘去年同期同比’为何与业务口径的同店同比 8.5% 不一致，并要求同时修正明细表字段语义

**错误假设**：仅凭 Tableau 卡片名称把‘去年同期同比’默认理解为同店同比，没有先核对实际 TWB 公式、ADS 字段和文档定义

**修正结论**：当前销售日报 KPI05 与 ads_store_daily_report.yoy_rate 的真实口径都是全量汇总同比：mtd_sales_amt / last_year_mtd_sales_amt - 1，不含同店过滤；若业务需要同店同比，必须先明确同店门店集合与过滤规则，再新增独立指标或改口径

**证据**：
- etl_ads_store_daily_report.py#L444
- etl_ads_store_daily_report.py#L448
- docs/销售部数据治理-子项目/销售日报自动化看板模板对齐与视觉分层跟进.md#L168
- 外部 Tableau 工作簿 KPI05 当前公式 = IF SUM([last_year_mtd_sales_amt]) = 0 THEN NULL ELSE SUM([mtd_sales_amt]) / SUM([last_year_mtd_sales_amt]) - 1 END

**预防动作**：后续凡是 Tableau 指标名称带同比、环比、达成率等业务词，不要按名称先验推断口径；必须同时核对 TWB 公式、ADS 字段来源和文档定义，再决定是否需要新增同店/同口径版本。

---

### [2026-05-12 16:33] · task · cutover-runtime

**触发场景**：准备两次总控 V2 gate 时发现 run_scheduled_total_control.bat 调用 scheduled_total_control.py 但未透传 %* 参数

**错误假设**：只确认 Python CLI 支持 --cutover-mode v2 不够；如果 Windows wrapper 不透传参数，用户通过 bat 执行 --cutover-mode v2 会被静默丢弃，实际仍跑默认 legacy

**修正结论**：总控 wrapper 必须使用 scheduled_total_control.py %* 透传追加参数；另新增显式 V2 wrapper 预置 --cutover-mode v2，并用单元测试静态检查 wrapper 内容

**证据**：
- run_scheduled_total_control.bat
- run_scheduled_total_control_v2.bat
- test_scheduled_total_control.py
- reports/cutover_v2_wrapper_validation_20260512.txt

**预防动作**：后续所有调度 bat wrapper 增加 CLI 参数时，都要同时检查 wrapper 是否透传 %*，并至少补 --help 或静态测试验证参数未被丢弃

---

### [2026-05-12 15:44] · task · etl-architecture

**触发场景**：为主链新增 cutover_mode 与 rollback_to_legacy 后，专题门店日报的 freshness 读源也需要随 cutover 语义切换，而且 run_schedule_once 既可能被 CLI 调用，也可能被包装层直接调用

**错误假设**：默认认为只要在 argparse 层给 --sales-freshness-source 设好默认值，或只把 cutover_mode 从总控透传下去，专题链就会自然与主链保持一致；把默认语义停留在 CLI 解析层即可

**修正结论**：cutover 相关默认语义必须在真正执行的运行函数里解算，而不能只依赖 CLI 默认值。应先统一 resolve_cutover_mode，再在运行函数内按 cutover_mode 推导 store_daily freshness：legacy 读 legacy，shadow_compare/v2 默认读 v2，显式 sales_freshness_source 仍优先；同时主链 ads_inventory_health 也必须保持 shadow_compare=旧链写数+v2 对账、只有 v2 才真正切读源，避免把观察模式误做成生产切换

**证据**：
- cutover_controls.py#L29
- cutover_controls.py#L49
- scheduled_store_daily_report.py#L2075
- run_etl.py#L782
- run_etl.py#L786
- run_etl.py#L802

**预防动作**：后续凡是新增 cutover/rollback 开关，都按两步检查：1）默认值是否在实际运行函数内而不是 parser 层解算；2）shadow_compare 是否仍保持 non-invasive，只做对账不改生产读源。若某函数既支持 CLI 又支持包装层直接调用，必须在函数入口自行归一化 mode 与派生 freshness/source。

---

### [2026-05-12 14:39] · task · scd2-interval

**触发场景**：对 RT117 做真实 dry-run 时，虽然 `earliest_history_effective_start_date` 已正确命中 2026-05-09，但摘要同时出现 `unchanged=0 / changed=72`，暴露出默认未填日期行被全量误判为 changed。

**错误假设**：默认把未填写 `生效日期` / `失效日期` 的负责人行直接解释成 `effective_start_date=snapshot_date` 后，再把生效起止日期一并纳入 `unchanged` 判等，不会影响“当前真值”日常导入。

**修正结论**：对未显式填写日期的当前真值行，`unchanged` 判等只能看实体与负责人 payload，不能把默认日期值当成真实业务变更；否则每次新 snapshot_date 都会把整张负责人历史误切一遍。只有显式区间变化或负责人实际变化时，才应进入 `changed`。

**证据**：
- tools/import_store_operation_owner_from_nas.py#L59
- tools/import_store_operation_owner_from_nas.py#L276
- tools/import_store_operation_owner_from_nas.py#L683
- tools/import_store_operation_owner_from_nas.py#L733
- test_store_operation_owner_import.py#L237
- reports/context_cache/owner_import_dry_run_20260512_rt117.json
- reports/context_cache/owner_import_dry_run_20260512_rt117_after_fix.json

**预防动作**：后续凡是“可选显式生效区间 + 默认当前真值”混合模式的 SCD2 导入，都要先做一次真实 dry-run 检查 diff 摘要：若未填日期的场景出现大面积 `changed`，优先检查判等逻辑是否把默认日期误当成业务变更。

---

### [2026-05-12 14:09] · user-feedback · downstream-compatibility

**触发场景**：用户明确约束影子链后续若替代旧链，ADS 相关 MySQL 表仍会被 Tableau 和其他下游继续消费。

**错误假设**：默认只要影子链计算结果对齐，就可以在替代旧链时自由调整 ADS 既有字段名，或通过重命名列来收口新旧口径。

**修正结论**：ADS 物理表的既有字段名属于外部消费契约。影子链替代旧链时，允许新增字段，但不得改名或删除既有 ADS 字段；若未来确需改名，必须先完成 Tableau 等消费层迁移并由用户明确确认。

**证据**：
- 用户当轮明确确认“当前 ADS 相关 etl 和 mysql 中的表结构，这些 ADS 都已经被 Tableau 和其他途径消费；切换时不允许导致 ADS 表结构字段名改变”。
- `docs/ARCHITECTURE.md`
- `docs/DATA_CONTRACTS.md`

**预防动作**：后续任何 DWS v2 -> ADS 切换、DDL 草案、ETL 改写或回滚方案，都把“既有 ADS 字段名不可改、仅允许新增列”作为首条兼容性检查项。

### [2026-05-12 14:05] · user-feedback · business-rule

**触发场景**：RT117 属于今天补录但需从 2026-05-09 起生效的负责人变更，且业务明确只能通过 Excel 负责人映射表自行修正

**错误假设**：默认认为负责人导入只需要维护 `snapshot_date` 当天的当前快照，必要时再依赖后台专题调度参数补回历史日期。

**修正结论**：门店负责人映射表是业务与后端交互的唯一入口时，导入器必须兼容可选 `生效日期`、`失效日期` 两列，调度也必须按 `earliest_history_effective_start_date` 自动扩展负责人链路回刷窗口，不能把历史回填责任转嫁给后台手工参数。

**证据**：
- tools/import_store_operation_owner_from_nas.py#L40
- tools/import_store_operation_owner_from_nas.py#L545
- tools/import_store_operation_owner_from_nas.py#L802
- tools/import_store_operation_owner_from_nas.py#L842
- tools/import_store_operation_owner_from_nas.py#L1177
- scheduled_store_daily_report.py#L765
- scheduled_store_daily_report.py#L822
- scheduled_store_daily_report.py#L833
- scheduled_store_daily_report.py#L1609

**预防动作**：后续凡是业务只能通过 Excel 维护历史生效关系的链路，都要同时检查三件事：导入模板是否能表达生效区间、历史表切片是否按业务起始日切换、下游调度是否按最早受影响日期自动回刷；不能只修其中一层。

---

### [2026-05-12 13:32] · task · etl-validation

**触发场景**：inventory same-snapshot 已确认不可作为 ads_inventory_health gate 的默认判定前提

**错误假设**：默认把 old dws_inventory_daily.etl_time 反推的 source_loaded_at cutoff 当成 inventory ADS gate 的必要验证条件，导致 same-snapshot 诊断失败时连带阻塞了 current baseline 下本来可独立判断的 gate 结论

**修正结论**：inventory 是否可用于 ADS gate，应优先看当前 ods_fa_storage 可比基线是否与 old DWS 对平，以及 dwd_inventory_storage_snapshot -> dws_inventory_daily_v2 是否自洽。old/v2 same-snapshot 只能作为精确诊断工具，不能再当 ADS gate 默认前提。

**证据**：
- scheduled_dws_v2_shadow.py#L443
- scheduled_dws_v2_shadow.py#L585
- scheduled_dws_v2_shadow.py#L724
- reports/context_cache/scheduled_dws_v2_shadow_20260512_131626.json#L117
- reports/context_cache/scheduled_dws_v2_shadow_20260512_131626.json#L168
- reports/context_cache/scheduled_dws_v2_shadow_20260512_131626.json#L193

**预防动作**：后续凡是评估 shadow 链是否可供 ADS gate 使用，先拆成 current baseline verdict 与 old/v2 diagnostic 两条线：前者决定是否可推进，后者只用于解释旧链差异，不要再把两者混成一个通过条件。

---

### [2026-05-12 11:49] · task · etl-validation

**触发场景**：复核 scheduled_dws_v2_shadow.py --inventory-align-with-old-dws 后，发现 inventory same-snapshot 已生效，但 ADS gate mismatch 未下降反而扩大

**错误假设**：默认认为只要 inventory v2 固定到 old DWS 的 max(etl_time)，old-v2 差异就会自然收敛，因此把排查重点继续放在 ADS 公式层

**修正结论**：same-snapshot 只能消除比较时点不一致；若 shadow 库存 DWD 的 source_max_loaded_at 仍落后 old DWS 的实际基线时点，差异会被显式暴露。应先检查 raw/DWD 是否补齐到同一 loaded_at，再判断 ADS gate。

**证据**：
- reports/context_cache/scheduled_dws_v2_shadow_20260512_113845.json#L108
- reports/context_cache/dws_inventory_v2_shadow_20260512_113538.json#L23
- reports/context_cache/dws_inventory_v2_shadow_20260512_113538.json#L39
- reports/context_cache/dws_inventory_v2_shadow_20260512_113538.json#L82
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_post_shadow_same_snapshot_20260512_114348.json#L275
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_post_shadow_same_snapshot_20260512_114348.json#L285
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_post_shadow_same_snapshot_20260512_114348.json#L306

**预防动作**：后续凡是库存类 shadow 验证，先同时核对 old DWS max(etl_time)、shadow DWD source_max_loaded_at 与 old DWS 可比基线结果；若 source_max_loaded_at 落后基线，不要把 mismatch 直接归因到 ADS 或 DWS 聚合口径。

---

### [2026-05-12 11:33] · task · etl-validation

**触发场景**：用户执行新一轮 scheduled_dws_v2_shadow.py 后复核 ads_inventory_health gate，发现 sales 31 天游标已补齐，但 residual mismatch 仍停在 457。

**错误假设**：默认认为 shadow 入口既然已经能跑 inventory v2，就自然具备 old DWS same-snapshot 对齐能力；于是把 post-shadow 的剩余差异直接归因到业务数据本身。

**修正结论**：对需要 old/v2 same-snapshot 判责的链路，若调度入口没有透传 align_with_old_dws 或 source_loaded_at_cutoff，inventory v2 会持续按未对齐快照写出，导致 ADS gate 的 residual mismatch 被 snapshot 差异放大。应先让 shadow 入口显式透传 same-snapshot 参数，再判断 residual mismatch 是否仍来自真实业务差异。

**证据**：
- scheduled_dws_v2_shadow.py#L696
- scheduled_dws_v2_shadow.py#L731
- scheduled_dws_v2_shadow.py#L979
- scheduled_dws_v2_shadow.py#L1062
- reports/context_cache/dws_inventory_v2_shadow_20260512_105244.json#L8
- reports/context_cache/dws_inventory_v2_shadow_20260512_105244.json#L9
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_post_shadow_20260512_1100.json#L272
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_post_shadow_20260512_1100.json#L282
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_post_shadow_20260512_1100.json#L303

**预防动作**：后续凡是用 shadow/旁路入口做 old-vs-v2 下游 gate 验证时，先核对入口是否已完整暴露关键判责参数；若报告里仍出现 align_with_old_dws=false 或 source_loaded_at_cutoff=null，就不能把 residual mismatch 直接当成链路真实差异。

---

### [2026-05-12 10:33] · task · etl-validation

**触发场景**：补一轮 ads_inventory_health 下游输入只读对账时，发现近期 dws_v2_shadow 和旧链最近几天对齐，但 ADS 门仍未闭合

**错误假设**：默认把近期 7 天 shadow 对齐视为足够证据，准备据此推进 S5 或宣称 ODS→DWD→DWS→ADS 已闭环

**修正结论**：若下游 ADS 消费 30 天滚动销售窗口，则验证必须覆盖完整消费窗口；当前 dws_sales_daily_v2 仅覆盖 20260428-20260512，不足以替代 ads_inventory_health 所需的 20260412-20260512，且 inventory old/v2 仍需 same snapshot timepoint 后再判责

**证据**：
- reports/context_cache/dws_v2_ads_inventory_health_input_validation_20260512.md
- etl_ads_health.py#L437-L463
- etl_ads_health.py#L714-L725

**预防动作**：后续 shadow readiness 评估必须拆成两层：近期 DWS 稳定性与 ADS 输入闭环；凡下游消费 30 天/7 天滚动窗口时，先核对 v2 历史覆盖区间，再做最终预插入行集只读对账

---

### [2026-05-12 09:59] · task · tableau-twb

**触发场景**：上一轮已经把顶部 7 张 KPI 卡的字体统一为固定蓝色，但用户重开销售日报后反馈 `去年同期同比` 仍显示橙色，同时确认 `KPI06_目标缺口` 已废弃，需要清理残留元数据。

**错误假设**：默认认为把 Text KPI 的 `customized-label` 字体色改成固定色，并把 `datalabel color-mode` 从 `match` 改成 `automatic`，就足以阻断 Tableau 按趋势方向继续给整张卡着色。

**修正结论**：在 Tableau 的 Text worksheet 中，只要 marks 仍保留 `color` shelf 或 `style-rule element='mark'` 下的颜色 palette 编码，文本卡就可能继续按 mark color 渲染；要实现完全固定配色，必须连同 `<encodings><color ... /></encodings>` 和 `<encoding attr='color' ...>` 一起移除，而且应对所有现用 KPI worksheet 一次性批量执行，不能只修用户当下截图中的单张卡。对于明确停用的 worksheet，除了 dashboard zone，还要同步清理 worksheet、window、thumbnail 等残留元数据。

**证据**：
- 用户反馈：`去年同期同比 卡 颜色不对`
- 用户反馈：`顺手清理 KPI06 的残留 worksheet、window、thumbnail 元数据`
- `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb` 中 `KPI05_去年同期同比` 修复前仍保留 `<color column='...[usr:Calculation_1730010000000651:qk]' />` 与 mark color palette 编码
- 清理后对外部 `.twb` 执行 XML 解析，结果 `XML_OK`

**预防动作**：后续统一 Text KPI 卡配色时，先检查 3 层：`customized-label` 字体色、`encodings` 是否仍挂 `color`、`style-rule element='mark'` 是否仍有 `<encoding attr='color'>`；并对当前所有现用 KPI worksheet 统一扫一遍，不要按截图逐张补。若用户停用某个 KPI worksheet，收口时顺手清理其 worksheet/window/thumbnail 残留。

---

### [2026-05-12 09:53] · user-feedback · tableau-twb

**触发场景**：用户在销售日报 Tableau 客户端确认，7 张 KPI 卡的趋势文案不应显示“较上期”，当前业务展示语义应改成“较昨日”；同时要求 7 张 KPI 卡颜色统一，不再让单张卡随涨跌变成不同强调色。

**错误假设**：默认把基于“上一报告日期”的日报 KPI 趋势文案保留成通用的“较上期/暂无上期”，并允许 Text KPI 卡继续用 mark color 跟随涨跌方向自动染色。

**修正结论**：在 `销售部自动化日报.twb` 的当前日报场景中，KPI 卡面对用户的展示文案应统一成“较昨日/暂无昨日”；7 张现用 KPI 卡的标题、主值、趋势文案应使用统一的固定文本配色，不能让单张卡因为同比/环比方向不同而出现单独的橙色或其它强调色。

**证据**：
- 用户反馈：`不要用 较上期（口径实际是比较昨日对吧） 用 较昨日`
- 用户反馈：`7个 KPI 卡 的颜色需要统一`
- `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb` 中 KPI 趋势文案计算统一基于 `Calculation_1730010000000601/0602` 的最新/上一报告日

**预防动作**：后续改日报类 Tableau KPI 文案时，若趋势比较基于最新报告日与上一报告日，先向用户确认前端语义应显示“较昨日”还是通用“较上期”；若 KPI 卡需要视觉统一，优先关闭 `datalabel color-mode=match` 这类随 mark color 自动染色的逻辑，再用显式字体颜色收口。

---

### [2026-05-12 09:00] · user-feedback · business-rule

**触发场景**：用户明确确认：RT105 昆明顺城购物中心店在 2026-05-08 后闭店，2026-05-09 起由使用新伯俊账号的新店承接同月目标；同时明确 Oracle C_STORE 的 `ISACTIVE='N'` 可视为闭店信号。

**错误假设**：默认认为只要 `dim_store` 主链已按 Oracle 失活状态自动剔除旧店，门店销售专题 ADS 就应静默跳过该店，不再要求业务同步修改月度目标完整快照。

**修正结论**：当前架构下，RT117 / `store_id=748` / 昆明万象城店 是独立新店账号，必须由业务写入月度目标配置表并从 2026-05-09 起生效；RT105 / `store_id=673` 虽会因 Oracle `ISACTIVE='N'` 被 `dim_store` 主链自动剔除，但若 `cfg_store_target_daily` / `dim_store_report_attr` 仍保留 RT105 且未加入 RT117，专题 ADS 应将其视为“目标完整快照未同步”并报错，而不是自动补全。若未来要支持“闭店自动 skip”，需要统一改造多张 ADS 的缺维校验，但这不能替代业务维护目标快照。

**证据**：
- etl_dim_store.py#L47
- etl_ads_store_daily_report.py#L104-L114
- etl_ads_store_daily_report.py#L900-L907
- etl_ads_daily_sales.py#L606
- docs/AGENT_LESSONS.md#L2164-L2168

**预防动作**：后续遇到闭店换账号或新店承接同月目标时，业务必须把月度目标文件当完整快照维护：旧店收口到失活前最后一天，新店从开店日新增；若希望专题 ADS 对已失活门店的 stale 配置改为 `warning + skip`，需单独评估 5 张 ADS 的统一改造范围。

### [2026-05-11 18:15] · user-feedback · etl-architecture

**触发场景**：用户明确指出：月客流要进入 ADS，但必须走 ODS - DWD - DWS - DIM - ADS 完整 API 数据流

**错误假设**：默认沿着月客流最小需求推进，准备直接基于 ODS 日级客流聚月到 ADS，先满足报表层结果。

**修正结论**：对于外部 API 接入，即使当前业务只关心月客流结果，也必须先确认用户是否要求完整数仓分层；一旦用户要求完整链路，ADS 只能消费 DWS，不能绕过 DIM / DWD / DWS 直接从 ODS 聚合。

**证据**：
- 用户反馈：月客流 还要 计算到 这个 ADS 中，但是我要的是 ODS - DWD - DWS - DIM -ADS 完整链路的 API 数据流
- SQL/draft_create_dwd_ovopark_passenger_flow_daily.sql
- etl_dws_ovopark_passenger_flow.py
- etl_ads_ovopark_store_monthly.py

**预防动作**：后续接外部 API 主题时，先在实现前确认目标是单一报表最小闭环，还是完整数仓链路；若用户明确要求完整链路，必须先补齐分层对象与脚本，再谈 ADS 指标交付。

---

### [2026-05-11 18:49] · user-feedback · tableau-twb

**触发场景**：用户在 Tableau 客户端重开销售日报后指出，KPI 趋势文案必须放到文本中才能显示，且 `KPI06_目标缺口` 打开时报 `Calculation_1730010000000017` 不存在

**错误假设**：默认认为只要把 KPI 副文案字段补到 `lod/detail`，`customized-label` 就会稳定显示；同时把 root datasource 中仍存在 `Calculation_1730010000000017` 当成当前 worksheet 也能继续引用其 `[usr:...]` 本地实例的证据

**修正结论**：在 `销售部自动化日报.twb` 这类 KPI 文本卡片里，防止 `<缺少字段!>` 与让副文案真实显示是两件事：前者要求字段进入当前 marks 上下文，后者应优先挂到 Text 编码；另外 root 字段存在不代表本 worksheet 已注册对应 `[usr:...]` 本地实例，`customized-label` 引用未注册实例时，客户端打开仍会报字段不存在

**证据**：
- 用户反馈：趋势文案 要放到 文本中 才能显示
- 用户反馈：`KPI06_目标缺口` 打开时报 `Calculation_1730010000000017` 不存在
- `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb` 中 `KPI06_目标缺口` 的 `customized-label` 曾引用 `[usr:Calculation_1730010000000017:nk]`
- `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb` 中 `KPI07_总日标` 已存在主值与趋势文案双 Text 编码

**预防动作**：后续修 Text KPI worksheet 时，先分别验证“字段已注册进当前 worksheet”与“副文案已真正挂到 Text”；若客户端报某个 `Calculation` 不存在，优先核对当前 worksheet 的 `datasource-dependencies` 是否注册了对应 `[usr:...]` 实例，不要只看 root datasource 定义

---

### [2026-05-11 15:44] · task · external-api-store-mapping

**触发场景**：打通万店掌 `mobileLogin -> getDepartments -> 客流接口` 首轮真实链路后，需要判断“支持第三方门店编码”是否已经能直接用于何方门店编码接入

**错误假设**：把“接口文档写了支持第三方门店编码”直接等同于“当前租户已经配置好了第三方店铺 ID”，从而默认可以跳过内部门店 ID 映射设计

**修正结论**：外部 SaaS 接口层支持第三方编码，不代表租户数据里已经维护了第三方编码实值。本次何方租户 `getDepartments` 全量 64 家门店里，`shopId` 与 `trilateralId` 均为空；因此当前可靠路径仍是内部 `depId` / `S_门店id`，第三方编码只能作为待验证增强路径，不能先写死到 ODS 设计里。

**证据**：
- 万店掌在线调试实测：`open.organize.departments.getDepartments` 在 `pageSize=100` 时返回 `total=64 / rows=64`
- 万店掌在线调试实测：全量门店样本中 `shopId` 非空数为 0、`trilateralId` 非空数为 0
- 万店掌在线调试实测：`open.shopweb.passengerFlow.getPassengerIndicatorData(depId=174679,2026-05-10)` 成功
- 万店掌在线调试实测：`open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData(id=S_174679,timeType=1)` 成功，响应中的 `shopId` 仍为空字符串

**预防动作**：后续对接任何“宣称支持第三方编码”的外部 SaaS 前，先用门店主数据全量样本统计第三方编码字段的非空率；只有在租户级实值存在后，才允许把第三方编码当成主接入键，否则默认保留内部 ID 映射表。

### [2026-05-11 13:21] · task · etl-validation

**触发场景**：排查 2026-05-11 中午门店日报专题 71/70 与 72/71 行数不一致失败时，发现 ads_store_daily_report 的前置校验未覆盖 store_scope 对 dim_store 的 INNER JOIN

**错误假设**：默认把输出行数不一致先归因到共同考核实体压缩或 subject/report_entity_id 冲突，忽略了前置 config_stats 与最终 SQL 的门店覆盖范围并不完全一致

**修正结论**：当 ETL 主 SQL 的基础范围依赖 dim_store、dim_product 等维表 INNER JOIN 时，前置 scope 校验必须使用同样的联接覆盖；否则会把真实的缺维门店问题包装成模糊的输出行数不一致。本次实际缺口是 dim_store 缺少 store_id=673 / RT105 / 昆明顺城购物中心店，而其上游根因是 Oracle C_STORE 已将 RT105 标成 ISACTIVE='N'，中午主链 dim_store 全量刷新后把该门店刷掉。

**证据**：
- etl_ads_store_daily_report.py#L85
- etl_ads_store_daily_report.py#L673
- etl_ads_store_daily_report.py#L857
- logs/store_daily_report_schedule_20260511.log#L34

**预防动作**：后续新增或审计 ETL 前置校验时，逐项对齐最终 SQL 的 base scope；凡是主 SQL 依赖 INNER JOIN 维表的场景，都要在写库前显式输出 missing dimension 计数和门店样例，避免只靠结果行数回推根因

---

### [2026-05-11 12:59] · task · external-api-auth

**触发场景**：实测万店掌 mobileLogin 与 getDepartments 首次联调时，默认把开放平台控制台口令和 ticket 直接复用到业务接口

**错误假设**：把开放平台开发者登录口令、控制台 ticket、应用级 AccessKey 和业务平台 mobileLogin 凭据视为同一套认证域，导致反复在错误的凭据层试调接口

**修正结论**：万店掌至少存在三层凭据域：开发者账号自身的 applicationKey/applicationSecret、具体应用 tableau_bi 的 AccessKey ID/Secret、以及业务平台 mobileLogin 的账号密码；控制台 ticket 不能直接充当 authenticator，控制台登录口令也不能默认等于 mobileLogin 口令

**证据**：
- 控制台内部接口 getDeveloperAppList 返回的 tableau_bi AccessKey 掩码与 userInfo.applicationKey 不同
- cloud.api 实测 open.shopweb.security.mobileLogin(18617002344, hefang.1234) 返回 103095/PASSWORD_ERROE
- cloud.api 实测 open.organize.departments.getDepartments 携带控制台 ticket 作为 authenticator 返回 9990001/TOKEN_NOT_EXIST

**预防动作**：后续对外部 SaaS 做首登联调时，先区分开发者控制台凭据、应用级调用密钥和业务侧登录凭据，再决定哪一层应该用于签名、哪一层应该用于 authenticator

---

### [2026-05-11 12:28] · task · external-api-permission

**触发场景**：在万店掌编辑APP页面为 tableau_bi 扩权时，尝试按数据域整类勾选以扩大探测面

**错误假设**：把数据域分类直接等同于只读数据接口集合，默认整类勾选不会引入额外高风险方法

**修正结论**：万店掌权限分类下可能同时混有 get/report 和 send/delete/save/update 等写接口；扩权时应优先按显式方法名白名单勾选，只在确认整类确实是纯读集合时才考虑分类级勾选

**证据**：
- 控制台编辑APP页实测：追溯类整类勾选后同时包含 open.ovopark.pos.deleteOrder、open.ovopark.pos.sendOrder、open.ovopark.event.sendEventOrder 等接口
- 本轮已将追溯类从整类全选回退为 stockprofitcheckData/stockInOutData/reportSales/searchPos 等只读白名单

**预防动作**：后续在外部 SaaS 平台做权限扩展时，先用小范围试勾验证分类内接口性质；若发现混入写接口，立即回退并改用显式方法名白名单

---

### [2026-05-11 12:04] · user-feedback · external-api-auth

**触发场景**：万店掌公开文档与SDK对 authenticator 来源描述冲突，用户拿到外部技术回复后需要重新收口

**错误假设**：把 SDK 示例中的 open.gateway.authentication 当成当前主链路真值，并继续把 mobileLogin 视为不能作为 authenticator 的初始来源

**修正结论**：以万店掌外部技术回复为准：authenticator 首次从 open.shopweb.security.mobileLogin 获取；公开示例里登录接口也要求先带 authenticator 的写法应视为模板噪音或过期示例；API 权限需在开放平台应用侧手工勾选

**证据**：
- 用户反馈：authenticator 从用户登录接口获取
- 用户反馈：账号不变 token 不变
- 控制台实测：应用 DC-000698 搜索 mobileLogin 返回 0 条，说明当前应用仍缺该权限

**预防动作**：后续遇到外部平台公开文档、SDK 示例与控制台权限状态不一致时，先以外部技术确认和控制台实测为准收口；在写接入方案前先核对当前应用是否已勾选登录接口与基础信息接口

---

### [2026-05-09 14:30] · task · tableau-twb

**触发场景**：对销售部自动化日报.twb 的 ds_ads_store_daily_report_basic 做 calculation 污染盘点并执行第一轮安全去重时，发现当前数据源 55 个 calculation 条目实际只对应 9 个唯一公式语义

**错误假设**：在多轮手工 XML / 试错编译过程中，按 worksheet 维度反复新增同语义 Calculation_ 字段，默认认为只要当前 sheet 能用就可以接受 root 级和 worksheet-local 双层复制

**修正结论**：对当前 HEFANG twb，新增或修补 worksheet 前必须先按公式归一化扫描 datasource 根级 calculation；若已有同语义 root 字段，直接复用已有 name，不得再新增新的 Calculation_...；worksheet-local datasource-dependencies 也不得机械复制 root calculation

**证据**：
- reports/context_cache/ds_ads_store_daily_report_basic_calc_inventory_20260509.md#L5
- reports/context_cache/ds_ads_store_daily_report_basic_calc_inventory_20260509.md#L8
- docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md#L32

**预防动作**：后续编译 twb 默认先跑 calculation inventory：先找 root 级同公式字段、再找零引用垃圾字段；只有在公式、粒度或展示语义确实不同的情况下才允许新增 calculation，且新增后要立即复查是否形成新的重复簇

---

### [2026-05-08 17:08] · task · tableau-twb

**触发场景**：复核 Opus 第二轮 TWB+PNG 联合学习 12 条建议并决定哪些应写入长期 Skill/知识库

**错误假设**：如果把单个样板的视觉细节、候选品牌色或过于刚性的页面骨架直接写成项目长期规则，会把启发式观察误当成硬约束，后续反而降低 Tableau 编译的适配性。

**修正结论**：只有在多份样板和 PNG 联合证据中重复出现、且能稳定转成 XML 编译流程的结论，才应固化到 Skill 与知识库；像品牌色、固定筛选器数量这类未确认或场景依赖强的内容，只能写成推荐骨架或临时策略。

**证据**：
- .github/skills/tableau-twb-compiler-hefang/SKILL.md
- .github/skills/tableau-twb-compiler-hefang/assets/twb_compilation_patterns.md
- docs/Tableau_TWB编译知识库/14份样板学习笔记_20260508.md
- docs/Tableau_TWB编译知识库/视觉效果图联合学习_20260508.md

**预防动作**：后续处理 Tableau 学习沉淀时，先把建议分成硬规则、推荐骨架、待确认设计事实三层，再决定写入位置；未确认品牌事实不得写死到项目长期约束。

---

### [2026-05-08 15:37] · task · tableau-twb

**触发场景**：用户将 14 份 Tableau twb 与同名 PNG 效果图放入 example 目录，要求结合可视化效果图再次学习

**错误假设**：只看 twb XML 会知道 worksheet、dashboard、zone 和 mark 结构，但无法判断页面应采用左侧导航、顶部 hero、卡片网格、暗色漏斗等视觉母版；若 PNG 缺失仍写视觉结论，会把结构证据误当成渲染证据

**修正结论**：先建立 twb 与 png 同名映射，确认 14 份 twb 中只有 10 份有同名 PNG；视觉结论只来自已匹配 PNG，缺图样板仅保留 XML 结构画像。后续编译 twb 时先选视觉母版，再落固定尺寸、粗网格、worksheet 占位和样式增强

**证据**：
- docs/Tableau_TWB编译知识库/视觉效果图联合学习_20260508.md
- reports/context_cache/tableau_twb_visual_corpus_profile_20260508.json
- reports/context_cache/tableau_twb_visual_corpus_summary_20260508.csv
- .github/skills/tableau-twb-compiler-hefang/assets/twb_compilation_patterns.md

**预防动作**：后续所有 Tableau 视觉复刻或学习任务，必须先做 twb/png 映射与缺图清单；没有 PNG 的样板不得写视觉层结论，不能把 XML 可解析等同于 Tableau 视觉已校准

---

### [2026-05-08 15:15] · task · tableau-twb

**触发场景**：用户要求学习14份Tableau twb并沉淀为后续编译twb的Skill

**错误假设**：只把样板twb作为本机会话素材或临时分析结果保存，会导致后续编译Tableau工作簿时无法稳定复用真实样板经验，也容易复制外部样板XML而不是沉淀抽象模式

**修正结论**：应把外部样板拆成结构画像、编译规则、验证清单和项目级Skill；仓库只保存抽象知识与分析产物，不保存14份样板完整XML；后续编译twb前先读取tableau-twb-compiler-hefang Skill和Tableau_TWB编译知识库

**证据**：
- docs/Tableau_TWB编译知识库/README.md
- docs/Tableau_TWB编译知识库/14份样板学习笔记_20260508.md
- .github/skills/tableau-twb-compiler-hefang/SKILL.md
- reports/context_cache/tableau_twb_corpus_profile_20260508.json

**预防动作**：后续凡涉及直接生成、修改或修复Tableau twb，先加载tableau-twb-compiler-hefang Skill，读取知识库与twb_compilation_patterns.md，再备份目标twb、解析XML风格、按清单验证并记录Tableau实测状态

---

### [2026-05-08 10:35] · task · sql-quality

**触发场景**：总控门店销售专题链在 ads_backfill 阶段连续失败，首错为 1052 ambiguous store_id

**错误假设**：为 ads_sales_org_daily 的 scope 统计补 target 门店过滤时，只校验了过滤条件是否存在，没有覆盖 join 后聚合列是否仍显式带表别名。

**修正结论**：凡是给统计 SQL 新增 join 或子查询作用域后，COUNT DISTINCT、SELECT DISTINCT、ORDER BY 等引用重复字段名的位置都必须显式带表别名；单测不仅要断言过滤条件存在，还要锁定关键聚合列的限定写法。

**证据**：
- logs/store_daily_report_schedule_20260508.log#L329
- etl_ads_sales_org_daily.py#L636
- test_ads_sales_scope_alignment.py#L126

**预防动作**：后续销售主题 ADS 调整 scope SQL 时，至少补一条针对关键聚合列别名的断言，避免生产重跑阶段才暴露 ambiguous column。

---

### [2026-05-08 10:14] · task · etl-validation

**触发场景**：评估 ads_sales_org_monthly 是否需要跟随销售主题 ADS 收口时，发现 RT116 在 2026-05-02~2026-05-06 造成 72 vs 71 的范围差，但金额对账仍完全对平。

**错误假设**：只盯 ads_sales_org_daily/ads_sales_org_monthly 的金额是否对平，容易误以为月级组织汇总无需继续修 scope。

**修正结论**：销售主题 sibling ADS 的范围漂移可以在金额上完全隐身：只要缺口门店当期没有销售事实，月级和日级汇总金额仍会对平；因此只要改了 store_scope，就必须同时做门店数/门店清单核验，而不能只看金额级对账。

**证据**：
- etl_ads_sales_org_monthly.py#L128
- etl_ads_sales_org_monthly.py#L574
- etl_ads_sales_org_monthly.py#L1108
- docs/ETL业务逻辑说明.md#L949

**预防动作**：后续凡是销售主题 ADS 做范围收口或评估是否需要收口时，都必须先补一组 active_stores vs target_effective_stores vs diff_store_list 的只读 SQL，再决定是否只修代码、不做金额解释。

---

### [2026-05-08 09:57] · task · business-rule

**触发场景**：排查门店销售专题 71 vs 72 告警时，发现 ads_store_daily_report 已按 report_date 当天目标生效门店收口，但 ads_daily_sales 与 ads_sales_org_daily 仍沿用更宽的有效门店范围。

**错误假设**：默认认为同一专题下多张 ADS 既然共享门店日报目标与商品规则，就会自然共享同一个有效门店 scope；因此把 target_stores != stores 当成单纯告警噪音，而没有继续追到具体门店与 SQL 收口边界。

**修正结论**：凡是销售主题 ADS 共享同一门店日报权威口径时，必须逐张核对 store_scope、scope_stats 和告警条件是否同时收口到 report_date 当天目标已生效门店；只要其中一张仍按 dim_store_report_attr 的宽口径取店，就会在月中新店、未来生效门店或阶段性目标场景下制造范围漂移。

**证据**：
- etl_ads_store_daily_report.py#L670
- etl_ads_daily_sales.py#L67
- etl_ads_daily_sales.py#L496
- etl_ads_sales_org_daily.py#L66
- etl_ads_sales_org_daily.py#L578

**预防动作**：后续只要修改 sales theme ADS 的门店范围、商品范围、共同考核目标优先级或订单正负号规则，必须把同专题兄弟表一起纳入检查，并补一条针对 scope SQL 与 scope_stats 的回归测试，避免再次出现下游 ADS 口径漂移。

---

### [2026-05-08 09:27] · task · etl-architecture

**触发场景**：追查 2026-05-08 00:18 左右 total-control 中 inventory shadow 的 1238 条 old_dws_alignment mismatch 时，需要判断这是口径预期差异还是 v2 逻辑问题。

**错误假设**：默认把 post-refresh 的 ods_fa_storage_raw/DWD source_loaded_at cutoff 当成 old dws_inventory_daily 的可比时点，直接在 shadow 写入步骤里拿 refreshed 数据去对旧链。

**修正结论**：若 old DWS 来自主链 ods_fa_storage，就不能把 refresh 过的 raw/DWD cutoff 直接当旧链基线；应先用主链 ods_fa_storage 对 old dws_inventory_daily 做可比基线，再把 shadow 写入后的校验限定为 DWD->v2 自洽。

**证据**：
- scheduled_dws_v2_shadow.py#L117
- scheduled_dws_v2_shadow.py#L265
- scheduled_dws_v2_shadow.py#L381
- scheduled_dws_v2_shadow.py#L840
- test_scheduled_dws_v2_shadow.py#L14
- docs/ODS-DWD-DWS-ADS架构完善子项目/08_M4_DWS_v2并行表_调度接入与回滚方案.md#L140
- CHANGELOG.md#L11

**预防动作**：后续凡是 snapshot/shadow 类链路需要与旧生产表对账时，先确认旧链真实上游与可比时点；若旧链和 shadow 用的不是同一 source 层，就把 old baseline probe 与 post-refresh self-check 拆开，不要复用同一 cutoff 结论。

---

### [2026-05-07 17:23] · task · business-rule

**触发场景**：排查 2026-05-07 门店日报 ads_backfill 因月中新店缺负责人切片报错时，确认 ERP 已建档门店不等于当月全程都应进入日报口径。

**错误假设**：默认把月中新店一旦写入目标文件就整月展开到 cfg_store_target_daily，并沿用负责人快照当日生效日去覆盖整月，导致预建店或未来生效门店在开店前就被要求具备负责人历史切片。

**修正结论**：门店日报与负责人快照都必须以 cfg_store_target_daily 的当日生效范围为准；业务通过目标模板的 生效开始日/生效结束日 控制月中新店、预建店和阶段性调整从哪一天开始纳入口径，负责人历史仍由 MySQL 内的 SCD2 自动维护，业务只维护目标配置表和负责人映射表。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L76
- tools/import_cfg_store_target_daily_from_nas.py#L582
- tools/import_store_operation_owner_from_nas.py#L256
- etl_ads_store_daily_report.py#L93
- test_import_cfg_store_target_daily_from_nas.py#L102

**预防动作**：后续凡遇到月中新店、预建店、负责人切换或主体归属切换，先确认专题口径是否由目标生效日驱动，再检查负责人历史；不要再用 ERP 建档日或整月默认展开替代业务生效日。

---

### [2026-05-07 15:51] · task · docs-tooling

**触发场景**：为 scheduled_dws_v2_shadow.py 完成文档同步后复扫 doc-sync，发现报告仍保留来自新调度脚本的高风险 code_only 阶段键，且总量被 .conda 噪声放大

**错误假设**：默认认为只要在 ARCHITECTURE/RUNBOOK/数据仓库手册里补齐 shadow 入口、命令和总控第三子链说明，doc-sync 就会自动消化新调度脚本的高风险术语

**修正结论**：check_doc_sync.py 的 code_only 结果会继续保留内部阶段键与环境噪声；新增调度脚本时，除了入口文档，还要单独评估是否为阶段键补专门说明，或先调整审计脚本的噪声过滤/白名单

**证据**：
- reports/docs_code_alignment.json
- scheduled_dws_v2_shadow.py
- docs/RUNBOOK.md

**预防动作**：后续新增调度或工具脚本后，先跑一次 doc-sync 基线并定向筛新增文件的 high-risk code_only；若术语属于内部阶段键，不要只看总 counts，应决定是文档化这些键，还是改审计规则过滤噪声

---

### [2026-05-07 14:49] · task · etl-reconciliation

**触发场景**：为库存 DWS v2 进入 S4 shadow run 固定 old DWS 同时点对账时，需要把 source_loaded_at cutoff 真正落成可执行流程

**错误假设**：以为只要给 etl_dws_inventory_v2.py 增加 source_loaded_at cutoff 参数，就能安全地拿更早时点重跑同一天并与旧 dws_inventory_daily 精确对比，忽略了 upsert 会保留上一次更晚快照才出现的 key

**修正结论**：当库存并行表需要按更早 cutoff 重跑同一个 snapshot_date 时，必须先删除目标表该 date_id 全量切片，再按同一 cutoff 全量重灌；否则 old-v2 对账会混入更晚 source snapshot 的残留 key，导致差异解释失真

**证据**：
- etl_dws_inventory_v2.py#L217
- etl_dws_inventory_v2.py#L323
- etl_dws_inventory_v2.py#L747
- SQL/check_dws_v2_parallel_reconciliation.sql#L265
- SQL/check_dws_v2_parallel_reconciliation.sql#L338

**预防动作**：后续凡是 snapshot 类并行表要支持 earlier cutoff 或 as-of 重跑时，先检查目标端是否需要 delete+reload，而不是默认沿用 upsert；对账 SQL 也要同时区分 old baseline probe、aligned source compare 和 aligned target compare

---

### [2026-05-07 14:00] · task · etl-validation

**触发场景**：DWS v2 S3 实跑验收时，库存 DWD→v2 mismatch 为 0，但 v2→旧 DWS 仍有 200 条同 key qty 差异

**错误假设**：把同一天库存快照当成可以直接与旧 dws_inventory_daily 精确对齐的充分条件

**修正结论**：库存并行验收必须固定同一 source snapshot timepoint；若旧 DWS 的 etl_time 早于 v2 的 source_max_loaded_at，同日同 key 的小幅 qty 差异应先归因为快照时点不同，而不是先判定转换逻辑错误

**证据**：
- reports/context_cache/dws_inventory_v2_s3_acceptance_20260507_1346.json
- dws_inventory_daily 20260507: max(etl_time)=2026-05-07T04:31:36Z
- inventory v2 vs old DWS: diff_rows=200 qty_total_diff=99 qtypurchaserem_total_diff=0

**预防动作**：后续凡是库存或快照类并行验收，必须先记录 source_max_loaded_at 与旧链路 max(etl_time)，再做 v2→旧 DWS 对账；若时点不一致，应先固定快照时点或把差异标记为时间点差异，不要直接判错。

---

### [2026-05-07 10:10] · task · etl-reconciliation

**触发场景**：M3 库存 full raw 初始化后，dwd_inventory_storage_snapshot 与 dws_inventory_daily 的 qty 仍差 337

**错误假设**：默认认为 Oracle full raw 初始化完成后，可直接拿新 DWD 快照与既有生产 DWS 当日快照做精确 qty 对平

**修正结论**：库存 full raw 与 DWS 对账必须确认快照时间点一致；本轮 raw→DWD 行数和 qtypurchaserem 自洽，qty 差异来自生产 ods_fa_storage/dws_inventory_daily 快照时间早于本次 Oracle full raw 初始化，不是 raw→DWD 转换错误

**证据**：
- reports/context_cache/m3_raw_full_sales_inventory_load_20260507.json
- docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md

**预防动作**：后续库存 DWD / DWS 对账先固定同一 source snapshot timepoint；若只验证 raw→DWD，优先检查 raw_rows、dwd_rows、duplicate_key_count、missing_sku_rows 和关键数量字段，再把与生产 DWS 的时间漂移单独标注

---

### [2026-05-07 09:24] · user-feedback · field-mapping

**触发场景**：用户说明 5 月仅用 v1，门店等级由 NAS 目标配置表新增列维护，负责人空值正常

**错误假设**：默认把 store_grade 缺失归因于 ads_store_daily_report 下游 ETL 未透传，或把 owner_name 为空视为数据异常

**修正结论**：ads_store_daily_report 已直接透传 dim_store_report_attr.store_grade，真实缺口在 tools/import_cfg_store_target_daily_from_nas.py 未解析 Excel 等级列；owner_name 为空可表示当前未分配负责人，不应阻断日报链路

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L505
- tools/import_cfg_store_target_daily_from_nas.py#L558
- tools/import_cfg_store_target_daily_from_nas.py#L1687
- etl_ads_store_daily_report.py#L175
- etl_ads_store_daily_report.py#L197
- test_import_cfg_store_target_daily_from_nas.py#L76

**预防动作**：排查日报字段空值时先区分下游透传缺失还是上游导入未写入；对负责人类可空字段先确认业务语义，再决定是否按异常处理

---

### [2026-05-06 10:32] · user-feedback · etl-architecture

**触发场景**：用户明确要求门店属性录入面不再直接维护SCD2历史，而改为业务只维护当前快照

**错误假设**：继续把 dim_store_report_attr 历史表当业务录入面，并把上一版历史仍有效一概视作不可覆盖

**修正结论**：应拆出 cfg_store_report_attr_snapshot 作为独立快照录入面，再由 ETL 自动承接 dim_store_report_attr 历史；只有真实同店多条当前有效记录时才失败

**证据**：
- logs/store_daily_report_schedule_20260506.log
- tools/import_cfg_store_target_daily_from_nas.py
- tools/import_store_operation_owner_from_nas.py

**预防动作**：后续遇到业务只维护当前真值的配置表时，先拆分快照输入表和历史消费表，不让业务直接维护SCD2区间；历史表只给下游按日期回看。

---

### [2026-05-06 09:26] · user-feedback · business-rule

**触发场景**：查看 20260502-20260506 总控日志时，连续发现门店销售专题在 import 阶段因配置表门店未命中 dim_store 而整链 FAILED；用户说明这些门店可能尚未在 Oracle/ERP 建店。

**错误假设**：默认把配置表中未在 Oracle/ERP 建店的门店一律视作专题调度致命失败，导致单个坏门店阻断整月其余门店的目标导入、负责人导入和六层 ADS 重跑。

**修正结论**：若仅部分门店未命中 dim_store，应把专题链改为 WARNING，企微和总控摘要列出未命中门店并跳过这些门店相关配置，继续执行其余门店；只有当全部门店都未命中，或共同考核归属会被整体清空时，才允许立即失败以避免空覆盖当月配置。

**证据**：
- logs/scheduled_total_control_20260502.log:门店销售专题 FAILED，原因=长沙运达汇店未命中 dim_store
- logs/scheduled_total_control_20260503.log:同一文件 MD5 连续失败，原因一致
- tools/import_cfg_store_target_daily_from_nas.py:未命中门店现改为 warning+skip，并保留全量未命中的安全失败阀
- scheduled_store_daily_report.py:专题调度与总控摘要现输出 WARNING 而非 FAILED

**预防动作**：后续排查专题调度失败时，先区分是部分门店未命中还是整月配置整体失效；凡是由配置表主数据缺口引起的局部问题，优先设计 warning+skip 和安全阀，不要直接让单个坏门店阻断其余门店链路。

---

### [2026-04-30 17:30] · task · etl-reconciliation

**触发场景**：M3 raw/DWD 近 1 天小窗口真实装载后执行最小对账时，DWD 行数与 raw 一致但与现有 DWS 完整日/完整快照汇总差异明显

**错误假设**：把 modified-window 小窗口装载结果直接当成完整业务日销售或完整库存快照，与 DWS 做全量口径对账

**修正结论**：raw→DWD 一致性检查和 DWD→DWS 完整口径对账必须分层判断；小窗口 modified/settime 子集只能证明旁路链路可写与主键/行数一致，不能证明完整日级或完整快照口径已对齐

**证据**：
- reports/context_cache/m3_raw_dwd_small_window_load_20260430.json
- etl_ods_fa_storage_raw.py
- etl_dwd_sales_retail_item.py
- etl_dwd_inventory_storage_snapshot.py

**预防动作**：后续 M3 验证先明确窗口类型：若要对齐 dws_sales_daily，需补完整业务日期 raw 或按受影响单据集合对账；若要对齐 dws_inventory_daily，需先设计 FA_STORAGE full raw/全量快照初始化，并显式选择 long_running 等 timeout_profile。

---

### [2026-04-30 14:45] · task · schema-verification

**触发场景**：用户反馈 M3 的 5 个 raw ODS / DWD DDL 已人工建表后，需要把此前“未执行 / 未建表”的草案状态改为当前真实状态

**错误假设**：容易把用户已执行 DDL 直接等同于生产链路已生效，或只改文档措辞而不先核验目标库结构、行数与剔除字段是否残留

**修正结论**：用户人工建表后必须先用 MySQL 元数据和行数做只读核验，再把状态精确写成“已建空表 / 未装载 / 未接调度 / DWS 与 ADS 不消费”；表已存在不代表 ETL 已写库、契约已生效或调度已接入

**证据**：
- reports/context_cache/m3_manual_ddl_verification_20260430.json
- SQL/draft_create_ods_m_retail_raw.sql
- SQL/draft_create_ods_m_retailitem_raw.sql
- SQL/draft_create_ods_fa_storage_raw.sql
- SQL/draft_create_dwd_sales_retail_item.sql
- SQL/draft_create_dwd_inventory_storage_snapshot.sql
- docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md

**预防动作**：后续凡是用户人工执行 DDL、索引或结构调整后，先落 `information_schema` / 行数 / 字段残留核验证据，再同步 DDL 头部、数据字典、数据契约和交接；若线上 COMMENT 仍含旧状态，只提供 ALTER 草案并由用户人工执行

---

### [2026-04-30 13:57] · user-feedback · field-selection

**触发场景**：M3 raw/DWD 草案字段筛选时，用户明确指出全量为 0 的 Oracle 模板字段不应进入新架构

**错误假设**：仅按字段存在或非空覆盖率判断字段可用，容易把 Oracle ERP 模板化冗余字段带入 raw ODS 与 DWD 草案

**修正结论**：新架构字段入选必须同时满足真实有业务值、语义明确和架构价值；全量为 0 或全量为空的模板字段应在 DDL、ETL 骨架和对账 SQL 中剔除，只在证据或剔除记录中保留

**证据**：
- reports/context_cache/oracle_field_usage_m3_zero_filter_20260430.json
- SQL/draft_create_ods_m_retailitem_raw.sql
- SQL/draft_create_ods_fa_storage_raw.sql
- SQL/draft_create_dwd_sales_retail_item.sql
- SQL/draft_create_dwd_inventory_storage_snapshot.sql
- docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md

**预防动作**：后续冻结 raw ODS 或 DWD 字段白名单时，先做全量空值/零值/非零覆盖扫描，再结合字段语义和业务价值分为 INCLUDE/DEFER/EXCLUDE；不得把源库模板字段原样克隆到新架构

---

### [2026-04-30 10:23] · task · field-semantics

**触发场景**：M3 raw/DWD DDL 草案中大量字段注释写作语义待确认，用户提供 AD_COLUMN 零售单字典和 FA_STORAGE 开发平台截图要求先对齐字段语义

**错误假设**：看到源字段非空或字段名相似时，容易直接把字段扩写为业务含义，或继续泛化写语义待确认，导致后续人工复核无法区分已证实语义和证据缺口

**修正结论**：M_RETAIL/M_RETAILITEM 字段语义以 ERP AD_COLUMN 字典显示名为优先证据；FA_STORAGE 字段以用户提供开发平台截图中可见显示名为证据；未命中字典或截图的字段只能写源字段原值、覆盖率和未命中说明，不扩写为已确认业务语义

**证据**：
- data/AD_COLUMN04301009.xlsx
- reports/context_cache/ad_column_retail_raw_semantics_20260430.csv
- docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md
- SQL/draft_create_ods_m_retail_raw.sql
- SQL/draft_create_ods_m_retailitem_raw.sql
- SQL/draft_create_ods_fa_storage_raw.sql

**预防动作**：后续做 ERP 源字段语义对齐时，先抽取官方字典或开发平台显示名形成 evidence cache；DDL 注释中明确区分已命中字典、截图可见和未命中补证三类字段，再进入建表或写 ETL

---

### [2026-04-29 17:29] · task · doc-sync

**触发场景**：输出 raw ODS / DWD 草案对象后，首次 doc-sync 复扫将 raw 表名识别为 code_only high

**错误假设**：只在 ODS-DWD-DWS-ADS 子项目 M3 文档中说明草案对象，就足以让仓库级文档同步和后续接棒读者理解这些表不是现网对象

**修正结论**：新增 raw ODS / DWD 草案对象时，除子项目滚动文档外，也必须在根级 ETL 逻辑说明、MYSQL 数据字典和 DATA_CONTRACTS 中标注 draft-only / 未执行 / 未落库 / 未接调度；否则 doc-sync 和后续读者容易把 raw 表名当成未同步的生产对象

**证据**：
- reports/docs_code_alignment.json: 2026-04-29 17:19 raw 表名为 code_only high，17:23 复扫后新增 raw/DWD 术语 not listed
- docs/ETL业务逻辑说明.md
- docs/MYSQL数据字典.md
- docs/DATA_CONTRACTS.md

**预防动作**：后续新增草案表、草案脚本或对账 SQL 时，先同步子项目文档，再在根级事实文档补充草案状态；复跑 scripts/check_doc_sync.py 后对本轮新增对象做定点检索确认。

---

### [2026-04-29 16:35] · task · etl-architecture

**触发场景**：输出 DWD DDL 草案与旁路 ETL 骨架时，尚未有 raw ODS 表、DWD 表和用户写库授权

**错误假设**：把骨架脚本写成可直接 INSERT/DELETE 的执行脚本，容易被误运行并写入尚未复核的 DWD 草案表

**修正结论**：草案阶段的旁路 ETL 骨架应默认只输出候选 SQL 或做只读 conn-test；即使保留 --execute 开关，也应显式拒绝写库，等用户确认 raw ODS DDL、DWD DDL、对账 SQL 和超时边界后再补真实写入实现

**证据**：
- etl_dwd_sales_retail_item.py
- etl_dwd_inventory_storage_snapshot.py
- docs/ODS-DWD-DWS-ADS架构完善子项目/07_M3_ODS字段白名单与DWD_DDL_ETL骨架.md

**预防动作**：后续所有未接主链的 DWD/ODS 旁路骨架，先采用 dry-run/conn-test-only 模式；未获用户授权前不提供可直接写生产库的默认执行路径

---

### [2026-04-29 16:02] · task · source-profiling

**触发场景**：探索 Oracle BOSNDS3 字段启用率并规划 ODS/DWD 时，需要判断哪些字段实际启用、哪些可能废弃

**错误假设**：容易把单次字段画像中的空值率直接解释成废字段，或把当前 ODS 已抽取字段误认为长期最优源事实范围

**修正结论**：字段画像只能证明统计窗口内非空覆盖情况；业务废弃必须结合多窗口/全量画像、代码引用、文档和用户确认。ODS 长期应作为源事实可追溯落地层，而不是只服务当前 DWS/ADS 的窄字段 staging

**证据**：
- docs/ODS-DWD-DWS-ADS架构完善子项目/06_M2_5_ORACLE源库画像与ODS_DWD规划.md
- reports/oracle_bosnds3_core_field_profile_202604.json

**预防动作**：后续做源库字段画像时，统一把字段分为当前链路已用、源侧有数据但未入库、统计窗口空、疑似废弃待确认四类；未经过用户确认前不要输出确定废字段清单

---

### [2026-04-29 15:46] · task · sales-theme-order-count

**触发场景**：审计 4 张销售专题 ADS 后继续落改时，发现‘统一到门店日报口径’在不同粒度下有两种实现方式

**错误假设**：默认把所有下游专题表都理解成应该直接承接 ads_store_daily_report 的订单数字段，忽略了月组织汇总和 SKU 汇总的粒度差异

**修正结论**：当下游表与门店日报同粒度或可安全上卷时，才直接承接 ads_store_daily_report 的订单数事实；若下游表处于更细粒度如 SKU，则应继承门店日报的判单规则和近零容差，而不是直接搬运门店层字段

**证据**：
- etl_ads_sales_org_monthly.py#L302
- etl_ads_sales_org_monthly.py#L313
- etl_ads_sku_daily.py#L171
- etl_ads_sku_daily.py#L190
- docs/DATA_CONTRACTS.md#L674
- docs/DATA_CONTRACTS.md#L761

**预防动作**：后续凡是用户要求‘承接上游权威口径’，必须先判断是直接承接字段、按当前粒度重算但继承规则，还是两者并存；在实现前把依赖类型写进测试和契约文档，再落代码。

---

### [2026-04-29 15:41] · task · etl-architecture

**触发场景**：复核 ODS-DWD-DWS-ADS 架构完善 M2 草案时，用户要求从首席数据官和数据架构师视角选择长期最优 DWD 边界

**错误假设**：把第一批 DWD 误收窄为当前 DWS/ADS 的核算事实或库存健康专用中间层，会让未来业务销售底表、全店仓库存分析继续绕回 ODS/Excel，削弱 DWD 的公共事实层价值

**修正结论**：销售 DWD 应定位为零售明细原子事实 + 关键业务上下文，而不是窄核算表；库存 DWD 应定位为全店仓库存快照事实层，库存健康只是第一批消费和验证场景

**证据**：
- docs/ODS-DWD-DWS-ADS架构完善子项目/05_M2第一批DWD主题设计冻结草案.md#L19-L20
- docs/ODS-DWD-DWS-ADS架构完善子项目/05_M2第一批DWD主题设计冻结草案.md#L124-L126
- docs/ODS-DWD-DWS-ADS架构完善子项目/05_M2第一批DWD主题设计冻结草案.md#L187-L189

**预防动作**：后续设计 DWD 时先判断其是否为跨主题可复用原子事实层；不得只按当前下游 ADS 的过滤范围或核算字段裁剪 DWD，字段级落地前再做源表血缘和宽表/维表归属拆分

---

### [2026-04-29 14:54] · task · business-rule

**触发场景**：排查餐具补配后门店日报专题单数仍有 1 / 2 差异时，发现杭州嘉里与广州天汇的根因不同

**错误假设**：默认把 ads_store_daily_report 的净单符号继续绑定在零售单主表单头 tot_amt_actual 上，同时默认业务 Excel/底表里的净零单会稳定等于 0

**修正结论**：门店日报订单数若基于过滤后的商品范围统计，单号正负应按过滤后明细汇总金额判断，而不是按整单单头金额判断；杭州嘉里 retail_id=6754010 因纳入口径内金额 -197.87、口径外辅料 +198.87，单头为 +1.00 但过滤后应记 -1。另一方面，业务侧 4月截止28日原始数据.xlsx 会把净零单 RT046P12604281600060004 汇总成 -2.2737367544323206e-13，导致广州天汇 Excel 上游把该单按 -1 计入实际单数，不能把业务 Excel 机械视为精确真值

**证据**：
- etl_ads_store_daily_report.py#L267
- etl_ads_store_daily_report.py#L295
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql#L158
- data/4月截止28日原始数据.xlsx:RT046P12604281600060004
- data/4月28日生意额及指标.xlsx:4月28日!B9

**预防动作**：后续凡是订单数建立在过滤后商品集合上时，都要先比较单头金额符号与过滤后金额符号是否可能分叉；对业务 Excel 或导出底表里的净零单，要先做四舍五入或 epsilon 容差判断，再决定记 0 还是记 -1 / 1。

---

### [2026-04-29 13:29] · user-feedback · doc-sync

**触发场景**：修订 ODS-DWD-DWS-ADS 子项目根文档来源描述时，用户指出当前尚未构建 DWD 层，DWS/ADS 数据来源不能被写成 DWD 来源

**错误假设**：把'修订旧的 DWS/ADS 来源描述'容易表述成来源将切到 DWD，未先强调这是按当前代码事实校准，而不是改变现有链路

**修正结论**：文档同步只能校准当前已实现事实：DWD 层仍未实现；当前 DWS 从 MySQL ODS 消费；ADS 来源按专题区分，不能把规划中的 DWD 写成现状来源

**证据**：
- run_etl.py#L631-L652
- etl_dws_sales.py#L39-L65
- etl_dws_inventory.py#L39-L59
- etl_ads_sales_org_monthly.py#L240-L254
- docs/ETL业务逻辑说明.md#L1224-L1230

**预防动作**：后续涉及分层架构文档同步时，先区分三件事：当前代码事实、历史旧描述、未来 DWD 规划；所有未实现 DWD 对象必须标注未实现/待确认，禁止写成当前来源

---

### [2026-04-29 13:10] · task · business-rule

**触发场景**：门店日报专题对账发现餐具导致 MTD 金额数量差异，用户确认门店日报专题所有 ADS 应纳入餐具；业务 Excel 实际单数按单号计数，销售金额负数订单按 -1 处理

**错误假设**：先把差异主要归因于时间窗口或数据晚到，没有优先检查 dim_report_product_rule 是否漏配近期新增类目；在业务已说明实际单数按单号净计数后，仍反复怀疑 ADS 净单公式本身错误

**修正结论**：门店日报专题金额/数量差异若集中落在少数门店且金额与数量都一起偏小，先核对 dim_report_product_rule 当前有效配置。category_id=459 餐具当前缺少有效规则，会同时影响 ads_store_daily_report 及统一复用其商品范围口径的 ads_store_daily_subject_report、ads_daily_sales、ads_sku_daily、ads_sales_org_daily、ads_sales_org_monthly。订单数口径仍按单号去重后，销售金额 >0 记 1、<0 记 -1、=0 记 0，与当前 ads_store_daily_report 净单逻辑一致

**证据**：
- etl_ads_store_daily_report.py#L204
- docs/MYSQL数据字典.md#L542
- SQL/alter_dim_report_product_rule_include_459_tableware.sql#L1
- reports/reconcile_store_daily_ads_2026-04-28.json#L1

**预防动作**：后续凡是门店日报专题出现新增类目商品，先查 dim_report_product_rule 是否已有当前有效记录，再判断是否需要人工补配置和重跑受影响日期；不要把商品范围漏配与时间窗口问题混为一谈

---

### [2026-04-29 10:43] · task · agent-tooling

**触发场景**：运行 Agent 上下文优化烟测脚本时，报告 JSON 中的子进程中文 stdout 出现乱码替换字符

**错误假设**：只在 subprocess.run 中指定 text=True 和 encoding='utf-8'，但未控制子进程自身 stdout 编码；Windows 下子进程仍可能按本地代码页输出中文，导致 UTF-8 捕获后生成乱码证据

**修正结论**：在运行内部 Python 子进程前设置 env['PYTHONIOENCODING']='utf-8'，并统一把报告中的仓库相对路径转为 POSIX 风格，保证烟测证据可读、可引用

**证据**：
- scripts/smoke_agent_context_optimization.py#L57-L70
- scripts/smoke_agent_context_optimization.py#L92-L102
- reports/agent_context_optimization_smoke.json

**预防动作**：后续编写会捕获 Python 子进程输出的 Windows 工具脚本时，默认注入 PYTHONIOENCODING=utf-8；涉及报告路径时使用 as_posix() 或 replace('\\\\','/') 统一展示

---

### [2026-04-29 10:31] · task · agent-customization

**触发场景**：复查上下文压缩改造时，尝试将 .github/instructions/*.instructions.md 的 applyTo 改成 YAML 数组

**错误假设**：只参考通用示例认为 applyTo 数组更稳定，未先以当前 VS Code 诊断校验；结果当前诊断明确要求 applyTo 必须是字符串，数组写法会导致 instruction 文件报错。

**修正结论**：本仓库当前 VS Code 诊断下，applyTo 保持字符串，并用逗号分隔多个 glob，例如 docs/**/*.md, README.md, AGENTS.md。修改 file instructions frontmatter 后必须跑 Problems/诊断检查。

**证据**：
- .github/instructions/sql.instructions.md#L4
- .github/instructions/docs.instructions.md#L4
- .github/instructions/python-etl.instructions.md#L4

**预防动作**：后续改 VS Code Copilot customization frontmatter 时，先用 get_errors 验证 schema；若文档示例与本机诊断冲突，以当前 VS Code 诊断为准。

---

### [2026-04-29 09:48] · user-feedback · agent-context

**触发场景**：讨论上下文压缩方向时，提出对 Oracle/MySQL 查询输出强制限宽限行

**错误假设**：默认把查询结果强制限宽限行作为上下文优化手段，可能让 Agent 无法获取完成任务所需的完整数据

**修正结论**：上下文优化不能牺牲必要数据完整性；需要完整 Oracle/MySQL 查询结果时应允许完整获取，并优先落盘到 reports/ 或 reports/context_cache/，对话中只摘要结论和证据路径

**证据**：
- .github/copilot-instructions.md#L51-L55
- .github/instructions/sql.instructions.md#L11-L12
- docs/数云数据同步-子项目资料/superpowers内化会议纪要.md#L112-L114

**预防动作**：后续设计 DB 查询上下文压缩时，不使用默认强制 LIMIT/限列替代证据获取；改用落盘、索引、摘要和定向读取

---

### [2026-04-28 15:47] · task · mcp/path

**触发场景**：用 DBHub 执行 SQL/check_ads_sales_org_monthly_min.sql 时，当前月勾稽先报 Illegal mix of collations，随后第一段又在 ONLY_FULL_GROUP_BY 下报聚合不合法

**错误假设**：默认沿用普通字符串用户变量和裸聚合写法，认为 MySQL 最小对账 SQL 在 DBHub 会话与当前 sql_mode 下也能直接稳定执行

**修正结论**：在当前仓库的 DBHub MySQL 会话里，字符串用户变量和关键字字面量要显式设成 utf8mb4_0900_ai_ci；同时一行 params CTE 的字段与聚合混用时必须用 MAX/MIN 包裹，才能让校验 SQL 稳定通过

**证据**：
- SQL/check_ads_sales_org_monthly_min.sql#L2
- SQL/check_ads_sales_org_monthly_min.sql#L4
- SQL/check_ads_sales_org_monthly_min.sql#L29
- SQL/check_ads_sales_org_monthly_min.sql#L466

**预防动作**：后续新增或重写 MySQL 最小对账 SQL 时，先按 DBHub 会话与 ONLY_FULL_GROUP_BY 环境检查字符串比较的 collation 和聚合合法性，不要等到库内执行时报错再补

---

### [2026-04-28 12:51] · task · performance-and-business-rule

**触发场景**：核对门店销售专题 2026-04-27 / v2 的直营差额与耗时时，发现 ads_sales_org_daily 已对齐，但 ads_sales_org_monthly 仍然失真且最耗时

**错误假设**：一度把专题链路的差额是否消除视为整体已收口，并默认 ads_sales_org_monthly 只是慢、未必还错；同时默认按 dws_sales_daily 做月汇总可近似复用门店日报口径

**修正结论**：ads_sales_org_monthly 当前同时存在正确性和性能共因问题：它按物理门店月目标直接汇总，未走共同考核主体覆盖，导致直营当前月目标多算 700000；它按 dws_sales_daily 聚合且未应用日报商品纳入口径，导致直营当前月实销多算 979537.74。即使补上商品纳入口径，dws_sales_daily 相比 ODS 明细口径仍残留 11620.22 差额，因此该月汇总不能继续作为门店日报专题的权威事实来源。性能上，该脚本对每个 report_date 都递归重算当年 1-当前月以及去年同期月序列，7 天批跑时会重复执行几乎相同的年内汇总，单脚本占总耗时约 58% 左右。

**证据**：
- etl_ads_sales_org_monthly.py:120-241
- logs/store_daily_report_schedule_20260428.log:157-259
- docs/AGENT_HANDOFF.md:312-332

**预防动作**：后续凡是给门店日报专题或销售专题补月汇总，先检查是否与 ads_store_daily_report / ODS 明细同源，显式核对共同考核主体覆盖、商品纳入口径和事实层来源；若需要按多天批量回刷，优先避免对每个 report_date 反复重算同一年度月序列。

---

### [2026-04-28 10:07] · task · etl-operations

**触发场景**：连接工厂 60 秒超时导致长跑组织汇总在总控中不稳定，需要兼顾稳定性与默认性能

**错误假设**：把所有 MySQL 直连统一绑死在 60 秒超时，或为救长 SQL 直接放弃连接工厂架构

**修正结论**：保留 db_connections.py 统一连接工厂，增加 default/etl/long_running 三档超时；仅对 ads_sales_org_daily 与 ads_sales_org_monthly 这类长事务调用方显式切 long_running，并在失败清理阶段保护 rollback/close 不覆盖首错

**证据**：
- db_connections.py#L47
- db_connections.py#L85
- db_connections.py#L105
- etl_ads_sales_org_daily.py#L499
- etl_ads_sales_org_daily.py#L747
- etl_ads_sales_org_monthly.py#L329
- etl_ads_sales_org_monthly.py#L610

**预防动作**：后续新增 ETL 先按历史耗时选择 timeout_profile；默认保持短超时，只给长事务显式升档，并在 except 清理中用 safe rollback/close 保留原始异常日志

---

### [2026-04-28 09:43] · task · etl-operations

**触发场景**：总控在 ads_sales_org_daily 阶段报首个错误 (0, '')，日志只剩命名锁释放失败

**错误假设**：把空异常直接当作连接工厂整体不可用或业务 SQL 无提示失败，忽略 ads_sales_org_daily 历史耗时长期超过 120 秒

**修正结论**：若 ads_sales_org_daily 每次都在约 60 秒失败，且 db_connections.py 为 PyMySQL 统一设置 read_timeout=60、write_timeout=60，应优先判定为连接工厂超时截断长 SQL；随后 rollback 或 close 在断开的连接上再次抛出 (0, '')，会遮蔽原始异常并跳过 logger.error

**证据**：
- db_connections.py#L33
- db_connections.py#L34
- db_connections.py#L68-L69
- etl_ads_sales_org_daily.py#L817-L831
- etl_ads_sales_org_daily.py#L857-L877
- logs/store_daily_report_schedule_20260428.log#L69-L79
- logs/store_daily_report_schedule_20260427.log#L92-L94

**预防动作**：对长跑 ADS 或回填任务不要盲目复用 60 秒 PyMySQL 读写超时；至少按模块覆盖更长 timeout，并把 except 清理中的 rollback 和 close 包裹二次保护，先保留原始异常再记录清理失败

---

### [2026-04-28 09:18] · task · business-rule

**触发场景**：ads_daily_sales 写入 SQL 执行时报 MySQL 1052 Column 'sales_date' in field list is ambiguous

**错误假设**：只在 JOIN ON 中限定 sales_date 即可，SELECT 和 GROUP BY 可以继续使用未限定列名

**修正结论**：当 CTE 同时 LEFT JOIN 多个也含 sales_date 的目标表时，SELECT 与 GROUP BY 中也必须使用主来源别名限定，例如 edb.sales_date、edb.report_entity_id、edb.area_name、edb.report_channel_type

**证据**：
- etl_ads_daily_sales.py:171-184 entity_target_daily 已改为 edb.* 限定字段
- logs/store_daily_report_schedule_20260428.log:42 报错为 MySQL 1052 sales_date 字段歧义

**预防动作**：后续修改多 CTE 聚合 SQL 时，凡字段名在左右表重复，SELECT、GROUP BY、ORDER BY、窗口排序和 JOIN 条件都要统一加来源别名；修后用 EXPLAIN 解析目标 INSERT

---

### [2026-04-28 08:58] · task · business-rule

**触发场景**：总控销售专题 ads_daily_sales 因 SQL 骨架缺少 sra.is_include_in_daily_report 条件失败

**错误假设**：看到骨架自检报错时只调整 REQUIRED_SQL_SNIPPETS 或放宽自检即可

**修正结论**：应先核对主 INSERT SQL 是否真实缺失权威口径过滤；本次 store_scope 确实漏了 sra.is_include_in_daily_report = 'Y'，需要补回 SQL，而不是削弱自检

**证据**：
- etl_ads_daily_sales.py:79-84 主 INSERT 的 store_scope 已补回门店日报纳入过滤
- logs/store_daily_report_schedule_20260428.log:14 报错指向 SQL 骨架缺少该关键片段

**预防动作**：后续遇到 SQL 骨架校验失败，先按 REQUIRED_SQL_SNIPPETS 回查主 SQL 血缘和权威口径，只有确认等价逻辑存在时才调整自检片段

---

### [2026-04-27 18:12] · task · path

**触发场景**：总控调度步骤5因 check_ods_incremental 执行失败返回码1

**错误假设**：在 ods_m_retail 和 ods_m_retailitem 已存在单列唯一索引 id 的情况下，质检脚本仍对整张 ODS 表做 GROUP BY id 查重，导致百万级数据下易命中 MySQL 读超时。

**修正结论**：ODS 重复ID校验必须先读 information_schema.statistics 判断是否已有单列唯一索引；若唯一索引已覆盖 id，duplicate_id_count 直接视为 0；只有缺索引时才回退到按当前增量窗口查重。

**证据**：
- tools/check_ods_incremental.py#L45; tools/check_ods_incremental.py#L73; tools/check_ods_incremental.py#L186; tools/check_ods_incremental.py#L224; SQL/create_ods_tables.sql#L30; SQL/create_ods_tables.sql#L48

**预防动作**：后续修改 ODS 质检工具前，先核对现网唯一键与索引策略，避免对大表执行与约束重复的全表 GROUP BY 校验。

---

### [2026-04-27 17:44] · task · business-rule

**触发场景**：销售主题 ADS 已统一到门店日报权威口径，但最小对账 SQL 仍沿用旧版 dws/门店目标逻辑。

**错误假设**：只同步展示文档和契约文档，未同步审计 SQL/check_ads_* 最小对账脚本是否仍与当前 ETL 同源。

**修正结论**：凡是销售主题 ADS 发生范围、主体目标优先级、商品规则或订单语义调整时，必须同时复查 SQL/check_ads_sales_org_daily_min.sql、SQL/check_ads_daily_sales_min.sql、SQL/check_ads_sku_daily_min.sql，并确保 SQL开发手册 的说明同步更新。

**证据**：
- etl_ads_sales_org_daily.py#L96
- etl_ads_sales_org_daily.py#L274
- etl_ads_daily_sales.py#L118
- etl_ads_daily_sales.py#L302
- etl_ads_sku_daily.py#L99
- etl_ads_sku_daily.py#L194
- SQL/check_ads_sales_org_daily_min.sql#L1
- SQL/check_ads_daily_sales_min.sql#L1
- SQL/check_ads_sku_daily_min.sql#L1

**预防动作**：以后凡遇到销售主题 ADS 口径重构，收口清单必须显式包含 核对 SQL、SQL开发手册、DATA_CONTRACTS、ETL业务逻辑说明 四项同批复扫。

---

### [2026-04-27 17:20] · task · business-rule

**触发场景**：销售主题ADS统一到门店日报权威口径后继续引用旧版v1/v2验证

**错误假设**：把旧逻辑形成的最小对账和写库记录继续当作新逻辑已验证证据

**修正结论**：只要门店范围、商品范围、共同考核目标或订单正负号规则变更，历史验证记录就只能视为旧逻辑记录；文档必须显式标注不覆盖新逻辑，并补做新口径验证

**证据**：
- etl_ads_sales_org_daily.py#L125-L352
- etl_ads_daily_sales.py#L118-L313
- etl_ads_sku_daily.py#L111-L417
- docs/AGENT_HANDOFF_archive.md#L460

**预防动作**：后续凡遇到销售主题ADS口径统一或字段语义调整，必须同步更新README、DATA_CONTRACTS和映射文档，并在结束前补充新口径最小对账，或明确写入待验证交接项。

---

### [2026-04-27 16:16] · task · validation

**触发场景**：统一 hefang_dw 连接工厂后执行项目级 compileall 语法检查

**错误假设**：直接在仓库根目录运行 python -m compileall -q . 会把本地 .conda 目录也纳入检查，可能因为解释器版本差异报出与项目代码无关的语法错误

**修正结论**：项目级语法检查应显式排除 .conda、.venv、example_repos、logs 等非项目源码目录；本轮排除后 115 个项目 Python 文件编译通过

**证据**：
- terminal: D:/Anaconda/envs/pyproject/python.exe -m compileall -q . 报 .conda/Lib/annotationlib.py#L327 SyntaxError
- terminal: 排除 .conda/.venv/example_repos/logs 后 checked=115

**预防动作**：后续在 hefang_dw 做全量语法检查时优先使用带 skip_dirs 的 compileall 脚本，避免把本地环境目录误判为项目代码问题

---

### [2026-04-27 15:31] · task · business-rule

**触发场景**：排查负责人拆解与页级总盘直营差额

**错误假设**：默认把 ads_sales_org_daily 与 ads_store_daily_report 的直营差额先归因为漏门店或漏负责人。

**修正结论**：700000 月目标差额全部落在 SUBJ_SZ_WXTD 深圳万象天地经营体，因 ads_store_daily_report 以 subject_month_target 覆盖两家源门店各 700000 的店级目标；939620.96 月销差额主要分散在 15 个直营网点/经营体，核心来自 ads_store_daily_report 按 dim_report_product_rule 过滤日报商品，而 ads_sales_org_daily 直接汇总 dws_sales_daily 净销售。

**证据**：
- etl_ads_sales_org_daily.py:64-111; etl_ads_store_daily_report.py:106-209; etl_ads_store_daily_report.py:358-363; 2026-04-27 直营差额只读SQL对账

**预防动作**：以后遇到门店日报与销售主题 ADS 对不上，先同时检查共同考核经营体目标覆盖和日报商品纳入口径，再下钻门店明细。

---

### [2026-04-27 15:15] · task · business-rule

**触发场景**：核对月度战役看板负责人拆解总和是否与页级总盘一致

**错误假设**：默认把 ads_store_daily_report 的负责人表总和直接当作页级总盘，并沿用 Tableau 行总计自动汇总日达成/月达成。

**修正结论**：负责人表当前只能代表负责人经营盘，不等于销售主题页级总盘；当前 latest report_date=2026-04-26 时，ads_sales_org_daily 的页级总盘为 17100000 / 12922481.32，而 ads_store_daily_report 全量仅为 16400000 / 11986289.57。且 Tableau 行总计若直接对日达成/月达成做平均会失真，负责人表当前 4 位负责人正确总计应按 sum(日销)/sum(日目标)、sum(月销)/sum(月目标) 重算。

**证据**：
- docs/MYSQL数据字典.md#L187-L226

**预防动作**：后续凡是负责人排名表，先声明其是否要对齐页级总盘；若展示总计，达成率字段必须改为基于 SUM 口径的展示字段，不直接使用 AVG 或原始 rate 列。

---

### [2026-04-27 14:28] · user-feedback · business-rule

**触发场景**：用户要求按 SQL/==线上销售月报SQL_3_0.sql 重新计算 2025-04-01~2025-04-26 各渠道累计实收金额

**错误假设**：先按 MySQL dim_store_report_attr.report_channel_type 去拆历史渠道，默认该配置可以回溯覆盖 2025 年 4 月的线上渠道统计。

**修正结论**：历史线上渠道月报应回到 Oracle C_STORE.CODE 月报口径，按线上月报 SQL 中固定渠道码列表统计，并将 DS015/DS032 合并为 得物；dim_store_report_attr 属于后补业务配置，不能作为 2025-04 历史线上渠道拆分依据。

**证据**：
- SQL/==线上销售月报SQL_3_0.sql:53-75
- SQL/==线上销售月报SQL_3_0.sql:167-206
- docs/MYSQL数据字典.md:516-539
- MySQL/DBHub 实查：dim_store_report_attr 最早 effective_start_date=2026-03-23

**预防动作**：后续凡查询 2026-03-23 之前的线上渠道历史数据，先确认是否要沿用线上月报 SQL 口径；若是，优先直接走 Oracle C_STORE.CODE 固定映射，不再默认套用 report_channel_type。

---

### [2026-04-27 14:11] · task · etl-operations

**触发场景**：同一天第2/第3次总控后，主链已刷新但门店销售专题 ADS 被判定 SKIPPED

**错误假设**：只用 ADS report_date 覆盖到统一上界来判断专题链是否新鲜，忽略同日多次主链刷新后的 dws_sales_daily.etl_time

**修正结论**：专题调度在日期覆盖已满足时，还必须比较近7天 dws_sales_daily.etl_time 与六张专题 ADS etl_time；若源 DWS 更新更晚，应按 source_freshness_branch 日期重跑，不能直接 SKIPPED

**证据**：
- scheduled_store_daily_report.py#L101
- scheduled_store_daily_report.py#L450
- scheduled_store_daily_report.py#L556
- scheduled_store_daily_report.py#L993
- test_scheduled_store_daily_report.py#L127

**预防动作**：后续处理同日多次总控、补数或幂等跳过问题时，先同时核对日期覆盖与源/目标 etl_time freshness；新增专题 ADS 时也要加入 AFFECTED_ADS_TABLE_DATE_COLUMNS 与 freshness 比较范围

---

### [2026-04-27 11:19] · user-feedback · business-rule

**触发场景**：用户明确指出月度战役左下模块不看战区粒度，而看负责人粒度

**错误假设**：默认沿用 ads_sales_org_daily 的战区拆解作为左下模块展示。

**修正结论**：月度战役左下模块应改为 ads_store_daily_report 的 owner_name 粒度负责人排名表，展示日销、日目标、日达成、月销、月目标、月达成，而不是继续展示战区汇总。

**证据**：
- docs/MYSQL数据字典.md#L187-L226

**预防动作**：后续 Tableau 模块设计先区分业务看的是组织汇总还是经营负责人粒度；涉及负责人排名先核对 ads_store_daily_report.owner_name 与日/月字段，不再默认回到 ads_sales_org_daily。

---

### [2026-04-27 10:38] · task · path

**触发场景**：为总控补门店销售专题执行摘要，并要求后续专题共用统一企业微信出口

**错误假设**：把总控摘要缺口当成单个专题文案漏拼问题，继续让主链和专题链各自直接发送企业微信。

**修正结论**：当总控需要统一展示多条子链执行情况时，应让子链输出结构化摘要并在总控模式下抑制各自企业微信，再由 scheduled_total_control.py 统一汇总发送唯一出口；后续新增专题按同一协议接入。

**证据**：
- control_chain_summary.py:1
- run_etl.py:316
- scheduled_store_daily_report.py:730
- scheduled_total_control.py:53
- docs/RUNBOOK.md:109
- docs/数据仓库与ETL手册.md:609

**预防动作**：后续凡是出现‘主链+专题’统一告警诉求，先检查是否缺少结构化摘要协议；禁止通过抓日志或在总控硬编码拼文案扩展专题，新增专题必须同时实现摘要输出和子链告警抑制。

---

### [2026-04-27 09:55] · user-feedback · etl-operations

**触发场景**：执行门店日报专题 ADS 补数时，长时间写库被终端空闲误判为已完成，并因重复触发导致 sales_org 命名锁重试

**错误假设**：把终端空闲或截断输出当成写库完成信号，在前一条写库未确认结束前又启动重复调度/单跑

**修正结论**：长时间写库必须同时用独立终端、Python 进程是否退出、命名锁是否释放、日志终行和库表结果共同确认完成；补数优先最小粒度重跑，前一条写库未完成时禁止再触发同链路

**证据**：
- scheduled_store_daily_report.py#L1770
- scheduled_store_daily_report.py#L1792
- etl_ads_sales_org_daily.py#L579
- etl_ads_sales_org_daily.py#L665

**预防动作**：后续凡是补数或显式 rerun，默认使用独立后台终端启动并等待完成；先查进程和 IS_USED_LOCK，再决定是否进入下一次重跑。

---

### [2026-04-27 09:27] · task · business-rule

**触发场景**：排查门店日报专题链连续多天成功运行但 ADS 最新 report_date 停在旧日期

**错误假设**：把目标链路 file_md5 幂等跳过和负责人 changed/new/exited=0 一起理解成无需继续推进新的业务日

**修正结论**：当专题调度的目标与负责人都未产生新受影响日期时，仍需按当前 data_version 下五张 ADS 的 report_date 覆盖判断是否存在自然日缺口；若存在缺口，应自动补到统一上界，而不是直接 SKIPPED

**证据**：
- scheduled_store_daily_report.py#L366; scheduled_store_daily_report.py#L1667; test_scheduled_store_daily_report.py#L157; test_scheduled_store_daily_report.py#L195

**预防动作**：后续凡是由配置表或快照驱动的日报专题调度，都要额外检查下游 ADS 是否需要按自然日推进，不能仅凭上游配置无变化就判定无需重跑

---

### [2026-04-24 15:13] · task · path

**触发场景**：评估销售主题 Windows 计划任务自动化时，需要决定是把专题链硬并入主链，还是只统一计划任务入口。

**错误假设**：把 Windows 计划任务入口合一等同于业务代码合链，直接把销售专题逻辑塞进 run_etl.py 或主链调度脚本内部。

**修正结论**：销售专题调度应继续保持独立业务边界，由更外层总控脚本顺序串联 scheduled_etl.py 与 scheduled_store_daily_report.py；主链成功后再触发专题链，主链失败则短路停止。

**证据**：
- scheduled_total_control.py:1
- run_etl.py:1
- scheduled_store_daily_report.py:1
- docs/ARCHITECTURE.md:552
- docs/数据仓库与ETL手册.md:820

**预防动作**：后续凡是遇到‘主链+专题’计划任务收口诉求，先判断是否只是入口编排问题；若专题链已有独立幂等、锁和失败语义，优先新增总控包装层，不直接改主链业务边界。

---

### [2026-04-24 13:26] · task · field-mapping

**触发场景**：开始在 Tableau 中实际搭月度战役卡片区时复核整月目标口径

**错误假设**：把 ads_daily_sales 最新 sales_date 的 cum_target_amt 当成整月目标，计划直接用单一数据源完成月度战役全部卡片与拆解。

**修正结论**：ads_daily_sales.cum_target_amt 仅表示截至当前 sales_date 的累计节奏目标，不是整月目标；月度目标、MTD 实际、MTD 同比及按战区/渠道的月度拆解应改用 ads_sales_org_daily 的 MTD 口径，ads_daily_sales 只继续负责月内趋势与预测相关日序列。

**证据**：
- etl_ads_daily_sales.py#L82-L179; etl_ads_sales_org_daily.py#L82-L196; docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L250-L305

**预防动作**：后续凡是月度战役 Tableau 落地，先区分整月目标类指标与日序列节奏类指标；卡片/拆解优先核对是否需要 month_target，再决定使用 ads_sales_org_daily 还是 ads_daily_sales。

---

### [2026-04-24 13:24] · user-feedback · business-rule

**触发场景**：用户确认 Tableau 月度战役模块的草图只用作布局参考

**错误假设**：默认把草图中的 3 大区 / 5 渠道枚举继续当作 Tableau 展示约束，计划额外做映射收口。

**修正结论**：草图仅提供布局参考；月度战役模块的战区、渠道筛选与拆解一律按现网 ads_daily_sales 的真实 area_name 与 report_channel_type 明细值展示，不额外压缩为草图枚举。

**证据**：
- docs/销售部数据治理-子项目/销售看板Tableau实施与追踪.md#L81-L92

**预防动作**：后续凡是销售看板 Tableau 落地任务，先区分草图中的布局参考与真实维度枚举；未获用户明确要求前，不主动新增展示映射层。

---

### [2026-04-24 13:18] · task · field-mapping

**触发场景**：评估 Tableau 月度战役指挥模块是否可直接消费 ads_daily_sales

**错误假设**：默认可以直接对 ads_daily_sales 整个月内日序列的 cum_target_amt、cum_actual_amt 求和来做顶部卡片和拆解表。

**修正结论**：ads_daily_sales 是 battle_month + sales_date + area_name + report_channel_type 的日序列表；趋势图消费整段 sales_date 序列，但顶部卡片、战役洞察、区域拆解、渠道拆解必须先固定到当前 report_date 下的最新 sales_date 再聚合，否则会把累计值按天重复累计。

**证据**：
- etl_ads_daily_sales.py#L41-L198; SQL/create_ads_daily_sales.sql#L1-L29; docs/销售部数据治理-子项目/HEFANG_Dashboard_Requirements_v1.3.md#L248-L270

**预防动作**：在 Tableau 数据源层先建立是否最新销售日或最新销售日过滤逻辑，再用该口径驱动卡片、洞察与拆解表；只有累计趋势图保留整段日序列。

---

### [2026-04-24 11:19] · task · mcp

**触发场景**：扩面治理 ads dim cfg 表 comment 漂移时，DBHub 查询仍把部分 column_comment 显示成乱码，容易误判为 ALTER 未生效

**错误假设**：仅根据 DBHub 直接返回的 column_comment 文本判断注释是否修好，看到坏字就默认数据库里还是错的

**修正结论**：对 MySQL 中文注释先看 HEX(column_comment) 再下结论；若十六进制可解回正确中文，则多半是 DBHub 展示链路乱码，不要重复执行 ALTER。

**证据**：
- SQL/alter_ads_dim_cfg_comment_alignment.sql#L89
- SQL/alter_ads_dim_cfg_comment_alignment.sql#L124
- SQL/alter_ads_dim_cfg_comment_alignment.sql#L129
- SQL/alter_ads_dim_cfg_comment_alignment.sql#L156

**预防动作**：后续凡是 comment 审计或库备注纠偏，先保留文本查询，再补 HEX(column_comment) 复核；只有 HEX 和直连结果都异常时才继续改库。

---

### [2026-04-24 10:57] · user-feedback · field-mapping

**触发场景**：用户授权直接修正 MySQL 表备注时，发现销售主题 ADS 的现网 table_comment/column_comment 仍停留在旧 ETL 时代，而当前专题调度和 ETL 已切到 report_channel_type 明细口径

**错误假设**：默认把 information_schema 里的备注直接当成当前业务语义，或只改数据字典而不回写现网 COMMENT 与仓库 DDL

**修正结论**：对应用层 ADS 的语义备注，应以当前 ETL 和调度链为准；现网 MySQL 只负责提供物理结构事实。若 comment 漂移，必须同时修现网 COMMENT、仓库 create/alter DDL 和数据字典，避免后续重建表或审计再次回退到旧注释

**证据**：
- scheduled_store_daily_report.py#L42
- scheduled_store_daily_report.py#L43
- scheduled_store_daily_report.py#L44
- etl_ads_daily_sales.py#L199
- etl_ads_sales_org_daily.py#L217
- etl_ads_sales_org_monthly.py#L226
- etl_ads_sku_daily.py#L296

**预防动作**：后续凡是审计 ADS 表备注、列注释或数据字典语义时，先区分语义权威和结构权威；专题 ADS 先看当前 ETL 与调度链，再看现网 comment，发现冲突后一次性同步 live COMMENT、DDL 与文档

---

### [2026-04-24 10:37] · user-feedback · doc-sync

**触发场景**：用户明确要求以实际 MySQL 数据库为权威事实，对比并同步 MYSQL 数据字典

**错误假设**：先按仓库代码、README 和旧字典描述复核数据字典，未把现网 MySQL 结构快照作为第一事实源

**修正结论**：当任务目标是同步 MYSQL 数据字典且用户明确指定以数据库为准时，必须先执行 tools/snapshot_mysql_hefangdw_schema.py 或直接查询 information_schema，再按现网列顺序、类型、默认值与已落地对象更新 docs/MYSQL数据字典.md；代码与 README 只能作为补充解释，不能覆盖现网结构事实

**证据**：
- tools/snapshot_mysql_hefangdw_schema.py#L17
- tools/snapshot_mysql_hefangdw_schema.py#L26
- docs/MYSQL数据字典.md#L3
- docs/MYSQL数据字典.md#L191
- docs/MYSQL数据字典.md#L240
- docs/MYSQL数据字典.md#L321

**预防动作**：后续凡是数据字典、表结构对齐、字段是否已落地这类任务，先落 schema 快照与 information_schema 证据，再改文档；若数据库注释与代码文案冲突，先按用户指定的权威源处理，并在交接里显式说明冲突待后续治理

---

### [2026-04-23 18:14] · task · data-reconciliation

**触发场景**：补做 2026-04-22 门店销售专题 ADS 到 Oracle 最终闭环时，发现 ADS 内部已对齐但 YTD 仍固定差 20 元

**错误假设**：默认把小额 YTD 差异继续归因于 ADS 聚合公式或 4/22 当日链路，而没有先验证是否是历史 ODS 明细漂移超出主链 7 天回带窗口

**修正结论**：若 4/22 日额与 MTD 已和 Oracle 对齐、仅 YTD 有固定小额差异，应优先按月、按日、按 store_id、按 SKU 下钻。此次已定位到 2026-03-15 的 store_id=693、retail_id=6719849、retail_item_id=13345710、m_productalias_id=12347：Oracle 金额 1321，MySQL ODS 同 ID 明细仅 1301，导致 ADS YTD 跟随 DWS 少 20 元；这属于历史 ODS 漂移，不是 ADS 口径错误。

**证据**：
- etl_dws_sales.py#L50
- etl_dws_sales.py#L54
- etl_dws_sales.py#L56
- run_etl.py#L59
- run_etl.py#L60
- run_etl.py#L544
- 2026-04-23只读查询：Oracle M_RETAIL/M_RETAILITEM 中 retail_id=6719849, retail_item_id=13345710, item_amt=1321；MySQL ods_m_retail/ods_m_retailitem 同 ID item_amt=1301

**预防动作**：后续 Oracle->ADS 闭环若出现固定小额 YTD 差异，先做 月->日->门店->SKU 的逐级下钻；若日额和 MTD 已闭环而 YTD 仍偏差，优先怀疑历史 ODS 明细漂移超出当前主链回带窗口，并补做指定历史日期的 ODS/DWS 回刷评估。

---

### [2026-04-23 17:31] · task · etl-operations

**触发场景**：门店日报专题调度批量重跑时长期出现 ads_sales_org_daily 命名锁等待

**错误假设**：默认把锁等待归因于单条 ads_sales_org_daily SQL 天然过慢，忽略了专题调度包装层可能被重复触发且子任务锁仅靠连接关闭释放。

**修正结论**：根因应拆成两层：一是 scheduled_store_daily_report.py 缺少顶层单实例锁，多个包装层实例会重复触发同一批日期；二是销售主题 ADS 虽有各表 GET_LOCK 串行化，但此前未显式 RELEASE_LOCK，应在事务结束后释放。

**证据**：
- scheduled_store_daily_report.py#L211
- scheduled_store_daily_report.py#L1598
- etl_ads_sales_org_daily.py#L520
- etl_ads_sales_org_daily.py#L603

**预防动作**：后续凡是包装层会批量调用多个覆盖型 ETL 的场景，先补顶层单实例锁，再检查子任务是否在事务完成后显式释放命名锁。

---

### [2026-04-23 15:27] · user-feedback · business-rule

**触发场景**：用户明确要求销售主题 ADS 每张表都输出 report_channel_type 细分类，且不要再生成 area_name='全国' 与 report_channel_type='全部' 物理汇总成员

**错误假设**：默认沿用 report_channel_type_group 粗分类和 全国/全部 物理总盘行作为销售主题 ADS 与 Tableau 的主消费口径

**修正结论**：销售主题 ADS 应直接以 area_name + report_channel_type 明细切片出数；report_channel_type_group 只保留为 dim_store_report_attr 的派生粗分类，不再作为销售主题 ADS 主输出字段；总盘聚合改由查询层或 Tableau 消费层完成，而不是继续物化 全国/全部 行

**证据**：
- etl_ads_sales_org_daily.py#L217
- etl_ads_daily_sales.py#L199
- SQL/create_ads_sales_org_daily.sql#L5
- SQL/check_ads_sales_org_daily_min.sql#L16

**预防动作**：后续遇到销售看板或销售主题 ADS 需求时，先确认总盘是否必须物化；若无明确要求，默认保留最细 report_channel_type 明细粒度，并把总计逻辑放到消费层聚合。

---

### [2026-04-23 11:48] · task · etl-architecture

**触发场景**：审计 2026-04-22 ADS 对 Oracle 差异时，发现 sales 看板类 ADS 偏小，而门店日报族基本对齐

**错误假设**：默认认为 dws_sales 既然已经接入 run_etl 主链，就会自然消费 ODS 近7天回刷后的晚到数据；同时把近30天 distinct date 覆盖度当成足够的完整性检查

**修正结论**：当 ODS 增量层默认回刷 7 天时，run_etl.py 中 dws_sales 主链也必须使用同样的 7 天窗口重算；仅检查近30天是否有 30 个 date_id 不能发现单日部分漏刷，容易让 2026-04-21 这类晚到补齐停留在 ODS 层

**证据**：
- run_etl.py#L59
- run_etl.py#L526
- run_etl.py#L544
- run_etl.py#L570
- etl_dws_sales.py#L178

**预防动作**：后续凡是多层增量链路出现上游默认回刷窗口时，必须逐层核对窗口是否一致；对 DWS/ADS 不要只做 distinct date 覆盖检查，还要补目标日期的金额级或行数级对账

---

### [2026-04-23 09:39] · task · etl-operations

**触发场景**：修复 ads_sku_daily attach_contribution 精度后重跑 2026-04-22/v2，组织层出现命名锁可重试告警

**错误假设**：看到 ads_sales_org_daily 命名锁等待后，容易继续怀疑 SKU 精度修复未生效，或把阶段性未落库误判为整条专题调度最终失败。

**修正结论**：应先用专题调度日志区分已完成层和等待层；若 ads_sku_daily 已成功输出且组织层仅因命名锁 hefang_dw:ads_sales_org_daily 等待，就转为跟踪最终落库或单独补跑组织层，不要重复回滚前一层精度修复。

**证据**：
- logs/store_daily_report_schedule_20260423.log#L171
- logs/store_daily_report_schedule_20260423.log#L173
- logs/store_daily_report_schedule_20260423.log#L190

**预防动作**：以后专题调度尾层告警先判定是数据错误还是锁等待；按日志+只读查库分层验收，必要时只补跑最后一层。

---

### [2026-04-23 08:46] · task · business-rule

**触发场景**：执行 2026-04-22 的 scheduled_store_daily_report.py 显式重跑验证负责人字段下沉

**错误假设**：默认认为专题调度尾层 ads_sku_daily 仍能沿用既有 attach_contribution 字段范围，未先校验新增日期上的极值是否会超过现有列精度

**修正结论**：2026-04-22 / v2 显式重跑时，ads_sku_daily 因 row 1051 的 attach_contribution 超出列范围而失败；但失败发生在第五层，门店层 ads_store_daily_report、主体层和 ads_daily_sales 已先写成功，验证负责人字段下沉时必须按分层结果复核，不能把尾层失败等同于全部未落库

**证据**：
- scheduled_store_daily_report.py:1570; scheduled_store_daily_report.py:776; etl_ads_store_daily_report.py:42; etl_ads_store_daily_report.py:993

**预防动作**：后续新增派生比率字段接入专题调度前，先对目标日期样本跑极值扫描并校对物理列精度；专题调度失败后按层核对已落库结果

---

### [2026-04-22 17:29] · task · field-mapping

**触发场景**：将负责人字段下沉到 ads_store_daily_report 并接入专题调度

**错误假设**：默认把代码改造和现网物理列已到位视为同一状态，容易遗漏 ads_store_daily_report.owner_name 尚未执行 ALTER 的运行前提

**修正结论**：owner_name 必须按最终经营实体粒度从 dim_store_operation_owner_assignment 命中有效切片，并在 ETL 前显式检查目标表物理列存在；若 alter 未执行，文档和验证都必须标记为未实现

**证据**：
- etl_ads_store_daily_report.py:149; etl_ads_store_daily_report.py:370; etl_ads_store_daily_report.py:526; etl_ads_store_daily_report.py:629; SQL/alter_ads_store_daily_report_add_owner_name.sql:1

**预防动作**：后续新增消费字段时，固定同步落增量 SQL、缺列校验、文档未实现标记与授权前 SQL 清单

---

### [2026-04-22 10:16] · user-feedback · business-rule

**触发场景**：负责人映射 NAS 正式文件缺少业务录入说明

**错误假设**：只在仓库文档中冻结负责人快照口径，没有把录入规则直接写进业务实际填写的 NAS 工作簿，导致业务入口和项目文档可能脱节。

**修正结论**：对外给业务维护的正式 NAS 配置文件，除了同步仓库文档，还要在工作簿内内置填写说明页和关键表头批注，把当前真值快照、共同考核只维护 SUBJECT、负责人可为空、不维护 Excel 历史区间等规则直接冻结在填写入口。

**证据**：
- docs/业务逻辑与指标规范.md#L162
- docs/数据仓库与ETL手册.md#L563
- docs/ETL业务逻辑说明.md#L671

**预防动作**：后续凡是 NAS/Excel 类业务配置模板进入正式使用阶段，都先检查是否已在工作簿内提供填写说明页或表头批注；不要只改仓库文档而遗漏业务实际填写入口。

---

### [2026-04-22 10:16] · task · etl-architecture

**触发场景**：主链路 ODS 处理跨窗口晚改后，第二次重跑又命中 invalid transaction 连接复用

**错误假设**：把窗口删旧、按源id删旧、append写入拆成多个独立事务，认为补按id删旧后就已具备幂等与重跑安全

**修正结论**：ODS 增量分块必须把按源id删旧与该分块写入放进同一 MySQL 事务连接；否则一旦写入异常，会出现半提交，并在后续复用到无效事务连接。当前已改为在同一 mysql_conn 中完成 delete_existing_ids + to_sql。

**证据**：
- etl_ods_incremental_utils.py#L7
- etl_ods_incremental_utils.py#L35
- etl_ods_m_retail.py#L243
- etl_ods_m_retailitem.py#L289
- test_ods_incremental_utils.py#L62

**预防动作**：后续凡是 ODS 增量或回填要做删旧后写新，先明确最小原子单元；同一业务分块内的删旧、去重替换、append 写入必须共用一个事务连接，再做断点续跑与状态表推进。

---

### [2026-04-22 09:20] · task · log-parsing

**触发场景**：排查 2026-04-22 凌晨 ODS 唯一键报错时，用户侧只看到了 SQLAlchemy 批量插入参数预览

**错误假设**：把参数预览中的第一条记录 id=6757272 当成真实冲突行，直接围绕预览样本排查

**修正结论**：IntegrityError 中的 duplicate entry 才是真实冲突键；SQLAlchemy bound parameter 列表只是整批待写入样本预览，必须先锁定 duplicate id=6745851，再去 MySQL/Oracle 对比该 id 的历史版本与最新源记录

**证据**：
- logs/ods_qc_20260422_000542.log#L5-L7
- logs/etl_20260422.log#L109-L112

**预防动作**：后续凡是排查 pandas.to_sql 或 SQLAlchemy executemany 唯一键报错，先读 duplicate entry 指向的业务键，再查目标表现存行和源表最新行；不要把 bound parameter 预览样本误当成冲突主键。

---

### [2026-04-21 17:53] · user-feedback · business-rule

**触发场景**：共同考核经营体负责人快照设计冻结

**错误假设**：默认让业务继续按门店粒度维护被共同考核吸收的 RT 行，或在 Excel 中同时保留门店行和经营体行。

**修正结论**：若门店已归入共同考核经营体，负责人快照只维护 SUBJECT 行；被吸收门店行不应继续出现在 NAS 快照，历史版本由 dim_store_operation_owner_assignment 在库内自动维护。

**证据**：
- tools/import_store_operation_owner_from_nas.py#L403-L483
- test_store_operation_owner_import.py#L13-L52
- docs/业务逻辑与指标规范.md#L311-L319

**预防动作**：后续凡是人工维护负责人、考核归属或组织归属快照，先判断最终消费粒度；若下游已冻结为经营体粒度，则模板、校验和导入表都只维护最终经营实体，不再保留被吸收门店的并存行。

---

### [2026-04-21 15:34] · user-feedback · business-rule

**触发场景**：用户明确否决负责人模板由业务维护SCD2生效区间，改为NAS当前快照每日读取

**错误假设**：沿用门店目标与属性配置的慢变录入方式，让业务在Excel中维护负责人生效开始日和结束日，再由下游直接承接SCD2历史。

**修正结论**：门店负责人Excel应定义为当前快照，不暴露SCD2录入给业务；业务只维护当前门店-负责人真值，ETL每日读取完整快照并与MySQL当前有效记录比对，再在 dim_store_operation_owner_assignment 中自动维护SCD2历史。

**证据**：
- reports/store_owner_mapping_template_20260421_v1.xlsx
- docs/AGENT_HANDOFF.md

**预防动作**：后续凡是门店负责人、组织归属这类低频人工配置，先区分“业务维护当前真值”还是“业务直接维护历史版本”；默认优先采用快照输入 + ETL差异比对 + 维表内SCD2维护，不把历史版本录入责任暴露给业务。

---

### [2026-04-21 14:58] · user-feedback · field-mapping

**触发场景**：用户确认门店负责人映射模板与承接表设计约束

**错误假设**：默认要求负责人编码、默认负责人必须唯一，并把无负责人门店视作导入缺失；同时倾向沿用月度目标的整月重载模型

**修正结论**：门店负责人映射以门店编码为业务维护主键；负责人编码不维护；一个负责人可负责多家门店；部分机场或免税店可无负责人；负责人变更必须支持按历史日期回溯生效，并按SCD2生效区间建模

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sales_rule_freeze.md#L23-L32

**预防动作**：后续实施模板与DDL时，不把负责人做成月度整月重载表，不把空负责人当导入错误；要求业务维护门店编码，并以空负责人表示未分配，按effective_start_date和effective_end_date做慢变版本

---

### [2026-04-20 17:58] · task · etl-history-alignment

**触发场景**：排查 ads_daily_sales.last_year_cum_actual_amt 在 2026-04 报告日全为 0，并继续上溯到 ODS 与 DWS 历史覆盖差异

**错误假设**：看到 dws_sales_daily 当前已经消费 ODS，就默认历史期事实也已和 ODS 全量对齐；同时把 run_etl.py 的近 30 天缺口回补误当成历史迁移补齐机制。

**修正结论**：ODS 引入和 DWS 切换并不等于历史重算。当前主调度只做 ODS 增量同步和 dws_sales_daily 近 30 天覆盖性回补；如果历史聚合源从 Oracle 直连切到 ODS，却没有自 ODS 可用最早业务日期起做一次全量历史重算，再去重跑依赖去年同期、累计或同期累计口径的 ADS，就会出现 last_year_* 长期为 0 或混杂旧口径的问题。

**证据**：
- CHANGELOG.md#L530
- docs/数据仓库与ETL手册.md#L402
- run_etl.py#L523
- run_etl.py#L558
- etl_dws_sales.py#L1
- etl_dws_sales.py#L238

**预防动作**：后续凡是发生 ODS 接管、聚合源替换或关键口径迁移，必须把 历史重算、下游 ADS 重跑、关键去年同期/累计指标抽样核对 作为同一轮上线检查项；不能把近窗补数视为全历史对齐。

---

### [2026-04-17 14:43] · task · business-rule

**触发场景**：评估 ads_sku_daily 接入专题调度后，是否应顺手把 category_health_tag 物化进 SKU 日表。

**错误假设**：默认可以直接拿 ads_inventory_health 的库存健康或库存侧指标，和 ads_sku_daily 当前的 sales_mix_pct 一起，在 `report_date + sku_id + area_name + report_channel_type_group` 粒度上给 SKU 打健康标签。

**修正结论**：在库存侧没有提供与 ads_sku_daily 同粒度的 `inventory_mix_pct` 之前，不应把 `category_health_tag` 物化到 ads_sku_daily。当前 ads_sku_daily 明确是 `报告日 + SKU + 大区 + 经营渠道粗分类` 粒度，并且 `sales_mix_pct` 也是在该切片内计算；而 ads_inventory_health 仍是单日 SKU 库存健康评估，不带 `area_name + report_channel_type_group` 组织切片。此时若直接拿全国 SKU 库存指标去和分区分渠道的销售占比比较，会形成跨粒度混算。首版应继续只展示 `sales_mix_pct`；若后续业务坚持上标签，先补同粒度库存占比，再冻结阈值，例如“高销低存 / 高存低销 / 均衡”。

**证据**：
- docs/DATA_CONTRACTS.md#L714
- docs/DATA_CONTRACTS.md#L717
- docs/DATA_CONTRACTS.md#L735
- docs/DATA_CONTRACTS.md#L847
- docs/DATA_CONTRACTS.md#L864
- docs/DATA_CONTRACTS.md#L874

**预防动作**：后续凡是准备在销售切片表上叠加“健康标签、结构标签、失衡标签”这类销售/库存对比字段时，先核对销售侧和库存侧是否共享同一唯一键与同一组织切片；若粒度未对齐，只能先保留原始占比字段，不要先造标签。

---

### [2026-04-17 12:55] · task · business-rule

**触发场景**：ads_sku_daily 新增连带业绩贡献率并做只读样例探针校验

**错误假设**：默认可以直接从 dws_sales_daily 的 SKU 日聚合反推连带贡献，且贡献率应天然落在 0 到 100 之间。

**修正结论**：连带业绩贡献率必须基于 ods_m_retail 与 ods_m_retailitem 的订单级明细按“含A订单中非A商品销售额 / 含A订单总金额 * 100”计算；由于分母取订单总金额而不是非A商品金额，结果可以大于 100%，不能擅自截断。

**证据**：
- etl_ads_sku_daily.py#L133
- etl_ads_sku_daily.py#L183
- etl_ads_sku_daily.py#L401
- docs/业务逻辑与指标规范.md#L548
- docs/业务逻辑与指标规范.md#L549

**预防动作**：后续凡是连带、共购、订单内贡献类指标，先确认是否需要订单级 ODS 明细，再用样例探针同时校验分子、分母和极值分布，不要先把结果限制在 0 到 100。

---

### [2026-04-17 11:56] · task · sql-scope

**触发场景**：在 ads_sku_daily 中新增近 7 天/30 天滚动趋势字段并做只读运行验证

**错误假设**：把 30 天滚动窗口直接并入月范围主聚合，默认认为只是在补充趋势指标，不会改变 ads_sku_daily 的输出 SKU 集合。

**修正结论**：月范围输出表若要引入更长观察窗，必须把滚动窗口指标拆成独立 CTE，再按原主键回连到月范围结果；不能让 30 天窗口直接决定主结果集，否则会把窗口内有历史但当月无事实的 SKU 带入输出。

**证据**：
- etl_ads_sku_daily.py#L107
- etl_ads_sku_daily.py#L167
- etl_ads_sku_daily.py#L558

**预防动作**：后续凡是给月/日边界固定的 ADS 增加近 7 天、近 30 天、去年同期等诊断窗时，先用只读 SQL 校验 row_count 是否仍等于原边界定义，再检查新增字段覆盖率，不要先默认结果集可以被宽窗口放大。

---

### [2026-04-16 18:37] · task · sql-collation

**触发场景**：ads_sku_daily 首次真实跑数时出现 MySQL 1267/MAX 与 1271/UNION 排序规则冲突，且最小对账 SQL 也因同类问题无法执行

**错误假设**：默认认为把代表字段聚合从 MAX 改为 ANY_VALUE 就足够，忽略了 dim_store、dim_product、dim_sku 与目标表之间混用 utf8mb4_unicode_ci / utf8mb4_0900_ai_ci 后，UNION 分支和校验 SQL 仍会继续冲突

**修正结论**：对最终输出参与 UNION、GROUP BY 校验或与全国/全部常量比较的字符串列统一显式使用 CONVERT(... USING utf8mb4) COLLATE utf8mb4_0900_ai_ci；校验 SQL 里的组织字段派生列与全国/全部常量也要同步显式指定同一 collation

**证据**：
- etl_ads_sku_daily.py:66
- etl_ads_sku_daily.py:106
- etl_ads_sku_daily.py:171
- SQL/check_ads_sku_daily_min.sql:8
- SQL/check_ads_sku_daily_min.sql:32
- SQL/check_ads_sales_org_monthly_min.sql:14

**预防动作**：后续凡是把 dim_store/dim_product/dim_sku 的文本列与 dim_store_report_attr 或 ads_* 目标表字段混合聚合、UNION、CONCAT DISTINCT、CASE 比较时，先查 information_schema.columns 的 collation，再在输出层统一显式 COLLATE，避免先上线再靠运行时报错定位

---

### [2026-04-16 15:41] · task · scheduler

**触发场景**：验证 202604考核数据配置表.xlsx 的专题调度写库链路

**错误假设**：默认以为必须等自动 IMPORTED 分支再次命中新文件，才能验证专题调度下游 ADS 写库是否正常。

**修正结论**：若同一 file_md5 + target_month + target_version 已在 log_store_target_import 中存在 SUCCESS，scheduled_store_daily_report.py 自动模式只会按设计 SKIPPED；此时应先确认幂等跳过事实，再用 --rerun-report-date 配合 --rerun-data-version 对单日受影响日期做最小显式重跑，以验证下游 ADS 写库链路。

**证据**：
- scheduled_store_daily_report.py#L947
- scheduled_store_daily_report.py#L1180

**预防动作**：后续凡是要验证专题调度写库而当前文件已命中 SUCCESS，先查 log_store_target_import 和 file_md5 判重，再决定等待新 md5 文件还是改走显式 rerun 验证，不要把幂等跳过误判成链路未生效。

---

### [2026-04-16 13:43] · task · scheduler

**触发场景**：将 ads_daily_sales 接入 scheduled_store_daily_report 的受影响日期批量重跑

**错误假设**：只改批量重跑执行函数而不同步摘要、告警、CLI 帮助和文档，容易让运行状态与监控描述失真。

**修正结论**：专题调度新增消费目标时，必须同时更新批量重跑执行流、成功/失败告警、CLI 帮助和运行文档，并补一条不触库的最小单元测试覆盖调用顺序与失败续跑上下文。

**证据**：
- scheduled_store_daily_report.py#L451
- test_scheduled_store_daily_report.py#L1
- docs/RUNBOOK.md#L175

**预防动作**：后续扩专题调度消费范围时，先补不触库单元测试，再统一搜索批量重跑相关文案并复跑 doc-sync 与 handoff。

---

### [2026-04-16 10:34] · task · sql-execution

**触发场景**：ads_daily_sales 最小对账 SQL 首次正式执行

**错误假设**：默认认为 ads_daily_sales 与 dim_store 的字符列排序规则一致，直接用 area_name='全国'、report_channel_type_group='全部' 过滤全国总盘即可。

**修正结论**：当对账 SQL 需要在 ads_daily_sales 上用中文字面量过滤全国总盘时，必须显式给字面量指定与目标列一致的排序规则；本案需写成 _utf8mb4'全国' COLLATE utf8mb4_0900_ai_ci 和 _utf8mb4'全部' COLLATE utf8mb4_0900_ai_ci，才能避免 Illegal mix of collations。

**证据**：
- SQL/check_ads_daily_sales_min.sql#L210
- SQL/check_ads_daily_sales_min.sql#L211

**预防动作**：后续凡是 MySQL 对账 SQL 需要把不同来源表的中文字符列与字面量做精确过滤或比较，先查询 information_schema.COLUMNS 确认 collation；若库内混用 utf8mb4_unicode_ci 与 utf8mb4_0900_ai_ci，优先在字面量侧显式 COLLATE，不要等正式对账时报错。

---

### [2026-04-16 09:27] · task · business-rule

**触发场景**：销售看板 ads_daily_sales 首次样板设计

**错误假设**：默认把月度战役首版做成整月预展开，并提前把预测字段下沉到物理表。

**修正结论**：ads_daily_sales 首版必须冻结为 battle_month=report_date 所在自然月月初，sales_date 只覆盖月初到 report_date；未来日期不预展开，forecast_month_end_amt 等预测字段继续留在消费层派生。

**证据**：
- docs/业务逻辑与指标规范.md#L166
- docs/业务逻辑与指标规范.md#L167
- docs/业务逻辑与指标规范.md#L174
- etl_ads_daily_sales.py#L9
- etl_ads_daily_sales.py#L10
- etl_ads_daily_sales.py#L11

**预防动作**：后续再落销售看板月战役相关表时，先冻结时间边界和物理落表边界；凡是缺统一预测参数或算法的字段，一律先留在消费层，不要为了凑宽表提前物化。

---

### [2026-04-15 17:25] · task · sql-execution

**触发场景**：ads_sales_org_daily 首次正式跑单日样本

**错误假设**：在 PyMySQL 参数化查询里直接写 DATE_FORMAT('%Y-%m-01') 和 DATE_FORMAT('%Y-%m-%d')，默认以为百分号只会被 MySQL 处理

**修正结论**：只要 cursor.execute(sql, params) 走 PyMySQL 参数绑定，SQL 里的 DATE_FORMAT 百分号就必须写成 %%Y/%%m/%%d；否则会在 mogrify 阶段被当成 Python 格式化占位符并报 unsupported format character

**证据**：
- etl_ads_sales_org_daily.py#L445
- etl_ads_sales_org_daily.py#L449
- etl_ads_sales_org_daily.py#L506

**预防动作**：后续凡是 ETL 中使用 PyMySQL 参数化 SQL 且包含 DATE_FORMAT、LIKE 或其他百分号字面量时，先检查是否需要把 % 转成 %%；如果 SQL 会直接渲染成字面量再执行，则再按另一条路径处理，不要混用两种语义

---

### [2026-04-15 17:15] · task · etl-defensive-coding

**触发场景**：销售看板 ads_sales_org_daily 首次 conn-test

**错误假设**：把仓库样板 ETL 的 conn-test 也设计成必须存在目标表，导致未授权建表前无法完成源依赖校验

**修正结论**：仓库样板阶段的 conn-test 只检查源表与 SQL 骨架，目标表缺失只告警；正式 run 仍要求目标表已建表，避免把仓库内实现和数据库已落地混为一谈

**证据**：
- etl_ads_sales_org_daily.py#L566
- etl_ads_sales_org_daily.py#L602
- SQL/create_ads_sales_org_daily.sql#L1

**预防动作**：后续新增独立样板 ETL 时，先区分 conn-test 和正式 run 的依赖边界；凡是数据库 DDL 仍需人工授权的对象，conn-test 默认只校验源依赖，不把目标表缺失直接判成失败

---

### [2026-04-15 15:51] · user-feedback · business-rule

**触发场景**：用户要求把门店年标独立配置对象单独定成一份字段契约，直接冻结未来接数入口

**错误假设**：仅保留预留入口描述，未把未来会接数的保密指标升级为独立对象级正式契约。

**修正结论**：当指标当前不展示、但业务已明确未来会重新接入时，不能只停留在字段预留；应提前冻结独立配置对象名、粒度、唯一键、字段契约、DDL 草案和分阶段启用边界。本案未来门店年标唯一接数入口已正式冻结为 cfg_store_target_annual，且对象只承接 annual_target_amt 真值，ytd_target_amt 由节奏配置或 ADS 层派生。

**证据**：
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L81
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L105
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L503
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L702

**预防动作**：后续再遇到当前不展示但未来要接入的保密指标时，先区分展示边界和接数边界；若未来接数方向已明确，就在规划阶段先冻结对象级契约，而不是只留模糊占位字段。

---

### [2026-04-15 15:10] · user-feedback · business-rule

**触发场景**：用户确认销售看板当前仍只看总年标，但要求预留门店年标入口，未来可能上看板

**错误假设**：把当前先只看总年标理解为可以完全取消门店年标入口与字段预留设计

**修正结论**：当前展示层仍只展示总年标，但设计层必须同时预留门店年标独立配置入口、ads_store_daily_report 的 annual_target_amt/ytd_target_amt 预留字段，以及门店年标汇总回 ads_sales_org_monthly 总盘的路径

**证据**：
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L146
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L561
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md#L755

**预防动作**：后续遇到保密字段时，先区分当前不展示和未来不接入；若业务明确未来会上看板，必须先在配置入口、预留字段和汇总路径上留口，再决定首版是否展示

---

### [2026-04-15 14:33] · user-feedback · path

**触发场景**：用户要求将门店日报目标导入使用的 NAS 根目录从 月度日目标配置表 调整为 目标配置表

**错误假设**：默认把旧 NAS 根目录 \\192.168.0.151\hefang总部\14-数据中台\销售部\月度日目标配置表 持续当作活动代码和活动文档的真值

**修正结论**：当前活动代码和活动文档应统一改用 \\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表；日志、归档交接和历史审计产物保留旧路径作为历史证据，不做追改

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L27
- README.md#L445
- docs/RUNBOOK.md#L214

**预防动作**：后续凡是 NAS/UNC 根目录被业务侧纠正，先全局检索并区分活动代码文档与历史归档产物；只更新前者，并在交付前复查旧路径是否仍残留在活动文件中。

---

### [2026-04-15 14:08] · user-feedback · business-rule

**触发场景**：用户继续审核年度经营目标模板，并明确业务填写端不希望维护门店编码。

**错误假设**：默认认为门店级目标模板应同时要求业务填写门店编码和门店名称。

**修正结论**：年度经营目标模板对业务侧应只要求填写门店名称，不要求填写门店编码；编码匹配便利性应由后续导入链路和标准门店名约束承担，而不是把查询编码成本转嫁给业务。

**证据**：
- data/templates/2026年度经营目标配置表_v1.xlsx

**预防动作**：后续设计业务填写模板时，先区分‘业务填写便利性’和‘系统匹配便利性’；若业务端拿不到编码，优先保留标准名称字段，并在说明中明确必须与系统标准名称一致。

---

### [2026-04-15 14:06] · user-feedback · business-rule

**触发场景**：用户审核年度经营目标模板并指出粒度不能停在目标范围，而必须细到每家门店。

**错误假设**：把年度目标真值模板设计成范围粒度，允许按公司/大区/渠道直接维护总盘目标。

**修正结论**：年度经营目标模板首版必须按门店粒度维护，即 1 行 = 1 个目标年度 + 1 家门店 + 1 个目标版本；公司级或其他汇总层目标应由门店级目标汇总得到，而不是在模板里另填一套总盘值。

**证据**：
- data/templates/2026年度经营目标配置表_v1.xlsx

**预防动作**：后续凡是设计目标真值模板，先确认最终分析最细粒度，并以该粒度建模板；聚合层指标优先从明细目标汇总，不再默认允许业务同时维护总盘和明细两套目标。

---

### [2026-04-15 13:38] · task · business-rule

**触发场景**：销售看板 ADS 基线文档已经扩展到字段契约和 DDL 草案后，继续判断是否能直接作为唯一权威资料启动逐表落地。

**错误假设**：把单一设计文档视为 6 张 ADS 主物理表从建表到落库上线的唯一权威资料。

**修正结论**：实施可以从总控基线文档启动，但真正开工必须以总控基线、现网契约文档、专题口径冻结文档、结构快照和文档审计产物组成权威资料包；缺源对象不能因为文档写全了就直接进入正式建表与落库。

**证据**：
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:884
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:893
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:922
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:1053

**预防动作**：后续凡是把设计基线升级为实施入口时，必须显式区分‘可排行动清单’与‘可直接落库’，并补写开工分级、权威资料组合和门禁，不再默认文档完善就等于可以全面开工。

---

### [2026-04-15 13:22] · task · data-model

**触发场景**：销售看板6张主物理表从主题级设计继续细化到字段契约与DDL草案

**错误假设**：在字段级设计阶段把排名、标签、诊断类展示字段直接全部塞进首版主表DDL，导致物理表承担过多消费层语义。

**修正结论**：字段级契约应先只覆盖物理落表字段；除现网已物化字段外，标签、排名、诊断类字段优先保留在消费视图或 Tableau 语义层，等口径和分母源稳定后再决定是否下沉。

**证据**：
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:450
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:618

**预防动作**：后续做 ADS 字段契约或 DDL 草案时，先区分物理落表字段与消费派生字段，再写 CREATE/ALTER 草案，避免用展示字段反向污染主表结构。

---

### [2026-04-15 12:14] · task · data-model

**触发场景**：销售看板ADS基线从8张ADS方案收敛为8个展示主题对应6张主物理表

**错误假设**：把草图展示主题直接等同于最终物理 ADS 数量，按页面模块1:1 规划落表。

**修正结论**：展示主题应与物理表解耦，按粒度、刷新链路和缺源状态收敛主物理表；本案正式结论为8个展示主题对应6张主物理表，ads_store_daily_subject_report 仅保留为兼容层。

**证据**：
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:30
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:34

**预防动作**：后续做驾驶舱或看板设计时，先拆展示层清单，再拆物理表清单，并逐项判断是否可共享组织汇总底表，避免按页面模块数量直接建表。

---

### [2026-04-15 11:29] · task · business-rule

**触发场景**：复核销售看板 8 张 ADS 是否冗余，以及是做 1 张宽表还是分主题 ADS

**错误假设**：默认按看板页面模块 1:1 落 8 张 ADS，或反向压成 1 张跨月度战略、日节奏、门店、SKU、漏斗的超级宽表。

**修正结论**：ADS 应优先按粒度、刷新链路和缺源成熟度拆分；同粒度同来源的主题可合并为共享聚合表，跨 store、SKU、funnel、org summary 粒度不可硬并。

**证据**：
- docs/销售部数据治理-子项目/销售看板ADS建设清单与数据源缺口.md:58,157,201,247,282,335,385; docs/DATA_CONTRACTS.md:587-672; run_etl.py:47-56

**预防动作**：后续新增看板主题时，先按粒度和刷新链路分组，再决定物理表数；禁止按页面模块机械 1:1 建表，也禁止把跨粒度主题硬并为 1 张表。

---

### [2026-04-15 12:12] · task · doc-sync

**触发场景**：按当前实际代码同步仓库与销售部数据治理文档时，发现历史规划对象、设计阶段描述与现网门店日报专题链路被混写。

**错误假设**：默认把历史设计文档里的 ADS 名称和过滤描述直接当成现状，继续把 ads_daily_report / ads_sales_summary 视为当前对象，并把门店日报明细过滤理解为“0 金额整体排除”或“仍处于待落地阶段”。

**修正结论**：当前门店日报现状真值应以独立专题链路为准：正式结果表是 ads_store_daily_report，统计主体层基于 ads_store_daily_report 继续产出 ads_store_daily_subject_report，目标导入与专题调度已经围绕 cfg_store_target_daily 落地；正式明细过滤口径是 ABS(ri.tot_amt_actual) >= 1。ads_daily_report、ads_sales_summary 仅是历史规划对象，不是现网对象。

**证据**：
- etl_ads_store_daily_report.py#L5-L13
- etl_ads_store_daily_report.py#L224-L228
- etl_ads_store_daily_subject_report.py#L70-L88
- scheduled_store_daily_report.py#L4-L10

**预防动作**：后续凡是审计销售部数据治理相关文档，先以当前专题脚本为现状真值，再回看设计文档；凡是历史规划对象，必须显式标注为“历史规划/未实现”，不能与现网链路并列描述。

---

### [2026-04-15 09:56] · task · path

**触发场景**：门店日报专题调度通过子进程调用目标导入脚本，且模板校验在摘要生成前失败。

**错误假设**：默认认为导入脚本失败时一定会生成 output-json，因此调度器可以稳定依据 validation_status 判断是否重试；同时把预解析/模板校验异常也落进通用重试分支。

**修正结论**：子进程在前置模板校验失败时可能根本不产出摘要 JSON，这类目标月份/目标版本不一致、缺少列等问题应直接写结构化失败摘要并归类为不可重试；调度器还需显式强制子进程 UTF-8 输出，避免日志乱码。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py:L2282-L2295
- tools/import_cfg_store_target_daily_from_nas.py:L2553
- tools/import_cfg_store_target_daily_from_nas.py:L2599-L2603
- scheduled_store_daily_report.py:L74-L95
- scheduled_store_daily_report.py:L575-L610
- scheduled_store_daily_report.py:L762-L772
- scheduled_store_daily_report.py:L1126-L1128

**预防动作**：凡是 ETL 包装脚本通过 subprocess 调下游导入工具时，都要求下游在 validation failure 和 unexpected error 两条路径都输出结构化摘要；上游禁止把模板/参数校验类错误放进盲目重试分支，并统一设置 PYTHONUTF8=1 与 PYTHONIOENCODING=utf-8。

---

### [2026-04-15 09:24] · user-feedback · business-rule

**触发场景**：用户要求将正式 ads_store_daily_report 调整为同步业务对账侧结果，并同时修正旧 Oracle 对账样例 SQL 的次级风险

**错误假设**：继续把 ri.tot_amt_actual <> 0 当成门店日报正式冻结口径，并忽略旧 Oracle 样例 SQL 仍停留在 145=礼盒的旧类目范围。

**修正结论**：当前正式门店日报口径已切换为 ABS(ri.tot_amt_actual) >= 1，绝对金额小于 1 的非零明细整体排除；旧 Oracle 日事实与 MTD/同比重算 SQL 也必须同步扩到 146/148/394 并使用同一过滤条件。

**证据**：
- etl_ads_store_daily_report.py#L228
- docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql#L142
- docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql#L177
- docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql#L147
- docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql#L182

**预防动作**：后续若业务确认门店日报对账口径变更，必须同时改正式 ETL、Oracle 样例重算 SQL 和相关业务文档；不要只改正式 SQL 或只改解释文档，否则正式口径与核对脚本会再次漂移。

---

### [2026-04-14 18:18] · task · output-consistency

**触发场景**：业务用 ERP 导出与门店日报截图对账时，发现月累计销量/上月同期销量少 1 或 2，金额只差 0.1/0.2/0.3，连带同步变低

**错误假设**：默认把截图右侧 MySQL 数字当成当前正式 ads_store_daily_report 现网结果，或先怀疑正式 ETL 漏纳了辅销品小额明细。

**修正结论**：先核对正式 ADS 与正式导出物是否一致。当前正式 ETL 在明细层只排除 ri.tot_amt_actual = 0，会保留 0.1/0.2/0.3 这类非零辅销品明细；本次截图右侧差异是另一套对账 SQL 额外过滤了 ABS(tot_amt_actual) < 1 的小额明细所致，不是当前正式 ads_store_daily_report 口径错误。旧 Oracle 样本重算 SQL 的 sample_category_scope 也仍停在 145=礼盒，不能直接拿来代表现行口径。

**证据**：
- etl_ads_store_daily_report.py:223-228
- docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql:117-140
- docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql:122-145

**预防动作**：后续门店日报若出现销量少 1/2、金额只差 0.1/0.2/0.3、连带同步偏低的对账现象，先只读核对正式 ADS 与正式导出物是否一致；若一致，再优先排查对账侧 SQL 是否误加 ABS(tot_amt_actual) >= 1 或沿用了旧 sample_category_scope，不要直接改正式 ETL。

---

### [2026-04-14 10:02] · task · path

**触发场景**：Windows 计划任务/脚本访问 NAS UNC 共享时因凭证丢失触发 WinError 1326

**错误假设**：默认假设 Windows 已保留 \\192.168.0.151\hefang总部 登录会话，直接对 UNC 路径执行 exists/iterdir/open

**修正结论**：在所有 UNC 访问前统一调用基于环境变量的 Win32 WNetAddConnection2W 自动鉴权，读取 HEFANG_NAS_USERNAME/HEFANG_NAS_PASSWORD 重建共享连接

**证据**：
- tools/nas_access.py:15,131; tools/import_cfg_store_target_daily_from_nas.py:29,415; scheduled_store_daily_report.py:79

**预防动作**：新增共享 NAS helper 并接入 NAS 读文件入口；将用户名或密码、NAS 环境变量、自动鉴权失败归类为不可重试

---

### [2026-04-11 21:44] · task · mcp

**触发场景**：将当前仓库的 VS Code Copilot 自定义能力架构迁移到其他项目

**错误假设**：默认把 .vscode/mcp.json、本项目后缀命名和 description 触发词原样复制到新项目即可复用。

**修正结论**：跨项目迁移应先收敛成单文件 clone pack；MCP 只能复制结构不能复制本地连接事实，skills/agents/prompts 的命名、description 触发词和文档路径都要改成新项目真实内容。

**证据**：
- docs/copilot_agent_clone_pack.md#L77
- docs/copilot_agent_clone_pack.md#L196
- docs/copilot_agent_clone_pack.md#L336
- .vscode/mcp.json#L13

**预防动作**：后续凡是做 Copilot 架构迁移，先输出单文件迁移入口，再逐项检查敏感配置、命名后缀、description 触发词和文档路径是否已去项目化。

---

### [2026-04-10 17:40] · user-feedback · business-rule

**触发场景**：用户明确要求最终日报必须直接产出 ads_store_daily_report 表格式，并且不再出现深圳万象天地店与快闪店专用两条物理门店行。

**错误假设**：把共同考核合并结果只放在 ads_store_daily_subject_report 这类下游兼容层，默认认为新增一个主体层表就等于满足最终交付口径。

**修正结论**：当用户指定最终业务交付表就是 ads_store_daily_report 时，必须先把 ads_store_daily_report 自身改成最终经营实体粒度；ads_store_daily_subject_report 只保留为基于最终结果补主体编码和主店锚点的兼容层。

**证据**：
- etl_ads_store_daily_report.py#L8
- etl_ads_store_daily_report.py#L184
- etl_ads_store_daily_subject_report.py#L89
- docs/DATA_CONTRACTS.md#L593
- docs/DATA_CONTRACTS.md#L672

**预防动作**：后续凡是用户强调最终产出表格式或 Tableau 直接消费对象时，先确认真正的业务交付表，再决定在哪一层改粒度；禁止用下游兼容层掩盖上游最终表口径未修正的问题。

---

### [2026-04-10 16:49] · user-feedback · path

**触发场景**：用户将 NAS 门店日报目标文件重命名为 YYYYMM考核数据配置表.xlsx，并要求后续按该格式按月维护

**错误假设**：默认沿用导入脚本当前只支持的 YYYY年MM月日目标配置表_vN.xlsx 自动扫描规则，未同步考虑新的月度命名约定

**修正结论**：门店日报目标导入链路应优先支持 YYYYMM考核数据配置表.xlsx，并保持兼容历史 YYYY年MM月日目标配置表_vN.xlsx；运行文档也要同步切换到新规则

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py:46; tools/import_cfg_store_target_daily_from_nas.py:395; tools/import_cfg_store_target_daily_from_nas.py:438; docs/RUNBOOK.md:211; docs/ARCHITECTURE.md:210; docs/DATA_CONTRACTS.md:371

**预防动作**：凡是依赖 NAS 文件自动扫描的脚本，执行前先确认当前目录命名规范；若用户调整命名，必须同时修改脚本匹配规则、运行手册和数据契约文档

---

### [2026-04-10 15:38] · task · doc-sync

**触发场景**：共同考核统计主体层实现后重跑 scripts/check_doc_sync.py，summary 出现大体量 docs_only/code_only，容易误判本轮文档未对齐

**错误假设**：把 reports/docs_code_alignment.json 的 docs_only/code_only 总量直接当成本轮阻塞结论，并假设审计结果会提供 findings 列表来定位新增对象是否对齐。

**修正结论**：本仓库的 doc-sync 结果要先看 summary 键结构，再对本轮新增表名和脚本名做 intersection 定点校验；.conda/.runtime 与历史噪音会显著放大 code_only/docs_only，总量不能直接代表本轮改动未对齐。

**证据**：
- reports/docs_code_alignment.json#L895477-L895495
- reports/docs_code_alignment.json#L14520-L14538

**预防动作**：后续做文档收口时，先刷新 reports/docs_code_alignment.json，再读取 summary 结构，并用本轮新增对象名做 intersection 精确核对，不要只看 totals 下结论。

---

### [2026-04-10 13:34] · user-feedback · business-rule

**触发场景**：用户补充：快闪店到了有正店的商场，也可能仍然独立考核，不能按商场自动判定合并

**错误假设**：假设只要快闪店与正店处于同一商场，就应自动并入该正店共同考核

**修正结论**：是否共同考核必须由业务显式配置，不能由同商场/快闪标签/门店类型自动推断；同商场既可能合并，也可能独立，设计上必须支持显式选择

**证据**：
- docs/DATA_CONTRACTS.md:300-331; docs/DATA_CONTRACTS.md:386-387; tools/import_cfg_store_target_daily_from_nas.py:301-357; tools/import_cfg_store_target_daily_from_nas.py:803-821

**预防动作**：后续改 NAS 模板或归属映射时，必须新增显式考核模式/统计主体字段，不得用门店类型、商场名或快闪标识隐式决定是否并考

---

### [2026-04-10 13:20] · user-feedback · business-rule

**触发场景**：用户补充快闪店 RT014 有时需并入同商场正店考核，有时到无正店城市需独立考核

**错误假设**：把快闪店归属规则理解成固定并店，默认 RT014 永久并入某个固定门店/商场主体即可

**修正结论**：快闪店归属应支持按生效区间动态切换：同商场有正店时可并入该考核主体；无正店城市时应独立成主体。归属规则本质是统计主体映射，不是门店主数据或渠道字段改写

**证据**：
- docs/DATA_CONTRACTS.md:300-331; docs/DATA_CONTRACTS.md:521-561; etl_ads_store_daily_report.py:90-120; SQL/==线上销售月报SQL_3_0.sql:63-67

**预防动作**：后续凡遇到快闪店，先判断该阶段是合并考核还是独立考核；设计时必须支持一店在不同日期映射到不同统计主体，禁止把 RT014 这类快闪店做成永久硬编码并店

---

### [2026-04-10 13:07] · user-feedback · business-rule

**触发场景**：门店日报新增快闪店 RT014，与深圳万象天地店同商场，需要按同一考核渠道合并月累计与目标，且 RT014 未来可能跨城市流转

**错误假设**：把快闪店直接当成普通独立门店，或把 RT014 永久硬编码并入某个固定门店/渠道，就能长期满足考核口径

**修正结论**：快闪店应保留原始 store_id 粒度事实，另建按 effective_start_date/effective_end_date 生效的统计归属映射层；RT014 在不同时间段可归属不同考核主体，月目标与达成应按统计主体汇总，不应通过改写底层店仓粒度实现

**证据**：
- docs/DATA_CONTRACTS.md:300-331; docs/DATA_CONTRACTS.md:521-561; etl_ads_store_daily_report.py:90-120; SQL/==线上销售月报SQL_3_0.sql:55-75

**预防动作**：后续凡遇到快闪店/并店/一店多码场景，先判断是否属于报表归属问题；优先新增统计主体与映射表，禁止直接改 report_channel_type 或在 store 粒度 ADS 中硬编码合并店码

---

### [2026-04-10 09:28] · user-feedback · business-rule

**触发场景**：用户确认 2026-04-07 门店日报对账差异的最终业务口径

**错误假设**：沿用现网 dim_report_product_rule 的 13 类集合推断业务真实商品范围，并把 RT086 这类跨天补录单先验当成口径排除；同时对净额为0的混合单订单数理解不清。

**修正结论**：门店日报商品范围应纳入 148=辅销品 与 394=配饰；订单数继续按净单口径计算，整单净额为0仍记0单，即使单内存在正负明细对冲；RT086 4月6日 1860 元正常销售单属于业务与 ODS/ADS 的时间差，不调整现有订单数与销售额口径。

**证据**：
- etl_ads_store_daily_report.py:104-110; etl_ads_store_daily_report.py:176-210; SQL/alter_dim_report_product_rule_include_148_394.sql

**预防动作**：后续审计门店日报差异时，先区分商品范围、净单口径、时间差三类问题；用户一旦确认新增纳入类目，先补 dim_report_product_rule，再决定是否重跑历史 ads_store_daily_report。

---

### [2026-04-10 09:25] · task · etl-cutover

**触发场景**：将 ads_inventory_health 达播来源从 ads_dabo_daily_sales 迁到标签主线前，需要评估切源是否只会改实现、不改结果。

**错误假设**：默认认为 legacy 兼容表仍在链路里就代表下游结果仍可信，因而把这次切换当成纯实现重构。

**修正结论**：切换达播来源前必须先做只读结果对比；当前现网 legacy 30 天达播销量和金额为 0，而标签主线可算出非 0 结果，因此首次正式重跑后 Tableau 可见的达播字段很可能发生业务级变化，必须把结果复核单列为交付项。

**证据**：
- etl_ads_health.py#L195
- etl_ads_health.py#L675
- run_etl.py#L640
- 只读SQL对比: legacy_30d qty=0 amount=0, label_30d qty=916 amount=576755.00

**预防动作**：后续凡是把 legacy 兼容表切到新主线，都先做新旧口径只读对比并记录差异，再决定是否把该任务按纯重构收口；若当前产出已失真或长期为 0，必须同步补 TODO/风险项并安排下游消费复核。

---

### [2026-04-09 16:25] · task · etl-architecture

**触发场景**：评估 hefang_dw 去除 dabo_etl 外部依赖时，需要先判断旧链路是否仍通过 legacy 表契约挂在主调度和库存健康链路上

**错误假设**：默认只要代码里不再直接调用外部仓库或脚本，就等于已经摆脱旧 dabo 机制依赖

**修正结论**：去外部依赖应先识别主链是否仍硬依赖 legacy 表；本仓库中真正的阻塞点不是外部仓库路径，而是 etl_ads_health.py 和 run_etl.py 仍消费 ads_dabo_daily_sales 这类兼容表契约

**证据**：
- run_etl.py#L582-L647
- etl_ads_health.py#L286-L307
- etl_ads_health.py#L504-L548

**预防动作**：后续做子项目内化或旧链路退役时，先按 代码调用 / 表契约 / 辅助工具 / 历史文档 四层盘点依赖，再决定删除顺序；凡 ads_inventory_health 等下游仍直接读 legacy 表时，一律先迁移消费再删表。

---

### [2026-04-09 16:03] · task · etl-scheduling

**触发场景**：将 run_etl.py 的 dabo_ready 从旧 ads_dabo_daily_sales 切到 ads_dabo_order_label 主线时，需要同时检查 ads_health 的真实达播消费源。

**错误假设**：默认认为只要把 dabo_ready 的检查表从 ads_dabo_daily_sales 改成 ads_dabo_order_label，就等于完成了主线切换。

**修正结论**：调度就绪判定和下游真实消费源必须分开审视；当前应采用双状态方案：dabo_ready 以 ads_dabo_order_label 最新批次作为主判定，而 ads_health 仍只在 ads_dabo_daily_sales 当日可用时回填兼容达播字段。

**证据**：
- run_etl.py#L210
- run_etl.py#L583
- run_etl.py#L640
- etl_ads_health.py#L165

**预防动作**：以后切换任何上游就绪判定前，先追踪下游真实消费表和回填入口；如果消费源尚未迁移，不要用同一个 ready 开关同时代表主线就绪和兼容回填。

---

### [2026-04-09 15:39] · task · field-mapping

**触发场景**：达播订单标签正式落库后发现 2 条带 -C1/-C2 的小红书组合单在 ads_dabo_order_label 无法用原始 system_order_id 精确桥到 ods_m_retail

**错误假设**：把异常组合单直接当作需要手工修值，或按通用规则批量去除 -C1/-C2 后缀；这会缺乏 ODS 实证，也可能误伤原本能 exact_hit 的组合单。

**修正结论**：保留原始 system_order_id，新增 canonical_system_order_id；只对精确未命中、且在同一 source_file 内存在唯一 exact-hit token superset 候选的组合单做 auto_alias 归一，下游桥接统一使用 COALESCE(canonical_system_order_id, system_order_id)。

**证据**：
- tools/load_dabo_order_labels_from_nas.py:_normalize_order_labels; tools/query_data.py:mysql_dabo_tagged_daily_by_billdate; reports/dabo_order_labels_apply_normalized_20260409.json

**预防动作**：以后遇到逗号组合单异常，先跑 dry-run 看 normalization_unresolved_count 与候选唯一性；坚持 exact_hit 优先、同文件约束、唯一候选约束，禁止把去后缀规则扩成全量默认逻辑。

---

### [2026-04-09 13:48] · task · field-mapping

**触发场景**：正式 apply 后核验 ads_dabo_order_label 与 ODS 桥接覆盖时，发现统一 Excel 中存在逗号拼接的 system_order_id。

**错误假设**：先验把 system_order_id 中的逗号视为脏数据分隔符，准备在没有证据时直接拆成多行标签。

**修正结论**：统一 Excel 的部分 system_order_id 本身就是源端组合串，ODS 中大量 oms_sourcecode 也按原串存储；当前 44 行逗号组合标签里已有 42 行可按原串直接命中，因此不能默认拆分，需先以 ODS 实证判断。

**证据**：
- reports/dabo_order_labels_apply_20260409.json
- ads_dabo_order_label 中 system_order_id LIKE '%,%' 的标签共 44 行，其中 42 行可按原串直接命中 ods_m_retail.oms_sourcecode，仅 2 行未命中。
- 未命中样本：P790425071352081601,P790432065893081001-C1；P790425071352081601,P790432065893081001-C2。

**预防动作**：后续遇到达播订单号含逗号、后缀 C1/C2 或类似组合格式时，先统计其在 ods_m_retail.oms_sourcecode 的原串命中情况，再决定是否需要拆分、补桥或保留原值。

---

### [2026-04-09 11:55] · task · sql-runtime-source

**触发场景**：门店日报 ADS ETL 被要求改为自包含执行，不再运行时依赖外部 .sql 文件。

**错误假设**：默认把 docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql 同时当作设计参考稿和生产运行时 SQL 来源。

**修正结论**：生产 ETL 的正式运行 SQL 应固化在可执行脚本内；docs 下的 SQL 骨架只保留为设计参考时，必须在代码与文档里明确降级为非运行时依赖。

**证据**：
- etl_ads_store_daily_report.py:L25
- etl_ads_store_daily_report.py:L26
- etl_ads_store_daily_report.py:L32
- etl_ads_store_daily_report.py:L468
- README.md:L429
- docs/ARCHITECTURE.md:L206
- docs/ETL业务逻辑说明.md:L595

**预防动作**：后续若某个 ETL 要独立调度或交付，先检查 SQL 是否仍从 docs/ 或临时目录读入；若是，优先改为脚本内置或正式运行资源，并同步在 README、ARCHITECTURE、ETL业务逻辑说明中写清运行时来源与参考稿边界。

---

### [2026-04-09 11:04] · user-feedback · business-rule

**触发场景**：门店日报 NAS 专题调度按六步图复核后，用户要求把自动模式门禁和门店属性同步补成完全一致的版本

**错误假设**：默认认为自动模式只要拿最新文件并做 MD5 判重即可，且 dim_store_report_attr 可以用同日切片整段刷新近似替代新增/退出/变更处理。

**修正结论**：自动模式只自动处理当前月份快照；历史或未来月份必须显式传入 --target-month 或 --file-path。dim_store_report_attr 同步必须按 store_id 区分未变化/变更/新增/退出，变更关旧开新，新增只开新，退出只关旧。

**证据**：
- scheduled_store_daily_report.py:177
- scheduled_store_daily_report.py:552
- tools/import_cfg_store_target_daily_from_nas.py:745
- tools/import_cfg_store_target_daily_from_nas.py:830
- tools/import_cfg_store_target_daily_from_nas.py:1036

**预防动作**：后续再按流程图或业务步骤核对专题调度时，必须逐项检查自动门禁、幂等跳过和 store_attr SCD 动作是否都已落到代码与文档，不能把近似实现视为完成。

---

### [2026-04-09 10:34] · task · path

**触发场景**：为门店日报专题调度实现 ads_store_daily_report 按日期列表批量重跑

**错误假设**：把批量重跑失败后的恢复简单交给外层整次专题调度重试，没有保留剩余待跑日期上下文。

**修正结论**：批量重跑一旦部分成功、部分失败，必须在专题调度层显式保留剩余 report_date 列表和已完成日期，并在下一次重试时只续跑剩余日期；否则正式导入已成功后会命中 file_md5+target_month+target_version 幂等跳过，导致剩余日期无法继续消费。

**证据**：
- scheduled_store_daily_report.py#L932

**预防动作**：后续实现任何按日期列表的专题补跑入口时，都先设计失败上下文对象和剩余日期续跑路径，不要把恢复语义寄托给整次调度重试。

---

### [2026-04-09 09:09] · task · business-rule

**触发场景**：为门店日报 NAS 自动导入设计受影响日期判断器

**错误假设**：可能只按 file_md5、门店数差异或局部单店变更去猜补跑范围，没有先按 cfg_store_target_daily 与 dim_store_report_attr 的写入语义和 ETL 命中语义定义日期窗口。

**修正结论**：第一阶段应先按配置对象语义冻结日期规则：cfg_store_target_daily 正式 apply 代表目标月+目标版本整月覆盖，dim_store_report_attr 同步代表从 store_attr_effective_start_date 到统一上界的日期受影响；最终补跑日期取两者并集，上界固定为 min(目标月月末, 调度执行日-1)，且当前不向其他 data_version 或跨月日期扩散。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L804
- tools/import_cfg_store_target_daily_from_nas.py#L842
- tools/import_cfg_store_target_daily_from_nas.py#L592
- docs/ETL业务逻辑说明.md#L609
- docs/ETL业务逻辑说明.md#L614
- docs/销售部数据治理-子项目/store_daily_report_design.md#L451
- docs/销售部数据治理-子项目/store_daily_report_design.md#L475
- docs/销售部数据治理-子项目/store_daily_report_design.md#L477

**预防动作**：后续实现受影响日期判断器时，先确认配置表写入粒度和 ETL 命中条件，再生成日期列表；不要反过来从 ADS 现存结果、文件 MD5 或单店差异直接推断补跑范围。

---

### [2026-04-08 18:04] · task · path

**触发场景**：为门店日报 NAS 目标导入补正式调度链路

**错误假设**：尝试直接把门店日报专题塞进 run_etl.py 主链，或把 import_cfg_store_target_daily_from_nas.py 改造成只服务自动调度的内部脚本。

**修正结论**：先新增独立专题调度入口 scheduled_store_daily_report.py，保持目标导入工具现有 dry-run/apply 契约不变；由调度层负责最新文件选择、log_store_target_import 的 file_md5 + target_month + target_version 判重，以及重试和告警包装。

**证据**：
- scheduled_store_daily_report.py#L193
- scheduled_store_daily_report.py#L297
- scheduled_store_daily_report.py#L392

**预防动作**：后续若继续补受影响日期判断或 ADS 批量重跑，也优先扩专题调度入口，不反向侵入 run_etl.py 主链或破坏现有导入工具的手工使用路径。

---

### [2026-04-08 17:00] · user-feedback · business-rule

**触发场景**：用户明确纠正 ads_store_daily_report 日订单数口径，要求每个订单按成交金额 >0/0/<0 记 1/0/-1

**错误假设**：曾将门店日报订单数理解为过滤后 COUNT(DISTINCT retail_id)，并对 TOT_AMT_ACTUAL=0 是否按行级数量兜底保留歧义

**修正结论**：ads_store_daily_report 的 day_order_cnt / mtd_order_cnt 必须先按 retail_id 去重，再按单头成交金额 >0=1、=0=0、<0=-1 汇总；TOT_AMT_ACTUAL=0 订单直接记 0，不再按行级数量兜底

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql#L158
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql#L195
- docs/DATA_CONTRACTS.md#L541
- docs/DATA_CONTRACTS.md#L546
- docs/销售部数据治理-子项目/store_daily_report_design.md#L203

**预防动作**：后续凡遇到门店日报订单数字段，不要直接套 COUNT(DISTINCT retail_id) 或引用其他报表的 qty 兜底规则；先确认是否为 ads_store_daily_report 专属净单口径，再同步 SQL 骨架、数据契约和设计文档。

---

### [2026-04-08 16:19] · user-feedback · business-rule

**触发场景**：用户排查 2026-04-07 门店日报对账差异，明确指出日订单数需要减去退单，并发现 146=配件 补纳后历史日报结果未自动刷新

**错误假设**：将门店日报日订单数简单实现为过滤后 COUNT(DISTINCT retail_id)，并默认 dim_report_product_rule 补纳新类目后历史 ads 结果会自动与现行规则一致

**修正结论**：门店日报订单数应按净单口径处理，至少不能把退单与净零头单按正向订单直接计入；当前 2026-04-07 实查有 11 家门店 current_order_cnt 与净单口径不一致。另一方面，dim_report_product_rule 于 2026-04-08 16:15 补入 146=配件后，只影响后续重算口径，不会自动改写 2026-04-08 11:06 已生成的 ads_store_daily_report 历史结果；4.7 当前金额差异 917.68 元全部来自 category_id=146 的历史未重跑。

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql:149-164 当前订单数按 COUNT(DISTINCT fd.retail_id) 统计
- SQL/==线上销售月报SQL_3_0.sql:108-112,167-206 TOT_AMT_ACTUAL=0 时需按行级 QTY 正负兜底
- docs/AGENT_HANDOFF.md:26-39 记录 146=配件 于 2026-04-08 16:15 才补入门店日报商品范围
- MySQL 实查 2026-04-07 RT054: ads 日销售额 4863.00，但按现行规则重算为 5508.00，缺失的 645.00 对应 category_id=146 配件单据 RT054P22604071000040001
- MySQL 实查 2026-04-07 全部门店：金额差异总计 917.68，恰好等于 category_id=146 配件当日贡献；11 家门店 current_order_cnt 与净单口径不一致

**预防动作**：后续凡是调整 dim_report_product_rule 或其他日报范围配置，必须同时区分‘口径已修复’与‘历史 ads 已重跑’两件事；涉及订单数时，不要直接沿用 COUNT(DISTINCT retail_id)，先明确退单与 TOT_AMT_ACTUAL=0 头单的净单规则，再落 SQL。

---

### [2026-04-08 16:14] · user-feedback · business-rule

**触发场景**：用户要求将 146=配件 纳入 dim_report_product_rule，并明确只影响门店日报商品范围

**错误假设**：把门店日报商品范围补类目，误当成需要同步修改 MAIN_CATEGORY_IDS 或库存健康等主销品 12 类链路。

**修正结论**：门店日报商品范围以 dim_report_product_rule 为准，可单独补纳 146=配件；config.py 中 MAIN_CATEGORY_IDS 仍保持 12 类模板，不随这次日报口径调整改动。

**证据**：
- config.py#L51
- etl_ads_store_daily_report.py#L175
- docs/ETL业务逻辑说明.md#L606
- docs/业务逻辑与指标规范.md#L147

**预防动作**：后续遇到日报补类目时，先区分门店日报专属口径和全局主销品口径；只有用户明确要求改主销品模板时，才考虑调整 MAIN_CATEGORY_IDS 及其下游链路。

---

### [2026-04-08 14:08] · user-feedback · field-mapping

**触发场景**：用户明确要求门店日报渠道类型按细分类作为最终业务真值存一列，并额外补一个可衍生的粗分类字段

**错误假设**：把粗分类字段也当成需要人工双写的业务列，或者继续沿用单列粗分类口径，导致细分类真值无法稳定落盘且旧插入链路需要同步维护两套口径

**修正结论**：dim_store_report_attr.report_channel_type 应存细分类最终真值；粗分类应定义为 report_channel_type_group，并由 report_channel_type 自动派生。当前仓库采用生成列 DDL 方案，先落仓库脚本，不在未授权回合直接改现网数据库

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py:48-57,127-136,638,980
- SQL/alter_dim_store_report_attr_add_channel_type_group.sql:1-24
- docs/DATA_CONTRACTS.md:318-330

**预防动作**：后续再遇到‘最终真值 + 可衍生粗分类’场景时，优先把粗分类设计为派生字段或生成列；先确认现网是否已授权改表，再分别同步代码、契约和运行文档，不再人工维护两套可漂移口径。

---

### [2026-04-08 13:15] · user-feedback · field-mapping

**触发场景**：用户要求以 NAS 中的 门店渠道分类(1).xlsx 作为月度日目标配置表的门店类型权威资料

**错误假设**：默认沿用 2026年03月/04月日目标配置表 里现有的粗分类门店类型，未将直营-奥莱、联营-免税、联营-奥莱、线上小程序等细分类作为权威真值覆盖。

**修正结论**：同步月度日目标配置表的 门店类型 时，应以 \\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店渠道分类(1).xlsx 中的 店铺名称->渠道类型 映射为准，并按门店名称精确匹配后覆盖目标表中的 门店类型。

**证据**：
- 用户在 2026-04-08 当轮明确要求
- \\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\门店渠道分类(1).xlsx
- reports/store_target_channel_type_sync_20260408_131416.json

**预防动作**：后续再处理月度目标 Excel 时，先读取权威渠道分类表做差异比对，再批量回写目标表；若出现门店名未匹配，先停在差异清单，不直接放行写入。

---

### [2026-04-08 11:32] · user-feedback · business-rule

**触发场景**：用户明确说明 20XX年0X月日目标配置表_v1.xlsx 是月内完整快照，同月日目标会变更且可能增删门店

**错误假设**：把月度日目标文件当成仅追加不回改的静态模板，或默认同月只会改数值不会出现新增/删除门店。

**修正结论**：月度日目标文件应被视为当月完整快照：同月内日目标会调整，且小概率会新增或删除门店。cfg_store_target_daily 需要支持按月整体替换当前真值；dim_store_report_attr 需要支持基于完整快照的新增、退出、字段变更三类差异处理。

**证据**：
- 用户在 2026-04-08 当轮确认；tools/import_cfg_store_target_daily_from_nas.py；tools/diff_store_report_attr_snapshot.py

**预防动作**：后续设计自动化链路时，不再把 NAS 文件视为仅追加数据源，而是按完整快照模式设计识别、差异计算、幂等重跑和历史审计。

---

### [2026-04-08 11:05] · task · mcp/path

**触发场景**：April 目标正式 apply 后，DBHub 只读查询返回 0 行，但项目直连与 apply 脚本都显示已成功写入

**错误假设**：把 DBHub 只读查询结果当成写后唯一真值，可能误判目标表写入失败。

**修正结论**：数据库写操作仍应走 hefang_dw 项目 Python 直连事务；写后校验优先可先看只读链路，但若 DBHub 与直连结果不一致，以项目直连查询为准，并在回复中明确说明。

**证据**：
- AGENTS.md:23-24; reports/store_target_nas_202604_apply.json; reports/store_daily_report_20260401_v1_validation.json

**预防动作**：后续凡涉及数据库写后校验，保留一份项目直连查询结果作为最终确认依据；若 DBHub 出现假阴性，不阻塞结论，但要在交接记录中注明链路不一致。

---

### [2026-04-08 10:59] · user-feedback · business-rule

**触发场景**：统一 Excel 达播主线从候选集推进到正式对象时，用户明确纠偏当前目标不是金额字段兼容，而是先构建订单标签表。

**错误假设**：把 Excel 金额字段兼容和旧 ads_dabo_daily_sales 聚合迁移当成当前主任务，导致容易继续沿着旧兼容表思路推进。

**修正结论**：当前优先级应改为基于系统单号构建 ads_dabo_order_label，为 ods_m_retail.oms_sourcecode 打上 是否达播/达播渠道 标签；生意额、退款等指标后续统一在 ODS 或 SQL 层按标签筛选计算。

**证据**：
- 用户在 2026-04-08 当轮明确纠偏：当前目标是通过 订单管理.xlsx 为 ODS 订单打达播标签，而不是先解决 Excel 金额字段兼容。
- SQL/create_ads_dabo_order_label.sql#L1-L25
- tools/load_dabo_order_labels_from_nas.py#L1-L226
- tools/query_data.py#L137-L247
- docs/达播数据运营上传指南.md#L13-L41

**预防动作**：后续遇到统一 Excel 或达播链路改造时，先确认当前目标是 输入契约收口、订单打标、还是指标聚合；若用户已明确要先做标签，就不要把金额字段兼容或旧聚合表迁移当成本轮 blocker。

---

### [2026-04-08 10:45] · user-feedback · business-rule

**触发场景**：用户确认 2026年04月日目标配置表_v1 是门店日报范围完整权威快照，并明确月目标与日目标加总不要求相等

**错误假设**：默认把月目标配置表理解为月目标与日目标加总必须一致的严格平衡模板，或将目标文件仅当作非完整快照处理。

**修正结论**：2026年04月日目标配置表_v1 代表当前门店日报范围的完整权威快照；业务会动态调整日目标但不会回改月目标，因此允许月目标与日目标加总不相等，不能把两者相等作为导入阻断条件。

**证据**：
- 用户在 2026-04-08 当轮确认；reports/store_target_nas_202604_target_only_dry_run.json

**预防动作**：后续涉及门店目标导入、日报目标校验和差异比对时，将完整快照属性与目标值平衡性拆开处理：范围对齐按完整快照执行，月目标与日目标加总仅做提示性监控，不做强一致阻断。

---

### [2026-04-08 10:23] · task · mcp/path

**触发场景**：NAS 门店日目标文件从固定单文件切换为按月份分文件

**错误假设**：默认假设 NAS 目录内始终只有一个固定文件名 月度日目标配置表.xlsx，导致用户改成分月文件后脚本直接找不到文件。

**修正结论**：门店日目标 NAS 文件应按目录扫描处理；当前目录固定为 \\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\\，文件命名优先 YYYYMM考核数据配置表.xlsx，并兼容历史 YYYY年MM月日目标配置表_vN.xlsx；目录内多个月份文件时必须显式传 --target-month，同月多版本时必须显式传 --file-path。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py; README.md; docs/RUNBOOK.md

**预防动作**：后续凡依赖 NAS/共享盘文件导入的脚本，先核实现网目录组织方式，再决定是固定路径、目录扫描还是版本选档；文档示例同步避免继续写死固定文件名。

---

### [2026-04-08 10:05] · task · path

**触发场景**：将达播统一 Excel 主线从外部 dabo_etl 迁回 hefang_dw 内部实现

**错误假设**：试图一步到位迁移旧 ads_dabo_order_bridge / ads_dabo_daily_sales 语义，并默认统一 Excel 可直接覆盖旧兼容链路

**修正结论**：应先在 hefang_dw 内落只读候选集提取层，稳定 system_order_id、platform_order_id、平台、主播、SKU 等输入契约；金额字段与旧兼容表迁移策略需单独确认后再落库

**证据**：
- tools/extract_dabo_order_candidates_from_nas.py
- docs/达播数据同步-子项目资料/达播数据同步任务续接上下文.md
- docs/达播数据同步-子项目资料/达播订单桥接Oracle实收实施说明.md

**预防动作**：后续遇到外部兼容链路字段语义未冻结的改造任务时，先落 candidate layer，再决定旧表迁移和兼容输出，不要把输入契约收口与兼容聚合改造绑在同一轮。

---

### [2026-04-08 09:48] · task · import-workflow

**触发场景**：门店日报目标 NAS 固定文件开始同时维护 2026-03 与 2026-04，多月份文件导致导入脚本 dry-run 失败

**错误假设**：默认假设月度日目标固定文件始终只包含一个目标月份，因此脚本按整文件解析且不要求显式选择月份

**修正结论**：NAS 固定文件现在可能同时包含多个月份；导入脚本必须支持 --target-month YYYY-MM 显式过滤，未传时直接失败并返回当前可选月份，避免整文件误导入

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L127
- tools/import_cfg_store_target_daily_from_nas.py#L301
- README.md#L395
- docs/RUNBOOK.md#L167

**预防动作**：后续处理 NAS 目标导入时，先检查 available_target_months；只要同一固定文件出现多个月份，就强制要求显式传 --target-month 并把该行为同步到运行文档

---

### [2026-04-08 09:35] · user-feedback · field-mapping

**触发场景**：用户基于整份云雀订单管理 Excel 与 Oracle 只读核查，纠正统一 Excel 的主桥接键与筛选规则

**错误假设**：延续旧 CSV / 前缀驱动 / 泛化 main_order_id 假设，默认把平台单号或旧主订单号语义当成 Oracle 主桥接键

**修正结论**：统一 Excel 主线下，应以系统单号作为 Oracle / ODS 主桥接键，平台单号仅作辅助追溯；筛选规则固定为 平台划渠道、状态=平台发货、主播名称非空且不以 HEFANG 开头；Oracle 侧只允许只读，桥接结果只能落 MySQL / HEFANG-DW 消费侧

**证据**：
- docs/达播数据同步-子项目资料/达播订单桥接Oracle实收实施说明.md#L117
- docs/达播数据同步-子项目资料/达播数据同步任务续接上下文.md#L41
- docs/达播数据运营上传指南.md#L21

**预防动作**：后续讨论达播桥接时，先区分统一 Excel 主线与旧 CSV 兼容链路；默认使用 system_order_id，不再把 platform_order_id 或文件名前缀当成主桥接依据；Oracle 一律保持只读

---

### [2026-04-07 16:56] · task · etl-snapshot

**触发场景**：固定 as-of 对账后，ODS 全量仍残留 58 单 / 75 件差异

**错误假设**：把全量完成后的残余差异只归因于 Oracle 在全量结束后继续出新，没有先验证全量运行期间的在途写入是否已被当前 full 模式覆盖。

**修正结论**：run_ods --full 不是全局一致快照：ods_m_retail 在 full 模式只取一次 M_RETAIL 的 max modifieddate 作为上界；ods_m_retailitem 又在更晚时点独立取 modifieddate 上界，并对 modifieddate is null 行单独跑一次 null_query。结果是全量运行期间写入的零售主单会漏到 ods_m_retail，66 条 online 明细虽然已写入 ods_m_retailitem 但因父单缺失在 join 对账中表现为缺口，另外 9 条 offline_settime 明细因 null_query 快照过早而真正漏写。

**证据**：
- etl_ods_m_retail.py#L170-L186
- etl_ods_m_retailitem.py#L223-L239
- etl_ods_m_retailitem.py#L296-L335

**预防动作**：以后审计长时间 full load 差异时，先做固定 as-of 对账，再检查各表 last_sync/上界时间是否一致；若不一致，优先考虑全局 as-of/SCN 快照或全量后的 recent catch-up，而不是只盯实时新增。

---

### [2026-04-07 16:36] · task · etl-ops

**触发场景**：前台 Python 终端消失后准备重跑 ODS 全量任务

**错误假设**：未先核对现存 Python 进程和表装载状态就再次启动 run_ods --full，可能导致两个全量进程并发覆盖 ODS 表。

**修正结论**：终端消失不等于任务停止；必须先检查 run_ods 进程、包装日志和表行数，若存在旧进程先清理，再用单实例加 Tee-Object 日志方式重跑。

**证据**：
- run_ods.py:91-102:主流程顺序执行 ods_m_retail/ods_m_retailitem 并在结束后跑质检
- etl_ods_m_retail.py:165-170:full 模式会 TRUNCATE ods_m_retail 后再分批写入

**预防动作**：今后遇到终端丢失时，先查 Win32_Process 和目标表行数；未确认旧任务退出前，禁止再次启动 run_ods --full。

---

### [2026-04-07 14:40] · task · etl-sync

**触发场景**：排查为什么新样本对应的 `ods_m_retail.oms_sourcecode` 在 MySQL 侧持续为空，且与 Oracle 实查结果不一致

**错误假设**：默认认为既然 `ods_m_retail` 已加了 `oms_sourcecode` 字段、也执行过一次历史回填，那么后续 ODS 增量同步会自动持续保留该字段

**修正结论**：历史回填只能补旧数据，不能替代在线同步；若主 ETL 的 Oracle 抽取 SQL 没有显式选出 `OMS_SOURCECODE`，增量模式下“删窗再重灌”的记录会把该字段重新写成空值。必须同时做到两件事：一是在 `etl_ods_m_retail.py` 的在线抽取里保留 `OMS_SOURCECODE`；二是在 `tools/check_ods_incremental.py` 里增加 Oracle/MySQL 的 `oms_sourcecode` 覆盖对照，避免再次静默回退。

**证据**：
- etl_ods_m_retail.py#L107-L118
- tools/check_ods_incremental.py#L28-L38
- tools/check_ods_incremental.py#L107-L137
- 2026-04-07 只读实查：MySQL `ods_m_retail` 在 `modifieddate >= 2026-03-31` 窗口 `blank_rows = total_rows`，但 Oracle 同期 `OMS_SOURCECODE` 仍有大量非空

**预防动作**：后续凡是给 ODS 表新增桥接字段或关键血缘字段，不能只做 DDL 与一次性回填；必须同时核对在线 ETL 的字段清单、增量删窗重灌行为，以及对应质检是否能直接暴露该字段覆盖率回退。

---

### [2026-04-07 13:58] · user-feedback · business-rule

**触发场景**：用户明确说明达播新入口改为云雀导出的 订单管理xxxxx.xlsx，平台不再来自文件名前缀，而是来自 Excel 内部业务字段

**错误假设**：沿用旧假设，继续把 dy/tm/xhs/sph 文件名前缀当作达播平台识别主路径

**修正结论**：新规则应基于 Excel 字段筛选达播订单：平台列初步划分渠道；状态=平台发货；主播名称非空且不以 HEFANG 开头；系统单号用于后续桥接 Oracle

**证据**：
- docs/达播数据同步-子项目资料/订单管理20260402093825.xlsx(sheet=T_V_OMSONLINEORDER; columns=系统单号/平台/状态/主播名称/平台发货时间/商品编码)
- ../dabo_etl/README.md#L65

**预防动作**：后续改造 dabo_etl 时先区分文件级识别和行级业务筛选；若源文件变成统一多平台导出，优先由行内字段派生平台与达播标签，不再依赖文件名前缀

---

### [2026-04-03 18:05] · task · sql-execution

**触发场景**：门店日报 ETL 在执行 SQL 骨架时只打印启动日志，ads_store_daily_report 未刷新到正式范围

**错误假设**：默认认为 PyMySQL 的 CLIENT.MULTI_STATEMENTS + cursor.execute(sql_script) + nextset() 在当前环境可稳定执行整段 SQL 骨架

**修正结论**：对单一 DELETE + INSERT 骨架，优先把会话变量渲染成 SQL 字面量并拆成分语句执行，避免 multi-statement 路径无回执中断

**证据**：
- etl_ads_store_daily_report.py:L49 SQL 删除语句锚点
- etl_ads_store_daily_report.py:L128-L131 渲染后拆分 DELETE/INSERT
- etl_ads_store_daily_report.py:L290-L294 分语句执行替代 multi-statement
- etl_ads_store_daily_report.py:L307-L317 run() 改为调用分语句执行

**预防动作**：后续新增类似 SQL 骨架 ETL 时，先避免会话变量 + multi-statement 组合；若脚本仅打印启动日志，优先检查落盘日志与结果表，再决定是否加探针

---

### [2026-04-03 17:25] · task · versioning

**触发场景**：门店日报正式范围从7家样本扩到NAS 71家门店时，需要批量同步 dim_store_report_attr

**错误假设**：默认把新的 effective_start_date 直接固定为目标月月初，忽略了目标月内已存在样本版本

**修正结论**：若目标月内已有 dim_store_report_attr 版本，应优先沿用该月现有最新 effective_start_date；仅当目标月无现存版本时才回退到月首，并在写库前检查其他起始日是否重叠

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L404
- tools/import_cfg_store_target_daily_from_nas.py#L444
- reports/store_target_nas_formal_scope_dry_run.json#L20
- reports/store_target_nas_formal_scope_dry_run.json#L21
- reports/store_target_nas_formal_scope_dry_run.json#L24

**预防动作**：后续凡是批量扩配置表正式范围或补历史版本，先查询目标月现存版本，再决定默认生效日；若要覆盖月首，必须显式传参并先做重叠检查

---

### [2026-04-03 16:44] · user-feedback · field-mapping

**触发场景**：门店日报准备从7家样本扩到NAS正式范围时，需要批量补dim_store_report_attr.report_channel_type

**错误假设**：尝试依据dim_store或Oracle C_STORE现有字段自动推断普通RT门店的直营/联营类型

**修正结论**：dim_store_report_attr.report_channel_type属于业务配置，现有dim_store与C_STORE缺少可稳定区分直营/联营的字段；正式扩范围前应在导入模板新增门店类型列，由业务逐店标注后再导入

**证据**：
- etl_ads_store_daily_report.py:143-172; docs/数据结构与映射手册.md:567-579; Oracle BOSNDS3.C_STORE只读实查

**预防动作**：后续扩正式范围时，先确认配置模板显式提供门店类型，不再按店名、编码或维表空字段猜测直营/联营

---

### [2026-04-03 16:23] · user-feedback · field-mapping

**触发场景**：用户纠正 NAS 目标文件中的机场免税门店标准名称

**错误假设**：把海口美兰机场T1 这类简称视为可被 cfg_store_target_daily 导入脚本自动命中 dim_store 标准门店名

**修正结论**：门店目标导入按 dim_store.store_name 做大小写不敏感精确匹配；当前正确标准名为 海口美兰国际机场店免税店(T1)，非标准简称会导致 dry-run 未命中并阻断 apply。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L323
- tools/import_cfg_store_target_daily_from_nas.py#L574
- reports/cfg_store_target_daily_dry_run.json#L75

**预防动作**：后续遇到 NAS 目标文件门店未命中时，先以 dry-run 的 missing_store_names 和候选建议为准，对照 dim_store.store_name 修正业务文件，不再凭简称或历史口语名直接放行。

---

### [2026-04-03 15:27] · user-feedback · path

**触发场景**：用户确认门店日报目标 NAS 实际目录与文件命名

**错误假设**：继续把门店日报目标导入目录和文件名保留为泛化的 NAS 指定目录/待约定

**修正结论**：门店日报目标导入目录固定为 \\192.168.0.151\hefang总部\14-数据中台\销售部\目标配置表\\，文件命名优先 YYYYMM考核数据配置表.xlsx，并兼容历史 YYYY年MM月日目标配置表_vN.xlsx。

**证据**：
- tools/import_cfg_store_target_daily_from_nas.py#L32
- README.md#L445
- docs/RUNBOOK.md#L214

**预防动作**：后续实现 NAS 导入脚本时，默认按该固定目录和文件名实现，不再把入口路径当成开放问题。

---

### [2026-04-03 14:57] · user-feedback · business-rule

**触发场景**：用户明确门店日报月目标与日目标的实际关系

**错误假设**：默认把日目标理解为月目标按日稳定拆分后的结果，并隐含日目标月内合计应等于月目标

**修正结论**：月目标是每月固定值，日目标会按业务节奏动态调整；月末冻结后，月内日目标合计允许不等于月目标。

**证据**：
- docs/业务逻辑与指标规范.md#L153
- docs/销售部数据治理-子项目/销售部日报透析.md#L231

**预防动作**：后续实现 NAS 导入与 DQ 时，不新增日目标合计等于月目标的校验，也不根据一方回算另一方。

---

### [2026-04-03 14:15] · user-feedback · business-rule

**触发场景**：用户纠正门店日报目标模板的日目标逻辑

**错误假设**：把导入模板收敛成日粒度窄表，弱化了业务按每日自定义百分比拆分日目标的真实工作方式

**修正结论**：业务模板应保留月目标 + 1日至31日目标的月宽表；日目标由业务按自定义百分比拆出的最终值填写，导入脚本不做均分。

**证据**：
- docs/销售部数据治理-子项目/销售部日报透析.md#L219
- docs/销售部数据治理-子项目/销售部日报透析.md#L227
- docs/销售部数据治理-子项目/销售部日报透析.md#L228

**预防动作**：后续实现 NAS 导入时，区分业务填写模板和数据库落表粒度：业务侧月宽表，入库侧展开为 cfg_store_target_daily 日粒度。

---

### [2026-04-03 13:59] · user-feedback · field-mapping

**触发场景**：用户确认门店日报目标导入模板契约

**错误假设**：继续把目标导入模板保持为开放状态

**修正结论**：目标导入固定为 xlsx；一行一店；按 store_name 映射；小数据量按目标日期+版本删旧后重灌；先不自动触发门店日报 ETL；需要导入日志表。

**证据**：
- docs/销售部数据治理-子项目/销售部日报3月23日.xlsx#4日目标
- docs/数据结构与映射手册.md#L571

**预防动作**：后续实现 NAS 导入脚本时，严格按该模板契约实现，不再扩展 csv/xls 或改成 store_code 主映射。

---

### [2026-04-03 13:11] · user-feedback · path

**触发场景**：用户明确门店日报目标配置的正式导入路径

**错误假设**：继续把 cfg_store_target_daily 的导入方式保留为待定或泛化为手工 Excel 导入

**修正结论**：门店日报正式交付明确采用业务投递 Excel 到 NAS 指定目录，由 Python 定时扫描导入 cfg_store_target_daily；该路径已于 2026-04-03 落盘导入脚本并完成 dry-run、首轮 `--apply` 与专项消费验证。

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_design.md#L402
- README.md#L370

**预防动作**：后续涉及门店日报目标导入时，默认按 NAS 扫描导入设计文档和实施待办推进，不再把导入路径当成开放问题。

---

### [2026-04-03 12:17] · user-feedback · business-rule

**触发场景**：用户确认门店日报目标配置少于有效门店数的处理方式

**错误假设**：把目标配置行数少于有效门店数是否告警还是失败继续保留为待确认开放项

**修正结论**：门店日报中，cfg_store_target_daily 行数少于有效门店数时只告警、不阻断；原因是未来门店数量可能收缩，允许部分门店暂时无目标但保留日报行

**证据**：
- docs/业务逻辑与指标规范.md
- docs/ETL业务逻辑说明.md
- docs/DATA_CONTRACTS.md

**预防动作**：后续遇到目标配置缺口时，不再重复追问是否要失败；默认沿用告警策略，只有用户再次明确要求升级为失败时才调整实现。

---

### [2026-04-03 11:50] · task · etl-defensive-coding

**触发场景**：门店日报正式 ETL 首次 conn-test

**错误假设**：假设 DictCursor 元数据键名固定且连接对象总已建立

**修正结论**：查询 information_schema 时要显式 alias 并用 row.get；rollback/close 前先判断 conn 是否存在

**证据**：
- etl_ads_store_daily_report.py#L117
- etl_ads_store_daily_report.py#L129
- etl_ads_store_daily_report.py#L334

**预防动作**：后续凡是封装新 ETL 入口，只要查元数据就固定做 alias、row.get、conn 判空三项防御

---

### [2026-04-03 11:03] · task · sql-null-handling

**触发场景**：门店日报阶段4样本写入 ADS 后复跑 SQL-4，对账只剩 2 条 month_ach_rate 差异

**错误假设**：误以为有 month_target 时，直接计算 ROUND(mf.mtd_sales_amt / td.month_target, 4) 就会得到 0；忽略了 LEFT JOIN 未命中时 mf.mtd_sales_amt 实际为 NULL。

**修正结论**：对‘有目标但无销售’门店，达成率分子必须先 COALESCE 为 0 再参与除法；否则 ADS 会产出 NULL，而 Oracle/业务期望是 0.0000。

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql#L258
- docs/销售部数据治理-子项目/store_daily_report_dq_result.md#L49
- docs/销售部数据治理-子项目/store_daily_report_dq_result.md#L72

**预防动作**：后续凡是 LEFT JOIN 聚合事实后再计算达成率、同比率或占比，都固定检查分子和分母的 NULL 语义；若业务期望是 0 而不是 NULL，必须在 ROUND/除法前先 COALESCE，并保留‘有目标无销售’样本做专项回归。

---

### [2026-04-03 09:46] · user-feedback · path

**触发场景**：用户指出 docs/misc 先前改名为 docs/专题资料 不符合真实用途，强调该目录用于子项目扩展时的上下文同步、权威资料沉淀与进度监督。

**错误假设**：把承载子项目续接与权威资料的目录按内容泛称命名为 docs/专题资料，忽略了它的工作流职责。

**修正结论**：这类目录应按用途命名为 docs/子项目资料；语义重点是子项目上下文、权威资料与续接监督，而不是泛泛的专题集合。

**证据**：
- README.md#L144
- .github/copilot-instructions.md#L121
- .github/skills/doc-sync-hefang/SKILL.md#L49

**预防动作**：后续重命名目录时，先判断目录承担的是内容主题还是工作流职责；凡用于跨会话续接、权威资料沉淀、进度监督的目录，优先按职责命名，不再使用 misc 或 专题资料 这类泛称。

---

### [2026-04-02 17:00] · task · etl-windowing

**触发场景**：为 ODS 原生补充按业务日期精确回刷能力时，需要同时满足 Oracle 抽取窗口与目标侧清理窗口一致而又不污染常规增量水位。

**错误假设**：把显式回刷窗口直接同时用于 Oracle 抽取和 MySQL 目标表删除，会忽略 ODS modifieddate/settime 当前按 UTC 存储/查询的事实，也容易把一次性补刷误写回 ods_sync_state。

**修正结论**：显式业务窗口模式应拆成两套时间语义：Oracle 抽取继续使用业务时间窗，MySQL 目标侧删除使用业务窗减 8 小时后的 UTC 时间窗；同时显式窗口模式应跳过 ods_sync_state 的窗口推进与 last_sync 回写，避免污染常规增量链路。

**证据**：
- run_ods.py#L92
- etl_ods_m_retail.py#L165
- etl_ods_m_retail.py#L257
- etl_ods_m_retail.py#L336
- etl_ods_m_retailitem.py#L177
- etl_ods_m_retailitem.py#L306
- etl_ods_m_retailitem.py#L449

**预防动作**：后续只要给 ODS 增量 ETL 增加显式补刷窗口，就固定检查三件事：1）源侧抽取与目标侧删除是否需要分离时区语义；2）显式补刷是否会误更新 sync_state；3）CLI 是否只保留一个正式入口，避免模块级脚本各自漂移。

---

### [2026-04-02 12:06] · task · etl-pattern

**触发场景**：治理 ods_m_retail 与 ods_m_retailitem 重复装载事故时，发现仅按目标时间窗先删后写仍会留下跨窗口旧副本

**错误假设**：把 ODS 增量幂等性近似理解为只要按 modifieddate/settime 窗口清理后再 append，就足以避免重复装载

**修正结论**：当源记录会因 modifieddate 或 settime 回刷而跨窗口移动时，窗口删写只能清掉当前时间窗内的旧行，无法清掉更早窗口残留的同 id 旧副本；应改为窗口清理后再对当前源 chunk 按业务 id 删除旧行并写入，同时在模块级加 MySQL 命名锁串行化同表同步。

**证据**：
- etl_ods_m_retail.py#L46
- etl_ods_m_retail.py#L243
- etl_ods_m_retail.py#L293
- etl_ods_m_retailitem.py#L47
- etl_ods_m_retailitem.py#L293
- etl_ods_m_retailitem.py#L385

**预防动作**：后续审查任何 ODS 增量 ETL 时，固定同时检查三件事：1）删除条件是否只依赖目标时间窗；2）写入前是否按当前源业务键替换旧行；3）是否存在同表并发运行保护。若三项缺一，就不要把其称为已具备重复装载防护。

---

### [2026-04-02 11:25] · user-feedback · mcp/path

**触发场景**：用户正式收敛 8.47 小时建索引排障，确认查询慢已解决、重复装载不是直接主因，要求后续不要再回到 SQL 猜测而是转虚拟化侧坐实

**错误假设**：在系统层排障里，如果只看到窗口前半段 ETL/维护任务重叠，就容易把它们误当成 8.47 小时 DDL 的主因，而忽略了索引在 ETL 结束后仍持续数小时这一长尾事实

**修正结论**：判断 DDL 异常慢主因时，要同时看重叠段和长尾段：若 ETL/维护任务只覆盖前半段，而 DDL 在其结束后仍长时间未完成，应把并发任务定性为放大因素，把共盘 IO 争用和保守 MySQL 参数上移为主因。映射 Storport 目标盘时，应先用 SCSITargetId 把 Target 编号对应到具体 VMware 虚拟磁盘和盘符，再下结论。

**证据**：
- logs/etl_20260402.log:22,23,28,255；Storport 摘要样本 2026-04-02 02:22:13 Target0 对应 C 盘，285605 IO / 3.11GB 读 / 1.48GB 写 / 9764 次 128ms+ / 704 次 2s 桶；Win32_DiskDrive 映射 SCSITargetId=0 -> 磁盘0 -> C盘

**预防动作**：后续凡是排查 MySQL DDL 异常慢，先把 DDL 窗口拆成重叠段与长尾段；再用 Storport/磁盘映射确认高延迟样本对应的实际盘符，避免把短时并发噪音误判为整段主因。

---

### [2026-04-02 10:09] · task · mcp/path

**触发场景**：用 VS Code chat session 还原手工 DDL 时间线，并与本地 ETL 日志逐分钟对齐时，需要把会话时间戳与日志时间基准统一

**错误假设**：直接把 chatSessions JSONL 里的 Unix 毫秒时间戳当作本地时间，导致会把 2026-04-02 00:59 误判成 4 月 2 日凌晨完成，而不是北京时间上午完成

**修正结论**：VS Code chat session 的 Unix 毫秒时间戳按 UTC 记录；与仓库 ETL 日志对齐时必须先换算到北京时间 +8。换算后本轮 idx_ods_m_retailitem_m_retail_id_productalias 的完成锚点约为 2026-04-02 08:59，本体 30505 秒窗口约落在 2026-04-02 00:31 至 08:59。

**证据**：
- logs/etl_20260401.log:122,145,397,420；VS Code chat session 时间轴：2026-04-01 09:38:33 UTC 为索引方案阶段、2026-04-02 00:59:37 UTC 为已建完索引反馈

**预防动作**：后续凡是拿 VS Code/Copilot 会话日志去对齐 ETL、调度或数据库日志，先确认时间戳时区并在结论里显式写出换算后的本地时间。

---

### [2026-04-02 09:43] · task · etl-concurrency

**触发场景**：通过 DBHub 只读排查 ods_m_retailitem 建索引期间是否存在并发写入与 online DDL 合并成本放大迹象

**错误假设**：默认认为只要有 performance_schema 和 event_scheduler，就能仅靠 SQL 回放历史并发写入细节并坐实 online DDL 放大来源

**修正结论**：当前能确认 log_bin=ON 且 binlog_format=ROW，run_etl 主链固定调用 run_ods，而 run_ods 每次都会执行 etl_ods_m_retailitem.run()；2026-04-01 日志显示当天至少在 00:06 和 12:31 两次对 ods_m_retailitem 增量写入 12596 与 12345 行。说明存在真实自动写入源，若索引构建跨过这些窗口，online DDL 合并成本会被放大。但 SQL 侧无法直接回放历史 DDL 窗口内的并发写入明细：information_schema.EVENTS 为空，performance_schema 虽开启但 dbhub_ro 无法读取 table_io_waits_summary_by_table，SHOW FULL PROCESSLIST 也只反映当前瞬时状态。

**证据**：
- DBHub 2026-04-02: log_bin=ON, binlog_format=ROW, event_scheduler=ON, current processlist has no active writers
- logs/etl_20260401.log#L122-L147
- logs/etl_20260401.log#L397-L422
- run_etl.py#L383-L385
- run_ods.py#L83-L92

**预防动作**：以后判断 DDL 异常慢是否被并发写入放大时，先分三层取证：当前 MySQL 状态、ETL 调度代码、当天 ETL 日志；若 SQL 侧没有可读的历史 statement 或 table IO 视图权限，不要假装能靠库内只读查询还原历史，需要尽快切换到 ETL 调度日志和任务日志。

---

### [2026-04-02 09:35] · task · system-io

**触发场景**：尝试在普通会话里做 C 盘 uncached 磁盘基准以验证 MySQL DDL 异常慢

**错误假设**：默认认为系统里已有 diskspd，或可直接用 WinSAT 在当前非提权 PowerShell 会话里替代执行磁盘基准

**修正结论**：当前环境中 diskspd.exe 不存在；系统自带 WinSAT.exe 虽存在，但在当前会话执行磁盘基准会报需要提升权限。因此 SQL 侧已收敛后，系统层 uncached 基准暂时卡在工具缺失与权限门槛，而不是分析方法问题。

**证据**：
- System check 2026-04-02: Get-Command diskspd.exe returned no result
- System check 2026-04-02: Get-Command WinSAT.exe -> C:\\Windows\\system32\\WinSAT.exe
- System check 2026-04-02: Start-Process WinSAT disk benchmark failed with elevation required

**预防动作**：以后进入系统层磁盘基准前，先检查 diskspd 是否安装、WinSAT 是否可提权执行；若两者都不可用，应尽早切换为用户手工执行或宿主机监控方案。

---

### [2026-04-02 09:31] · task · system-io

**触发场景**：系统层验证 MySQL datadir/tmpdir 是否共用 C 盘并检查轻量磁盘基准

**错误假设**：把 datadir 和 tmpdir 同盘直接等同于已证明物理磁盘异常慢

**修正结论**：已确认 datadir 与 tmpdir 共用 C 盘 VMware 系统盘，但 256MB 轻量写入和读测试结果偏高，更像缓存参与下的乐观值；当前只能确认共盘高风险，不能仅凭这组结果断言物理盘异常慢。

**证据**：
- System check 2026-04-02: datadir on C drive, tmpdir on C drive
- System check 2026-04-02: C drive is Windows Server 2019 system drive on VMware Virtual disk
- System benchmark 2026-04-02: write ~2041MB/s, seq read ~2965MB/s, random read ~68379 IOPS

**预防动作**：以后做 DDL 慢盘验证时，先区分共盘高风险和已证明物理慢盘；轻量文件测试若紧跟写入后读取，应优先视作缓存敏感结果。

---

### [2026-04-02 09:26] · task · ddl-performance

**触发场景**：继续通过 DBHub MCP 只读排查 ods_m_retailitem 建索引 8.47 小时的目录路径与 IO 参数根因

**错误假设**：只盯着 buffer pool 和 sort buffer 大小，忽略 datadir、tmpdir、InnoDB 临时表空间与刷盘耐久参数叠加在同一系统盘时对 DDL 的放大效应

**修正结论**：当前 datadir 位于 C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Data\\，tmpdir 位于 C:\\Windows\\...\\Temp，innodb_tmpdir 为空，说明数据文件与临时文件都在 C 盘路径上；再叠加 sync_binlog=1、innodb_flush_log_at_trx_commit=1、innodb_io_capacity=200、innodb_page_cleaners=1 等偏保守设置，建索引异常慢更像系统盘路径风险与保守 IO/耐久参数共同作用，而不是单一参数问题。

**证据**：
- DBHub 2026-04-02: datadir=C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Data\\
- DBHub 2026-04-02: tmpdir=C:\\Windows\\SERVIC~1\\NETWOR~1\\AppData\\Local\\Temp, innodb_tmpdir=''
- DBHub 2026-04-02: sync_binlog=1, innodb_flush_log_at_trx_commit=1, innodb_io_capacity=200, innodb_page_cleaners=1

**预防动作**：以后排查 MySQL DDL 异常慢时，必须把 datadir/tmpdir/innodb_tmpdir 与刷盘耐久参数一起看；若数据目录和临时目录都落在系统盘，且 IO 参数偏保守，仅靠 SQL 侧已经不足以继续判断，需尽快转系统层做磁盘基准测试。

---

### [2026-04-02 09:22] · task · ddl-performance

**触发场景**：通过 DBHub MCP 只读排查 ods_m_retailitem 约 309 万行表创建二级索引耗时约 30505 秒的原因

**错误假设**：把 8.47 小时建索引先归因于表行数本身过大，低估 MySQL 内存参数与临时目录落盘路径对 DDL 耗时的放大作用

**修正结论**：当前 ods_m_retailitem 仅约 300.7 万行、数据约 499.5MB、索引约 110.6MB、碎片约 5MB，并无明显表级异常；更可疑的是 innodb_buffer_pool_size 仅 128MB、innodb_sort_buffer_size 仅 1MB、max_heap_table_size 仅 16MB、innodb_tmpdir 为空且 tmpdir 指向 Windows 服务账号临时目录，说明建索引异常慢更像 MySQL 内存参数偏小叠加虚拟机/系统盘临时 IO 路径过慢，而不是单纯表大。

**证据**：
- DBHub 2026-04-02: SHOW TABLE STATUS ods_m_retailitem -> Rows=3006961, Data_length=499531776, Index_length=110559232, Data_free=5242880
- DBHub 2026-04-02: innodb_buffer_pool_size=134217728, innodb_sort_buffer_size=1048576, max_heap_table_size=16777216
- DBHub 2026-04-02: tmpdir=C:\\Windows\\SERVIC~1\\NETWOR~1\\AppData\\Local\\Temp, innodb_tmpdir=''

**预防动作**：以后遇到 ODS 表建索引异常慢时，先同时检查表大小、buffer pool、sort buffer、有效内存临时表上限以及 tmpdir/innodb_tmpdir 落盘路径；若表总大小远超 buffer pool 且临时目录落在系统盘或慢盘，不要再把问题简单归因为数据量。

---

### [2026-04-02 09:00] · task · sql-validation

**触发场景**：用户已人工落地两条关键索引后，使用 DBHub MCP 进入慢 SQL 真实性能验证阶段

**错误假设**：以为建完头表和连接索引后，性能结论只能靠普通 SELECT 体感判断，难以在 MCP 场景下拿到可信的分步耗时

**修正结论**：在 DBHub MCP 场景下，普通 SELECT 返回结果集时未直接附带 wall-clock 时间，但 EXPLAIN ANALYZE 可提供可信的分步实际耗时。当前验证结果显示：头表过滤后 r_rows=14，联表后 join_rows=17，整条查询 EXPLAIN ANALYZE 顶层约 504ms，说明执行路径已进入秒级以下，残余耗时主要在 ri 按 m_retail_id 的多次 index lookup，而不是排序或 MCP 前端假卡死。

**证据**：
- DBHub 2026-04-02: SELECT target query returned 17 rows
- DBHub 2026-04-02: EXPLAIN ANALYZE top sort actual time=504..504 rows=17 loops=1
- DBHub 2026-04-02: r_rows=14, join_rows=17

**预防动作**：以后在 MCP 场景验证索引效果时，优先成组执行：目标 SQL、EXPLAIN ANALYZE、头表基线 COUNT、联表基线 COUNT；不要只看 EXPLAIN 预估行数，也不要只凭聊天窗口体感判断是否还慢。

---

### [2026-04-01 17:40] · task · index-design

**触发场景**：基于 DBHub 已确认的 EXPLAIN 结果，为 ods_m_retail 与 ods_m_retailitem 联表慢 SQL 设计最小高价值索引方案

**错误假设**：可能一开始就把注意力放到表达式排序或主键治理上，忽略了先给头表过滤和子表连接建立最基本的可选访问路径

**修正结论**：对这条 SQL，最小且高价值的两条索引应优先覆盖 r 头表过滤与 ri 子表连接：r 上建议 (billdate, c_store_id, status, isactive, id)，让优化器有机会先按日期+门店缩小 retail_id 集合；ri 上建议 (m_retail_id, m_productalias_id)，让从 r 到 ri 的联接变成 ref 查找。ORDER BY ABS(ri.tot_amt_actual) 仍可能保留排序阶段，因此前两条索引优先解决的是驱动表选择与大范围扫描，而不是表达式排序本身。

**证据**：
- DBHub EXPLAIN FORMAT=TREE 2026-04-01: Table scan on ri -> Sort abs(ri.tot_amt_actual) DESC, ri.id -> Index lookup on r using idx_ods_m_retail_id
- DBHub SHOW INDEX 2026-04-01: ods_m_retail 仅有 modifieddate/oms_sourcecode/id；ods_m_retailitem 仅有 modifieddate/settime
- DBHub duplicate-batch diagnosis 2026-04-01: no-primary-key is governance issue, not direct cause of slow plan

**预防动作**：以后给 ODS 联表查询补索引时，先问三件事：头表是否有强过滤复合索引、子表是否有连接键索引、ORDER BY 是否是无法被普通 BTree 吃掉的表达式；优先先补前两类，再单独评估排序问题。

---

### [2026-04-01 17:35] · task · etl-pattern

**触发场景**：通过 DBHub MCP 继续追查 ods_m_retail 与 ods_m_retailitem 重复 id 的 etl_batch_id 分布，判断是否为一次性重复装载

**错误假设**：看到 ODS 表存在少量重复 id 时，可能把它解释为长期 append 型落地语义，而不是具体批次事故

**修正结论**：当前重复 id 高度集中在两个批次：ods_m_retail 只涉及 20260323164556 与 20260323164557，各 50 行；ods_m_retailitem 只涉及 5523b5af03fd432f9335ab2c9475e3ef 与 c1fdc1ccd1f64735b28b3b124f581455，各 70 行。结合上一轮样本明细看，重复更像同一批次窗口被重复装载一次，而不是长期 append 型 ODS 设计。

**证据**：
- DBHub 2026-04-01: ods_m_retail duplicate batch distribution -> 20260323164556:50, 20260323164557:50
- DBHub 2026-04-01: ods_m_retailitem duplicate batch distribution -> 5523b5af03fd432f9335ab2c9475e3ef:70, c1fdc1ccd1f64735b28b3b124f581455:70
- DBHub 2026-04-01 previous sample check: duplicate rows differ mainly by etl_batch_id

**预防动作**：以后判断 ODS 重复是设计使然还是装载事故时，先统计重复 id 涉及的 etl_batch_id 数量与集中度；若只集中在 1 到 2 个批次，优先按重复装载事故处理，不要先入为主接受 append 语义。

---

### [2026-04-01 17:27] · task · schema-governance

**触发场景**：通过 DBHub MCP 对 ods_m_retail 与 ods_m_retailitem 做只读主键可行性诊断，用户已知 id 存在少量重复

**错误假设**：可能把这两张 ODS 表上的 id 当成天然主键字段，只要重复量很小就可以直接升为 PRIMARY KEY

**修正结论**：两张表中的重复 id 都表现为同一业务记录被不同 etl_batch_id 重复装载：样本记录除 etl_batch_id 与 etl_loaded_at 外业务字段一致，且 COUNT(DISTINCT CONCAT(id,'#',etl_batch_id)) 与总行数完全一致，因此 id 不能直接做 PRIMARY KEY；(id, etl_batch_id) 更像物理装载层候选唯一键

**证据**：
- DBHub 2026-04-01: ods_m_retail 样本 id 6720701/6720702/6720703 均为 cnt=2 且 distinct_batch_cnt=2，业务字段一致
- DBHub 2026-04-01: ods_m_retailitem 样本 id 13346947/13346948/13346949 均为 cnt=2 且 distinct_batch_cnt=2，业务字段一致
- DBHub 2026-04-01: ods_m_retail total_rows=1860209, distinct_id_batch=1860209；ods_m_retailitem total_rows=3092035, distinct_id_batch=3092035

**预防动作**：以后给 ODS 表评估主键或唯一键时，先查重复 id 的明细是否只是跨 etl_batch_id 重复装载；若是，就不要把业务 id 直接当主键，而应先区分业务唯一键与批次装载唯一键。

---

### [2026-04-01 17:03] · task · sql-performance

**触发场景**：通过 DBHub MCP 诊断 ods_m_retail 与 ods_m_retailitem 联表慢 SQL，服务端执行 300+ 秒且 PROCESSLIST 状态为 executing

**错误假设**：先验认为 r 上 billdate、c_store_id、status、isactive 过滤足够窄，优化器会先从 ods_m_retail 过滤后再关联 ods_m_retailitem

**修正结论**：实际 EXPLAIN FORMAT=TREE 显示优化器从 ods_m_retailitem 开始全表扫描约 301 万行，随后按 ABS(ri.tot_amt_actual), ri.id 排序，再通过 idx_ods_m_retail_id 回表过滤 ods_m_retail；根因是 ri 缺少 m_retail_id 及过滤列索引，r 也缺少覆盖筛选条件的复合索引，ORDER BY 表达式进一步放大排序代价

**证据**：
- etl_dws_sales.py#L56
- etl_dws_sales.py#L57
- DBHub EXPLAIN FORMAT=TREE 2026-04-01: Table scan on ri -> Sort abs(ri.tot_amt_actual) DESC, ri.id -> Index lookup on r using idx_ods_m_retail_id

**预防动作**：以后排查 ods_m_retail 与 ods_m_retailitem 的慢联表 SQL，先看 EXPLAIN 是否从 ri 全表扫开始；若 WHERE 主要筛选在 r 头表，优先评估 r(billdate, c_store_id, status, isactive, id) 与 ri(m_retail_id, m_productalias_id) 等复合索引，再判断 ORDER BY 表达式是否必须保留。

---

### [2026-04-01 16:01] · task · mcp/path

**触发场景**：执行带 CTE 的 Oracle 重算 SQL

**错误假设**：直接把 WITH 开头的查询交给 mcp_oracle_reqd_query，导致工具把查询误判为非 SELECT

**修正结论**：对 mcp_oracle_reqd_query 执行含 CTE 的 Oracle 只读查询时，需要包装成 SELECT * FROM ( WITH ... SELECT ... )，并把 ORDER BY 放在最外层

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql#L1
- docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql#L1

**预防动作**：后续遇到 Oracle MCP 的 CTE 查询，先按外层 SELECT 包装再执行；只有包装后仍失败时，才回退到项目直连只读查询。

---

### [2026-04-01 14:59] · user-feedback · path

**触发场景**：用户澄清数据库写操作授权流程，明确默认查询走 dbhub_ro，只读与写入审批分离

**错误假设**：把数据库写操作规则理解成只能由用户人工执行，忽略了用户在当轮审核 SQL 后也可能授权 Agent 代执行

**修正结论**：默认查询与结构探查仍走 DBHub 只读账号 dbhub_ro；凡涉及 DDL/DML，Agent 必须先明确给出拟执行 SQL 或命令并申请权限。只有在用户当轮明确回复授权后，Agent 才可改走 hefang_dw 项目的 Python 直连事务执行写操作，写后校验优先仍走只读链路。

**证据**：
- AGENTS.md#L21
- AGENTS.md#L24

**预防动作**：后续任务只要触及数据库写操作，固定先在回复里列出拟执行 SQL/命令和风险，再等待用户书面授权；未授权时停留在只读探查或人工执行建议层。

---

### [2026-04-01 10:27] · user-feedback · business-rule

**触发场景**：用户指出 4/1 日月累计应统计 3/1~3/31，而不是出现全 0 结果

**错误假设**：默认把日报月累计固定写成 TRUNC(SYSDATE,'MM') 到 SYSDATE-1，忽略每月1日时起始日会大于结束日。

**修正结论**：日报模板的月累计窗口应分两种情况：非每月1日取本月1日到昨天；若今天是每月1日，则回退到上一个完整自然月。去年同期窗口需对该起止区间整体回退 12 个月。

**证据**：
- SQL/==日报数据SQL.sql:15
- SQL/==日报数据SQL.sql:44
- SQL/==日报数据SQL.sql:117
- docs/业务逻辑与指标规范.md:129

**预防动作**：后续凡是写 T+1 日报中的月累计/同期累计 SQL，都要显式验证每月1日场景，先检查统计窗口是否满足 start<=end，再决定是否需要回退到上一个完整自然月。

---

### [2026-03-31 17:58] · task · mcp/path

**触发场景**：用户授权清空达播桥接表与聚合表后重导历史样本，尝试先用 DBHub MCP 执行 DELETE 与校验

**错误假设**：误以为 mcp_dbhub_execute_sql 在用户授权后可直接承担生产库 DELETE，且写后查询可作为最终权威结果

**修正结论**：DBHub MCP 当前使用只读账号 dbhub_ro，不能执行 DELETE；授权后的写操作应改走 dabo_etl 项目自己的 MySQL 连接，且若 MCP 写后查询结果与直连不一致，应以项目直连查询为准

**证据**：
- ../dabo_etl/config/config.yaml#L8
- ../dabo_etl/src/db_handler.py#L44

**预防动作**：后续需要执行 MySQL DML 时，先明确用户授权，再用项目 Python 环境 + DatabaseHandler 执行；写后回归优先使用同一条直连链路复核

---

### [2026-03-31 17:46] · task · business-rule

**触发场景**：使用历史样本排查 ads_dabo_order_bridge 平台仍为 unknown 时，评估是否可直接用 dabo_etl --file 回放修复

**错误假设**：误以为直接重放历史 CSV 能原地覆盖 unknown 平台标记

**修正结论**：dabo_etl 单文件回放会先写 ads_dabo_order_bridge，再删除近 60 天 ads_dabo_daily_sales 后重写聚合；且桥接表唯一键包含 platform_code，所以把旧 unknown 文件按 dy 重放会新增 dy 行而不是覆盖 unknown 行

**证据**：
- ../dabo_etl/src/main.py#L57
- ../dabo_etl/sql/create_tables_mysql.sql#L22

**预防动作**：历史平台修复优先走一次性纠偏 SQL 或专门迁移脚本，避免直接对生产库执行 --file 回放

---

### [2026-03-31 17:34] · user-feedback · business-rule

**触发场景**：用户补充还有视频号 sph，要求把平台前缀集合从 dy/tm/xhs 扩到 dy/tm/xhs/sph

**错误假设**：默认前缀集合只保留 dy/tm/xhs，遗漏了视频号前缀

**修正结论**：平台识别约定扩为 <platform_prefix>_YYYYMMDD.csv，当前前缀集合固定为 dy/tm/xhs/sph，其中 sph=视频号

**证据**：
- docs/达播数据同步/达播订单桥接Oracle实收实施说明.md#L293
- docs/达播数据同步/达播数据同步任务续接上下文.md#L44

**预防动作**：后续新增平台时先由用户明确前缀缩写，再同步更新 dabo_etl 配置、README、需求文档与 hefang_dw 达播设计文档

---

### [2026-03-31 17:04] · user-feedback · business-rule

**触发场景**：用户明确要求平台识别只走文件名前缀，且不再兼容旧 dabo 前缀，并将小红书前缀修正为 xhs

**错误假设**：默认保留 dabo_YYYYMMDD.csv 兼容回退，并误把小红书前缀写成 xmh

**修正结论**：平台识别固定为 <platform_prefix>_YYYYMMDD.csv；当前前缀约定为 dy/tm/xhs；未配置前缀直接拒绝处理，不再回退旧 dabo 前缀

**证据**：
- docs/达播数据同步/达播订单桥接Oracle实收实施说明.md#L293
- docs/达播数据同步/达播数据同步任务续接上下文.md#L44

**预防动作**：后续新增平台时只补前缀映射和上传规范，不再引入默认平台兜底或旧命名兼容逻辑

---

### [2026-03-31 16:12] · task · backfill-strategy

**触发场景**：继续推进 ods_m_retail.oms_sourcecode 主线时，发现历史回填脚本虽然字段与索引齐备，但全量 apply 仍是单条大 UPDATE。

**错误假设**：把大表历史回填理解成'只要补好索引和字段就可以直接做整表 UPDATE JOIN'，低估了长事务本身带来的锁等待与恢复成本。

**修正结论**：对大表历史回填，优先采用'Oracle/源侧装载到暂存表 + 按主键范围分批 apply + 中断后 apply-only 恢复'的模式，不要默认使用单条大 UPDATE 直接覆盖全量。

**证据**：
- tools/backfill_ods_m_retail_oms_sourcecode.py#L138
- tools/backfill_ods_m_retail_oms_sourcecode.py#L340
- docs/ETL业务逻辑说明.md#L598

**预防动作**：后续遇到任何 MySQL 大表补字段回填或桥接键补齐，固定先检查是否能拆成暂存表和分批 apply；只有验证批次粒度可控后，才进入人工执行阶段。

---

### [2026-03-31 16:08] · user-feedback · path

**触发场景**：用户明确要求：所有改表结构、增删改数据、建表建索引、回填补数都必须由其人工执行，Agent 只能给出 SQL 和步骤。

**错误假设**：默认把代理可执行的数据库 DDL/DML 视为常规实现手段，在未获得用户再次明确授权时也可能直接落库。

**修正结论**：本仓库后续默认只允许数据库只读探查；凡涉及建表、改表、建索引、补数回填或任何 DDL/DML，Agent 只能整理可执行 SQL、脚本和风险提示，由用户人工执行。

**证据**：
- AGENTS.md#L21
- AGENTS.md#L22
- AGENTS.md#L23

**预防动作**：后续任务只要触及数据库写操作，先停在 SQL/命令清单层，不直接执行；若用户临时要求代执行，也必须再次明确确认操作范围后再处理。

---

### [2026-03-31 16:01] · user-feedback · mysql-lock

**触发场景**：用户指出 ods_m_retail 回填阻塞的直接原因是 MySQL 内多个事件锁住表，并在疏通后要求重新评估主线方向。

**错误假设**：把之前的回填失败主要归因于脚本或字段设计问题，低估了 MySQL 侧并发事件锁对 ods_m_retail 更新路径的直接影响。

**修正结论**：先区分环境锁冲突与脚本实现风险；当锁被疏通后，应先用样本级回填验证更新路径，再判断是否恢复 ODS 主线。若样本级回填能正常返回且 cache-only=0，说明当前样本已可纯走 ODS，剩余问题只在历史全量回填策略。

**证据**：
- tools/backfill_ods_m_retail_oms_sourcecode.py#L95
- tools/backfill_ods_m_retail_oms_sourcecode.py#L261
- etl_ods_m_retail.py#L113
- tools/query_data.py#L84

**预防动作**：后续遇到大表回填先做两步切分：1）先核对是否存在外部锁/事件阻塞；2）锁解除后先跑样本级或小范围回填验证路径，再决定是否进入全量历史回填。

---

### [2026-03-31 14:32] · task · query-design

**触发场景**：修复 SQL-4 目标配置状态与样本范围冻结风险

**错误假设**：默认把缺失目标配置 COALESCE 为 0、把 diff_value IS NULL 视为 OK，并让 SQL-4 在运行时按维表动态重建样本范围。

**修正结论**：对账 SQL 必须显式区分 PRESENT、VERSION_MISMATCH、MISSING_IN_CFG_TARGET，且只有双侧都为 NULL 或 diff_value = 0 时才能判 OK；SQL-4 必须直接消费冻结样本清单，而不是再按维表动态回查。

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql#L63
- docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql#L144
- docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql#L518
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md#L239

**预防动作**：后续审查对账 SQL 时，固定检查三项：缺失配置是否被折叠成 0、NULL 差异是否被误判为 OK、SQL-4 是否与 SQL-2/SQL-3 共用同一份冻结样本清单。

---

### [2026-03-31 14:04] · task · query-design

**触发场景**：再次深挖样本模板时，发现即使已有建表/清表说明，仍缺少'步骤7之后中断的恢复起点'和'sample_category_scope 超过3类如何扩展'这两类执行边界；同时临时表结构漂移与当前 SQL-4 DDL 不一致时，TRUNCATE 不能修复结构问题

**错误假设**：默认执行者会自然推导出中断恢复起点、类目模板扩容方式和结构漂移处理方式，导致文档虽然覆盖主流程，但在异常恢复和模板扩展边界上仍留有静默出错空间

**修正结论**：执行模板除了主流程外，还必须写明：1）若在刷新 day 临时表后中断，恢复时至少从 SQL-3 刷新步骤重新开始；2）sample_category_scope 超过模板展示行数时要继续追加 UNION ALL；3）临时表结构与当前 DDL 不一致时必须删表重建，不能只 TRUNCATE

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql

**预防动作**：后续做执行文档深审时，除了核对主流程，还要额外检查'中断恢复'、'模板扩容'、'结构漂移'三类边界是否都有显式说明

---

### [2026-03-31 13:59] · user-feedback · query-design

**触发场景**：用户要求再次深挖时，继续发现样本模板虽然已覆盖重跑清表与模板替换，但仍缺少'旧表结构漂移时必须重建而不是仅TRUNCATE'以及'7行模板只是最小样本，不是上限'这两类执行边界说明

**错误假设**：把临时对账表默认成结构永远稳定、样本数量永远等于最小模板行数，导致执行文档只覆盖了清空数据和替换占位值，却没有覆盖结构漂移与样本扩容边界

**修正结论**：阶段4执行模板除了说明建表、清表和占位替换，还必须显式声明两类边界：1）若现有临时表结构落后于当前 SQL-4 DDL，必须删表重建；2）7行样本模板只是最小覆盖，不是上限，实际样本超过7家时必须继续扩展表格、Oracle模板和SQL-4 IN列表

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql

**预防动作**：后续审查执行模板时，固定补查两项：1）初始化说明是否区分首次建表、重跑清表和结构漂移重建；2）模板行数是否明确写成最小模板而非固定上限

---

### [2026-03-31 13:53] · user-feedback · query-design

**触发场景**：用户继续逐层深挖样本模板时指出：重跑对账时若把两张临时表在同一步提前 TRUNCATE，会让 mtd 临时表在 SQL-3 尚未重新执行前就被清空；同时步骤3/8虽然提示替换 sample_store_scope 和 sample_category_scope，但未显式指回第6节模板与第3节步骤B来源，执行者可能只改原SQL中的单行示例

**错误假设**：把'清空旧数据'当作一次性批量预处理，而不是与各自导入步骤配对的局部初始化动作；同时默认执行者会自行把文档中的模板来源和 SQL 文件中的单行占位对应起来，没有在执行步骤中显式给出模板引用路径

**修正结论**：执行链路中的 TRUNCATE 应与各自导入目标表紧邻，避免跨步骤提前清空造成中断时态不一致；凡是 SQL 文件内只有单行 sample_store_scope/sample_category_scope 占位，而执行文档要求扩展为多行范围时，必须在执行步骤中显式引用第6节模板和第3节步骤B的来源说明

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql

**预防动作**：后续审查执行文档时，固定做两项检查：1）每个 TRUNCATE 是否与对应导入步骤紧邻；2）凡是单行示例要扩展为范围模板的地方，执行步骤是否显式指向模板章节和来源章节

---

### [2026-03-31 13:45] · user-feedback · query-design

**触发场景**：用户做跨文件逐条交叉审计时指出：同样一句'检查 params CTE 中所有 DATE'在 SQL-2 和 SQL-3 中风险完全不同，因为 SQL-2 只有2处，SQL-3却有9处；若文档不把替换数量显式写出，执行者很容易只替换前几处就误以为完成

**错误假设**：把跨文件替换提醒写成统一套话，没有把不同 SQL 的硬编码数量差异显式暴露给执行者，导致复杂文件中的高风险替换点被低估；同时模板文案使用'3行示例'这类字面描述，容易让执行者把模板行数误当成可保留结构

**修正结论**：凡是执行文档引用多个 SQL 文件的同类替换动作，必须显式标注每个文件的实际替换数量或检查点数量；模板文案应强调'不要保留任何占位值原样执行'，而不是依赖当前模板展示了几行示例

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql

**预防动作**：后续审查跨文件执行模板时，固定做两项检查：1）同类替换动作是否按文件分别标注数量；2）模板说明是否聚焦占位值替换原则，而不是引用容易变化的示例行数

---

### [2026-03-31 13:33] · user-feedback · query-design

**触发场景**：用户深度审计样本模板时指出：临时对账表即使已建成，重跑样本对账时若不先清空旧数据，导入会因(report_date, store_id)主键冲突失败；同时文档若只自称样本门店清单，会弱化其对 sample_category_scope 的唯一参考职责

**错误假设**：把初始化理解为只要补建表 DDL 就够了，忽略了重跑场景下旧数据也是前置状态的一部分；同时文档用途描述过窄，只强调 sample_store_scope，容易让执行者低估同一文档对类目范围的权威性

**修正结论**：执行模板不仅要覆盖首次建表，还要显式覆盖重跑时的清表动作；凡是同一文档同时承接 sample_store_scope 和 sample_category_scope 两类输入，就应在标题和用途说明中同步声明，避免执行者去别处寻找类目范围入口

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql

**预防动作**：后续审查执行文档时，固定检查两点：1）首次执行与重跑是否都有前置初始化动作；2）文档标题和用途是否完整反映其实际承接的输入类型，不能只写一半职责

---

### [2026-03-31 13:22] · user-feedback · query-design

**触发场景**：用户深度审计样本模板时指出：执行链路要求先导入 Oracle 结果到 MySQL 临时表，但建表 DDL 只埋在 SQL-4 头部注释里，文档若不前置暴露建表动作，执行者会严格按顺序撞上不存在表报错；同时文档与下游 SQL 复用了同名步骤C，容易误读

**错误假设**：默认执行者会自行翻到下游 SQL 头注释提取 DDL，且低估了跨文件复用'步骤A/步骤C'命名带来的语义冲突，导致样本模板虽给了执行顺序，但仍缺少对象初始化和步骤消歧义

**修正结论**：凡是执行文档依赖下游 SQL 注释中的 DDL 或前置对象，必须在主执行链路里显式加入'先建对象再导入'步骤；凡是跨文档复用步骤编号但语义不同，必须加消歧义说明并声明以各自文件上下文为准

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql

**预防动作**：后续审查执行模板时，固定做两项检查：1）每个导入目标或中间对象是否在前序步骤显式创建；2）若多个文件复用步骤A/B/C命名，是否已经补充职责边界和主从关系说明

---

### [2026-03-31 11:12] · task · field-mapping

**触发场景**：验证达播 CSV 订单号是否能作为 Oracle 达播识别桥接键

**错误假设**：默认把达播 CSV 的子订单编号当作 Oracle 反查主键，或假设只能继续依赖 CSV 中的商家收入金额统计达播。

**修正结论**：对抖音样本文件，稳定桥接键是主订单编号，对应 Oracle BOSNDS3.M_RETAIL.OMS_SOURCECODE；退款负单可通过同一键继续命中 RC 单据，适合直接回算 Oracle TOT_AMT_ACTUAL 口径的达播生意额。

**证据**：
- data/dabo_20260204.csv:1;docs/达播数据运营上传指南.md:11;docs/达播数据运营上传指南.md:12;SQL/==日报数据SQL.sql:117;SQL/==日报数据SQL.sql:118

**预防动作**：后续做各平台达播识别时，先拿平台样本文件验证主订单编号与 Oracle OMS_SOURCECODE 的映射关系；未验证前不要直接用子订单编号或 CSV 商家收入金额替代 Oracle 生意额。

---

### [2026-03-31 10:58] · task · field-mapping

**触发场景**：探索是否可从 Oracle 线上渠道单据直接筛出达播销售

**错误假设**：默认认为 DS009/DS006/DS001/DS024 等线上渠道单据或 Oracle 现有字段足以稳定标记达人合作销售。

**修正结论**：当前 Oracle 侧仅能通过门店编码识别平台总渠道，未发现稳定可复用的达人/直播标签；现有达播口径依赖外部 CSV 导入到 ads_dabo_daily_sales，且物理粒度仅为 sale_date+product_alias_code。

**证据**：
- SQL/==日报数据SQL.sql:27;SQL/==日报数据SQL.sql:30;SQL/==日报数据SQL.sql:31;SQL/==日报数据SQL.sql:32;SQL/达播数据ETL建表.sql:2;SQL/达播数据ETL建表.sql:3;SQL/达播数据ETL建表.sql:6;SQL/达播数据ETL建表.sql:9;docs/达播数据运营上传指南.md:16;docs/达播数据运营上传指南.md:17;docs/DATA_CONTRACTS.md:443;docs/DATA_CONTRACTS.md:445;docs/DATA_CONTRACTS.md:447

**预防动作**：后续涉及达播口径时，先验证来源系统是否存在稳定达人标签；若没有，默认走外部文件链路，并在模型设计阶段显式补 channel/platform 字段。

---

### [2026-03-31 10:31] · user-feedback · query-design

**触发场景**：用户深度审计样本门店模板，指出步骤C和步骤D中的MySQL候选查询依赖 @data_version，但文档若不显式要求先 SET，会让 INNER JOIN 静默返回空集并被误判为无数据

**错误假设**：把会话变量前置条件只补在 SQL-4 执行步骤里，忽略了前置候选查询同样依赖 @data_version，导致文档存在相同缺陷在不同位置重复出现

**修正结论**：凡是文档中的交互式 MySQL 查询使用 @report_date、@data_version 等会话变量，都必须在查询前显式写明同会话先 SET；同时执行前自检也要覆盖变量是否已设置，不能只检查占位值替换

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql

**预防动作**：后续审查执行文档时，把'会话变量前置条件'和'自检是否覆盖变量就绪'作为固定检查项；只要出现 @变量，就检查文档是否在同段或前置步骤中要求先 SET

---

### [2026-03-31 09:53] · user-feedback · query-design

**触发场景**：用户继续系统审计样本门店模板，指出执行模板除了给SQL片段，还必须显式给出步骤标签、变量设置步骤和所有强输入模板，否则执行者会在文档内迷路或静默跑空

**错误假设**：把执行说明写成零散的SQL片段和替换要求，没有完整覆盖步骤标签、sample_category_scope模板、SQL-4变量设置等执行关键点，导致执行链路虽接近完整，但仍存在静默失败入口

**修正结论**：阶段4执行模板不仅要提供SQL片段，还要保证文档内引用可定位、所有强输入都有模板、所有必须设置的变量都有正式步骤提示；否则即使SQL正确，执行者仍可能按错顺序或漏设参数

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql

**预防动作**：后续编写执行文档时，按'步骤标签、输入模板、变量设置、执行顺序、结果落盘'五类清单逐项检查，不允许只给片段不给操作路径

---

### [2026-03-31 09:46] · user-feedback · query-design

**触发场景**：用户继续深度审计样本门店模板，指出 sample_category_scope 缺少替换模板，且主样本日期同步提醒不应只停留在文档内SQL，还要覆盖 SQL-2/SQL-3 的 params CTE

**错误假设**：把样本模板的可执行输入近似理解为只有 sample_store_scope，忽略了 sample_category_scope 与 params 日期硬编码同样是 Oracle 重算 SQL 的强约束输入

**修正结论**：阶段4执行模板必须同时给出 sample_store_scope、sample_category_scope 和 params 日期三类输入的替换提示；任何一类保留示例值，SQL-2/SQL-3 都可能静默返回偏差结果

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql

**预防动作**：后续审查执行模板时，按'范围输入、规则输入、窗口参数'三类逐项检查是否都有明确替换说明，不允许只覆盖其中一部分

---

### [2026-03-31 09:42] · user-feedback · query-design

**触发场景**：用户继续深度审计样本门店模板，指出样本门店范围之外，sample_category_scope 同样是 Oracle 重算 SQL 的强约束输入，且文档中的硬编码日期需要显式同步提醒

**错误假设**：把样本执行模板的关注点过度集中在 sample_store_scope，忽略了 sample_category_scope 也是必须替换的占位输入；同时未明确提示第3节多段SQL中的日期硬编码需要跟随主样本日期同步修改

**修正结论**：阶段4凡是驱动 SQL-2/SQL-3 的输入模板，必须同时覆盖门店范围和类目范围两类占位；若文档内存在多处样本日期硬编码，必须显式提示执行者同步替换所有日期字面值

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql

**预防动作**：后续写执行模板时，先检查所有 CTE 占位输入是否都被文档覆盖；凡是存在重复日期硬编码的文档，必须增加统一日期替换提醒

---

### [2026-03-31 09:40] · user-feedback · query-design

**触发场景**：用户再次深度审计样本门店模板，指出Oracle占位符若设计成SQL非法或语义模糊的万能占位，会提高执行出错率；同时指出SQL-4前置链路不能把执行、导入、验证合并描述

**错误假设**：为兼顾字符串和NULL两种场景，使用了不自解释的万能占位形式，并把SQL-2/SQL-3执行与临时表导入写成合并步骤，导致执行者难以按顺序落地

**修正结论**：模板中的占位既要可替换，也要保留清晰的SQL类型提示；对存在NULL分支的字段，应给出显式有值/无值示例。凡是跨Oracle到MySQL的执行链路，必须拆成执行、导入、验证三类顺序步骤，不能合并描述

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md

**预防动作**：后续编写执行模板时，先检查占位是否会误导字段类型；对跨库流程，默认写成'执行SQL -> 导入结果 -> 验证导入成功 -> 执行下一步'的显式顺序

---

### [2026-03-31 09:35] · user-feedback · query-design

**触发场景**：用户深度审计样本门店模板，指出无销售候选SQL即使改到头表+明细表联合判断，若未叠加商品类目裁剪，仍可能与最终filtered_detail口径不一致

**错误假设**：把候选样本筛选SQL仅收口到头表状态过滤和明细行非0金额过滤，就认为已经足够接近最终日报有效销售定义，忽略了dim_report_product_rule类目裁剪对第5类样本有效性的影响

**修正结论**：若样本选择服务于day_ach_rate=0、零销售不漏行等边界验证，必须显式评估其与最终filtered_detail是否仍有差距；即使暂时无法接入商品规则，也要在文档中明确风险，并要求用SQL-2结果人工复核第5类样本

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql

**预防动作**：后续编写候选样本SQL时，不能只说'接近最终口径'；必须明确列出仍缺哪些过滤层，并说明这些缺口会影响哪一类验证样本和哪条DQ规则

---

### [2026-03-31 09:33] · user-feedback · query-design

**触发场景**：用户再次审计样本门店模板，指出无销售候选门店若只按ods_m_retail头表判断，会与SQL骨架和SQL-2的明细行级有效交易口径不一致

**错误假设**：在样本门店模板里用ods_m_retail头表直接判断当日是否有销售，忽略了明细行非0金额和m_productalias_id非空等过滤，导致候选样本可能偏离最终日报有效交易集

**修正结论**：阶段4凡是为样本对账服务的无销售候选筛选，至少应按ods_m_retail加ods_m_retailitem联合判断，并对齐头表状态过滤与明细行非0金额过滤；不能只看头表汇总口径

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql

**预防动作**：后续凡是编写样本筛选SQL或对账前置查询，先检查是否与最终事实集的过滤层级一致；若最终结果依赖明细行级过滤，就禁止退化成头表级存在性判断

---

### [2026-03-31 09:28] · user-feedback · query-design

**触发场景**：用户审计阶段4样本门店清单模板，指出候选SQL不应依赖ADS结果表且Oracle与MySQL替换片段不能混写

**错误假设**：把SQL-2/SQL-3的Oracle FROM dual替换片段与SQL-4的MySQL IN (...)过滤条件写在同一套不对称说明里，并错误使用ads_store_daily_report反查无销售样本，形成循环依赖

**修正结论**：样本门店模板必须分别给出Oracle侧sample_store_scope替换片段和MySQL侧IN (...)替换片段；在阶段4找有目标但无销售门店时，应优先基于ODS或Oracle事实判断无销售，不能依赖ADS结果表

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sample_store_scope.md

**预防动作**：后续凡是同时服务Oracle SQL和MySQL SQL的模板，必须分开写替换片段并标注语法归属；凡是对账前置样本查询，禁止依赖待验证的目标结果表本身

---

### [2026-03-30 17:53] · user-feedback · output-consistency

**触发场景**：用户明确表示不希望每次都依赖另一个模型审计，才发现 SQL 输出层仍有遗漏指标

**错误假设**：在 SQL-4 中完成 compare_base 计算后，没有逐项核对 metric_compare 是否把需求清单里的所有派生指标都展开到最终输出层，导致计算层有字段、输出层漏分支

**修正结论**：以后遇到宽表计算后再做平面展开的 SQL，必须把需求清单与最终输出层逐项对照；compare_base 中存在的必检指标，必须在 metric_compare 或等价最终输出层完整落地，不能只停在中间层

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_ads_compare.sql
- docs/销售部数据治理-子项目/store_daily_report_reconciliation_plan.md

**预防动作**：把计算层字段枚举完整性检查作为固定自检步骤：先列出需求指标清单，再逐项核对最终输出层是否覆盖；若是 UNION ALL 平面展开，必须手工勾掉每个指标后才算完成

---

### [2026-03-30 17:06] · task · business-rule

**触发场景**：为阶段4编写门店日报对账SQL时，发现以事实CTE为驱动会漏掉无销售但应纳入样本范围的门店

**错误假设**：让Oracle日事实/MTD重算SQL以 day_fact 或 mtd_fact 为驱动，导致无销售门店整行缺失，和ADS以门店范围驱动的行为不一致

**修正结论**：门店日报对账SQL应以 sample_store_scope 或 store_scope 为驱动，再 LEFT JOIN 各事实CTE，这样无销售但在范围内的门店也会输出一行，并能正确对账 0 值结果

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql,docs/销售部数据治理-子项目/store_daily_report_oracle_mtd_yoy_recalc.sql,docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql

**预防动作**：后续设计任何日报/宽表对账SQL时，先确认结果集的主键粒度由范围表驱动，而不是由事实聚合CTE驱动；尤其要覆盖无销售但应出行的样本对象

---

### [2026-03-30 16:52] · task · field-mapping

**触发场景**：为阶段4编写Oracle日事实重算SQL时，需要把MySQL配置层的商品纳入口径传入Oracle执行

**错误假设**：最初草案按 product_id 传递样本商品范围，跨库场景下若商品量大，Oracle CTE 需要粘贴大量 UNION ALL 片段，执行与维护成本过高

**修正结论**：在当前门店日报场景中，商品纳入口径优先传 category_id 范围；Oracle 侧通过 M_PRODUCT.M_DIM4_ID 按类目过滤，比枚举 product_id 更稳健。M_PRODUCT 的类目字段以 etl_dim_product.py 为准：M_DIM4_ID -> category_id

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_oracle_day_fact_recalc.sql,etl_dim_product.py

**预防动作**：后续凡是设计跨库对账SQL，先评估是否可传递更高层级的范围键（如 category_id）而不是枚举明细主键，必要时先核对 ETL 中的真实源字段映射

---

### [2026-03-30 11:52] · user-feedback · business-rule

**触发场景**：用户指出阶段4对账方案过度偏向Excel，而真实对账应以ADS结果对齐Oracle源事实

**错误假设**：把样本对账主线写成了与Excel快照对齐，弱化了Oracle主对账地位

**修正结论**：线下销售日报阶段4应以Oracle按冻结口径重算结果作为ADS主对账基准，Excel仅作为同日快照参考和展示结果辅助复核，不作为持续维护的真值源

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_reconciliation_plan.md,docs/销售部数据治理-子项目/线下销售日报任务续接上下文.md

**预防动作**：后续凡是写对账、验收、样本核对文档，必须先显式区分主对账基准与辅助参考源，优先写明Oracle/MySQL系统对账链路

---

### [2026-03-27 13:34] · user-feedback · business-rule

**触发场景**：用户在阶段 3 收口时最终拍板目标版本读取策略

**错误假设**：把 cfg_store_target_daily 的目标版本读取策略继续保留为开放问题

**修正结论**：线下销售日报 SQL 读取 cfg_store_target_daily 时，固定按 target_version = @data_version 精确匹配，由 ETL 或调度层显式传入正确版本

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql:205,docs/销售部数据治理-子项目/线下销售日报任务续接上下文.md:221

**预防动作**：后续实现 ETL 时直接把目标版本作为运行参数传入，不在 SQL 内部再做最新版本推断或回退逻辑

---

### [2026-03-27 13:34] · user-feedback · business-rule

**触发场景**：用户在阶段 3 收口时最终拍板 ADS 退货口径

**错误假设**：把退货是否拆分毛销/退货/净销字段继续保留为开放问题

**修正结论**：线下销售日报 ADS 统一采用净额/净量口径，金额与数量字段按正负值直接汇总，不额外拆毛销/退货/净销三套字段

**证据**：
- docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql:145,docs/销售部数据治理-子项目/store_daily_report_sql_skeleton.sql:160,docs/销售部数据治理-子项目/线下销售日报任务续接上下文.md:220

**预防动作**：后续推进样本对账、DQ 和代码实现时，默认以净额/净量口径落地；若业务要看退货明细，回到 dws_sales_daily 或明细层钻取

---

### [2026-03-26 15:52] · task · field-mapping

**触发场景**：核对线下销售日报中有赞渠道实体是否需要额外映射层

**错误假设**：在未核实 Oracle 事实前，把 HEFANG JEWELRY(有赞) 是否对应单个店仓 ID 当成开放问题

**修正结论**：Oracle 实查确认 HEFANG JEWELRY(有赞) 在 C_STORE 中稳定对应单个店仓：ID=96、CODE=DS003；M_RETAIL 事实中该实体当前也仅命中 C_STORE_ID=96，可在当前设计阶段按单个 dim_store.store_id 处理

**证据**：
- data/store_daily_report_design.md
- BOSNDS3.C_STORE
- BOSNDS3.M_RETAIL

**预防动作**：后续遇到报表实体与店仓映射问题时，先在 Oracle 主档和事实表双重核实是否单 ID 稳定映射，再决定是否需要额外实体映射层

---

### [2026-03-26 15:52] · user-feedback · business-rule

**触发场景**：线下销售日报设计阶段收口 0 金额口径

**错误假设**：沿用通用销售汇总对 TOT_AMT_ACTUAL=0 的兜底思路，未把日报口径明确冻结为整体排除 0 金额记录

**修正结论**：线下销售日报场景中，0 金额记录整体排除，这是销售部已确认的固定业务规则；日报订单数、连带、客单价等指标必须基于排除 0 金额后的有效交易集合重新计算

**证据**：
- data/store_daily_report_design.md
- etl_dws_sales.py#L50
- etl_dws_sales.py#L55

**预防动作**：后续凡是从通用 DWS 迁移到专项 ADS 的设计，先单独核对业务是否覆盖或推翻通用 0 金额处理规则，不要默认沿用兜底口径

---

### [2026-03-24 10:55] · task · mcp/path

**触发场景**：仓库根 `.mcp.json` 已配置 Oracle，但当前 Copilot 会话里始终拿不到 Oracle MCP 工具

**错误假设**：默认认为只要仓库根 `.mcp.json` 存在 `oracle` server，当前 VS Code / Copilot 会话就会自动暴露 Oracle MCP 工具

**修正结论**：当前项目里，Copilot 会话实际注册 MCP 工具时优先看工作区 `.vscode/mcp.json` 与用户级 `mcp.json`；仓库根 `.mcp.json` 不能保证直接暴露为会话工具。2026-03-24 将 Oracle 接入工作区 `.vscode/mcp.json` 后，当前会话已可直接使用 Oracle 查询接口；其中 `mcp_oracle_reqd_query` 稳定可用，而 `mcp_oracle_list_tables` / `mcp_oracle_describe_table` 可能返回空或识别失败。

**证据**：
- .vscode/mcp.json
- .vscode/start_oracle_mcp.ps1
- AGENTS.md

**预防动作**：以后排查“某个 MCP server 配了但会话里看不到工具”时，先确认 VS Code 实际生效的是工作区 `.vscode/mcp.json` 还是用户级 `mcp.json`，再判断是否需要重载窗口、新开聊天，或回退到只读 SQL 查询。

---

### [2026-03-24 09:50] · task · mcp/path

**触发场景**：VS Code 中 io.github.bytebase/dbhub 长时间等待 initialize 后退出码 1

**错误假设**：先把问题当成 DSN 或用户级 mcp.json 参数填写错误

**修正结论**：在 Windows + Node 24.14.0 环境下，@bytebase/dbhub 安装阶段会因 better-sqlite3 原生依赖失败而直接退出；切到本地 Node 22 后可正常启动 MCP server

**证据**：
- .vscode/start_dbhub.ps1; .vscode/mcp.json; docs/AGENT_HANDOFF.md

**预防动作**：以后排查 DBHub 启动失败时，先用 npx -p @bytebase/dbhub dbhub --help 或本地包装脚本验证运行时兼容性，再检查 DSN/inputs。

---

### [2026-03-23 17:46] · task · field-mapping

**触发场景**：dim_channel 自动化测试误判缺少 DS001

**错误假设**：将 O2O_RETAIL_CHANNEL.WING_CODE 直接假设成 DS001 这类店仓编码，并据此在测试中硬编码检查 WING_CODE='DS001'

**修正结论**：dim_channel.WING_CODE 应按 Oracle O2O_RETAIL_CHANNEL 原值理解；2026-03-23 实查 Oracle 与 MySQL 均为 87 条且 WING_CODE 全部非空，但不存在 DS001 这一硬编码值

**证据**：
- etl_dim_channel.py#L27
- test_etl_automation.py#L193
- docs/数据结构与映射手册.md#L196

**预防动作**：涉及字段语义时，先对照现网源表样本与目标表实查结果，再决定测试断言，不要把业务侧 C_STORE.CODE 规则直接外推到其他表字段

---

### [2026-03-23 17:37] · task · business-rule

**触发场景**：Oracle/MySQL 销售对账差异 7%+

**错误假设**：dws_sales 仅按单据头 TOT_AMT_ACTUAL 正负归类，遗漏 TOT_AMT_ACTUAL=0 时需按行级 QTY 正负兜底的标准口径；测试记录数也未使用与 Oracle 相同的渠道过滤

**修正结论**：dws_sales 标准口径应在 TOT_AMT_ACTUAL=0 时按行级 QTY 正负兜底，并确保 Oracle/MySQL 对账使用完全一致的过滤条件与 0.5% 误差阈值

**证据**：
- etl_dws_sales.py#L50
- etl_dws_sales.py#L55
- test_etl_automation.py#L268
- test_etl_automation.py#L309

**预防动作**：遇到销售口径对账异常时，先回查仓库内标准 SQL 口径文档，再检查测试过滤条件是否与 Oracle SQL 完全一致

---

### [2026-03-23 17:06] · task · incremental-logic

**触发场景**：2026-03-23 主链重跑时 dws_inventory 与 ads_health 出现 1213/1205 锁冲突

**错误假设**：将按日覆盖的 delete+insert 拆成跨事务步骤，且在上游失败后仍继续驱动下游 ADS 计算

**修正结论**：dws_inventory 和 ads_health 的当日覆盖必须使用命名锁串行化；ads_health 的 delete+insert 必须放在同一事务内；run_etl 在 dws_sales 或 dws_inventory 未成功时应跳过 ads_health

**证据**：
- etl_dws_inventory.py#L121
- etl_ads_health.py#L313
- run_etl.py#L472

**预防动作**：以后凡是日快照覆盖型 ETL，先检查是否需要命名锁、单事务覆盖和下游依赖短路，避免重跑时删空当天数据

---

### [2026-03-23 16:27] · task · field-mapping

**触发场景**：将 dws_inventory 从 Oracle 切到 ODS 时核对 qty_valid 口径

**错误假设**：默认把 ods_fa_storage.qtyvalid 当作 dws_inventory.qty_valid 的来源

**修正结论**：当前库存快照仍应沿用 qty 作为 qty_valid 口径，因为 Oracle FA_STORAGE.QTYVALID 在现网未维护、通常为 0；切到 ODS 后也不能直接改口径

**证据**：
- docs/ETL业务逻辑说明.md:398-406; docs/数据结构与映射手册.md:519-529; etl_dws_inventory.py:31-38

**预防动作**：以后改库存链前先核对字段是否为未维护字段；ODS 化只迁移链路，不默认改变业务口径

---

### [2026-03-23 16:10] · task · business-rule

**触发场景**：ODS 刚接入主自动化链、但 DWS 尚未切换到消费 ODS 时进行文档同步

**错误假设**：把主链接入 ODS 误写成 ODS 已经与 DWS/ADS 全链打通

**修正结论**：文档只能同步已实现层级：当前仅确认 run_etl 已纳入 ods_sync；dws_sales 和 dws_inventory 仍直连 Oracle，需待第二阶段代码落地后再改来源描述

**证据**：
- run_etl.py:47-56; run_etl.py:381-391; docs/ARCHITECTURE.md; docs/ETL业务逻辑说明.md

**预防动作**：以后先区分调度接入与事实源切换两个阶段，文档同步按已实现阶段最小更新

---

### [2026-03-23 11:50] · task · copilot-agent

**触发场景**：继续推进第二阶段 custom agents 时，需要提高 agent picker 与自然语言发现的稳定性

**错误假设**：默认认为 agent 只要职责边界写得准确，description 用抽象表述即可，被 Copilot 发现的概率不会受真实提问措辞影响。

**修正结论**：对 `.github/agents/*.agent.md` 来说，description 既是说明文，也是发现面；除了职责描述外，必须补齐更贴近用户真实提问方式的触发词，例如“怎么推进”“先别改代码”“帮我补文档”“帮我看看这次改动有没有风险”，否则 agent picker 和自然语言发现都更容易出现歧义。

**证据**：
- .github/agents/planner-hefang.agent.md
- .github/agents/etl-auditor-hefang.agent.md
- .github/agents/doc-syncer-hefang.agent.md
- .github/agents/db-inspector-hefang.agent.md
- .github/agents/reviewer-hefang.agent.md

**预防动作**：后续继续扩 agent 或 prompt 时，先从真实聊天里的用户说法反推 description，而不是只写领域术语；验收重点也应包含“是否容易被看懂和选中”，而不只看文件是否存在。

### [2026-03-23 11:31] · task · copilot-hook

**触发场景**：用户反馈最新一次 Python 版 PostToolUse 和 Stop 都没有任何 warning 卡片，需要区分“hook 未执行”与“宿主未展示”

**错误假设**：默认认为只要 Python hook 返回非零退出码并把提示写到标准输出，宿主就会像 PowerShell warning 一样稳定展示卡片。

**修正结论**：当前仓库环境下，Python hook 走 `stdout + exit 1` 仍可能只落日志、不出 UI 卡片；若日志已确认命中 `warning`，但用户侧完全无卡片，应优先把提示输出切到 `stderr`，而不是先怀疑 hooks 没运行。

**证据**：
- scripts/copilot_post_edit_reminder.py
- scripts/copilot_session_close_reminder.py
- logs/copilot_post_edit_reminder.log
- logs/copilot_session_close_reminder.log

**预防动作**：后续调试 Copilot hooks 时，先用日志判断“是否执行”，再用 `stdout/stderr` 组合判断“是否可见”；Python 化只解决宿主噪音，不代表 warning 展示语义会自动等同于 PowerShell 路径。

### [2026-03-23 11:32] · task · copilot-hook

**触发场景**：Stop 改成 Python 后，真实 Copilot UI 仍出现旧 `cmd` 路径报错与 `pwsh` 风格噪音，需要同时处理 Stop 与 PostToolUse 的宿主配置滞后问题

**错误假设**：默认认为只要修改 `.github/hooks` 里的命令，当前宿主会立刻热更新到新路径，因此可以直接删除旧的 `.cmd` 或 `.ps1` 实现。

**修正结论**：在当前 Copilot hooks 试验阶段，宿主可能继续沿用旧会话中的 `pwsh` 或 `cmd` 路径；更稳妥的做法是把主实现切到 Python，同时保留旧入口作为极薄兼容包装层，把旧路径统一转发到 Python，先消除“路径不存在”和中文 stderr 噪音，再观察宿主 UI 是否完成收敛。

**证据**：
- .github/hooks/post-edit-reminder-hefang.json
- scripts/copilot_post_edit_reminder.py
- scripts/copilot_post_edit_reminder.ps1
- scripts/copilot_session_close_reminder.cmd
- scripts/copilot_session_close_reminder.py

**预防动作**：后续继续调整 Copilot hooks 命令链路时，优先采用“新主实现 + 旧入口兼容层”的双轨收口方式；只有在真实宿主已确认切到新路径后，再考虑移除旧包装层。

### [2026-03-23 11:02] · task · copilot-hook

**触发场景**：在真实 Copilot 会话中复测 Stop hook warning 的 UI 展示效果

**错误假设**：默认认为只要脚本显式设置 UTF-8，PowerShell 非零 stderr 路径下的中文 warning 文案就能在宿主 UI 中稳定正常显示。

**修正结论**：真实 Copilot UI 已能展示 `Warning from Stop hook`，但 PowerShell 非零 stderr 中文文案仍可能乱码；若当前目标是先稳定可读性，应优先把用户侧 warning 文案收敛为 ASCII，再把中文说明保留在日志、文档或后续研究中。

**证据**：
- scripts/copilot_session_close_reminder.ps1
- .github/hooks/post-edit-reminder-hefang.json
- docs/子项目资料/superpowers内化会议纪要.md

**预防动作**：后续继续试验 Copilot hooks 的 UI warning 时，把“是否显示”和“是否可读”拆成两层验收；只要宿主走的是 PowerShell 非零 stderr 路径，就优先用 ASCII 提示文案做稳定性基线。

---

### [2026-03-23 11:10] · task · copilot-hook

**触发场景**：真实 Copilot UI 中 Stop warning 已可见，但卡片里仍混入一部分 PowerShell 错误格式元信息

**错误假设**：默认认为只要把 warning 文案改成 ASCII，就能一并消除宿主展示层里的 PowerShell 错误包装噪音。

**修正结论**：ASCII 只能解决文案乱码，不能保证消除顶层 `pwsh` 命令带来的错误格式输出；若要进一步压缩卡片噪音，应优先尝试改顶层 hook 调用方式，例如增加 `cmd` 包装层，减少宿主直接消费 PowerShell 错误记录的机会。

**证据**：
- .github/hooks/post-edit-reminder-hefang.json
- scripts/copilot_session_close_reminder.cmd
- scripts/copilot_session_close_reminder.ps1

**预防动作**：后续凡是基于 PowerShell 非零退出码做 UI warning 的 hooks，都应把“提示文案编码”和“顶层命令包装噪音”视为两个独立问题分开收敛。

---

### [2026-03-23 11:18] · task · copilot-hook

**触发场景**：将 Stop warning 文案改成 ASCII 并加入 `cmd` 包装层后，真实 Copilot UI 中仍保留明显的 `NativeCommandError` 风格噪音

**错误假设**：默认认为只要避开中文和直接 `pwsh` 调用，宿主就不会再把 Stop hook 的失败路径包装成 PowerShell 风格错误记录。

**修正结论**：`cmd` 只能减变量，不能保证消除宿主的 PowerShell 错误包装；如果目标是继续压缩 warning 卡片噪音，更可靠的方向是把 Stop hook 实现切到 Python，并改走标准输出加非零退出码，而不是继续围绕 PowerShell 包装层打补丁。

**证据**：
- .github/hooks/post-edit-reminder-hefang.json
- scripts/copilot_session_close_reminder.py
- docs/子项目资料/superpowers内化会议纪要.md

**预防动作**：后续为 Copilot hooks 设计提醒脚本时，若仓库本身已有稳定 Python 运行前提，优先考虑 Python 作为提醒型 hook 的实现语言，减少宿主对 PowerShell 错误语义的附加包装。

---

### [2026-03-23 10:54] · task · copilot-hook

**触发场景**：设计 Stop 收口提醒时，需要避免被工作树历史未提交改动误导

**错误假设**：默认认为 Stop hook 可以直接根据 git diff 或工作树脏状态判断本轮是否发生了有意义编辑

**修正结论**：在当前仓库里，更稳妥的最小证据是复用 PostToolUse 日志中的最近命中类型；这样可以基于真实编辑信号生成收口提醒，并显著降低历史脏改动带来的误报。

**证据**：
- scripts/copilot_session_close_reminder.ps1
- logs/copilot_post_edit_reminder.log
- logs/copilot_session_close_reminder.log

**预防动作**：后续继续扩提醒型 Stop hook 时，优先复用已存在的运行时日志或明确会话内信号，不要先退回到粗粒度的工作树脏状态判断。

---

### [2026-03-23 10:19] · task · copilot-hook

**触发场景**：排查 GitHub Copilot PostToolUse hook 已执行但聊天卡片不稳定展示 warning

**错误假设**：默认认为命中后返回退出码 0 + JSON systemMessage 就等同于稳定的 UI warning 卡片。

**修正结论**：在 VS Code Copilot 的 PostToolUse 场景下，systemMessage 虽是合法输出字段，但更可能被宿主静默消费或不稳定展示；若目标是提高用户可见的 warning，应优先试验其他非零退出码产生 non-blocking warning。

**证据**：
- scripts/copilot_post_edit_reminder.ps1#L73
- scripts/copilot_post_edit_reminder.ps1#L75
- logs/copilot_post_edit_reminder.log#L47

**预防动作**：后续设计提醒型 hook 时，先区分‘给模型/宿主注入上下文’与‘强制用户侧可见 warning’两条路径；涉及 UI 展示的试点一律同时落日志证据并做真实宿主复测。

---

### [2026-03-20 15:27] · task · incremental-logic

**触发场景**：审计并调整 dws_sales 的增量逻辑时，发现代码、契约和说明文档对水位语义描述不一致

**错误假设**：把 dws_sales_daily 描述成类似 ODS 的双水位增量，默认认为存在 MODIFIEDDATE/SETTIME 断点水位

**修正结论**：当前 dws_sales_daily 的真实实现是按 business date 窗口 DELETE+INSERT 的滚动回刷；若要补偿晚到修改，应通过扩大日期回刷窗口或改为消费 ODS，而不是口头宣称已有双水位。

**证据**：
- etl_dws_sales.py#L124
- etl_dws_sales.py#L171
- run_etl.py#L383
- docs/DATA_CONTRACTS.md#L289
- docs/数据仓库与ETL手册.md#L294

**预防动作**：后续审计 DWS/ADS 增量逻辑时，先区分‘独立断点水位’和‘业务日期窗口重刷’两种模式；文档必须直接跟代码实现对齐，不能沿用 ODS 术语。

---

### [2026-03-20 12:04] · task · field-mapping

**触发场景**：拿到 hfsy 真实连接后，对 t_member_bind_info 的 *1 列和 DecryptionTags 做全表补证

**错误假设**：仅根据 SHOW CREATE TABLE 推断 *1 列可直接作为现成明文字段使用，没有继续验证真实覆盖率

**修正结论**：DDL 中存在 platAccount1、bindMobile1、birthday1、gender1、name1 等列，并不代表当前数据已回填；2026-03-20 全表统计显示这些列和 DecryptionTags 在 hfsy 当前快照中均为全空，第一阶段应回到密文字段加本地 AES 解密主路径。

**证据**：
- reports/hfsy_bind_coverage_by_plat.json
- reports/hfsy_probe_stage2.json
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L33

**预防动作**：后续遇到外部接入表时，字段是否可用必须同时验证结构和覆盖率；只看 DDL 只能证明列存在，不能证明链路可直接依赖。

---

### [2026-03-20 11:38] · task · doc-sync

**触发场景**：用户要求将 hfsy 连接上下文同步到文档，并直接给出真实数据库密码

**错误假设**：把用户在会话中给出的真实密码直接写回 git 跟踪文档，或在示例命令中复现明文密码

**修正结论**：可以同步 host、port、database、user 等连接事实，但真实密码只能保留为会话事实，文档与脚本中一律改为环境变量或本地安全注入占位。

**证据**：
- docs/HFSY数据字典.md#L10
- docs/RUNBOOK.md#L58
- docs/RUNBOOK.md#L64
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L59

**预防动作**：后续只要用户提供真实连接密码，先把仓库安全约束前置，再将文档落盘范围限制为非敏感元信息和注入方式说明。

---

### [2026-03-20 09:50] · task · field-mapping

**触发场景**：收到数云 xlsx 与 hfsy 实库连接信息后执行第2轮实表校正

**错误假设**：把标准方案中的 12 张 fdi_* JSON 表和 MySQL 8.0+ 建议，当成当前真实落库结构与硬前提

**修正结论**：当前真实来源库为 hfsy，版本为 MySQL 5.7.42，核心表为 t_member_info、t_member_bind_info、t_trade、t_order、t_pin_xid_rel、sys_area；第一阶段实现应优先消费 t_member_bind_info 中现成的 *1 解密列，仅在缺失时回退本地 AES 解密。

**证据**：
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L24
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L31
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L42

**预防动作**：后续审计外部接入时，必须区分标准方案、字段字典、真实实表三层证据；只有真实实表可以直接驱动开发设计，推荐版本不能当成当前环境硬门槛。

---

### [2026-03-19 18:11] · user-feedback · path

**触发场景**：用户澄清公司开发环境的数据库职责边界

**错误假设**：默认假设存在内部DBA/运维或其他数据库同事，可协助导出CRM实表证据

**修正结论**：当前公司开发环境下只有用户一人负责数据库；Oracle在阿里云，MySQL和hefang_dw项目在公司服务器虚拟机上由用户一手搭建。后续需要真实CRM结构或样本时，应先向用户索取其可直接导出的材料；若当前环境不存在该对象，再建议向数云方索取。

**证据**：
- .github/copilot-instructions.md#L18
- AGENTS.md#L18
- .claude/CLAUDE.md#L53

**预防动作**：以后涉及实表、样本、SHOW CREATE TABLE时，先判断本地环境是否存在目标对象；若不存在，不再建议找假定存在的内部同事，而是转向用户本人或外部对接方。

---

### [2026-03-19 18:35] · task · business-rule

**触发场景**：数云CRM第1轮 12 表字段级仲裁

**错误假设**：默认 12 张表都能直接从仲裁文档推出完整的会员匹配方案。

**修正结论**：只有 `fdi_member_info`、`fdi_jos_pin_xid` 与订单链路文档足够支撑第一阶段设计；`fdi_member_point_his`、`fdi_member_grade_his`、`fdi_refund`、`fdi_rate` 仍缺少稳定会员映射证据，必须等真实 `shuyun_ods` 实表或样本再闭环。

**证据**：
- docs/子项目资料/业务数据推送数据库标准方案.md#L17
- docs/子项目资料/业务数据推送数据库标准方案.md#L77
- docs/子项目资料/业务数据推送数据库标准方案.md#L129
- docs/子项目资料/业务数据推送数据库标准方案.md#L468
- docs/子项目资料/跟数云方沟通同步的问题.md#L11
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L47

**预防动作**：后续做外部系统接入审计时，先按“可直接设计 / 待实表验证 / 文档本身缺口”三类拆表，不要把所有文档对象都当成同样成熟。

---

### [2026-03-19 18:10] · task · doc-sync

**触发场景**：再次审计数云CRM实施计划

**错误假设**：把不存在于工作区的 R10 文档当作证据来源，并把固定加密协议做成可配置项；同时误判 `.env.example` 缺失。

**修正结论**：工作区审计只能引用仓库内真实存在的证据文件；当前仓库已存在 `.env.example`，CRM 仅需扩展；`AES-128-ECB-PKCS5Padding + Base64` 是固定协议，不应再暴露 `SHUYUN_AES_MODE`、`SHUYUN_AES_KEY_ENCODING` 之类运行时开关。若数云标准方案与沟通确认单冲突，以沟通确认单为最终仲裁。

**证据**：
- .env.example#L1
- docs/子项目资料/跟数云方沟通同步的问题.md#L5
- docs/子项目资料/业务数据推送数据库标准方案.md#L17
- docs/子项目资料/数云CRM实施上下文与下一步执行入口.md#L21

**预防动作**：后续审计外部接入方案时，先核对“证据文件是否真实存在于工作区”，再区分“固定协议”与“可配置参数”，避免把协议常量设计成环境变量。

---

### [2026-03-19 17:31] · task · business-rule

**触发场景**：数云CRM实施计划交叉审计

**错误假设**：默认认为京东业务表plat_account可直接用于会员关联，且加密输入格式只有一种

**修正结论**：京东业务表plat_account实际为pinid，关联会员前必须先做pin→xid映射；加解密实现必须兼容裸Base64密文和可能存在的~{cipher}~{version}~包裹格式；数云默认按update_time每小时同步一次，ODS建议MySQL 8.0+。

**证据**：
- docs/子项目资料/跟数云方沟通同步的问题.md#L7
- docs/子项目资料/跟数云方沟通同步的问题.md#L10
- docs/子项目资料/跟数云方沟通同步的问题.md#L15
- docs/子项目资料/敏感数据加密规则.md#L10
- docs/子项目资料/敏感数据加密规则.md#L18

**预防动作**：后续实现CRM ETL前，先以仲裁材料覆盖方案假设，并在计划或代码注释中明确写出京东mapping、加密兼容策略和调度频率。

---

### [2026-03-18 14:51] · task · mcp

**触发场景**：VS Code 已重载但当前聊天仍看不到 MCP 工具

**错误假设**：误以为只要仓库根目录存在 .mcp.json 并重载窗口，当前会话就会自动获得 mcp__mysql__/mcp__oracle__ 工具

**修正结论**：.mcp.json 只负责宿主级配置；MCP server 能启动不等于当前聊天会话工具面已刷新。若本轮会话创建时未挂载 MCP 工具，通常需要新开聊天会话重新注册工具。

**证据**：
- .mcp.json#L1
- .claude/settings.json#L1

**预防动作**：区分三层：配置文件存在、server 可启动、当前会话工具已暴露。测试 MCP 时三层都要分别验证。

---

### [2026-03-18 14:40] · task · field-mapping

**触发场景**：Oracle 侧查询“近期销量最好的 3 个产品”时，第一次查询报 ORA-00904。

**错误假设**：误以为 `M_PRODUCT` 存在 `NAME_CN` 字段可直接作为商品名称。

**修正结论**：本仓库已确认的真实映射以 [etl_dim_product.py](etl_dim_product.py#L31) 为准，`M_PRODUCT.NAME` 对应 `product_code`，`M_PRODUCT.VALUE` 对应 `product_name`。

**证据**：
- [etl_dim_product.py](etl_dim_product.py#L33)
- [etl_dim_product.py](etl_dim_product.py#L34)
- [tools/query_data.py](tools/query_data.py#L1)

**预防动作**：凡是 Oracle 真实字段查询，先对照现有 ETL 抽取 SQL、快照或字段映射文档，禁止凭字段命名习惯猜测列名。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.19 | 2026-06-25 | 新增门店日报同店同比在月中快闪合并后需将去年同期分母截到合并前一天的业务规则经验 |
| v1.18 | 2026-06-22 | 新增 Tableau 字段汉化脚本应回退识别 datasource 且缺失 root column 需按目标分组全集补列的经验 |
| v1.17 | 2026-06-19 | 新增负责人共同考核同月过渡允许 STORE 与 SUBJECT 并存且仅告警的业务规则经验 |
| v1.16 | 2026-06-01 | 新增负责人表同比与门店明细总计需联动对齐顶部 KPI same-store 口径的经验 |
| v1.15 | 2026-06-01 | 新增 popup_scope 过滤免税时应使用 LEFT JOIN 保留属性缺失快闪店的经验 |
| v1.14 | 2026-06-01 | 新增销售日报同店同比 / 同店+快闪同比必须排除免税门店的业务口径经验 |
| v1.13 | 2026-04-23 | 新增销售主题 ADS 应保持 report_channel_type 明细粒度、不要继续物化 全国/全部 总盘行的业务纠偏经验 |
| v1.12 | 2026-04-17 | 新增 category_health_tag 在库存粒度未对齐前不得提前物化到 ads_sku_daily 的经验 |
| v1.11 | 2026-04-15 | 新增用户纠正门店日报目标 NAS 根目录的路径经验 |
| v1.10 | 2026-04-15 | 更新门店日报目标 NAS 根目录经验为 目标配置表，并统一当前目录与命名真值 |
| v1.9 | 2026-04-09 | 补充 dabo_ready 主线切换时需将调度就绪判定与 ads_health 真实消费源解耦的经验 |
| v1.8 | 2026-04-09 | 补充达播订单标签 canonical_system_order_id 保守归一规则经验，明确 exact_hit 优先与同文件唯一 superset 候选约束 |
| v1.7 | 2026-04-09 | 补充统一 Excel 中逗号拼接 system_order_id 应先按 ODS 原串命中验证、不可先验拆分的经验 |
| v1.6 | 2026-04-08 | 补充门店日报渠道模型应以细分类为真值、粗分类应派生或生成列承接的字段语义经验 |
| v1.5 | 2026-04-08 | 补充达播统一 Excel 主线应优先构建订单标签表、而非先做金额字段兼容的业务纠偏经验 |
| v1.4 | 2026-04-07 | 新增 ods_m_retail 在线 ETL 漏抽 oms_sourcecode、导致增量删窗重灌后字段回空的排障经验 |
| v1.3 | 2026-04-03 | 补充门店目标导入标准门店名匹配经验，并更新 NAS 导入路径条目为已实现且已验证 |
| v1.2 | 2026-03-19 | 补充数云CRM 12 表字段级仲裁经验，明确哪些表已可设计、哪些仍需实表验证 |
| v1.1 | 2026-03-19 | 补充数云CRM实施计划再审计经验，明确证据优先级与固定协议不应配置化 |
| v1.0 | 2026-03-18 | 新增 Agent 经验台帐与首条字段映射经验 |