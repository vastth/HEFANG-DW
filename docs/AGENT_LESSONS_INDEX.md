# AGENT_LESSONS_INDEX.md — Agent 经验台帐索引

> 自动生成文件。用于先按关键词定位经验，再定向读取 `docs/AGENT_LESSONS.md` 具体条目，避免整篇经验台账进入常规上下文。
> 生成时间：2026-07-13 12:04:38；索引条目：298 / 298。

## 使用方式

1. 先按关键词、分类或触发场景搜索本索引。
2. 命中后再按 `原文行号` 定向读取 `docs/AGENT_LESSONS.md` 的对应条目。
3. 不要把完整 `docs/AGENT_LESSONS.md` 作为常规上下文读取。

## 索引

| 日期 | 分类 | 关键词 | 触发场景 | 原文行号 |
|---|---|---|---|---:|
| 2026-07-13 12:04 | business-rule | business-rule, oracle, sales | 门店日报同店资格从去年同期销售额改为开业日期 | 36 |
| 2026-07-05 11:51 | etl-operations | etl-operations, oracle | NAS 已修正门店生效开始日，但总控仍因负责人快照 unexpected entity 失败 | 52 |
| 2026-07-02 13:12 | tableau-rt119-store-cod… | tableau-rt119-store-code-fx, sales, bus… | 销售部自动化日报.twb 需要把澳门伦敦人店 RT119 的销售额按固定汇率 0.84 折算为 RMB，且不改 w… | 72 |
| 2026-07-01 11:51 | tableau-sql-scope | tableau-sql-scope, oracle, sales, busin… | 修复 销售部自动化日报-Old.twb 负责人汇总同店同比与 KPI 漂移 | 86 |
| 2026-07-01 10:40 | business-rule | sales, business-rule, performance, agen… | 用户更正销售日报同店同比应统一走含免税冻结口径 | 104 |
| 2026-07-01 10:11 | tableau-sql-scope | tableau-sql-scope, sales | 修复 销售部自动化日报 KPI05 同店同比 6.14% 异常 | 121 |
| 2026-06-25 00:00 | business-rule | sales, business-rule | 用户指出北京国贸店经营体在 `2026-06-15` 起由主体店与快闪店合并后，ERP 销售终端转移到快闪店，主体… | 138 |
| 2026-06-22 17:53 | tableau-xml | tableau-xml, oracle, sales, inventory,… | 修复 伯俊Oracle数据建模.twb 剩余英文库存字段与语义文件夹归类时，批量脚本首次运行失败且库存字段仍未显示… | 155 |
| 2026-06-22 13:41 | sales-scope | sales-scope, sales, business-rule | 用户重开 `销售部自动化日报.twb` 后指出，`销售趋势分析_日销售趋势` 从 `2026-06-16` 起少了… | 174 |
| 2026-06-19 01:00 | business-rule | oracle, business-rule | 用户指出负责人快照在共同考核 2026-06-18/19 生效切换窗口内，不应因 STORE 与 SUBJECT… | 189 |
| 2026-06-18 16:35 | field-semantics | field-semantics, sales, business-rule | 门店销售专题报错 `门店ID 不是合法整数：RT050`，用户明确指出 `门店考核归属` sheet 里的 `门店… | 206 |
| 2026-06-17 11:20 | tableau-xml | tableau-xml, oracle, performance | Tableau 字段文件夹已生效但字段窗格仍显示英文内部名 | 222 |
| 2026-06-17 11:05 | tableau-xml | tableau-xml, oracle, performance | 批量给 Tableau twb 顶层 column 标签补写 caption 属性 | 239 |
| 2026-06-16 15:54 | tableau-twb | tableau-twb, oracle, mysql, powershell | 复制旧伯俊建模 workbook 生成 clean 新建版时，需要把旧的辅助 datasource 和 cfg 快… | 255 |
| 2026-06-16 14:12 | tableau-twb | tableau-twb, oracle | 用户在 `伯俊Oracle数据建模.twb` 中已经把 sales 和 calendar 的关系改成 `BILL_… | 271 |
| 2026-06-12 16:45 | tableau-twb | tableau-twb, oracle, sales, business-ru… | 修复 HEFANG门店实时销售战情看板时，门店明细页的同店本期月销列虽然公式已切到实时口径，但显示值仍停在快照，且… | 288 |
| 2026-06-12 16:26 | tableau-sql-modeling | tableau-sql-modeling, sales, performance | 用户明确要求门店维/商品维/销售事实以 Tableau 星型模型落地，销售事实默认全量并通过 Extract 控制… | 306 |
| 2026-06-12 15:55 | tableau-twb | tableau-twb, sales | 用户重开 HEFANG门店实时销售战情看板 后，实时战情_门店实时销售明细 的 度量名称 筛选器出现红感叹并提示筛… | 323 |
| 2026-06-12 15:03 | tableau-twb | tableau-twb, oracle, mysql, sales | 用户指出 HEFANG门店实时销售战情看板 的 门店实时销售明细表 中 同店本期月销_辅助、月达成率、同店同比 仍… | 340 |
| 2026-06-12 14:36 | business-rule | oracle, sales, business-rule | 用户核对 `HEFANG门店实时销售战情看板.twb` 时指出，顶部“今日0销售门店数”“进度落后门店数”都多算了… | 357 |
| 2026-06-11 15:09 | tableau-twb | tableau-twb | 用户在 SKU 生命周期看板的 Tableau 数据模型页反馈 `sales_sku_daily` 与 `sku_… | 377 |
| 2026-06-08 10:31 | business-rule | business-rule, sales | 北京国贸 2026-06-07 月累计销售额差额追查时，用户要求从根上避免新品类因漏配被排除 | 395 |
| 2026-06-06 09:15 | retirement-governance | retirement-governance, sales, doc-sync | 退役销售专题 ADS 并收口专题调度/文档/SQL | 415 |
| 2026-06-03 09:43 | business-rule | sales, business-rule | 用户要求按线上销售月报SQL3.0口径扩展门店范围并保留全部RT门店 | 432 |
| 2026-06-01 13:45 | tableau-kpi | tableau-kpi, sales, business-rule | 顶部 KPI 的 `同店同比 / 同店+当期快闪同比` 已修正为 `2.2% / 4.5%` 后，用户继续要求区域… | 448 |
| 2026-06-01 12:25 | tableau-kpi | tableau-kpi, sales, business-rule | 在 `销售部自动化日报.twb` 中为 same-store KPI 排除免税门店后，`同店同比` 回到 `2.2… | 465 |
| 2026-06-01 12:13 | business-rule | sales, business-rule | 用户核对 `销售部自动化日报.twb` 的 2026-05-31 顶部 KPI，指出业务口径应为“同店同比 2.2… | 480 |
| 2026-06-01 10:43 | etl-window | etl-window | ads_sku_daily 同时承接 MTD 与滚动30天窗口，且 report_date 落在 31 天月份月末 | 495 |
| 2026-06-01 10:13 | reporting-ops | reporting-ops, sales, business-rule | 2026-06-01 电商销售日报表『管道输出』在 6-1 跨月时，绿色月累计区多列直接变成 0。 | 512 |
| 2026-06-01 09:24 | etl-operations | etl-operations, oracle, sales | 2026-06-01 09:19 总控 V2 中，门店销售专题在修复 target_month 跨月门禁后，仍在… | 528 |
| 2026-06-01 09:07 | etl-operations | etl-operations, sales | 2026-06-01 00:05 总控 V2 中，门店销售专题在 previous-day 自动模式下没有处理 2… | 546 |
| 2026-05-27 13:20 | business-rule | business-rule, sales | 用户同步门店负责人真实业务变化：原负责人 Gloria 已离职，当前暂未任命新负责人，业务先用 NEW 作为临时负… | 564 |
| 2026-05-26 15:21 | business-rule | business-rule, mysql, sales | 用户明确同步门店销售明细总和的免税 KPI 口径 | 582 |
| 2026-05-26 10:34 | etl-diagnostics | etl-diagnostics, oracle, mysql, sales,… | 总控 V2 手动运行后，`test_etl_automation.py` 的 `dws_sales_daily`… | 599 |
| 2026-05-26 09:57 | business-rule | business-rule | 用户指出 is_duty_free 判断还应看门店渠道类型是否包含免税，例如 联营-免税 | 617 |
| 2026-05-26 09:47 | schema-migration | schema-migration, mysql | 免税月累计 dry-run 在 RT110 修正后报 Unknown column target_month | 634 |
| 2026-05-26 09:43 | business-truth | business-truth | 用户确认 RT110 / 杭州萧山国际机场店确实属于免税门店 | 651 |
| 2026-05-26 09:40 | field-semantics | field-semantics | 免税月累计 Excel 业务真值中 门店ID 列实际填写 RT 门店编码，且存在维表免税标记不一致 | 668 |
| 2026-05-26 09:17 | field-semantics | field-semantics, doc-sync | 用户指出免税月累计 Excel 首列 reportdate 应改为 目标月份 | 685 |
| 2026-05-26 00:12 | tableau-twb | tableau-twb, oracle | 用户在 Tableau 数据模型页看到 `sales.csv -> calendar.csv` relations… | 703 |
| 2026-05-25 18:05 | tableau-twb | tableau-twb, business-rule | 用户在 `HEFANG复刻.twb` 中发现 `99.月份标签 = LEFT(DATENAME('month',[… | 718 |
| 2026-05-25 17:51 | business-rule | business-rule, sales | 免税门店只有外部月累计销售额，需要接入门店日报专题调度与 Tableau 总盘 | 733 |
| 2026-05-25 17:18 | field-mapping | field-mapping, oracle, sales | 用户指出 `HEFANG复刻.twb` 当前 `product_code` 仍是 SPU 条码粒度，要求改成 SK… | 750 |
| 2026-05-22 16:00 | business-rule | business-rule, sales | 用户指出 2026-05-21 销售日报 KPI05 业务上应为 `-1.89%`，但 `ads_store_da… | 766 |
| 2026-05-22 13:58 | field-mapping | field-mapping, sales, business-rule | 用户明确否定“直接改销售日报 workbook XML 让同比率对齐”的路线，要求先回退 workbook，再把… | 783 |
| 2026-05-21 12:00 | tableau-twb | tableau-twb, sales, business-rule | 用户指出 HEFANG实时销售战情看板 中 今日达成率 与 线性进度偏差 同屏不一致，截图里 5.8% 与 14.… | 801 |
| 2026-05-21 11:44 | tableau-twb | tableau-twb, sales | 用户指出 HEFANG 实时战情看板的 实时战情_今日累计销售进度 到 11 点仍接近 0，问题集中在 今日累计销… | 819 |
| 2026-05-21 11:25 | tableau-twb | tableau-twb, sales, business-rule | 用户在回看 `HEFANG门店实时销售战情看板.twb` 的 `实时战情_今日累计销售进度` 图后，明确指出上一轮… | 838 |
| 2026-05-21 11:07 | tableau-twb | tableau-twb, oracle, sales, performance | 用户在 `HEFANG门店实时销售战情看板.twb` 中发现，10:45 左右左上角 `今日实时销售额` 只有几千… | 853 |
| 2026-05-20 13:51 | tableau-twb | tableau-twb, sales | 销售日报 Tableau 门店经营明细启用底部总计后，用户发现 KPI 卡片与明细总计比例不一致 | 868 |
| 2026-05-20 13:15 | tableau-twb | tableau-twb, sales | 用户在核对 `销售部自动化日报.twb` 的“销售贡献占比”饼图时，发现勾选“其他”后会额外冒出 6.7%，并进一… | 884 |
| 2026-05-14 15:00 | etl-architecture | etl-architecture, oracle, business-rule | 用户明确指出，`dim_store` 不应再因为 Oracle `C_STORE.ISACTIVE='N'` 就被… | 901 |
| 2026-05-14 13:40 | tableau-schema | tableau-schema | 吸收 `tableau/tableau-document-schemas` 官方 XSD 时，`xmlschema… | 917 |
| 2026-05-14 13:13 | tableau-twb | tableau-twb, sales | 用户首次重开 HEFANG 门店实时销售战情看板后回传截图，顶部摘要卡空白、时间进度只剩标题，6 张 KPI 卡出… | 935 |
| 2026-05-14 13:01 | tableau-twb | tableau-twb, mysql, sales, business-rule | 为 HEFANG 门店实时销售战情看板补页头与 KPI 首屏时，准备复用 销售部自动化日报.twb 的 KPI 文… | 953 |
| 2026-05-14 11:47 | tableau-twb | tableau-twb, oracle, sales | HEFANG门店实时销售战情看板.twb 可打开后，实时战情_分时销售 中的中文字段全部出现红色感叹号，Table… | 971 |
| 2026-05-14 11:39 | tableau-twb | tableau-twb, sales | 在 HEFANG门店实时销售战情看板.twb 中，把 shelf-sorts 改成 computed-sort 后… | 989 |
| 2026-05-14 11:33 | tableau-twb | tableau-twb, sales | 在 HEFANG门店实时销售战情看板.twb 中新增排行 worksheet 后，为 rows 排序直接照搬其它… | 1006 |
| 2026-05-14 11:08 | tableau-twb | tableau-twb, oracle, mysql, sales | 为 HEFANG门店实时销售战情看板.twb 从空白 workbook 骨架注入 Oracle/MySQL dat… | 1022 |
| 2026-05-14 09:30 | cutover-runtime | cutover-runtime, inventory, performance | 2026-05-14 00:05 Windows 自动触发 `run_scheduled_total_contro… | 1039 |
| 2026-05-13 09:42 | cutover-runtime | cutover-runtime, sales, inventory | 2026-05-13 用户将 Windows 计划任务入口切到 run_scheduled_total_contr… | 1058 |
| 2026-05-12 17:50 | etl-architecture | etl-architecture, inventory, performance | 总控 V2 双跑 gate 中 DWS v2 shadow 主体对账为 0 mismatch，但 ads_inve… | 1077 |
| 2026-05-12 16:43 | tableau-twb | tableau-twb, sales, business-rule, doc-… | 用户追问销售日报‘去年同期同比’为何与业务口径的同店同比 8.5% 不一致，并要求同时修正明细表字段语义 | 1096 |
| 2026-05-12 16:33 | cutover-runtime | cutover-runtime | 准备两次总控 V2 gate 时发现 run_scheduled_total_control.bat 调用 sch… | 1114 |
| 2026-05-12 15:44 | etl-architecture | etl-architecture, sales, inventory | 为主链新增 cutover_mode 与 rollback_to_legacy 后，专题门店日报的 freshne… | 1132 |
| 2026-05-12 14:39 | scd2-interval | scd2-interval, performance | 对 RT117 做真实 dry-run 时，虽然 `earliest_history_effective_star… | 1152 |
| 2026-05-12 14:09 | downstream-compatibility | downstream-compatibility, mysql | 用户明确约束影子链后续若替代旧链，ADS 相关 MySQL 表仍会被 Tableau 和其他下游继续消费。 | 1173 |
| 2026-05-12 14:05 | business-rule | business-rule | RT117 属于今天补录但需从 2026-05-09 起生效的负责人变更，且业务明确只能通过 Excel 负责人映… | 1188 |
| 2026-05-12 13:32 | etl-validation | etl-validation, inventory, performance | inventory same-snapshot 已确认不可作为 ads_inventory_health gate… | 1211 |
| 2026-05-12 11:49 | etl-validation | etl-validation, inventory, business-rul… | 复核 scheduled_dws_v2_shadow.py --inventory-align-with-old-… | 1231 |
| 2026-05-12 11:33 | etl-validation | etl-validation, inventory, performance | 用户执行新一轮 scheduled_dws_v2_shadow.py 后复核 ads_inventory_heal… | 1252 |
| 2026-05-12 10:33 | etl-validation | etl-validation, sales, inventory, perfo… | 补一轮 ads_inventory_health 下游输入只读对账时，发现近期 dws_v2_shadow 和旧链… | 1275 |
| 2026-05-12 09:59 | tableau-twb | tableau-twb, sales | 上一轮已经把顶部 7 张 KPI 卡的字体统一为固定蓝色，但用户重开销售日报后反馈 `去年同期同比` 仍显示橙色，… | 1292 |
| 2026-05-12 09:53 | tableau-twb | tableau-twb, sales | 用户在销售日报 Tableau 客户端确认，7 张 KPI 卡的趋势文案不应显示“较上期”，当前业务展示语义应改成… | 1310 |
| 2026-05-12 09:00 | business-rule | business-rule, oracle, sales, agent-con… | 用户明确确认：RT105 昆明顺城购物中心店在 2026-05-08 后闭店，2026-05-09 起由使用新伯俊… | 1327 |
| 2026-05-11 18:15 | etl-architecture | etl-architecture | 用户明确指出：月客流要进入 ADS，但必须走 ODS - DWD - DWS - DIM - ADS 完整 API… | 1344 |
| 2026-05-11 18:49 | tableau-twb | tableau-twb, sales, agent-context | 用户在 Tableau 客户端重开销售日报后指出，KPI 趋势文案必须放到文本中才能显示，且 `KPI06_目标缺… | 1362 |
| 2026-05-11 15:44 | external-api-store-mapp… | external-api-store-mapping, doc-sync | 打通万店掌 `mobileLogin -> getDepartments -> 客流接口` 首轮真实链路后，需要判… | 1380 |
| 2026-05-11 13:21 | etl-validation | etl-validation, oracle, sales, business… | 排查 2026-05-11 中午门店日报专题 71/70 与 72/71 行数不一致失败时，发现 ads_stor… | 1396 |
| 2026-05-11 12:59 | external-api-auth | external-api-auth | 实测万店掌 mobileLogin 与 getDepartments 首次联调时，默认把开放平台控制台口令和 ti… | 1414 |
| 2026-05-11 12:28 | external-api-permission | external-api-permission | 在万店掌编辑APP页面为 tableau_bi 扩权时，尝试按数据域整类勾选以扩大探测面 | 1431 |
| 2026-05-11 12:04 | external-api-auth | external-api-auth, doc-sync | 万店掌公开文档与SDK对 authenticator 来源描述冲突，用户拿到外部技术回复后需要重新收口 | 1447 |
| 2026-05-09 14:30 | tableau-twb | tableau-twb, sales, inventory, business… | 对销售部自动化日报.twb 的 ds_ads_store_daily_report_basic 做 calcula… | 1464 |
| 2026-05-08 17:08 | tableau-twb | tableau-twb | 复核 Opus 第二轮 TWB+PNG 联合学习 12 条建议并决定哪些应写入长期 Skill/知识库 | 1481 |
| 2026-05-08 15:37 | tableau-twb | tableau-twb, performance | 用户将 14 份 Tableau twb 与同名 PNG 效果图放入 example 目录，要求结合可视化效果图再… | 1499 |
| 2026-05-08 15:15 | tableau-twb | tableau-twb, performance | 用户要求学习14份Tableau twb并沉淀为后续编译twb的Skill | 1517 |
| 2026-05-08 10:35 | sql-quality | sql-quality, sales, business-rule | 总控门店销售专题链在 ads_backfill 阶段连续失败，首错为 1052 ambiguous store_id | 1535 |
| 2026-05-08 10:14 | etl-validation | etl-validation, sales | 评估 ads_sales_org_monthly 是否需要跟随销售主题 ADS 收口时，发现 RT116 在 20… | 1552 |
| 2026-05-08 09:57 | business-rule | sales, business-rule | 排查门店销售专题 71 vs 72 告警时，发现 ads_store_daily_report 已按 report… | 1570 |
| 2026-05-08 09:27 | etl-architecture | etl-architecture, inventory | 追查 2026-05-08 00:18 左右 total-control 中 inventory shadow 的… | 1589 |
| 2026-05-07 17:23 | business-rule | business-rule, mysql, sales | 排查 2026-05-07 门店日报 ads_backfill 因月中新店缺负责人切片报错时，确认 ERP 已建档… | 1610 |
| 2026-05-07 15:51 | docs-tooling | docs-tooling, business-rule, doc-sync | 为 scheduled_dws_v2_shadow.py 完成文档同步后复扫 doc-sync，发现报告仍保留来自… | 1629 |
| 2026-05-07 14:49 | etl-reconciliation | etl-reconciliation, inventory | 为库存 DWS v2 进入 S4 shadow run 固定 old DWS 同时点对账时，需要把 source_… | 1646 |
| 2026-05-07 14:00 | etl-validation | etl-validation, inventory, performance | DWS v2 S3 实跑验收时，库存 DWD→v2 mismatch 为 0，但 v2→旧 DWS 仍有 200… | 1665 |
| 2026-05-07 10:10 | etl-reconciliation | etl-reconciliation, oracle, inventory,… | M3 库存 full raw 初始化后，dwd_inventory_storage_snapshot 与 dws_… | 1682 |
| 2026-05-07 09:24 | field-mapping | field-mapping | 用户说明 5 月仅用 v1，门店等级由 NAS 目标配置表新增列维护，负责人空值正常 | 1698 |
| 2026-05-06 10:32 | etl-architecture | etl-architecture | 用户明确要求门店属性录入面不再直接维护SCD2历史，而改为业务只维护当前快照 | 1718 |
| 2026-05-06 09:26 | business-rule | oracle, sales, business-rule | 查看 20260502-20260506 总控日志时，连续发现门店销售专题在 import 阶段因配置表门店未命中… | 1735 |
| 2026-04-30 17:30 | etl-reconciliation | etl-reconciliation, timeout, sales, inv… | M3 raw/DWD 近 1 天小窗口真实装载后执行最小对账时，DWD 行数与 raw 一致但与现有 DWS 完整… | 1753 |
| 2026-04-30 14:45 | schema-verification | schema-verification, mysql, inventory,… | 用户反馈 M3 的 5 个 raw ODS / DWD DDL 已人工建表后，需要把此前“未执行 / 未建表”的草… | 1771 |
| 2026-04-30 13:57 | field-selection | field-selection, oracle, inventory, per… | M3 raw/DWD 草案字段筛选时，用户明确指出全量为 0 的 Oracle 模板字段不应进入新架构 | 1792 |
| 2026-04-30 10:23 | field-semantics | field-semantics, performance, doc-sync | M3 raw/DWD DDL 草案中大量字段注释写作语义待确认，用户提供 AD_COLUMN 零售单字典和 FA_… | 1812 |
| 2026-04-29 17:29 | doc-sync | mysql, doc-sync | 输出 raw ODS / DWD 草案对象后，首次 doc-sync 复扫将 raw 表名识别为 code_onl… | 1832 |
| 2026-04-29 16:35 | etl-architecture | etl-architecture, timeout, inventory | 输出 DWD DDL 草案与旁路 ETL 骨架时，尚未有 raw ODS 表、DWD 表和用户写库授权 | 1850 |
| 2026-04-29 16:02 | source-profiling | source-profiling, oracle, doc-sync | 探索 Oracle BOSNDS3 字段启用率并规划 ODS/DWD 时，需要判断哪些字段实际启用、哪些可能废弃 | 1867 |
| 2026-04-29 15:46 | sales-theme-order-count | sales-theme-order-count, sales, doc-sync | 审计 4 张销售专题 ADS 后继续落改时，发现‘统一到门店日报口径’在不同粒度下有两种实现方式 | 1883 |
| 2026-04-29 15:41 | etl-architecture | etl-architecture, sales, inventory, bus… | 复核 ODS-DWD-DWS-ADS 架构完善 M2 草案时，用户要求从首席数据官和数据架构师视角选择长期最优 D… | 1903 |
| 2026-04-29 14:54 | business-rule | sales, business-rule | 排查餐具补配后门店日报专题单数仍有 1 / 2 差异时，发现杭州嘉里与广州天汇的根因不同 | 1920 |
| 2026-04-29 13:29 | doc-sync | mysql, sales, inventory, doc-sync | 修订 ODS-DWD-DWS-ADS 子项目根文档来源描述时，用户指出当前尚未构建 DWD 层，DWS/ADS 数… | 1939 |
| 2026-04-29 13:10 | business-rule | mysql, sales, business-rule, doc-sync | 门店日报专题对账发现餐具导致 MTD 金额数量差异，用户确认门店日报专题所有 ADS 应纳入餐具；业务 Excel… | 1958 |
| 2026-04-29 10:43 | agent-tooling | agent-tooling, agent-context | 运行 Agent 上下文优化烟测脚本时，报告 JSON 中的子进程中文 stdout 出现乱码替换字符 | 1976 |
| 2026-04-29 10:31 | agent-customization | agent-customization, doc-sync, agent-co… | 复查上下文压缩改造时，尝试将 .github/instructions/*.instructions.md 的 a… | 1993 |
| 2026-04-29 09:48 | agent-context | oracle, mysql, performance, agent-conte… | 讨论上下文压缩方向时，提出对 Oracle/MySQL 查询输出强制限宽限行 | 2010 |
| 2026-04-28 15:47 | mcp/path | mcp/path, dbhub, oracle, mysql, sales | 用 DBHub 执行 SQL/check_ads_sales_org_monthly_min.sql 时，当前月勾… | 2027 |
| 2026-04-28 12:51 | performance-and-busines… | performance-and-business-rule, sales, b… | 核对门店销售专题 2026-04-27 / v2 的直营差额与耗时时，发现 ads_sales_org_daily… | 2045 |
| 2026-04-28 10:07 | etl-operations | etl-operations, timeout, mysql, sales,… | 连接工厂 60 秒超时导致长跑组织汇总在总控中不稳定，需要兼顾稳定性与默认性能 | 2062 |
| 2026-04-28 09:43 | etl-operations | etl-operations, timeout, mysql, sales,… | 总控在 ads_sales_org_daily 阶段报首个错误 (0, '')，日志只剩命名锁释放失败 | 2083 |
| 2026-04-28 09:18 | business-rule | business-rule, oracle, mysql | ads_daily_sales 写入 SQL 执行时报 MySQL 1052 Column 'sales_date… | 2104 |
| 2026-04-28 08:58 | business-rule | sales, business-rule | 总控销售专题 ads_daily_sales 因 SQL 骨架缺少 sra.is_include_in_daily… | 2120 |
| 2026-04-27 18:12 | path | path, timeout, mysql | 总控调度步骤5因 check_ods_incremental 执行失败返回码1 | 2136 |
| 2026-04-27 17:44 | business-rule | business-rule, sales, doc-sync | 销售主题 ADS 已统一到门店日报权威口径，但最小对账 SQL 仍沿用旧版 dws/门店目标逻辑。 | 2151 |
| 2026-04-27 17:20 | business-rule | sales, business-rule, doc-sync, agent-c… | 销售主题ADS统一到门店日报权威口径后继续引用旧版v1/v2验证 | 2174 |
| 2026-04-27 16:16 | validation | validation | 统一 hefang_dw 连接工厂后执行项目级 compileall 语法检查 | 2192 |
| 2026-04-27 15:31 | business-rule | sales, business-rule | 排查负责人拆解与页级总盘直营差额 | 2208 |
| 2026-04-27 15:15 | business-rule | business-rule, mysql, sales, doc-sync | 核对月度战役看板负责人拆解总和是否与页级总盘一致 | 2223 |
| 2026-04-27 14:28 | business-rule | business-rule, dbhub, oracle, mysql, sa… | 用户要求按 SQL/==线上销售月报SQL_3_0.sql 重新计算 2025-04-01~2025-04-26… | 2238 |
| 2026-04-27 14:11 | etl-operations | etl-operations, oracle, sales | 同一天第2/第3次总控后，主链已刷新但门店销售专题 ADS 被判定 SKIPPED | 2256 |
| 2026-04-27 11:19 | business-rule | business-rule, mysql, sales, doc-sync | 用户明确指出月度战役左下模块不看战区粒度，而看负责人粒度 | 2275 |
| 2026-04-27 10:38 | path | path, sales | 为总控补门店销售专题执行摘要，并要求后续专题共用统一企业微信出口 | 2290 |
| 2026-04-27 09:55 | etl-operations | etl-operations, sales | 执行门店日报专题 ADS 补数时，长时间写库被终端空闲误判为已完成，并因重复触发导致 sales_org 命名锁重试 | 2310 |
| 2026-04-27 09:27 | business-rule | business-rule, sales | 排查门店日报专题链连续多天成功运行但 ADS 最新 report_date 停在旧日期 | 2328 |
| 2026-04-24 15:13 | path | path, sales | 评估销售主题 Windows 计划任务自动化时，需要决定是把专题链硬并入主链，还是只统一计划任务入口。 | 2343 |
| 2026-04-24 13:26 | field-mapping | field-mapping, sales | 开始在 Tableau 中实际搭月度战役卡片区时复核整月目标口径 | 2362 |
| 2026-04-24 13:24 | business-rule | business-rule, sales | 用户确认 Tableau 月度战役模块的草图只用作布局参考 | 2377 |
| 2026-04-24 13:18 | field-mapping | field-mapping, sales, business-rule | 评估 Tableau 月度战役指挥模块是否可直接消费 ads_daily_sales | 2392 |
| 2026-04-24 11:19 | mcp | mcp, dbhub, mysql | 扩面治理 ads dim cfg 表 comment 漂移时，DBHub 查询仍把部分 column_commen… | 2407 |
| 2026-04-24 10:57 | field-mapping | field-mapping, mysql, sales, doc-sync | 用户授权直接修正 MySQL 表备注时，发现销售主题 ADS 的现网 table_comment/column_c… | 2425 |
| 2026-04-24 10:37 | doc-sync | mysql, doc-sync | 用户明确要求以实际 MySQL 数据库为权威事实，对比并同步 MYSQL 数据字典 | 2446 |
| 2026-04-23 18:14 | data-reconciliation | data-reconciliation, oracle, mysql, sal… | 补做 2026-04-22 门店销售专题 ADS 到 Oracle 最终闭环时，发现 ADS 内部已对齐但 YTD… | 2466 |
| 2026-04-23 17:31 | etl-operations | etl-operations, sales | 门店日报专题调度批量重跑时长期出现 ads_sales_org_daily 命名锁等待 | 2487 |
| 2026-04-23 15:27 | business-rule | business-rule, sales | 用户明确要求销售主题 ADS 每张表都输出 report_channel_type 细分类，且不要再生成 area… | 2505 |
| 2026-04-23 11:48 | etl-architecture | etl-architecture, oracle, sales | 审计 2026-04-22 ADS 对 Oracle 差异时，发现 sales 看板类 ADS 偏小，而门店日报族… | 2523 |
| 2026-04-23 09:39 | etl-operations | etl-operations, sales | 修复 ads_sku_daily attach_contribution 精度后重跑 2026-04-22/v2，… | 2542 |
| 2026-04-23 08:46 | business-rule | business-rule | 执行 2026-04-22 的 scheduled_store_daily_report.py 显式重跑验证负责人… | 2559 |
| 2026-04-22 17:29 | field-mapping | field-mapping, doc-sync | 将负责人字段下沉到 ads_store_daily_report 并接入专题调度 | 2574 |
| 2026-04-22 10:16 | business-rule | business-rule, doc-sync | 负责人映射 NAS 正式文件缺少业务录入说明 | 2589 |
| 2026-04-22 10:16 | etl-architecture | etl-architecture, mysql | 主链路 ODS 处理跨窗口晚改后，第二次重跑又命中 invalid transaction 连接复用 | 2606 |
| 2026-04-22 09:20 | log-parsing | log-parsing, oracle, mysql | 排查 2026-04-22 凌晨 ODS 唯一键报错时，用户侧只看到了 SQLAlchemy 批量插入参数预览 | 2625 |
| 2026-04-21 17:53 | business-rule | business-rule | 共同考核经营体负责人快照设计冻结 | 2641 |
| 2026-04-21 15:34 | business-rule | business-rule, mysql, agent-context | 用户明确否决负责人模板由业务维护SCD2生效区间，改为NAS当前快照每日读取 | 2658 |
| 2026-04-21 14:58 | field-mapping | field-mapping, sales | 用户确认门店负责人映射模板与承接表设计约束 | 2674 |
| 2026-04-20 17:58 | etl-history-alignment | etl-history-alignment, oracle, sales | 排查 ads_daily_sales.last_year_cum_actual_amt 在 2026-04 报告日… | 2689 |
| 2026-04-17 14:43 | business-rule | mysql, sales, inventory, business-rule | 评估 ads_sku_daily 接入专题调度后，是否应顺手把 category_health_tag 物化进 S… | 2709 |
| 2026-04-17 12:55 | business-rule | business-rule, sales | ads_sku_daily 新增连带业绩贡献率并做只读样例探针校验 | 2729 |
| 2026-04-17 11:56 | sql-scope | sql-scope, oracle | 在 ads_sku_daily 中新增近 7 天/30 天滚动趋势字段并做只读运行验证 | 2748 |
| 2026-04-16 18:37 | sql-collation | sql-collation, dbhub, mysql, sales | ads_sku_daily 首次真实跑数时出现 MySQL 1267/MAX 与 1271/UNION 排序规则冲… | 2765 |
| 2026-04-16 15:41 | scheduler | scheduler | 验证 202604考核数据配置表.xlsx 的专题调度写库链路 | 2785 |
| 2026-04-16 13:43 | scheduler | scheduler, doc-sync, agent-context | 将 ads_daily_sales 接入 scheduled_store_daily_report 的受影响日期批… | 2801 |
| 2026-04-16 10:34 | sql-execution | sql-execution, dbhub, mysql, business-r… | ads_daily_sales 最小对账 SQL 首次正式执行 | 2818 |
| 2026-04-16 09:27 | business-rule | business-rule, sales | 销售看板 ads_daily_sales 首次样板设计 | 2834 |
| 2026-04-15 17:25 | sql-execution | sql-execution, oracle, mysql, sales | ads_sales_org_daily 首次正式跑单日样本 | 2854 |
| 2026-04-15 17:15 | etl-defensive-coding | etl-defensive-coding, sales | 销售看板 ads_sales_org_daily 首次 conn-test | 2871 |
| 2026-04-15 15:51 | business-rule | business-rule, mysql, sales | 用户要求把门店年标独立配置对象单独定成一份字段契约，直接冻结未来接数入口 | 2888 |
| 2026-04-15 15:10 | business-rule | business-rule, sales | 用户确认销售看板当前仍只看总年标，但要求预留门店年标入口，未来可能上看板 | 2906 |
| 2026-04-15 14:33 | path | path, sales, doc-sync | 用户要求将门店日报目标导入使用的 NAS 根目录从 月度日目标配置表 调整为 目标配置表 | 2923 |
| 2026-04-15 14:08 | business-rule | business-rule | 用户继续审核年度经营目标模板，并明确业务填写端不希望维护门店编码。 | 2940 |
| 2026-04-15 14:06 | business-rule | business-rule | 用户审核年度经营目标模板并指出粒度不能停在目标范围，而必须细到每家门店。 | 2955 |
| 2026-04-15 13:38 | business-rule | business-rule, sales, doc-sync | 销售看板 ADS 基线文档已经扩展到字段契约和 DDL 草案后，继续判断是否能直接作为唯一权威资料启动逐表落地。 | 2970 |
| 2026-04-15 13:22 | data-model | data-model, sales | 销售看板6张主物理表从主题级设计继续细化到字段契约与DDL草案 | 2988 |
| 2026-04-15 12:14 | data-model | data-model, sales | 销售看板ADS基线从8张ADS方案收敛为8个展示主题对应6张主物理表 | 3004 |
| 2026-04-15 11:29 | business-rule | business-rule, sales | 复核销售看板 8 张 ADS 是否冗余，以及是做 1 张宽表还是分主题 ADS | 3020 |
| 2026-04-15 12:12 | doc-sync | sales, business-rule, doc-sync | 按当前实际代码同步仓库与销售部数据治理文档时，发现历史规划对象、设计阶段描述与现网门店日报专题链路被混写。 | 3035 |
| 2026-04-15 09:56 | path | path, oracle, sales | 门店日报专题调度通过子进程调用目标导入脚本，且模板校验在摘要生成前失败。 | 3053 |
| 2026-04-15 09:24 | business-rule | oracle, sales, business-rule, doc-sync | 用户要求将正式 ads_store_daily_report 调整为同步业务对账侧结果，并同时修正旧 Oracle… | 3074 |
| 2026-04-14 18:18 | output-consistency | output-consistency, oracle, mysql, sale… | 业务用 ERP 导出与门店日报截图对账时，发现月累计销量/上月同期销量少 1 或 2，金额只差 0.1/0.2/0… | 3093 |
| 2026-04-14 10:02 | path | path | Windows 计划任务/脚本访问 NAS UNC 共享时因凭证丢失触发 WinError 1326 | 3110 |
| 2026-04-11 21:44 | mcp | mcp, doc-sync, agent-context | 将当前仓库的 VS Code Copilot 自定义能力架构迁移到其他项目 | 3125 |
| 2026-04-10 17:40 | business-rule | business-rule | 用户明确要求最终日报必须直接产出 ads_store_daily_report 表格式，并且不再出现深圳万象天地店… | 3143 |
| 2026-04-10 16:49 | path | path, sales, doc-sync | 用户将 NAS 门店日报目标文件重命名为 YYYYMM考核数据配置表.xlsx，并要求后续按该格式按月维护 | 3162 |
| 2026-04-10 15:38 | doc-sync | business-rule, doc-sync | 共同考核统计主体层实现后重跑 scripts/check_doc_sync.py，summary 出现大体量 do… | 3177 |
| 2026-04-10 13:34 | business-rule | business-rule | 用户补充：快闪店到了有正店的商场，也可能仍然独立考核，不能按商场自动判定合并 | 3193 |
| 2026-04-10 13:20 | business-rule | business-rule, sales | 用户补充快闪店 RT014 有时需并入同商场正店考核，有时到无正店城市需独立考核 | 3208 |
| 2026-04-10 13:07 | business-rule | business-rule, sales | 门店日报新增快闪店 RT014，与深圳万象天地店同商场，需要按同一考核渠道合并月累计与目标，且 RT014 未来可… | 3223 |
| 2026-04-10 09:28 | business-rule | sales, business-rule | 用户确认 2026-04-07 门店日报对账差异的最终业务口径 | 3238 |
| 2026-04-10 09:25 | etl-cutover | etl-cutover, inventory | 将 ads_inventory_health 达播来源从 ads_dabo_daily_sales 迁到标签主线前… | 3253 |
| 2026-04-09 16:25 | etl-architecture | etl-architecture, inventory, doc-sync | 评估 hefang_dw 去除 dabo_etl 外部依赖时，需要先判断旧链路是否仍通过 legacy 表契约挂在… | 3271 |
| 2026-04-09 16:03 | etl-scheduling | etl-scheduling | 将 run_etl.py 的 dabo_ready 从旧 ads_dabo_daily_sales 切到 ads_… | 3288 |
| 2026-04-09 15:39 | field-mapping | field-mapping, mysql | 达播订单标签正式落库后发现 2 条带 -C1/-C2 的小红书组合单在 ads_dabo_order_label… | 3306 |
| 2026-04-09 13:48 | field-mapping | field-mapping | 正式 apply 后核验 ads_dabo_order_label 与 ODS 桥接覆盖时，发现统一 Excel… | 3321 |
| 2026-04-09 11:55 | sql-runtime-source | sql-runtime-source, sales, doc-sync | 门店日报 ADS ETL 被要求改为自包含执行，不再运行时依赖外部 .sql 文件。 | 3338 |
| 2026-04-09 11:04 | business-rule | business-rule, sales, doc-sync | 门店日报 NAS 专题调度按六步图复核后，用户要求把自动模式门禁和门店属性同步补成完全一致的版本 | 3359 |
| 2026-04-09 10:34 | path | path, sales, agent-context | 为门店日报专题调度实现 ads_store_daily_report 按日期列表批量重跑 | 3378 |
| 2026-04-09 09:09 | business-rule | business-rule, sales | 为门店日报 NAS 自动导入设计受影响日期判断器 | 3393 |
| 2026-04-08 18:04 | path | path, sales | 为门店日报 NAS 目标导入补正式调度链路 | 3415 |
| 2026-04-08 17:00 | business-rule | sales, business-rule, doc-sync | 用户明确纠正 ads_store_daily_report 日订单数口径，要求每个订单按成交金额 >0/0/<0… | 3432 |
| 2026-04-08 16:19 | business-rule | mysql, sales, business-rule, agent-cont… | 用户排查 2026-04-07 门店日报对账差异，明确指出日订单数需要减去退单，并发现 146=配件 补纳后历史日… | 3451 |
| 2026-04-08 16:14 | business-rule | business-rule, sales, inventory | 用户要求将 146=配件 纳入 dim_report_product_rule，并明确只影响门店日报商品范围 | 3470 |
| 2026-04-08 14:08 | field-mapping | field-mapping, sales, doc-sync | 用户明确要求门店日报渠道类型按细分类作为最终业务真值存一列，并额外补一个可衍生的粗分类字段 | 3488 |
| 2026-04-08 13:15 | field-mapping | field-mapping, sales | 用户要求以 NAS 中的 门店渠道分类(1).xlsx 作为月度日目标配置表的门店类型权威资料 | 3505 |
| 2026-04-08 11:32 | business-rule | business-rule | 用户明确说明 20XX年0X月日目标配置表_v1.xlsx 是月内完整快照，同月日目标会变更且可能增删门店 | 3522 |
| 2026-04-08 11:05 | mcp/path | mcp/path, dbhub, agent-context | April 目标正式 apply 后，DBHub 只读查询返回 0 行，但项目直连与 apply 脚本都显示已成功… | 3537 |
| 2026-04-08 10:59 | business-rule | business-rule | 统一 Excel 达播主线从候选集推进到正式对象时，用户明确纠偏当前目标不是金额字段兼容，而是先构建订单标签表。 | 3552 |
| 2026-04-08 10:45 | business-rule | business-rule, sales | 用户确认 2026年04月日目标配置表_v1 是门店日报范围完整权威快照，并明确月目标与日目标加总不要求相等 | 3571 |
| 2026-04-08 10:23 | mcp/path | mcp/path, sales, doc-sync | NAS 门店日目标文件从固定单文件切换为按月份分文件 | 3586 |
| 2026-04-08 10:05 | path | path, oracle, agent-context | 将达播统一 Excel 主线从外部 dabo_etl 迁回 hefang_dw 内部实现 | 3601 |
| 2026-04-08 09:48 | import-workflow | import-workflow, sales, business-rule,… | 门店日报目标 NAS 固定文件开始同时维护 2026-03 与 2026-04，多月份文件导致导入脚本 dry-r… | 3618 |
| 2026-04-08 09:35 | field-mapping | field-mapping, oracle, mysql, agent-con… | 用户基于整份云雀订单管理 Excel 与 Oracle 只读核查，纠正统一 Excel 的主桥接键与筛选规则 | 3636 |
| 2026-04-07 16:56 | etl-snapshot | etl-snapshot, oracle | 固定 as-of 对账后，ODS 全量仍残留 58 单 / 75 件差异 | 3653 |
| 2026-04-07 16:36 | etl-ops | etl-ops | 前台 Python 终端消失后准备重跑 ODS 全量任务 | 3670 |
| 2026-04-07 14:40 | etl-sync | etl-sync, oracle, mysql | 排查为什么新样本对应的 `ods_m_retail.oms_sourcecode` 在 MySQL 侧持续为空，且… | 3686 |
| 2026-04-07 13:58 | business-rule | business-rule, oracle | 用户明确说明达播新入口改为云雀导出的 订单管理xxxxx.xlsx，平台不再来自文件名前缀，而是来自 Excel… | 3704 |
| 2026-04-03 18:05 | sql-execution | sql-execution, mysql, sales | 门店日报 ETL 在执行 SQL 骨架时只打印启动日志，ads_store_daily_report 未刷新到正式… | 3720 |
| 2026-04-03 17:25 | versioning | versioning, sales | 门店日报正式范围从7家样本扩到NAS 71家门店时，需要批量同步 dim_store_report_attr | 3738 |
| 2026-04-03 16:44 | field-mapping | field-mapping, oracle, sales | 门店日报准备从7家样本扩到NAS正式范围时，需要批量补dim_store_report_attr.report_c… | 3757 |
| 2026-04-03 16:23 | field-mapping | field-mapping | 用户纠正 NAS 目标文件中的机场免税门店标准名称 | 3772 |
| 2026-04-03 15:27 | path | path, sales | 用户确认门店日报目标 NAS 实际目录与文件命名 | 3789 |
| 2026-04-03 14:57 | business-rule | business-rule, sales | 用户明确门店日报月目标与日目标的实际关系 | 3806 |
| 2026-04-03 14:15 | business-rule | business-rule, sales | 用户纠正门店日报目标模板的日目标逻辑 | 3822 |
| 2026-04-03 13:59 | field-mapping | field-mapping, sales | 用户确认门店日报目标导入模板契约 | 3839 |
| 2026-04-03 13:11 | path | path, sales, doc-sync | 用户明确门店日报目标配置的正式导入路径 | 3855 |
| 2026-04-03 12:17 | business-rule | business-rule, sales | 用户确认门店日报目标配置少于有效门店数的处理方式 | 3871 |
| 2026-04-03 11:50 | etl-defensive-coding | etl-defensive-coding, mysql, sales | 门店日报正式 ETL 首次 conn-test | 3888 |
| 2026-04-03 11:03 | sql-null-handling | sql-null-handling, oracle, sales | 门店日报阶段4样本写入 ADS 后复跑 SQL-4，对账只剩 2 条 month_ach_rate 差异 | 3905 |
| 2026-04-03 09:46 | path | path, doc-sync, agent-context | 用户指出 docs/misc 先前改名为 docs/专题资料 不符合真实用途，强调该目录用于子项目扩展时的上下文同… | 3922 |
| 2026-04-02 17:00 | etl-windowing | etl-windowing, oracle, mysql | 为 ODS 原生补充按业务日期精确回刷能力时，需要同时满足 Oracle 抽取窗口与目标侧清理窗口一致而又不污染常… | 3939 |
| 2026-04-02 12:06 | etl-pattern | etl-pattern, mysql | 治理 ods_m_retail 与 ods_m_retailitem 重复装载事故时，发现仅按目标时间窗先删后写仍… | 3960 |
| 2026-04-02 11:25 | mcp/path | mcp/path, mysql | 用户正式收敛 8.47 小时建索引排障，确认查询慢已解决、重复装载不是直接主因，要求后续不要再回到 SQL 猜测而… | 3980 |
| 2026-04-02 10:09 | mcp/path | mcp/path, agent-context | 用 VS Code chat session 还原手工 DDL 时间线，并与本地 ETL 日志逐分钟对齐时，需要把… | 3995 |
| 2026-04-02 09:43 | etl-concurrency | etl-concurrency, dbhub, mysql | 通过 DBHub 只读排查 ods_m_retailitem 建索引期间是否存在并发写入与 online DDL… | 4010 |
| 2026-04-02 09:35 | system-io | system-io, mysql, powershell, performan… | 尝试在普通会话里做 C 盘 uncached 磁盘基准以验证 MySQL DDL 异常慢 | 4029 |
| 2026-04-02 09:31 | system-io | system-io, mysql, performance | 系统层验证 MySQL datadir/tmpdir 是否共用 C 盘并检查轻量磁盘基准 | 4046 |
| 2026-04-02 09:26 | ddl-performance | ddl-performance, dbhub, mysql | 继续通过 DBHub MCP 只读排查 ods_m_retailitem 建索引 8.47 小时的目录路径与 IO… | 4063 |
| 2026-04-02 09:22 | ddl-performance | ddl-performance, dbhub, mysql, performa… | 通过 DBHub MCP 只读排查 ods_m_retailitem 约 309 万行表创建二级索引耗时约 305… | 4080 |
| 2026-04-02 09:00 | sql-validation | sql-validation, dbhub, business-rule, p… | 用户已人工落地两条关键索引后，使用 DBHub MCP 进入慢 SQL 真实性能验证阶段 | 4097 |
| 2026-04-01 17:40 | index-design | index-design, dbhub, business-rule | 基于 DBHub 已确认的 EXPLAIN 结果，为 ods_m_retail 与 ods_m_retailite… | 4114 |
| 2026-04-01 17:35 | etl-pattern | etl-pattern, dbhub | 通过 DBHub MCP 继续追查 ods_m_retail 与 ods_m_retailitem 重复 id 的… | 4131 |
| 2026-04-01 17:27 | schema-governance | schema-governance, dbhub, mysql | 通过 DBHub MCP 对 ods_m_retail 与 ods_m_retailitem 做只读主键可行性诊断… | 4148 |
| 2026-04-01 17:03 | sql-performance | sql-performance, dbhub, sales, business… | 通过 DBHub MCP 诊断 ods_m_retail 与 ods_m_retailitem 联表慢 SQL，服… | 4165 |
| 2026-04-01 16:01 | mcp/path | mcp/path, oracle, sales | 执行带 CTE 的 Oracle 重算 SQL | 4182 |
| 2026-04-01 14:59 | path | path, dbhub, agent-context | 用户澄清数据库写操作授权流程，明确默认查询走 dbhub_ro，只读与写入审批分离 | 4198 |
| 2026-04-01 10:27 | business-rule | business-rule | 用户指出 4/1 日月累计应统计 3/1~3/31，而不是出现全 0 结果 | 4214 |
| 2026-03-31 17:58 | mcp/path | mcp/path, dbhub, mysql | 用户授权清空达播桥接表与聚合表后重导历史样本，尝试先用 DBHub MCP 执行 DELETE 与校验 | 4232 |
| 2026-03-31 17:46 | business-rule | business-rule, mysql | 使用历史样本排查 ads_dabo_order_bridge 平台仍为 unknown 时，评估是否可直接用 da… | 4248 |
| 2026-03-31 17:34 | business-rule | business-rule, oracle, doc-sync, agent-… | 用户补充还有视频号 sph，要求把平台前缀集合从 dy/tm/xhs 扩到 dy/tm/xhs/sph | 4264 |
| 2026-03-31 17:04 | business-rule | business-rule, oracle, agent-context | 用户明确要求平台识别只走文件名前缀，且不再兼容旧 dabo 前缀，并将小红书前缀修正为 xhs | 4280 |
| 2026-03-31 16:12 | backfill-strategy | backfill-strategy, oracle, mysql | 继续推进 ods_m_retail.oms_sourcecode 主线时，发现历史回填脚本虽然字段与索引齐备，但全… | 4296 |
| 2026-03-31 16:08 | path | path, agent-context | 用户明确要求：所有改表结构、增删改数据、建表建索引、回填补数都必须由其人工执行，Agent 只能给出 SQL 和步… | 4313 |
| 2026-03-31 16:01 | mysql-lock | mysql-lock, mysql, performance | 用户指出 ods_m_retail 回填阻塞的直接原因是 MySQL 内多个事件锁住表，并在疏通后要求重新评估主线… | 4330 |
| 2026-03-31 14:32 | query-design | query-design, sales | 修复 SQL-4 目标配置状态与样本范围冻结风险 | 4348 |
| 2026-03-31 14:04 | query-design | query-design, sales, doc-sync | 再次深挖样本模板时，发现即使已有建表/清表说明，仍缺少'步骤7之后中断的恢复起点'和'sample_categor… | 4366 |
| 2026-03-31 13:59 | query-design | query-design, oracle, sales, doc-sync | 用户要求再次深挖时，继续发现样本模板虽然已覆盖重跑清表与模板替换，但仍缺少'旧表结构漂移时必须重建而不是仅TRUN… | 4381 |
| 2026-03-31 13:53 | query-design | query-design, oracle, sales, doc-sync | 用户继续逐层深挖样本模板时指出：重跑对账时若把两张临时表在同一步提前 TRUNCATE，会让 mtd 临时表在 S… | 4396 |
| 2026-03-31 13:45 | query-design | query-design, oracle, sales, doc-sync | 用户做跨文件逐条交叉审计时指出：同样一句'检查 params CTE 中所有 DATE'在 SQL-2 和 SQL… | 4411 |
| 2026-03-31 13:33 | query-design | query-design, sales, doc-sync | 用户深度审计样本模板时指出：临时对账表即使已建成，重跑样本对账时若不先清空旧数据，导入会因(report_date… | 4426 |
| 2026-03-31 13:22 | query-design | query-design, oracle, mysql, sales, doc… | 用户深度审计样本模板时指出：执行链路要求先导入 Oracle 结果到 MySQL 临时表，但建表 DDL 只埋在… | 4441 |
| 2026-03-31 11:12 | field-mapping | field-mapping, oracle | 验证达播 CSV 订单号是否能作为 Oracle 达播识别桥接键 | 4456 |
| 2026-03-31 10:58 | field-mapping | field-mapping, oracle, sales | 探索是否可从 Oracle 线上渠道单据直接筛出达播销售 | 4471 |
| 2026-03-31 10:31 | query-design | query-design, mysql, sales, doc-sync | 用户深度审计样本门店模板，指出步骤C和步骤D中的MySQL候选查询依赖 @data_version，但文档若不显式… | 4486 |
| 2026-03-31 09:53 | query-design | query-design, sales, doc-sync | 用户继续系统审计样本门店模板，指出执行模板除了给SQL片段，还必须显式给出步骤标签、变量设置步骤和所有强输入模板，… | 4501 |
| 2026-03-31 09:46 | query-design | query-design, oracle, sales, doc-sync | 用户继续深度审计样本门店模板，指出 sample_category_scope 缺少替换模板，且主样本日期同步提醒… | 4516 |
| 2026-03-31 09:42 | query-design | query-design, oracle, sales, doc-sync | 用户继续深度审计样本门店模板，指出样本门店范围之外，sample_category_scope 同样是 Oracl… | 4531 |
| 2026-03-31 09:40 | query-design | query-design, oracle, mysql, sales | 用户再次深度审计样本门店模板，指出Oracle占位符若设计成SQL非法或语义模糊的万能占位，会提高执行出错率；同时… | 4546 |
| 2026-03-31 09:35 | query-design | query-design, oracle, sales, business-r… | 用户深度审计样本门店模板，指出无销售候选SQL即使改到头表+明细表联合判断，若未叠加商品类目裁剪，仍可能与最终fi… | 4561 |
| 2026-03-31 09:33 | query-design | query-design, oracle, sales, business-r… | 用户再次审计样本门店模板，指出无销售候选门店若只按ods_m_retail头表判断，会与SQL骨架和SQL-2的明… | 4576 |
| 2026-03-31 09:28 | query-design | query-design, oracle, mysql, sales, bus… | 用户审计阶段4样本门店清单模板，指出候选SQL不应依赖ADS结果表且Oracle与MySQL替换片段不能混写 | 4591 |
| 2026-03-30 17:53 | output-consistency | output-consistency, sales | 用户明确表示不希望每次都依赖另一个模型审计，才发现 SQL 输出层仍有遗漏指标 | 4606 |
| 2026-03-30 17:06 | business-rule | business-rule, oracle, sales | 为阶段4编写门店日报对账SQL时，发现以事实CTE为驱动会漏掉无销售但应纳入样本范围的门店 | 4622 |
| 2026-03-30 16:52 | field-mapping | field-mapping, oracle, mysql, sales, bu… | 为阶段4编写Oracle日事实重算SQL时，需要把MySQL配置层的商品纳入口径传入Oracle执行 | 4637 |
| 2026-03-30 11:52 | business-rule | business-rule, oracle, mysql, sales, do… | 用户指出阶段4对账方案过度偏向Excel，而真实对账应以ADS结果对齐Oracle源事实 | 4652 |
| 2026-03-27 13:34 | business-rule | business-rule, sales, agent-context | 用户在阶段 3 收口时最终拍板目标版本读取策略 | 4667 |
| 2026-03-27 13:34 | business-rule | business-rule, sales, agent-context | 用户在阶段 3 收口时最终拍板 ADS 退货口径 | 4682 |
| 2026-03-26 15:52 | field-mapping | field-mapping, oracle, sales | 核对线下销售日报中有赞渠道实体是否需要额外映射层 | 4697 |
| 2026-03-26 15:52 | business-rule | business-rule, sales | 线下销售日报设计阶段收口 0 金额口径 | 4714 |
| 2026-03-24 10:55 | mcp/path | mcp/path, oracle, agent-context | 仓库根 `.mcp.json` 已配置 Oracle，但当前 Copilot 会话里始终拿不到 Oracle MC… | 4731 |
| 2026-03-24 09:50 | mcp/path | mcp/path, dbhub, agent-context | VS Code 中 io.github.bytebase/dbhub 长时间等待 initialize 后退出码 1 | 4748 |
| 2026-03-23 17:46 | field-mapping | field-mapping, oracle, mysql | dim_channel 自动化测试误判缺少 DS001 | 4763 |
| 2026-03-23 17:37 | business-rule | oracle, mysql, sales, business-rule, do… | Oracle/MySQL 销售对账差异 7%+ | 4780 |
| 2026-03-23 17:06 | incremental-logic | incremental-logic, sales, inventory | 2026-03-23 主链重跑时 dws_inventory 与 ads_health 出现 1213/1205… | 4798 |
| 2026-03-23 16:27 | field-mapping | field-mapping, oracle, inventory, busin… | 将 dws_inventory 从 Oracle 切到 ODS 时核对 qty_valid 口径 | 4815 |
| 2026-03-23 16:10 | business-rule | business-rule, oracle, sales, inventory… | ODS 刚接入主自动化链、但 DWS 尚未切换到消费 ODS 时进行文档同步 | 4830 |
| 2026-03-23 11:50 | copilot-agent | copilot-agent, doc-sync, agent-context | 继续推进第二阶段 custom agents 时，需要提高 agent picker 与自然语言发现的稳定性 | 4845 |
| 2026-03-23 11:31 | copilot-hook | copilot-hook, powershell, agent-context | 用户反馈最新一次 Python 版 PostToolUse 和 Stop 都没有任何 warning 卡片，需要区… | 4862 |
| 2026-03-23 11:32 | copilot-hook | copilot-hook, powershell, agent-context | Stop 改成 Python 后，真实 Copilot UI 仍出现旧 `cmd` 路径报错与 `pwsh` 风格… | 4878 |
| 2026-03-23 11:02 | copilot-hook | copilot-hook, powershell, doc-sync, age… | 在真实 Copilot 会话中复测 Stop hook warning 的 UI 展示效果 | 4895 |
| 2026-03-23 11:10 | copilot-hook | copilot-hook, powershell, agent-context | 真实 Copilot UI 中 Stop warning 已可见，但卡片里仍混入一部分 PowerShell 错误… | 4912 |
| 2026-03-23 11:18 | copilot-hook | copilot-hook, powershell, agent-context | 将 Stop warning 文案改成 ASCII 并加入 `cmd` 包装层后，真实 Copilot UI 中仍… | 4929 |
| 2026-03-23 10:54 | copilot-hook | copilot-hook, agent-context | 设计 Stop 收口提醒时，需要避免被工作树历史未提交改动误导 | 4946 |
| 2026-03-23 10:19 | copilot-hook | copilot-hook, agent-context | 排查 GitHub Copilot PostToolUse hook 已执行但聊天卡片不稳定展示 warning | 4963 |
| 2026-03-20 15:27 | incremental-logic | incremental-logic, sales, doc-sync | 审计并调整 dws_sales 的增量逻辑时，发现代码、契约和说明文档对水位语义描述不一致 | 4980 |
| 2026-03-20 12:04 | field-mapping | field-mapping, agent-context | 拿到 hfsy 真实连接后，对 t_member_bind_info 的 *1 列和 DecryptionTags… | 4999 |
| 2026-03-20 11:38 | doc-sync | doc-sync, agent-context | 用户要求将 hfsy 连接上下文同步到文档，并直接给出真实数据库密码 | 5016 |
| 2026-03-20 09:50 | field-mapping | field-mapping, mysql, doc-sync, agent-c… | 收到数云 xlsx 与 hfsy 实库连接信息后执行第2轮实表校正 | 5034 |
| 2026-03-19 18:11 | path | path, oracle, mysql, agent-context | 用户澄清公司开发环境的数据库职责边界 | 5051 |
| 2026-03-19 18:35 | business-rule | business-rule, doc-sync, agent-context | 数云CRM第1轮 12 表字段级仲裁 | 5068 |
| 2026-03-19 18:10 | doc-sync | doc-sync, agent-context | 再次审计数云CRM实施计划 | 5088 |
| 2026-03-19 17:31 | business-rule | business-rule, mysql | 数云CRM实施计划交叉审计 | 5106 |
| 2026-03-18 14:51 | mcp | mcp, oracle, mysql | VS Code 已重载但当前聊天仍看不到 MCP 工具 | 5125 |
| 2026-03-18 14:40 | field-mapping | field-mapping, oracle, sales, inventory… | Oracle 侧查询“近期销量最好的 3 个产品”时，第一次查询报 ORA-00904。 | 5141 |
