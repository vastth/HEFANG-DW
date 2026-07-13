# M2.5 Oracle BOSNDS3 源库画像与 ODS / DWD 长期规划

---

## 1. 文档状态与边界

| 项 | 说明 |
|----|------|
| 文档状态 | 初版只读画像与规划草案 |
| 探查对象 | Oracle `BOSNDS3` 核心源表、MySQL 当前 ODS / DIM / DWS / ADS 链路 |
| 本轮已执行 | Oracle 结构快照、MySQL 结构快照、Oracle 核心表字段启用率画像、当前链路行数快照 |
| 本轮未执行 | 未建表、未改 ETL、未改 SQL、未改调度、未执行数据库写操作 |
| 关键边界 | 字段非空率只能证明“统计窗口内是否有数据”，不能单独判定字段业务废弃 |

本文件用于承接 M2 设计冻结后的 M3 前置工作：先从 Oracle 源库事实和当前 ODS / DWD 缺口出发，规划长期最优的 ODS 与 DWD 链路，再决定后续是否输出 DDL 草案与旁路 ETL 骨架。

后续状态更新：截至 2026-04-30，用户已人工完成 M3 的 3 张 raw ODS 表和 2 张 DWD 表建表并修正表注释；Copilot 已按授权完成近 1 天 raw / DWD 旁路小窗口装载和最小对账，但未接调度，当前 DWS / ADS 仍不消费这些表。证据见 `reports/context_cache/m3_manual_ddl_verification_20260430.json` 与 `reports/context_cache/m3_raw_dwd_small_window_load_20260430.json`。

---

## 2. 本轮证据清单

| 证据 | 结果摘要 |
|------|----------|
| `tools/snapshot_oracle_bosnds3_schema.py --schema BOSNDS3 --output reports/snapshot_oracle_bosnds3_schema.json` | 2026-04-29 15:48:38 生成 Oracle 核心表结构快照，返回 10 张核心表 |
| `tools/snapshot_mysql_hefangdw_schema.py --output reports/snapshot_mysql_hefangdw_schema.json` | 2026-04-29 15:48:51 生成 MySQL 数仓结构快照，返回 36 张表 |
| `reports/oracle_bosnds3_core_field_profile_202604.json` | 2026-04-29 15:51:16 生成核心表字段启用率画像；`M_RETAIL` / `M_RETAILITEM` 仅统计 2026-04 单据窗口，其他核心表为全表统计 |
| Oracle `ALL_TABLES` 只读查询 | `BOSNDS3` 下共 2705 张表，其中 `NUM_ROWS > 0` 的表 695 张，`NUM_ROWS` 为空 187 张，`NUM_ROWS` 为 0 或空 2010 张 |
| MySQL 只读行数查询 | 当前 `ods_m_retail` 1888385 行、`ods_m_retailitem` 3135008 行、`ods_fa_storage` 200981 行；下游 `dws_sales_daily` 1471634 行、`dws_inventory_daily` 8345346 行 |

说明：Oracle `ALL_TABLES.NUM_ROWS` 是统计信息，不一定实时；涉及精确对账时，应以实际 `COUNT(*)` 或本项目 ETL 验证 SQL 为准。

---

## 3. BOSNDS3 全库结构初步画像

### 3.1 表规模概览

| 指标 | 值 | 说明 |
|------|----|------|
| 总表数 | 2705 | 来自 `ALL_TABLES` |
| 有统计行数且大于 0 的表 | 695 | 可作为“疑似启用表”候选，不等同当前业务必用 |
| 统计行数为 0 或空的表 | 2010 | 可能是废弃、配置空表、临时表、视图替代表或统计未采集，需分层判断 |
| 统计行数未知 | 187 | 需按主题单独核实 |

### 3.2 表名前缀分布

| 前缀 | 表数 | 有数据表数 | 统计行数合计 | 初步理解 |
|------|------|------------|--------------|----------|
| `M` | 519 | 157 | 39537928 | ERP 主业务单据、商品、库存移动、零售等核心域 |
| `C` | 452 | 106 | 8348152 | 基础档案、门店、客户 / VIP 等主数据 |
| `B` | 252 | 28 | 2448187 | 促销、结算、业务配置等候选域 |
| `RP` | 199 | 6 | 10864 | 报表 / 结果类表，需谨慎作为源事实 |
| `AD` | 62 | 52 | 11982070 | 系统流程 / 实例日志，通常不作为业务事实首选 |
| `O2O` | 60 | 24 | 4971841 | 电商 / O2O 单据、渠道、状态日志等 |
| `FA` | 41 | 24 | 19710736 | 财务库存、库存余额 / 月结等 |
| `OMS` | 11 | 5 | 110646260 | OMS 订单和库存变化大表，后续电商 / 库存流水主题需重点审计 |
| `SY` | 17 | 17 | 35871189 | 数云 / 会员相关同步域候选，不应直接并入本轮 DWD 首批实现 |
| `YUNQUE` / `YZ` | 相关前缀 | 多张大表 | 千万级 | 电商 / 有赞 / 云雀来源候选，需另设专题审计 |

结论：BOSNDS3 不是只有当前 ODS 已接入的 3 张事实源表。长期 ODS / DWD 规划必须按“主题域 + 事实粒度 + 数据启用证据”逐步展开，不能全库盲扫，也不能只按当前 DWS / ADS 已消费字段倒推源库价值。

---

## 4. 第一批核心链路表结构与字段启用率

### 4.1 当前核心表结构快照

| 表 | Oracle 结构字段数 | 当前用途 |
|----|-------------------|----------|
| `M_RETAIL` | 273 | 零售单头，当前抽取为 `ods_m_retail` |
| `M_RETAILITEM` | 142 | 零售明细，当前抽取为 `ods_m_retailitem` |
| `FA_STORAGE` | 24 | 当前库存余额，当前抽取为 `ods_fa_storage` |
| `C_STORE` | 353 | 店仓档案，当前抽取为 `dim_store` |
| `C_AREA` | 11 | 区域档案，当前被 `dim_store` 关联 |
| `M_PRODUCT` | 173 | 商品档案，当前抽取为 `dim_product` |
| `M_PRODUCT_ALIAS` | 27 | SKU / 条码档案，当前抽取为 `dim_sku` |
| `M_ATTRIBUTESETINSTANCE` | 23 | 颜色、尺码等属性实例，当前被商品 / SKU 维度关联 |
| `M_DIM` | 15 | 商品类别、性质、系列、品牌等维度编码 |
| `O2O_RETAIL_CHANNEL` | 12 | 电商渠道维度，当前抽取为 `dim_channel` |
| `FA_MONTHSTORE` | 72 | 月进销存，当前未进入第一批 ODS / DWD 主链 |

当前结构快照工具按 `docs/数据结构与映射手册.md` 的核心表清单过滤，返回 10 张表；文档中旧列出的 `O2O_SO` / `O2O_SOITEM` 在本轮精确表名查询中未返回，后续应改按 `O2O_RETSO`、`O2O_RETSOITEM`、`YUNQUE_ORDERS`、`YUNQUE_ORDERSITEM`、`OMS_ORDERSO_LOG` 等候选表重新审计电商订单源。

### 4.2 字段启用率概览

| 表 | 统计范围 | 行数 | 字段数 | 有数据字段 | 空字段 | 初步结论 |
|----|----------|------|--------|------------|--------|----------|
| `M_RETAIL` | 2026-04 单据窗口 | 27607 | 273 | 120 | 153 | 单头字段很宽，当前 ODS 只接入少量核算 / 状态字段，会员、营业员、支付、来源等上下文存在补充价值 |
| `M_RETAILITEM` | 2026-04 单据窗口 | 41675 | 142 | 90 | 52 | 明细行字段宽于当前 ODS，营业员、折扣、退货、属性实例等字段需要进入 M3 字段血缘 |
| `FA_STORAGE` | 全表 | 201086 | 24 | 24 | 0 | 24 个字段全部有数据；当前 ODS 仅取 8 个字段，库存 ODS 扩展优先级高 |
| `C_STORE` | 全表 | 220 | 353 | 173 | 180 | 店仓档案字段极宽；DIM 不应盲目全量扩列，需另建源档案 ODS / 字段白名单 |
| `C_AREA` | 全表 | 9 | 11 | 10 | 1 | 区域档案较小，可作为门店维度补充 |
| `M_PRODUCT` | 全表 | 7051 | 173 | 82 | 91 | 商品档案有大量可用维度字段，当前 `dim_product` 只覆盖少量字段 |
| `M_PRODUCT_ALIAS` | 全表 | 8235 | 27 | 19 | 8 | SKU 维度结构较清晰，当前 `dim_sku` 已覆盖核心条码 / 商品 / 颜色尺码 |
| `M_ATTRIBUTESETINSTANCE` | 全表 | 44935 | 23 | 16 | 7 | `VALUE1` / `VALUE2` 全覆盖，支持颜色 / 尺码；其他属性需按业务再确认 |
| `M_DIM` | 全表 | 205 | 15 | 12 | 3 | 商品属性维表可继续作为 DIM 标准化基础 |
| `O2O_RETAIL_CHANNEL` | 全表 | 87 | 12 | 12 | 0 | 渠道维度字段全部有数据，当前 `dim_channel` 覆盖主要分析字段 |

---

## 5. 当前 ODS / DIM 字段覆盖缺口

### 5.1 销售源表

| 源表 | 当前已进入 MySQL 的主要字段 | 源侧有数据但当前未入 ODS / DIM 的重点字段 | 规划判断 |
|------|-----------------------------|----------------------------------------------|----------|
| `M_RETAIL` | `ID`、`DOCNO`、`BILLDATE`、`C_STORE_ID`、`OMS_SOURCECODE`、`TOT_AMT_ACTUAL`、`TOT_AMT_LIST`、`TOT_QTY`、`STATUS`、`ISACTIVE`、`MODIFIEDDATE` | `CREATIONDATE`、`DOCTYPE`、`DESCRIPTION`、`AVG_DISCOUNT`、`C_VIP_ID`、`SALESREP_ID`、`PAY_STATUS`、`PAYERID`、`PAYTIME`、`CLOSE_STATUS`、`REFNO`、`ISRETURNED` 等 | 当前 ODS 更像“DWS 所需窄字段落地”，不是长期最优的源事实 ODS；M3 应先补销售上下文字段白名单 |
| `M_RETAILITEM` | `ID`、`M_RETAIL_ID`、`M_PRODUCT_ID`、`M_PRODUCTALIAS_ID`、`QTY`、`PRICELIST`、`PRICEACTUAL`、`TOT_AMT_ACTUAL`、`TOT_AMT_LIST`、`MODIFIEDDATE`、`SETTIME` | `M_ATTRIBUTESETINSTANCE_ID`、`ORDERNO`、`C_VIP_ID`、`SALESREP_ID`、`DISCOUNT`、`DESCRIPTION`、`STATUS`、`TYPE`、`RQTY`、`SALESREPS_ID`、`SALESREPS_NAME`、`RCANQTY` 等；`RETURNQTY` 后续全量非零值扫描确认全量为 0，已不进入 M3 草案 | `dwd_sales_retail_item` 若要承接业务销售底表上下文，不能只依赖当前 11 个明细字段 |

关键字段覆盖证据：2026-04 窗口内 `M_RETAIL.C_VIP_ID` 覆盖率约 27.57%，`M_RETAIL.SALESREP_ID` 覆盖率约 27.25%，`M_RETAIL.OMS_SOURCECODE` 覆盖率约 65.11%；`M_RETAILITEM.SALESREPS_ID` / `SALESREPS_NAME` 覆盖率约 36.58%，`M_RETAILITEM.SETTIME` 覆盖率约 36.59%，`M_RETAILITEM.MODIFIEDDATE` 覆盖率约 63.43%。这些字段不是“全量必填”，但明显不是废字段。

### 5.2 库存源表

| 源表 | 当前已进入 MySQL 的主要字段 | 源侧有数据但当前未入 ODS 的重点字段 | 规划判断 |
|------|-----------------------------|--------------------------------------|----------|
| `FA_STORAGE` | `ID`、`C_STORE_ID`、`M_PRODUCT_ID`、`M_PRODUCTALIAS_ID`、`QTY`、`QTYVALID`、`QTYPURCHASEREM`、`ISACTIVE` | `CREATIONDATE`、`MODIFIEDDATE`、`M_ATTRIBUTESETINSTANCE_ID`、`QTYPREOUT`、`QTYPREIN`、`QTY_FREEZE`、`QTY_OMS`、`QTYOMSTRANSLATE`、`QTYPREOUT1`；`QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 后续全量非零值扫描确认为全量 0 | M2.5 的非空画像只能说明字段存在 / 非空，不等于字段有真实业务值；库存 ODS / DWD 应保留真实有数据且语义明确的库存信号，并剔除 Oracle 模板冗余字段 |

当前库存 DWS 把 `qty_valid` 直接设为 `fs.qty`，并把 `qty_occupy` 置 0；后续补证也确认源侧 `FA_STORAGE.QTYVALID` 全量为 0，因此新 DWD 不再保留该物理字段，仅在对账 SQL 中用 `qty` 生成 `qty_valid` 等价值与现有 DWS 对比。`QTYPREOUT`、`QTYPREIN`、`QTY_FREEZE`、`QTY_OMS`、`QTYPURCHASEREM`、`QTYOMSTRANSLATE`、`QTYPREOUT1` 等真实库存信号仍应纳入长期库存 DWD 评审。长期看，库存 DWD 不应继续用当前 DWS 的简化字段作为唯一事实来源。

### 5.3 商品 / 店仓 / 渠道维度

| 主题 | 当前覆盖 | 关键缺口 | 规划判断 |
|------|----------|----------|----------|
| 商品 | `dim_product` 覆盖商品编码、名称、类别、性质、系列、品牌、价格、成本、材质、主销标识 | `M_PRODUCT` 中仍有 70 个有数据但未进入当前维度的字段；`M_DIM2_ID` 覆盖 99.09%，`MARKETDATE` 覆盖 32.15%，`M_DIM5_ID` 覆盖 50.87% | 不建议盲目把 173 个商品字段都塞进 `dim_product`；建议先建立商品字段白名单和可复用业务宽表 |
| SKU | `dim_sku` 覆盖条码、商品、颜色、尺码、有效标识 | `M_PRODUCT_ALIAS` 的 `INTSCODE`、`FORCODE`、`COMMENTS`、`MODIFIEDDATE`、`CLRALIAS` 等字段有数据 | SKU 维度第一阶段基本够用；若要替代业务底表，再补条码扩展字段 |
| 店仓 | `dim_store` 覆盖编码、名称、区域、仓 / 店、云仓、中心仓、类型、有效标识 | `C_STORE` 中地址、电话、租期、门店属性、组织归属等字段大量有数据 | 店仓 DIM 应保持稳定分析字段；如需完整源档案，建议新建 ODS 店仓档案镜像或配置宽表，不直接污染核心 DIM |
| 渠道 | `dim_channel` 覆盖渠道名称、编码、`WING_CODE`、主渠道标识、平台类型、有效标识 | 其余审计字段有数据但对当前分析价值有限 | 当前渠道维度可作为第一阶段标准维表，后续电商订单专题再扩展 |

---

## 6. “启用字段 / 疑似废字段”判定原则

| 分类 | 判定依据 | 本轮可下的结论 | 后续动作 |
|------|----------|----------------|----------|
| 当前链路已用字段 | 已在 ETL / SQL 中引用，且源侧有数据 | 可视为已启用字段 | 继续作为 ODS / DWD 必备字段 |
| 源侧有数据但当前未入库字段 | 字段画像非空，当前 ETL 未引用 | 不能判为废字段，属于“潜在有价值字段” | 按业务主题进入 M3 字段血缘白名单评审 |
| 统计窗口空字段 | 本轮画像 `non_null_rows = 0` | 只能判为“本窗口 / 本表样本未启用” | 若连续多个窗口、全量、代码和业务均无引用，再标记疑似废弃 |
| 全量为 0 / 全量为空模板字段 | 全量扫描 `non_zero_rows = 0` 或 `non_null_rows = 0`，且用户确认无未来业务意义 | 可判为不进入新架构草案 DDL 的模板冗余字段 | 在证据文件和文档中记录剔除原因，不再进入 raw ODS / DWD 字段清单 |
| 业务废弃字段 | 空值率、代码引用、文档、用户确认均支持废弃 | 本轮尚未形成最终废弃结论 | 需要用户确认，不由 Agent 单方面判定 |

因此，本轮不输出“确定废字段清单”，只输出“空字段样例”和“源侧有数据但未入库字段清单”。

---

## 7. 源数据 → ODS → DWD → DWS → ADS 当前事实链路

### 7.1 销售链路

| 层 | 当前对象 | 事实 |
|----|----------|------|
| Oracle 源 | `M_RETAIL`、`M_RETAILITEM` | 单头 / 明细宽表，字段远多于当前 ODS；2026-04 窗口有会员、营业员、来源、支付等上下文字段启用迹象 |
| ODS | `ods_m_retail`、`ods_m_retailitem` | 当前仅保留 DWS 所需核心字段和水位字段，分别 13 列 |
| DWD | 已完成旁路完整业务日期验证 / 未接调度 | `dwd_sales_retail_item` 已由用户人工建表；先按授权完成 2103 行小窗口 upsert，2026-05-07 又完成 20260428-20260430 完整业务日期 DWD 5103 行重算并与现有 DWS 日级汇总对齐；当前 DWS / ADS 不消费 |
| DWS | `dws_sales_daily` | 直接从 ODS 聚合，过滤有效单据、状态 2、日期窗口、SKU 非空 |
| ADS | 销售专题 ADS / 库存健康 ADS | 销售专题多为 ODS 直读；库存健康从 `dws_sales_daily` 读取近 30 / 7 天销售 |

长期问题：销售 ADS 已经出现多处 ODS 直读，说明 DWS 未覆盖所有业务分析维度；如果不先补 DWD，后续每个 ADS 都会重复写过滤、金额、门店、商品规则。

### 7.2 库存链路

| 层 | 当前对象 | 事实 |
|----|----------|------|
| Oracle 源 | `FA_STORAGE` | 字段少但存在模板化冗余；真实库存信号包含当前库存、预出、预入、冻结、OMS、采购欠数等，`QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 后续全量非零值扫描确认为全量 0 |
| ODS | `ods_fa_storage` | 当前仅保留 8 个源字段 + ETL 字段 |
| DWD | 已完成旁路 full raw 快照验证 / 未接调度 | `dwd_inventory_storage_snapshot` 已由用户人工建表；先按授权完成 5741 行小窗口 upsert，2026-05-07 又完成 20260507 full raw 快照 DWD 201946 行初始化；当前 DWS / ADS 不消费；与现有 DWS `qty` 差 337 来自快照时间点不同 |
| DWS | `dws_inventory_daily` | 从 ODS 读取后过滤有效库存、SKU 非空、总仓 / 云仓，并按店仓 + 商品 + SKU 汇总 |
| ADS | `ads_inventory_health` | 以 DWS 库存为主表，并结合 DWS 销售、商品、达播销量输出库存健康 |

长期问题：当前 DWS 已经把库存事实裁剪为库存健康服务范围；若直接把它当 DWD，会丢失全店仓、全库存信号和未来库存运营分析所需字段。

---

## 8. ODS 长期规划建议

### 8.1 ODS 目标形态

ODS 应定位为“源表可追溯落地层”，不是下游 DWS 的窄字段 staging。长期建议：

1. 当前已接入的 `ods_m_retail`、`ods_m_retailitem`、`ods_fa_storage` 保持兼容，不直接破坏生产链路。
2. 新增字段应采用兼容式扩展或新建 `ods_*_wide` / `ods_*_raw` 旁路表方案，先验证再替换。
3. 对大表 ODS 必须保留源主键、水位字段、批次字段、装载时间和源侧状态字段。
4. ODS 不直接固化 DWS / ADS 业务过滤；只保留源事实和轻量质量标识。
5. 对 Oracle 大表扩字段前，必须评估单批数据量、索引、水位、回刷窗口、MySQL 写入事务和 `timeout_profile`。

### 8.2 第一批 ODS 扩展优先级

| 优先级 | 对象 | 建议动作 | 原因 |
|--------|------|----------|------|
| P1 | `ods_fa_storage` | 优先评审扩展 `FA_STORAGE` 中真实有数据且语义明确的源字段，至少补齐 `MODIFIEDDATE`、`M_ATTRIBUTESETINSTANCE_ID`、`QTYPREOUT`、`QTYPREIN`、`QTY_FREEZE`、`QTY_OMS`、`QTYPURCHASEREM`、`QTYOMSTRANSLATE`、`QTYPREOUT1` 等 | 源表字段少但有模板冗余；剔除全零字段后更能保护库存 DWD 长期质量 |
| P1 | `ods_m_retailitem` | 补明细上下文白名单：`ORDERNO`、`C_VIP_ID`、`SALESREP_ID`、`M_ATTRIBUTESETINSTANCE_ID`、`DISCOUNT`、`STATUS`、`TYPE`、`RQTY`、`RCANQTY`、`SALESREPS_ID`、`SALESREPS_NAME`；`RETURNQTY` 不进入 M3 草案 | 支撑销售 DWD 承接业务销售底表上下文 |
| P1 | `ods_m_retail` | 补单头上下文白名单：`CREATIONDATE`、`DOCTYPE`、`DESCRIPTION`、`AVG_DISCOUNT`、`C_VIP_ID`、`SALESREP_ID`、`PAY_STATUS`、`PAYTIME`、`REFNO`、`ISRETURNED` 等 | 支撑会员、营业员、来源、支付、退货等归因 |
| P2 | 商品 / SKU 源档案 | 不直接大改 `dim_product`；先设计商品源字段白名单或商品业务宽表 | 商品源字段宽，需避免把 DIM 变成不可维护的大宽表 |
| P2 | 店仓源档案 | 不直接大改 `dim_store`；如有业务需要，设计店仓源档案 ODS 或管理属性宽表 | `C_STORE` 353 字段中大量字段有数据，但核心分析维度只需要稳定字段 |
| P3 | 电商 / 会员 / 库存流水候选表 | 单独开专题画像，不与第一批 DWD 混在一起实现 | 表量大、来源复杂，容易影响主线节奏 |

---

## 9. DWD 长期规划建议

### 9.1 第一批 DWD 仍保持两条主线

| DWD 候选对象 | 长期定位 | 本轮源库画像对 M2 决策的支撑 |
|--------------|----------|-------------------------------|
| `dwd_sales_retail_item` | 零售明细原子事实 + 关键业务上下文 | `M_RETAIL` / `M_RETAILITEM` 的会员、营业员、来源、折扣、退货等字段存在启用迹象；当前 ODS 仅保留核算字段，不足以支撑长期业务底表复现 |
| `dwd_inventory_storage_snapshot` | 全店仓库存快照事实层 | `FA_STORAGE` 字段少但有模板冗余；剔除全零字段后仍有当前库存、预出、预入、冻结、OMS、采购欠数等真实库存信号，当前 ODS / DWS 明显裁剪了多个库存信号；库存健康只应是第一批消费场景 |

### 9.2 DWD 与 ODS / DIM 的边界

1. DWD 不直接读 Oracle，应从 ODS / DIM 消费，保证水位、批次、重跑和 MySQL 内部对账可控。
2. DWD 可以沉淀跨主题可复用的状态标识和事实标识，例如 `is_valid_retail_flag`、`has_sku_flag`、`dws_sales_scope_flag`、`is_total_warehouse_flag`、`is_cloud_store_flag`。
3. DWD 不直接承接所有展示字段；展示名、类别名、门店名、颜色尺码等优先由 DIM 或业务宽表视图补齐。
4. DWD 不提前按现有 DWS / ADS 过滤裁剪事实；DWS / ADS 通过标识和规则维表明确消费范围。
5. DWD 的 freshness 不应长期借用 `dws_sales_daily.etl_time`；应逐步建立事实水位或按日期 `MAX(etl_time)` 的元数据视图。

### 9.3 后续候选主题

| 主题 | 候选源表 | 暂不进入第一批的原因 |
|------|----------|----------------------|
| 零售支付 / 购物券 | `M_RETAILPAYITEM`、促销 / 券相关 `B_*`、`C_VOU*` 候选表 | 需先确认 ERP 销售底表中购物券字段真实来源，不能只凭字段名猜测 |
| 会员 / VIP | `C_CLIENT_VIP`、`C_VIPTYPE`、`SY_VIP`、`SY_MEMBER` 等 | 与数云 / CRM 子项目边界相关，需单独确认事实源优先级 |
| 库存流水 / 调拨 / 盘点 | `M_TRANSFER`、`M_TRANSFERITEM`、`M_INVENTORY`、`M_INVENTORYITEM`、`OMS_FA_STORAGE_CHANGE` | 表量大且事务语义复杂，适合库存快照 DWD 稳定后进入第二阶段 |
| 电商订单 | `O2O_RETSO`、`O2O_RETSOITEM`、`YUNQUE_ORDERS`、`YUNQUE_ORDERSITEM`、`OMS_ORDERSO_LOG` | 当前文档中 `O2O_SO` / `O2O_SOITEM` 精确表名未命中，需要先做电商源表专题画像 |

---

## 10. M3 建议路线

| 顺序 | 动作 | 产物 | 边界 |
|------|------|------|------|
| 1 | 锁定第一批源字段白名单 | `M_RETAIL` / `M_RETAILITEM` / `FA_STORAGE` 字段血缘表 | 只读分析，不输出 DDL |
| 2 | 设计 ODS 扩展方案 | 兼容扩字段方案或旁路 `ods_*_raw` 方案对比 | DDL 只输出给用户人工执行 |
| 3 | 设计 DWD DDL 草案 | `dwd_sales_retail_item`、`dwd_inventory_storage_snapshot` 候选 DDL | 已在 M3 后由用户人工建表；后续 ALTER / 索引仍需授权 / 人工执行 |
| 4 | 设计旁路 ETL | 独立脚本、`--conn-test`、小窗口参数、锁名、`timeout_profile` | 已实现默认 dry-run、显式 `--execute` 写入；不接 `run_etl.py` |
| 5 | 小窗口只读 / 写入验证 | 行数、主键重复、金额、库存信号、字段覆盖率对账 SQL | 已按用户授权完成近 1 天小窗口写入；2026-05-07 已按用户授权完成销售完整业务日期与库存 full raw 初始化验证 |
| 6 | DWS v2 对账方案 | 从 DWD 聚合到并行 DWS v2，与现有 DWS 对账 | 不原地替换生产表 |

---

## 11. 风险与待确认项

| 编号 | 风险 / 待确认 | 当前建议 |
|------|---------------|----------|
| R1 | Oracle 全库 2705 张表，不能全库盲扫字段值 | 先按当前链路核心表画像，再按主题域扩展 |
| R2 | `ALL_TABLES.NUM_ROWS` 不是实时行数 | 用作规模参考；上线前必须用实际 `COUNT(*)` / 对账 SQL 复核 |
| R3 | 当前 ODS 窄字段已服务生产，直接扩改有兼容风险 | 采用旁路表或兼容字段扩展，先小窗口验证 |
| R4 | `M_RETAIL` / `M_RETAILITEM` 空字段很多，但部分字段可能只在特殊渠道 / 历史期间启用 | 不直接判废；需跨月份 / 全量 / 业务复核 |
| R5 | 库存字段非空不等同真实有业务值，字段语义也不一定等同业务口径 | `QTYVALID`、`QTY_BAS`、`QTY_BAS_PREOUT`、`QTYDIRTY` 已按全量为 0 剔除；`QTYPREOUT`、`QTYPREIN`、`QTY_OMS`、`QTYPURCHASEREM`、`QTYOMSTRANSLATE` 等仍需用户结合 ERP 底表确认含义和使用边界 |
| R6 | 会员、营业员、购物券字段有启用迹象，但维表来源未确认 | M3 先做字段血缘，不急于建 DWD 宽字段 |
| R7 | 大表扩字段或历史回填存在超时风险 | 必须显式使用 `timeout_profile='etl'` 或 `long_running`，并保留耗时证据 |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.5 | 2026-05-07 | 补记 M3 后续已完成销售完整业务日期 raw / DWD 与库存 full raw / DWD 初始化验证，但不改变 M2.5 源库画像边界和生产链路边界 |
| v0.4 | 2026-04-30 | 补记 M3 raw / DWD 已完成旁路小窗口装载验证，但不改变 M2.5 源库画像和生产链路边界 |
| v0.3 | 2026-04-30 | 补记 M3 后用户已人工完成 5 张 raw / DWD 表建表，区分画像阶段“未执行”与当前已建空表状态 |
| v0.2 | 2026-04-30 | 补充全量非零值扫描结论，校正 M2.5 初版“非空即有数据”的表述边界，标注全零模板字段不进入 M3 草案 |
| v0.1 | 2026-04-29 | 新增 Oracle BOSNDS3 核心源库画像、字段启用率初步结论与 ODS / DWD 长期规划草案 |
