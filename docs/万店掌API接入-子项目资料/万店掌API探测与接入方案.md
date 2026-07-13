# 万店掌API探测与接入方案

## 1. 目标

- 记录万店掌开放平台与何方数仓项目相关的 API 探测结论。
- 形成后续把门店客流等数据接入 MySQL 的最小实施方案。
- 明确当前已确认事实、待确认项与未实现项，避免把探测结论误写成已落地能力。

## 2. 已确认事实

### 2.1 文档入口与业务域

- 文档中心入口：<https://docs.open.ovopark.com/documentCore>
- 实际知识库入口：<https://open-wiki.ovopark.com:5443/>
- 当前文档中心可见的主要业务域包括：权限、基础信息、客流、追溯、设备、巡店、云值守、告警、AI场景、CRM、营销、消息中心等。
- 当前与何方项目最直接相关的数据域，优先级初判如下：
  - P1：客流
  - P1：基础信息（门店、组织架构）
  - P2：设备
  - P2：CRM
  - P3：巡店、告警、AI场景

### 2.2 API 公共请求参数

根据“公共请求参数”文档，调用开放平台 API 时至少包含以下字段：

| 参数 | 是否必填 | 说明 |
|---|---|---|
| `_aid` | 是 | 从开放平台应用获取的 AppId |
| `_akey` | 是 | 开放平台分配的开发者 key |
| `_mt` | 是 | 接口名称 |
| `_sm` | 是 | 签名算法，文档显示支持 `md5`、`sha1` |
| `_requestMode` | 是 | 请求方式，文档示例为 `POST` |
| `_version` | 是 | 版本号，文档示例为 `v1` |
| `_timestamp` | 是 | 时间戳，格式为 `yyyyMMddHHmmss` |
| `_sig` | 是 | 签名值 |
| `_format` | 否 | 返回格式，当前文档说明默认仅支持 `json` |

### 2.3 `_sig` 生成规则

根据“_sig参数说明”文档，签名规则已确认如下：

1. 将请求体参数中除 `_sig` 以外的所有参数加入集合。
2. 按参数名升序排序。
3. 将排序后的“参数名 + 参数值”依次拼接成字符串。
4. 在拼接串头尾各加一次应用的 `AccessKey Secret`。
5. 对最终字符串做 `MD5` 加密并转大写，得到 `_sig`。

文档示例里出现了方法名 `open.shopweb.security.mobileLogin`，说明平台至少存在一个登录相关接口，用于换取万店掌用户 token；但本轮尚未展开读取“权限 API”或登录接口的完整字段说明。

### 2.4 应用接入文档当前确认范围

- “应用接入”文档主要描述的是万店掌页面中转与单点跳转参数。
- 已确认参数包含：`token`、`ssoToken`、`groupId`、`client`、`shopId`、`target`。
- 该文档更偏页面跳转接入，不等同于后台数据拉取 API 的请求鉴权文档。

### 2.5 控制台实证结果

- 已使用提供账号登录开放平台控制台，当前账号可正常进入 `控制台 -> 应用中心`。
- 当前账号控制台状态显示为“试用版”，且消息通知中可见“企业认证通知 - 通过审核”。
- 当前账号下已存在 1 个应用：`tableau_bi`。
- 控制台中可见该应用的 `AppID` 为 `DC-000698`。
- 控制台中可见该应用已配置 `AccessKey ID` 与 `AccessKey Secret`，但页面默认只显示脱敏值；本轮未在仓库中记录任何完整凭据。
- 通过控制台前端内部接口 `getDeveloperAppList` 实测可确认：`tableau_bi` 存在一组独立的应用级 `AccessKey ID / Secret`；它与当前开发者账号自身的 `applicationKey / applicationSecret` 不是同一组凭据。
- 当前应用标签显示为 `bi客流`，与实际授权列表一致，说明这是一个以客流分析为主的应用，而不是全量开放平台应用。

### 2.6 当前应用已授权 API 范围

- 在本轮进入 `编辑APP` 页面并实际勾选、提交后，控制台 `API列表` 显示当前应用共授权 88 个接口。
- 当前应用仍以“客流”域接口为主体，但已从“只有客流接口”扩展到“客流 + 鉴权 + 用户组织树 + 门店基础信息 + 部分追溯只读接口”。
- 已确认保留授权的核心客流方法包括：
   - `open.shopweb.passengerFlow.getPassengerIndicatorData`
   - `open.shopweb.passengerFlow.getPassengerIndicatorDataTrend`
   - `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData`
   - `open.passengerflow.getManyShopsPassengerIndicatorData`
   - `open.shopweb.passengerFlow.getManyStoresFlowGroupDistribution`
   - `open.passengerflow.getPassengerFlowEventPage`
- 本轮已补开的关键鉴权 / 基础信息方法包括：
   - `open.shopweb.security.mobileLogin`
   - `open.shopweb.privilege.getToken`
   - `open.organize.departments.getDepartments`
   - `open.gateway.authentication`
   - `open.gateway.getBusinessOrg`
- 本轮已补开的组织 / 用户 / 门店枚举方法包括：
   - `open.shopweb.organize.getOrganizesTrees`（v1 / v2）
   - `open.shopweb.organize.getTreeNode`
   - `open.shopweb.organize.allOrganizes`
   - `open.shopweb.user.getUsersByEnterprise`
   - `open.shopweb.user.getUsersByDepId`
   - `open.shopweb.user.getUserDetails`
   - `open.shopweb.user.getUserByType`
   - `open.shopweb.departments.getDepartmentByShopIdsOrIds`
   - `open.shopweb.departments.getDeptListByPage`
   - `open.shopweb.departments.getDepsHavingFlowDevice`
   - `open.shopweb.departments.getShopManager`
   - `open.shopweb.departments.getManagerByDep`
   - `open.shopweb.departments.getSubTagsAndShops`
   - `open.shopweb.departments.getAllDepartmentTypeList`
- 追溯域本轮做过一次“整类勾选”试探，但发现会混入 `send` / `delete` 类明显写接口，因此已回退为显式只读白名单，只保留：
   - `open.ovopark.pos.stockprofitcheckData`
   - `open.ovopark.pos.stockInOutData`
   - `open.ovopark.pos.getMachineIdsList`
   - `open.ovopark.pos.reportSalesHourly`
   - `open.ovopark.storehouse.getSkuType`
   - `open.ovopark.pos.getDictsByTree`
   - `open.ovopark.iposAbnormalOrder.getList`
   - `open.ovopark.pos.stockinoutsummaryData`
   - `open.ovopark.pos.reportSales`
   - `open.ovopark.pos.searchPos`

说明：这意味着当前现成应用已经不再只是“客流类后台拉数”能力，而是已具备“登录换 token -> 拉组织 / 用户 / 门店主数据 -> 拉客流 / 部分追溯数据”的最小探测闭环。

### 2.7 当前最相关的已确认方法名

根据知识库文档，当前与“何方门店客流入仓”最相关的接口如下：

| 场景 | 方法名 | 说明 |
|---|---|---|
| 用户登录 | `open.shopweb.security.mobileLogin` | 文档显示可用用户名密码登录 |
| 获取企业用户 token | `open.shopweb.privilege.getToken` | 可为企业下某个用户换取 token |
| 门店列表 | `open.organize.departments.getDepartments` | 获取门店接口（新） |
| 单门店 / 单组织指标 | `open.shopweb.passengerFlow.getPassengerIndicatorData` | 获取客流指标数据 |
| 多门店小时指标 | `open.passengerflow.getManyShopsPassengerIndicatorData` | 获取多门店客流小时数据 |
| 多门店小时指标（支持第三方门店编码） | `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData` | 适合跨系统门店编码对接 |

### 2.8 当前仍未完全确认的鉴权点

- 外部技术已明确回复：`authenticator` 首次应从“用户登录”接口获取。
- 也就是说，初始 token 来源是 `open.shopweb.security.mobileLogin`，而不是业务接口调用前人工构造。
- 公开文档示例里“用户登录接口也要求先传 authenticator Header”的写法，与外部技术回复冲突，当前应视为文档模板噪音或过期示例，不宜继续按该示例做首登判断。
- SDK 文档里出现的 `open.gateway.authentication` 仍可视为补充线索，但在已有外部技术确认的前提下，不再把它当作当前主链路真值源。
- 当前可确认：
   - Query 参数层使用 `_aid`、`_akey`、`_sig`。
   - Header 层使用 `authenticator`。
   - `authenticator` 初始值来自“用户登录”接口返回。

### 2.9 当前对 `authenticator` 的收口结论

- 依据外部技术回复，`authenticator` 是根据账号生成的 token。
- 外部技术明确说明：账号不变，`authenticator` token 不变。
- 因此当前最稳妥的落地判断是：
   - 首次通过 `open.shopweb.security.mobileLogin` 获取 `authenticator`；
   - 后续业务接口都复用该账号级 token；
   - 不需要把 `authenticator` 当成高频刷新令牌来设计调度链路，除非后续实测发现平台行为与回复不一致。

### 2.10 外部技术回复已确认的权限边界

- API 权限由开放平台应用侧手工勾选，不是平台自动全开。
- 本轮已实操验证：
   - `mobileLogin`、`getToken`、`departments`、`gateway.authentication` 确实都在当前应用编辑页可选；
   - 勾选并提交后，它们已能在 `API列表` 中检索到；
   - 这说明当前应用的权限缺口已经从“未开通”推进为“已开通，可直接进入真实调用验证”。
- 同时也验证出另一个边界：
   - 某些数据域分类（例如追溯）并不等于“纯读接口集合”；
   - 若整类全开，可能会混入 `send`、`delete` 等写接口；
   - 因此后续继续扩权时，更稳妥的方式是“按显式方法名白名单补勾”，而不是一律整类全选。

### 2.11 外部技术回复已确认的门店编码边界

- 门店编码映射需要由何方自行维护。
- 是否能直接用何方门店编码调用，取决于具体客流接口是否支持第三方门店编码。
- 外部技术已明确提示：大部分接口只能根据万店掌自己的门店 `shopId` 获取数据。
- 因此，后续接入设计不能默认“所有客流接口都能直接吃我方门店编码”。
- 当前最合理的设计是：
   - 优先选择明确支持第三方门店编码的接口；
   - 对不支持第三方编码的接口，维护一张“何方门店编码 -> 万店掌 shopId”映射表。

### 2.12 本轮真实调用验证结果

- 已使用控制台实测反查出的应用级 `AccessKey ID / Secret` 对 `cloud.api` 直接发起签名请求，确认签名链路可达，不再停留在“文档推断”。
- 对 `open.shopweb.security.mobileLogin` 使用 `userName=18617002344`、`password=hefang.1234` 实测，返回业务错误码 `103095 / PASSWORD_ERROE`，说明当前提供的控制台登录口令不能直接作为 `mobileLogin` 的业务登录密码。
- 对 `open.shopweb.security.mobileLogin` 使用 `userName=hefang`、`password=hefang.1234` 实测，返回 `103094 / USERNAME_ERROE`，说明该接口当前识别的用户名口径不是开放平台昵称 `hefang`，而更接近手机号 / 业务登录名。
- 将当前控制台登录态里的 `ticket` 直接放入 `authenticator` 请求头调用 `open.organize.departments.getDepartments`，返回 `9990001 / TOKEN_NOT_EXIST`，说明控制台登录票据不能直接冒充业务接口 `authenticator`。
- 综上，本轮阻塞已进一步收口为：
   - 应用级 `AppId / AccessKey / Secret` 已确认可用；
   - API 权限已确认可用；
   - 但缺少一组真正能通过 `mobileLogin` 的万店掌业务账号密码，因此暂时无法继续拉取 `getDepartments` 与客流样本。

### 2.13 docx 与主线登录页新增证据

- 用户转述万店掌外部技术最新回复：`mobileLogin` 需要使用“登录主线的后台主线账号密码”，并明确要求参考 `开放平台对接.docx`。
- 本轮已解析 `docs/万店掌API接入-子项目资料/开放平台对接.docx`，其中写明：
   - `enterpriseId(groupId)` 可以在 `open.shopweb.security.mobileLogin` 的返回中获取；
   - 用户登录接口中的用户名和密码，使用的是“登录主线：`https://www.ovopark.com/login` 的账号和密码”。
- 本轮已打开 `https://www.ovopark.com/login/mobile` 实测页面，确认该页面是独立的用户名 / 密码登录入口，不是开放平台控制台页面。
- 当前浏览器里的主线登录页正文中出现了一个疑似历史用户名 `18551288127`，但尚无证据证明它就是本项目后续联调可用的正式账号，因此当前只能把它视为候选线索，不能直接当成已确认事实。
- 因此，当前最准确的阻塞描述应更新为：
   - 不是缺“开放平台控制台密码”；
   - 而是缺“登录主线 `ovopark.com/login` 对应后台账号的正确密码”；
   - 若 `18551288127` 就是该主线账号，则当前仅剩密码缺口待补。

### 2.14 主线账号实调已打通，下游样本已拿到

- 用户本轮提供了一组真实主线后台账号密码，已通过在线调试成功触发 `open.shopweb.security.mobileLogin`，调试器自动写入可用的 `Ovo-Authorization` 请求头；本轮文档只记录“已成功拿到可用授权”，不写入真实 token 或密码。
- `open.organize.departments.getDepartments` 已实测成功：
   - 默认请求返回 `total=64`、`rows=10`；
   - `pageNumber=2,pageSize=5` 返回 5 条样本，说明分页参数有效；
   - `pageSize=100` 返回 `rows=64`，说明当前何方租户在万店掌里可见门店总数为 64。
- `getDepartments` 当前已确认的稳定字段包括：`id`、`name`、`organizeId`、`organizeName`、`depOrganizeId`、`openStatus`、`validateDate`、`longitude`、`latitude` 等。
- `getDepartments` 全量 64 家门店中，`shopId` 与 `trilateralId` 当前均为空，说明“接口支持第三方门店编码”并不等于“当前租户已经配置了第三方门店编码”。
- `open.shopweb.passengerFlow.getPassengerIndicatorData(depId=174679,startTime=2026-05-10 00:00:00,endTime=2026-05-10 23:59:59)` 已实测成功，返回指标包括：`passengerFlow=113`、`passPassengerFlow=1275`、`outsidePassengerFlow=1388`、`inShopRate=8.14`、`outFlowCount=195`。
- `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData(id=S_174679,startTime=2026-05-10 00:00:00,endTime=2026-05-10 23:59:59,timeType=1)` 已实测成功，返回 `name=北京国贸店`、`depId=174679`、`shopId=""` 以及按小时展开的 `dataList`。
- 综上，当前最可靠的接入事实已经从“缺密码、缺 authenticator”推进为：
   - 主线账号登录链路已打通；
   - 门店主数据和客流样本都已拿到；
   - 但标准入仓路径当前仍应以万店掌内部 `depId` / `S_门店id` 为主，第三方 `shopId` 路径仍缺实值证据。

## 3. 与何方项目的接入关系

### 3.1 现有 ODS 接入骨架

当前仓库已有成熟的“外部源 -> ODS -> 统一调度”骨架，可复用：

- MySQL 与 Oracle 连接配置统一在 `config.py`、`db_connections.py`
- ODS 单表脚本样式可参考 `etl_ods_m_retail.py`
- ODS 统一调度入口可参考 `run_ods.py`
- 增量同步状态依赖 `ods_sync_state`

### 3.2 万店掌接入建议

以下为未实现的候选方案，需在获取真实应用凭据与目标接口字段后再落地：

1. 若只落客流数据，当前 `tableau_bi` 应用已经具备“登录 + 门店主数据 + 客流接口”最小链路，可直接进入 ODS 设计阶段。
2. 若还要同时落门店主数据，当前应用也已补齐 `open.organize.departments.getDepartments`，并已拿到全量 64 家样本，可直接进入字段字典整理。
3. 新增独立子链路，不直接混入现有 Oracle ODS 同步。
4. 采用完整 `ODS - DWD - DWS - DIM - ADS` 五层设计：
   - DIM：`dim_ovopark_shop_mapping` 承接何方门店与万店掌 `depId / S_门店id` 的映射，以及 62 条 exact 命中的初始 seed 候选。
   - ODS Raw：保存原始响应 JSON、请求时间、分页信息、门店维度、接口名。
   - ODS Clean：按稳定字段拆平，例如门店当前快照、门店日级客流、门店小时客流。
   - DWD：将 ODS 客流与已确认门店映射压平成“何方门店 x 万店掌门店 x 日期”的可追溯事实表。
   - DWS：按门店汇总为日表与月表，供下游专题和宽表复用。
   - ADS：输出门店月客流经营宽表，供经营分析与 Tableau 使用。
5. 在接口选型上优先级应调整为：
   - 已实测可用的内部 ID 路径：`open.shopweb.passengerFlow.getPassengerIndicatorData`
   - 已实测可用的内部 `S_门店id` 小时接口：`open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData`
   - 门店基础信息接口 `open.organize.departments.getDepartments`
   - 第三方 `shopId` 路径：仅在平台侧补齐门店编码后再启用
6. 优先接入两个主题：
   - 客流基础数据
   - 门店基础信息
7. 调度上建议先独立脚本运行，再视稳定性决定是否并入 `run_ods.py` / `run_etl.py`。

## 4. MySQL 落地候选设计

以下均为候选对象；本轮已将草案落盘到未执行 SQL / ETL 文件，但尚未执行任何 DDL / DML：

- `SQL/draft_create_dim_ovopark_shop_mapping.sql`
- `SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql`
- `SQL/draft_create_ods_ovopark_tables.sql`
- `SQL/draft_create_dwd_ovopark_passenger_flow_daily.sql`
- `SQL/draft_create_dws_ovopark_passenger_flow_daily.sql`
- `SQL/draft_create_dws_ovopark_passenger_flow_monthly.sql`
- `SQL/draft_create_ads_ovopark_store_monthly.sql`

### 4.1 候选门店映射维表

`dim_ovopark_shop_mapping`

设计口径：

- 粒度：`何方门店编码 x 万店掌 dep_id x 生效开始日`
- 主接入键：`ovopark_dep_id` + `ovopark_dep_key = S_<dep_id>`
- 作用：当第三方 `shopId` 仍为空时，承接何方门店到万店掌内部 ID 的人工或半自动映射
- 关键字段：
   - 何方侧：`hefang_store_id`、`hefang_store_code`、`hefang_store_name`
   - 万店掌侧：`ovopark_dep_id`、`ovopark_dep_key`、`ovopark_shop_name`、`ovopark_organize_id`、`ovopark_organize_name`、`ovopark_dep_organize_id`
   - 预留三方编码：`ovopark_shop_id`、`ovopark_trilateral_id`
   - 维表治理：`mapping_status`、`match_source`、`effective_start_date`、`effective_end_date`、`is_current`、`confirmed_by`、`confirmed_at`

说明：该表按 SCD2 草案设计，避免门店迁店、重开、组织调整时覆盖历史映射。

补充说明：基于 `reports/context_cache/ovopark_dim_store_initial_match_20260511.csv` 已额外生成 `SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql`，其中包含 62 条 `exact_name / exact_name_area` 初始映射 seed；该文件仍是 draft，执行前需人工复核。

### 4.2 候选原始响应表

`ods_ovopark_api_raw`

设计口径：

- 粒度：一次接口请求一行
- 用途：保留脱敏请求参数与完整响应 JSON，支撑调试、补数和字段回放
- 关键字段：`api_name`、`request_object_type`、`request_object_key`、`request_page_number`、`request_page_size`、`request_window_start`、`request_window_end`、`request_param_json`、`response_stat_code`、`response_total`、`response_row_count`、`gateway_request_id`、`response_json`

说明：安全边界上不落 `authenticator` / `Ovo-Authorization` 原文，只保留脱敏请求参数和请求关键键值。

### 4.3 候选门店当前快照表

`ods_ovopark_shop`

设计口径：

- 粒度：`dep_id` 当前快照一行
- 主键：`dep_id`
- 主接入键：`dep_id` 与 `dep_key = S_<dep_id>`
- 关键字段：`shop_name`、`address`、`organize_id`、`organize_name`、`dep_organize_id`、`group_id`、`shop_id`、`trilateral_id`、`open_status`、`validate_status`、`validate_date`、`longitude`、`latitude`、`dev_count`

说明：当前 64 家样本里 `shopId` 与 `trilateralId` 均为空，因此这张表的主口径仍是内部 `dep_id`，不是第三方门店编码。

### 4.4 候选日级客流表

`ods_ovopark_passenger_flow_daily`

设计口径：

- 粒度：`date_id x dep_id x is_on_business_time`
- 来源接口：`open.shopweb.passengerFlow.getPassengerIndicatorData`
- 关键字段：`passenger_flow`、`outside_passenger_flow`、`in_shop_rate`、`out_flow_count`、`dressing_rate`、`pass_passenger_flow`、`dressing_passenger_flow`
- 请求证据字段：`request_window_start`、`request_window_end`、`requested_at`

说明：该表面向标准“单门店单日”重算；若后续需要任意时间窗分析，应回看 `ods_ovopark_api_raw`，而不是扩大这张表的唯一键。

### 4.5 候选小时级客流表

`ods_ovopark_passenger_flow_hourly`

设计口径：

- 粒度：`biz_date_id x stat_time x dep_id x time_type x request_object_type`
- 来源接口：`open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData`
- 主接入键：`dep_id / dep_key`，请求根对象可为 `S_<dep_id>` 或 `O_<organize_id>`
- 关键字段：`shop_name`、`shop_id`、`passenger_flow`、`pass_passenger_flow`、`in_count_having_pass_device`、`outside_passenger_flow`、`in_shop_rate`、`out_flow_count`、`dressing_rate`、`duplicated_flow`

说明：样本已证明该接口在 `id=S_174679,timeType=1` 下可返回小时数组，但响应中的 `shopId` 当前仍为空，因此小时表同样不应默认依赖第三方门店编码。

### 4.6 候选 DWD 日事实表

`dwd_ovopark_passenger_flow_daily`

设计口径：

- 粒度：`date_id x store_id x ovopark_dep_id x is_on_business_time`
- 作用：把万店掌 ODS 日级客流与 `dim_ovopark_shop_mapping` 当前有效映射压平成何方门店事实
- 关键字段：`store_id`、`store_code`、`store_name`、`area_name`、`ovopark_dep_id`、`ovopark_dep_key`、`mapping_status`、`match_source`、`passenger_flow`、`outside_passenger_flow`、`pass_passenger_flow`、`source_request_window_start`、`source_requested_at`

说明：该层不再直接暴露“未映射 dep_id”，只有 `MATCHED` 且当前有效的门店才允许进入 DWD。

### 4.7 候选 DWS 日聚合表

`dws_ovopark_passenger_flow_daily`

设计口径：

- 粒度：`date_id x store_id`
- 作用：沉淀门店全天 / 营业时间客流、店外客流、过店客流、试衣客流等日级聚合指标
- 关键字段：`all_day_passenger_flow`、`business_time_passenger_flow`、`all_day_outside_passenger_flow`、`all_day_in_shop_rate`、`covered_dep_count`、`source_dwd_row_count`

说明：该层统一按日级门店汇总，供月表、日报和其它经营专题复用。

### 4.8 候选 DWS 月聚合表

`dws_ovopark_passenger_flow_monthly`

设计口径：

- 粒度：`report_date x data_version x target_year x target_month x store_id`
- 作用：沉淀门店月累计客流、月均日客流、进店率、试衣率与覆盖率
- 关键字段：`month_passenger_flow`、`month_business_time_passenger_flow`、`month_avg_daily_passenger_flow`、`month_in_shop_rate`、`days_with_data`、`calendar_day_count`、`data_coverage_rate`

说明：`report_date` 代表当月观测截点，便于与现有 ADS 月表风格对齐。

### 4.9 候选 ADS 月宽表

`ads_ovopark_store_monthly`

设计口径：

- 粒度：`report_date x data_version x target_year x target_month x store_id`
- 作用：在月客流基础上补齐负责人、渠道类型、门店等级、免税标识、万店掌 dep_id 等经营分析字段
- 关键字段：`owner_name`、`report_channel_type`、`store_grade`、`is_duty_free`、`ovopark_dep_id`、`month_passenger_flow`、`month_avg_daily_passenger_flow`、`data_coverage_rate`

说明：用户要求的“月客流进入 ADS”在该层交付，但计算路径必须经过 ODS/DWD/DWS，而不是 ODS 直接聚到 ADS。

### 4.10 设计收口结论

- 当前 ODS 主键应站在万店掌内部 ID 视角，而不是第三方 `shopId` 视角。
- `dim_ovopark_shop_mapping` 是当前接入设计的必备前置，而不是可选优化项。
- 用户当前已明确要求走完整 `ODS - DWD - DWS - DIM - ADS` 链路，因此“直接从 ODS 聚月到 ADS”不再是可接受方案。
- 若后续平台侧补齐 `shopId` / `trilateralId` 实值，可在不推翻现有 ODS 主键的前提下，把它们作为增强映射字段补入。

## 5. 建议的实施顺序

1. 当前应用 `tableau_bi` 已确认存在，且需显式区分“应用级 `AppId / AccessKey ID / Secret`”与“开发者账号级 `applicationKey / applicationSecret`”；前者用于本应用 API 调用，后者不应混用到 `mobileLogin` / `getDepartments` 链路。
2. 当前应用已补勾 `open.shopweb.security.mobileLogin`、`open.shopweb.privilege.getToken`、`open.organize.departments.getDepartments`、`open.gateway.authentication` 等关键权限。
3. 已确认一组真实主线后台账号可以通过 `open.shopweb.security.mobileLogin`，当前阶段不再缺业务登录凭据。
4. 继续沿当前成功链路沉淀接口事实：
   - `open.organize.departments.getDepartments` 已返回 `total=64` 的门店主数据样本；
   - `open.shopweb.passengerFlow.getPassengerIndicatorData` 已返回单门店日级指标；
   - `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData` 已返回小时级 `dataList`。
5. 优先走当前已授权且已实测成功的客流接口，先做字段字典和 ODS 建模：
   - `open.shopweb.passengerFlow.getPassengerIndicatorData`
   - `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData`
6. 由于 `getDepartments` 全量样本中的 `shopId` 与 `trilateralId` 目前均为空，先评审并人工执行 `dim_ovopark_shop_mapping` DDL 与 exact seed 草案。
7. 再人工执行 ODS / DWD / DWS / ADS 各层 draft DDL：
   - `SQL/draft_create_ods_ovopark_tables.sql`
   - `SQL/draft_create_dwd_ovopark_passenger_flow_daily.sql`
   - `SQL/draft_create_dws_ovopark_passenger_flow_daily.sql`
   - `SQL/draft_create_dws_ovopark_passenger_flow_monthly.sql`
   - `SQL/draft_create_ads_ovopark_store_monthly.sql`
8. 先跑独立脚本做最小验证：
   - `etl_ods_ovopark_shop.py --conn-test`
   - `etl_ods_ovopark_passenger_flow.py --conn-test`
   - `etl_dwd_ovopark_passenger_flow_daily.py --conn-test`
   - `etl_dws_ovopark_passenger_flow.py --stage daily --conn-test`
   - `etl_dws_ovopark_passenger_flow.py --stage monthly --conn-test`
   - `etl_ads_ovopark_store_monthly.py --conn-test`
9. 再按小窗口、短日期范围逐层执行 `--execute`，优先验证 1 天 ODS、1 天 DWD、1 天 DWS、1 个 report_date 的月表与 ADS。
10. 用业务快照和样本门店对账月客流后，再决定是否并入 `run_ods.py` / `run_etl.py` 或保持独立调度。

## 6. 当前待确认项

- 已确认：当前账号下存在可用应用 `tableau_bi`，`authenticator` 首次来自用户登录接口。
- 已确认：当前应用已实际补开 `mobileLogin`、`getToken`、`departments`、`gateway.authentication` 等关键权限，控制台 `API列表` 当前为 88 条。
- 已确认：控制台开发者账号自身的 `applicationKey / applicationSecret` 与 `tableau_bi` 的应用级 `AccessKey ID / Secret` 不是同一组凭据。
- 已确认：用户提供的一组主线后台账号可以真实通过 `mobileLogin`，并成功换到可用授权头。
- 待确认：客流接口是按日、按小时，还是支持更细粒度事件级明细。
- 待确认：`open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData` 中可选 `shopId` 的“第三方门店编码”字段格式与我方门店编码是否完全兼容；当前租户样本里该字段仍为空。
- 待确认：门店编码是否能与何方现有 `dim_store` 直接映射，还是需要中间映射表。
- 待确认：接口频控、分页上限、历史回溯窗口、是否支持按更新时间增量拉取。
- 待确认：客流 API 属于免费类、基础类还是增值类接口，是否会影响批量拉数频次。
- 待确认：账号不变 token 不变这一规则，是否跨密码修改、权限变更、企业切换等场景仍然成立。
- 待确认：本轮新增的组织、门店、追溯只读接口分别返回哪些字段、是否要求额外 role / group / org 参数，以及是否存在增值包限制。
- 待确认：设备、告警、AI场景、CRM 等其它数据域中，哪些接口值得继续扩权，哪些分类会混入明显写接口。

## 7. 已获得的外部确认

已由万店掌外部技术明确回复如下：

1. `authenticator` 从文档里的“用户登录”接口获取。
2. API 权限由应用侧在开放平台手工勾选。
3. 调用示例文档和沙箱调试已提供。
4. `authenticator` 是根据账号生成的，账号不变 token 不变。
5. 门店接口权限也需要在开放平台手工勾选。
6. 门店编码需由何方自行维护；是否支持门店编码，要看具体客流接口，大部分接口只能按万店掌自己的门店 `shopId` 调用。

## 8. 建议的后续动作

当前不再优先对外追问“`authenticator` 从哪里来”或“哪组账号能登录”，而应直接执行：

1. 先复核并人工执行 `SQL/draft_create_dim_ovopark_shop_mapping.sql` 与 `SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql`，把 62 条 exact 命中转成当前有效映射。
2. 再人工执行 ODS / DWD / DWS / ADS 五层 draft DDL，完成库表准备。
3. 逐层跑 conn-test 和小范围 `--execute`：先 `etl_ods_ovopark_shop.py`、再 `etl_ods_ovopark_passenger_flow.py`、再 `etl_dwd_ovopark_passenger_flow_daily.py`、再 `etl_dws_ovopark_passenger_flow.py`、最后 `etl_ads_ovopark_store_monthly.py`。
4. 用用户已提供的业务快照，对 `ads_ovopark_store_monthly` 中的月客流做门店级抽样对账，确认 `month_passenger_flow` 与 `days_with_data` 是否符合预期。
5. 若还要继续扩大数据源，优先沿“显式方法名白名单”补开设备、告警、AI场景、CRM 中的读接口，不建议再直接整类全选。

## 9. 安全约束

- 本目录不得落盘 API 账号、密码、`_akey`、`AccessKey Secret`、token、Webhook 或任何真实凭据。
- 凭据只允许放在用户本地环境变量、未纳入 git 的本地配置，或运行时输入中。

## 10. 证据来源

- 万店掌文档中心：<https://docs.open.ovopark.com/documentCore>
- 万店掌知识库“应用接入”：<https://open-wiki.ovopark.com:5443/node/0198c648-bcdb-7c94-8dbf-1283eb960a0e>
- 万店掌知识库“公共请求参数”：<https://open-wiki.ovopark.com:5443/node/0198c64a-fa72-79be-bcff-c2a592853503>
- 万店掌知识库“_sig参数说明”：<https://open-wiki.ovopark.com:5443/node/0198c64d-abf6-7dfe-8d94-82f4f1857e0b>
- 万店掌知识库“SDK使用说明”：<https://open-wiki.ovopark.com:5443/node/0198c652-e101-7ae2-8b3c-b4631906d859>
- 万店掌知识库“用户登录”：<https://open-wiki.ovopark.com:5443/node/019932c1-696f-7478-9878-e8a435bc35e9>
- 万店掌知识库“获取企业用户token”：<https://open-wiki.ovopark.com:5443/node/019932c1-6a10-71da-a1ac-d210e168ab1f>
- 万店掌知识库“获取门店接口”：<https://open-wiki.ovopark.com:5443/node/019932c2-c5e0-7af4-8245-68bcfaa39780>
- 万店掌知识库“获取客流指标数据”：<https://open-wiki.ovopark.com:5443/node/019932c3-de46-724f-a565-bae59c8bd63f>
- 万店掌知识库“获取门店客流指标数据（按小时）”：<https://open-wiki.ovopark.com:5443/node/019932c3-de09-771f-8cf8-c2839f9b3764>
- 万店掌知识库“获取开放平台组织架构_多门店客流小时指标数据（支持第三方门店编码）”：<https://open-wiki.ovopark.com:5443/node/019932c3-de77-78d4-919d-033c99d6b7d6>
- 万店掌控制台“编辑APP -> 开通服务接口”实测勾选结果
- 万店掌控制台“应用中心 -> tableau_bi -> API列表”实测结果
- 万店掌控制台内部接口 `getDeveloperAppList` 实测结果
- 本轮 `cloud.api` 直连实测：`open.shopweb.security.mobileLogin`、`open.organize.departments.getDepartments`
- `docs/万店掌API接入-子项目资料/开放平台对接.docx` 解析结果
- `https://www.ovopark.com/login/mobile` 页面实测结果
- 用户 2026-05-11 提供的万店掌外部技术回复
- 本仓库参考脚本：`config.py`、`db_connections.py`、`etl_ods_m_retail.py`、`run_ods.py`

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.1 | 2026-05-11 | 新增完整 `ODS - DWD - DWS - DIM - ADS` 链路草案：补充 exact 映射 seed、DWD 日事实、DWS 日/月聚合、ADS 月宽表，以及对应独立 ETL 脚本清单 |
| v1.0 | 2026-05-11 | 将 MySQL 落地候选设计收口为已落盘的 DDL 草案，明确 `dim_ovopark_shop_mapping` 与 ODS 四张候选表均以 `depId / S_门店id` 为主接入路径 |
| v0.9 | 2026-05-11 | 补充主线账号成功联调后的真实样本：`mobileLogin`、`getDepartments`、单门店客流与多门店小时客流均已打通，并收口第三方门店编码仍无实值 |
| v0.8 | 2026-05-11 | 根据外部技术回复与 `开放平台对接.docx` 进一步收口：`mobileLogin` 必须使用 `ovopark.com/login` 主线后台账号密码，并补充主线登录页实测线索 `18551288127` |
| v0.7 | 2026-05-11 | 新增真实调用验证结论：已区分应用级 AccessKey 与开发者 key，`mobileLogin` 对控制台口令返回 `PASSWORD_ERROE`，控制台 `ticket` 不能直接充当 `authenticator` |
| v0.6 | 2026-05-11 | 记录本轮已实际勾选并提交的权限：应用授权扩展到 88 个接口，补齐 `mobileLogin/getToken/departments/gateway.authentication`，并将追溯域改为只读白名单 |
| v0.5 | 2026-05-11 | 根据万店掌外部技术回复修正真值源：`authenticator` 由用户登录接口获取，权限需在开放平台手工勾选，门店编码支持取决于具体接口 |
| v0.4 | 2026-05-11 | 收口 `authenticator` 判断：当前最可能是网关 token，公开文档未写完整，建议对外确认其获取方式与授权状态 |
| v0.3 | 2026-05-11 | 补充 SDK 文档线索：`authenticator` 实为 token，SDK 示例使用 `open.gateway.authentication` 获取，但当前应用授权列表未检索到该方法 |
| v0.2 | 2026-05-11 | 补充控制台实证：确认现有应用 `tableau_bi`、`AppID=DC-000698`、44 个已授权客流 API，以及基础信息接口未授权的边界 |
| v0.1 | 2026-05-11 | 初始化万店掌 API 探测与 MySQL 接入方案文档，沉淀已确认的鉴权规则、业务域与候选落地设计 |