# 万店掌API续接上下文

## 当前状态

- 已新建 `docs/万店掌API接入-子项目资料/` 作为万店掌开放平台接入的专题目录。
- 已完成公开文档探测 + 控制台只读探测。
- 已确认当前账号下存在可用应用 `tableau_bi`，`AppID = DC-000698`，并已配置 AccessKey。
- 已在控制台 `编辑APP` 页面实际补勾并提交关键权限，当前 `API列表` 显示共 88 个 API。
- 已确认当前应用不再只是“客流域”授权，而是已补齐登录、token、组织树、门店主数据和一组追溯只读接口。
- 已进入真实 API 调用验证，并已确认应用级 AccessKey 可用、签名链路可达。
- 已用用户提供的主线后台账号成功触发 `open.shopweb.security.mobileLogin` 在线调试，调试器已自动写入可用的 `Ovo-Authorization` 请求头，说明 authenticator 主链已打通。
- 已用该授权成功调用 `open.organize.departments.getDepartments`：默认返回 10 条，`pageNumber/pageSize` 参数生效，`pageSize=100` 时返回 `total=64 / rows=64`。
- 已用 `depId=174679` 成功调用 `open.shopweb.passengerFlow.getPassengerIndicatorData`，确认单门店日级客流指标结构可取。
- 已用 `id=S_174679`、`timeType=1` 成功调用 `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData`，确认小时粒度多门店接口可走内部门店 ID 路径。
- 已在仓库中落盘完整 `ODS - DWD - DWS - DIM - ADS` 草案：包含 Ovopark API 客户端、ODS 门店/客流脚本、DWD 日事实脚本、DWS 日/月聚合脚本、ADS 月宽表脚本，以及对应 draft SQL。
- 已从 `reports/context_cache/ovopark_dim_store_initial_match_20260511.csv` 自动生成 `SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql`，共 62 条 exact 映射 seed 候选。
- 当前新的核心缺口不再是账号密码，而是两件事：`getDepartments` 全量 64 家门店中 `shopId` 与 `trilateralId` 仍为空，且完整链路虽然已落盘 draft，但仍待人工 apply 和首轮执行验证。

## 已完成探测

### 1. 文档入口

- 文档中心：<https://docs.open.ovopark.com/documentCore>
- 知识库：<https://open-wiki.ovopark.com:5443/>

### 2. 已确认文档

- 应用接入
- 公共请求参数
- `_sig参数说明`
- 用户登录
- 获取企业用户token
- 获取门店接口
- 获取客流指标数据
- 获取门店客流指标数据（按小时）
- 获取开放平台组织架构_多门店客流小时指标数据（支持第三方门店编码）

### 3. 已确认结论

- API 调用需要 `_aid`、`_akey`、`_mt`、`_sm`、`_requestMode`、`_version`、`_timestamp`、`_sig`。
- `_sig` 规则是“去掉 `_sig` 后按参数名升序拼接，再在首尾加 `AccessKey Secret`，最后 `MD5` 大写”。
- 文档目录中存在“基础信息”“客流”等业务域，说明平台具备门店与客流类开放能力。
- `应用接入` 文档描述的是页面跳转 / SSO 场景，不应直接当作后台数据接口文档。
- 当前应用 `tableau_bi` 已授权 `open.shopweb.passengerFlow.getPassengerIndicatorData`。
- 当前应用 `tableau_bi` 已授权 `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData`，该接口文档明确写了“支持第三方门店编码”。
- 外部技术已明确：`authenticator` 首次从 `open.shopweb.security.mobileLogin` 获取，且账号不变 token 不变。
- 当前应用已在控制台实际补勾并提交：
	- `open.shopweb.security.mobileLogin`
	- `open.shopweb.privilege.getToken`
	- `open.organize.departments.getDepartments`
	- `open.gateway.authentication`
	- `open.gateway.getBusinessOrg`
- 当前应用还补齐了一组组织 / 用户 / 门店枚举接口，以及一组追溯只读接口。
- 追溯域做过一次整类勾选试探，但发现会混入 `send` / `delete` 类写接口，因此最终回退为显式只读白名单，而不是整类全开。
- 已通过控制台内部接口 `getDeveloperAppList` 实测确认：`tableau_bi` 的应用级 `AccessKey ID / Secret` 与开发者账号自身的 `applicationKey / applicationSecret` 不是同一组凭据。
- 已用应用级 `AccessKey ID / Secret` 对 `cloud.api` 直连实测：
	- `open.shopweb.security.mobileLogin(userName=18617002344, password=hefang.1234)` 返回 `103095 / PASSWORD_ERROE`
	- `open.shopweb.security.mobileLogin(userName=hefang, password=hefang.1234)` 返回 `103094 / USERNAME_ERROE`
	- `open.organize.departments.getDepartments` 若把当前控制台 `ticket` 直接放进 `authenticator` 请求头，会返回 `9990001 / TOKEN_NOT_EXIST`
- 已解析 `开放平台对接.docx`，文档明确说明：`mobileLogin` 的用户名和密码使用的是“登录主线 `https://www.ovopark.com/login` 的账号和密码”。
- 已打开主线登录页 `https://www.ovopark.com/login/mobile` 实测，确认页面为独立的用户名 / 密码登录入口；页面正文中出现一个疑似历史用户名 `18551288127`，但当前仍未确认该号码是否就是本项目后续联调应使用的正式后台账号。
- 已用用户提供的主线后台账号成功完成 `open.shopweb.security.mobileLogin` 在线调试，调试器自动生成的 `Ovo-Authorization` 请求头随后可直接复用到下游调试接口。
- `open.organize.departments.getDepartments` 已实测成功：
	- 默认请求返回 `total=64`、`rows=10`
	- `pageNumber=2,pageSize=5` 时返回 5 条样本，说明分页参数有效
	- `pageSize=100` 时返回 `rows=64`，说明当前何方租户可见门店总数为 64
- `getDepartments` 响应中当前已确认的稳定字段包括：`id`、`name`、`organizeId`、`organizeName`、`depOrganizeId`、`openStatus`、`validateDate`、`longitude`、`latitude` 等。
- `getDepartments` 全量 64 家门店中，`shopId` 与 `trilateralId` 当前均为空，说明“支持第三方门店编码”是接口能力，不等于当前租户已经配置了第三方门店编码。
- `open.shopweb.passengerFlow.getPassengerIndicatorData(depId=174679,startTime=2026-05-10 00:00:00,endTime=2026-05-10 23:59:59)` 已实测成功，返回示例指标包括：`passengerFlow=113`、`passPassengerFlow=1275`、`outsidePassengerFlow=1388`、`inShopRate=8.14`、`outFlowCount=195`。
- `open.shopwebpassengerflow.customerflow.getManyShopsPassengerIndicatorData(id=S_174679,startTime=2026-05-10 00:00:00,endTime=2026-05-10 23:59:59,timeType=1)` 已实测成功，返回 `name=北京国贸店`、`depId=174679`、`shopId=""` 以及按小时展开的 `dataList`。
- 当前阶段已经可以明确：
	- 凭据阻塞已解除；
	- 门店主数据与客流样本已取到；
	- 当前真正待收口的是“何方门店编码如何映射到万店掌内部 `depId` / `S_门店id`，以及第三方 `shopId` 为什么为空”。

## 下一轮优先动作

1. 人工复核并执行 `SQL/draft_create_dim_ovopark_shop_mapping.sql` 与 `SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql`，先把 62 条 exact 命中转为当前有效映射。
2. 再人工执行 Ovopark 全链路 draft DDL：ODS / DWD / DWS / ADS 五层表结构。
3. 逐层跑 `--conn-test` 与小范围 `--execute`：
	- `etl_ods_ovopark_shop.py`
	- `etl_ods_ovopark_passenger_flow.py`
	- `etl_dwd_ovopark_passenger_flow_daily.py`
	- `etl_dws_ovopark_passenger_flow.py --stage daily`
	- `etl_dws_ovopark_passenger_flow.py --stage monthly`
	- `etl_ads_ovopark_store_monthly.py`
4. 用业务快照与样本门店核对 `ads_ovopark_store_monthly.month_passenger_flow`、`days_with_data`、`data_coverage_rate`。
5. 若还要继续扩大数据源，再按显式方法名补开设备 / 告警 / AI场景 / CRM 中的只读接口。

## 未实现项提醒

- 当前已新增 Ovopark Python 脚本与 draft SQL，但尚未执行任何 CREATE / ALTER / INSERT / UPDATE / DELETE。
- 当前文档内提到的 ODS / DWD / DWS / ADS 文件均已落盘为 draft，实现状态是“代码已就绪、数据库未 apply、链路未首跑”。

## 风险点

- 风险 1：当前 64 家门店样本里 `shopId` 与 `trilateralId` 均为空，说明第三方门店编码虽然在接口能力层受支持，但在当前租户数据里还没有落到可直接复用的实值。
- 风险 2：已跑通的标准客流接口当前都依赖万店掌内部 `depId` 或 `S_门店id`；若不先建立映射，后续入仓仍无法直接与何方现有门店主数据对齐。
- 风险 3：客流接口属于按时间窗取数的基础类接口，批量回溯历史数据前仍需确认频控、并发与抓数窗口策略。
- 风险 4：追溯等外延数据域若继续按整类勾选推进，会混入明显写接口；后续扩权仍应坚持显式白名单。
- 风险 5：`SQL/draft_seed_dim_ovopark_shop_mapping_exact_20260511.sql` 中 62 条 exact seed 仍来自自动初配结果，执行前必须人工复核，尤其是跨区同名门店。

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-05-11 | 记录 Ovopark 完整 `ODS - DWD - DWS - DIM - ADS` draft 已落盘，并将下一步收口到人工 apply 映射 seed、执行全链路 conn-test 与小窗口首跑 |
| v0.9 | 2026-05-11 | 记录主线账号实调已打通，补充 `getDepartments`、日级客流、小时客流的真实样本结果，并将核心缺口收口为门店编码映射 |
| v0.8 | 2026-05-11 | 补充 `开放平台对接.docx` 和主线登录页实测证据，明确 `mobileLogin` 必须使用 `ovopark.com/login` 主线后台账号密码 |
| v0.7 | 2026-05-11 | 记录真实调用验证：应用级 AccessKey 已反查确认，控制台口令对 `mobileLogin` 返回 `PASSWORD_ERROE`，控制台 `ticket` 不能直接用作 `authenticator` |
| v0.6 | 2026-05-11 | 记录本轮已实操补开关键权限并提交成功：当前 API 列表 88 条，后续重心切到真实登录与样本取数 |
| v0.5 | 2026-05-11 | 根据外部技术回复修正：`authenticator` 从 `mobileLogin` 获取，权限需手工勾选，多数客流接口需用万店掌 `shopId` |
| v0.4 | 2026-05-11 | 收口结论：`authenticator` 仍需外部确认获取方式，未确认前不建议继续实现调用脚本 |
| v0.3 | 2026-05-11 | 补充 SDK 对 `authenticator` 的说明，并记录 `open.gateway.authentication` 当前未在应用授权列表中检索到 |
| v0.2 | 2026-05-11 | 补充控制台实证、现有应用授权范围、已确认可用客流方法与权限缺口 |
| v0.1 | 2026-05-11 | 初始化万店掌 API 续接上下文，记录首轮文档探测范围、当前状态与下一轮动作 |