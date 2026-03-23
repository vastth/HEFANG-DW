# 数云 CRM 实施上下文与下一步执行入口

> 状态：待实施
> 创建日期：2026-03-19
> 当前版本：v2.8
> 用途：本文件是数云 CRM 主题的跨对话上下文主文件；切换到新对话窗口时，优先提供本文件即可恢复当前阶段事实、进度、风险与下一步实现入口。
> 说明：本计划已按当前仓库结构校正，不再直接沿用 R10 中与现状不一致的目录和主键假设。

## 目标

何方珠宝需要对接数云 CRM 系统，将数云当前落在 `hfsy` 库中的会员、绑定、订单、京东 pin-xid 映射等表，清洗入 `hefang_dw` 的 DWD 层。核心能力包括：

- AES-128-ECB 解密
- 跨平台账户匹配
- 增量水位管理
- 独立调度与告警
- 后续分析层扩展能力

本计划的目标不是一次性完成全部 CRM 链路，而是先以最小闭环方式完成会员链路，再扩展到交易、订单、退单。

## 当前阶段快照

- 已确认真实来源不是 12 张 `fdi_*` JSON 表，而是 `hfsy` 中的 typed tables，第一阶段只围绕 `t_member_info`、`t_member_bind_info`、`t_pin_xid_rel` 开工。
- 已确认当前源库版本为 MySQL `5.7.42`，所有第一阶段 SQL、DDL、增量与幂等实现都必须保持 5.7 兼容。
- 已确认 `t_member_bind_info` 的 `platAccount1`、`bindMobile1`、`birthday1`、`gender1`、`name1` 与 `DecryptionTags` 当前全空，因此不能按“现成明文字段可直接消费”设计；第一阶段应回到“密文字段 + 本地 AES 解密”主路径。
- 已确认 `t_member_info.modified` 与 `t_member_bind_info.modified` 当前全表无空值和异常格式，但因字段类型仍是字符串，后续增量实现仍需保留排序与 lookback 保护。
- 已确认 `t_order_copy` 与 `t_order_copy1` 和 `t_order` 按 `order_item_id` 100% 重叠，当前可排除出正式链路；第二阶段订单链路先只消费 `t_order`。
- 已完成文档证据收口、HFSY 数据字典、快照、交接记录与经验台帐沉淀；当前尚未开始 CRM 实现代码开发。

## 当前推进进度

- 已完成：方案审计、实库探查、连接事实确认、三项关键补证、文档同步、handoff、lesson。
- 当前阶段：实现前最后收口已完成，已经可以直接进入第一阶段代码落地。
- 尚未开始：`.env.example` 扩展、`config.py` 新增 CRM 配置、CRM DWD DDL、`utils/crypto.py`、`utils/account_match.py`、`etl_dwd_member.py`、`run_crm_etl.py`、`test_crm_etl.py`。

## 下一步执行入口

建议下一轮实现严格按以下顺序推进：

1. 扩展 `.env.example` 与 `config.py`，新增 `SHUYUN_ODS_*`、AES key、租户、lookback 和批处理参数。
2. 新增 `SQL/create_dwd_crm_tables.sql`，先只覆盖 `dwd_member`、`etl_watermark`、`etl_run_log`、`etl_fix_log` 等第一阶段必需对象。
3. 新增 `utils/crypto.py`，固定实现 `AES-128-ECB-PKCS5Padding + Base64`，兼容裸 Base64 与可能的包裹格式。
4. 新增 `utils/account_match.py`，封装淘系 `uni_id`、京东 `pin→xid` 与其他平台账号匹配逻辑。
5. 新增 `etl_dwd_member.py`，只实现会员链路，不提前并入订单、交易、退单。
6. 新增 `run_crm_etl.py`，先独立调度，支持 `--conn-test` 与 `--step dwd_member`。
7. 新增 `test_crm_etl.py`，至少覆盖 crypto、account_match、watermark 与 member transform。

## 新对话承接方式

切换到新对话窗口时，建议直接提供本文件，并附一句：

- “以此文件为完整上下文，按‘下一步执行入口’开始实现第一阶段 CRM 会员链路。”

如果只想先做一小步，也可以明确指定：

- “以此文件为上下文，只先实现 Phase 0 的 `.env.example` 与 `config.py` 扩展。”
- “以此文件为上下文，只先创建 `SQL/create_dwd_crm_tables.sql` 与 `utils/crypto.py`。”

## 校正依据

本计划按以下现有代码事实校正：

| 事实 | 当前仓库依据 |
|------|-------------|
| 配置集中在 `config.py`，已统一管理连接、重试、任务展示名 | 来源：[config.py](../../config.py#L9-L104) |
| 仓库根目录已存在 `.env.example`，当前仅覆盖 Oracle/MySQL 连接模板，CRM 阶段应扩展而非新建 | 来源：[.env.example](../../.env.example#L1-L12) |
| 主调度已具备步骤编排、摘要告警、连接测试与重试框架 | 来源：[run_etl.py](../../run_etl.py#L47-L47)；[run_etl.py](../../run_etl.py#L257-L308)；[run_etl.py](../../run_etl.py#L310-L653) |
| 现有 ETL 模式为根目录平铺 `etl_*.py`，未使用 `etl/` 包结构 | 来源：[README.md](../../README.md#L100-L108) |
| ODS 增量已使用独立水位表 `ods_sync_state`，说明 CRM 不宜复用该表 | 来源：[etl_ods_m_retail.py](../../etl_ods_m_retail.py#L20-L74)；[SQL/create_ods_tables.sql](../../SQL/create_ods_tables.sql#L46-L54) |
| 当前自动化测试仅覆盖现有 DIM/DWS/ADS，尚无 CRM 用例 | 来源：[test_etl_automation.py](../../test_etl_automation.py#L1-L340) |
| `docs/misc/数云实施-何方会员订单数据字典表 .xlsx` 展示的真实对象不是 12 张 `fdi_*` JSON 表，而是“会员批量信息表 / 单个会员绑定字段表 / 订单主表 / 订单明细表 / 省市区”五类字典，名称与字段更接近 `t_member_info`、`t_member_bind_info`、`t_trade`、`t_order`、`sys_area` | 来源：`docs/misc/数云实施-何方会员订单数据字典表 .xlsx` |
| 2026-03-20 只读探查 `hfsy` 实库，已确认真实核心表为 `t_member_info`、`t_member_bind_info`、`t_trade`、`t_order`、`t_pin_xid_rel`、`sys_area`，并存在 `t_order_copy`、`t_order_copy1` 两张疑似备份表 | 实表证据：2026-03-20 `SHOW TABLES` |
| 2026-03-20 已落盘 `reports/snapshot_mysql_hfsy_schema.json` 与 `docs/HFSY数据字典.md`，用于后续字段映射、DDL 设计与文档对齐 | 审计产物：`reports/snapshot_mysql_hfsy_schema.json`；`docs/HFSY数据字典.md` |
| 2026-03-20 只读探查 `hfsy` 实库，已确认当前数云落库 MySQL 版本为 `5.7.42`，因此 MySQL 8.0 只能视为建议，不是当前接入硬前提 | 实表证据：2026-03-20 `SELECT VERSION()` |
| 2026-03-20 用户补充当前 `hfsy` 实库连接事实：`8.134.87.152:33066`、数据库名 `hfsy`、接入账号 `shuyun668`；真实密码已提供，但按仓库安全约束仅作为会话事实使用，不写入 git 跟踪文档 | 来源：2026-03-20 用户提供连接信息；[docs/HFSY数据字典.md](../HFSY数据字典.md#L1-L9) |
| 2026-03-20 全表统计显示 `t_member_bind_info` 的 `platAccount1`、`bindMobile1`、`birthday1`、`gender1`、`name1` 与 `DecryptionTags` 当前均为全空，不能再把 `*1` 列当作已可直接消费的现成明文字段 | 审计产物：`reports/hfsy_bind_coverage_by_plat.json`；`reports/hfsy_probe_stage2.json` |
| 2026-03-20 全表统计显示 `t_member_info.modified` 与 `t_member_bind_info.modified` 当前空串数、空值数与非标准时间格式数均为 0 | 审计产物：`reports/hfsy_modified_quality.json` |
| 2026-03-20 全表重叠统计显示 `t_order_copy`、`t_order_copy1` 与 `t_order`、彼此之间均按 `order_item_id` 100% 重叠，当前更接近同域复制子集或备份切片 | 审计产物：`reports/hfsy_order_copy_counts.json`；`reports/hfsy_order_copy_overlap.json`；`reports/hfsy_probe_stage2.json` |
| `t_member_bind_info` 已存在 `platAccount1`、`bindMobile1`、`birthday1`、`gender1`、`name1` 等解密后字段，说明第一阶段实现不能再默认“只能消费密文并本地全量解密” | 实表证据：2026-03-20 `SHOW CREATE TABLE t_member_bind_info` |
| 标准方案显示会员 ODS 表物理主键基于 `plat_account + uni_shop_id`，说明 `account_match_key` 更适合作为派生关联键而非物理主键 | 来源：[业务数据推送数据库标准方案.md](业务数据推送数据库标准方案.md#L17-L24)；[业务数据推送数据库标准方案.md](业务数据推送数据库标准方案.md#L44-L63) |
| 仲裁材料已确认加密算法为 AES-128-ECB + Base64，密钥以 32 位 hex 字符串传入，对应 16 bytes | 来源：[敏感数据加密规则.md](敏感数据加密规则.md#L10-L18)；[敏感数据加密规则.md](敏感数据加密规则.md#L131-L156)；[跟数云方沟通同步的问题.md](跟数云方沟通同步的问题.md#L5-L5) |
| 仲裁材料已确认标准方案口径的增量依据为 `update_time`、推送频率为每小时一次、ODS 建议 MySQL 8.0 及以上，但该结论已被 `hfsy` 实表现状部分覆盖 | 来源：[跟数云方沟通同步的问题.md](跟数云方沟通同步的问题.md#L7-L10) |
| 仲裁材料已确认京东业务表 `plat_account` 为 pinid，关联会员必须先做 pin→xid 映射；淘系统一使用 `uni_id` | 来源：[跟数云方沟通同步的问题.md](跟数云方沟通同步的问题.md#L13-L16) |

## 交叉审计结论

本计划已结合仲裁材料再次校正，新增约束如下：

1. 加密算法固定为 `AES-128-ECB-PKCS5Padding + Base64`，密钥为 32 位 hex 字符串，对应 16 bytes。
2. 包裹格式 `~{cipher}~{version}~` 尚未被数云方明确确认是否覆盖全部字段，因此实现时必须兼容“裸 Base64 密文”和“包裹密文”两种输入。
3. 增量推送依据为 `update_time`，默认同步频率为每小时一次，因此第一阶段联调与验收不按分钟级实时口径设计。
4. 数云标准方案虽然建议 MySQL 8.0 及以上，但当前 `hfsy` 实表运行在 MySQL 5.7.42，且核心表默认字符集为 `utf8`；因此后续接入实现必须以 5.7 兼容为硬前提，不能预设 8.0 专有能力。
5. 京东 `xid` 直接使用原值，不做解密；京东业务表中的 `plat_account` 实际为 pinid，必须先做 pin→xid 映射后才能关联会员。
6. 淘系业务统一使用 `uni_id` 作为跨表匹配键，不直接使用业务表 `plat_account` 对会员表做关联。
7. 仲裁优先级按“`跟数云方沟通同步的问题.md` > `敏感数据加密规则.md` > `业务数据推送数据库标准方案.md`”执行；若标准方案中的字段语义与沟通确认单冲突，以沟通确认单为准。
8. 当前仓库根目录已存在 `.env.example`，CRM 阶段应在原模板上追加变量，而不是再新建一份模板文件。
9. 第 2 轮审计起，证据优先级应切换为“`hfsy` 实表 DDL / 行数 / 样例 > `数云实施-何方会员订单数据字典表 .xlsx` > 三份旧仲裁文档”，旧文档不再作为最高优先级事实源。
10. 当前真实接入对象不是 12 张 `fdi_*` JSON 表，而是 `t_member_info`、`t_member_bind_info`、`t_trade`、`t_order`、`t_pin_xid_rel`、`sys_area` 等 typed tables；因此第一阶段实现必须从“解析 `fdi_response` JSON”切换为“直接消费结构化列”。
11. 当前已确认的源端连接元信息为：MySQL `5.7.42`、`8.134.87.152:33066`、数据库 `hfsy`、接入账号 `shuyun668`；真实密码虽然已由用户提供，但不得落盘到仓库文档、脚本或示例配置。

## 当前已确认源端连接事实

- 数据库类型与版本：MySQL `5.7.42`
- 部署地址：`8.134.87.152:33066`
- 当前数据库名：`hfsy`
- 当前接入账号：`shuyun668`
- 密码处理原则：真实密码已由用户在当前会话提供，但按仓库安全约束不写入任何被 git 跟踪的文档、脚本或配置；后续联调与只读探查均应通过环境变量或用户本地安全介质注入
- 当前仓库状态：代码层尚未正式接入 `hfsy` 变量模板；在进入 CRM 实施前，可先以只读探查脚本或独立终端临时变量方式复用该连接

## 第 1 轮字段级仲裁矩阵

本轮目标不是落地代码，而是把标准方案中的 12 张数云 ODS 表按统一维度拆平，确认哪些规则已经被仲裁材料覆盖，哪些仍然只能标记为“待实表验证”。

审计维度统一为：

- 物理主键
- 增量字段
- 加密字段
- 跨表匹配键
- 京东特殊规则
- 当前审计状态
- 实施优先级

| ODS 表 | 物理主键 | 增量字段 | 加密字段仲裁 | 跨表匹配键仲裁 | 京东特殊规则 | 当前审计状态 | 实施优先级 |
|------|------|------|------|------|------|------|------|
| `fdi_member_info` | `(plat_account, uni_shop_id)` | `update_time` | `name`/`sex`/`birthday`/`bind_mobile`/`plat_account` 按 AES-128-ECB + Base64 处理；`mix_mobile` 保留密文，暂不作为第一阶段必解字段 | 淘系用 `uni_id`；非淘系优先 `plat_account_plain`；京东会员侧先 `pin→xid` 再参与匹配 | 会员表 `plat_account` 对京东仍按 pin 处理，需结合 `fdi_jos_pin_xid` 生成 `account_match_key` | 已仲裁，可进入设计 | P0 |
| `fdi_member_point_his` | `id` | `update_time` | 未见需解密字段 | 当前三份仲裁材料均未给出与会员宽表的稳定关联方案，只能暂保留 `member_id` 原值 | 无新增京东特例 | 部分仲裁，待实表确认是否需要会员映射 | P2 |
| `fdi_member_grade_his` | `id` | `update_time` | 未见需解密字段 | 当前仅确认保留 `member_id` 原值；未形成跨平台统一键方案 | 无新增京东特例 | 部分仲裁，待实表确认是否需要会员映射 | P2 |
| `fdi_trade` | `uni_order_id` | `update_time` | `plat_account` 视为可能加密字段，实现需兼容裸 Base64 与包裹格式 | 淘系用 `uni_id`；京东业务侧 `plat_account` 不可直接当会员键 | 京东 `plat_account=pinid`，关联会员前必须走 `pin→xid` | 已仲裁，可进入设计 | P1 |
| `fdi_order` | `uni_order_item_id` | `update_time` | `plat_account` 在样例中可能为明文 UUID，也可能在其他平台为密文，因此实现需“按内容判断并兼容解密失败回退” | 淘系用 `uni_id`；其他平台按账号键；京东仍不能直接用业务表 `plat_account` | 京东 `plat_account=pinid`，关联会员前必须走 `pin→xid` | 已仲裁，但字段形态存在平台差异 | P1 |
| `fdi_refund` | `uni_refund_id` | `update_time` | 标准样例未出现 `plat_account` 字段，本轮不引入账号解密假设 | 与会员关联应优先走订单侧匹配链路，不直接假设退款表自带稳定会员键 | 京东退款同样继承订单侧 `pin→xid` 约束 | 部分仲裁，待实表确认是否存在账号字段 | P1 |
| `商品类目表` | `(category_id, uni_shop_id, partner)` | `update_time` | 未见需解密字段 | 无会员匹配需求 | 无 | 标准方案 DDL 片段缺少显式表名，文档本身存在缺口；仅确认结构意图 | P2 |
| `fdi_product` | `uni_product_id` | `update_time` | 未见需解密字段 | 无会员匹配需求 | 无 | 已仲裁，可直接按标准表保留 | P2 |
| `fdi_sku` | `(sku_id, plat_code)` | `update_time` | 未见需解密字段 | 无会员匹配需求 | 无 | 已仲裁，可直接按标准表保留 | P2 |
| `fdi_tag` | `(uni_id, shop_id)` | `update_time` | `plat_account` 为 AES 加密字段；标签值本身不加密 | 以 `uni_id` 为主键侧对象键；不建议改造为账号键 | 京东若需标签与会员对齐，仍应以已映射后的会员键消费，不在 ODS 层改造 | 已仲裁，但第一阶段不进入实施 | P2 |
| `fdi_rate` | `(product_id, order_id, uni_shop_id)` | `update_time` | `plat_account` 为可能加密字段；样例中 `uni_id` 允许为空 | 优先走 `uni_order_id` / `order_id` 与订单链路关联，不直接依赖 `uni_id` 非空 | 京东评价若要关联会员，仍应复用订单链路的 `pin→xid` 结果 | 部分仲裁，待实表确认评价侧账号分布 | P2 |
| `fdi_jos_pin_xid` | `(pin, uni_shop_id)` | `update_time` | `pin` 保持解密能力；`xid` 与标准方案冲突时以沟通确认单为准，当前按“原值直用，不解密”实现 | 该表本身就是京东匹配桥表，产物应服务 `member/trade/order/refund` | 半小时内可能延迟，缺失要允许补到后再回刷 | 已仲裁，可进入设计 | P0 |

### 第 1 轮发现清单

#### 需立即固化的结论

1. 12 张表中，真正影响第一阶段会员闭环设计的只有 `fdi_member_info` 与 `fdi_jos_pin_xid`；`fdi_trade`、`fdi_order`、`fdi_refund` 属于第二阶段扩展对象。
2. 三份仲裁材料已经足以把“统一增量字段= `update_time`”“统一推送频率=每小时”“统一 ODS 版本前提=MySQL 8.0+”“京东业务账号=pinid”“淘系匹配键= `uni_id`”固化进实施计划。
3. `fdi_order` 与 `fdi_trade` 的 `plat_account` 可能同时存在“裸明文”和“包裹密文”两类形态，因此代码实现不能把“字段名=plat_account”直接等价成“必须解密成功”。

#### 当前仍不能宣称完全闭环的点

1. `fdi_member_point_his`、`fdi_member_grade_his` 的 `member_id` 如何稳定映射到第一阶段 `dwd_member` 主键，三份仲裁材料都没有给出完整规则。
2. `fdi_refund` 标准样例未透出 `plat_account`，因此退款与会员的直接关联路径目前不能只靠文档证明。
3. `商品类目表` 在标准方案中缺少完整 `CREATE TABLE <table_name>` 头部，说明标准方案文档本身存在排版或抄录缺口，后续必须用实表或建表 SQL 校验。
4. `fdi_jos_pin_xid` 中 `xid` 是否始终是“原值直用”而非部分租户密文，目前只有沟通确认单证据，没有实表证据。

### 第 1 轮结论

本轮完成后，可以把 12 张表分为三类：

- 已可按仲裁文档进入设计：`fdi_member_info`、`fdi_trade`、`fdi_order`、`fdi_product`、`fdi_sku`、`fdi_tag`、`fdi_jos_pin_xid`
- 已知结构但匹配路径仍待实表验证：`fdi_member_point_his`、`fdi_member_grade_his`、`fdi_refund`、`fdi_rate`
- 标准方案文档自身不完整，必须用实表补证：`商品类目表`

因此，第 1 轮结束后，对“仲裁文档之间的字段级一致性”可给出高置信结论，但对“真实落库是否 100% 符合仲裁矩阵”仍不能越过实表验证阶段。

## 第 2 轮实表校正结论

2026-03-20 已拿到两类新证据：

- `docs/misc/数云实施-何方会员订单数据字典表 .xlsx`
- `hfsy` 实库的只读 `SHOW TABLES` / `SHOW CREATE TABLE`

基于这两类证据，可以确认以下事实：

1. 当前真实落库并非标准方案中的 12 张 `fdi_*` 表，而是 8 张 `t_*` / `sys_*` 表，核心业务表为 `t_member_info`、`t_member_bind_info`、`t_trade`、`t_order`、`t_pin_xid_rel`、`sys_area`。
2. `t_member_info` 与 xlsx 中“会员批量信息表”一致；`t_member_bind_info` 与“单个会员绑定字段表”一致；`t_trade` 与“订单主表”一致；`t_order` 与“订单明细表”一致；`sys_area` 与“省市区”一致。
3. `t_member_bind_info` 虽然已预留 `platAccount1`、`bindMobile1`、`birthday1`、`gender1`、`name1` 等解密后字段，但 2026-03-20 全表统计显示这些列当前全部为空，因此第一阶段不能依赖其作为现成明文字段，仍需以原密文字段 + 本地 AES 解密为主路径，并把 `*1` 列保留为未来可能启用的补充来源。
4. `t_pin_xid_rel` 已作为京东 pin→xid 映射桥表存在，因此京东会员匹配不再停留在文档假设层，可以直接按实表设计缓存与补偿逻辑。
5. `t_order_copy`、`t_order_copy1` 当前各自 613925 行，且与 `t_order`、彼此之间都按 `order_item_id` 100% 重叠，更接近同域复制子集或备份切片；在数云方明确业务用途前，正式链路应只消费 `t_order`。
6. 本轮已生成 `hfsy` 结构快照与独立数据字典，后续字段映射、代码实现与文档同步都应直接引用这些审计产物，而不是重复手抄字段定义。

据此，第 2 轮后的第一阶段实现范围应重定向为：

- `hfsy.t_member_info`
- `hfsy.t_member_bind_info`
- `hfsy.t_pin_xid_rel`

`t_trade`、`t_order` 作为第二阶段订单链路；`sys_area` 作为地区码辅助维表；不再以 `fdi_member_info` / `fdi_jos_pin_xid` 作为当前唯一设计起点。

## 当前审计发现清单

以下发现基于当前已落盘证据整理，用于指导下一步实现优先级与风险控制。

### High

1. 当前真实来源已确认是 `hfsy` 的 typed tables，而不是标准方案中的 12 张 `fdi_*` JSON 表；若继续按 `fdi_response` 解析链路设计，第一阶段实现会直接偏离真实源结构。
2. 当前源库版本是 MySQL 5.7.42，因此第一阶段所有 SQL、索引与增量实现必须保持 5.7 兼容，不能引入 8.0 专有语法或能力假设。
3. 第一阶段可直接闭环的最小输入范围已收敛为 `t_member_info`、`t_member_bind_info`、`t_pin_xid_rel`；`t_trade` 与 `t_order` 应延后到第二阶段，不宜在会员链路未稳定前一起推进。
4. `t_order_copy` 与 `t_order_copy1` 当前各自 613925 行，且与 `t_order`、彼此之间按 `order_item_id` 都是 100% 重叠；现阶段已可将其视为复制子集或备份切片，并明确排除出正式消费范围。
5. `t_member_bind_info` 的 `*1` 列与 `DecryptionTags` 在当前实库中全空，说明“直接消费现成明文字段”的前提并不成立；第一阶段会员链路必须回到“密文字段 + 本地解密”主路径。

### Medium

1. `t_member_info.modified` 与 `t_member_bind_info.modified` 虽然当前全表统计的空串数、空值数和非标准格式数均为 0，但字段类型仍是字符串时间列；后续增量逻辑仍需显式处理排序语义与 lookback 回看窗口。
2. `DecryptionTags` 在当前实库中也是全空，因此既不能作为“已解密”标记，也不能作为字段启用开关；是否启用解密逻辑只能基于实际值覆盖率和联调结果判断。
3. 京东 `pin→xid` 实表桥接已确认存在，但仍需在实现时保留缺失补偿与回刷逻辑，不能把桥表完整率视为天然 100%。

### Low

1. `reports/snapshot_mysql_hfsy_schema.json` 与 `docs/HFSY数据字典.md` 已成为当前最重要的源侧审计产物，后续字段映射、DDL 设计和文档同步应统一引用这两份产物。
2. 旧仲裁材料仍有参考价值，但当前角色已从“主事实源”降为“补充语义和协议说明”，不应再覆盖实表证据。
3. 当前文档链路已经把 HFSY 数据字典纳入同步清单，后续若新增 CRM DWD 表或字段映射，需要连同该文档一并维护。

### 待补证项

1. 数云侧是否计划在后续链路中真正回填 `t_member_bind_info` 的 `*1` 明文字段，还是这些列长期仅作为预留结构存在
2. `t_order_copy`、`t_order_copy1` 的正式命名语义与保留策略仍建议由数云方补一个口头或书面确认，但当前技术结论已足够支撑“先排除出正式链路”

## 校正结论

### 1. 目录结构按当前仓库执行

不引入 `etl/` 子包重构。

- 通用工具模块新增到 `utils/`
- ETL 主脚本继续放仓库根目录
- 调度入口继续采用独立脚本方式

这样可以与现有根目录平铺模式保持一致，避免为了 CRM 需求先做仓库结构重构。

### 2. dwd_member 主键改为稳定原值键

`dwd_member` 不采用 `(tenant, platform, account_match_key)` 作为主键。

校正后的主键建议为：

- `(tenant, plat_account_raw, uni_shop_id)`

原因：

- `account_match_key` 受解密结果、京东 xid 映射结果影响，存在口径波动风险
- `plat_account_raw` 来自 ODS 原值，更适合作为稳定幂等键
- `account_match_key` 应保留为跨表关联键和索引列，而非主键列

### 3. account_match_key 只作为关联键

统一规则如下：

- 淘系：`uni_id`
- 京东：`xid`
- 其他平台：优先 `plat_account_plain`，失败时回退 `plat_account_raw`

该字段用于会员与交易、订单、退单的横向匹配，不承担主键职责。

### 4. CRM 使用独立水位表

新增 `etl_watermark`，不复用 `ods_sync_state`。

原因：

- `ods_sync_state` 已服务现有 Oracle ODS 增量逻辑
- CRM 来源库、更新字段、断点语义均不同
- 复用会增加排障与运维混淆成本

### 5. CRM 链路先独立调度，不并入主 run_etl

第一阶段新增 `run_crm_etl.py`，先独立运行验证稳定性。

当前 [run_etl.py](../../run_etl.py#L47-L47) 的步骤顺序仍是现有 8 个任务，且连接测试只覆盖 Oracle 与当前 MySQL 数仓，不覆盖 `shuyun_ods`。因此 CRM 不应直接并入主调度。

### 6. 实施范围按“最小闭环”推进

第一阶段只要求跑通：

- 配置接入
- DWD 建表
- 加解密工具
- 账户匹配工具
- `etl_dwd_member.py`
- `run_crm_etl.py`
- `test_crm_etl.py`

`dwd_trade`、`dwd_order`、`dwd_refund` 在会员链路稳定后再扩展。

## 校正后的实施阶段

### Phase 0：配置与依赖准备

#### 0.1 修改 `config.py`

在现有配置结构后新增：

- `SHUYUN_ODS_CONFIG`
- `SHUYUN_ODS_CONN_STR`
- `SHUYUN_AES_KEY`
- `SHUYUN_TENANT`
- `SHUYUN_LOOKBACK_MINUTES`
- `PIN_XID_BATCH_SIZE`
- `UPSERT_BATCH_SIZE`

并在 `TASK_DISPLAY_NAME` 中追加：

- `dwd_member`
- `dwd_trade`
- `dwd_order`
- `dwd_refund`

来源：[config.py](../../config.py#L29-L42)；[config.py](../../config.py#L79-L89)

说明：`AES-128-ECB-PKCS5Padding + Base64` 已由仲裁材料固定，不建议再暴露 `SHUYUN_AES_KEY_ENCODING`、`SHUYUN_AES_MODE` 这类运行时开关，避免实现偏离协议。

#### 0.2 扩展现有 `.env.example`

当前仓库根目录已存在 `.env.example`，但仅覆盖 Oracle/MySQL 连接模板。本阶段应在原文件上补充 CRM 变量，而不是新建第二份模板文件。

至少包含：

- `SHUYUN_ODS_HOST`
- `SHUYUN_ODS_PORT`
- `SHUYUN_ODS_USER`
- `SHUYUN_ODS_PASSWORD`
- `SHUYUN_ODS_DB`
- `SHUYUN_AES_KEY`
- `SHUYUN_TENANT`
- `SHUYUN_LOOKBACK_MINUTES`
- `PIN_XID_BATCH_SIZE`
- `UPSERT_BATCH_SIZE`

如需固化运维参数，可额外预留：

- `CRM_ETL_SCHEDULE_HOURS`，默认 `1`

#### 0.3 增加依赖声明

新增依赖：

```bash
pip install pycryptodome
```

由于当前仓库没有统一的 `requirements.txt`，本阶段不强制新增依赖文件；至少要在 `README.md` 与 `docs/RUNBOOK.md` 中记录该依赖。

#### 0.4 环境前置条件

- 数云真实落库当前为 MySQL 5.7.42，第一阶段所有 SQL 与工具实现必须兼容 5.7
- 源表默认字符集当前为 `utf8`，不能预设 `utf8mb4_bin` 为实表事实
- 第一阶段按“每小时一次”推送频率规划联调与验收窗口

### Phase 1：DDL 建模

#### 1.1 新增 `SQL/create_shuyun_ods_tables.sql`

用途：数云 ODS 结构留档与交付前对照。

说明：

- 该文件仅作结构参考
- ODS 实际建表由数云推送端完成
- 排序规则保持 `utf8mb4_bin`

#### 1.2 新增 `SQL/create_dwd_crm_tables.sql`

第一版新增以下表：

| 表名 | 用途 | 主键策略 |
|------|------|----------|
| `dwd_member` | 会员宽表 | `(tenant, plat_account_raw, uni_shop_id)` |
| `dwd_trade` | 交易主表 | `(tenant, trade_no)` |
| `dwd_order` | 订单明细 | `(tenant, order_no)` |
| `dwd_refund` | 退单明细 | `(tenant, refund_no)` |
| `etl_watermark` | CRM 增量水位 | `(tenant, source_table)` |
| `etl_run_log` | CRM 运行日志 | 自增 ID |
| `etl_fix_log` | 数据修复记录 | 自增 ID |

其中 `dwd_member` 需额外保留：

- `plat_account_raw`
- `plat_account_plain`
- `account_match_key`
- `jd_xid_missing`
- `jd_xid_decrypt_fail`

### Phase 2：通用工具模块

新增 `utils/` 目录，而不是采用 R10 中的 `etl/` 子包设计。

#### 2.1 `utils/__init__.py`

空文件，用于声明包。

#### 2.2 `utils/crypto.py`

提供：

- `ShuyunCrypto`
- `get_crypto()`

要求：

- 初始化时校验密钥是否存在、是否为合法 16 字节 key
- 算法固定为 `AES-128-ECB-PKCS5Padding + Base64`
- 解密函数需兼容“裸 Base64 密文”和可能存在的 `~{cipher}~{version}~` 包裹格式
- 包裹格式解析失败时记录 warning，并回退尝试按裸 Base64 解密
- 解密失败记录 warning，不直接中断整个批次
- 支持测试用 `encrypt()`
- `xid` 是否解密与标准方案存在冲突时，以沟通确认单为准；当前按“`xid` 直接使用原值”实现，不对 `xid` 再做 AES 解密

#### 2.3 `utils/account_match.py`

提供：

- `build_account_match_key()`
- `JosPinXidCache`

要求：

- 淘系直接使用 `uni_id`
- 京东统一使用 `xid`
- 京东业务表 `plat_account` 视为 pinid，必须经 `fdi_jos_pin_xid` 做 pin→xid 映射后才能参与会员关联
- `xid` 直接使用原值，不做解密
- 其他平台优先明文账号，失败时回退原值
- `fdi_jos_pin_xid` 查询与缓存逻辑封装在该模块内

### Phase 3：第一阶段 ETL 最小闭环

#### 3.1 优先实现 `etl_dwd_member.py`

结构对齐现有 ETL 风格，保持根目录平铺：

- `_get_watermark()`
- `_update_watermark()`
- `extract()`
- `transform()`
- `load()`
- `run()`

参考实现风格：

- [etl_ods_m_retail.py](../../etl_ods_m_retail.py#L20-L242)
- [etl_dws_sales.py](../../etl_dws_sales.py#L18-L211)

#### 3.2 `extract()` 规则

- 来源表：`hfsy.t_member_info` 与 `hfsy.t_member_bind_info`
- 增量字段：当前按两张实表的 `modified` 字段分别管理水位，不再预设统一 `update_time`
- 读取条件：`modified > watermark - lookback`
- 空跑模式只检查连接与 SQL 可执行性，不落表

#### 3.3 `transform()` 规则

- 直接消费结构化列，不再把第一阶段默认建立在 `fdi_response` JSON 解析之上
- 先关联 `t_member_info` 与 `t_member_bind_info`
- 优先使用 `platAccount1`、`bindMobile1`、`birthday1`、`gender1`、`name1` 等解密后字段
- 若 `*1` 字段为空，再回退到原密文字段并按 AES 规则尝试本地解密
- 产出 `plat_account_raw`、`plat_account_plain`
- 计算 `account_match_key`
- 京东记录从 `t_pin_xid_rel` 做 pin→xid 映射，再计算 `account_match_key`
- 京东记录标记 `jd_xid_missing`
- 解密失败标记 `jd_xid_decrypt_fail`

#### 3.4 `load()` 规则

- 采用 `INSERT ... ON DUPLICATE KEY UPDATE`
- 以 `(tenant, plat_account_raw, uni_shop_id)` 保证幂等写入
- 成功后更新 `etl_watermark`
- 失败时不推进水位

### Phase 4：独立调度入口

#### 4.1 新增 `run_crm_etl.py`

本阶段不修改 [run_etl.py](../../run_etl.py#L310-L653)，先新增独立调度入口。

第一版支持：

- `--conn-test`
- `--step dwd_member`
- 全链路执行
- 重试与摘要告警

#### 4.2 `--conn-test` 校验范围

不同于现有主调度，CRM 空跑至少要校验：

- `hefang_dw` MySQL 可连接
- `hfsy` MySQL 可连接
- AES key 配置合法

现有主调度连接测试仅覆盖 Oracle 与当前 MySQL，来源：[run_etl.py](../../run_etl.py#L257-L308)

#### 4.3 调度频率建议

- 第一阶段联调按“每小时一次”推送频率校验
- 不将 CRM 设计为分钟级实时任务
- 稳定后再决定是否纳入统一调度体系

### Phase 5：测试补齐

#### 5.1 新增 `test_crm_etl.py`

当前 [test_etl_automation.py](../../test_etl_automation.py#L1-L340) 未覆盖任何 CRM 逻辑，因此需要新增专门测试文件。

第一版至少覆盖：

- `crypto` 加解密 round-trip
- `account_match_key` 平台匹配规则
- `JosPinXidCache` 命中与缺失场景
- `etl_dwd_member.transform()` 输出字段完整性
- `etl_watermark` 推进逻辑

#### 5.2 第一阶段验收命令

```bash
python run_crm_etl.py --conn-test
python run_crm_etl.py --step dwd_member
python run_crm_etl.py
```

### Phase 6：会员链路稳定后再扩展

以下内容在第一阶段完成后推进：

- `etl_dwd_trade.py`
- `etl_dwd_order.py`
- `etl_dwd_refund.py`

扩展到交易、订单、退单时，必须继承以下口径：

- 京东业务表 `plat_account` 一律视为 pinid
- 所有京东业务表关联会员前必须执行 pin→xid 映射
- 淘系业务统一使用 `uni_id` 作为跨表匹配键
- 未确认包裹格式的加密字段一律走兼容解密逻辑

以下内容暂标记为“未实现”，不纳入当前交付范围：

- `etl_ads_member.py`
- CRM 并入主 `run_etl.py`
- Tableau 会员看板
- ERP 与 CRM 的深入交叉验证报表

## 校正后的实际执行顺序

```text
Step 1: 修改 config.py
Step 2: 扩展 .env.example
Step 3: 新增 SQL/create_dwd_crm_tables.sql
Step 4: 新增 utils/crypto.py
Step 5: 新增 utils/account_match.py
Step 6: 新增 etl_dwd_member.py
Step 7: 新增 run_crm_etl.py
Step 8: 新增 test_crm_etl.py
Step 9: 同步文档
Step 10: 写入 AGENT_HANDOFF.md
Step 11: 再扩 dwd_trade / dwd_order / dwd_refund
```

## 文档同步范围

实施代码后，同步更新以下文件：

| 文件 | 同步内容 |
|------|----------|
| `docs/MYSQL数据字典.md` | 新增 CRM DWD 表与管控表结构 |
| `docs/HFSY数据字典.md` | 新增数云源侧 `hfsy` 实表与字段字典 |
| `docs/数据结构与映射手册.md` | 新增数云 ODS 到 DWD 字段映射 |
| `docs/DATA_CONTRACTS.md` | 新增 CRM 契约与水位说明 |
| `docs/ARCHITECTURE.md` | 新增 CRM 数据流与独立调度入口 |
| `docs/ETL业务逻辑说明.md` | 新增 CRM ETL 逻辑 |
| `docs/RUNBOOK.md` | 新增依赖安装、空跑与排障说明 |
| `README.md` | 新增 CRM 入口与运行命令 |
| `CHANGELOG.md` | 新增版本记录 |
| `docs/AGENT_HANDOFF.md` | 记录本轮变更交接 |

## 数据验证 SQL

```sql
-- 水位推进检查
SELECT *
FROM etl_watermark
ORDER BY updated_at DESC;

-- 会员行数对照
SELECT COUNT(*) FROM hfsy.t_member_info;
SELECT COUNT(*) FROM hfsy.t_member_bind_info;
SELECT COUNT(*) FROM hefang_dw.dwd_member;

-- 解密质量
SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN name_plain IS NULL THEN 1 ELSE 0 END) AS name_null,
  SUM(CASE WHEN mobile_plain IS NULL THEN 1 ELSE 0 END) AS mobile_null
FROM dwd_member;

-- 匹配键覆盖率
SELECT
  platform,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN account_match_key IS NOT NULL AND account_match_key <> '' THEN 1 ELSE 0 END) AS matched_rows
FROM dwd_member
GROUP BY platform;

-- 主键稳定性检查
SELECT tenant, plat_account_raw, uni_shop_id, COUNT(*) AS row_cnt
FROM dwd_member
GROUP BY tenant, plat_account_raw, uni_shop_id
HAVING COUNT(*) > 1;
```

## 风险与前置条件

### 必须先确认

- AES key 是否真实为 16 字节可用 key
- 包裹格式 `~{cipher}~{version}~` 是否仍适用于当前 `t_member_bind_info` / `t_trade` 中的加密字段，以及第一阶段应否完全按“本地解密”作为默认主路径
- 当前 `hfsy` 是否还会继续补充退款、标签、评价、商品、SKU 等实表，还是第一阶段只维护会员与订单主链路

### 第一阶段主要风险

- 京东 xid 映射缺失导致匹配键覆盖率下降
- `modified` 作为字符串时间列，仍可能导致水位排序与 lookback 处理复杂化
- `*1` 明文字段当前全空，若实现时仍按“优先使用 *1 列”设计，会直接造成会员链路字段缺失
- 实表结构与旧标准方案不一致，继续按 `fdi_*` 假设开发会直接走偏

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v2.8 | 2026-03-20 | 重命名为跨对话上下文入口文档，并新增当前阶段快照、推进进度与下一步执行入口 |
| v2.7 | 2026-03-20 | 纳入 *1 列全空、DecryptionTags 全空、modified 全量合规与 copy 表 100% 重叠的补证结果 |
| v2.6 | 2026-03-20 | 补充 hfsy 实库连接事实，明确 host/port/db/user 与真实密码不落盘的处理边界 |
| v2.5 | 2026-03-20 | 新增当前审计发现清单，按 High/Medium/Low 和待补证项整理实现边界与风险 |
| v2.4 | 2026-03-20 | 纳入数云 xlsx 与 hfsy 实表证据，确认真实源表为 `t_*` 结构、MySQL 5.7.42 与现成解密列，实施计划切换到第 2 轮实表校正 |
| v2.3 | 2026-03-19 | 完成第 1 轮 12 表字段级仲裁矩阵，明确各表主键、增量、加密、匹配键、京东特例与待实表验证项 |
| v2.2 | 2026-03-19 | 再次审计后修正证据链，明确 `.env.example` 为扩展而非新增，并增加仲裁优先级与固定加密协议约束 |
| v2.1 | 2026-03-19 | 吸收仲裁材料结论，补充加密兼容策略、每小时同步口径、MySQL 8.0 前提与京东业务表 pin→xid 约束 |
| v2.0 | 2026-03-19 | 按当前代码库校正目录结构、主键策略、水位方案、测试范围与调度边界 |
