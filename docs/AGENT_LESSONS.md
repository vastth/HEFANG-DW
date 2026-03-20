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

### [2026-03-20 09:50] · task · field-mapping

**触发场景**：收到数云 xlsx 与 hfsy 实库连接信息后执行第2轮实表校正

**错误假设**：把标准方案中的 12 张 fdi_* JSON 表和 MySQL 8.0+ 建议，当成当前真实落库结构与硬前提

**修正结论**：当前真实来源库为 hfsy，版本为 MySQL 5.7.42，核心表为 t_member_info、t_member_bind_info、t_trade、t_order、t_pin_xid_rel、sys_area；第一阶段实现应优先消费 t_member_bind_info 中现成的 *1 解密列，仅在缺失时回退本地 AES 解密。

**证据**：
- docs/misc/数云CRM数据接入实施计划.md#L24
- docs/misc/数云CRM数据接入实施计划.md#L31
- docs/misc/数云CRM数据接入实施计划.md#L42

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
- docs/misc/业务数据推送数据库标准方案.md#L17
- docs/misc/业务数据推送数据库标准方案.md#L77
- docs/misc/业务数据推送数据库标准方案.md#L129
- docs/misc/业务数据推送数据库标准方案.md#L468
- docs/misc/跟数云方沟通同步的问题.md#L11
- docs/misc/数云CRM数据接入实施计划.md#L47

**预防动作**：后续做外部系统接入审计时，先按“可直接设计 / 待实表验证 / 文档本身缺口”三类拆表，不要把所有文档对象都当成同样成熟。

---

### [2026-03-19 18:10] · task · doc-sync

**触发场景**：再次审计数云CRM实施计划

**错误假设**：把不存在于工作区的 R10 文档当作证据来源，并把固定加密协议做成可配置项；同时误判 `.env.example` 缺失。

**修正结论**：工作区审计只能引用仓库内真实存在的证据文件；当前仓库已存在 `.env.example`，CRM 仅需扩展；`AES-128-ECB-PKCS5Padding + Base64` 是固定协议，不应再暴露 `SHUYUN_AES_MODE`、`SHUYUN_AES_KEY_ENCODING` 之类运行时开关。若数云标准方案与沟通确认单冲突，以沟通确认单为最终仲裁。

**证据**：
- .env.example#L1
- docs/misc/跟数云方沟通同步的问题.md#L5
- docs/misc/业务数据推送数据库标准方案.md#L17
- docs/misc/数云CRM数据接入实施计划.md#L21

**预防动作**：后续审计外部接入方案时，先核对“证据文件是否真实存在于工作区”，再区分“固定协议”与“可配置参数”，避免把协议常量设计成环境变量。

---

### [2026-03-19 17:31] · task · business-rule

**触发场景**：数云CRM实施计划交叉审计

**错误假设**：默认认为京东业务表plat_account可直接用于会员关联，且加密输入格式只有一种

**修正结论**：京东业务表plat_account实际为pinid，关联会员前必须先做pin→xid映射；加解密实现必须兼容裸Base64密文和可能存在的~{cipher}~{version}~包裹格式；数云默认按update_time每小时同步一次，ODS建议MySQL 8.0+。

**证据**：
- docs/misc/跟数云方沟通同步的问题.md#L7
- docs/misc/跟数云方沟通同步的问题.md#L10
- docs/misc/跟数云方沟通同步的问题.md#L15
- docs/misc/敏感数据加密规则.md#L10
- docs/misc/敏感数据加密规则.md#L18

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
| v1.2 | 2026-03-19 | 补充数云CRM 12 表字段级仲裁经验，明确哪些表已可设计、哪些仍需实表验证 |
| v1.1 | 2026-03-19 | 补充数云CRM实施计划再审计经验，明确证据优先级与固定协议不应配置化 |
| v1.0 | 2026-03-18 | 新增 Agent 经验台帐与首条字段映射经验 |