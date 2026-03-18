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
| v1.0 | 2026-03-18 | 新增 Agent 经验台帐与首条字段映射经验 |