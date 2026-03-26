---
name: data-query
description: 将“帮我查 XXX 数据”路由为结构查询、固定对账、自由查数三类，优先使用只读 MCP，失败时回退到 Python 查询工具。
argument-hint: "[查询需求，如 最近7天销售排行 | 留空=先澄清需求]"
---

## /data-query — 数据查询与对账工作流

### 目标分类

当用户说“帮我查 XXX 数据”时，先判断属于哪一类：
- 结构查询：字段、索引、表结构、表是否存在、字段类型是否一致
- 固定对账：ODS 增量对账、零售明细质量校验、固定口径核对
- 自由查数：临时统计、样本抽查、导出明细、最近 N 天汇总

### 优先规则

1. 结构字段 / 索引 / 表结构问题
   - 优先委托 `db-inspector`
   - 若 MCP 不可用，先检查工作区 `.vscode/mcp.json` 与用户级 `mcp.json`；仓库根 `.mcp.json` 仅作兼容/本地参考，必要时改用结构快照脚本只读查看

2. ODS 固定对账
   - `tools/check_ods_incremental.py`
   - `tools/check_ods_retailitem_quality.py`
   - 这类请求优先走现成脚本，不重复拼装 SQL

3. 自由查数
   - MCP 可用时：优先用 MySQL / Oracle MCP 执行只读 SQL
   - MCP 不可用时：回退到 `tools/query_data.py`
   - 若是 ADS 固定导出，可直接使用 `tools/export_ads.py`

### 只读边界

- 仅允许 `SELECT` 或 `WITH` 查询
- 禁止写操作、DDL、存储过程调用、多语句执行
- 大结果集优先导出为 `csv` / `excel` / `json`，避免直接在终端展开
- 结构快照脚本 `tools/snapshot_mysql_hefangdw_schema.py` 与 `tools/snapshot_oracle_bosnds3_schema.py` 只看结构，不看数据值

### 推荐流程

1. 先判断是结构查询、固定对账，还是自由查数
2. 若 MCP 可用，先走 MCP，只做只读查询
3. 若 MCP 不可用，使用 `tools/query_data.py`
4. 若结果过大，改用 `--output csv|excel|json --output-path ...`
5. 若需求稳定复用，沉淀为新的模板查询

### 常用命令

```bash
# 查看可用模板
python tools/query_data.py --list-templates

# MySQL：最近 7 天销售排行
python tools/query_data.py --template mysql_sales_rank_7d

# Oracle：最近 7 天零售单据统计
python tools/query_data.py --source oracle --template oracle_retail_docs_7d

# 自由查数并导出
python tools/query_data.py --sql "SELECT * FROM ads_inventory_health WHERE snapshot_date = :dt" --param dt=20260318 --output csv
```