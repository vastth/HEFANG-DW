---
name: schema-snap
description: 执行 MySQL 和 Oracle 数据库结构快照，更新 reports/ 目录下的 JSON 快照文件，并对比 docs/MYSQL数据字典.md 检测字段漂移。
---

## /schema-snap — 数据库结构快照

### 执行步骤

**第一步：MySQL 结构快照**
```bash
python tools/snapshot_mysql_hefangdw_schema.py
```
输出：`reports/snapshot_mysql_hefangdw_schema.json`

**第二步：Oracle 结构快照**
```bash
python tools/snapshot_oracle_bosnds3_schema.py
```
输出：`reports/snapshot_oracle_bosnds3_schema.json`

**第三步：检测字段漂移**

若已安装 MySQL MCP，委托 `db-inspector` 子代理对比快照 vs 文档。

若未安装 MCP，读取新生成的 `reports/snapshot_mysql_hefangdw_schema.json` 与 `docs/MYSQL数据字典.md`，手动对比差异。

**第四步：输出漂移报告**

```
## 结构快照报告 — [日期时间]

### MySQL 快照
- 快照文件：reports/snapshot_mysql_hefangdw_schema.json
- 覆盖表数：N 张
- 字段漂移：
  - 新增字段（实际有、文档无）：[列出]
  - 删除字段（文档有、实际无）：[列出]
  - 类型变更：[列出]

### Oracle 快照
- 快照文件：reports/snapshot_oracle_bosnds3_schema.json
- 覆盖表数：N 张

### 建议
```

**第五步：若发现漂移**

询问用户是否更新 `docs/MYSQL数据字典.md`，若确认则委托 `doc-syncer` 子代理执行。

### 注意
- 快照工具需要真实数据库连接（MYSQL_* 和 ORACLE_* 环境变量必须已设置）
- `reports/` 目录已在 `.gitignore` 中，快照结果不会提交到版本库
- 建议每周至少执行一次，避免文档漂移积累
