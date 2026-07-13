---
name: "Docs Rules"
description: "Use when editing Markdown documentation or README files. Covers evidence citations, version records, doc-sync boundaries, context compression, and business-rule confirmation."
applyTo: "docs/**/*.md, README.md, AGENTS.md"
---

# 文档专用规则

- 文档更新默认使用简体中文；代码、路径、表名、字段名、SQL 关键字保持原文规范。
- 写入事实前必须确认来源；涉及代码、SQL、脚本或数据库事实时，优先引用真实文件、行号、脚本输出、快照或查询结果。
- 不把规划、草案、候选表、未落库对象写成已实现；规划项必须显式标注“未实现”“候选”或“待确认”。
- 不单方面修改业务口径。SABC 阈值、库存状态、销售公式、过滤常量、业务枚举等只能在用户确认后同步。
- 大型治理文档采用定向更新：先读取当前状态、目录、命中片段或版本记录，不默认整篇读取历史归档。
- `docs/AGENT_LESSONS.md` 优先通过 `docs/AGENT_LESSONS_INDEX.md`、关键词检索或脚本命中后再读取具体条目，避免把完整经验台账放入常规上下文。
- 文档底部保留“版本记录”表；若文档已有版本记录，本轮新增一条，避免重写历史无关内容。
- 修改 ETL、SQL、表结构、调度、指标口径对应文档后，应运行或说明未运行 `python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。
- 完成一组有意义的文档变更后，检查是否需要写入 `docs/AGENT_HANDOFF.md`；形成可复用经验时写入 `docs/AGENT_LESSONS.md`。
