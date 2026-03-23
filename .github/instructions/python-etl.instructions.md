---
name: "Python ETL Rules"
description: "Use when editing ETL Python files, ETL scheduling scripts, or ETL automation tests. Covers lineage checks, incremental logic, idempotency, doc sync, and minimum verification."
applyTo: "{etl_*.py,run_etl.py,run_ods.py,scheduled_etl.py,test_etl_automation.py}"
---

# Python ETL 专用规则

- 修改 ETL 代码前，先确认真实的源表、目标表、相关建表 SQL、字段字典和业务说明，优先核对 `SQL/*.sql`、`docs/MYSQL数据字典.md`、`docs/数据结构与映射手册.md`、`docs/ETL业务逻辑说明.md`。
- 将增量逻辑视为高风险改动。显式检查水位字段、时间字段、全量/增量切换、重跑/回填行为、去重策略和幂等性，不要只改局部 SQL 或过滤条件。
- 区分“代码事实”和“业务口径”。如果阈值、状态映射、销售公式或过滤常量没有在现有代码与文档中被证实，先标记待确认，再向用户确认。
- 不要默认本地 MySQL 已存在 `shuyun_ods`、`fdi_*` 或其他 CRM 落库对象。只有在用户提供查询结果、快照、截图或实表证据时，才能把这些对象当作事实。
- 修改 ETL、调度或 ETL 自动化测试时，显式评估文档影响和最小验证动作。说明应同步哪些文档，并运行当前任务最相关的最小验证链路。
- 优先修根因，不做掩盖血缘关系、增量语义或数据风险的表面重构。
- 命令示例优先使用 Windows 环境下可直接执行的 `python` 与 `pwsh`。
