---
description: 审计 HEFANG-DW 的 ETL、SQL 依赖、字段映射与增量逻辑
agent: etl-reviewer
subtask: true
---

# /etl-audit

审计范围：`$ARGUMENTS`

执行要求：

1. 优先定位对应 `etl_*.py`、`run_*.py`、`SQL/*.sql`、`docs/DATA_CONTRACTS.md`、`docs/ARCHITECTURE.md`
2. 检查字段映射、来源表、目标表、增量条件、幂等性、重试/异常处理
3. 判断文档描述是否与代码一致
4. 不修改代码，输出发现清单

输出格式：

- 审计范围
- CRITICAL：会导致错误结果、重复写入、漏数、核心文档错误的项
- WARNING：实现或文档存在漂移、风险较高但未必立即出错的项
- INFO：可优化项
- 建议下一步：是否需要 `/doc-sync`、`/quality-check` 或人工确认
