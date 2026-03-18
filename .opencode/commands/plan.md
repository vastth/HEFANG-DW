---
description: 为 HEFANG-DW 的复杂任务生成最小可执行方案
agent: planner
subtask: true
---

# /plan

针对以下任务先输出实施方案，不直接修改文件：

`$ARGUMENTS`

要求：

1. 先识别影响范围：脚本、SQL、文档、配置、运行命令
2. 明确哪些属于高风险变更：`etl_*.py`、`SQL/*.sql`、`run_etl.py`、`run_ods.py`、`scheduled_etl.py`、`config.py`
3. 优先最小变更，不主动扩大重构范围
4. 若涉及业务口径变化，必须标出需人工确认
5. 输出分阶段计划：准备、实施、验证、交接

输出应包含：

- 目标
- 受影响文件
- 实施步骤
- 验证方式
- 风险与待确认项
