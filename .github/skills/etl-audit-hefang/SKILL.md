---
name: etl-audit-hefang
description: "Use when auditing ETL modules, ETL scheduling scripts, or ETL automation tests. Checks field lineage, business-rule boundaries, incremental logic, idempotency, evidence gaps, and documentation sync risk, and returns prioritized findings."
argument-hint: "[模块名、文件名或范围，例如：dws_sales | ods | 全部 ETL]"
---

# etl-audit-hefang

## 作用

对 ETL 相关实现做只读审计，输出按优先级分层的发现清单，帮助在修改前、上线前或回归时发现字段映射、增量逻辑、幂等性和文档同步风险。

## 何时使用

- 用户要求“审计 ETL”“检查字段映射”“核对增量逻辑”
- 计划修改 `etl_*.py`、`run_etl.py`、`run_ods.py`、`scheduled_etl.py` 前
- 怀疑数据漏数、重复、口径漂移或调度链路异常时

## 输入

- 模块名、文件名或范围
- 用户关注重点，例如“水位字段”“幂等性”“是否漏同步文档”

## 执行步骤

1. 确定审计范围，并映射到真实文件。
2. 读取对应 ETL 文件、调度入口、相关 SQL 和核心文档。
3. 逐项检查字段来源、目标表、增量逻辑、幂等性、业务口径边界和证据链。
4. 判断是否存在文档同步风险与最小验证缺口。
5. 输出分级发现，并给出建议后续动作。

## 核心检查项

- 字段映射是否与现有字典和建表 SQL 一致
- 目标表主键、唯一键、去重逻辑是否清晰
- 增量水位、时间字段、回填和重跑行为是否合理
- 幂等性是否足够，是否会引入重复写入或漏写
- 业务口径是否被越权修改
- 文档是否应同步更新
- 当前结论是否有证据缺口

## 输出格式

优先使用以下结构：

1. 需立即处理（CRITICAL）
2. 需计划处理（WARNING）
3. 建议优化（INFO）
4. 审计覆盖范围
5. 建议后续动作

每条发现尽量包含：文件、问题、影响、建议。

## 本仓库专用约束

- 这是只读审计 skill，不直接改代码。
- 如遇业务口径争议，只标记待确认，不私自裁定。
- 如用户未提供证据，不把 CRM 实表或外部链路当作已证事实。
- 若审计对象命中 ETL 文件，应同时受 `.github/instructions/python-etl.instructions.md` 约束。

## 推荐后续动作

- 若存在 CRITICAL：优先修复，再做最小验证。
- 若存在 WARNING：记录到待办或下一轮处理。
- 若全部通过：再决定是否进入实现、文档同步或交接。
