---
name: planning-hefang
description: "Use when the user asks to plan, scope, or discuss an ETL, audit, doc-sync, backfill, CRM, or data-workflow task before implementation. Clarifies goals, evidence gaps, risks, and a concrete step-by-step plan for this repository."
argument-hint: "[任务目标或模块范围，例如：审计 dws_sales 增量逻辑]"
---

# planning-hefang

## 作用

在真正修改代码、文档或配置之前，先把任务目标、范围、证据、风险和执行步骤拆清楚，避免直接进入实现导致返工。

## 何时使用

- 用户明确说“先规划”“先讨论”“先别动手”
- 任务涉及多个模块、多个文档或外部证据链
- 任务包含 ETL、补数、对账、CRM、结构审计、文档同步等高上下文场景
- 当前证据不足，不能直接进入实现

## 输入

- 用户目标或问题
- 模块名、文件名或范围
- 是否允许立即修改代码
- 是否已有查询结果、快照、截图或样本数据

## 执行步骤

1. 复述用户目标，并明确本轮是“规划”还是“规划后立即实施”。
2. 标出任务涉及的代码、文档、数据库或外部材料范围。
3. 区分已确认事实、待补证事实和不能假设的内容。
4. 列出 3 到 7 步实施计划，尽量落到真实文件、脚本或检查动作。
5. 标出风险、待确认项和建议的下一步。

## 输出格式

优先使用以下结构：

1. 目标与范围
2. 已知事实
3. 待确认项
4. 实施步骤
5. 风险与建议

## 本仓库专用约束

- 优先引用真实文件、脚本和文档，不给抽象空话。
- 需要数据库事实时，优先使用用户已掌握材料、用户可执行查询结果或已落盘快照。
- 不默认存在内部 DBA、运维或其他数据库开发同事。
- 若发现任务涉及 ETL 代码，后续实施时应自动受 `.github/instructions/python-etl.instructions.md` 约束。

## 不应做的事

- 不在证据不足时虚构表、字段、口径或现网状态。
- 不跳过风险提示直接给出“可以开改”的结论。
- 不用规划输出替代后续的审计、验证或文档同步。

## 推荐后续动作

- 若任务边界已清晰：进入实现或专项 skill。
- 若任务存在高风险证据缺口：先让用户确认或补证。
- 若任务只是为了梳理方向：把结论同步到 `docs/子项目资料/superpowers内化会议纪要.md` 或对应专题文档。
