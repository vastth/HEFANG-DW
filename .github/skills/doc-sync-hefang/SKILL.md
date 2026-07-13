---
name: doc-sync-hefang
description: "Use when checking whether code and documentation are aligned after ETL, SQL, schema, scheduling, or README changes. Runs doc sync audit, summarizes gaps, and keeps business-rule changes in confirmation-required mode."
argument-hint: "[变更范围或目标，例如：dws_sales 字段调整 | 仅扫描 | 审计后修复]"
---

# doc-sync-hefang

## 作用

在代码、SQL、表结构、调度或运行说明发生变化后，检查文档是否同步，并在合适时机推动修复与复扫。

## 何时使用

- 用户说“同步文档”“检查文档和代码是否一致”“跑一下 doc-sync”
- 完成 ETL、SQL、调度或 README 变更后
- 准备交付、合并或阶段收口前

## 输入

- 变更范围或目标模块
- 是否只做扫描
- 是否允许在确认后执行修复

## 执行步骤

1. 运行 `python scripts/check_doc_sync.py --output reports/docs_code_alignment.json`。
2. 读取审计结果，区分 `MISSING`、`OUTDATED`、`OK`。
3. 结合 `.github/copilot-instructions.md` 中的同步矩阵，识别必须更新的文档范围。
4. 输出差异清单、风险等级和建议修复顺序。
5. 若用户确认允许修复，再执行文档修订。
6. 修复后再次运行审计脚本，确认差异数量是否下降。

## 输出格式

优先使用以下结构：

1. 差异摘要
2. 高风险项
3. 中低风险项
4. 建议修复范围
5. 复扫结果或下一步

## 本仓库专用约束

- 第一阶段默认采用“先扫描、再确认修复”的模式，不直接全自动改文档。
- 涉及业务口径时，必须先停下来让用户确认，不能自行改写口径文档。
- 修订必须优先引用真实代码、SQL、脚本、快照或本轮审计 JSON 作为证据。
- 若本轮讨论涉及 Copilot 能力设计，也要检查 `docs/子项目资料/superpowers内化会议纪要.md` 是否需要同步更新。

## 不应做的事

- 不跳过审计脚本直接凭记忆改文档。
- 不把规划项、未实现项误写成已实现现状。
- 不在修复后省略复扫。

## 推荐后续动作

- 若有高风险差异：优先修复表名、入口脚本和核心任务键名。
- 若仅有低风险差异：可留到下一轮，但应在输出中明确标注。
- 若全部通过：再进入交接或收口检查。
