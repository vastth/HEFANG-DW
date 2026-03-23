---
name: completion-check-hefang
description: "Use when a coding, ETL, audit, or documentation task seems finished and you need a structured completion check. Verifies validation status, remaining risks, doc-sync needs, handoff needs, and lesson-capture needs before ending the task."
argument-hint: "[本轮任务摘要，例如：完成 dws_sales 文档对齐 | 结束前收口检查]"
---

# completion-check-hefang

## 作用

在任务准备结束时做结构化收口，避免“看起来完成”但缺验证、缺文档同步、缺交接或缺经验沉淀。

## 何时使用

- 用户说“帮我收口”“检查这次任务能不能结束”“做一下完工检查”
- 完成一组代码或文档变更后
- 准备结束当前会话前

## 输入

- 本轮任务摘要
- 本轮变更范围
- 是否已执行测试、脚本或人工验证
- 是否仍存在未确认假设

## 执行步骤

1. 汇总本轮已修改文件和核心结果。
2. 检查是否运行过最小验证，或明确哪些验证未执行。
3. 检查是否应进行文档同步或重新复扫。
4. 检查是否应写入 `docs/AGENT_HANDOFF.md`。
5. 检查是否形成可复用经验，是否应写入 `docs/AGENT_LESSONS.md`。
6. 输出“可结束 / 不建议结束”结论，并附上剩余缺口。

## 输出格式

优先使用以下结构：

1. 收口结论
2. 已完成项
3. 未验证缺口
4. 建议补做事项
5. 是否需要 handoff / lesson

## 本仓库专用约束

- 不能把“未运行验证”表述成“已验证通过”。
- 若本轮涉及 ETL、SQL、调度或文档治理，显式检查是否需要 `doc-sync-hefang`。
- 若本轮形成可复用经验或用户明确纠正了业务逻辑/字段语义，应提示写入 `docs/AGENT_LESSONS.md`。
- 若本轮有有意义变更，应提示检查 `docs/AGENT_HANDOFF.md` 是否已更新。

## 不应做的事

- 不用模糊措辞掩盖未验证风险。
- 不把收口检查等同于测试执行。
- 不跳过交接和经验沉淀提醒。

## 推荐后续动作

- 若缺口较小：列出 1 到 3 个补做动作后结束。
- 若缺口较大：建议继续处理，不要宣称完成。
- 若已满足收口条件：再进入 handoff 或结束会话。
