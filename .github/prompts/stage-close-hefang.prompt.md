---
name: "阶段收口检查 Hefang"
description: "Use when doing a structured completion check before ending a task. 触发词：阶段收口检查、完工检查、这次任务能不能结束、结束前检查、收口复查。"
agent: "agent"
tools: [read, search, todo]
argument-hint: "[本轮任务摘要，例如：完成第二阶段 agents 落地 | 结束前收口]"
---

根据当前仓库事实，对本轮工作做一次结构化收口检查。

## 执行要求

1. 先读取 [docs/AGENT_HANDOFF.md](../../docs/AGENT_HANDOFF.md) 最新记录，确认最近一次有意义变更与未完成项。
2. 再读取必要的变更文件或相关文档，只基于已验证事实做结论。
3. 明确区分：已完成项、未验证缺口、仍待确认项。
4. 显式检查是否需要 doc-sync、handoff、lesson。
5. 不把“未跑验证”写成“已验证通过”。

## 输出格式

1. 收口结论
2. 已完成项
3. 未验证缺口
4. 建议补做事项
5. 是否需要 handoff / lesson