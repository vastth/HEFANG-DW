---
name: "Runtime Acceptance Hefang"
description: "Use when validating VS Code Copilot runtime discovery for instructions, skills, agents, prompts, or hooks. 触发词：运行时验收、可发现性验证、skill 是否出现、agent picker 验收、References/Diagnostics 检查。"
agent: "agent"
tools: [read, search, todo]
argument-hint: "[验收范围，例如：第一阶段 skills + instructions | 第二阶段 agents]"
---

对当前仓库的 GitHub Copilot 自定义能力做一次运行时验收，重点关注“是否真的被 VS Code 发现和可用”，而不是只看静态文件是否存在。

## 执行要求

1. 先读取 [docs/AGENT_HANDOFF.md](../../docs/AGENT_HANDOFF.md) 最新记录，确认当前阶段和未完成项。
2. 再读取 [docs/misc/superpowers内化会议纪要.md](../../docs/misc/superpowers内化会议纪要.md)，确认当前要验收的是第一阶段还是第二阶段对象。
3. 先输出一份最小验收计划，明确本轮要检查哪些对象：instructions、skills、agents、prompts、hooks。
4. 对仓库内能直接验证的部分，先做静态核对；对必须依赖 VS Code UI 的部分，明确告诉用户要观察哪里，例如 References、Diagnostics、`/` 列表、agent picker。
5. 如果当前聊天代理无法直接读取 UI 结果，向用户收集最少量的人工观测结果后再继续判断。
6. 不要把“未观测到的问题”表述成“已经验证通过”。

## 输出格式

1. 验收范围
2. 已确认通过
3. 保留观察项
4. 未通过项
5. 建议下一步