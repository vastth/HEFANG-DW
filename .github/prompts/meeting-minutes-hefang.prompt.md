---
name: "Meeting Minutes Hefang"
description: "Use when updating the superpowers/copilot design meeting notes after a discussion. 触发词：会议纪要更新、同步会议纪要、补充新增结论、更新阶段状态、记录内化讨论结论。"
agent: "agent"
tools: [read, search, edit]
argument-hint: "[本轮讨论摘要，例如：补充第三阶段 hooks 设计稿 | 更新阶段状态]"
---

根据本轮讨论结果，更新 [docs/misc/superpowers内化会议纪要.md](../../docs/misc/superpowers内化会议纪要.md)。

## 执行要求

1. 先读取 [docs/AGENT_HANDOFF.md](../../docs/AGENT_HANDOFF.md) 最新记录，确认当前阶段与最近一次内化动作。
2. 再读取 [docs/misc/superpowers内化会议纪要.md](../../docs/misc/superpowers内化会议纪要.md)，只更新与本轮讨论直接相关的章节。
3. 优先补以下内容：当前状态、本轮新增结论、阶段进展、待决事项、版本记录。
4. 如果本轮讨论形成了新的能力分层、实施顺序、边界约束或验收结论，必须明确写入，而不是只停留在口头总结。
5. 不把“设计中”“计划中”“未启用”的对象写成已落地现状。
6. 若本轮仅形成设计稿，没有实际创建对应文件，必须显式标注为“设计稿”或“未启用”。

## 输出格式

1. 本轮更新点
2. 修改到的章节
3. 仍待确认项