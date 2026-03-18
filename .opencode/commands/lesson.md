---
description: 写入 HEFANG-DW Agent 经验台帐
agent: build
---

# /lesson

基于当前任务或用户纠错，将一条可复用经验写入 `docs/AGENT_LESSONS.md`：`$ARGUMENTS`

执行要求：

1. 先判断本次经验来源：任务排障，还是用户业务纠错
2. 使用 `python scripts/log_agent_lesson.py ...` 写入一条标准经验记录
3. 若经验与仓库强相关，再同步到 repo memory
4. 经验必须包含：触发场景、错误假设、修正结论、证据、预防动作

若本次没有形成可复用经验，应明确说明无需写入。