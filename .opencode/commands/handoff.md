---
description: 写入 HEFANG-DW Agent 交接记录
agent: build
---

# /handoff

基于当前工作区变更，向 `docs/AGENT_HANDOFF.md` 追加标准交接记录：`$ARGUMENTS`

执行要求：

1. 优先读取当前 git 变更摘要
2. 整理变更文件、变更类型、一句话说明
3. 调用 `python scripts/log_agent_action.py` 追加记录
4. 记录中必须包含：本次做了什么、接棒注意事项、未完成项
5. 写入后确认记录已追加成功

若本次只有纯阅读或没有实质变更，应明确说明无需写入。
