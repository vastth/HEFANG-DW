---
name: handoff
description: 写入 AGENT_HANDOFF.md 交接记录。完成一组有意义的代码/文档变更后，自动汇总变更内容并调用 scripts/log_agent_action.py 写入标准格式的交接记录。
argument-hint: "[简短摘要，如：新增etl-auditor子代理]"
---

## /handoff — 写入 Agent 交接日志

### 执行步骤

**第一步：收集变更信息**

运行以下命令获取本次会话的变更摘要：
```bash
git diff --name-only HEAD
git diff --stat HEAD
```

若尚无 commit，使用：
```bash
git status --short
git diff --cached --name-only
```

**第二步：整理变更清单**

根据 git 输出，按格式整理每个变更文件：
- 格式：`路径:变更类型:一句话说明`
- 变更类型：`新增` / `修改` / `删除`
- 示例：`.claude/agents/etl-auditor.md:新增:ETL审计专家子代理`

**第三步：调用 log_agent_action.py 写入**

```bash
python scripts/log_agent_action.py \
  --agent "Claude Code" \
  --action "$ARGUMENTS" \
  --summary "（根据变更内容自动生成一句话摘要）" \
  --files "文件1路径:变更类型:说明" \
  --files "文件2路径:变更类型:说明" \
  --notes "（Copilot 接棒时需注意的风险点或同步事项）" \
  --todos "（未完成的待办项，无则填 无）"
```

**第四步：确认写入成功**

读取 `docs/AGENT_HANDOFF.md` 末尾 20 行，确认新记录已追加，时间戳正确。

### 参数说明
- `$ARGUMENTS`：你传入的简短摘要（作为 `--action` 值）
- 若未传参数，自动根据 git diff 内容生成摘要
- `--notes` 重点关注：哪些文档需要人工确认、哪些测试需要验证

### 示例调用
```
/handoff 新增 everything-claude-code 四层架构（agents/skills/hooks/mcp）
```
