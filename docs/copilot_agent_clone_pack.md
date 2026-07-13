# VS Code Copilot Agent 可克隆开发架构单文件

> 文档目的：把 hefang_dw 当前已经落地并经过仓库级实践验证的 GitHub Copilot 自定义能力架构，整理成一个可直接复制到其他项目的单文件入口。
>
> 适用场景：你准备在另一台设备、另一个 VS Code 工作区或另一个项目里，复用当前这套 instructions / skills / agents / prompts / hooks / MCP / 治理闭环。
>
> 使用原则：这份文件不是业务文档，而是“Copilot 能力迁移手册”。迁移时应复制架构，不应原样照搬 hefang_dw 的业务词汇、数据库事实和真实凭据。

---

## 1. 架构总览

当前仓库的 Copilot 自定义能力按 9 层组织：

1. 全局常驻规则层
   - 入口文件：[.github/copilot-instructions.md](../.github/copilot-instructions.md)
   - 作用：定义语言偏好、协作协议、文档同步红线、环境现实约束、先读 handoff 再执行等全局规则。

2. 文件域规则层
   - 入口文件：
     - [.github/instructions/python-etl.instructions.md](../.github/instructions/python-etl.instructions.md)
     - [.github/instructions/sql.instructions.md](../.github/instructions/sql.instructions.md)
     - [.github/instructions/docs.instructions.md](../.github/instructions/docs.instructions.md)
   - 作用：将 ETL、SQL、Markdown 文档等高上下文规则从全局说明里拆出，减少不相关对话的上下文负担。

3. 技能工作流层
   - 入口目录：[.github/skills](../.github/skills)
   - 当前已落地：
     - [.github/skills/planning-hefang/SKILL.md](../.github/skills/planning-hefang/SKILL.md)
     - [.github/skills/etl-audit-hefang/SKILL.md](../.github/skills/etl-audit-hefang/SKILL.md)
     - [.github/skills/doc-sync-hefang/SKILL.md](../.github/skills/doc-sync-hefang/SKILL.md)
     - [.github/skills/completion-check-hefang/SKILL.md](../.github/skills/completion-check-hefang/SKILL.md)
     - [.github/skills/project-bootstrap-hefang/SKILL.md](../.github/skills/project-bootstrap-hefang/SKILL.md)
   - 作用：承接“规划、审计、文档对齐、收口、新项目搭框架”这类多步骤但可复用的工作流。

4. 角色隔离层
   - 入口目录：[.github/agents](../.github/agents)
   - 当前已落地：
     - [.github/agents/planner-hefang.agent.md](../.github/agents/planner-hefang.agent.md)
     - [.github/agents/etl-auditor-hefang.agent.md](../.github/agents/etl-auditor-hefang.agent.md)
     - [.github/agents/doc-syncer-hefang.agent.md](../.github/agents/doc-syncer-hefang.agent.md)
     - [.github/agents/db-inspector-hefang.agent.md](../.github/agents/db-inspector-hefang.agent.md)
     - [.github/agents/reviewer-hefang.agent.md](../.github/agents/reviewer-hefang.agent.md)
   - 作用：将“规划、审计、对齐、结构核对、评审”拆成职责单一且工具受控的 agent，降低上下文污染。

5. 单任务 prompt 层
   - 入口目录：[.github/prompts](../.github/prompts)
   - 当前已落地：
     - [.github/prompts/runtime-acceptance-hefang.prompt.md](../.github/prompts/runtime-acceptance-hefang.prompt.md)
     - [.github/prompts/meeting-minutes-hefang.prompt.md](../.github/prompts/meeting-minutes-hefang.prompt.md)
     - [.github/prompts/stage-close-hefang.prompt.md](../.github/prompts/stage-close-hefang.prompt.md)
   - 作用：把“运行时验收、会议纪要更新、阶段收口”这种单任务入口收敛为 prompt，而不是每次临时重新描述。

6. Hook 提醒层
   - 入口文件：[.github/hooks/post-edit-reminder-hefang.json](../.github/hooks/post-edit-reminder-hefang.json)
   - 运行脚本：
     - [scripts/copilot_post_edit_reminder.py](../scripts/copilot_post_edit_reminder.py)
     - [scripts/copilot_session_close_reminder.py](../scripts/copilot_session_close_reminder.py)
   - 作用：在编辑后和会话结束前给出非阻断提醒，把“记得 doc-sync、handoff、lesson、最小验证”从记忆动作变成触发动作。

7. MCP 与本地启动层
   - 入口文件：
     - [.vscode/mcp.json](../.vscode/mcp.json)
     - [.vscode/settings.json](../.vscode/settings.json)
     - [.vscode/start_dbhub.ps1](../.vscode/start_dbhub.ps1)
     - [.vscode/start_oracle_mcp.ps1](../.vscode/start_oracle_mcp.ps1)
   - 作用：把 VS Code Copilot 运行时真正可见的 MCP 入口和本地解释器 / 启动脚本固化到工作区。

8. 治理与审计闭环层
   - 关键文件：
     - [docs/AGENT_HANDOFF.md](AGENT_HANDOFF.md)
     - [docs/AGENT_LESSONS.md](AGENT_LESSONS.md)
       - [docs/AGENT_LESSONS_INDEX.md](AGENT_LESSONS_INDEX.md)
     - [docs/TODO_ISSUES.md](TODO_ISSUES.md)
     - [scripts/log_agent_action.py](../scripts/log_agent_action.py)
     - [scripts/log_agent_lesson.py](../scripts/log_agent_lesson.py)
   - 作用：让“交接、经验沉淀、未完成项、风险跟踪”成为架构的一部分，而不是聊天结束时的可选动作。

9. 上下文压缩与防注入层
    - 关键文件：
       - [scripts/agent_context_pack.py](../scripts/agent_context_pack.py)
       - [scripts/build_agent_lessons_index.py](../scripts/build_agent_lessons_index.py)
       - [reports/agent_context_summary.md](../reports/agent_context_summary.md)
    - 作用：把开局上下文、经验台账、大型审计产物和数据库查询结果从“整篇读取”改成“短摘要 + 索引 + 定向证据读取”，同时明确数据库文本和外部内容只作为数据证据，不能覆盖项目硬约束。

---

## 2. 最小可迁移包

### 2.1 必须复制

| 当前文件/目录 | 是否必需 | 迁移建议 | 说明 |
|------|------|------|------|
| [.github/copilot-instructions.md](../.github/copilot-instructions.md) | 是 | 直接复制后改项目名、约束和文档矩阵 | 整个架构的总入口，没有它就只剩零散 skill/agent |
| [.github/instructions](../.github/instructions) | 是 | 至少保留 1 个与你新项目最核心文件类型对应的 instructions | 负责把领域规则从全局说明中拆出去 |
| [.github/skills](../.github/skills) | 是 | 至少复制 planning、doc-sync、completion-check 三类技能 | 没有技能层，Copilot 只能依赖全局 instructions |
| [docs/AGENT_HANDOFF.md](AGENT_HANDOFF.md) | 是 | 新项目必须新建同名文件 | 没有 handoff，就没有多轮接棒入口 |
| [docs/AGENT_LESSONS.md](AGENT_LESSONS.md) | 是 | 新项目必须新建同名文件 | 没有 lesson，经验无法累积 |
| [docs/AGENT_LESSONS_INDEX.md](AGENT_LESSONS_INDEX.md) | 是 | 可由脚本生成 | 避免整篇经验台账进入常规上下文 |
| [docs/TODO_ISSUES.md](TODO_ISSUES.md) | 是 | 新项目必须新建同名文件 | 没有 todo 风险台账，P0/P1/P2 无法落盘 |
| [scripts/log_agent_action.py](../scripts/log_agent_action.py) | 是 | 直接复制后保留脚本路径 | handoff 自动化写入依赖它 |
| [scripts/log_agent_lesson.py](../scripts/log_agent_lesson.py) | 是 | 直接复制后保留脚本路径 | lesson 自动化写入依赖它 |
| [scripts/agent_context_pack.py](../scripts/agent_context_pack.py) | 是 | 复制后改项目文档路径 | 开局短上下文包生成入口 |
| [scripts/build_agent_lessons_index.py](../scripts/build_agent_lessons_index.py) | 是 | 复制后改经验台账解析规则 | 经验台账索引生成入口 |

### 2.2 推荐一起复制

| 当前文件/目录 | 是否推荐 | 迁移建议 | 说明 |
|------|------|------|------|
| [.github/agents](../.github/agents) | 推荐 | 第二阶段就复制 | 适合需要角色隔离和工具边界的新项目 |
| [.github/prompts](../.github/prompts) | 推荐 | 复制后改 prompt 描述和引用路径 | 适合把固定治理动作收敛成 slash 入口 |
| [.github/hooks/post-edit-reminder-hefang.json](../.github/hooks/post-edit-reminder-hefang.json) | 推荐 | 复制后改命名和脚本匹配规则 | 能把编辑提醒和收口提醒自动化 |
| [scripts/copilot_post_edit_reminder.py](../scripts/copilot_post_edit_reminder.py) | 推荐 | 按新项目文件类型修改正则 | PostToolUse 提醒实际逻辑在这里 |
| [scripts/copilot_session_close_reminder.py](../scripts/copilot_session_close_reminder.py) | 推荐 | 保留逻辑，调整提示文字即可 | Stop 提醒依赖最近编辑命中日志 |
| [.vscode/settings.json](../.vscode/settings.json) | 推荐 | 改成新设备 / 新项目解释器 | 工作区 Python 与运行时细节统一入口 |

### 2.3 按需复制

| 当前文件/目录 | 适用条件 | 迁移建议 | 说明 |
|------|------|------|------|
| [.vscode/mcp.json](../.vscode/mcp.json) | 新项目需要 MCP | 不要原样复制真实连接串，只复制结构 | 当前文件带本地环境事实，不应直接提交到其他仓库 |
| [.vscode/start_dbhub.ps1](../.vscode/start_dbhub.ps1) | 新项目仍走 DBHub | 保留脚本结构，改 Node 路径和 DSN 注入方式 | DBHub 的 stdio 启动器 |
| [.vscode/start_oracle_mcp.ps1](../.vscode/start_oracle_mcp.ps1) | 新项目仍连 Oracle | 保留脚本结构，改环境变量名和默认 schema | Oracle MCP 启动器 |
| [AGENTS.md](../AGENTS.md) | 新项目还要兼容 OpenCode / Claude | 可选复制 | VS Code Copilot 主链不强依赖它，但多 Agent 协作时有价值 |
| [.claude](../.claude) | 需要 Claude 兼容层 | 可选复制 | 不属于 VS Code Copilot 最小闭环 |

---

## 3. 推荐目录树

建议把新项目的目标结构整理成下面这样：

```text
<new-project>/
├── .github/
│   ├── copilot-instructions.md
│   ├── instructions/
│   │   └── <domain>.instructions.md
│   ├── skills/
│   │   ├── planning-<project>/SKILL.md
│   │   ├── doc-sync-<project>/SKILL.md
│   │   ├── completion-check-<project>/SKILL.md
│   │   └── <other-workflow>/SKILL.md
│   ├── agents/
│   │   ├── planner-<project>.agent.md
│   │   ├── reviewer-<project>.agent.md
│   │   └── <other-role>.agent.md
│   ├── prompts/
│   │   ├── runtime-acceptance-<project>.prompt.md
│   │   ├── meeting-minutes-<project>.prompt.md
│   │   └── stage-close-<project>.prompt.md
│   └── hooks/
│       └── post-edit-reminder-<project>.json
├── .vscode/
│   ├── settings.json
│   ├── mcp.json
│   ├── start_dbhub.ps1
│   └── start_oracle_mcp.ps1
├── docs/
│   ├── AGENT_HANDOFF.md
│   ├── AGENT_LESSONS.md
│   ├── TODO_ISSUES.md
│   └── <project-docs>.md
└── scripts/
    ├── log_agent_action.py
    ├── log_agent_lesson.py
    ├── copilot_post_edit_reminder.py
    └── copilot_session_close_reminder.py
```

---

## 4. 迁移时必须替换的内容

复制这套架构时，至少要替换以下 8 类内容：

1. 项目标识
   - 把 `hefang_dw`、`Hefang`、`-hefang` 改成新项目的项目名、slug 或团队后缀。

2. 技能与 agent 名称
   - skill 的 `name` 应与目录名一致。
   - agent 的 `name` 要贴近新项目语义，不要继续保留 `Hefang`。

3. `description` 触发词
   - 这是 Copilot 发现 skill / prompt / agent 的关键字段。
   - 迁移后必须改成贴近新项目真实提问方式的关键词。

4. 领域 instructions
   - 当前仓库的 file instructions 偏 Python ETL。
   - 如果新项目不是 ETL 项目，应改成更贴近领域的 instructions，例如 `python-app.instructions.md`、`sql-audit.instructions.md`、`frontend.instructions.md`。

5. Hook 正则规则
   - [scripts/copilot_post_edit_reminder.py](../scripts/copilot_post_edit_reminder.py) 里的文件匹配规则是 hefang_dw 定制版。
   - 新项目必须按自己的文件类型和关键文档改写正则。

6. 文档路径与治理对象
   - prompt、instructions、meeting notes 中引用的文档路径必须改成新项目真实存在的文件。
   - 不要把不存在的 `docs/AGENT_HANDOFF.md`、`docs/TODO_ISSUES.md`、专项会议纪要路径写进新项目。

7. MCP 连接方式
   - [.vscode/mcp.json](../.vscode/mcp.json) 当前包含本地连接事实。
   - 新项目只能复制结构，不能复制真实 DSN、用户名、密码或本机路径。

8. 数据库 / 运行环境现实约束
   - 当前仓库默认“用户是唯一数据库负责人”的约束，是 hefang_dw 的环境事实。
   - 新项目应改成自身真实的组织、权限和部署边界。

---

## 5. 分阶段部署顺序

不要一次性把所有 primitives 都搬过去。推荐按下面顺序分阶段部署：

### 阶段 A：先落最小闭环

复制并改造以下内容：

1. `.github/copilot-instructions.md`
2. `docs/AGENT_HANDOFF.md`
3. `docs/AGENT_LESSONS.md`
4. `docs/TODO_ISSUES.md`
5. `scripts/log_agent_action.py`
6. `scripts/log_agent_lesson.py`
7. 至少 3 个 skill：planning、doc-sync、completion-check
8. 至少 1 个领域 instructions

阶段 A 的目标不是“功能很多”，而是“已经形成治理闭环”。

### 阶段 B：再补角色隔离

复制并改造以下内容：

1. `.github/agents/`
2. `Reviewer` 类 agent
3. `Planner` 类 agent
4. 至少 1 个领域审计 agent

阶段 B 的目标是把“复杂任务都进一个大 agent”改成“职责单一、工具受控”。

### 阶段 C：最后补 prompts、hooks、MCP

复制并改造以下内容：

1. `.github/prompts/`
2. `.github/hooks/post-edit-reminder-<project>.json`
3. `scripts/copilot_post_edit_reminder.py`
4. `scripts/copilot_session_close_reminder.py`
5. `.vscode/mcp.json` 和启动脚本

阶段 C 的目标是把“靠记忆执行”进一步升级成“靠触发机制执行”。

---

## 6. 最小模板片段

下面这些模板片段不是 hefang_dw 原文，而是适合迁移到新项目时直接改名复用的通用骨架。

### 6.1 skill 模板

```md
---
name: planning-<project>
description: "Use when the user asks to plan, scope, or discuss a complex task before implementation. Trigger phrases include: 帮我规划, 先拆方案, 先别动手."
argument-hint: "[任务目标或范围]"
---

# planning-<project>

## 作用

在真正修改代码、文档或配置之前，先把目标、范围、证据和风险拆清楚。

## 输入

- 用户目标
- 涉及模块或文件
- 是否允许直接实施

## 执行步骤

1. 复述目标与边界
2. 标出涉及的代码、文档、数据库或外部材料
3. 区分已知事实与待确认项
4. 给出 3 到 7 步执行计划
5. 输出风险与建议下一步
```

### 6.2 agent 模板

```md
---
name: "Reviewer <Project>"
description: "Use when reviewing changes, checking delivery risk, or doing final acceptance review. Trigger phrases include: review, 代码评审, 完工检查, 上线前检查."
tools: [read, search]
argument-hint: "[评审范围]"
user-invocable: true
---

你是 <project> 的评审代理，负责从风险、回归、遗漏验证和交付完整性角度做 review。

## 约束

- 以发现问题为优先
- 不把未运行的验证描述为已通过
- 不直接改代码，只做评审
```

### 6.3 prompt 模板

```md
---
name: "Runtime Acceptance <Project>"
description: "Use when validating VS Code Copilot runtime discovery for instructions, skills, agents, prompts, or hooks."
agent: "agent"
tools: [read, search, todo]
argument-hint: "[验收范围]"
---

对当前仓库的 GitHub Copilot 自定义能力做一次运行时验收，重点检查：

1. `/` 列表里能否看到目标 skill / prompt
2. agent picker 里能否看到目标 agent
3. References / Diagnostics 是否能看到 instructions
4. hooks 是否至少按日志证明被执行
```

### 6.4 hook 配置模板

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "type": "command",
        "command": "python .\\scripts\\copilot_post_edit_reminder.py",
        "timeout": 10
      }
    ],
    "Stop": [
      {
        "type": "command",
        "command": "python .\\scripts\\copilot_session_close_reminder.py",
        "timeout": 10
      }
    ]
  }
}
```

### 6.5 MCP 配置模板

> 注意：这里只给结构，不给任何真实连接信息。

```json
{
  "servers": {
    "dbhub": {
      "type": "stdio",
      "command": "powershell.exe",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        "${workspaceFolder}\\.vscode\\start_dbhub.ps1",
        "-Dsn",
        "<LOCAL_ONLY_DSN>"
      ]
    },
    "oracle": {
      "type": "stdio",
      "command": "powershell.exe",
      "args": [
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        "${workspaceFolder}\\.vscode\\start_oracle_mcp.ps1"
      ]
    }
  }
}
```

---

## 7. 新项目部署检查清单

迁移完成后，至少要做下面 10 项检查：

1. `.github/copilot-instructions.md` 已改成新项目真实约束，而不是仍写 hefang_dw。
2. 至少 1 个 file instructions 已命中新项目真实文件类型，而不是继续写 `etl_*.py`。
3. 至少 3 个 skill 已在 `/` 列表中可见。
4. 至少 1 个 agent 已在 agent picker 中可见。
5. `description` 中的触发词已经改成新项目真实提问方式。
6. `docs/AGENT_HANDOFF.md`、`docs/AGENT_LESSONS.md`、`docs/TODO_ISSUES.md` 已存在。
7. `scripts/log_agent_action.py` 与 `scripts/log_agent_lesson.py` 能在新项目路径下正常运行。
8. hook 配置至少能写日志，不要求一上来就追求 UI warning 完美展示。
9. `.vscode/mcp.json` 已替换为本地安全配置，没有提交真实密钥或连接串。
10. 已用 [`.github/prompts/runtime-acceptance-hefang.prompt.md`](../.github/prompts/runtime-acceptance-hefang.prompt.md) 的思路，在新项目做过一次静态 + 运行时验收。

---

## 8. 迁移建议结论

如果你想把这套架构迁到另一个项目，最稳妥的做法不是“一次性全搬”，而是：

1. 先搬治理闭环
   - `copilot-instructions` + `handoff` + `lesson` + `todo` + 写入脚本。

2. 再搬技能层
   - 先有 planning / doc-sync / completion-check 三件套。

3. 然后再搬 agent、prompt、hook、MCP
   - 这些都是增强层，不是第一天就必须一次到位。

4. 整个过程中只复制“架构和方法”，不要复制 hefang_dw 的业务词汇、数据库事实和真实凭据。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.1 | 2026-04-29 | 新增上下文压缩与防注入层，补充 AGENT_LESSONS_INDEX 与 agent_context_pack 迁移要求 |
| v1.0 | 2026-04-11 | 新增 VS Code Copilot Agent 可克隆开发架构单文件，沉淀复制矩阵、替换项、部署顺序与模板片段 |