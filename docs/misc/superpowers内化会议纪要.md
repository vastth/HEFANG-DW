# superpowers 内化会议纪要

> 文档定位：记录“将 obra/superpowers 的方法论内化到 hefang_dw 的 GitHub Copilot 环境中”的讨论结论、阶段方案、边界约束与后续待决事项。
>
> 维护方式：后续每轮相关讨论结束后，优先更新本文件，再决定是否进入实际落地。
>
> 当前状态：讨论中，未实施。

---

## 1. 会议背景

- 目标不是直接安装或照搬 `obra/superpowers`，而是吸收其中适合当前仓库与 GitHub Copilot 环境的方法论。
- 当前仓库在 Copilot 侧已有全局规则文件 [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md)，但尚未形成与 Claude 侧同等完整的“技能 / 角色 / 工作流”分层。
- 当前仓库已具备可复用脚本基础，可作为后续 Copilot 工作流的执行底座：
  - [scripts/check_doc_sync.py](../../scripts/check_doc_sync.py)
  - [scripts/log_agent_action.py](../../scripts/log_agent_action.py)
  - [scripts/log_agent_lesson.py](../../scripts/log_agent_lesson.py)
  - [scripts/doctor.ps1](../../scripts/doctor.ps1)
- 当前仓库已具备协作文档基础：
  - [docs/AGENT_HANDOFF.md](../AGENT_HANDOFF.md)
  - [docs/AGENT_LESSONS.md](../AGENT_LESSONS.md)
  - [docs/TODO_ISSUES.md](../TODO_ISSUES.md)

---

## 2. 本次会议结论

### 2.1 总体判断

- `superpowers` 的价值不在某个单独插件，而在“初始规则 + 技能 + 角色 + 流程 + 验证闭环”的组合方式。
- 当前 VS Code 下的 GitHub Copilot 已支持 `instructions`、`prompt files`、`custom agents`、`agent skills`、`hooks`、`MCP servers` 与插件机制，因此原则上具备承接这套方法论的能力。
- 对 `hefang_dw` 来说，不应追求“完整复刻 superpowers”，而应做“按项目现实约束裁剪后的内化”。

### 2.2 已达成共识

- 不把所有能力继续堆进 [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md)。
- 保留该文件作为“全局常驻规则层”，只承载始终成立的约束。
- 将高频、可复用、多步骤的能力拆为 skill、prompt、custom agent、hook 与脚本配合。
- 先做能力架构设计，再做落地实现，避免边做边改结构。

### 2.3 当前推荐路线

- 采用“三阶段推进”方案。
- 第一阶段优先解决“是否能稳定触发、是否能闭环”。
- 第二阶段再解决“角色隔离与上下文污染”。
- 第三阶段再考虑“自动化程度与工具接入深度”。

---

## 3. 三阶段方案

### 3.1 第一阶段：能力骨架

目标：将 Copilot 从“只有总指令”升级为“总指令 + 明确工作流入口”。

优先能力：

1. 规划能力
   - 先澄清目标、边界、证据来源，再形成实施计划。
2. ETL 审计能力
   - 只读检查字段映射、增量逻辑、口径风险、幂等性风险。
3. 文档对齐能力
   - 按变更类型映射需同步的文档，而不是依赖临场记忆。
4. 收口能力
   - 在任务结束前统一检查验证、遗留风险、交接与经验沉淀。
5. 交接与经验沉淀能力
   - 将 handoff 与 lesson 从制度要求升级为标准工作流动作。

### 3.2 第二阶段：角色分化

目标：按任务类型隔离上下文，避免一个大而全 Agent 处理所有事情。

建议角色：

1. Planner
2. ETL Auditor
3. Doc Syncer
4. DB Inspector
5. Reviewer

### 3.3 第三阶段：自动化与工具接入

目标：将高频动作从“靠记忆执行”升级为“靠触发机制执行”。

候选方向：

1. hook 提醒
2. 更完整的 prompt / skill 入口
3. MCP 与外部工具联动增强

说明：第三阶段必须基于真实可用的工具面开展，不能默认“配置存在 = 当前聊天会话可用”。

---

## 4. 能力映射原则

### 4.1 适合保留在全局 instructions 的内容

- 语言偏好
- 证据优先原则
- 环境现实约束
- 文档同步红线
- 先读交接、再执行
- 禁止臆造数据库对象或内部协作角色

### 4.2 适合拆成 file instructions 的内容

- Python ETL 文件的审计重点
- SQL 文件的命名、口径与变更边界
- 文档文件的版本记录、证据引用与同步要求

### 4.3 适合做成 skill 或 prompt 的内容

- 规划
- ETL 审计
- 文档对齐
- 质检收口
- handoff
- lesson
- backfill

### 4.4 适合做成 custom agent 的内容

- Planner
- ETL Auditor
- Doc Syncer
- DB Inspector
- Reviewer

### 4.5 适合交给脚本或 hook 的内容

- 编辑 ETL / SQL 后的同步提醒
- 结束前的验证提醒
- 交接记录写入
- 经验台帐写入
- 文档审计产物刷新

---

## 5. 第一阶段建议先做的 5 个能力

1. 规划型 skill
2. ETL 审计型 skill
3. 文档对齐型 skill
4. 收口检查型 prompt 或 skill
5. Python ETL 专用 instructions

说明：第一阶段不优先引入复杂 hook，也不追求 worktree、强制 TDD、复杂分支收尾等能力。

---

## 6. 当前明确不优先的方向

- 不优先完整复刻 `superpowers` 的仓库结构与命名
- 不优先照搬 worktree 驱动的多分支流程
- 不优先引入高度仪式化的强制 TDD 流程
- 不优先把所有流程自动强制跳转
- 不把设计建立在“当前会话一定可见 MCP 工具”的假设上

---

## 7. 约束与风险

### 7.1 当前约束

- 用户是当前环境中唯一负责数据库与数仓的人，不默认存在内部 DBA / 运维协同。
- Oracle 在阿里云，MySQL 与 `hefang_dw` 运行在公司服务器虚拟机。
- 真实数据库结构、样本、推送事实必须优先基于用户可提供材料或真实查询结果。

### 7.2 当前风险

- Copilot 自定义能力虽然原生支持较多类型，但是否在当前会话稳定触发，仍需后续逐项验证。
- 当前仓库 Copilot 侧主要是单文件规则，后续拆分时要避免“规则重复、上下文过载、触发不稳定”。
- 若过早引入 hook，可能导致日常交互过重，需要分阶段验证收益。

---

## 8. 后续会议更新规则

后续凡涉及“superpowers 内化”“Copilot 能力设计”“自定义工作流拆分”的讨论，原则上同步更新本文件，更新顺序如下：

1. 先补“本轮新增结论”
2. 再更新“三阶段方案”或“能力清单”
3. 若开始实施，再补“实施进展”与“落地文件清单”
4. 若出现明确踩坑，再同步到 [docs/AGENT_LESSONS.md](../AGENT_LESSONS.md)

---

## 9. 待决事项

- 第一阶段 5 个能力的详细规格尚未逐项定稿。
- `.github` 下未来的分层目录结构尚未定稿。
- 需进一步判断哪些内容继续留在 [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md)，哪些内容必须拆出去。
- 是否为后续“会议纪要更新”再配一个专门 prompt / skill，尚未决定。

---

## 10. 本轮新增结论（2026-03-20）

- 已确认采用“三阶段推进”作为后续 superpowers 内化主路线。
- 已确认需要单独维护一份会议纪要式文档，作为后续讨论和决策的持续沉淀载体。
- 当前文档为首次建档，尚未进入实际能力落地阶段。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.1 | 2026-03-20 | 首次建档，记录 superpowers 内化讨论结论、三阶段方案与后续更新规则 |
