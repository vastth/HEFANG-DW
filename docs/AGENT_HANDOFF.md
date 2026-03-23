# AGENT_HANDOFF.md — Agent 协作交接日志

> **这是 Claude Code 与 GitHub Copilot 之间的共享状态文件。**
>
> - **Claude Code**：每次完成一组有意义的代码/文档变更后，必须在本文件顶部追加一条记录。
> - **GitHub Copilot**：接手任何任务（审计、续写、重构）前，必须先读本文件最新一条记录，了解当前项目状态。
>
> **格式约定**：新记录追加在"交接日志"节的顶部（最新在最前）。保留最近 10 条，更早的归档到 `docs/AGENT_HANDOFF_archive.md`。
>
> **写入方式**：
> ```bash
> # 推荐：使用辅助脚本（自动格式化 + 追加）
> python scripts/log_agent_action.py \
>   --agent "Claude Code" \
>   --action "新增文件" \
>   --summary "一句话描述" \
>   --files "路径1:新增:说明" "路径2:修改:说明" \
>   --notes "Copilot 接棒须知1" "接棒须知2" \
>   --todos "未完成项1" "未完成项2"
>
> # 或直接手动在本文件顶部追加（见下方模板）
> ```

---

## 交接日志

---

### [2026-03-23 15:35] · GitHub Copilot · 审计当前 ETL 链路打通情况

**摘要**：确认 ODS 仍为独立链路，主自动化链仅覆盖 DIM/DWS/达播检查/ADS，且当前 DWS/DIM 运行时未消费 ODS。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/AGENT_HANDOFF.md` | 修改 | 追加本轮只读链路审计结论 |

**Copilot 接棒须知**：
- scheduled_etl.py 当前只调 run_etl.py，未串 run_ods.py。
- run_etl.py 主链步骤不含任何 ods 任务，ODS 仍需独立调度。
- etl_dws_sales.py 与 etl_dws_inventory.py 当前仍直连 Oracle，未切到消费 ods_m_retail/ods_m_retailitem/ods_fa_storage。
- ADS 已消费 DWS 与 DIM，因此主链内部 DIM→DWS→ADS 是连通的，但 ODS→DWS/DIM 尚未打通。

**未完成项**：
- [ ] 如需真正打通自动化全链路，先明确 run_ods.py 与 run_etl.py 的调度前后关系及失败策略。
- [ ] 如需真正让 ODS 成为事实源，需要把 dws_sales/dws_inventory 改为从 ODS 聚合，并评估 dim 是否仍保持直连 Oracle。

---

### [2026-03-23 11:45] · GitHub Copilot · 继续推进第二阶段 agent 内化

**摘要**：收敛 5 个 agent 的 description，并把推进重心切回 agents 可发现性验收

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/agents/planner-hefang.agent.md` | 修改 | 补充更贴近真实提问的触发词 |
| `.github/agents/etl-auditor-hefang.agent.md` | 修改 | 补充字段血缘和自然语言触发词 |
| `.github/agents/doc-syncer-hefang.agent.md` | 修改 | 补充数据字典与补文档类触发词 |
| `.github/agents/db-inspector-hefang.agent.md` | 修改 | 补充结构漂移与快照核对触发词 |
| `.github/agents/reviewer-hefang.agent.md` | 修改 | 补充风险评审类自然语言触发词 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 更新当前状态为 hooks 通过并切回第二阶段 agent 收敛 |
| `CHANGELOG.md` | 修改 | 新增 v0.8.11 记录 agent description 收敛 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀 agent description 应贴近真实提问方式的经验 |

**Copilot 接棒须知**：
- 当前 hooks 不再作为阶段阻断项，后续第三阶段只在不破坏现有逻辑的前提下再做体验优化。
- 下一步优先在 agent picker 和自然语言场景里观察 5 个 agent 是否更容易被找到和理解。

**未完成项**：
- [ ] 在 VS Code Copilot 的 agent picker 中复测 5 个 agent 的可见性与描述可理解性
- [ ] 根据真实使用反馈继续收窄各 agent 的 tools 集合，避免授权过宽


---

### [2026-03-23 11:41] · GitHub Copilot · 确认 hooks 按逻辑正常执行

**摘要**：用户已确认 Stop 与 PostToolUse 都能出现，本轮验收以 hooks 按逻辑运行作为通过标准

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/AGENT_HANDOFF.md` | 修改 | 追加本轮 hooks 运行验收结论 |

**Copilot 接棒须知**：
- 当前不再继续纠结 warning 卡片的 UI 细节，后续以日志命中、去重行为和真实触发结果作为主要验收依据。
- Stop 与 PostToolUse 当前都已有真实触发证据；若后续再调 UI 展示，属于体验优化，不影响本轮通过。

**未完成项**：
- [ ] 若后续继续优化，仅在不破坏当前触发逻辑的前提下收敛 UI 文案或噪音



---

### [2026-03-23 11:21] · GitHub Copilot · 收敛 Copilot hooks Python 化兼容层

**摘要**：将 PostToolUse 切到 Python，并为旧的 pwsh/cmd 路径补齐兼容包装层

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/hooks/post-edit-reminder-hefang.json` | 修改 | PostToolUse 与 Stop 主入口统一收敛到 Python |
| `scripts/copilot_post_edit_reminder.py` | 新增 | 新增 Python 版 PostToolUse 提醒主实现 |
| `scripts/copilot_post_edit_reminder.ps1` | 修改 | 改为转发到 Python 的兼容包装层 |
| `scripts/copilot_session_close_reminder.ps1` | 修改 | 改为转发到 Python 的兼容包装层 |
| `scripts/copilot_session_close_reminder.cmd` | 新增 | 恢复 Stop 旧 cmd 路径兼容包装层 |
| `CHANGELOG.md` | 修改 | 记录 PostToolUse Python 化与兼容层策略 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 更新当前 hooks 主实现状态 |
| `docs/AGENT_LESSONS.md` | 修改 | 记录宿主配置滞后时需保留旧入口兼容层的经验 |

**Copilot 接棒须知**：
- 当前 Stop 与 PostToolUse 主实现均已切到 Python，但需在真实 Copilot UI 中再观察宿主噪音是否下降。
- 若当前会话仍沿用旧 hook 配置，兼容包装层已可避免旧 cmd/ps1 路径缺失导致的额外报错。

**未完成项**：
- [ ] 在真实 Copilot 会话中复测 Python 版 Stop warning 卡片是否更干净
- [ ] 在真实 Copilot 会话中复测 PostToolUse warning 是否摆脱 pwsh NativeCommandError 风格噪音
- [ ] 根据真实 UI 结果决定何时移除旧的 pwsh/cmd 兼容包装层




---

### [2026-03-23 11:08] · GitHub Copilot · 确认 Stop UI 可见并修正提示可读性

**摘要**：真实 Copilot 会话已观察到 Warning from Stop hook，并将 Stop 提示文案收敛为 ASCII 以规避 stderr 中文乱码。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `scripts/copilot_session_close_reminder.ps1` | 修改 | 将 Stop warning 文案和动作提示改为 ASCII，优先保证宿主 UI 可读性 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 补充 Stop warning 已在真实 UI 显示且中文 stderr 会乱码的结论 |
| `CHANGELOG.md` | 修改 | 补充 v0.8.10 的真实 UI 观测与 ASCII 收敛说明 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀 Stop hook UI 可显示但中文 stderr 可能乱码的经验 |

**Copilot 接棒须知**：
- 当前 Stop hook 已有真实 Copilot UI 证据，后续不必再验证‘会不会显示’，重点转到‘是否稳定显示’和‘文案是否可读’。
- 只要继续沿用 PowerShell 非零 stderr 路径，用户侧提示建议优先保持 ASCII；中文说明放日志、会议纪要和经验台账。
- 本轮仅做了最小可读性修正，未改变 Stop 提醒的触发窗口、去重策略和证据来源。

**未完成项**：
- [ ] 在真实 Copilot 会话中继续观察 Stop warning 的稳定性，而不只是单次可见
- [ ] 根据后续复测结果决定是否也把 PostToolUse warning 文案收敛为 ASCII
- [ ] 继续决定下一步优先做第二阶段 agent picker 验收，还是继续扩第三阶段提醒型 hooks





---

### [2026-03-23 10:54] · GitHub Copilot · 新增 Stop 收口提醒试点

**摘要**：新增基于 PostToolUse 日志信号的最小 Stop hook，并完成去重验证。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/hooks/post-edit-reminder-hefang.json` | 修改 | 扩展 Stop 事件并接入 session close 脚本 |
| `scripts/copilot_session_close_reminder.ps1` | 新增 | 基于最近 PostToolUse 命中日志输出非阻断收口提醒并做短时去重 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 记录第二个提醒型 hook 试点与当前边界 |
| `CHANGELOG.md` | 修改 | 新增 v0.8.10 Stop 收口提醒试点记录 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀 Stop hook 应优先复用运行时日志信号的经验 |

**Copilot 接棒须知**：
- 当前第三阶段已同时具备 PostToolUse 和 Stop 两个提醒型 hook 试点，但仍以非阻断 warning 为主，不进入 ask/deny。
- Stop 提醒当前依赖 logs/copilot_post_edit_reminder.log 作为最近编辑证据，避免被历史未提交改动误报带偏；若后续窗口或去重策略不合适，应直接调 scripts/copilot_session_close_reminder.ps1。
- 本轮已手工验证：首次运行 Stop 脚本返回 warning，短时间重复运行同签名返回 continue。

**未完成项**：
- [ ] 在真实 Copilot 会话里观察 Stop warning 是否稳定展示
- [ ] 根据真实使用情况收敛最近窗口和去重时间
- [ ] 继续决定下一步优先做第二阶段 agent picker 验收，还是继续扩第三阶段提醒型 hooks






---

### [2026-03-23 10:45] · GitHub Copilot · 继续细分 PostToolUse docs 规则

**摘要**：将文档类提醒继续拆到数据字典类和协作文治理类，并验证六类文档样例均命中预期规则。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `scripts/copilot_post_edit_reminder.ps1` | 修改 | 新增 data-dictionary 与 governance-docs 两类规则并收窄 runbook-docs 范围 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 记录 docs 规则按后续动作差异继续细分 |
| `CHANGELOG.md` | 修改 | 新增 v0.8.9 记录 docs 二次细分 |

**Copilot 接棒须知**：
- 当前 docs 细分的意义是让 warning 直接对应后续动作：数据字典关注字段/契约/映射，治理文档关注 handoff/lesson/todo 一致性，运行文档关注命令与说明同步。
- 本轮最小验证已在日志中确认 MYSQL数据字典、AGENT_HANDOFF、RUNBOOK、README、会议纪要和普通 docs 分别命中 data-dictionary、governance-docs、runbook-docs、readme、meeting-minutes、doc。

**未完成项**：
- [ ] 在真实 Copilot 会话中分别编辑数据字典类和协作文治理类文档，观察新的 warning 分类是否稳定显示
- [ ] 若后续还要继续细分，只在某一类文件具有明确不同收口动作时再新增规则，避免为分类而分类







---

### [2026-03-23 10:24] · GitHub Copilot · 细分 PostToolUse docs 提醒规则

**摘要**：将文档类 PostToolUse 提醒拆为会议纪要类、运行文档类、README 类和兜底 docs 类，并完成最小命中验证。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `scripts/copilot_post_edit_reminder.ps1` | 修改 | 新增 meeting-minutes、runbook-docs、readme 三类 docs 规则并修正匹配正则 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 记录 docs 细粒度规则扩展与当前阶段状态 |
| `CHANGELOG.md` | 修改 | 新增 v0.8.8 记录 docs 细粒度规则扩展 |

**Copilot 接棒须知**：
- 当前 docs 类提醒已不再统一落到 doc；后续若继续细分，可优先考虑数据字典类与协作文档类，而不是继续增加过多低收益分支。
- 本轮最小验证已在日志中确认四类输入分别命中 meeting-minutes、runbook-docs、readme 和 doc；若下一步做真实 UI 复测，优先改这四类文件观察 warning 展示。

**未完成项**：
- [ ] 在真实 Copilot 会话中分别编辑会议纪要、RUNBOOK 和 README，观察不同 docs 子类 warning 是否稳定显示
- [ ] 若后续继续扩规则，评估是否单独拆出数据字典类或交接治理类文档提醒








---

### [2026-03-23 10:19] · GitHub Copilot · 调整 PostToolUse warning 返回策略

**摘要**：将提醒型 hook 从 systemMessage 成功返回切换为非阻断 warning 退出码，并同步沉淀 UI 展示排障结论。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `scripts/copilot_post_edit_reminder.ps1` | 修改 | 命中提醒时改为 stderr 文案加退出码 1，未命中仍返回 continue JSON |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 记录 systemMessage 与稳定 UI warning 的边界，并更新第三阶段当前状态 |
| `CHANGELOG.md` | 修改 | 新增 v0.8.7 记录 warning 返回策略调整 |
| `docs/AGENT_LESSONS.md` | 修改 | 补充 PostToolUse warning 展示排障经验 |

**Copilot 接棒须知**：
- 当前 hook 已不再把 systemMessage 作为 UI warning 的主要实现路径；若后续继续做提醒型 hooks，优先区分上下文注入与用户侧 warning 两类目标。
- 本轮真实日志已出现 result=warning，说明宿主已接收到非阻断 warning 路径；下一步应让用户在真实聊天中复测卡片展示稳定性。

**未完成项**：
- [ ] 在真实 Copilot 会话中再次编辑 docs 或 Copilot 自定义文件，观察 Warning from Post-ToolUse hook 是否比之前更稳定显示
- [ ] 若 UI 仍不稳定，继续查 GitHub Copilot Chat Hooks 输出面板与版本差异，确认是否属于宿主预览行为限制









---

### [2026-03-23 09:55] · GitHub Copilot · 扩展 PostToolUse 提醒粒度

**摘要**：继续推进第三阶段，扩展 `PostToolUse` 提醒分类，新增 Copilot 自定义能力文件的收口提醒，并明确日志优先于 UI warning。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `scripts/copilot_post_edit_reminder.ps1` | 修改 | 扩展提醒规则，新增 Copilot 自定义能力文件场景，并细化 ETL / SQL / docs 提示文本 |
| `docs/misc/superpowers内化会议纪要.md` | 修改 | 记录 `PostToolUse` 第一轮扩展范围，并明确日志为执行真值 |
| `CHANGELOG.md` | 修改 | 记录 v0.8.6 PostToolUse 提醒粒度扩展 |

**Copilot 接棒须知**：
- 当前第三阶段已经证明 `PostToolUse` hook 能在真实宿主里运行；后续扩展仍应优先选择“可日志验证”的提醒型逻辑，不把 UI warning 是否显示当成唯一验收标准。
- 下一步若继续推进，优先考虑 `Stop` 收口提醒试点，而不是直接进入 `PreToolUse` 阻断型逻辑。

**未完成项**：
- [ ] 在真实 Copilot 会话中验证 Copilot 自定义能力文件修改时是否会命中新的 `copilot-customization` 提醒
- [ ] 继续决定第三阶段下一步是扩 `PostToolUse` 细粒度规则，还是新增 `Stop` 收口提醒
- [ ] 视实际误报情况继续收敛正则匹配和提示文案











## 模板（新记录请按此格式）

```markdown
---

### [YYYY-MM-DD HH:MM] · <Claude Code | GitHub Copilot> · <操作类型>

**摘要**：<一句话描述做了什么>

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `path/to/file` | 新增/修改/删除 | 具体说明 |

**影响范围**：<受影响的功能/表/ETL步骤/文档>

**Copilot 接棒须知**：
- <注意事项，例如：某文件与某代码需保持同步>
- <风险点或需要人工确认的口径>

**未完成项**：
- [ ] <TODO>
```
