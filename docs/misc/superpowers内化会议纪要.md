# superpowers 内化会议纪要

> 文档定位：记录“将 obra/superpowers 的方法论内化到 hefang_dw 的 GitHub Copilot 环境中”的讨论结论、阶段方案、边界约束与后续待决事项。
>
> 维护方式：后续每轮相关讨论结束后，优先更新本文件，再决定是否进入实际落地。
>
> 当前状态：第三阶段两个最小提醒型 hook 试点已按“逻辑正常执行”完成本轮验收，后续不再纠结 UI 细节；当前推进重心切回第二阶段 custom agents 的可发现性与描述收敛，整体仍以低阻断、可回退、可审计为主。

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

### 5.1 规划型 skill

- 暂定名称：`planning-hefang` 或 `需求澄清与实施规划`
- 推荐原语：skill
- 推荐位置：`.github/skills/planning-hefang/SKILL.md`
- 触发语建议：
   - “帮我规划”
   - “先别动手，先拆方案”
   - “给我一个实施计划”
   - “这个需求怎么做”
- 适用场景：
   - 新增 ETL 模块
   - 调整字段映射
   - 文档审计或结构审计前的方案拆解
   - 进入 CRM、补数、对账、同步类复杂任务前
- 输入：
   - 用户目标
   - 涉及模块或文件范围
   - 是否允许修改代码
   - 是否已有证据材料
- 输出：
   - 目标与边界
   - 前置证据缺口
   - 拟执行步骤
   - 风险与待确认项
   - 是否建议进入实施
- 执行流程建议：
   1. 先复述目标与限制
   2. 明确涉及代码、文档、数据库还是外部材料
   3. 列出缺失证据与必须先确认的事项
   4. 给出 3 到 7 步实施计划
   5. 明确哪些步骤需要人工确认
- 不应做的事：
   - 不直接修改代码
   - 不替代审计与验证
   - 不在证据缺失时虚构字段、表或业务口径
- 与现有资产的关系：
   - 可复用 [docs/AGENT_HANDOFF.md](../AGENT_HANDOFF.md) 中未完成项作为规划输入
   - 可引用 [docs/TODO_ISSUES.md](../TODO_ISSUES.md) 中的风险项
- 验收标准：
   - 计划中必须显式写出“证据来源”与“待确认项”
   - 不能只给抽象建议，必须落到本仓库文件或脚本层面

### 5.2 ETL 审计型 skill

- 暂定名称：`etl-audit-hefang`
- 推荐原语：skill
- 推荐位置：`.github/skills/etl-audit-hefang/SKILL.md`
- 触发语建议：
   - “审计这个 ETL”
   - “看看增量逻辑有没有问题”
   - “检查字段映射”
   - “核对口径”
- 适用场景：
   - 审计 `etl_*.py`
   - 审计 `run_etl.py`、`run_ods.py` 的调度关系
   - 上线前或重构前做只读检查
- 输入：
   - 模块名、文件名或层级范围，例如 `dws_sales`、`ods`、`全部 ETL`
   - 用户特别关注点，例如“增量逻辑”或“字段映射”
- 输出：
   - 分级发现清单：`CRITICAL / WARNING / INFO`
   - 文件路径与行号
   - 修复建议
   - 是否建议进入 `/quality-check` 或文档同步
- 核心检查项：
   - 字段映射是否与现有字典一致
   - 业务口径是否越权修改
   - 增量水位、时间字段、幂等性是否合理
   - 是否存在证据链缺口
   - 是否遗漏相关文档同步点
- 执行方式建议：
   - 第一版可以参考现有 Claude 侧 [etl-audit skill](../../.claude/skills/etl-audit/SKILL.md) 的结构
   - 如后续需要上下文隔离，再升级为 skill + custom agent 联动
- 不应做的事：
   - 不直接修改代码
   - 不在无证据时裁定业务口径对错
   - 不跳过风险分级
- 验收标准：
   - 输出必须先给发现，再给摘要
   - 每条发现必须能定位到真实文件

### 5.3 文档对齐型 skill

- 暂定名称：`doc-sync-hefang`
- 推荐原语：skill
- 推荐位置：`.github/skills/doc-sync-hefang/SKILL.md`
- 触发语建议：
   - “同步文档”
   - “检查文档和代码是否一致”
   - “跑一下 doc-sync”
- 适用场景：
   - ETL、SQL、表结构、README、运行方式变更后
   - 定期例行文档健康检查
- 输入：
   - 变更范围
   - 是否仅审计、还是允许自动修复
- 输出：
   - 差异清单
   - 风险等级
   - 受影响文档列表
   - 如用户确认，给出修复结果与复扫结果
- 推荐执行流程：
   1. 运行 [scripts/check_doc_sync.py](../../scripts/check_doc_sync.py)
   2. 解析 `MISSING / OUTDATED / OK`
   3. 根据 [.github/copilot-instructions.md](../../.github/copilot-instructions.md) 中同步矩阵识别必须更新的文档
   4. 若用户确认，再执行文档修订
   5. 复跑审计并落盘结果
- 第一阶段边界：
   - 第一阶段建议仍保留“先审计、再确认修复”的交互，不做全自动文档改写
   - 如涉及业务口径，必须停下来请用户确认
- 与现有资产的关系：
   - 可直接继承现有 Claude 侧 [doc-sync skill](../../.claude/skills/doc-sync/SKILL.md) 的主流程
   - 把本次新增的 [docs/misc/superpowers内化会议纪要.md](superpowers%E5%86%85%E5%8C%96%E4%BC%9A%E8%AE%AE%E7%BA%AA%E8%A6%81.md) 也纳入同步清单
- 验收标准：
   - 必须能区分“仅扫描”和“允许修复”两种模式
   - 修复后必须再次验证

### 5.4 收口检查型 skill

- 暂定名称：`completion-check-hefang`
- 推荐原语：skill，优先于 prompt
- 推荐位置：`.github/skills/completion-check-hefang/SKILL.md`
- 触发语建议：
   - “帮我收口”
   - “检查这次任务能不能结束”
   - “做一下完工检查”
- 适用场景：
   - 任意一组有意义的代码或文档变更完成后
   - 准备交付、准备合并、准备结束会话前
- 输入：
   - 本轮变更范围
   - 本轮是否执行过测试或验证
   - 是否存在未确认假设
- 输出：
   - 已完成项清单
   - 未验证风险清单
   - 建议补跑的验证项
   - 是否需要 handoff
   - 是否需要 lesson
- 推荐执行流程：
   1. 汇总本轮变更文件
   2. 检查是否跑过最小验证
   3. 检查是否涉及文档同步
   4. 检查是否应写入 [docs/AGENT_HANDOFF.md](../AGENT_HANDOFF.md)
   5. 检查是否应写入 [docs/AGENT_LESSONS.md](../AGENT_LESSONS.md)
   6. 输出“可结束 / 不建议结束”判断
- 第一阶段不应做的事：
   - 不强制自动执行所有验证命令
   - 不替代 handoff skill 本身
- 验收标准：
   - 输出必须是“结论 + 缺口 + 下一步”结构
   - 不能只说“已完成”，必须说明是否真的验证过

### 5.5 Python ETL 专用 instructions

- 暂定名称：`python-etl.instructions.md`
- 推荐原语：file instructions
- 推荐位置：`.github/instructions/python-etl.instructions.md`
- `applyTo` 建议：`etl_*.py`, `run_etl.py`, `run_ods.py`, `scheduled_etl.py`, `test_etl_automation.py`
- 作用：
   - 将 ETL 领域特定规则从全局 instructions 中拆出，减少常驻上下文负担
   - 让与 ETL 无关的对话不必加载这些细节
- 应包含的规则：
   - 先确认字段来源与目标表
   - 审核增量逻辑、水位字段与幂等性
   - 涉及业务口径时必须区分“代码事实”和“待确认业务规则”
   - 涉及 ETL 变更时要显式考虑文档同步与最小验证
   - 禁止默认数据库中存在未证实的 CRM 表
- 不建议放入该文件的内容：
   - handoff 的具体命令模板
   - 通用中文输出偏好
   - 与 ETL 无关的文档、前端或 Git 规范
- 验收标准：
   - 能显著减少 [.github/copilot-instructions.md](../../.github/copilot-instructions.md) 的 ETL 专用细节
   - 不使用 `applyTo: "**"` 这类高负载写法

### 5.6 第一阶段落地顺序建议

推荐顺序：

1. 先落 `python-etl.instructions.md`
2. 再落 `planning-hefang` skill
3. 再落 `etl-audit-hefang` skill
4. 再落 `doc-sync-hefang` skill
5. 最后落 `completion-check-hefang` skill

排序理由：

- 先拆 instructions，先减轻全局规则负担
- 再做规划与审计，先提升“开始前”和“中途”的正确性
- 最后再做收口，避免先设计一个没有上游输入的结束动作

### 5.7 第一阶段统一规格模板

为避免后续每个能力风格漂移，第一阶段建议所有新能力统一包含以下字段或章节：

- 名称
- description
- use when / 触发语
- 输入参数
- 执行步骤
- 输出格式
- 不应做的事
- 依赖脚本或文档
- 验收标准

说明：若采用 YAML frontmatter，`description` 里必须显式写出触发关键词，避免技能无法被 Copilot 发现。

### 5.8 `.github` 目标目录结构

第一阶段建议将 GitHub Copilot 自定义能力落为以下结构：

```text
.github/
├── copilot-instructions.md                 # 全局常驻规则，仅保留所有任务都必须遵守的约束
├── instructions/
│   └── python-etl.instructions.md         # ETL / 调度 / 测试相关文件专用规则
├── skills/
│   ├── planning-hefang/
│   │   └── SKILL.md
│   ├── etl-audit-hefang/
│   │   └── SKILL.md
│   ├── doc-sync-hefang/
│   │   └── SKILL.md
│   └── completion-check-hefang/
│       └── SKILL.md
├── prompts/                                # 第一阶段预留，暂不创建实体文件
├── agents/                                 # 第二阶段再启用
└── hooks/                                  # 第三阶段再启用
```

设计说明：

- 第一阶段不建议提前创建空的 `agents/` 或 `hooks/` 实体文件，但纪要中保留目标结构，便于后续按阶段扩展。
- `prompts/` 第一阶段可只作为预留目录概念，不急于创建，因为当前 4 个高优先能力更适合 skill。
- 若后续出现“单次问答模板”类需求，例如“更新会议纪要”“生成审计摘要”，再补 `prompts/` 下的具体文件。

### 5.9 `copilot-instructions.md` 与 `python-etl.instructions.md` 的拆分边界

#### 应继续保留在 `copilot-instructions.md` 的内容

- 接棒协议与强制先读 [docs/AGENT_HANDOFF.md](../AGENT_HANDOFF.md)
- 语言偏好：统一使用简体中文
- 开发环境现实约束
- 事实来源优先级与禁止臆造原则
- 文档同步的总原则、同步矩阵与检查清单
- 业务口径变更的责任边界
- 审计闭环与文档校验产物要求
- 仅允许修改 `README.md` 与 `docs/*.md` 这一条当前文档讨论场景下的保护规则

#### 应迁移到 `python-etl.instructions.md` 的内容

- 处理 `etl_*.py`、`run_etl.py`、`run_ods.py`、`scheduled_etl.py`、`test_etl_automation.py` 时的专用审计要求
- 增量水位、时间字段、幂等性、回填、重跑等 ETL 结构性检查点
- 字段来源、目标表、建表 SQL、验证脚本之间的核对要求
- 涉及 ETL 代码修改时，应补看的文档与最小验证动作
- 禁止默认本地 MySQL 已落地未证实 CRM 表这一 ETL 场景专用提醒

#### 当前不建议迁移的内容

- handoff / lesson 的命令行模板
- 所有与文档治理直接相关的总清单
- 与 Copilot 全局行为有关的语言、输出、协作协议
- 与 ETL 无关的通用 Git、沟通或 Markdown 规范

#### 拆分后的预期效果

- 全局 instructions 变短，减少与 ETL 无关任务的上下文负担
- ETL 相关对话命中专用规则时更集中，不需要从总指令里筛出领域细节
- 后续若增加 SQL 专用 instructions 或 docs 专用 instructions，也能沿同样方式继续拆分

### 5.10 第一阶段实施门槛

在正式创建 `.github/instructions/` 与 `.github/skills/` 之前，先满足以下门槛：

1. 会议纪要中 `.github` 目标结构已定稿
2. `copilot-instructions.md` 的保留内容与迁移内容边界已定稿
3. 第一阶段 4 个 skill 的命名不再频繁变动
4. 确认第一阶段先不依赖 MCP 可见性，也不依赖 hooks 强制触发

说明：满足以上门槛后，再进入实际文件创建阶段，可以显著降低返工。

### 5.11 第一阶段实施进展

#### 已完成

1. 已创建 [`.github/instructions/python-etl.instructions.md`](../../.github/instructions/python-etl.instructions.md)
2. 已在 [`.github/copilot-instructions.md`](../../.github/copilot-instructions.md) 中明确“全局常驻规则”和“ETL 专用 instructions”的分层关系
3. 已创建 [`.github/skills/planning-hefang/SKILL.md`](../../.github/skills/planning-hefang/SKILL.md)
4. 已创建 [`.github/skills/etl-audit-hefang/SKILL.md`](../../.github/skills/etl-audit-hefang/SKILL.md)
5. 已创建 [`.github/skills/doc-sync-hefang/SKILL.md`](../../.github/skills/doc-sync-hefang/SKILL.md)
6. 已创建 [`.github/skills/completion-check-hefang/SKILL.md`](../../.github/skills/completion-check-hefang/SKILL.md)

#### 当前效果

- Copilot 在处理 `etl_*.py`、`run_etl.py`、`run_ods.py`、`scheduled_etl.py`、`test_etl_automation.py` 时，可自动加载更聚焦的 ETL 规则。
- 全局总指令不再承担 ETL 领域的全部细节扩展职责，后续滚动推进时更适合继续往 `.github/instructions/` 和 `.github/skills/` 分层。
- Copilot 已具备一个明确的“先规划、后实施”技能入口，能把复杂任务先收敛成可执行步骤，而不是直接进入修改。
- Copilot 已具备一个 ETL 只读审计入口，后续讨论和实现前可以先用 skill 输出分级发现，再决定是否进入修改。
- Copilot 已具备文档对齐与收口检查入口，第一阶段“规划 → 审计 → 文档对齐 → 收口”的基础闭环已经具备。

#### 下一滚动项

1. 进入第一阶段验收，验证 4 个 skill 与 1 个 instructions 在 VS Code Copilot 中的可发现性与触发效果
2. 如触发效果正常，再考虑是否开始第二阶段的 custom agents 设计
3. 如触发效果不稳定，优先修正 `description`、命名和目录布局，而不是继续新增能力

### 5.12 第一阶段验收结果

#### A. 静态验收结果

结论：通过。

已确认项：

1. `.github/instructions/` 与 `.github/skills/` 目录存在，位置符合 VS Code Copilot 官方约定。
2. `planning-hefang`、`etl-audit-hefang`、`doc-sync-hefang`、`completion-check-hefang` 的目录名与 `SKILL.md` 中 `name` 一致。
3. 4 个 skill 均包含 `description`，且描述中包含明确触发场景与能力说明，满足技能发现的基础条件。
4. `python-etl.instructions.md` 已位于 `.github/instructions/` 下，且包含 `description` 与 `applyTo`。
5. 当前仓库未发现显式关闭 `chat.includeApplyingInstructions`、`chat.instructionsFilesLocations`、`chat.agentSkillsLocations`、`chat.useAgentsMdFile` 等设置的工作区配置文件。

#### B. 保留风险

1. 当前只能完成仓库内静态验收，无法仅靠文件结构确认 VS Code Copilot 运行时是否真正加载这些能力。
2. `python-etl.instructions.md` 的 `applyTo` 仍需通过实际编辑目标文件验证是否命中。
3. 4 个 skill 是否会被 Copilot 自动发现，仍取决于运行时技能发现机制与 `description` 匹配效果。

#### C. 运行时人工验收步骤

建议按以下顺序人工验收：

1. 打开一个 ETL 文件，例如 `etl_dws_sales.py`，在 Chat 中发起与 ETL 修改相关的请求。
2. 检查回复中的 References 区域，确认是否出现 `.github/instructions/python-etl.instructions.md`。
3. 在 Chat 中右键打开 Diagnostics，确认 `.github/instructions/python-etl.instructions.md` 已被发现且无格式错误。
4. 输入 `/`，检查 4 个 skill 是否出现在技能列表中：
   - `planning-hefang`
   - `etl-audit-hefang`
   - `doc-sync-hefang`
   - `completion-check-hefang`
5. 分别用自然语言触发一次：
   - “先帮我规划 dws_sales 增量逻辑调整”
   - “审计这个 ETL 的字段映射和增量逻辑”
   - “检查文档和代码是否一致”
   - “帮我做一下完工检查”
6. 若未自动触发，再分别用 `/planning-hefang`、`/etl-audit-hefang`、`/doc-sync-hefang`、`/completion-check-hefang` 手动调用，确认 slash command 可用。

#### D. 当前判定

- 静态结构：通过
- 运行时手动调用：通过
- 运行时自动发现：未做完整细测，保留为观察项
- 阶段结论：按用户本轮判定，第一阶段验收先视为通过，继续进入第二阶段角色分化

#### E. 当前说明

1. 本轮已确认 4 个 hefang skill 可在 `/` 列表中被发现，但同时存在 Claude 侧无后缀同名能力，因此列表中会并存两套近似入口。
2. `python-etl.instructions.md` 未继续做完整的运行时细测；当前没有发现 frontmatter 或目录结构层面的阻断问题，但“自然语言自动命中稳定性”仍不应表述为已充分验证。
3. 第一阶段验收结论以“用户决定先收口并继续推进”为准，后续若发现自动触发稳定性问题，再回到 `description`、命名和目录布局层面修正。

### 5.13 第二阶段建议先做的 5 个角色

第二阶段目标从“能力可发现”切到“角色隔离与上下文减载”。当前建议先落以下 5 个 custom agents：

1. Planner
   - 处理范围澄清、风险拆解、实施顺序规划。
2. ETL Auditor
   - 只读审计 ETL、调度和测试，输出分级发现。
3. Doc Syncer
   - 负责文档对齐、差异归类和修订执行。
4. DB Inspector
   - 负责结构核对、快照比对与数据库证据梳理。
5. Reviewer
   - 负责交付前 review、风险复查与收口判断。

### 5.14 第二阶段落地原则

1. 第二阶段优先落 `.github/agents/*.agent.md` 骨架，不急于同时引入 hooks。
2. 每个 agent 只承担单一职责，tools 取最小集合，避免再次做成“大而全 Copilot”。
3. `description` 必须显式包含触发场景与关键词，保证 agent picker 和子代理发现面足够明确。
4. 当前仍不把设计建立在 MCP 在本聊天中一定可见的假设上；DB Inspector 需允许“快照/文档证据优先，MCP 可见时再增强”。

### 5.15 第二阶段实施进展

#### 已完成

1. 已创建 `.github/agents/planner-hefang.agent.md`
2. 已创建 `.github/agents/etl-auditor-hefang.agent.md`
3. 已创建 `.github/agents/doc-syncer-hefang.agent.md`
4. 已创建 `.github/agents/db-inspector-hefang.agent.md`
5. 已创建 `.github/agents/reviewer-hefang.agent.md`

#### 当前效果

- 第一阶段的 `instructions + skills` 继续保留为工作流入口。
- 第二阶段开始补齐“按角色隔离上下文”的 custom agents 层，后续可根据任务类型直接选择 Planner、Auditor、Doc Syncer、DB Inspector、Reviewer。
- 当前第二阶段只落代理骨架与职责边界，不同时引入 hooks、prompt 扩张或强制自动化。

#### 下一滚动项

1. 在 VS Code Copilot 的 agent picker 中验证 5 个 agent 的可见性与描述是否易懂。
2. 按真实使用反馈收敛每个 agent 的 tools 集合，避免授权过宽。
3. 若角色分化效果稳定，再决定是否进入第三阶段的 hooks、prompt 或 MCP 增强。

#### 当前补充

- 当前 5 个 agent 已进入第二轮 description 收敛，重点不是新增职责，而是补齐更贴近真实提问方式的触发词，降低 agent picker 与自然语言发现的不稳定性。
- 当前第二阶段的近期验收目标应从“agent 文件是否存在”切到“agent picker 是否容易选中、description 是否能让用户一眼看懂用途”。

### 5.16 运行时验收 prompt

- 已创建 [`.github/prompts/runtime-acceptance-hefang.prompt.md`](../../.github/prompts/runtime-acceptance-hefang.prompt.md)
- 定位：把“运行时验收协助模式”固化为可重复调用的 prompt，减少后续每轮重复口头描述 References、Diagnostics、`/` 列表和 agent picker 的检查步骤。
- 边界：该 prompt 只负责组织验收步骤与收集观察结果，不把未观测的 UI 结果包装成已验证通过。
- 当前用途：可用于第一阶段 skills / instructions 的复验，也可用于第二阶段 agents 的可见性验收。

### 5.17 第三阶段 hooks 设计稿

当前结论：第三阶段先写设计稿，不立即启用 `.github/hooks/*.json`。

#### 设计目标

1. 将高频提醒从“靠记忆”升级为“靠运行时事件稳定触发”。
2. 保持 hooks 小而可审计，不把复杂业务判断直接塞进 hook 脚本。
3. 先做提醒型 hooks，再做守门型 hooks，最后再考虑自动执行型 hooks。

#### 分层推进建议

##### Stage 3A：提醒型 hooks

优先落非阻断、低风险、短耗时的提醒型 hooks：

1. `PostToolUse`
   - 当编辑 `etl_*.py`、`run_etl.py`、`run_ods.py`、`scheduled_etl.py`、`SQL/*.sql` 时，提示检查 doc-sync、handoff、lesson。
2. `PostToolUse`
   - 当编辑 `docs/*.md` 或 `README.md` 时，提示检查版本记录、交接记录与是否需要复扫文档审计。
3. `Stop`
   - 会话结束前提醒检查 `docs/AGENT_HANDOFF.md`、`docs/AGENT_LESSONS.md` 和未完成项是否已处理。

##### Stage 3B：守门型 hooks

在提醒型 hooks 稳定后，再考虑增加 ask/deny 类守门逻辑：

1. `PreToolUse`
   - 修改 `config.py` 中业务常量、删除关键测试断言、触发高风险 git 命令时，要求额外确认。
2. `PreToolUse`
   - 当用户请求“审计文档对齐”且涉及数据库结构事实时，提醒先确认是否需要生成快照。

##### Stage 3C：自动执行型 hooks

只在前两层收益明确后再考虑：

1. `PostToolUse`
   - 在特定文档修订后自动提示刷新 `reports/docs_code_alignment.json`。
2. `Stop`
   - 在检测到本轮有有意义变更但未写 handoff 时，输出结构化阻断或强提醒。

#### 当前不建议立即启用的 hooks

1. 长时间运行的 hooks
   - 例如直接在 hook 内跑全量质检、全量快照、全量文档审计。
2. 高误报阻断 hooks
   - 例如没有充分证据时就禁止普通编辑，容易造成日常交互过重。
3. 依赖当前会话一定可见 MCP 的 hooks
   - 当前仍不能把设计建立在 MCP 已挂载的前提上。

#### 当前推荐的第一批 hook 候选

1. `post-edit-reminder-hefang`
   - 类型：提醒型
   - 事件：`PostToolUse`
   - 作用：编辑 ETL / SQL / docs 后提示 doc-sync、handoff、lesson
2. `session-close-reminder-hefang`
   - 类型：提醒型
   - 事件：`Stop`
   - 作用：会话结束前提醒检查交接、经验和未完成项

#### 当前边界

- 本轮仅形成第三阶段设计稿，尚未创建 `.github/hooks/*.json`。
- 若后续启动 hooks，优先先做提醒型，再做守门型，不直接从阻断型开始。

### 5.19 第三阶段首个 hook 试点

#### 已完成

1. 已创建 [`.github/hooks/post-edit-reminder-hefang.json`](../../.github/hooks/post-edit-reminder-hefang.json)
2. 已创建 [scripts/copilot_post_edit_reminder.ps1](../../scripts/copilot_post_edit_reminder.ps1)
3. 已创建 [scripts/copilot_session_close_reminder.ps1](../../scripts/copilot_session_close_reminder.ps1)

#### 当前行为

- 当前试点只覆盖 `PostToolUse`。
- 当工具调用内容中命中 ETL、SQL、docs、`README.md` 或 Copilot 自定义能力文件编辑痕迹时，输出一条非阻断提醒。
- 当前提醒只做收口提示，不做 ask/deny，不阻断正常操作。

#### 当前扩展

- 已将提醒粒度从“ETL / SQL / docs”扩到“Copilot 自定义能力 / ETL / SQL / docs”四类。
- 对 ETL 提醒增加了“最小验证”提示，对 SQL 提醒补上 `doc-sync`，对 docs 提醒补上“必要复扫”提示。
- 对 `.github/agents`、`.github/skills`、`.github/prompts`、`.github/hooks`、`.github/instructions` 与 `copilot-instructions.md` 的修改，单独提示检查运行时验收、会议纪要、handoff 与 lesson。

#### 当前边界

- 当前试点不依赖 MCP，也不触发自动执行的 doc-sync、handoff 或 lesson。
- 当前试点是否被 VS Code Copilot 运行时真正加载，仍需后续在真实会话中观察。
- 当前脚本只做轻量文本匹配，避免把复杂业务判断直接塞进 hook。
- 当前 UI 对 `Warning from Post-ToolUse hook` 的展示不稳定，因此是否执行成功应优先以日志为准，而不是仅以聊天卡片是否出现 warning 为准。
- 2026-03-23 本轮已按用户确认收口为“Stop 与 PostToolUse 均按逻辑正常执行”；后续若继续调整 warning 展示，仅作为体验优化，不再作为阶段阻断项。

### 5.21 第二个提醒型 hook 试点

#### 已完成

1. 已在 [`.github/hooks/post-edit-reminder-hefang.json`](../../.github/hooks/post-edit-reminder-hefang.json) 中新增 `Stop` 事件配置
2. 已创建 [scripts/copilot_session_close_reminder.ps1](../../scripts/copilot_session_close_reminder.ps1)

#### 当前行为

- 当前 `Stop` 试点不直接扫描整个工作树，也不尝试判断“是否必须阻断结束”。
- 当前脚本复用 [logs/copilot_post_edit_reminder.log](../../logs/copilot_post_edit_reminder.log) 里的最近命中规则，作为“本轮确实发生过哪些编辑类型”的轻量证据源。
- 当最近窗口内出现 `copilot-customization`、`etl`、`sql` 或各类 docs 命中时，在结束前输出一条非阻断收口提醒，提示检查验证、文档一致性、handoff、lesson 与未完成项。

#### 当前边界

- 当前 `Stop` 试点仍然只做提醒，不做 ask/deny 或强阻断。
- 当前提醒窗口和去重策略都是经验值，后续应根据真实误报情况继续收敛。
- 当前实现优先解决“不能被历史脏工作树带偏”的问题，因此证据来源选为最近 `PostToolUse` 日志，而不是 `git diff`。
- 2026-03-23 真实 Copilot 会话已确认 `Warning from Stop hook` 能显示；但 PowerShell 非零 stderr 路径上的中文文案在宿主 UI 中出现乱码，因此当前 Stop 提示文案优先收敛为 ASCII，先保证用户可读性与稳定识别。
- 为继续压缩 warning 卡片中的宿主噪音，当前已把 `Stop` 事件的顶层命令切到 `cmd /d /c` 包装层，避免直接由 `pwsh` 顶层命令向宿主抛出过多 PowerShell 错误格式信息；是否进一步改善，仍需真实 UI 复测。
- 2026-03-23 继续复测后确认，仅增加 `cmd` 包装层仍不足以消除宿主 UI 里的 `NativeCommandError` 风格噪音；当前已把 `Stop` 实现切换为 `python` 脚本，并改走“标准输出 + 非零退出码”链路，目标是继续降低宿主对 PowerShell 错误元信息的包装。

### 5.18 高复用 prompt 补齐

- 已创建 [`.github/prompts/meeting-minutes-hefang.prompt.md`](../../.github/prompts/meeting-minutes-hefang.prompt.md)
- 定位：将“更新 superpowers / Copilot 能力设计会议纪要”固化为单任务 prompt，减少后续每轮手动整理章节与版本记录的成本。
- 边界：只负责将讨论结论落到会议纪要，不替代 handoff、doc-sync 或完工检查。
- 当前用途：适用于更新当前状态、本轮新增结论、阶段进展、待决事项和版本记录。

### 5.20 阶段收口检查 prompt

- 已创建 [`.github/prompts/stage-close-hefang.prompt.md`](../../.github/prompts/stage-close-hefang.prompt.md)
- 定位：给“阶段收口检查”提供 prompt 入口，与 [`.github/skills/completion-check-hefang/SKILL.md`](../../.github/skills/completion-check-hefang/SKILL.md) 形成 prompt/skill 双入口。
- 边界：该 prompt 聚焦单次收口检查，不替代 skill 在复杂场景下的工作流指导。
- 当前用途：适用于结束前快速检查已完成项、未验证缺口与 handoff/lesson 需求。

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

- 需在 VS Code Copilot 的 agent picker 中验证 5 个 `.github/agents/*.agent.md` 是否可发现、命名是否清晰。
- 若第二阶段 agent 的可见性或触发不稳定，需决定是优先优化 `description`，还是为高频任务补充 prompt 兜底。
- 是否在第二阶段稳定后引入第三阶段 hooks 与更深的 MCP 联动，尚未最终决定。

---

## 10. 本轮新增结论（2026-03-20）

- 已确认采用“三阶段推进”作为后续 superpowers 内化主路线。
- 已确认需要单独维护一份会议纪要式文档，作为后续讨论和决策的持续沉淀载体。
- 当前文档为首次建档，尚未进入实际能力落地阶段。
- 已完成第一阶段 5 个能力的逐项规格化，明确了每项能力的推荐原语、触发语、输入输出、执行边界与落地顺序。
- 第一阶段默认优先使用 `skill + file instructions` 组合，不急于引入复杂 hook 或大而全 custom agent。
- 已定稿第一阶段的 `.github` 目标目录结构，并明确 `copilot-instructions.md` 与 `python-etl.instructions.md` 的拆分边界。
- 当前已具备进入第一阶段实际文件设计的前置条件，但尚未开始创建 `.github/instructions/` 与 `.github/skills/` 实体文件。
- 第一阶段已正式启动，首个落地点为 [`.github/instructions/python-etl.instructions.md`](../../.github/instructions/python-etl.instructions.md)。
- 第一阶段已完成前两项：`python-etl.instructions.md` 与 `planning-hefang` skill。
- 第一阶段已完成前三项：`python-etl.instructions.md`、`planning-hefang`、`etl-audit-hefang`。
- 第一阶段第一批骨架已齐：`python-etl.instructions.md`、`planning-hefang`、`etl-audit-hefang`、`doc-sync-hefang`、`completion-check-hefang`。
- 下一步从“继续加功能”切换到“做第一阶段验收”，先验证触发与可用性，再决定是否进入第二阶段。
- 第一阶段静态验收已通过；当前剩余工作是完成 VS Code Copilot 运行时的可发现性与触发验收。
- 本轮按用户判定先结束第一阶段运行时细测，并将阶段结论推进为“可继续进入第二阶段”，但自然语言自动触发稳定性仍保留为观察项。
- 第二阶段已正式启动，首批目标为 5 个 custom agents：Planner、ETL Auditor、Doc Syncer、DB Inspector、Reviewer。
- 当前第二阶段仅落角色骨架与工具边界，不同步引入 hooks 或更深的自动化约束。
- 已新增 `runtime-acceptance-hefang` prompt，用于把运行时验收流程沉淀为可复用入口。
- 第三阶段已先形成 hooks 设计稿，明确采用“提醒型 → 守门型 → 自动执行型”的分层推进，而不是立即启用 hooks。
- 已新增 `meeting-minutes-hefang` prompt，用于把会议纪要更新动作沉淀为单任务入口。
- 第三阶段已落第一个最小提醒型 hook 试点，但当前仍只做非阻断提醒，是否被宿主稳定加载仍待运行时观察。
- 第三阶段已确认 `PostToolUse` 在真实宿主里能够执行；当前未收口的是 UI 展示稳定性，而不是 hook 本身是否运行。
- 第三阶段已完成第一轮 `PostToolUse` 扩展，开始覆盖 Copilot 自定义能力文件修改后的提醒场景。
- 第三阶段已继续细分 docs 提醒规则，当前将文档类拆为“会议纪要类 / 数据字典类 / 协作文治理类 / 运行文档类 / README 类 / 其他 docs”六档，优先按后续动作差异而不是按文件名表面相似度拆分。
- 已确认 `systemMessage` 虽是合法输出字段，但对 `PostToolUse` 命中场景更可能被宿主静默消费或不稳定展示，不能把它等同于显式 warning 卡片。
- 为提高 UI 可见性，当前试点已将命中提醒从“退出码 0 + JSON `systemMessage`”切换为“非阻断 warning 退出码 + stderr 文案”；后续运行时验收重点改为观察 warning 卡片是否更稳定出现。
- 已新增 `stage-close-hefang` prompt，与 `completion-check-hefang` skill 形成 prompt/skill 双入口。
- 第三阶段已新增第二个提醒型 hook 试点 `Stop`，用于在结束前基于最近编辑命中类型给出收口提醒，不再依赖工作树脏状态做粗粒度判断。
- 已确认 Stop hook 在真实 Copilot UI 中可以展示 warning；当前剩余问题不再是“会不会显示”，而是“提示文案是否稳定可读”。
- 当前 Stop hook 的下一个观察点已从“是否可见”切到“PowerShell 错误元信息能否继续收敛”。
- 当前最新观察点已进一步收敛为：切换到 Python 实现后，宿主 warning 卡片是否还能显示，以及额外错误元信息是否明显下降。
- 当前 hooks 试点已按“逻辑执行正确”完成本轮验收，后续观察重点从 warning UI 细节切回第二阶段 agents 的发现面与可理解性。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.1 | 2026-03-20 | 首次建档，记录 superpowers 内化讨论结论、三阶段方案与后续更新规则 |
| v0.2 | 2026-03-20 | 细化第一阶段 5 个能力的可实施规格，明确触发语、输入输出、边界与落地顺序 |
| v0.3 | 2026-03-20 | 定稿 .github 目标目录结构，并明确全局 instructions 与 Python ETL instructions 的拆分边界 |
| v0.4 | 2026-03-20 | 启动第一阶段实施，新增 python-etl.instructions.md 并记录滚动推进状态 |
| v0.5 | 2026-03-20 | 新增 planning-hefang skill 骨架，并更新第一阶段实施进展 |
| v0.6 | 2026-03-20 | 新增 etl-audit-hefang skill 骨架，并更新第一阶段实施进展 |
| v0.7 | 2026-03-20 | 新增 doc-sync-hefang 与 completion-check-hefang skill 骨架，补齐第一阶段基础闭环 |
| v0.8 | 2026-03-20 | 完成第一阶段静态验收，并补充运行时人工验收步骤与判定标准 |
| v0.9 | 2026-03-20 | 按用户判定收口第一阶段验收，并启动第二阶段 custom agents 骨架 |
| v0.10 | 2026-03-20 | 新增 runtime-acceptance-hefang prompt，沉淀运行时验收协助模式 |
| v0.11 | 2026-03-20 | 新增第三阶段 hooks 设计稿，并补齐 meeting-minutes-hefang prompt |
| v0.12 | 2026-03-20 | 落首个 PostToolUse 提醒型 hook 试点，并新增阶段收口检查 prompt |
| v0.13 | 2026-03-23 | 扩展 PostToolUse 提醒粒度，新增 Copilot 自定义能力文件场景并明确以日志作为执行真值 |
| v0.14 | 2026-03-23 | 明确 PostToolUse 的 `systemMessage` 不等同于稳定 UI warning，并将提醒策略切换为非阻断 warning 退出码 |
| v0.15 | 2026-03-23 | 细分 docs 提醒规则，新增会议纪要类、运行文档类与 README 类 PostToolUse 提示 |
| v0.16 | 2026-03-23 | 继续细分 docs 提醒规则，新增数据字典类与协作文治理类 PostToolUse 提示 |
| v0.17 | 2026-03-23 | 新增 Stop 收口提醒试点，基于最近 PostToolUse 命中日志在结束前输出非阻断提醒 |
| v0.18 | 2026-03-23 | 确认 hooks 按逻辑正常执行，并将推进重点切回第二阶段 agents 的 description 收敛与可发现性验收 |
