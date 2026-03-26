# AGENT_LESSONS.md — Agent 经验台帐

> 用途：沉淀可复用的排障结论、字段语义修正、业务逻辑纠错与查询踩坑，避免同类错误重复发生。
>
> 适用对象：GitHub Copilot / Claude Code / OpenCode 子代理。

---

## 记录规则

以下场景必须记入经验台帐：

- 一次排障后得到可复用结论，例如字段不存在、真实来源表与文档不一致、某命令在本仓库有固定坑。
- 用户明确指出“业务逻辑不对”“字段语义不对”“SQL 口径不对”，且已确认修正方向。
- 同类问题预计会在后续任务中反复出现。

推荐写入命令：

```powershell
python scripts/log_agent_lesson.py \
  --source task \
  --category field-mapping \
  --trigger "Oracle 查询字段报错" \
  --mistake "误以为 M_PRODUCT 存在 NAME_CN" \
  --correction "以 etl_dim_product.py 为准：NAME=product_code，VALUE=product_name" \
  --evidence "etl_dim_product.py#L33" "etl_dim_product.py#L34" \
  --prevention "涉及源表字段时，先对照 ETL 抽取 SQL 或快照后再写查询"
```

若经验是用户业务纠错，应将 `--source` 设为 `user-feedback`。

---

## 经验记录

### [2026-03-24 10:55] · task · mcp/path

**触发场景**：仓库根 `.mcp.json` 已配置 Oracle，但当前 Copilot 会话里始终拿不到 Oracle MCP 工具

**错误假设**：默认认为只要仓库根 `.mcp.json` 存在 `oracle` server，当前 VS Code / Copilot 会话就会自动暴露 Oracle MCP 工具

**修正结论**：当前项目里，Copilot 会话实际注册 MCP 工具时优先看工作区 `.vscode/mcp.json` 与用户级 `mcp.json`；仓库根 `.mcp.json` 不能保证直接暴露为会话工具。2026-03-24 将 Oracle 接入工作区 `.vscode/mcp.json` 后，当前会话已可直接使用 Oracle 查询接口；其中 `mcp_oracle_reqd_query` 稳定可用，而 `mcp_oracle_list_tables` / `mcp_oracle_describe_table` 可能返回空或识别失败。

**证据**：
- .vscode/mcp.json
- .vscode/start_oracle_mcp.ps1
- AGENTS.md

**预防动作**：以后排查“某个 MCP server 配了但会话里看不到工具”时，先确认 VS Code 实际生效的是工作区 `.vscode/mcp.json` 还是用户级 `mcp.json`，再判断是否需要重载窗口、新开聊天，或回退到只读 SQL 查询。

---

### [2026-03-24 09:50] · task · mcp/path

**触发场景**：VS Code 中 io.github.bytebase/dbhub 长时间等待 initialize 后退出码 1

**错误假设**：先把问题当成 DSN 或用户级 mcp.json 参数填写错误

**修正结论**：在 Windows + Node 24.14.0 环境下，@bytebase/dbhub 安装阶段会因 better-sqlite3 原生依赖失败而直接退出；切到本地 Node 22 后可正常启动 MCP server

**证据**：
- .vscode/start_dbhub.ps1; .vscode/mcp.json; docs/AGENT_HANDOFF.md

**预防动作**：以后排查 DBHub 启动失败时，先用 npx -p @bytebase/dbhub dbhub --help 或本地包装脚本验证运行时兼容性，再检查 DSN/inputs。

---

### [2026-03-23 17:46] · task · field-mapping

**触发场景**：dim_channel 自动化测试误判缺少 DS001

**错误假设**：将 O2O_RETAIL_CHANNEL.WING_CODE 直接假设成 DS001 这类店仓编码，并据此在测试中硬编码检查 WING_CODE='DS001'

**修正结论**：dim_channel.WING_CODE 应按 Oracle O2O_RETAIL_CHANNEL 原值理解；2026-03-23 实查 Oracle 与 MySQL 均为 87 条且 WING_CODE 全部非空，但不存在 DS001 这一硬编码值

**证据**：
- etl_dim_channel.py#L27
- test_etl_automation.py#L193
- docs/数据结构与映射手册.md#L196

**预防动作**：涉及字段语义时，先对照现网源表样本与目标表实查结果，再决定测试断言，不要把业务侧 C_STORE.CODE 规则直接外推到其他表字段

---

### [2026-03-23 17:37] · task · business-rule

**触发场景**：Oracle/MySQL 销售对账差异 7%+

**错误假设**：dws_sales 仅按单据头 TOT_AMT_ACTUAL 正负归类，遗漏 TOT_AMT_ACTUAL=0 时需按行级 QTY 正负兜底的标准口径；测试记录数也未使用与 Oracle 相同的渠道过滤

**修正结论**：dws_sales 标准口径应在 TOT_AMT_ACTUAL=0 时按行级 QTY 正负兜底，并确保 Oracle/MySQL 对账使用完全一致的过滤条件与 0.5% 误差阈值

**证据**：
- etl_dws_sales.py#L50
- etl_dws_sales.py#L55
- test_etl_automation.py#L268
- test_etl_automation.py#L309

**预防动作**：遇到销售口径对账异常时，先回查仓库内标准 SQL 口径文档，再检查测试过滤条件是否与 Oracle SQL 完全一致

---

### [2026-03-23 17:06] · task · incremental-logic

**触发场景**：2026-03-23 主链重跑时 dws_inventory 与 ads_health 出现 1213/1205 锁冲突

**错误假设**：将按日覆盖的 delete+insert 拆成跨事务步骤，且在上游失败后仍继续驱动下游 ADS 计算

**修正结论**：dws_inventory 和 ads_health 的当日覆盖必须使用命名锁串行化；ads_health 的 delete+insert 必须放在同一事务内；run_etl 在 dws_sales 或 dws_inventory 未成功时应跳过 ads_health

**证据**：
- etl_dws_inventory.py#L121
- etl_ads_health.py#L313
- run_etl.py#L472

**预防动作**：以后凡是日快照覆盖型 ETL，先检查是否需要命名锁、单事务覆盖和下游依赖短路，避免重跑时删空当天数据

---

### [2026-03-23 16:27] · task · field-mapping

**触发场景**：将 dws_inventory 从 Oracle 切到 ODS 时核对 qty_valid 口径

**错误假设**：默认把 ods_fa_storage.qtyvalid 当作 dws_inventory.qty_valid 的来源

**修正结论**：当前库存快照仍应沿用 qty 作为 qty_valid 口径，因为 Oracle FA_STORAGE.QTYVALID 在现网未维护、通常为 0；切到 ODS 后也不能直接改口径

**证据**：
- docs/ETL业务逻辑说明.md:398-406; docs/数据结构与映射手册.md:519-529; etl_dws_inventory.py:31-38

**预防动作**：以后改库存链前先核对字段是否为未维护字段；ODS 化只迁移链路，不默认改变业务口径

---

### [2026-03-23 16:10] · task · business-rule

**触发场景**：ODS 刚接入主自动化链、但 DWS 尚未切换到消费 ODS 时进行文档同步

**错误假设**：把主链接入 ODS 误写成 ODS 已经与 DWS/ADS 全链打通

**修正结论**：文档只能同步已实现层级：当前仅确认 run_etl 已纳入 ods_sync；dws_sales 和 dws_inventory 仍直连 Oracle，需待第二阶段代码落地后再改来源描述

**证据**：
- run_etl.py:47-56; run_etl.py:381-391; docs/ARCHITECTURE.md; docs/ETL业务逻辑说明.md

**预防动作**：以后先区分调度接入与事实源切换两个阶段，文档同步按已实现阶段最小更新

---

### [2026-03-23 11:50] · task · copilot-agent

**触发场景**：继续推进第二阶段 custom agents 时，需要提高 agent picker 与自然语言发现的稳定性

**错误假设**：默认认为 agent 只要职责边界写得准确，description 用抽象表述即可，被 Copilot 发现的概率不会受真实提问措辞影响。

**修正结论**：对 `.github/agents/*.agent.md` 来说，description 既是说明文，也是发现面；除了职责描述外，必须补齐更贴近用户真实提问方式的触发词，例如“怎么推进”“先别改代码”“帮我补文档”“帮我看看这次改动有没有风险”，否则 agent picker 和自然语言发现都更容易出现歧义。

**证据**：
- .github/agents/planner-hefang.agent.md
- .github/agents/etl-auditor-hefang.agent.md
- .github/agents/doc-syncer-hefang.agent.md
- .github/agents/db-inspector-hefang.agent.md
- .github/agents/reviewer-hefang.agent.md

**预防动作**：后续继续扩 agent 或 prompt 时，先从真实聊天里的用户说法反推 description，而不是只写领域术语；验收重点也应包含“是否容易被看懂和选中”，而不只看文件是否存在。

### [2026-03-23 11:31] · task · copilot-hook

**触发场景**：用户反馈最新一次 Python 版 PostToolUse 和 Stop 都没有任何 warning 卡片，需要区分“hook 未执行”与“宿主未展示”

**错误假设**：默认认为只要 Python hook 返回非零退出码并把提示写到标准输出，宿主就会像 PowerShell warning 一样稳定展示卡片。

**修正结论**：当前仓库环境下，Python hook 走 `stdout + exit 1` 仍可能只落日志、不出 UI 卡片；若日志已确认命中 `warning`，但用户侧完全无卡片，应优先把提示输出切到 `stderr`，而不是先怀疑 hooks 没运行。

**证据**：
- scripts/copilot_post_edit_reminder.py
- scripts/copilot_session_close_reminder.py
- logs/copilot_post_edit_reminder.log
- logs/copilot_session_close_reminder.log

**预防动作**：后续调试 Copilot hooks 时，先用日志判断“是否执行”，再用 `stdout/stderr` 组合判断“是否可见”；Python 化只解决宿主噪音，不代表 warning 展示语义会自动等同于 PowerShell 路径。

### [2026-03-23 11:32] · task · copilot-hook

**触发场景**：Stop 改成 Python 后，真实 Copilot UI 仍出现旧 `cmd` 路径报错与 `pwsh` 风格噪音，需要同时处理 Stop 与 PostToolUse 的宿主配置滞后问题

**错误假设**：默认认为只要修改 `.github/hooks` 里的命令，当前宿主会立刻热更新到新路径，因此可以直接删除旧的 `.cmd` 或 `.ps1` 实现。

**修正结论**：在当前 Copilot hooks 试验阶段，宿主可能继续沿用旧会话中的 `pwsh` 或 `cmd` 路径；更稳妥的做法是把主实现切到 Python，同时保留旧入口作为极薄兼容包装层，把旧路径统一转发到 Python，先消除“路径不存在”和中文 stderr 噪音，再观察宿主 UI 是否完成收敛。

**证据**：
- .github/hooks/post-edit-reminder-hefang.json
- scripts/copilot_post_edit_reminder.py
- scripts/copilot_post_edit_reminder.ps1
- scripts/copilot_session_close_reminder.cmd
- scripts/copilot_session_close_reminder.py

**预防动作**：后续继续调整 Copilot hooks 命令链路时，优先采用“新主实现 + 旧入口兼容层”的双轨收口方式；只有在真实宿主已确认切到新路径后，再考虑移除旧包装层。

### [2026-03-23 11:02] · task · copilot-hook

**触发场景**：在真实 Copilot 会话中复测 Stop hook warning 的 UI 展示效果

**错误假设**：默认认为只要脚本显式设置 UTF-8，PowerShell 非零 stderr 路径下的中文 warning 文案就能在宿主 UI 中稳定正常显示。

**修正结论**：真实 Copilot UI 已能展示 `Warning from Stop hook`，但 PowerShell 非零 stderr 中文文案仍可能乱码；若当前目标是先稳定可读性，应优先把用户侧 warning 文案收敛为 ASCII，再把中文说明保留在日志、文档或后续研究中。

**证据**：
- scripts/copilot_session_close_reminder.ps1
- .github/hooks/post-edit-reminder-hefang.json
- docs/misc/superpowers内化会议纪要.md

**预防动作**：后续继续试验 Copilot hooks 的 UI warning 时，把“是否显示”和“是否可读”拆成两层验收；只要宿主走的是 PowerShell 非零 stderr 路径，就优先用 ASCII 提示文案做稳定性基线。

---

### [2026-03-23 11:10] · task · copilot-hook

**触发场景**：真实 Copilot UI 中 Stop warning 已可见，但卡片里仍混入一部分 PowerShell 错误格式元信息

**错误假设**：默认认为只要把 warning 文案改成 ASCII，就能一并消除宿主展示层里的 PowerShell 错误包装噪音。

**修正结论**：ASCII 只能解决文案乱码，不能保证消除顶层 `pwsh` 命令带来的错误格式输出；若要进一步压缩卡片噪音，应优先尝试改顶层 hook 调用方式，例如增加 `cmd` 包装层，减少宿主直接消费 PowerShell 错误记录的机会。

**证据**：
- .github/hooks/post-edit-reminder-hefang.json
- scripts/copilot_session_close_reminder.cmd
- scripts/copilot_session_close_reminder.ps1

**预防动作**：后续凡是基于 PowerShell 非零退出码做 UI warning 的 hooks，都应把“提示文案编码”和“顶层命令包装噪音”视为两个独立问题分开收敛。

---

### [2026-03-23 11:18] · task · copilot-hook

**触发场景**：将 Stop warning 文案改成 ASCII 并加入 `cmd` 包装层后，真实 Copilot UI 中仍保留明显的 `NativeCommandError` 风格噪音

**错误假设**：默认认为只要避开中文和直接 `pwsh` 调用，宿主就不会再把 Stop hook 的失败路径包装成 PowerShell 风格错误记录。

**修正结论**：`cmd` 只能减变量，不能保证消除宿主的 PowerShell 错误包装；如果目标是继续压缩 warning 卡片噪音，更可靠的方向是把 Stop hook 实现切到 Python，并改走标准输出加非零退出码，而不是继续围绕 PowerShell 包装层打补丁。

**证据**：
- .github/hooks/post-edit-reminder-hefang.json
- scripts/copilot_session_close_reminder.py
- docs/misc/superpowers内化会议纪要.md

**预防动作**：后续为 Copilot hooks 设计提醒脚本时，若仓库本身已有稳定 Python 运行前提，优先考虑 Python 作为提醒型 hook 的实现语言，减少宿主对 PowerShell 错误语义的附加包装。

---

### [2026-03-23 10:54] · task · copilot-hook

**触发场景**：设计 Stop 收口提醒时，需要避免被工作树历史未提交改动误导

**错误假设**：默认认为 Stop hook 可以直接根据 git diff 或工作树脏状态判断本轮是否发生了有意义编辑

**修正结论**：在当前仓库里，更稳妥的最小证据是复用 PostToolUse 日志中的最近命中类型；这样可以基于真实编辑信号生成收口提醒，并显著降低历史脏改动带来的误报。

**证据**：
- scripts/copilot_session_close_reminder.ps1
- logs/copilot_post_edit_reminder.log
- logs/copilot_session_close_reminder.log

**预防动作**：后续继续扩提醒型 Stop hook 时，优先复用已存在的运行时日志或明确会话内信号，不要先退回到粗粒度的工作树脏状态判断。

---

### [2026-03-23 10:19] · task · copilot-hook

**触发场景**：排查 GitHub Copilot PostToolUse hook 已执行但聊天卡片不稳定展示 warning

**错误假设**：默认认为命中后返回退出码 0 + JSON systemMessage 就等同于稳定的 UI warning 卡片。

**修正结论**：在 VS Code Copilot 的 PostToolUse 场景下，systemMessage 虽是合法输出字段，但更可能被宿主静默消费或不稳定展示；若目标是提高用户可见的 warning，应优先试验其他非零退出码产生 non-blocking warning。

**证据**：
- scripts/copilot_post_edit_reminder.ps1#L73
- scripts/copilot_post_edit_reminder.ps1#L75
- logs/copilot_post_edit_reminder.log#L47

**预防动作**：后续设计提醒型 hook 时，先区分‘给模型/宿主注入上下文’与‘强制用户侧可见 warning’两条路径；涉及 UI 展示的试点一律同时落日志证据并做真实宿主复测。

---

### [2026-03-20 15:27] · task · incremental-logic

**触发场景**：审计并调整 dws_sales 的增量逻辑时，发现代码、契约和说明文档对水位语义描述不一致

**错误假设**：把 dws_sales_daily 描述成类似 ODS 的双水位增量，默认认为存在 MODIFIEDDATE/SETTIME 断点水位

**修正结论**：当前 dws_sales_daily 的真实实现是按 business date 窗口 DELETE+INSERT 的滚动回刷；若要补偿晚到修改，应通过扩大日期回刷窗口或改为消费 ODS，而不是口头宣称已有双水位。

**证据**：
- etl_dws_sales.py#L124
- etl_dws_sales.py#L171
- run_etl.py#L383
- docs/DATA_CONTRACTS.md#L289
- docs/数据仓库与ETL手册.md#L294

**预防动作**：后续审计 DWS/ADS 增量逻辑时，先区分‘独立断点水位’和‘业务日期窗口重刷’两种模式；文档必须直接跟代码实现对齐，不能沿用 ODS 术语。

---

### [2026-03-20 12:04] · task · field-mapping

**触发场景**：拿到 hfsy 真实连接后，对 t_member_bind_info 的 *1 列和 DecryptionTags 做全表补证

**错误假设**：仅根据 SHOW CREATE TABLE 推断 *1 列可直接作为现成明文字段使用，没有继续验证真实覆盖率

**修正结论**：DDL 中存在 platAccount1、bindMobile1、birthday1、gender1、name1 等列，并不代表当前数据已回填；2026-03-20 全表统计显示这些列和 DecryptionTags 在 hfsy 当前快照中均为全空，第一阶段应回到密文字段加本地 AES 解密主路径。

**证据**：
- reports/hfsy_bind_coverage_by_plat.json
- reports/hfsy_probe_stage2.json
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L33

**预防动作**：后续遇到外部接入表时，字段是否可用必须同时验证结构和覆盖率；只看 DDL 只能证明列存在，不能证明链路可直接依赖。

---

### [2026-03-20 11:38] · task · doc-sync

**触发场景**：用户要求将 hfsy 连接上下文同步到文档，并直接给出真实数据库密码

**错误假设**：把用户在会话中给出的真实密码直接写回 git 跟踪文档，或在示例命令中复现明文密码

**修正结论**：可以同步 host、port、database、user 等连接事实，但真实密码只能保留为会话事实，文档与脚本中一律改为环境变量或本地安全注入占位。

**证据**：
- docs/HFSY数据字典.md#L10
- docs/RUNBOOK.md#L58
- docs/RUNBOOK.md#L64
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L59

**预防动作**：后续只要用户提供真实连接密码，先把仓库安全约束前置，再将文档落盘范围限制为非敏感元信息和注入方式说明。

---

### [2026-03-20 09:50] · task · field-mapping

**触发场景**：收到数云 xlsx 与 hfsy 实库连接信息后执行第2轮实表校正

**错误假设**：把标准方案中的 12 张 fdi_* JSON 表和 MySQL 8.0+ 建议，当成当前真实落库结构与硬前提

**修正结论**：当前真实来源库为 hfsy，版本为 MySQL 5.7.42，核心表为 t_member_info、t_member_bind_info、t_trade、t_order、t_pin_xid_rel、sys_area；第一阶段实现应优先消费 t_member_bind_info 中现成的 *1 解密列，仅在缺失时回退本地 AES 解密。

**证据**：
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L24
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L31
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L42

**预防动作**：后续审计外部接入时，必须区分标准方案、字段字典、真实实表三层证据；只有真实实表可以直接驱动开发设计，推荐版本不能当成当前环境硬门槛。

---

### [2026-03-19 18:11] · user-feedback · path

**触发场景**：用户澄清公司开发环境的数据库职责边界

**错误假设**：默认假设存在内部DBA/运维或其他数据库同事，可协助导出CRM实表证据

**修正结论**：当前公司开发环境下只有用户一人负责数据库；Oracle在阿里云，MySQL和hefang_dw项目在公司服务器虚拟机上由用户一手搭建。后续需要真实CRM结构或样本时，应先向用户索取其可直接导出的材料；若当前环境不存在该对象，再建议向数云方索取。

**证据**：
- .github/copilot-instructions.md#L18
- AGENTS.md#L18
- .claude/CLAUDE.md#L53

**预防动作**：以后涉及实表、样本、SHOW CREATE TABLE时，先判断本地环境是否存在目标对象；若不存在，不再建议找假定存在的内部同事，而是转向用户本人或外部对接方。

---

### [2026-03-19 18:35] · task · business-rule

**触发场景**：数云CRM第1轮 12 表字段级仲裁

**错误假设**：默认 12 张表都能直接从仲裁文档推出完整的会员匹配方案。

**修正结论**：只有 `fdi_member_info`、`fdi_jos_pin_xid` 与订单链路文档足够支撑第一阶段设计；`fdi_member_point_his`、`fdi_member_grade_his`、`fdi_refund`、`fdi_rate` 仍缺少稳定会员映射证据，必须等真实 `shuyun_ods` 实表或样本再闭环。

**证据**：
- docs/misc/业务数据推送数据库标准方案.md#L17
- docs/misc/业务数据推送数据库标准方案.md#L77
- docs/misc/业务数据推送数据库标准方案.md#L129
- docs/misc/业务数据推送数据库标准方案.md#L468
- docs/misc/跟数云方沟通同步的问题.md#L11
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L47

**预防动作**：后续做外部系统接入审计时，先按“可直接设计 / 待实表验证 / 文档本身缺口”三类拆表，不要把所有文档对象都当成同样成熟。

---

### [2026-03-19 18:10] · task · doc-sync

**触发场景**：再次审计数云CRM实施计划

**错误假设**：把不存在于工作区的 R10 文档当作证据来源，并把固定加密协议做成可配置项；同时误判 `.env.example` 缺失。

**修正结论**：工作区审计只能引用仓库内真实存在的证据文件；当前仓库已存在 `.env.example`，CRM 仅需扩展；`AES-128-ECB-PKCS5Padding + Base64` 是固定协议，不应再暴露 `SHUYUN_AES_MODE`、`SHUYUN_AES_KEY_ENCODING` 之类运行时开关。若数云标准方案与沟通确认单冲突，以沟通确认单为最终仲裁。

**证据**：
- .env.example#L1
- docs/misc/跟数云方沟通同步的问题.md#L5
- docs/misc/业务数据推送数据库标准方案.md#L17
- docs/misc/数云CRM实施上下文与下一步执行入口.md#L21

**预防动作**：后续审计外部接入方案时，先核对“证据文件是否真实存在于工作区”，再区分“固定协议”与“可配置参数”，避免把协议常量设计成环境变量。

---

### [2026-03-19 17:31] · task · business-rule

**触发场景**：数云CRM实施计划交叉审计

**错误假设**：默认认为京东业务表plat_account可直接用于会员关联，且加密输入格式只有一种

**修正结论**：京东业务表plat_account实际为pinid，关联会员前必须先做pin→xid映射；加解密实现必须兼容裸Base64密文和可能存在的~{cipher}~{version}~包裹格式；数云默认按update_time每小时同步一次，ODS建议MySQL 8.0+。

**证据**：
- docs/misc/跟数云方沟通同步的问题.md#L7
- docs/misc/跟数云方沟通同步的问题.md#L10
- docs/misc/跟数云方沟通同步的问题.md#L15
- docs/misc/敏感数据加密规则.md#L10
- docs/misc/敏感数据加密规则.md#L18

**预防动作**：后续实现CRM ETL前，先以仲裁材料覆盖方案假设，并在计划或代码注释中明确写出京东mapping、加密兼容策略和调度频率。

---

### [2026-03-18 14:51] · task · mcp

**触发场景**：VS Code 已重载但当前聊天仍看不到 MCP 工具

**错误假设**：误以为只要仓库根目录存在 .mcp.json 并重载窗口，当前会话就会自动获得 mcp__mysql__/mcp__oracle__ 工具

**修正结论**：.mcp.json 只负责宿主级配置；MCP server 能启动不等于当前聊天会话工具面已刷新。若本轮会话创建时未挂载 MCP 工具，通常需要新开聊天会话重新注册工具。

**证据**：
- .mcp.json#L1
- .claude/settings.json#L1

**预防动作**：区分三层：配置文件存在、server 可启动、当前会话工具已暴露。测试 MCP 时三层都要分别验证。

---

### [2026-03-18 14:40] · task · field-mapping

**触发场景**：Oracle 侧查询“近期销量最好的 3 个产品”时，第一次查询报 ORA-00904。

**错误假设**：误以为 `M_PRODUCT` 存在 `NAME_CN` 字段可直接作为商品名称。

**修正结论**：本仓库已确认的真实映射以 [etl_dim_product.py](etl_dim_product.py#L31) 为准，`M_PRODUCT.NAME` 对应 `product_code`，`M_PRODUCT.VALUE` 对应 `product_name`。

**证据**：
- [etl_dim_product.py](etl_dim_product.py#L33)
- [etl_dim_product.py](etl_dim_product.py#L34)
- [tools/query_data.py](tools/query_data.py#L1)

**预防动作**：凡是 Oracle 真实字段查询，先对照现有 ETL 抽取 SQL、快照或字段映射文档，禁止凭字段命名习惯猜测列名。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.2 | 2026-03-19 | 补充数云CRM 12 表字段级仲裁经验，明确哪些表已可设计、哪些仍需实表验证 |
| v1.1 | 2026-03-19 | 补充数云CRM实施计划再审计经验，明确证据优先级与固定协议不应配置化 |
| v1.0 | 2026-03-18 | 新增 Agent 经验台帐与首条字段映射经验 |