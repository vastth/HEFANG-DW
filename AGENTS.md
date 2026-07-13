# HEFANG-DW OpenCode Agent Guide

本文件是 HEFANG-DW 在 OpenCode Desktop / CLI 下的最小工作流入口。

## 共享硬约束入口

- 通用硬约束统一以 [.github/copilot-instructions.md](.github/copilot-instructions.md) 为唯一真值源，包括执行目标、证据要求、交接、数据库读写边界、超时治理、文档同步、业务口径、上下文压缩与安全约束。
- 本文件只保留 OpenCode Desktop / CLI 的增量信息：事实源优先级、OpenCode 常用命令、MCP 现状、外部示例仓库约定与 Windows 使用习惯。
- 若本文件与 [.github/copilot-instructions.md](.github/copilot-instructions.md) 对同一硬约束表述不一致，以 [.github/copilot-instructions.md](.github/copilot-instructions.md) 为准。

## 目标

- 作为 OpenCode Desktop / CLI 的轻量入口，帮助快速定位事实源、常用命令、MCP 状态与 Windows 使用习惯。
- 以 AI agent 为主协助脚本开发、审计、文档同步与交接；通用执行边界统一回到 [.github/copilot-instructions.md](.github/copilot-instructions.md)。

## 事实源优先级

1. `*.py`、`SQL/*.sql`、`config.py`
2. `docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/DATA_CONTRACTS.md`
3. `README.md`

## 工作原则（OpenCode 增量）

- 在涉及需要构建新 DDL 等业务模块时，总是站在 CDO 首席数据官的视角思考。
- 复杂任务优先使用 `/plan` 收敛目标、证据缺口和执行顺序；是否进入修改仍按 [.github/copilot-instructions.md](.github/copilot-instructions.md) 的通用硬约束执行。
- 只读查询链路优先走 DBHub 只读账号 `dbhub_ro`；若 DBHub 结果与项目直连查询结果不一致，以 `hefang_dw` 项目直连查询为准，并在回复中说明差异来源。
- OpenCode 日常开局可直接运行 `python scripts/agent_context_pack.py` 生成短上下文包；其余上下文压缩、防注入和大结果落盘规则按 [.github/copilot-instructions.md](.github/copilot-instructions.md) 的 `HC-CTX` 执行。

## 环境补充

- Oracle 位于阿里云；MySQL 与 `hefang_dw` 运行在公司服务器虚拟机。
- 需要真实结构、样本或推送事实时，优先使用用户当前可直接导出的材料；若本地环境没有该对象，再建议向外部对接方索取。
- 若用户已明确当前 MySQL 未落任何 CRM 表，则后续不得再把本地 MySQL 当作 `shuyun_ods` 的实证来源。

## 常用 OpenCode 命令

- `/plan`：先输出实施方案
- `/etl-audit [模块]`：审计 ETL / SQL / 字段映射与增量逻辑
- `/doc-sync`：检查文档与代码是否同步
- `/quality-check`：运行项目级最小质检链路
- `/handoff [摘要]`：写入 Agent 交接记录
- `/lesson [摘要]`：写入 Agent 经验台帐

## 关键项目入口

- 主 ETL：`run_etl.py`
- ODS 调度：`run_ods.py`
- 定时包装：`scheduled_etl.py`
- 文档审计：`scripts/check_doc_sync.py`
- 交接记录：`scripts/log_agent_action.py`
- 经验台帐：`scripts/log_agent_lesson.py`
- 运行手册：`docs/RUNBOOK.md`
- 架构地图：`docs/ARCHITECTURE.md`

## MCP 现状

- 2026-03-24 已验证：当前 VS Code / Copilot 会话实际注册 MCP 工具时，优先看工作区 `.vscode/mcp.json` 与用户级 `mcp.json`，不会自动把仓库根 `.mcp.json` 暴露为会话工具。
- MySQL 已通过工作区 `.vscode/mcp.json` 中的 DBHub 配置打通，当前会话可直接执行 DBHub 结构查询与只读 SQL。
- Oracle 已通过工作区 `.vscode/mcp.json` + `.vscode/start_oracle_mcp.ps1` 打通；连接串来自本机环境变量 `ORACLE_CONNECTION_STRING`，默认 schema 为 `BOSNDS3`。
- Oracle MCP 当前以 `mcp_oracle_reqd_query` 最稳定；`mcp_oracle_list_tables` 可能返回空，`mcp_oracle_describe_table` 对部分表可能识别失败，此时回退到 `ALL_TABLES` / `ALL_TAB_COLUMNS` 只读查询。
- 若新加 MCP server 后当前会话仍看不到对应工具，优先重载 VS Code 并新开聊天，而不是继续沿用旧会话。

## 外部示例仓库

- 外部示例仓库统一存放在 `example_repos/`
- 后续 clone 参考仓库时，默认落盘到 `example_repos/<repo-name>/`
- `example_repos/` 不属于 HEFANG-DW 主项目事实源，也不纳入文档同步审计范围

## Windows 优先

- 默认使用 `python` 与 `pwsh`
- 不依赖 Node/TypeScript 插件链路
- 命令示例优先按本地 Windows 环境编写
