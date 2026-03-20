# HEFANG-DW OpenCode Agent Guide

本文件是 HEFANG-DW 在 OpenCode Desktop / CLI 下的最小工作流入口。

## 目标

- 以 AI agent 为主完成脚本开发、审计、文档同步与交接
- 不改业务口径时，优先做最小变更
- 所有事实以代码、SQL、配置和现有文档为准，不臆造

## 事实源优先级

1. `*.py`、`SQL/*.sql`、`config.py`
2. `docs/ARCHITECTURE.md`、`docs/RUNBOOK.md`、`docs/DATA_CONTRACTS.md`
3. `README.md`

## 工作原则

- 复杂任务先规划，再修改
- 不修改业务 SQL / ETL 核心逻辑，除非用户明确批准
- 修改 ETL、SQL、表结构相关内容后，必须检查文档同步
- 完成一组有意义的变更后，必须追加 `docs/AGENT_HANDOFF.md`
- 每次排障后若形成可复用结论，或用户明确纠正业务逻辑/字段语义，必须写入 `docs/AGENT_LESSONS.md`
- 所有密钥、连接串、Webhook 只允许通过环境变量提供

## 环境现实约束

- 当前公司开发环境中，用户是唯一负责数据库与数仓项目的人；不要默认存在内部 DBA、运维或其他数据库开发同事可协助。
- Oracle 位于阿里云；MySQL 与 `hefang_dw` 运行在公司服务器虚拟机，且均由用户一手搭建。
- 需要真实结构、样本或推送事实时，优先向用户索取其当前可直接导出的材料；若本地环境没有该对象，再建议向外部对接方索取。
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

## 外部示例仓库

- 外部示例仓库统一存放在 `example_repos/`
- 后续 clone 参考仓库时，默认落盘到 `example_repos/<repo-name>/`
- `example_repos/` 不属于 HEFANG-DW 主项目事实源，也不纳入文档同步审计范围

## Windows 优先

- 默认使用 `python` 与 `pwsh`
- 不依赖 Node/TypeScript 插件链路
- 命令示例优先按本地 Windows 环境编写
