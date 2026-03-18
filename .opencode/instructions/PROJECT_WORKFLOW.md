# HEFANG-DW Project Workflow for OpenCode

## 适用范围

- OpenCode Desktop / CLI
- Windows 本地优先
- 以 AI agent 为主的日常开发、审计、文档维护

## 默认流程

1. 复杂任务先运行 `/plan`
2. 修改脚本或文档时，优先最小改动
3. 若涉及 ETL、SQL、字段、调度、运行方式，运行 `/doc-sync`
4. 交付前运行 `/quality-check`
5. 完成后运行 `/handoff [摘要]`

## 常用事实源

- `docs/ARCHITECTURE.md`
- `docs/RUNBOOK.md`
- `docs/DATA_CONTRACTS.md`
- `.claude/CLAUDE.md`
- `README.md`

## 外部参考仓库

- 外部示例仓库统一放在 `example_repos/`
- 后续 clone 示例仓库时，默认使用 `example_repos/<repo-name>/` 作为落盘路径
- 审计、文档同步、质检默认忽略 `example_repos/` 下内容，避免参考项目污染主仓结果

## 高风险变更

- `etl_*.py`
- `run_etl.py`
- `run_ods.py`
- `scheduled_etl.py`
- `SQL/*.sql`
- `config.py`

遇到以上文件改动时：

- 不要擅自改变业务口径
- 不要写入真实密钥
- 优先引用已有脚本与文档
- 必要时补充交接记录
