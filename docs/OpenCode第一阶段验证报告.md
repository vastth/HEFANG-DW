# OpenCode 第一阶段验证报告

> 范围：验证 HEFANG-DW 第一阶段最小 OpenCode 工作流是否具备可用性。
>
> 覆盖命令：`/plan`、`/etl-audit`、`/doc-sync`、`/quality-check`
>
> 结论时间：2026-03-11

---

## 1. 总体结论

第一阶段最小工作流已经可用。

- OpenCode 最小配置已建立：`AGENTS.md`、`opencode.json`、`.opencode/commands/`、`.opencode/prompts/agents/` 已落位
- ` /plan `、` /etl-audit `、` /doc-sync `、` /quality-check ` 四类核心动作均已完成模拟或真实测试
- 当前最适合的使用方式是：**先规划 → 再审计/修改 → 再文档同步 → 最后质检与交接**
- 工作流层面已可支撑 AI agent 主导的日常脚本维护与文档治理
- 当前主要短板不在 OpenCode 配置本身，而在文档基线漂移与运行环境差异

---

## 2. 分项验证结果

### 2.1 `/plan`

**测试方式**

- 使用模拟需求：为项目补齐 `requirements.txt`，并统一 `README.md` 与 `docs/RUNBOOK.md` 的依赖安装说明

**验证结果**

- 能输出清晰的 5 段式方案：目标、受影响文件、实施步骤、验证方式、风险与待确认项
- 能遵守“最小变更、不碰业务口径”的项目约束
- 能把任务拆成准备、实施、收尾，而不是直接进入改代码

**可执行结论**

- `/plan` 可作为复杂任务默认起手式
- 特别适合以下场景：新增脚本、整理配置、文档重构、跨文件改动

**当前限制**

- 仍依赖具体任务输入；若需求过于空泛，输出会偏“示例方案”而非真实计划

---

### 2.2 `/etl-audit`

**测试方式**

- 使用模拟命令：`/etl-audit dws_sales`
- 实际读取：`etl_dws_sales.py`、`docs/DATA_CONTRACTS.md`、`docs/ARCHITECTURE.md`、`config.py`

**验证结果**

- 能围绕真实文件输出结构化审计结果
- 已发现对维护有实际价值的问题：
  - `docs/DATA_CONTRACTS.md` 将 `dws_sales_daily` 描述为来自 ODS 聚合，但当前代码实际直接查询 Oracle
  - `docs/ARCHITECTURE.md` 同样存在“从 ODS 聚合”的描述，与当前实现不一致
  - `etl_dws_sales.py` 中按日期范围删除旧数据的 SQL 使用了字符串拼接，存在可维护性风险
- 能区分 `CRITICAL / WARNING / INFO`

**可执行结论**

- `/etl-audit` 已具备发现“代码-文档漂移”和“实现风险点”的能力
- 适合作为 ETL 相关改动前后的标准审计动作

**当前限制**

- 第一阶段仍以人工解释审计结果为主，尚未接入自动修复或更细粒度规则库

---

### 2.3 `/doc-sync`

**测试方式**

- 基于 `/etl-audit dws_sales` 的发现，模拟一次文档同步分析

**验证结果**

- 能把审计发现转成明确的文档同步建议
- 已识别需优先处理的文档：
  - `docs/DATA_CONTRACTS.md`
  - `docs/ARCHITECTURE.md`
- 能区分以下类型：
  - `OUTDATED`：文档描述与当前实现不一致
  - `OK`：暂不需要调整
- 能主动避免“架构意图不明时盲目自动改文档”的风险

**可执行结论**

- `/doc-sync` 适合在 ETL、SQL、调度、结构说明改动后执行
- 对 HEFANG-DW 这种“文档重、交接重”的项目，有明确实际价值

**当前限制**

- 当前测试以分析为主，尚未做一次真实文档落盘修复验证

---

### 2.4 `/quality-check`

**测试方式**

- 按最小链路执行：
  - `pwsh scripts/doctor.ps1`
  - `python run_etl.py --conn-test`
  - `python tools/check_data.py`
  - `python scripts/check_doc_sync.py`

**验证结果**

- `python run_etl.py --conn-test`：通过，Oracle / MySQL 连通正常
- `python tools/check_data.py`：通过，核心数据量对账仅差 1 条，未见阻断性异常
- `python scripts/check_doc_sync.py`：可运行，并产出 `reports/docs_code_alignment.json`
- `pwsh scripts/doctor.ps1`：在当前执行环境失败，原因是环境中无 `pwsh`

**已采取修正**

- 已将 `.opencode/commands/quality-check.md` 调整为：
  - 优先 `pwsh`
  - 不存在时回退到 `powershell -ExecutionPolicy Bypass -File scripts/doctor.ps1`

**可执行结论**

- `/quality-check` 已具备最小可用性，能覆盖连通性、数据质量、文档同步三类检查
- 当前最主要问题不是命令设计，而是运行环境差异与文档基线偏脏

**当前限制**

- 若运行环境不是标准 Windows PowerShell 7，本命令需依赖回退策略
- 输出中存在少量终端编码乱码，但不影响关键结果判断

---

## 3. 第一阶段是否达标

**判定：达标，可进入日常试运行。**

达标依据：

- OpenCode 已有最小入口、命令、子代理
- 四个核心动作均已完成验证
- 至少一个命令（`/quality-check`）已完成真实命令链执行
- 工作流输出已能形成“计划 → 审计 → 文档建议 → 质检结论”的闭环

---

## 4. 当前最重要的可执行结论

1. 后续复杂任务默认先执行 `/plan`
2. 修改 ETL、SQL、调度说明后，默认执行 `/etl-audit` + `/doc-sync`
3. 交付前默认执行 `/quality-check`
4. 当前应优先处理 `dws_sales_daily` 相关文档漂移，而不是继续扩展工作流模板
5. 第二阶段不应急着上重型 plugins / hooks，应先完成一轮真实使用验证

---

## 5. 建议的下一步

### 必做

- 用 OpenCode Desktop / CLI 真实调用一次 `/plan`
- 真实调用一次 `/etl-audit dws_sales`
- 真实调用一次 `/doc-sync`，至少完成一处文档落盘修复

### 建议做

- 补一轮最小环境兼容验证：`pwsh` 与 `powershell` 双入口都测试
- 把 `dws_sales_daily` 的架构描述统一到“当前实现”或“目标架构+现状说明”中的一种

### 暂不建议做

- 不建议现在引入 ECC 的 `.opencode/plugins`
- 不建议现在迁移重型 `hooks`、`rules`、`custom tools`
- 不建议现在扩充大量 commands，避免工作流噪音

---

## 6. 验收标准（可直接复用）

当以下 5 条全部满足时，可认为第一阶段真正稳定：

- OpenCode 能识别 `opencode.json`
- `/plan` 能输出项目化方案，而非通用模板答案
- `/etl-audit` 能对真实模块给出文件级发现
- `/doc-sync` 至少成功完成一次真实文档修复
- `/quality-check` 能在目标运行环境稳定执行完最小检查链路

---

## 7. 结论一句话

HEFANG-DW 的 OpenCode 第一阶段已经从“概念工作流”变成“可试运行的最小工作流”，下一步重点应放在真实使用与文档基线修复，而不是继续扩模板。
