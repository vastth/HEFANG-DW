# TODO_ISSUES.md — 协作待办与风险追踪

> 目的：记录无法归因或待补证据的事项，按 P0/P1/P2 分级。
>
> 说明：P0 需在交互中主动提醒；P1/P2 按计划跟进。

---

## 分级定义

- P0：阻断生产/审计的关键问题，必须立即处理。
- P1：影响一致性或可追溯性，需要尽快修复。
- P2：低风险或优化类事项，可排期处理。

---

## P0

| 编号 | 事项 | 影响 | 证据 | 状态 | 负责人 | 更新时间 |
|---|---|---|---|---|---|---|
| P0-001 | 暂无 | - | - | - | - | - |

---

## P1

| 编号 | 事项 | 影响 | 证据 | 状态 | 负责人 | 更新时间 |
|---|---|---|---|---|---|---|
| P1-001 | dim_channel 来源已定位到 Oracle O2O_RETAIL_CHANNEL，但目标库现存数据仍待回填验证 | 仓库内已补齐标准ETL链路，但 docs/AGENT_HANDOFF.md 已明确“未执行真实 ETL 写库”；若目标库仍保留历史数据，则血缘闭环尚未真正落到实库 | [etl_dim_channel.py](etl_dim_channel.py#L27)；[SQL/create_dim_channel.sql](SQL/create_dim_channel.sql#L1)；[reports/snapshot_oracle_bosnds3_schema.json](reports/snapshot_oracle_bosnds3_schema.json#L9870)；[docs/AGENT_HANDOFF.md](docs/AGENT_HANDOFF.md#L55) | 待验证 | GitHub Copilot | 2026-03-18 |

---

## P2

| 编号 | 事项 | 影响 | 证据 | 状态 | 负责人 | 更新时间 |
|---|---|---|---|---|---|---|
| P2-001 | 暂无 | - | - | - | - | - |

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.0 | 2026-03-01 | 新增协作待办与风险追踪清单 |
| v1.1 | 2026-03-18 | 将 P1-001 调整为“链路已补齐、目标库待回填验证” |
