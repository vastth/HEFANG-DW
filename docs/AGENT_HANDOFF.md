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

### [2026-07-13 13:29] · GitHub Copilot · 回刷并复核门店日报开业日期同店口径

**摘要**：已按授权逐日回刷 2026-07-01 至 2026-07-12 的 v1 门店日报，并完成结果行数、同店辅助字段和 DQ 告警复核。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `ads_store_daily_report` | 数据回刷 | 仅重建 2026-07-01 至 2026-07-12 的 v1 数据 |
| `docs/AGENT_HANDOFF.md` | 修改 | 记录授权回刷与只读复核结论 |

**Copilot 接棒须知**：
- 12 个授权日期全部回刷成功；7 月 1 至 4 日每日报表 74 行，7 月 5 至 12 日每日报表 75 行，均与预检的最终经营实体数一致。
- 同店开业日期不可用告警符合预期：7 月 1 至 4 日为 3 家（RT014、RT140、RT121），7 月 5 至 12 日为 4 家（新增 RT123）；无 dim_store 缺失。
- 同店辅助字段全部非空；已保留当期零、去年同期正值的实体（每日 2 至 8 家），并在 7 月 1 日观察到当期正、去年同期零的 5 家实体。

**未完成项**：
- [ ] 无需继续回刷；如需业务复核，可按门店编码核实 RT014、RT140、RT121、RT123 的 Oracle OPENDATE 是否补齐。

---

### [2026-07-13 13:20] · GitHub Copilot · 验证开业日期刷新并完成门店日报回刷前检

**摘要**：用户已执行 dim_store.open_date DDL 与维表刷新；已完成当前报表月 v1 的只读 DQ，尚未执行 ADS 回刷。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/MYSQL数据字典.md` | 修改 | 回填用户已执行 DDL 和维表刷新后的实际统计 |
| `CHANGELOG.md` | 修改 | 更新 v0.8.82 的人工验证结果 |
| `docs/AGENT_HANDOFF.md` | 修改 | 记录本次生产前置与只读预检 |

**Copilot 接棒须知**：
- 用户已人工执行 SQL/alter_dim_store_add_open_date.sql 并刷新 dim_store；实际 231 家、可用开业日期 95 家、不可用 136 家、无越界日期。
- 2026-07-01 至 2026-07-12 的 v1 同店范围无 dim_store 缺失；开业日期不可用门店 3 至 4 家，按冻结规则为非同店并应输出 DQ 告警。

**未完成项**：
- [ ] 等待用户明确授权后，才按当前报表月实际日期范围逐日回刷 ads_store_daily_report，限定 data_version=v1。
- [ ] 回刷后执行结果行数、同店辅助字段和开业日期不可用告警复核。


---

### [2026-07-13 12:04] · GitHub Copilot · 切换门店日报同店资格为开业日期

**摘要**：完成开业日期维度、同店SQL、测试与业务技术文档对齐；未执行任何生产写操作。

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `etl_dim_store.py` | 修改 | 安全抽取OPENDATE并在全量刷新前检查目标字段 |
| `etl_ads_store_daily_report.py` | 修改 | 同店资格改为源物理门店开业日期并补DQ告警 |
| `SQL/alter_dim_store_add_open_date.sql` | 新增 | 用户人工执行的可空字段DDL |
| `test_dim_store.py` | 新增 | 验证转换与回滚兼容 |
| `test_ads_store_daily_report.py` | 修改 | 锁定双侧零销售同店边界 |
| `docs/` | 修改 | 同步数据字典、契约、映射、ETL说明、业务规范和冻结稿 |

**Copilot 接棒须知**：
- Oracle只读验证通过：OPENDATE原始空值136、非法日期0
- 现网dim_store尚无open_date；ETL会在TRUNCATE前失败保护
- 本轮未执行DDL、dim_store刷新、ADS回刷或TWB修改

**未完成项**：
- [ ] 由用户人工执行SQL/alter_dim_store_add_open_date.sql并用SQL/check_dim_store_open_date.sql确认
- [ ] 完成dim_store刷新后，先对当前报表月v1做只读DQ与EXPLAIN，再由用户授权回刷ADS



---

### [2026-07-05 11:51] · GitHub Copilot · 更新常见问题SOP手册

**摘要**：补充错误有效期切片残留导致总控阻塞的人工排查与修复SOP

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `docs/常见问题SOP手册.md` | 修改 | 新增错误有效期切片残留场景、人工查询SQL、人工修复SQL模板与修复后调度指令 |

**Copilot 接棒须知**：
- 本轮仅更新文档，未执行任何数据库写操作
- SOP已明确只改NAS不足以回拨dim_store_report_attr月内错误切片

**未完成项**：
- [ ] 如后续再次出现同类问题，可按SOP先SELECT核对现网切片，再由用户人工执行UPDATE/DELETE




---

### [2026-07-02 13:16] · GitHub Copilot · 修复 销售部自动化日报 RT119 葡币折算

**摘要**：将 RT119 门店销售金额在 twb datasource 层统一折算为 RMB

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `工作簿/销售部自动化日报.twb` | 修改 | 3 个 datasource 已接入 RT119 *0.84 折算 |

**Copilot 接棒须知**：
- 基础日报 datasource 已改为 Custom SQL；负责人月汇总和同店同比也已接入相同折算逻辑；注意基础日报使用 store_code=RT119，而 ODS 明细使用 c_store_id=RT119。

**未完成项**：
- [ ] 请用户重开 Tableau 工作簿核对门店日报、负责人页和同店同比是否全部收敛到 RMB 口径





---

### [2026-07-01 11:52] · GitHub Copilot · 修复 旧版销售日报负责人汇总同店同比口径

**摘要**：将 销售部自动化日报-Old.twb 的负责人月度汇总改为与 KPI 一致的 helper 聚合口径

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报-Old.twb` | 修改 | 移除负责人月度汇总 datasource 中 same_store_mtd_sales_amt > 0 的过滤并对齐 KPI helper 聚合口径 |
| `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md` | 修改 | 记录旧版负责人汇总同店同比虚高的根因与修复动作 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀负责人汇总不能私自缩窄同店母集的经验 |
| `docs/AGENT_LESSONS_INDEX.md` | 修改 | 重建经验索引纳入本轮 lesson |

**Copilot 接棒须知**：
- 已创建备份 D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报-Old.backup_owner_summary_align_kpi_20260701_1145.twb
- 修改后的旧版 twb 已通过 Python ElementTree XML_OK 校验
- 负责人汇总 worksheet 公式未改，只修复底层 ds_owner_monthly_yoy_live 两处重复 SQL

**未完成项**：
- [ ] 请用户重开 销售部自动化日报-Old.twb，确认区域负责人月度汇总总计与顶部 KPI、门店经营明细总计的同店同比一致






---

### [2026-07-01 10:41] · GitHub Copilot · 恢复销售日报含免税同店口径

**摘要**：按用户更正，将销售日报顶部KPI、区域负责人汇总、门店经营明细重新统一到含免税冻结口径

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `工作簿/销售部自动化日报.twb` | 修改 | 顶部KPI与区域负责人汇总改回含免税 helper 聚合口径 |
| `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md` | 修改 | 记录业务口径更正后恢复含免税冻结版本的修复动作 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀用户更正业务口径后应优先回滚到业务真值的经验 |
| `docs/AGENT_LESSONS_INDEX.md` | 修改 | 重建经验索引纳入本轮业务口径更正 |

**Copilot 接棒须知**：
- 已创建新备份：销售部自动化日报.backup_restore_include_duty_free_same_store_20260701_1110.twb。
- 已完成 twb XML 解析校验，结果 XML_OK。
- 当前预期三处同店同比总计将重新收敛到含免税 helper 口径，约为 6.14%。

**未完成项**：
- [ ] 请用户重开 Tableau 工作簿，确认顶部KPI、区域负责人月度汇总、门店经营明细总计的同店同比已统一。







---

### [2026-07-01 10:23] · GitHub Copilot · 强化沟通语言规则

**摘要**：将英文术语首次出现必须附中文解释、避免未解释黑话表达的要求写入项目级 Copilot 指令

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `.github/copilot-instructions.md` | 修改 | 扩展 HC-LANG 并新增 v3.1 版本记录 |

**Copilot 接棒须知**：
- 用户明确要求后续对话避免只抛英文术语、英文缩写和职场黑话。
- 已同步更新长期用户偏好记忆，后续会话也应沿用这条规则。

**未完成项**：
- [ ] 无








---

### [2026-07-01 10:12] · GitHub Copilot · 修复 Tableau 同店同比 KPI 范围漂移

**摘要**：将销售日报 KPI05 的 same_store_daily 从 ADS 实体辅助字段聚合改回 physical same_store_store_set 汇总，修正 2026-06-30 的 6.14% 异常口径

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `D:/tianhao/Documents/我的 Tableau 存储库/工作簿/销售部自动化日报.twb` | 修改 | ds_kpi_same_store_yoy_physical_live 的 same_store_daily 改为汇总 same_store_store_set |
| `docs/Tableau_TWB编译知识库/Tableau_TWB错误修复台帐.md` | 修改 | 记录 same_store KPI 6.14% 异常的根因与修复动作 |
| `docs/AGENT_LESSONS.md` | 修改 | 补充 Tableau same_store_daily scope drift 经验 |
| `docs/AGENT_LESSONS_INDEX.md` | 修改 | 重建经验索引纳入本轮 lesson |

**Copilot 接棒须知**：
- 已用 Python ElementTree 解析销售部自动化日报.twb，XML 结构校验通过。
- same_store_daily 的两份 SQL 副本都已切换到 same_store_store_set；剩余 ads_store_daily_report 引用属于 operating_entity_daily，不是本轮问题。
- 业务口头 15.83%/25.12% 尚未完全映射到当前仓库某一条已落地 SQL，本轮先修掉已确认错误的 6.14%。

**未完成项**：
- [ ] 请用户重开 Tableau 工作簿，确认 KPI05 渲染值已脱离 6.14% 并符合 physical same-store 预期。
- [ ] 如需继续追业务 15.83%/25.12%，下一步重点核对共同考核主体 BJGM/GZTH 与免税口径是否还存在另一层业务筛选。









---

### [2026-06-25 09:49] · GitHub Copilot · 修复门店日报同店同比分母截止规则

**摘要**：门店日报在月中快闪合并时，同店去年同期分母改为截到合并前一天

**变更文件**：

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `etl_ads_store_daily_report.py` | 修改 | 新增快闪合并截止作用域并截断 same_store_last_year_mtd_sales_amt |
| `test_ads_store_daily_report.py` | 修改 | 新增月中快闪合并同比分母回归测试 |
| `docs/DATA_CONTRACTS.md` | 修改 | 同步同店去年同期分母截止规则 |
| `docs/ETL业务逻辑说明.md` | 修改 | 补充月中快闪合并的同比辅助口径说明 |
| `CHANGELOG.md` | 修改 | 记录本轮修复与验证结果 |
| `docs/AGENT_LESSONS.md` | 修改 | 沉淀月中快闪合并同比分母截断经验 |
| `docs/AGENT_LESSONS_INDEX.md` | 修改 | 重建经验索引 |

**Copilot 接棒须知**：
- 当前仅完成 SQL 生成与文档同步验证，未执行真实写库重跑
- 若用户人工重跑 2026-06-15 之后的 ads_store_daily_report，需要回查 SUBJ_BJGM 的 same_store_last_year_mtd_sales_amt 是否变为截至 2025-06-14

**未完成项**：
- [ ] 用户人工重跑受影响 report_date/data_version 的 ads_store_daily_report
- [ ] 重开 销售部自动化日报.twb 验证 北京国贸店经营体 同比是否改为以 2025-06-14 为分母上界











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
