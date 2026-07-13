# M5 ADS v2 闭环切换清单

---

## 1. 文档用途

本清单用于承接 2026-05-12 用户确认的激进推进策略：不再等待 3 到 7 天总控非阻断 shadow 连续运行证据，改为由用户手工执行两次总控 V2 模式；若主链、专题链和 shadow 非阻断边界均可接受，则继续推进架构重构收口。

本文只固化执行顺序、验收证据和 ADS 兼容红线，不授权 Agent 直接执行生产写库。2026-05-12 用户已按本清单手工完成两轮 `run_scheduled_total_control_v2.bat`，本文件已回填本次双跑证据与剩余边界。

---

## 2. 硬边界

| 边界 | 说明 |
|------|------|
| 写库边界 | 两次总控 V2 运行由用户人工执行；Agent 只准备 wrapper、命令、文档和验收清单 |
| ADS 字段契约 | 所有既有 ADS 表字段不得改名、不得删除；确需扩展时只能新增字段 |
| 读源切换范围 | 当前直接从旧销售 / 库存 DWS 切到 `_v2` 的 ADS 只有 `ads_inventory_health` |
| 销售 ADS 边界 | 多数销售 ADS 事实口径已是 ODS / 配置 / ADS 派生，不应强行改读 `dws_sales_daily_v2` 汇总表，否则会丢失单据级、SKU 连带、考核归属和目标配置等细粒度逻辑 |
| shadow 边界 | 非 V2 生产入口下，`dws_v2_shadow` 仍是非阻断子链；但 Windows 计划任务进入 V2 wrapper 后，`_v2` DWS 已成为 `ads_inventory_health` 读源，必须先以阻断型 `DWS v2 读源预刷新` 完成，再运行主链 ADS |

---

## 3. ADS 依赖盘点结论

补充：2026-06-10 用户已手工移除 `ads_sales_org_daily`、`ads_sales_org_monthly`、`ads_sku_daily`，当前 ADS v2 闭环范围仅保留仍在运行的 4 张 ADS；下文 2026-05-12 双跑记录中提到的“六层 ADS”仅保留为当时历史事实，不再代表当前运行对象。

| ADS / 脚本 | 旧 DWS 依赖 | V2 闭环处理 |
|------------|-------------|--------------|
| `ads_inventory_health` / `etl_ads_health.py` | 直接依赖 `dws_inventory_daily` 与 `dws_sales_daily` | 已具备 `legacy / shadow_compare / v2` 显式切换；V2 模式改读 `dws_inventory_daily_v2 + dws_sales_daily_v2` |
| `ads_store_daily_report` / `etl_ads_store_daily_report.py` | 不直接依赖旧销售 DWS；主体逻辑直读 `ods_m_retail`、`ods_m_retailitem`、考核配置、目标配置和负责人映射 | 不强行改读 DWS v2 汇总；只需确认总控 V2 模式下 freshness 可随 cutover 派生到 v2 |
| `ads_store_daily_subject_report` / `etl_ads_store_daily_subject_report.py` | 依赖 `ads_store_daily_report` | 跟随门店日报专题链；不单独切 DWS v2 |
| `ads_daily_sales` / `etl_ads_daily_sales.py` | 直读 ODS 明细 | 保持 ODS / 配置口径；不强行改读 DWS v2 |
| `ads_sales_org_daily` / `ads_sales_org_monthly` / `ads_sku_daily` | 已退役并从现网移除 | 不再纳入当前 ADS v2 闭环范围；历史 gate 记录仅作存档 |
| `ads_ovopark_store_monthly` / `etl_ads_ovopark_store_monthly.py` | 依赖独立的 `dws_ovopark_passenger_flow_monthly` | 不属于当前销售 / 库存 DWS v2 闭环范围，后续单独治理 |

结论：本轮“所有 ADS 切到 V2 闭环”的当前可执行定义是：所有仍在运行且直接依赖旧销售 / 库存 DWS 的 ADS 已具备 V2 切换并完成双跑验收；其余仍在运行的 ADS 需确认不依赖旧 DWS，保持既有 ODS / 配置 / ADS 派生口径，并通过总控 V2 双跑证明不会被 cutover 参数破坏。已退役的 `ads_sales_org_daily`、`ads_sales_org_monthly`、`ads_sku_daily` 不再纳入当前闭环范围。

---

## 4. 两次总控 V2 执行清单

### 4.1 推荐命令

优先使用显式 V2 wrapper：

```powershell
.\run_scheduled_total_control_v2.bat
```

等价 Python 命令：

```powershell
python scheduled_total_control.py --cutover-mode v2
```

如需立即验证回退路径：

```powershell
.\run_scheduled_total_control_v2.bat --rollback-to-legacy
python scheduled_total_control.py --cutover-mode v2 --rollback-to-legacy
```

### 4.2 第 1 轮验收

| 检查项 | 通过标准 | 证据 |
|--------|----------|------|
| 进程退出码 | 退出码为 0；若因 shadow 失败仅降级 WARNING，总控不应被 shadow 阻断 | 控制台日志、总控摘要 |
| 主链 | `run_etl.py` 在 `v2` 模式下完成，`ads_inventory_health` 使用 `_v2` DWS 源 | 总控摘要、主链结构化摘要 |
| 门店专题 | 专题链接受同一 cutover 参数，freshness 来源可派生到 v2 或显式覆盖 | 总控摘要、专题摘要 |
| shadow | `dws_v2_shadow` 不阻断总控；若失败，记录 WARNING 和失败阶段 | 总控摘要、shadow JSON |
| ADS 字段 | 未出现改名、删字段或消费层字段缺失报错 | 控制台日志、Tableau / 下游人工观察 |

### 4.3 第 2 轮验收

第 2 轮必须在第 1 轮完成后再次执行同一 V2 命令，重点验证幂等性、命名锁释放、重复运行后的行数 / 金额 / 字段稳定性：

| 检查项 | 通过标准 | 证据 |
|--------|----------|------|
| 幂等重复运行 | 第二轮退出码仍为 0，不出现重复键、锁残留、事务清理异常 | 控制台日志、总控摘要 |
| V2 DWS 重算 | `dws_sales_daily_v2` / `dws_inventory_daily_v2` 重算阶段可重复执行 | shadow JSON、DWS v2 运行 JSON |
| ADS 生产表 | 既有字段仍存在，`ads_inventory_health` 可由 V2 源产出 | 主链摘要、ADS 下游观察 |
| rollback 可用性 | 如需要，可立即执行 `--rollback-to-legacy` 并恢复 legacy 读源 | 回退命令日志 |

---

## 5. 双跑后推进判定

| 判定 | 条件 | 下一步 |
|------|------|--------|
| 通过 | 两轮总控 V2 退出码均为 0；无 ADS 字段缺失；shadow 未阻断主链；主要数据差异有证据解释 | 已进入 M6：用户已把计划任务入口切到 V2 wrapper；后续重点从“是否切换”转为“V2 读源预刷新顺序与实跑稳定性” |
| 有警告可继续 | shadow WARNING 但主链 / 专题链成功，且 WARNING 不涉及 V2 数据写坏或 ADS 字段破坏 | 记录 WARNING，按影响决定是否先修复再切计划任务 |
| 阻断 | 主链失败、专题链失败影响业务输出、`ads_inventory_health` V2 写出异常、字段缺失或锁残留 | 立即执行 rollback 命令或恢复 legacy 入口，保留日志后修复 |

---

## 6. 2026-05-12 双跑结果回填

### 6.1 双跑总控摘要

| 轮次 | 时间范围 | 总控状态 | 主链 | 门店销售专题 | DWS v2 shadow | 结论 |
|------|----------|----------|------|--------------|---------------|------|
| 第 1 轮 | 2026-05-12 16:42:10 ~ 16:58:55 | `WARNING`，成功=2 / 警告=1 / 失败=0 | `SUCCESS`；`ads_inventory_health` 已切到 `dws_inventory_daily_v2 + dws_sales_daily_v2`，写出 3087 行 | `SUCCESS`；DWS freshness 命中 4 天，已重跑 2026-05-05 ~ 2026-05-08 六层 ADS | `WARNING`；销售 DWD→v2 mismatch=0，库存当前 ODS 基线 mismatch=0，库存 DWD→v2 mismatch=0；仅报告型 ADS compare 命中 SQL 别名错误 | shadow 未阻断总控，生产主链与专题链成功 |
| 第 2 轮 | 2026-05-12 16:59:34 ~ 17:23:22 | `WARNING`，成功=2 / 警告=1 / 失败=0 | `SUCCESS`；`ads_inventory_health` 继续使用 V2 DWS 源，写出 3087 行，17:02 完成 | `SUCCESS`；DWS freshness 命中 7 天，已重跑 2026-05-05 ~ 2026-05-11 六层 ADS | `WARNING`；销售 DWD→v2 mismatch=0，库存当前 ODS 基线 mismatch=0，库存 DWD→v2 mismatch=0；报告型 ADS compare 仍命中同一 SQL 别名错误 | 第二轮幂等通过，shadow 警告非数据写坏类 |

证据入口：

- 总控日志：`logs/scheduled_total_control_20260512.log`
- 主链日志：`logs/etl_20260512.log`
- 门店销售专题日志：`logs/store_daily_report_schedule_20260512.log`
- 第 1 轮 shadow JSON：`reports/context_cache/scheduled_dws_v2_shadow_20260512_165855.json`、`reports/context_cache/dws_sales_v2_shadow_20260512_165535.json`、`reports/context_cache/dws_inventory_v2_shadow_20260512_165535.json`
- 第 2 轮 shadow JSON：`reports/context_cache/scheduled_dws_v2_shadow_20260512_172320.json`、`reports/context_cache/dws_sales_v2_shadow_20260512_171935.json`、`reports/context_cache/dws_inventory_v2_shadow_20260512_171935.json`

### 6.2 下游消费层观察

用户人工观察结论：

1. `etl_ads_store_daily_report.py` 产出的数据源对应消费的 Tableau 看板，最新数据时间为 2026-05-12 17:11，各指标数据正确。
2. 库存看板消费的 `ads_inventory_health` 表，最新更新日期为 2026-05-12 17:02，数据显示正常。

上述观察满足本清单的 ADS 字段兼容与下游消费层验收要求：本轮未发现 Tableau 字段缺失、既有 ADS 字段改名或删除造成的消费层阻断。

### 6.3 Shadow WARNING 处置

两轮 `DWS v2 Shadow` 的非阻断 WARNING 均来自报告型 `ads_inventory_health` shadow compare SQL：`Unknown column 'ranked.color' in 'field list'`。该错误不发生在生产 `ads_inventory_health` 写表路径，且主链已在 V2 模式成功写出 `ads_inventory_health`。

处置：已在 `etl_ads_health.py` 将 shadow 投影中 `sku.sku_color` / `sku.sku_size` 显式别名为 `color` / `size`，并新增回归测试保护；验证输出已落盘到 `reports/m5_v2_gate_followup_tests_20260512.txt`，结果为 16 项通过。

### 6.4 M5 gate 判定

本次两轮 V2 总控 gate 判定为“通过，可进入 M6 讨论”：

- 主链两轮均成功，且 `ads_inventory_health` 两轮均显式使用 V2 DWS 源。
- 门店销售专题两轮均成功，且 cutover 参数未破坏专题 ADS 链路。
- DWS v2 shadow 两轮均未阻断总控；核心销售与库存 DWD→v2 对账均为 0 mismatch。
- ADS 字段契约未发现破坏；用户已确认销售日报与库存看板消费正常。
- 总控仍存在非阻断 WARNING：达播标签/legacy CSV 未就绪，达播字段按 0 处理；这属于既有达播源就绪问题，不属于 V2 读源切换阻断。

---

## 7. 当前未完成项

1. 用户已进入 M6，并已将 Windows 计划任务入口切到 `run_scheduled_total_control_v2.bat`；Agent 未自动替换生产计划任务。
2. 用户 09:48 前后手动生产重跑 V2 wrapper 已通过，已验证顺序为 `DWS v2 读源预刷新 -> 主链 -> 门店销售专题 -> 后置 DWS v2 Shadow: SKIPPED`；后续仍需观察 Windows 自动触发批次是否保持同样顺序。
3. 默认 `legacy` 代码模式与 `--rollback-to-legacy` 回退入口仍保留；若 V2 预刷新失败，主链会被有意跳过，避免继续写出空的 `ads_inventory_health`。
4. 达播标签主线与 legacy CSV 当前未就绪，`ads_inventory_health` 达播字段仍按 0 处理；该项与 V2 gate 分离，后续按达播源治理继续跟踪。

---

## 8. M6 入口切换后续记录（2026-05-13）

2026-05-13 用户报告手工 / 计划任务入口执行 `run_scheduled_total_control_v2.bat` 失败，并说明已将 Windows 计划任务入口切到该 V2 wrapper，预计中午 12:30 再次运行。

已确认的失败链路：

1. 总控 wrapper 层失败属实：`scheduled_total_control.py` 收到主链 `scheduled_etl.py --cutover-mode v2` 退出码 1，随后跳过门店专题和后置 shadow。
2. 主链 SQL 写表路径本身已完成；失败来自 `scheduled_etl.py` 后置校验。
3. `ads_inventory_health` 在 V2 模式读取 `dws_inventory_daily_v2 + dws_sales_daily_v2`，但 20260513 当日两张 `_v2` DWS 表仍为空，因此 ADS 写出 0 行并触发校验失败。
4. 根因不是 ADS 字段兼容问题，而是总控顺序仍沿用“主链 -> 专题 -> 后置 shadow”；当生产 ADS 已读 `_v2` DWS 时，DWS v2 刷新必须前置。

已完成的修复：

1. `scheduled_total_control.py` 在有效 cutover 模式为 `v2` 且非 `--conn-test` / `--shadow-only` 时，先执行阻断型 `DWS v2 读源预刷新`。
2. 预刷新子链调用 `scheduled_dws_v2_shadow.py --skip-ads-shadow-validation`，只刷新 raw/DWD/DWS v2 读源，不在主链 ADS 写出前比较持久化 `ads_inventory_health`。
3. 预刷新失败时，总控有意跳过主链与专题链，避免继续写出空 ADS；预刷新成功后，后置 `DWS v2 Shadow` 记为 `SKIPPED`，避免重复执行同一刷新。
4. 验证证据：`D:/Anaconda/envs/pyproject/python.exe -m unittest test_scheduled_total_control.py test_scheduled_dws_v2_shadow.py` 共 18 项通过，输出已保存到 `reports/m6_v2_prerefresh_tests_20260513.txt`。

### 8.1 生产手动重跑核验

用户 09:48 前后手动重跑生产 `run_scheduled_total_control_v2.bat` 后，Agent 仅做日志与只读数据核验，未代执行生产总控。

核验结论：本次生产手动重跑通过。

| 检查项 | 结果 |
|--------|------|
| 总控时间 | 2026-05-13 09:47:44 ~ 09:58:35 |
| 总控状态 | `SUCCESS`，成功=3 / 警告=0 / 失败=0 / 跳过=1 |
| 链路顺序 | `DWS v2 读源预刷新 -> 主链调度 -> 门店销售专题 -> DWS v2 Shadow: SKIPPED` |
| DWS v2 读源预刷新 | `SUCCESS`；销售与库存对账均为 0 mismatch；ADS 持久化 compare 按 `--skip-ads-shadow-validation` 预期跳过 |
| 主链调度 | `SUCCESS`；`ads_inventory_health` 使用 `dws_inventory_daily_v2 + dws_sales_daily_v2` 写出 3088 行 |
| 门店销售专题 | `SUCCESS`；门店日报、专题、销售组织与 SKU 日表 / 月表均完成重跑 |
| 只读行数核验 | `dws_sales_daily_v2_20260513=177`、`dws_inventory_daily_v2_20260513=75168`、`ads_inventory_health_20260513=3088` |
| 非阻断提醒 | 达播标签 / legacy CSV 未就绪，达播字段按 0 处理；该项与 V2 读源顺序修复分离 |

证据入口：`reports/m6_v2_prerefresh_production_rerun_20260513.txt`。

注意：本记录说明用户手动生产重跑已通过；仍不能据此宣称后续 12:30 或其它 Windows 自动触发批次已成功，需按实际日志另行核验。

---

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v0.5 | 2026-06-10 | 补记退役三表已从现网移除，并将 ADS v2 闭环盘点范围收口到当前仍在运行的 4 张 ADS |
| v0.4 | 2026-05-13 | 补记用户 09:48 前后手动生产重跑 V2 wrapper 已成功，记录链路顺序、只读行数与后续自动触发观察边界 |
| v0.3 | 2026-05-13 | 补记 M6 Windows 计划任务入口已切 V2、09:09 失败根因、V2 读源预刷新顺序修复和下一次实跑观察口径 |
| v0.2 | 2026-05-12 | 回填两轮总控 V2 gate 执行结果、下游 Tableau / 库存看板人工观察、shadow WARNING 根因与修复验证，判定 M5 可进入 M6 讨论 |
| v0.1 | 2026-05-12 | 新增 ADS v2 闭环切换清单，固化用户决策下的两次总控 V2 gate、ADS 依赖分类、字段兼容红线和 rollback 路径 |