---
name: etl-audit
description: 对指定 ETL 模块执行完整审计，检查字段映射、业务口径、增量逻辑、幂等性设计。输出带优先级的发现清单。
argument-hint: "[模块名，如 dws_sales 或 ods_m_retail | 留空=审计全部]"
---

## /etl-audit — ETL 完整审计

### 执行步骤

**第一步：确定审计范围**

根据 `$ARGUMENTS` 确定要审计的模块：
- 若传入 `dws_sales`，则审计 `etl_dws_sales.py`
- 若传入 `ods`，则审计全部 `etl_ods_*.py`
- 若未传参数，则审计全部 `etl_*.py`（约 9 个模块）

**第二步：委托 etl-auditor 子代理执行审计**

将以下信息传递给 `etl-auditor`：
1. 待审计的文件列表
2. 用户的关注重点（若有）

**第三步：汇总审计结果**

接收 `etl-auditor` 的发现清单后，按优先级排序：

```
## ETL 审计报告摘要 — [日期]

### 需立即处理（❌ CRITICAL）
[列出所有 CRITICAL 发现，逐条附上文件:行号 + 修复建议]

### 需计划处理（⚠️ WARNING）
[列出所有 WARNING 发现]

### 建议优化（ℹ️ INFO）
[列出所有 INFO 建议]

### 审计覆盖
- 审计模块：N 个
- 发现问题：CRITICAL×A，WARNING×B，INFO×C
- 通过率：X%
```

**第四步：建议后续行动**

- 若有 CRITICAL：建议立即修复，并在修复后运行 `/quality-check` 验证
- 若有 WARNING：建议在下次迭代中处理，记录到 `docs/TODO_ISSUES.md`
- 若全部通过：建议运行 `/handoff` 记录审计结果

### 示例调用
```
/etl-audit dws_sales        # 只审计销售日报
/etl-audit ods              # 审计全部 ODS 层
/etl-audit                  # 审计全部模块
```
