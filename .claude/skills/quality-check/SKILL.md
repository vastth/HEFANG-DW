---
name: quality-check
description: 运行全套质检工具并汇总结果。依次执行连通性测试、ETL空跑、数据质量检查、文档同步审计，最终输出一份三行摘要报告。
---

## /quality-check — 全套质检

### 执行步骤

**第一步：连通性测试**
```bash
python tools/test_connection.py
```
预期：`Oracle: OK` 和 `MySQL: OK`

**第二步：ETL 空跑（不连接真实数据库）**
```bash
python run_etl.py --conn-test
```
设置 `ETL_CONN_TEST=1` 跳过真实连接，验证脚本无语法/逻辑错误。

Windows 设置方式：
```powershell
$env:ETL_CONN_TEST = "1"
python run_etl.py --conn-test
```

**第三步：数据质量检查**
```bash
python tools/check_data.py
```
关注：红色警告（❌）行，黄色提示（⚠️）可记录但不阻断。

**第四步：文档同步审计**
```bash
python scripts/check_doc_sync.py
```
关注：`MISSING` 和 `OUTDATED` 标记。

**第五步：输出汇总报告**

```
## 质检报告 — [日期时间]

| 检查项 | 结果 | 说明 |
|--------|------|------|
| 连通性 | ✅/❌ | Oracle OK / MySQL FAIL: ... |
| ETL空跑 | ✅/❌ | 7步全部通过 / 第N步失败: ... |
| 数据质量 | ✅/⚠️/❌ | N项警告: ... |
| 文档同步 | ✅/⚠️/❌ | N项MISSING: ... |

**整体评估**：✅ 全部通过 / ⚠️ 有警告 / ❌ 存在阻断项
**建议**：[若有失败，给出排查建议]
```

### 注意
- 若任一步骤失败，仍继续执行后续步骤（全部收集后统一汇报）
- 连通性失败不影响 ETL 空跑（空跑使用 ETL_CONN_TEST=1）
- 若需修复文档同步问题，运行 `/doc-sync`
