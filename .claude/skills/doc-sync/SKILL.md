---
name: doc-sync
description: 检查代码与文档的同步状态，列出 MISSING/OUTDATED 项，并在用户确认后委托 doc-syncer 子代理自动修复。
---

## /doc-sync — 文档同步检查与修复

### 执行步骤

**第一步：运行文档同步审计**
```bash
python scripts/check_doc_sync.py
```

**第二步：解析审计结果**

读取输出，提取：
- `MISSING`：文档中缺少的内容（代码有、文档没有）
- `OUTDATED`：文档记录与代码不一致的内容
- `OK`：无需更新的项

**第三步：展示差异清单**

以表格形式展示需要更新的项目：

```
## 文档同步差异报告

| 文档 | 差异类型 | 具体内容 |
|------|---------|---------|
| docs/MYSQL数据字典.md | MISSING | 字段 xxx 未记录 |
| docs/DATA_CONTRACTS.md | OUTDATED | dws_sales_daily 水位字段已变更 |

共发现 N 项需更新，M 项已同步。
```

**第四步：确认修复**

若有需要更新的项目：
- 告知用户差异内容
- 询问是否执行自动修复
- 若用户确认，委托 `doc-syncer` 子代理执行更新

**第五步：二次验证**

修复后再次运行 `python scripts/check_doc_sync.py`，确认无 MISSING/OUTDATED。

### 使用场景
- 完成 ETL 变更后检查文档是否对齐
- 定期（每周）例行文档健康检查
- 在提交代码前确认文档已同步
