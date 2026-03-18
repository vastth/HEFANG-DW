---
description: 检查并同步 HEFANG-DW 文档与代码的差异
agent: doc-syncer
subtask: true
---

# /doc-sync

针对以下范围检查文档同步情况：`$ARGUMENTS`

执行要求：

1. 优先运行或参考 `python scripts/check_doc_sync.py`
2. 汇总 `MISSING`、`OUTDATED`、`OK`
3. 只在确有必要时更新文档，避免无意义改写
4. 若涉及业务口径变更，停止自动修订并明确标注需人工确认
5. 更新后建议再次验证

优先关注文档：

- `README.md`
- `docs/ARCHITECTURE.md`
- `docs/RUNBOOK.md`
- `docs/DATA_CONTRACTS.md`
- `docs/MYSQL数据字典.md`
- `docs/ETL业务逻辑说明.md`

输出应包含：

- 差异清单
- 建议修改的文档
- 如已修改，列出修改摘要
- 下一步验证建议
