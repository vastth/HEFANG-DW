---
description: 运行 HEFANG-DW 最小质检链路
agent: build
---

# /quality-check

对当前变更执行最小质量检查：`$ARGUMENTS`

默认按 Windows 本地环境执行，优先使用以下链路：

1. 优先：`pwsh scripts/doctor.ps1`
   回退：`powershell -ExecutionPolicy Bypass -File scripts/doctor.ps1`
2. `python run_etl.py --conn-test`
3. `python tools/check_data.py`
4. `python scripts/check_doc_sync.py`

若用户明确指定更小范围，可缩减检查项；否则默认执行上述最小链路。

若当前环境不存在 `pwsh`，则自动改用 `powershell` 执行 `doctor.ps1`，不要因此中断整条质检链路。

输出要求：

- 每项是否通过
- 失败或警告的关键原因
- 是否阻断交付
- 建议下一步修复动作
