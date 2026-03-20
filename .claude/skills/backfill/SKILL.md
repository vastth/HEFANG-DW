---
name: backfill
description: 触发指定 ETL 模块的历史数据回填，支持双水位窗口参数。用户说"回填"、"补数"、"重跑历史"时使用。
---

# 回填工作流

## 步骤

1. **确认目标模块**：询问用户要回填哪个模块
   - `ods_m_retail`（零售流水 ODS）
   - `ods_m_retailitem`（零售明细 ODS）
   - `dws_sales`（销售汇总 DWS）
   - `dws_inventory`（库存快照 DWS）

2. **确认回填窗口**：询问起止日期，默认过去 7 天，最大 30 天
   - 格式：`YYYY-MM-DD`

3. **生成回填命令**并展示给用户确认：
   ```bash
   # ODS 回填示例
   python run_ods.py --mode backfill --start-date <START> --end-date <END>

   # ETL 全链路回填示例
   python run_etl.py --backfill --start-date <START> --end-date <END>
   ```

4. **等待用户确认**后再执行（回填会覆盖历史数据，需谨慎）

5. **执行后验证**：
   ```bash
   python tools/check_data.py
   ```

6. **写入交接记录**：完成后调用 `/handoff` 记录本次回填范围与结果

## 注意事项
- 回填窗口超过 7 天时，建议分段执行（每次 7 天），避免单次数据量过大
- dws_inventory 为每日快照，回填会重算指定日期全量库存，耗时较长
- 回填前确认 Oracle 源数据在该时间段内完整
