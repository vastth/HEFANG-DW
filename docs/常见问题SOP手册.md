
# 门店渠道类型修正操作模板（可复用）

适用场景：业务在 NAS 目标配置表里把某门店渠道类型录错（例如“联营-免税”应改为“联营-奥莱”），需要当日修正并让报表生效。

## 一、标准闭环（先导入，再重跑）

1. 先做 dry-run，确认本次修正会改哪些门店和字段

命令：
python tools/import_cfg_store_target_daily_from_nas.py --target-month <YYYY-MM> --sync-store-report-attr --preview-limit 20

重点检查以下三项：
- store_attr_changed_preview 是否包含目标门店（例如 store_id=583）
- changed_fields 是否包含 report_channel_type
- store_attr_diff_counts.changed 是否大于 0

2. 再执行正式 apply（这一步才会把 NAS 修正写入配置与维表）

命令：
python tools/import_cfg_store_target_daily_from_nas.py --target-month <YYYY-MM> --apply --sync-store-report-attr --created-by <your_name>

3. 然后重跑受影响日期（只跑 rerun 不会导入 NAS）

最小做法：只重跑修正日。

命令：
.\run_scheduled_store_daily_report.bat --rerun-report-date <YYYY-MM-DD> --rerun-data-version <v1|v2>

更稳妥做法：重跑本月从 1 号到修正日（避免月累计口径残留历史值）。

4. 最后做三层验证

- 维表是否生效：dim_store_report_attr
- 结果表是否生效：ads_store_daily_report（目标日期）
- Tableau 刷新后展示是否一致

## 二、可直接复用的最短命令组

1) dry-run
python tools/import_cfg_store_target_daily_from_nas.py --target-month <YYYY-MM> --sync-store-report-attr --preview-limit 20

2) apply
python tools/import_cfg_store_target_daily_from_nas.py --target-month <YYYY-MM> --apply --sync-store-report-attr --created-by <your_name>

3) rerun（单日）
.\run_scheduled_store_daily_report.bat --rerun-report-date <YYYY-MM-DD> --rerun-data-version <v1|v2>

4) 只读校验 SQL（门店维表）
SELECT
	store_id,
	store_code,
	store_name,
	report_channel_type,
	is_duty_free,
	effective_start_date,
	effective_end_date,
	updated_at
FROM dim_store_report_attr
WHERE store_id = <store_id>
ORDER BY effective_end_date DESC, id DESC;

5) 只读校验 SQL（日报结果表）
SELECT
	report_date,
	data_version,
	store_id,
	store_code,
	store_name,
	report_channel_type,
	is_duty_free,
	updated_at
FROM ads_store_daily_report
WHERE report_date = '<YYYY-MM-DD>'
	AND data_version = '<v1|v2>'
	AND store_id = <store_id>;

## 三、常见误区与排障

1. 误区：只执行 rerun 命令

现象：日志显示“显式批量重跑完成”，但门店渠道类型没有变化。

原因：显式 rerun 只重跑 ADS，不执行目标导入与维表同步。

处理：必须先执行 import_cfg_store_target_daily_from_nas.py --apply --sync-store-report-attr，再 rerun。

2. 误区：NAS 已改但调度日志仍显示“命中相同 MD5 跳过 apply”

现象：自动调度命中幂等，继续沿用上次成功导入记录。

原因：文件未变更或当前调度命中的文件仍是旧版本。

处理：先 dry-run 核对 file_md5 与 changed preview，再执行 apply。

3. 误区：只看 report_channel_type，不核对 is_duty_free

现象：同店同比、免税口径相关指标异常。

处理：渠道类型修正后，必须同时检查 is_duty_free 是否符合业务口径。

4. 误区：昨天错误生效日已经被自动调度写入 MySQL，今天只改 NAS 就以为能自动修复

现象：
- NAS 已把门店生效开始日改回正确日期，但总控仍报“负责人快照出现不应维护的实体编码”。
- `tools/import_cfg_store_target_daily_from_nas.py --sync-store-report-attr` dry-run 中，`store_attr_effective_start_source` 仍显示 `existing_latest_in_target_month`。
- `store_attr_effective_start_date` 仍停留在错误日期，例如 `2026-07-27`。

原因：
- 目标导入工具会优先复用目标月内 `dim_store_report_attr` 已存在的最新 `effective_start_date`，而不是自动回退到 NAS 当前修正后的更早日期。
- 门店属性差异判断只比较 `store_code / store_name / report_channel_type / store_grade / is_duty_free / is_include_in_daily_report`，不比较 `effective_start_date`，因此单纯修正 NAS 生效日期不会把现网错误切片自动回拨。
- 负责人导入按 `snapshot_date` 从 `dim_store_report_attr` 读取“当天有效且纳入口径”的门店，再叠加 `cfg_store_target_daily` 构造应维护实体集合；若目标表已有该门店，而 `dim_store_report_attr` 仍要到未来日期才生效，负责人文件里该门店就会被判成 `unexpected_entities` 并阻断总控。

证据来源：
- [tools/import_cfg_store_target_daily_from_nas.py](tools/import_cfg_store_target_daily_from_nas.py#L102)
- [tools/import_cfg_store_target_daily_from_nas.py](tools/import_cfg_store_target_daily_from_nas.py#L1421)
- [tools/import_cfg_store_target_daily_from_nas.py](tools/import_cfg_store_target_daily_from_nas.py#L1734)
- [tools/import_cfg_store_target_daily_from_nas.py](tools/import_cfg_store_target_daily_from_nas.py#L1817)
- [tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L335)
- [tools/import_store_operation_owner_from_nas.py](tools/import_store_operation_owner_from_nas.py#L983)
- [logs/store_daily_report_schedule_20260705.log](logs/store_daily_report_schedule_20260705.log#L1)

## 四、错误有效期切片残留的人工修复 SOP

适用场景：
- 昨天业务把 NAS 目标配置表的 `生效开始日` 填错，自动调度已把错误日期写入 `dim_store_report_attr`。
- 今天业务虽然改回了 NAS，但总控仍因负责人快照校验失败被卡住。

### 4.1 先做三步只读确认

1. 核对 NAS 当前真值

目标：确认业务文件里目标门店的 `生效开始日` 已改回正确日期。

建议命令：
python tools/import_cfg_store_target_daily_from_nas.py --target-month <YYYY-MM> --sync-store-report-attr --preview-limit 20

重点看：
- `store_attr_effective_start_date`
- `store_attr_effective_start_source`
- `store_attr_unchanged_preview` / `store_attr_changed_preview`

若 `store_attr_effective_start_source=existing_latest_in_target_month`，说明现网月内已有更晚切片，NAS 当前修正不会自动回拨。

2. 核对现网错误切片是否存在

只读 SQL（门店属性维表）：

```sql
SELECT
		id,
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		is_include_in_daily_report,
		effective_start_date,
		effective_end_date,
		remark,
		updated_at
FROM dim_store_report_attr
WHERE store_code = '<RT门店编码>'
ORDER BY effective_start_date, id;
```

3. 核对负责人阻塞是否由这条切片触发

只读验证命令：

```bash
python tools/import_store_operation_owner_from_nas.py --snapshot-date <YYYY-MM-DD> --preview-limit 0
```

若输出中出现：
- `validation_status = FAILED`
- `unexpected_entities` 包含目标门店编码

则说明当前负责人链路确实被该门店的现网有效期状态卡住。

### 4.2 人工修复 SQL 模板

说明：
- 以下 SQL 仅作为人工执行模板，Agent 不会自动落库。
- 先 `SELECT`，再由用户人工执行 `UPDATE` / `DELETE`。
- 变量说明：
	- `<STORE_CODE>`：例如 `RT123`
	- `<CORRECT_START_DATE>`：NAS 当前业务真值，例如 `2026-07-03`
	- `<WRONG_START_DATE>`：现网错误切片开始日，例如 `2026-07-27`

模板 A：错误切片只有一条，且只是开始日填错，优先直接改开始日

```sql
SELECT
		id,
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		is_include_in_daily_report,
		effective_start_date,
		effective_end_date,
		remark,
		updated_at
FROM dim_store_report_attr
WHERE store_code = '<STORE_CODE>'
ORDER BY effective_start_date, id;

UPDATE dim_store_report_attr 
SET effective_start_date = '<CORRECT_START_DATE>'
WHERE store_code = '<STORE_CODE>'
	AND effective_start_date = '<WRONG_START_DATE>'
	AND effective_end_date = '9999-12-31';

SELECT
		id,
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		is_include_in_daily_report,
		effective_start_date,
		effective_end_date,
		remark,
		updated_at
FROM dim_store_report_attr
WHERE store_code = '<STORE_CODE>'
ORDER BY effective_start_date, id;
```

模板 B：错误切片与更早正确切片并存，需要先确认是否重复，再决定删除还是回拨

```sql
SELECT
		id,
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		is_include_in_daily_report,
		effective_start_date,
		effective_end_date,
		remark,
		updated_at
FROM dim_store_report_attr
WHERE store_code = '<STORE_CODE>'
ORDER BY effective_start_date, id;

DELETE FROM dim_store_report_attr
WHERE store_code = '<STORE_CODE>'
	AND effective_start_date = '<WRONG_START_DATE>'
	AND effective_end_date = '9999-12-31';

SELECT
		id,
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		is_include_in_daily_report,
		effective_start_date,
		effective_end_date,
		remark,
		updated_at
FROM dim_store_report_attr
WHERE store_code = '<STORE_CODE>'
ORDER BY effective_start_date, id;
```

使用边界：
- 若现网只有错误切片，没有更早正确切片，优先用模板 A。
- 若现网已经同时存在一条更早正确切片和一条多余错误切片，优先评估模板 B。
- 若同一门店存在多条月内交错切片，先人工核对业务真值后再操作，不要直接套模板。

### 4.3 修复后的程序调度指令

场景 1：只需要验证总控是否不再阻塞

```bash
python scheduled_store_daily_report.py --conn-test
```

预期：
- 负责人快照导入不再报 `unexpected_entities`
- 脚本退出码为 `0`

场景 2：修复后执行门店日报专题正式调度

```bash
.\run_scheduled_store_daily_report.bat
```

说明：
- 该命令会重新走目标导入、负责人导入、免税月累计导入、受影响日期判断与 ADS 重跑。
- 若当前目标文件与日志幂等键相同，目标导入可能命中 SUCCESS 跳过；这不影响负责人链路验证是否解阻。

场景 3：若总控仍需补跑指定日期，仅重跑 ADS

```bash
.\run_scheduled_store_daily_report.bat --rerun-report-date <YYYY-MM-DD> --rerun-data-version <v1|v2>
```

说明：
- 显式 rerun 只重跑 ADS，不会重新导入 NAS。
- 因此它只能用于“维表和配置已修好后的结果层补跑”，不能替代前面的现网切片修复。

### 4.4 修复后最小验证清单

1. 维表验证

```sql
SELECT
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		is_include_in_daily_report,
		effective_start_date,
		effective_end_date,
		updated_at
FROM dim_store_report_attr
WHERE store_code = '<STORE_CODE>'
ORDER BY effective_start_date, id;
```

2. 负责人链路 dry-run 验证

```bash
python tools/import_store_operation_owner_from_nas.py --snapshot-date <YYYY-MM-DD> --preview-limit 0
```

3. 总控链路 conn-test 验证

```bash
python scheduled_store_daily_report.py --conn-test
```

4. 如需核对结果层，再补跑后检查日报结果表

```sql
SELECT
		report_date,
		data_version,
		store_id,
		store_code,
		store_name,
		report_channel_type,
		is_duty_free,
		updated_at
FROM ads_store_daily_report
WHERE report_date = '<YYYY-MM-DD>'
	AND data_version = '<v1|v2>'
	AND store_code = '<STORE_CODE>';
```

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.1 | 2026-07-05 | 新增“错误有效期切片残留”场景，补充现网查询 SQL、人工修复 SQL 模板与修复后的调度指令 |
| v1.0 | 2026-07-05 | 初版：沉淀门店渠道类型修正的标准闭环、最短命令组与常见误区 |