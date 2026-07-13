-- M3 raw / DWD table comment correction SQL.
-- Status: 已由用户人工执行；当前 information_schema 表注释已不含“草案 / 未执行”旧字样。
-- Boundary: 本文件仅作已执行 SQL 留档；Agent 未代执行 ALTER。
-- Context: 用户已人工完成 5 张 M3 raw / DWD 表建表，并已人工执行本文件修正线上表注释。
-- Evidence: reports/context_cache/m3_manual_ddl_verification_20260430.json
-- Timeout / lock risk: 仅修改表注释，通常为元数据操作；仍可能等待 metadata lock。建议业务低峰执行，执行前确认无长事务占用这些表，单表逐条执行并观察耗时。

ALTER TABLE ods_m_retail_raw COMMENT = 'ODS raw 零售单头，来源 Oracle M_RETAIL，M3 已人工建空表';
ALTER TABLE ods_m_retailitem_raw COMMENT = 'ODS raw 零售明细，来源 Oracle M_RETAILITEM，M3 已人工建空表';
ALTER TABLE ods_fa_storage_raw COMMENT = 'ODS raw 库存余额，来源 Oracle FA_STORAGE，M3 已人工建空表';
ALTER TABLE dwd_sales_retail_item COMMENT = 'DWD 销售零售明细事实，M3 已人工建空表，未接调度';
ALTER TABLE dwd_inventory_storage_snapshot COMMENT = 'DWD 库存店仓快照事实，M3 已人工建空表，未接调度';
