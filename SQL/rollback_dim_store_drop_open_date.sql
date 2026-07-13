-- 用途：仅在代码已经回滚且用户确认不再保留开业日期时，人工删除 dim_store.open_date。
-- 默认建议：代码回滚时保留该可空列；旧版 pandas.to_sql 按列名插入，不会因额外可空列失败。
-- 风险：DROP COLUMN 会永久删除已同步的开业日期，执行前应先备份或确认可从 Oracle 重建。

ALTER TABLE dim_store
    DROP COLUMN open_date;