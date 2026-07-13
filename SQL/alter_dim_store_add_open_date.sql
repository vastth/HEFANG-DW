-- 用途：为 dim_store 增加门店开业日期。
-- 来源：Oracle BOSNDS3.C_STORE.OPENDATE（NUMBER(10)，YYYYMMDD）。
-- 执行边界：仅由用户人工执行；执行前先运行 SQL/check_dim_store_open_date.sql。
-- 风险：当前 dim_store 仅约 231 行，数据改写量低；ALTER 仍可能等待元数据锁，建议避开 ETL 运行窗口。

ALTER TABLE dim_store
    ADD COLUMN open_date DATE NULL COMMENT '门店开业日期，来源 BOSNDS3.C_STORE.OPENDATE'
    AFTER is_active;