ALTER TABLE ads_store_daily_report
    ADD COLUMN same_store_mtd_sales_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '同店本期累计销售额' AFTER last_year_mtd_sales_amt,
    ADD COLUMN same_store_last_year_mtd_sales_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '同店去年同期累计销售额' AFTER same_store_mtd_sales_amt;