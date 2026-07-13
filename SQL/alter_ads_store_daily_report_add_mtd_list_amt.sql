-- 为销售日报 Tableau 明细表“月折扣率”总计提供聚合分母。
-- 执行边界：由用户在目标 MySQL 人工执行；Agent 不直接落库。
-- 超时/锁风险：单列 ADD COLUMN，建议在门店专题调度空窗执行；执行前确认无 ads_store_daily_report 写入事务。

ALTER TABLE ads_store_daily_report
  ADD COLUMN mtd_list_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '月累计吊牌金额，月折扣率分母' AFTER mtd_sales_amt;