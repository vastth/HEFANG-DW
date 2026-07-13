CREATE TABLE IF NOT EXISTS ads_daily_sales (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    report_date DATE NOT NULL COMMENT '报告日期',
    battle_month DATE NOT NULL COMMENT '战役月份首日',
    sales_date DATE NOT NULL COMMENT '销售日期',
    area_name VARCHAR(50) NOT NULL COMMENT '战区名称；不再生成全国汇总行',
    report_channel_type VARCHAR(32) NOT NULL COMMENT '经营渠道细分类；不再生成全部汇总行',
    day_target_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '当日节奏目标',
    day_actual_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '当日实际',
    cum_target_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '月累计目标',
    cum_actual_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '月累计实际',
    last_year_cum_actual_amt DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '去年同期累计实际',
    data_version VARCHAR(32) NOT NULL DEFAULT 'v1' COMMENT '数据版本号',
    etl_time DATETIME NOT NULL COMMENT 'ETL时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    UNIQUE KEY uk_ads_daily_sales (
        report_date,
        data_version,
        battle_month,
        sales_date,
        area_name,
        report_channel_type
    ),
    KEY idx_ads_daily_sales_battle (battle_month, sales_date, data_version),
    KEY idx_ads_daily_sales_org (report_date, area_name, report_channel_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='销售看板月度战役日节奏表（已接专题调度，未接run_etl主链）';