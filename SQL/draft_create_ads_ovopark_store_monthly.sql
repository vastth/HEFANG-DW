-- ADS Ovopark store monthly draft DDL.
-- Status: 草案；未执行 DDL；仅用于万店掌 API 全链路分层设计。
-- Execution boundary: CREATE / ALTER / INDEX / INSERT / DELETE / 回填仍由用户人工执行；Agent 只提供草案与执行顺序。
-- Evidence:
--   SQL/draft_create_dws_ovopark_passenger_flow_monthly.sql
--   etl_ads_store_daily_report.py

CREATE TABLE IF NOT EXISTS ads_ovopark_store_monthly (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    report_date DATE NOT NULL COMMENT '报表观察日',
    target_year SMALLINT NOT NULL COMMENT '目标年份',
    target_month TINYINT NOT NULL COMMENT '目标月份',
    store_id BIGINT NOT NULL COMMENT '何方门店ID',
    store_code VARCHAR(64) NOT NULL COMMENT '何方门店编码',
    store_name VARCHAR(255) NOT NULL COMMENT '何方门店名称',
    owner_name VARCHAR(255) NULL COMMENT '门店负责人，来源 dim_store_operation_owner_assignment',
    area_name VARCHAR(64) NULL COMMENT '何方大区',
    report_channel_type VARCHAR(64) NULL COMMENT '日报渠道类型，来源 dim_store_report_attr.report_channel_type',
    store_grade VARCHAR(64) NULL COMMENT '门店等级，来源 dim_store_report_attr.store_grade',
    is_duty_free CHAR(1) NULL COMMENT '是否免税店，来源 dim_store_report_attr.is_duty_free',

    ovopark_dep_id BIGINT NULL COMMENT '万店掌内部门店ID，来源 dim_ovopark_shop_mapping.ovopark_dep_id',
    ovopark_dep_key VARCHAR(64) NULL COMMENT '万店掌门店请求键，来源 dim_ovopark_shop_mapping.ovopark_dep_key',
    month_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计进客流',
    month_business_time_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计营业时间进客流',
    month_outside_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计店外客流',
    month_pass_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计过店客流',
    month_out_flow_count BIGINT NOT NULL DEFAULT 0 COMMENT '月累计出店客流',
    month_dressing_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计试衣间客流',
    month_avg_daily_passenger_flow DECIMAL(18,2) NULL COMMENT '月均日客流',
    month_in_shop_rate DECIMAL(10,4) NULL COMMENT '月进店率',
    month_dressing_rate DECIMAL(10,4) NULL COMMENT '月试衣率',
    days_with_data INT NOT NULL DEFAULT 0 COMMENT '月内有数据天数',
    calendar_day_count INT NOT NULL DEFAULT 0 COMMENT '目标月日历天数',
    data_coverage_rate DECIMAL(10,4) NULL COMMENT '覆盖率 = 有数据天数 / 日历天数',
    data_version VARCHAR(32) NOT NULL DEFAULT 'v1' COMMENT '版本号',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ADS 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_ads_ovopark_store_monthly (report_date, data_version, target_year, target_month, store_id),
    KEY idx_ads_ovopark_store_monthly_area (area_name, target_year, target_month),
    KEY idx_ads_ovopark_store_monthly_owner (owner_name, target_year, target_month),
    KEY idx_ads_ovopark_store_monthly_channel (report_channel_type, target_year, target_month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS 万店掌门店月客流宽表草案，服务经营分析与 Tableau 月报';