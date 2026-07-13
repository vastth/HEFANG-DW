-- DWS Ovopark passenger flow monthly draft DDL.
-- Status: 草案；未执行 DDL；仅用于万店掌 API 全链路分层设计。
-- Execution boundary: CREATE / ALTER / INDEX / INSERT / DELETE / 回填仍由用户人工执行；Agent 只提供草案与执行顺序。
-- Evidence:
--   SQL/draft_create_dws_ovopark_passenger_flow_daily.sql

CREATE TABLE IF NOT EXISTS dws_ovopark_passenger_flow_monthly (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    report_date DATE NOT NULL COMMENT '报表观察日，通常为跑批日或目标自然日',
    target_year SMALLINT NOT NULL COMMENT '目标年份',
    target_month TINYINT NOT NULL COMMENT '目标月份',
    store_id BIGINT NOT NULL COMMENT '何方门店ID',
    store_code VARCHAR(64) NOT NULL COMMENT '何方门店编码',
    store_name VARCHAR(255) NOT NULL COMMENT '何方门店名称',
    area_name VARCHAR(64) NULL COMMENT '何方大区',

    month_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计进客流',
    month_business_time_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计营业时间进客流',
    month_outside_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计店外客流',
    month_pass_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计过店客流',
    month_out_flow_count BIGINT NOT NULL DEFAULT 0 COMMENT '月累计出店客流',
    month_dressing_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '月累计试衣间客流',
    month_avg_daily_passenger_flow DECIMAL(18,2) NULL COMMENT '月均日客流，按有数天数计算',
    month_in_shop_rate DECIMAL(10,4) NULL COMMENT '月进店率，按汇总分子分母重算',
    month_dressing_rate DECIMAL(10,4) NULL COMMENT '月试衣率，按汇总分子分母重算',
    days_with_data INT NOT NULL DEFAULT 0 COMMENT '月内有数据的自然日数',
    calendar_day_count INT NOT NULL DEFAULT 0 COMMENT '目标月日历天数',
    data_coverage_rate DECIMAL(10,4) NULL COMMENT '覆盖率 = 有数据天数 / 日历天数',

    source_dws_day_row_count BIGINT NOT NULL DEFAULT 0 COMMENT '参与聚合的 DWS 日表行数',
    data_version VARCHAR(32) NOT NULL DEFAULT 'v1' COMMENT '版本号',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DWS 月表装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_dws_ovopark_pfd_monthly (report_date, data_version, target_year, target_month, store_id),
    KEY idx_dws_ovopark_pfm_store (store_id, target_year, target_month),
    KEY idx_dws_ovopark_pfm_area (area_name, target_year, target_month),
    KEY idx_dws_ovopark_pfm_report (report_date, data_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS 万店掌门店月聚合客流草案，来源 dws_ovopark_passenger_flow_daily';