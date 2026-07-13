-- DWS Ovopark passenger flow daily draft DDL.
-- Status: 草案；未执行 DDL；仅用于万店掌 API 全链路分层设计。
-- Execution boundary: CREATE / ALTER / INDEX / INSERT / DELETE / 回填仍由用户人工执行；Agent 只提供草案与执行顺序。
-- Evidence:
--   SQL/draft_create_dwd_ovopark_passenger_flow_daily.sql
--   docs/万店掌API接入-子项目资料/万店掌API探测与接入方案.md

CREATE TABLE IF NOT EXISTS dws_ovopark_passenger_flow_daily (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    date_id INT NOT NULL COMMENT '业务日期，YYYYMMDD，来源 dwd_ovopark_passenger_flow_daily.date_id',
    store_id BIGINT NOT NULL COMMENT '何方门店ID',
    store_code VARCHAR(64) NOT NULL COMMENT '何方门店编码',
    store_name VARCHAR(255) NOT NULL COMMENT '何方门店名称',
    area_name VARCHAR(64) NULL COMMENT '何方大区',

    all_day_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '全天进客流',
    business_time_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '营业时间进客流',
    all_day_outside_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '全天店外客流',
    all_day_pass_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '全天过店客流',
    all_day_out_flow_count BIGINT NOT NULL DEFAULT 0 COMMENT '全天出店客流',
    all_day_dressing_passenger_flow BIGINT NOT NULL DEFAULT 0 COMMENT '全天试衣间客流',
    all_day_in_shop_rate DECIMAL(10,4) NULL COMMENT '全天进店率，按汇总分子分母重算',
    all_day_dressing_rate DECIMAL(10,4) NULL COMMENT '全天试衣率，按汇总分子分母重算',

    covered_dep_count INT NOT NULL DEFAULT 0 COMMENT '参与聚合的万店掌 dep_id 数量',
    source_dwd_row_count BIGINT NOT NULL DEFAULT 0 COMMENT '参与聚合的 DWD 行数',
    source_min_requested_at DATETIME NULL COMMENT '参与聚合源请求最早时间',
    source_max_requested_at DATETIME NULL COMMENT '参与聚合源请求最晚时间',
    load_batch_id VARCHAR(64) NULL COMMENT 'DWS 装载批次ID',
    validation_status VARCHAR(20) NOT NULL DEFAULT 'PENDING' COMMENT '校验状态：PENDING/PASSED/FAILED/WAIVED',
    validation_note VARCHAR(512) NULL COMMENT '校验说明',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DWS 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_dws_ovopark_pfd_daily (date_id, store_id),
    KEY idx_dws_ovopark_pfd_store (store_id, date_id),
    KEY idx_dws_ovopark_pfd_area (area_name, date_id),
    KEY idx_dws_ovopark_pfd_validation (validation_status, date_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWS 万店掌门店日聚合客流草案，来源 dwd_ovopark_passenger_flow_daily';