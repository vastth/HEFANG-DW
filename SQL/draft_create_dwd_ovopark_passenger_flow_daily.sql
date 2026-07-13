-- DWD Ovopark passenger flow daily draft DDL.
-- Status: 草案；未执行 DDL；仅用于万店掌 API 全链路分层设计。
-- Execution boundary: CREATE / ALTER / INDEX / INSERT / DELETE / 回填仍由用户人工执行；Agent 只提供草案与执行顺序。
-- Evidence:
--   SQL/draft_create_dim_ovopark_shop_mapping.sql
--   SQL/draft_create_ods_ovopark_tables.sql
--   reports/context_cache/ovopark_dim_store_initial_match_20260511.csv
--   docs/万店掌API接入-子项目资料/万店掌API探测与接入方案.md

CREATE TABLE IF NOT EXISTS dwd_ovopark_passenger_flow_daily (
    date_id INT NOT NULL COMMENT '业务日期，YYYYMMDD，来源 ods_ovopark_passenger_flow_daily.date_id',
    store_id BIGINT NOT NULL COMMENT '何方门店ID，来源 dim_ovopark_shop_mapping.hefang_store_id',
    store_code VARCHAR(64) NOT NULL COMMENT '何方门店编码，来源 dim_store.store_code / dim_ovopark_shop_mapping.hefang_store_code',
    store_name VARCHAR(255) NOT NULL COMMENT '何方门店名称，来源 dim_store.store_name / dim_ovopark_shop_mapping.hefang_store_name',
    area_name VARCHAR(64) NULL COMMENT '何方大区，来源 dim_store.area_name',

    ovopark_dep_id BIGINT NOT NULL COMMENT '万店掌内部门店ID，来源 ods_ovopark_passenger_flow_daily.dep_id',
    ovopark_dep_key VARCHAR(64) NOT NULL COMMENT '万店掌门店请求键，格式 S_<dep_id>',
    ovopark_shop_name VARCHAR(255) NULL COMMENT '万店掌门店名称，来源 ods_ovopark_shop.shop_name / ODS 客流表',
    ovopark_organize_id BIGINT NULL COMMENT '万店掌组织ID，来源 ods_ovopark_shop.organize_id',
    ovopark_organize_name VARCHAR(255) NULL COMMENT '万店掌组织名称，来源 ods_ovopark_shop.organize_name',
    mapping_status VARCHAR(32) NOT NULL COMMENT '当前映射状态快照，来源 dim_ovopark_shop_mapping.mapping_status',
    match_source VARCHAR(32) NULL COMMENT '当前映射来源，来源 dim_ovopark_shop_mapping.match_source',

    is_on_business_time INT NOT NULL DEFAULT 0 COMMENT '是否营业时间内，0=全天，1=营业时间内',
    passenger_flow INT NOT NULL DEFAULT 0 COMMENT '进客流，来源 ODS 日级客流',
    outside_passenger_flow INT NOT NULL DEFAULT 0 COMMENT '店外客流，来源 ODS 日级客流',
    pass_passenger_flow INT NOT NULL DEFAULT 0 COMMENT '过店客流，来源 ODS 日级客流',
    out_flow_count INT NOT NULL DEFAULT 0 COMMENT '出店客流，来源 ODS 日级客流',
    dressing_passenger_flow INT NOT NULL DEFAULT 0 COMMENT '试衣间客流，来源 ODS 日级客流',
    in_shop_rate DECIMAL(10,4) NULL COMMENT '进店率，来源 ODS 日级客流',
    dressing_rate DECIMAL(10,4) NULL COMMENT '试衣率，来源 ODS 日级客流',

    source_request_window_start DATETIME NOT NULL COMMENT '源请求窗口开始时间',
    source_request_window_end DATETIME NOT NULL COMMENT '源请求窗口结束时间',
    source_requested_at DATETIME NOT NULL COMMENT '源请求发起时间',
    source_response_stat_code INT NULL COMMENT '源接口状态码',
    source_ods_batch_id VARCHAR(64) NULL COMMENT '来源 ODS 批次ID',
    etl_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'DWD 装载时间',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (date_id, store_id, ovopark_dep_id, is_on_business_time),
    KEY idx_dwd_ovopark_pfd_store (store_id, date_id),
    KEY idx_dwd_ovopark_pfd_dep (ovopark_dep_id, date_id),
    KEY idx_dwd_ovopark_pfd_area (area_name, date_id),
    KEY idx_dwd_ovopark_pfd_watermark (source_requested_at, source_request_window_end)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DWD 万店掌门店日级客流事实草案，已将 dep_id 映射到何方门店';