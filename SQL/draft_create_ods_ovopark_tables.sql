-- Ovopark ODS draft DDL.
-- Status: 草案；未执行 DDL；仅用于按 depId / S_门店id 收口万店掌接入设计。
-- Execution boundary: CREATE / ALTER / 索引创建 / 写库 / 回填仍由用户人工执行；Agent 只提供草案与执行顺序。
-- Security boundary: 不在 ODS 中持久化 authenticator / Ovo-Authorization 原文；请求侧只保留脱敏参数和请求关键键值。
-- Evidence:
--   docs/万店掌API接入-子项目资料/万店掌API探测与接入方案.md
--   docs/万店掌API接入-子项目资料/万店掌API续接上下文.md
--   docs/AGENT_HANDOFF.md

CREATE TABLE IF NOT EXISTS ods_ovopark_api_raw (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    api_name VARCHAR(128) NOT NULL COMMENT '接口方法名，例如 open.organize.departments.getDepartments',
    request_method VARCHAR(10) NOT NULL COMMENT 'HTTP方法，GET/POST',
    request_route VARCHAR(255) NOT NULL DEFAULT 'https://cloudapi.ovopark.com/cloud.api' COMMENT '请求路由',
    request_object_type VARCHAR(32) NULL COMMENT '请求对象类型：DEP_ID/DEP_KEY/ORG_KEY/PAGE',
    request_object_key VARCHAR(128) NULL COMMENT '请求对象键值，例如 174679、S_174679、O_65446',
    request_shop_id VARCHAR(128) NULL COMMENT '第三方店铺ID入参；仅在接口支持时使用',
    request_page_number INT NULL COMMENT '分页页码',
    request_page_size INT NULL COMMENT '分页大小',
    request_time_type INT NULL COMMENT '时间类型，来源 timeType',
    request_start_hour INT NULL COMMENT '开始小时，来源 starthour',
    request_end_hour INT NULL COMMENT '结束小时，来源 endhour',
    request_is_on_business_time INT NULL COMMENT '是否营业时间内，来源 isOnBusinessTime',
    request_window_start DATETIME NULL COMMENT '请求窗口开始时间',
    request_window_end DATETIME NULL COMMENT '请求窗口结束时间',
    request_param_json LONGTEXT NULL COMMENT '脱敏后的请求参数JSON，不包含 authenticator / Ovo-Authorization',

    response_stat_code INT NULL COMMENT '接口 stat.code',
    response_codename VARCHAR(64) NULL COMMENT '接口 stat.codename',
    response_result VARCHAR(64) NULL COMMENT '接口 result',
    response_total INT NULL COMMENT '列表型接口 total',
    response_row_count INT NULL COMMENT '列表或明细条数',
    gateway_request_id VARCHAR(64) NULL COMMENT '网关 requestId',
    response_json LONGTEXT NOT NULL COMMENT '完整响应JSON',

    requested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '请求发起时间',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ETL批次ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS落地时间',

    PRIMARY KEY (id),
    KEY idx_ods_ovopark_api_raw_api_time (api_name, requested_at),
    KEY idx_ods_ovopark_api_raw_object (request_object_type, request_object_key, requested_at),
    KEY idx_ods_ovopark_api_raw_window (request_window_start, request_window_end),
    KEY idx_ods_ovopark_api_raw_batch (etl_batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='万店掌API原始响应表草案；仅保留脱敏请求参数与完整响应JSON';


CREATE TABLE IF NOT EXISTS ods_ovopark_shop (
    dep_id BIGINT NOT NULL COMMENT '万店掌内部门店ID，来源 getDepartments.data.rows[].id',
    dep_key VARCHAR(64) NOT NULL COMMENT '万店掌门店请求键，格式 S_<dep_id>',
    shop_name VARCHAR(255) NOT NULL COMMENT '门店名称，来源 getDepartments.data.rows[].name',
    address VARCHAR(500) NULL COMMENT '门店地址，来源 getDepartments.data.rows[].address',
    organize_id BIGINT NULL COMMENT '组织ID，来源 getDepartments.data.rows[].organizeId',
    organize_name VARCHAR(255) NULL COMMENT '组织名称，来源 getDepartments.data.rows[].organizeName',
    dep_organize_id BIGINT NULL COMMENT '门店归属组织节点ID，来源 getDepartments.data.rows[].depOrganizeId',
    group_id BIGINT NULL COMMENT '企业组ID，来源 getDepartments.data.rows[].groupId',
    shop_id VARCHAR(128) NULL COMMENT '第三方店铺ID，来源 getDepartments.data.rows[].shopId；当前样本为空',
    trilateral_id VARCHAR(128) NULL COMMENT '第三方门店编码，来源 getDepartments.data.rows[].trilateralId；当前样本为空',
    country_code VARCHAR(32) NULL COMMENT '国家编码，来源 getDepartments.data.rows[].countryCode',
    location_code VARCHAR(64) NULL COMMENT '行政区划编码，来源 getDepartments.data.rows[].location',
    longitude DECIMAL(12,6) NULL COMMENT '经度，来源 getDepartments.data.rows[].longitude',
    latitude DECIMAL(12,6) NULL COMMENT '纬度，来源 getDepartments.data.rows[].latitude',
    open_status INT NULL COMMENT '开业状态，来源 getDepartments.data.rows[].openStatus',
    validate_status INT NULL COMMENT '到期状态，来源 getDepartments.data.rows[].validateStatus',
    validate_date DATETIME NULL COMMENT '服务到期时间，来源 getDepartments.data.rows[].validateDate',
    close_time DATETIME NULL COMMENT '闭店时间，来源 getDepartments.data.rows[].closeTime',
    create_time DATETIME NULL COMMENT '门店创建时间，来源 getDepartments.data.rows[].createTime',
    device_register_time DATETIME NULL COMMENT '设备注册时间，来源 getDepartments.data.rows[].deviceRegisterTime',
    ipc_current_count INT NULL COMMENT '当前IPC数量，来源 getDepartments.data.rows[].ipcCurrentCount',
    ipc_count_limit INT NULL COMMENT 'IPC上限，来源 getDepartments.data.rows[].ipcCountLimit',
    dev_count INT NULL COMMENT '设备数量，来源 getDepartments.data.rows[].devCount',
    has_pc INT NULL COMMENT '是否有PC，来源 getDepartments.data.rows[].hasPc',
    is_complete_config INT NULL COMMENT '是否配置完成，来源 getDepartments.data.rows[].isCompleteconfig',
    service_permission VARCHAR(32) NULL COMMENT '服务权限标识，来源 getDepartments.data.rows[].servicePermission',

    source_request_at DATETIME NULL COMMENT '本次门店快照请求时间',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ETL批次ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS落地时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (dep_id),
    UNIQUE KEY uk_ods_ovopark_shop_dep_key (dep_key),
    KEY idx_ods_ovopark_shop_org (organize_id, open_status),
    KEY idx_ods_ovopark_shop_dep_org (dep_organize_id),
    KEY idx_ods_ovopark_shop_shop_id (shop_id),
    KEY idx_ods_ovopark_shop_trilateral_id (trilateral_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='万店掌门店当前快照表草案，主接入键为 dep_id / S_门店id';


CREATE TABLE IF NOT EXISTS ods_ovopark_passenger_flow_daily (
    date_id INT NOT NULL COMMENT '业务日期，YYYYMMDD，按请求窗口开始日归属',
    dep_id BIGINT NOT NULL COMMENT '万店掌内部门店ID，来源请求参数 depId',
    dep_key VARCHAR(64) NOT NULL COMMENT '万店掌门店请求键，格式 S_<dep_id>',
    shop_id VARCHAR(128) NULL COMMENT '第三方店铺ID，来源门店映射或门店快照；当前样本多为空',
    shop_name VARCHAR(255) NULL COMMENT '门店名称，建议由 ods_ovopark_shop 或 dim_ovopark_shop_mapping 补齐',
    request_window_start DATETIME NOT NULL COMMENT '请求窗口开始时间',
    request_window_end DATETIME NOT NULL COMMENT '请求窗口结束时间',
    is_on_business_time INT NOT NULL DEFAULT 0 COMMENT '是否营业时间内，0=全部，1=营业时间内',

    passenger_flow INT NULL COMMENT '进客流，来源 passengerFlow',
    outside_passenger_flow INT NULL COMMENT '店外客流，来源 outsidePassengerFlow',
    in_shop_rate DECIMAL(10,4) NULL COMMENT '进店率，来源 inShopRate',
    out_flow_count INT NULL COMMENT '出店客流，来源 outFlowCount',
    dressing_rate DECIMAL(10,4) NULL COMMENT '试衣率，来源 dressingRate',
    pass_passenger_flow INT NULL COMMENT '过店客流，来源 passPassengerFlow',
    dressing_passenger_flow INT NULL COMMENT '试衣间客流，来源 dressingPassengerFlow',

    response_stat_code INT NULL COMMENT '接口状态码，来源 stat.code',
    requested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '请求发起时间',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ETL批次ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS落地时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (date_id, dep_id, is_on_business_time),
    KEY idx_ods_ovopark_pfd_dep_key (dep_key, date_id),
    KEY idx_ods_ovopark_pfd_shop_id (shop_id, date_id),
    KEY idx_ods_ovopark_pfd_batch (etl_batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='万店掌单门店日级客流指标表草案，标准接入键为 dep_id';


CREATE TABLE IF NOT EXISTS ods_ovopark_passenger_flow_hourly (
    biz_date_id INT NOT NULL COMMENT '业务日期，YYYYMMDD，按请求 startTime 所在日期归属',
    stat_time DATETIME NOT NULL COMMENT '接口返回时间点，来源 data[].dataList[].time',
    dep_id BIGINT NOT NULL COMMENT '万店掌内部门店ID，来源响应 data[].depId',
    dep_key VARCHAR(64) NOT NULL COMMENT '万店掌门店请求键，格式 S_<dep_id>',
    request_object_key VARCHAR(64) NOT NULL COMMENT '请求根对象键，门店用 S_<id>，组织用 O_<id>',
    request_object_type VARCHAR(16) NOT NULL COMMENT '请求根对象类型：STORE/ORG',
    shop_id VARCHAR(128) NULL COMMENT '第三方店铺ID，来源响应 data[].shopId；当前样本多为空',
    shop_name VARCHAR(255) NULL COMMENT '门店名称，来源响应 data[].name',
    time_type INT NOT NULL COMMENT '时间类型，当前小时粒度固定为 1',
    start_hour INT NULL COMMENT '请求开始小时，来源 starthour',
    end_hour INT NULL COMMENT '请求结束小时，来源 endhour',
    is_on_business_time INT NOT NULL DEFAULT 0 COMMENT '是否营业时间内，来源 isOnBusinessTime；默认 0=全天',

    passenger_flow INT NULL COMMENT '进客流，来源 passengerFlow',
    pass_passenger_flow INT NULL COMMENT '过店客流，来源 passPassengerFlow',
    in_count_having_pass_device INT NULL COMMENT '有过店设备的进店客流，来源 inCountHavingPassDevice',
    outside_passenger_flow INT NULL COMMENT '店外客流，来源 outSidePassengerFlow',
    in_shop_rate DECIMAL(10,4) NULL COMMENT '进店率，来源 inShopRate',
    out_flow_count INT NULL COMMENT '出店客流，来源 outFlowCount',
    dressing_rate DECIMAL(10,4) NULL COMMENT '试衣率，来源 dressingRate',
    duplicated_flow INT NULL COMMENT '非去重客流，来源 duplicatedFlow',

    response_stat_code INT NULL COMMENT '接口状态码，来源 stat.code',
    requested_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '请求发起时间',
    etl_batch_id VARCHAR(64) NOT NULL COMMENT 'ETL批次ID',
    etl_loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ODS落地时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (biz_date_id, stat_time, dep_id, time_type, request_object_type, request_object_key, is_on_business_time),
    KEY idx_ods_ovopark_pfh_dep_key (dep_key, stat_time),
    KEY idx_ods_ovopark_pfh_request_object (request_object_key, stat_time),
    KEY idx_ods_ovopark_pfh_shop_id (shop_id, stat_time),
    KEY idx_ods_ovopark_pfh_batch (etl_batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='万店掌小时级客流指标表草案，当前可靠路径为 S_门店id 或 O_组织id';