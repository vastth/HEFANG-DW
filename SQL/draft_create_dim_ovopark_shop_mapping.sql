-- Ovopark shop mapping draft DDL.
-- Status: 草案；未执行 DDL；仅用于按 depId / S_门店id 收口接入设计。
-- Execution boundary: CREATE / ALTER / 索引创建 / 写库 / 回填仍由用户人工执行；Agent 只提供草案与执行顺序。
-- Evidence:
--   docs/万店掌API接入-子项目资料/万店掌API探测与接入方案.md
--   docs/万店掌API接入-子项目资料/万店掌API续接上下文.md
--   docs/AGENT_HANDOFF.md

CREATE TABLE IF NOT EXISTS dim_ovopark_shop_mapping (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
    hefang_store_id BIGINT NULL COMMENT '何方门店ID，来源 dim_store.store_id',
    hefang_store_code VARCHAR(64) NULL COMMENT '何方门店编码，来源 dim_store.store_code；PENDING 时允许为空',
    hefang_store_name VARCHAR(255) NULL COMMENT '何方门店名称，来源 dim_store.store_name；PENDING 时允许为空',

    ovopark_dep_id BIGINT NOT NULL COMMENT '万店掌内部门店ID，来源 getDepartments.data.rows[].id',
    ovopark_dep_key VARCHAR(64) NOT NULL COMMENT '万店掌门店请求键，格式 S_<ovopark_dep_id>',
    ovopark_shop_name VARCHAR(255) NOT NULL COMMENT '万店掌门店名称，来源 getDepartments.data.rows[].name',
    ovopark_organize_id BIGINT NULL COMMENT '万店掌组织ID，来源 getDepartments.data.rows[].organizeId',
    ovopark_organize_name VARCHAR(255) NULL COMMENT '万店掌组织名称，来源 getDepartments.data.rows[].organizeName',
    ovopark_dep_organize_id BIGINT NULL COMMENT '万店掌门店归属组织节点ID，来源 getDepartments.data.rows[].depOrganizeId',
    ovopark_shop_id VARCHAR(128) NULL COMMENT '第三方店铺ID，来源 getDepartments.data.rows[].shopId；当前样本为空',
    ovopark_trilateral_id VARCHAR(128) NULL COMMENT '第三方门店编码，来源 getDepartments.data.rows[].trilateralId；当前样本为空',

    mapping_status VARCHAR(32) NOT NULL DEFAULT 'PENDING' COMMENT '映射状态：PENDING/CANDIDATE/MATCHED/IGNORED/DISABLED',
    match_source VARCHAR(32) NULL COMMENT '映射来源：MANUAL/AUTO_EXACT_NAME/AUTO_EXACT_NAME_AREA/AUTO_ADDRESS/SHOP_ID/DEP_ID',
    effective_start_date DATE NOT NULL COMMENT '映射生效开始日',
    effective_end_date DATE NOT NULL DEFAULT '2099-12-31' COMMENT '映射生效结束日',
    is_current CHAR(1) NOT NULL DEFAULT 'Y' COMMENT '是否当前有效（Y/N）',
    current_hefang_guard TINYINT GENERATED ALWAYS AS (CASE WHEN is_current = 'Y' THEN 1 ELSE NULL END) STORED COMMENT '何方门店当前行唯一性保护辅助列',
    current_ovopark_guard TINYINT GENERATED ALWAYS AS (CASE WHEN is_current = 'Y' THEN 1 ELSE NULL END) STORED COMMENT '万店掌门店当前行唯一性保护辅助列',
    confirmed_by VARCHAR(64) NULL COMMENT '确认人',
    confirmed_at DATETIME NULL COMMENT '确认时间',
    notes VARCHAR(1000) NULL COMMENT '备注',

    etl_batch_id VARCHAR(64) NULL COMMENT '最近一次同步批次ID',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    PRIMARY KEY (id),
    UNIQUE KEY uk_dim_ovopark_shop_mapping (hefang_store_code, ovopark_dep_id, effective_start_date),
    UNIQUE KEY uk_dim_ovopark_shop_mapping_dep_key (ovopark_dep_key, effective_start_date),
    UNIQUE KEY uk_dim_ovopark_shop_mapping_current_hefang (hefang_store_code, current_hefang_guard),
    UNIQUE KEY uk_dim_ovopark_shop_mapping_current_dep (ovopark_dep_id, current_ovopark_guard),
    KEY idx_dim_ovopark_shop_mapping_current (hefang_store_code, is_current, effective_start_date, effective_end_date),
    KEY idx_dim_ovopark_shop_mapping_hefang_id (hefang_store_id, is_current),
    KEY idx_dim_ovopark_shop_mapping_dep_id (ovopark_dep_id, is_current),
    KEY idx_dim_ovopark_shop_mapping_shop_id (ovopark_shop_id),
    KEY idx_dim_ovopark_shop_mapping_trilateral_id (ovopark_trilateral_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='何方门店与万店掌门店映射维表（SCD2草案）';