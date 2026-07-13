-- ODS layer tables (1:1 copy from Oracle)

CREATE TABLE IF NOT EXISTS ods_fa_storage (
    id BIGINT NOT NULL,
    c_store_id BIGINT NULL,
    m_product_id BIGINT NULL,
    m_productalias_id BIGINT NULL,
    qty DECIMAL(18,4) NULL,
    qtyvalid DECIMAL(18,4) NULL,
    qtypurchaserem DECIMAL(18,4) NULL,
    isactive CHAR(1) NULL,
    etl_batch_id VARCHAR(32) NOT NULL,
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ods_m_retail (
    id BIGINT NOT NULL,
    docno VARCHAR(40) NULL,
    billdate INT NULL,
    c_store_id BIGINT NULL,
    oms_sourcecode VARCHAR(512) NULL,
    tot_amt_actual DECIMAL(18,2) NULL,
    tot_amt_list DECIMAL(18,2) NULL,
    tot_qty DECIMAL(18,4) NULL,
    status INT NULL,
    isactive CHAR(1) NULL,
    modifieddate DATETIME NULL,
    etl_batch_id BIGINT NOT NULL DEFAULT 0,
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_ods_m_retail_id (id),
    KEY idx_ods_m_retail_oms_sourcecode (oms_sourcecode)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ods_m_retailitem (
    id BIGINT NOT NULL,
    m_retail_id BIGINT NULL,
    m_product_id BIGINT NULL,
    m_productalias_id BIGINT NULL,
    qty DECIMAL(18,4) NULL,
    pricelist DECIMAL(18,2) NULL,
    priceactual DECIMAL(18,2) NULL,
    tot_amt_actual DECIMAL(18,2) NULL,
    tot_amt_list DECIMAL(18,2) NULL,
    modifieddate DATETIME NULL,
    settime DATETIME NULL,
    etl_batch_id VARCHAR(32) NOT NULL,
    etl_loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_ods_m_retailitem_id (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ods_sync_state (
    table_name VARCHAR(64) NOT NULL,
    last_sync DATETIME NULL,
    current_window_start DATETIME NULL,
    current_window_end DATETIME NULL,
    status VARCHAR(20) NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    rows_written INT DEFAULT 0,
    PRIMARY KEY (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
