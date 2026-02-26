-- Incremental ODS migration helpers

-- Add modifieddate columns if missing
ALTER TABLE ods_m_retail ADD COLUMN modifieddate DATETIME NULL;
ALTER TABLE ods_m_retailitem ADD COLUMN modifieddate DATETIME NULL;
ALTER TABLE ods_m_retailitem ADD COLUMN settime DATETIME NULL;

-- Create sync state table
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

-- Add window state columns if missing
ALTER TABLE ods_sync_state ADD COLUMN current_window_start DATETIME NULL;
ALTER TABLE ods_sync_state ADD COLUMN current_window_end DATETIME NULL;
ALTER TABLE ods_sync_state ADD COLUMN status VARCHAR(20) NULL;

-- Optional indexes for incremental deletes
CREATE INDEX idx_ods_m_retail_modifieddate ON ods_m_retail (modifieddate);
CREATE INDEX idx_ods_m_retailitem_modifieddate ON ods_m_retailitem (modifieddate);
CREATE INDEX idx_ods_m_retailitem_settime ON ods_m_retailitem (settime);
