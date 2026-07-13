ALTER TABLE ods_m_retail
    ADD COLUMN oms_sourcecode VARCHAR(512) NULL AFTER c_store_id;

CREATE INDEX idx_ods_m_retail_oms_sourcecode ON ods_m_retail (oms_sourcecode);