ALTER TABLE ads_dabo_order_label
    ADD COLUMN canonical_system_order_id VARCHAR(512) NULL AFTER system_order_id,
    ADD COLUMN normalization_status VARCHAR(32) NOT NULL DEFAULT 'unreviewed' AFTER canonical_system_order_id,
    ADD COLUMN normalization_rule VARCHAR(64) NULL AFTER normalization_status,
    ADD COLUMN normalization_evidence TEXT NULL AFTER normalization_rule,
    ADD KEY idx_ads_dabo_order_label_canonical_system_order (canonical_system_order_id(255));