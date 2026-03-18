ALTER TABLE dim_channel
    CHANGE COLUMN store_code WING_CODE VARCHAR(40) NULL COMMENT '对应店仓编码(O2O_RETAIL_CHANNEL.WING_CODE)';

ALTER TABLE dim_channel
    DROP INDEX idx_store_code,
    ADD INDEX idx_wing_code (WING_CODE);