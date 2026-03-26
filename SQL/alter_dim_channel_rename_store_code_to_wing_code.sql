ALTER TABLE dim_channel
    CHANGE COLUMN store_code WING_CODE VARCHAR(40) NULL COMMENT '渠道挂接码(O2O_RETAIL_CHANNEL.WING_CODE，保留Oracle原值)';

ALTER TABLE dim_channel
    DROP INDEX idx_store_code,
    ADD INDEX idx_wing_code (WING_CODE);