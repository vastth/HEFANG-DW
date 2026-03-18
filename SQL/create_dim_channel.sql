-- 电商渠道维度表（来自 Oracle O2O_RETAIL_CHANNEL）
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id INT PRIMARY KEY COMMENT '渠道ID(O2O_RETAIL_CHANNEL.ID)',
    channel_name VARCHAR(50) NOT NULL COMMENT '渠道名称(O2O_RETAIL_CHANNEL.NAME)',
    channel_code VARCHAR(20) COMMENT '渠道编码(O2O_RETAIL_CHANNEL.CODE)',
    WING_CODE VARCHAR(40) COMMENT '对应店仓编码(O2O_RETAIL_CHANNEL.WING_CODE)',
    is_main TINYINT DEFAULT 0 COMMENT '是否主要渠道(1=是,0=否)',
    platform_type VARCHAR(20) COMMENT '平台类型(按渠道名称派生)',
    is_active CHAR(1) DEFAULT 'Y' COMMENT '是否有效(O2O_RETAIL_CHANNEL.ISACTIVE)',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    INDEX idx_channel_code (channel_code),
    INDEX idx_wing_code (WING_CODE)
) COMMENT '电商渠道维度表';