-- SKU维度表（来自Oracle M_PRODUCT_ALIAS + M_ATTRIBUTESETINSTANCE）
CREATE TABLE IF NOT EXISTS dim_sku (
    sku_id BIGINT PRIMARY KEY COMMENT 'SKU主键(M_PRODUCT_ALIAS.ID)',
    sku_barcode VARCHAR(80) COMMENT '条码(M_PRODUCT_ALIAS.NO)',
    product_id BIGINT COMMENT '货号ID(M_PRODUCT.ID)',
    sku_color VARCHAR(50) COMMENT '颜色',
    sku_size VARCHAR(50) COMMENT '尺寸',
    is_active CHAR(1) DEFAULT 'Y' COMMENT '是否有效',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_product_id (product_id),
    INDEX idx_barcode (sku_barcode)
) COMMENT 'SKU维度表';
