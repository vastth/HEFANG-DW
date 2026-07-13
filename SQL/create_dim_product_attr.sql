CREATE TABLE IF NOT EXISTS dim_product_attr (
    product_id BIGINT NULL COMMENT '商品ID(dim_product.product_id)',
    color TEXT COMMENT '颜色',
    size TEXT COMMENT '尺寸',
    KEY idx_dim_product_attr_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品属性表（颜色/尺寸，取每个货号的第一个SKU）';