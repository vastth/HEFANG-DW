-- 将dim_sku中的color/size列重命名为sku_color/sku_size（避免Oracle保留字冲突）
ALTER TABLE dim_sku
    CHANGE COLUMN color sku_color VARCHAR(50) NULL COMMENT '颜色',
    CHANGE COLUMN size sku_size VARCHAR(50) NULL COMMENT '尺寸';
