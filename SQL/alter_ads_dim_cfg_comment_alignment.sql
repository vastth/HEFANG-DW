ALTER TABLE ads_dabo_daily_sales
  COMMENT = '达播日销售兼容汇总表（标签批次不可用时供 ads_inventory_health 回退）',
  MODIFY COLUMN sale_date DATE NOT NULL COMMENT '发货日期',
  MODIFY COLUMN product_alias_code VARCHAR(80) NOT NULL COMMENT 'SKU条码',
  MODIFY COLUMN dabo_sales_qty INT NOT NULL DEFAULT 0 COMMENT '达播销量',
  MODIFY COLUMN dabo_order_count INT NOT NULL DEFAULT 0 COMMENT '达播订单数',
  MODIFY COLUMN dabo_revenue DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '达播实收金额',
  MODIFY COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE ads_dabo_order_bridge
  COMMENT = '达播订单明细桥接表（原始 CSV 明细）',
  MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  MODIFY COLUMN platform_code VARCHAR(32) NOT NULL COMMENT '平台代码',
  MODIFY COLUMN platform_name VARCHAR(64) NOT NULL COMMENT '平台名称',
  MODIFY COLUMN main_order_id VARCHAR(64) NOT NULL COMMENT '主订单编号',
  MODIFY COLUMN sub_order_id VARCHAR(64) NOT NULL COMMENT '子订单编号',
  MODIFY COLUMN sale_date DATE NOT NULL COMMENT '发货日期',
  MODIFY COLUMN product_alias_code VARCHAR(80) NOT NULL COMMENT 'SKU条码',
  MODIFY COLUMN qty INT NOT NULL DEFAULT 0 COMMENT '销量',
  MODIFY COLUMN revenue_csv DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT 'CSV实收金额',
  MODIFY COLUMN order_status VARCHAR(32) NOT NULL COMMENT '订单状态',
  MODIFY COLUMN influencer_id VARCHAR(64) NULL COMMENT '主播ID',
  MODIFY COLUMN influencer_name VARCHAR(128) NULL COMMENT '主播名称',
  MODIFY COLUMN ad_channel VARCHAR(128) NULL COMMENT '广告渠道',
  MODIFY COLUMN traffic_channel VARCHAR(128) NULL COMMENT '流量渠道',
  MODIFY COLUMN source_file VARCHAR(255) NOT NULL COMMENT '来源文件名',
  MODIFY COLUMN source_file_date DATE NULL COMMENT '来源文件日期',
  MODIFY COLUMN import_batch_id VARCHAR(64) NOT NULL COMMENT '导入批次ID',
  MODIFY COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE ads_dabo_order_label
  COMMENT = '达播订单标签表（统一 Excel 标签主线）',
  MODIFY COLUMN source_file VARCHAR(255) NOT NULL COMMENT '来源 Excel 文件名',
  MODIFY COLUMN source_sheet VARCHAR(128) NOT NULL COMMENT '来源工作表',
  MODIFY COLUMN source_file_mtime DATETIME NOT NULL COMMENT '来源文件修改时间',
  MODIFY COLUMN first_source_row_number INT NOT NULL COMMENT '首个来源行号',
  MODIFY COLUMN source_row_count INT NOT NULL DEFAULT 0 COMMENT '同 system_order_id 命中的来源行数',
  MODIFY COLUMN system_order_id VARCHAR(512) NOT NULL COMMENT '原始系统单号',
  MODIFY COLUMN canonical_system_order_id VARCHAR(512) NULL COMMENT '归一后的优先桥接键',
  MODIFY COLUMN normalization_status VARCHAR(32) NOT NULL DEFAULT 'unreviewed' COMMENT '归一状态',
  MODIFY COLUMN normalization_rule VARCHAR(64) NULL COMMENT '归一规则名',
  MODIFY COLUMN normalization_evidence TEXT NULL COMMENT '归一证据 JSON',
  MODIFY COLUMN platform_order_id VARCHAR(128) NULL COMMENT '平台单号',
  MODIFY COLUMN is_dabo_order TINYINT NOT NULL DEFAULT 1 COMMENT '是否达播订单',
  MODIFY COLUMN dabo_source VARCHAR(64) NOT NULL DEFAULT 'yunque_order_management' COMMENT '标签来源',
  MODIFY COLUMN dabo_channel_code VARCHAR(32) NOT NULL COMMENT '达播渠道代码',
  MODIFY COLUMN dabo_channel_name VARCHAR(64) NOT NULL COMMENT '达播渠道名称',
  MODIFY COLUMN influencer_id VARCHAR(128) NULL COMMENT '主播ID',
  MODIFY COLUMN influencer_name VARCHAR(128) NOT NULL COMMENT '主播名称',
  MODIFY COLUMN order_status VARCHAR(64) NOT NULL COMMENT '订单状态',
  MODIFY COLUMN platform_ship_time VARCHAR(32) NULL COMMENT '平台发货时间原始文本',
  MODIFY COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  MODIFY COLUMN updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间';

ALTER TABLE ads_dabo_order_retail_bridge
  COMMENT = '达播订单到零售单头桥接缓存表',
  MODIFY COLUMN source_file VARCHAR(255) NOT NULL COMMENT '达播样本文件名',
  MODIFY COLUMN main_order_id VARCHAR(512) NOT NULL COMMENT '达播主订单编号',
  MODIFY COLUMN retail_id BIGINT NOT NULL COMMENT '零售单ID(M_RETAIL.ID)',
  MODIFY COLUMN billdate INT NOT NULL COMMENT '单据日期(YYYYMMDD)',
  MODIFY COLUMN retail_tot_amt_actual DECIMAL(18,2) NULL COMMENT '零售单头实收金额',
  MODIFY COLUMN retail_status INT NULL COMMENT '零售单状态(M_RETAIL.STATUS)',
  MODIFY COLUMN retail_isactive CHAR(1) NULL COMMENT '是否有效(M_RETAIL.ISACTIVE)',
  MODIFY COLUMN synced_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '最近同步时间';

ALTER TABLE ads_inventory_health
  COMMENT = '库存健康度应用表（达播字段优先取标签主线，ODS/缓存兜底）',
  MODIFY COLUMN id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  MODIFY COLUMN snapshot_date DATE NULL COMMENT '快照日期',
  MODIFY COLUMN product_id BIGINT NULL COMMENT '商品ID',
  MODIFY COLUMN product_code VARCHAR(80) NULL COMMENT '商品编码',
  MODIFY COLUMN product_name VARCHAR(200) NULL COMMENT '商品名称',
  MODIFY COLUMN category_id INT NULL COMMENT '类别ID',
  MODIFY COLUMN category_name VARCHAR(50) NULL COMMENT '类别名称',
  MODIFY COLUMN property_id INT NULL COMMENT '性质ID',
  MODIFY COLUMN property_name VARCHAR(50) NULL COMMENT '性质名称',
  MODIFY COLUMN series_id INT NULL COMMENT '系列ID',
  MODIFY COLUMN series_name VARCHAR(100) NULL COMMENT '系列名称',
  MODIFY COLUMN price_list DECIMAL(12,2) NULL COMMENT '吊牌价',
  MODIFY COLUMN total_qty INT NULL COMMENT '总库存',
  MODIFY COLUMN warehouse_qty INT NULL COMMENT '总仓库存',
  MODIFY COLUMN cloud_qty INT NULL COMMENT '云仓库存',
  MODIFY COLUMN purchase_rem_qty INT NULL COMMENT '采购欠数/在途库存',
  MODIFY COLUMN sales_qty_30d INT NULL COMMENT '近30天销量（全量）',
  MODIFY COLUMN sales_amt_30d DECIMAL(14,2) NULL COMMENT '近30天销售额（全量）',
  MODIFY COLUMN sales_qty_7d INT NULL COMMENT '近7天销量（全量）',
  MODIFY COLUMN dabo_sales_qty_30d INT NOT NULL DEFAULT 0 COMMENT '近30天达播销量',
  MODIFY COLUMN dabo_sales_qty_7d INT NOT NULL DEFAULT 0 COMMENT '近7天达播销量',
  MODIFY COLUMN dabo_latest_date DATE NULL COMMENT '达播最新日期（按SKU）',
  MODIFY COLUMN dabo_revenue_30d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近30天达播销售额',
  MODIFY COLUMN dabo_revenue_7d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近7天达播销售额',
  MODIFY COLUMN natural_sales_qty_30d INT NOT NULL DEFAULT 0 COMMENT '近30天自然销量（全量-达播）',
  MODIFY COLUMN natural_sales_qty_7d INT NOT NULL DEFAULT 0 COMMENT '近7天自然销量（全量-达播）',
  MODIFY COLUMN natural_revenue_30d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近30天自然销售额（全量-达播）',
  MODIFY COLUMN natural_revenue_7d DECIMAL(14,2) NOT NULL DEFAULT 0.00 COMMENT '近7天自然销售额（全量-达播）',
  MODIFY COLUMN return_qty_30d INT NULL COMMENT '近30天退货数量',
  MODIFY COLUMN return_amount_30d DECIMAL(14,2) NULL COMMENT '近30天退货金额',
  MODIFY COLUMN daily_avg_sales DECIMAL(10,2) NULL COMMENT '近30天日均销量（全量）',
  MODIFY COLUMN daily_avg_sales_7d DECIMAL(10,2) NULL COMMENT '近7天日均销量（全量）',
  MODIFY COLUMN natural_daily_avg_sales DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '近30天自然日均销量',
  MODIFY COLUMN natural_daily_avg_sales_7d DECIMAL(10,2) NOT NULL DEFAULT 0.00 COMMENT '近7天自然日均销量',
  MODIFY COLUMN sales_velocity DECIMAL(5,2) NULL COMMENT '销售加速度（7天日均/30天日均）',
  MODIFY COLUMN natural_sales_velocity DECIMAL(5,2) NULL COMMENT '自然销售加速度',
  MODIFY COLUMN turnover_days DECIMAL(10,1) NULL COMMENT '周转天数',
  MODIFY COLUMN inventory_status VARCHAR(20) NULL COMMENT '库存状态',
  MODIFY COLUMN sku_grade CHAR(1) NULL COMMENT 'SABC分级',
  MODIFY COLUMN suggest_qty INT NULL COMMENT '建议补货数量',
  MODIFY COLUMN etl_time DATETIME NULL COMMENT 'ETL时间戳',
  MODIFY COLUMN sales_trend VARCHAR(20) NULL COMMENT '销售趋势（全量）',
  MODIFY COLUMN status_priority INT NULL COMMENT '库存状态优先级(1紧急缺货->6停售)',
  MODIFY COLUMN sales_rank INT NULL COMMENT '销售排名',
  MODIFY COLUMN sales_ratio DECIMAL(5,2) NULL COMMENT '销售占比',
  MODIFY COLUMN cumulative_ratio DECIMAL(5,2) NULL COMMENT '累计占比',
  MODIFY COLUMN created_at DATETIME NULL COMMENT '创建时间',
  MODIFY COLUMN sku_id BIGINT NULL COMMENT 'SKU主键(M_PRODUCT_ALIAS.ID)',
  MODIFY COLUMN sku_barcode VARCHAR(80) NULL COMMENT '条码(M_PRODUCT_ALIAS.NO)',
  MODIFY COLUMN color VARCHAR(50) NULL COMMENT 'SKU颜色',
  MODIFY COLUMN size VARCHAR(50) NULL COMMENT 'SKU尺寸';

ALTER TABLE cfg_store_target_daily
  COMMENT = '门店日报目标配置表',
  MODIFY COLUMN created_by VARCHAR(50) NULL COMMENT '创建人/导入人';

ALTER TABLE cfg_store_operation_owner_snapshot
  COMMENT = '门店经营负责人当前快照表',
  MODIFY COLUMN entity_id BIGINT NULL COMMENT '经营实体ID；普通门店=store_id，共同考核主体=挂靠主店store_id',
  MODIFY COLUMN entity_code VARCHAR(64) NOT NULL COMMENT '经营实体编码；普通门店=store_code，共同考核主体=subject_code',
  MODIFY COLUMN owner_name VARCHAR(100) NULL COMMENT '负责人名称，可为空',
  MODIFY COLUMN source_file_name VARCHAR(255) NULL COMMENT '来源文件名',
  MODIFY COLUMN created_by VARCHAR(64) NULL COMMENT '创建人',
  MODIFY COLUMN updated_by VARCHAR(64) NULL COMMENT '更新人';

ALTER TABLE dim_store_operation_owner_assignment
  COMMENT = '门店经营负责人SCD2历史表',
  MODIFY COLUMN entity_id BIGINT NULL COMMENT '经营实体ID；普通门店=store_id，共同考核主体=挂靠主店store_id',
  MODIFY COLUMN entity_code VARCHAR(64) NOT NULL COMMENT '经营实体编码；普通门店=store_code，共同考核主体=subject_code',
  MODIFY COLUMN owner_name VARCHAR(100) NULL COMMENT '负责人名称，可为空',
  MODIFY COLUMN source_snapshot_date DATE NOT NULL COMMENT '触发当前版本生效的快照日期',
  MODIFY COLUMN effective_end_date DATE NOT NULL COMMENT '生效结束日',
  MODIFY COLUMN is_current CHAR(1) NOT NULL DEFAULT 'Y' COMMENT '是否当前有效（Y/N）',
  MODIFY COLUMN created_by VARCHAR(64) NULL COMMENT '创建人',
  MODIFY COLUMN updated_by VARCHAR(64) NULL COMMENT '更新人';

ALTER TABLE dim_store_report_attr
  COMMENT = '门店日报业务属性配置表',
  MODIFY COLUMN report_channel_type_group VARCHAR(20)
    GENERATED ALWAYS AS (
      CASE
        WHEN report_channel_type IN ('小程序', '线上小程序') THEN '小程序'
        WHEN report_channel_type IN ('直营', '直营-奥莱') THEN '直营'
        WHEN report_channel_type IN ('联营', '联营-免税', '联营-奥莱') THEN '联营'
        ELSE NULL
      END
    ) STORED COMMENT '日报渠道粗分类（由 report_channel_type 派生）',
  MODIFY COLUMN store_grade VARCHAR(20) NULL COMMENT '店铺等级(A/B/C/S级)',
  MODIFY COLUMN effective_start_date DATE NOT NULL COMMENT '生效开始日期';

ALTER TABLE dim_channel
  COMMENT = '电商渠道维度表',
  MODIFY COLUMN created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间';

ALTER TABLE dim_product_attr
  COMMENT = '商品属性表（颜色/尺寸，取每个货号的第一个SKU）',
  MODIFY COLUMN product_id BIGINT NULL COMMENT '商品ID(dim_product.product_id)',
  MODIFY COLUMN color TEXT COMMENT '颜色',
  MODIFY COLUMN size TEXT COMMENT '尺寸';