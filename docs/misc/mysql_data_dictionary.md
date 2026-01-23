## ads_inventory_health | 库存健康度应用表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | snapshot_date | date | YES |  |  |
| 3 | product_id | bigint | YES |  |  |
| 4 | product_code | varchar(80) | YES |  |  |
| 5 | product_name | varchar(200) | YES |  |  |
| 6 | category_id | int | YES |  |  |
| 7 | category_name | varchar(50) | YES |  |  |
| 8 | property_id | int | YES |  |  |
| 9 | property_name | varchar(50) | YES |  |  |
| 10 | series_id | int | YES |  |  |
| 11 | series_name | varchar(100) | YES |  |  |
| 12 | price_list | decimal(12,2) | YES |  |  |
| 13 | total_qty | int | YES |  |  |
| 14 | warehouse_qty | int | YES |  |  |
| 15 | cloud_qty | int | YES |  |  |
| 16 | purchase_rem_qty | int | YES |  |  |
| 17 | sales_qty_30d | int | YES |  |  |
| 18 | sales_amt_30d | decimal(14,2) | YES |  |  |
| 19 | sales_qty_7d | int | YES |  |  |
| 20 | return_qty_30d | int | YES |  |  |
| 21 | return_amount_30d | decimal(14,2) | YES |  |  |
| 22 | daily_avg_sales | decimal(10,2) | YES |  |  |
| 23 | daily_avg_sales_7d | decimal(10,2) | YES |  |  |
| 24 | sales_velocity | decimal(5,2) | YES |  |  |
| 25 | turnover_days | decimal(10,1) | YES |  |  |
| 26 | inventory_status | varchar(20) | YES |  |  |
| 27 | sku_grade | char(1) | YES |  |  |
| 28 | suggest_qty | int | YES |  |  |
| 29 | etl_time | datetime | YES |  |  |
| 30 | sales_trend | varchar(20) | YES |  |  |
| 31 | status_priority | int | YES |  |  |
| 32 | sales_rank | int | YES |  |  |
| 33 | sales_ratio | decimal(5,2) | YES |  |  |
| 34 | cumulative_ratio | decimal(5,2) | YES |  |  |
| 35 | created_at | datetime | YES |  |  |

## dim_category | 类别维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | category_id | int | NO |  | 类别ID |
| 2 | category_name | varchar(50) | NO |  | 类别名称 |
| 3 | is_main_product | char(1) | YES | Y | 是否主销品类别 |
| 4 | sort_order | int | YES | 0 | 排序 |
| 5 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dim_channel | 电商渠道维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | channel_id | int | NO |  | 渠道ID |
| 2 | channel_name | varchar(50) | NO |  | 渠道名称 |
| 3 | channel_code | varchar(20) | YES |  | 渠道编码 |
| 4 | store_code | varchar(40) | YES |  | 对应店仓编码 |
| 5 | is_main | tinyint | YES | 0 | 是否主要渠道 |
| 6 | platform_type | varchar(20) | YES |  | 平台类型 |
| 7 | is_active | char(1) | YES | Y | 是否有效 |
| 8 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dim_date | 日期维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | date_id | int | NO |  | 日期ID格式YYYYMMDD |
| 2 | date_value | date | NO |  | 日期 |
| 3 | date_year | int | NO |  | 年 |
| 4 | date_month | int | NO |  | 月 |
| 5 | date_day | int | NO |  | 日 |
| 6 | date_quarter | int | NO |  | 季度 |
| 7 | week_of_year | int | NO |  | 年周数 |
| 8 | day_of_week | int | NO |  | 周几1到7 |
| 9 | day_name_cn | varchar(10) | YES |  | 周几中文 |
| 10 | month_name_cn | varchar(10) | YES |  | 月份中文 |
| 11 | is_weekend | tinyint | YES | 0 | 是否周末 |
| 12 | is_holiday | tinyint | YES | 0 | 是否节假日 |
| 13 | holiday_name | varchar(50) | YES |  | 节假日名称 |
| 14 | year_month | varchar(7) | YES |  | 年月格式YYYY-MM |
| 15 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dim_product | 商品维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | product_id | bigint | NO |  | 商品ID |
| 2 | product_code | varchar(80) | NO |  | 商品编码款号 |
| 3 | product_name | varchar(200) | YES |  | 商品名称 |
| 4 | category_id | int | YES |  | 类别ID |
| 5 | category_name | varchar(50) | YES |  | 类别名称 |
| 6 | property_id | int | YES |  | 性质ID |
| 7 | property_name | varchar(50) | YES |  | 性质名称 |
| 8 | series_id | int | YES |  | 系列ID |
| 9 | series_name | varchar(100) | YES |  | 系列名称 |
| 10 | brand_id | int | YES |  | 品牌ID |
| 11 | brand_name | varchar(50) | YES |  | 品牌名称 |
| 12 | year_id | int | YES |  | 年份ID |
| 13 | year_name | varchar(20) | YES |  | 年份 |
| 14 | price_list | decimal(12,2) | YES |  | 吊牌价 |
| 15 | price_cost | decimal(12,2) | YES |  | 成本价 |
| 16 | is_main_product | char(1) | YES | Y | 是否主销品 |
| 17 | is_active | char(1) | YES | Y | 是否有效 |
| 18 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 19 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 20 | material | text | YES |  |  |

## dim_product_attr | 

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | product_id | bigint | YES |  |  |
| 2 | color | text | YES |  |  |
| 3 | size | text | YES |  |  |

## dim_store | 店仓维度表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | store_id | bigint | NO |  | 店仓ID |
| 2 | store_code | varchar(40) | NO |  | 店仓编码 |
| 3 | store_name | varchar(255) | YES |  | 店仓名称 |
| 4 | area_id | int | YES |  | 区域ID |
| 5 | area_name | varchar(100) | YES |  | 区域名称 |
| 6 | is_warehouse | tinyint | YES | 0 | 是否仓库 |
| 7 | is_store | tinyint | YES | 0 | 是否门店 |
| 8 | is_cloud_store | char(1) | YES | N | 是否云仓 |
| 9 | is_center | char(1) | YES | N | 是否物流中心 |
| 10 | store_type | varchar(20) | YES |  | 类型 |
| 11 | is_active | char(1) | YES | Y | 是否有效 |
| 12 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 13 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |

## dws_inventory_daily | 日库存快照表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | date_id | int | NO |  | 日期ID |
| 3 | store_id | bigint | NO |  | 店仓ID |
| 4 | product_id | bigint | NO |  | 商品ID |
| 5 | qty | int | YES | 0 | 库存数量 |
| 6 | qty_valid | int | YES | 0 | 可用库存 |
| 7 | qty_occupy | int | YES | 0 | 占用数量 |
| 8 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 9 | etl_time | datetime | YES |  | ETL时间戳 |
| 10 | qtypurchaserem | bigint | YES | 0 | 采购欠数/在途 |

## dws_sales_daily | 日销售汇总表

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | id | bigint | NO |  |  |
| 2 | date_id | int | NO |  | 日期ID |
| 3 | store_id | bigint | NO |  | 店仓ID |
| 4 | product_id | bigint | NO |  | 商品ID |
| 5 | sales_qty | int | YES | 0 | 销售数量 |
| 6 | sales_amount | decimal(14,2) | YES | 0.00 | 销售金额 |
| 7 | sales_amount_list | decimal(14,2) | YES | 0.00 | 吊牌金额 |
| 8 | return_qty | int | YES | 0 | 退货数量 |
| 9 | return_amount | decimal(14,2) | YES | 0.00 | 退货金额 |
| 10 | net_qty | int | YES | 0 | 净销量 |
| 11 | net_amount | decimal(14,2) | YES | 0.00 | 净销售额 |
| 12 | order_count | int | YES | 0 | 订单数 |
| 13 | created_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 14 | updated_at | datetime | YES | CURRENT_TIMESTAMP |  |
| 15 | etl_time | datetime | YES |  | ETL时间戳 |
| 16 | store_code | varchar(32) | YES |  | 源店仓编码（如 DS001） |
| 17 | is_cloud_store | char(1) | YES | N | 是否云仓(Y/N) |
