# HFSY 数据字典

> 数据库: hfsy
> 快照时间: 2026-03-20 10:39:47
> 数据库版本: 5.7.42
> 部署IP: 8.134.87.152
> 端口: 33066
> 接入账号: shuyun668
> 结构快照: reports/snapshot_mysql_hfsy_schema.json
> 说明: 本文档基于 hfsy 实库只读结构快照生成，用于数云 CRM 接入阶段的真实字段核对；真实密码已由用户在 2026-03-20 会话中提供，但按仓库安全约束不写入 git 跟踪文档，运行时通过环境变量注入。

## sys_area
- 描述: 地区表
- 引擎: InnoDB
- 当前行数: 719691

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | code | char(12) | NO |  | PRI | 代码 |
| 2 | level | tinyint(1) | NO |  | MUL | 级别 |
| 3 | name | varchar(255) | NO |  |  | 名称 |
| 4 | is_end | tinyint(1) | NO | 1 |  | 是否有子项 |
| 5 | parent_code | char(12) | NO |  | MUL | 父级代码 |
| 6 | catlog | int(10) | NO | 0 |  | 城乡分类码 |

## t_member_bind_info
- 描述: 会员绑定信息表
- 引擎: InnoDB
- 当前行数: 1616705

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | memberId | varchar(64) | NO |  |  | 会员ID |
| 2 | shopId | varchar(64) | NO |  | PRI | 店铺ID |
| 3 | platAccount | varchar(255) | NO |  | PRI | 平台账号，淘宝平台是淘宝账号 |
| 4 | platCode | varchar(16) | NO |  | PRI | 平台编码 |
| 5 | birthday | varchar(32) | YES |  |  | 生日 yyyy-MM-dd |
| 6 | gender | varchar(50) | YES |  |  | 性别：F(女); M(男) |
| 7 | mixMobile | varchar(255) | YES |  |  | 淘宝会员通的混淆手机号 |
| 8 | guideId | varchar(255) | YES |  |  | 导购ID，非导购业务可以不填 |
| 9 | bindMobile | varchar(100) | YES |  |  | 绑卡手机号 |
| 10 | name | varchar(256) | YES |  |  |  |
| 11 | cardPlanId | varchar(20) | YES |  | MUL | 卡计划ID |
| 12 | modified | varchar(64) | YES |  |  | 会员信息变更时间，格式：yyyy-MM-dd HH:mm:ss |
| 13 | created | varchar(32) | YES |  |  | 入会时间 yyyy-MM-dd HH:mm:ss |
| 14 | bindStatus | varchar(4) | YES |  |  | 绑卡状态：1是绑定，0是解绑 |
| 15 | DecryptionTags | varchar(4) | YES |  |  | 无意义(解密标记：NULL是无解密，1是已解密)。存在按修改日期修改具体数据的情况。 |
| 16 | platAccount1 | varchar(50) | YES |  |  | 解密后平台账号 |
| 17 | bindMobile1 | varchar(20) | YES |  |  | 解密后绑卡手机号 |
| 18 | birthday1 | varchar(10) | YES |  |  | 解密后生日：YYYY-MM-DD |
| 19 | gender1 | varchar(4) | YES |  |  | 解密后性别：F(女); M(男) |
| 20 | name1 | varchar(50) | YES |  |  | 解密后名称 |

## t_member_info
- 描述: 会员信息表
- 引擎: InnoDB
- 当前行数: 1945360

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | memberId | varchar(64) | NO |  | PRI | 会员ID |
| 2 | cardPlanId | varchar(64) | NO |  | MUL | 卡计划ID |
| 3 | gradePeriod | varchar(32) | YES |  |  | 等级有效期 |
| 4 | totalPoint | varchar(20) | YES |  |  | 历史累计积分 |
| 5 | expiredPoint | varchar(20) | YES |  |  | 已过期的积分 |
| 6 | grade | varchar(64) | YES |  |  | 系统会员等级：需要通过卡等级查询接口映射对应的id才能得到会员等级，例如：数字1~5等级 |
| 7 | availablePoint | varchar(20) | YES |  |  | 可用积分 |
| 8 | consumedPoint | varchar(20) | YES |  |  | 已消费积分 |
| 9 | modified | varchar(32) | YES |  |  | 最近修改时间 |
| 10 | status | varchar(32) | YES |  |  | 会员状态，NORMAL：正常，CLOSED：关闭 |

## t_order
- 描述: 子订单信息表
- 引擎: InnoDB
- 当前行数: 1587425

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | order_item_id | varchar(300) | NO |  | PRI | 子订单ID |
| 2 | shop_id | varchar(64) | YES |  |  | 店铺ID |
| 3 | order_sn | varchar(300) | NO |  | MUL | 订单编号 |
| 4 | goods_number | int(11) | YES |  |  | 商品数量 |
| 5 | goods_price | decimal(13,2) | YES |  |  | 商品价格 |
| 6 | discount | decimal(13,2) | YES |  |  | 折扣金额 |
| 7 | logistics_no | varchar(255) | YES |  |  | 快递单号 |
| 8 | product_id | varchar(255) | YES |  |  | 商品ID |
| 9 | refund_fee | decimal(13,2) | YES |  |  | 子订单退款金额 |
| 10 | adjust_fee | decimal(13,2) | YES |  |  | 子订单调整金额 |
| 11 | logistics_company | varchar(255) | YES |  |  | 快递公司 |
| 12 | is_refund | tinyint(4) | YES |  |  | 是否退单：1(是); 0(否) |
| 13 | sku_id | varchar(150) | YES |  |  | 商品SKU_ID |
| 14 | product_name | varchar(350) | YES |  |  | 商品名称 |
| 15 | outer_product_id | varchar(350) | YES |  |  | 外部商品ID |
| 16 | outer_sku_id | varchar(350) | YES |  |  | 外部商品SKU_ID |
| 17 | payment | decimal(13,2) | YES |  |  | 分摊到子订单的应付金额 |
| 18 | plat_code | varchar(64) | YES |  |  | 平台编码 |

## t_order_copy
- 描述: 子订单信息表
- 引擎: InnoDB
- 当前行数: 613925
- 说明: 当前仅按结构快照记录，尚未确认是否为正式业务表或备份表。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | order_item_id | varchar(300) | NO |  | PRI | 子订单ID |
| 2 | shop_id | varchar(64) | YES |  |  | 店铺ID |
| 3 | order_sn | varchar(300) | NO |  | MUL | 订单编号 |
| 4 | goods_number | int(11) | YES |  |  | 商品数量 |
| 5 | goods_price | decimal(13,2) | YES |  |  | 商品价格 |
| 6 | discount | decimal(13,2) | YES |  |  | 折扣金额 |
| 7 | logistics_no | varchar(255) | YES |  |  | 快递单号 |
| 8 | product_id | varchar(255) | YES |  |  | 商品ID |
| 9 | refund_fee | decimal(13,2) | YES |  |  | 子订单退款金额 |
| 10 | adjust_fee | decimal(13,2) | YES |  |  | 子订单调整金额 |
| 11 | logistics_company | varchar(255) | YES |  |  | 快递公司 |
| 12 | is_refund | tinyint(4) | YES |  |  | 是否退单：1(是); 0(否) |
| 13 | sku_id | varchar(150) | YES |  |  | 商品SKU_ID |
| 14 | product_name | varchar(350) | YES |  |  | 商品名称 |
| 15 | outer_product_id | varchar(350) | YES |  |  | 外部商品ID |
| 16 | outer_sku_id | varchar(350) | YES |  |  | 外部商品SKU_ID |
| 17 | payment | decimal(13,2) | YES |  |  | 分摊到子订单的应付金额 |
| 18 | plat_code | varchar(64) | YES |  |  | 平台编码 |

## t_order_copy1
- 描述: 子订单信息表
- 引擎: InnoDB
- 当前行数: 613925
- 说明: 当前仅按结构快照记录，尚未确认是否为正式业务表或备份表。

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | order_item_id | varchar(300) | NO |  | PRI | 子订单ID |
| 2 | shop_id | varchar(64) | YES |  |  | 店铺ID |
| 3 | order_sn | varchar(300) | NO |  | MUL | 订单编号 |
| 4 | goods_number | int(11) | YES |  |  | 商品数量 |
| 5 | goods_price | decimal(13,2) | YES |  |  | 商品价格 |
| 6 | discount | decimal(13,2) | YES |  |  | 折扣金额 |
| 7 | logistics_no | varchar(255) | YES |  |  | 快递单号 |
| 8 | product_id | varchar(255) | YES |  |  | 商品ID |
| 9 | refund_fee | decimal(13,2) | YES |  |  | 子订单退款金额 |
| 10 | adjust_fee | decimal(13,2) | YES |  |  | 子订单调整金额 |
| 11 | logistics_company | varchar(255) | YES |  |  | 快递公司 |
| 12 | is_refund | tinyint(4) | YES |  |  | 是否退单：1(是); 0(否) |
| 13 | sku_id | varchar(150) | YES |  |  | 商品SKU_ID |
| 14 | product_name | varchar(350) | YES |  |  | 商品名称 |
| 15 | outer_product_id | varchar(350) | YES |  |  | 外部商品ID |
| 16 | outer_sku_id | varchar(350) | YES |  |  | 外部商品SKU_ID |
| 17 | payment | decimal(13,2) | YES |  |  | 分摊到子订单的应付金额 |
| 18 | plat_code | varchar(64) | YES |  |  | 平台编码 |

## t_pin_xid_rel
- 描述: pin转xid字段表
- 引擎: InnoDB
- 当前行数: 38571

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | pin | varchar(300) | NO |  | PRI | 订单pinId（主键） |
| 2 | platCode | varchar(64) | YES |  |  | 平台编码 |
| 3 | shopId | varchar(64) | YES |  |  | 店铺ID |
| 4 | xid | varchar(300) | YES |  |  | 会员xid |

## t_trade
- 描述: 主订单信息表
- 引擎: InnoDB
- 当前行数: 1230834

| 序号 | 字段名 | 类型 | 可空 | 默认值 | 键 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | order_sn | varchar(300) | NO |  | PRI | 订单编号 |
| 2 | shop_id | varchar(64) | YES |  |  | 店铺ID |
| 3 | plat_code | varchar(64) | YES |  |  | 平台编码 |
| 4 | receiver_province | varchar(64) | YES |  |  | 收货人省编码 |
| 5 | receiver_city | varchar(64) | YES |  |  | 收货人市编码 |
| 6 | receiver_district | varchar(64) | YES |  |  | 收货人区编码 |
| 7 | pay_time | varchar(20) | YES |  |  | 支付时间 |
| 8 | shipping_fee | decimal(13,2) | YES |  |  | 快递费用 |
| 9 | shipping_time_fh | varchar(20) | YES |  |  | 发货时间 |
| 10 | last_update | varchar(20) | YES |  |  | 淘宝正单 最新修改时间 |
| 11 | order_amount | decimal(13,2) | YES |  |  | 应付金额（最终成交价，包含邮费、不包含退款费用） |
| 12 | plat_account | varchar(255) | YES |  |  | 平台账号 |
| 13 | order_status | varchar(64) | YES |  |  | 订单状态 |
| 14 | receiver_mobile | varchar(500) | YES |  |  | 收货人手机号（淘宝为空，抖音自己已经加密，无法解密） |
| 15 | trade_discount_fee | decimal(13,2) | YES |  |  | 订单级优惠金额 |
| 16 | total_fee | decimal(13,2) | YES |  |  | 订单优惠前总金额 |
| 17 | adjust_fee | decimal(13,2) | YES |  |  | 手工调整优惠金额 |
| 18 | refund_fee | decimal(13,2) | YES |  |  | 订单退款金额 |
| 19 | item_discount_fee | decimal(13,2) | YES |  |  | 商品级优惠总金额 |
| 20 | is_presale | tinyint(4) | YES |  |  | 是否为预售订单,1:是 0:否 |
| 21 | product_num | int(11) | YES |  |  | 订单商品总数 |
| 22 | end_time | varchar(20) | YES |  |  | 确认收货时间 |
| 23 | presale_status | varchar(32) | YES |  |  | 预售订单状态SY_FRONT_NOPAID_FINAL_NOPAID(定金未付尾款未付)SY_FRONT_PAID_FINAL_NOPAID(定金已付尾款未付)SY_FRONT_PAID_FINAL_PAID(定金和尾款都付)FRONT_PAID_FRONT_FORFEITED(预售定金罚没) |
| 24 | receiver_address | varchar(2000) | YES |  |  | 收货地址 |
| 25 | receiver_name | varchar(700) | YES |  |  | 收货人姓名 |
| 26 | shipping_name | varchar(200) | YES |  |  | 快递方式 |
| 27 | add_time | varchar(20) | YES |  | MUL | 下单时间 |
| 28 | shuyun_modified | varchar(20) | YES |  |  | 数云订单修改时间 |

## 使用说明

- 第一阶段会员链路建议优先使用 `t_member_info`、`t_member_bind_info`、`t_pin_xid_rel`。
- 2026-03-20 全表统计显示，`t_member_bind_info` 的 `platAccount1`、`bindMobile1`、`birthday1`、`gender1`、`name1` 与 `DecryptionTags` 当前均为全空；因此这些列目前只能视为预留结构，第一阶段不能依赖其作为现成明文字段。
- 2026-03-20 全表统计显示，`t_order_copy`、`t_order_copy1` 各自 613925 行，且与 `t_order`、彼此之间都按 `order_item_id` 100% 重叠；当前更接近同域复制子集或备份切片，应排除出正式消费链路。

## 版本记录

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| v1.2 | 2026-03-20 | 同步 *1 列全空、DecryptionTags 全空以及 copy 表与主表 100% 重叠的补证结果 |
| v1.1 | 2026-03-20 | 补充 hfsy 实库连接元信息，并明确真实密码不落盘、仅通过环境变量注入 |
| v1.0 | 2026-03-20 | 基于 hfsy 实库结构快照新增数据字典 |
