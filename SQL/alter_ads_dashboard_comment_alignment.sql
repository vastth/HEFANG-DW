ALTER TABLE ads_daily_sales
    COMMENT = '销售看板月度战役日节奏表（已接专题调度，未接run_etl主链）',
    MODIFY COLUMN area_name VARCHAR(50) NOT NULL COMMENT '战区；物理层不再生成 全国 汇总行';