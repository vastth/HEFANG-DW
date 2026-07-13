# -*- coding: utf-8 -*-

import inspect
import unittest
from datetime import date, datetime

from etl_ads_store_daily_report import (
    REQUIRED_COLUMNS,
    _build_sql_statements,
    _fetch_config_stats,
    _log_same_store_open_date_quality,
)


class TestAdsStoreDailyReport(unittest.TestCase):
    def test_build_sql_statements_uses_filtered_amount_for_order_sign(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 4, 28),
            data_version='v2',
            etl_time=datetime(2026, 4, 29, 15, 0, 0),
        )

        self.assertIn('ROUND(SUM(fd.tot_amt_actual), 4) AS filtered_retail_amt', insert_sql)
        self.assertIn('WHEN ABS(fd.filtered_retail_amt) < 0.0001 THEN 0', insert_sql)
        self.assertIn('WHEN fd.filtered_retail_amt > 0 THEN 1', insert_sql)
        self.assertIn('WHEN fd.filtered_retail_amt < 0 THEN -1', insert_sql)
        self.assertNotIn('WHEN fd.retail_amt > 0 THEN 1', insert_sql)
        self.assertNotIn('WHEN fd.retail_amt < 0 THEN -1', insert_sql)

    def test_build_sql_statements_exposes_mtd_list_amt_for_discount_total(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 5, 20),
            data_version='v2',
            etl_time=datetime(2026, 5, 20, 14, 30, 0),
        )

        self.assertIn('mtd_list_amt,\n    mtd_sales_qty,', insert_sql)
        self.assertIn('SUM(fd.tot_amt_list) AS mtd_list_amt', insert_sql)
        self.assertIn('COALESCE(mf.mtd_list_amt, 0.00) AS mtd_list_amt', insert_sql)
        self.assertIn('WHEN COALESCE(mf.mtd_list_amt, 0) = 0 THEN NULL', insert_sql)
        self.assertIn('mtd_list_amt', REQUIRED_COLUMNS['ads_store_daily_report'])

    def test_build_sql_statements_excludes_only_fixed_non_sales_categories(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 6, 7),
            data_version='v1',
            etl_time=datetime(2026, 6, 8, 10, 0, 0),
        )

        self.assertIn('excluded_category_scope AS (', insert_sql)
        self.assertIn('SELECT 147 AS category_id', insert_sql)
        self.assertIn('UNION ALL SELECT 149', insert_sql)
        self.assertIn('UNION ALL SELECT 150', insert_sql)
        self.assertIn('WHERE dp.category_id IS NOT NULL', insert_sql)
        self.assertIn('AND ecs.category_id IS NULL', insert_sql)
        self.assertNotIn('FROM dim_report_product_rule', insert_sql)

    def test_build_sql_statements_overlays_duty_free_external_mtd_sales(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 5, 23),
            data_version='v1',
            etl_time=datetime(2026, 5, 23, 12, 0, 0),
        )

        self.assertIn('duty_free_mtd_fact AS (', insert_sql)
        self.assertIn('FROM cfg_duty_free_store_mtd_sales dfm', insert_sql)
        self.assertIn('ON dfm.store_id = sem.source_store_id', insert_sql)
        self.assertIn('WHERE dfm.target_month = p.target_month', insert_sql)
        self.assertIn("WHEN res.is_duty_free = 'Y' AND dmf.external_mtd_sales_amt IS NOT NULL THEN dmf.external_mtd_sales_amt", insert_sql)
        self.assertIn('LEFT JOIN duty_free_mtd_fact dmf', insert_sql)
        self.assertIn('END AS mtd_sales_amt,', insert_sql)
        self.assertIn('END AS month_ach_rate,', insert_sql)
        self.assertIn(') AS mtd_rank,', insert_sql)

    def test_build_sql_statements_exposes_same_store_yoy_helper_amounts(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 5, 22),
            data_version='v2',
            etl_time=datetime(2026, 5, 22, 13, 30, 0),
        )

        self.assertIn('same_store_mtd_sales_amt,\n    same_store_last_year_mtd_sales_amt,\n    yoy_rate,', insert_sql)
        self.assertIn('sem.source_store_id,', insert_sql)
        self.assertIn('same_store_current_fact AS (', insert_sql)
        self.assertIn('same_store_last_year_fact AS (', insert_sql)
        self.assertIn('same_store_entity_fact AS (', insert_sql)
        self.assertIn('SUM(COALESCE(sscf.same_store_mtd_sales_amt, 0.00)) AS same_store_mtd_sales_amt', insert_sql)
        self.assertIn(
            'SUM(COALESCE(sslyf.same_store_last_year_mtd_sales_amt, 0.00)) AS same_store_last_year_mtd_sales_amt',
            insert_sql,
        )
        self.assertIn('sss.open_date AS source_store_open_date', insert_sql)
        self.assertIn('sem.source_store_open_date IS NOT NULL', insert_sql)
        self.assertIn('sem.source_store_open_date <= p.same_store_open_cutoff', insert_sql)
        self.assertNotIn('WHERE COALESCE(sslyf.same_store_last_year_mtd_sales_amt, 0) > 0', insert_sql)
        self.assertIn('ass.assignment_role,\n        ass.subject_code,', insert_sql)
        self.assertIn("AND COALESCE(sem.assignment_role, '') <> '快闪'", insert_sql)
        self.assertIn('COALESCE(ssef.same_store_mtd_sales_amt, 0.00) AS same_store_mtd_sales_amt', insert_sql)
        self.assertIn(
            'COALESCE(ssef.same_store_last_year_mtd_sales_amt, 0.00) AS same_store_last_year_mtd_sales_amt',
            insert_sql,
        )
        self.assertIn('WHEN COALESCE(ssef.same_store_last_year_mtd_sales_amt, 0) = 0 THEN NULL', insert_sql)
        self.assertIn(
            'ELSE ROUND((ssef.same_store_mtd_sales_amt / ssef.same_store_last_year_mtd_sales_amt) - 1, 4)',
            insert_sql,
        )
        self.assertIn('same_store_mtd_sales_amt', REQUIRED_COLUMNS['ads_store_daily_report'])
        self.assertIn('same_store_last_year_mtd_sales_amt', REQUIRED_COLUMNS['ads_store_daily_report'])
        self.assertIn('open_date', REQUIRED_COLUMNS['dim_store'])

    def test_same_store_helpers_keep_current_zero_prior_positive_store(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 7, 5),
            data_version='v1',
            etl_time=datetime(2026, 7, 5, 12, 0, 0),
        )

        self.assertIn('FROM store_entity_map sem', insert_sql)
        self.assertIn('LEFT JOIN same_store_current_fact sscf', insert_sql)
        self.assertIn('LEFT JOIN same_store_last_year_fact sslyf', insert_sql)
        self.assertIn('SUM(COALESCE(sscf.same_store_mtd_sales_amt, 0.00))', insert_sql)
        self.assertNotIn('COALESCE(sscf.same_store_mtd_sales_amt, 0) > 0', insert_sql)
        self.assertNotIn('COALESCE(sslyf.same_store_last_year_mtd_sales_amt, 0) > 0', insert_sql)
        self.assertIn(
            'ELSE ROUND((ssef.same_store_mtd_sales_amt / ssef.same_store_last_year_mtd_sales_amt) - 1, 4)',
            insert_sql,
        )
        # 本期为 0、去年同期为正数时，仍保留同店成员，行级同比为 -100%。
        self.assertEqual(round((0 / 100) - 1, 4), -1.0)
        # 本期为正数、去年同期为 0 时，成员不因销售事实缺失被过滤，行级同比按零分母为 NULL。
        self.assertIsNone(None if 0 == 0 else round((100 / 0) - 1, 4))

    def test_open_date_quality_warning_contains_required_context(self):
        config_stats = {
            'same_store_scope_count': 20,
            'unusable_open_date_count': 2,
            'unusable_open_date_examples': '1/RT001/门店甲, 2/RT002/门店乙',
        }

        with self.assertLogs('etl_ads_store_daily_report', level='WARNING') as captured:
            _log_same_store_open_date_quality(config_stats, date(2026, 7, 5), 'v1')

        message = '\n'.join(captured.output)
        self.assertIn('report_date=2026-07-05', message)
        self.assertIn('data_version=v1', message)
        self.assertIn('total_scope=20', message)
        self.assertIn('unusable_count=2', message)
        self.assertIn('RT001', message)

    def test_build_sql_statements_caps_same_store_last_year_for_midmonth_flash_merge(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 6, 22),
            data_version='v1',
            etl_time=datetime(2026, 6, 22, 22, 0, 0),
        )

        self.assertIn('flash_merge_cutoff_scope AS (', insert_sql)
        self.assertIn("sa.effective_start_date,", insert_sql)
        self.assertIn('ac.effective_start_date,', insert_sql)
        self.assertIn("DATE_SUB(MIN(ass.effective_start_date), INTERVAL 1 DAY) AS merge_before_date", insert_sql)
        self.assertIn("AND ass.effective_start_date > p.target_month", insert_sql)
        self.assertIn('LEFT JOIN flash_merge_cutoff_scope fmcs', insert_sql)
        self.assertIn("DATE_SUB(fmcs.merge_before_date, INTERVAL 1 YEAR)", insert_sql)

    def test_build_sql_statements_uses_latest_history_store_attr_fallback(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 5, 14),
            data_version='v1',
            etl_time=datetime(2026, 5, 14, 15, 0, 0),
        )

        self.assertIn('target_store_scope AS (', insert_sql)
        self.assertIn('joint_assessment_member_scope AS (', insert_sql)
        self.assertIn('joint_assessment_anchor_scope AS (', insert_sql)
        self.assertIn('store_attr_scope AS (', insert_sql)
        self.assertIn('store_attr_candidates AS (', insert_sql)
        self.assertIn('source_store_scope AS (', insert_sql)
        self.assertIn('target_mtd_latest AS (', insert_sql)
        self.assertIn('owner_assignment_candidates AS (', insert_sql)
        self.assertIn('t.target_date BETWEEN p.target_month AND p.report_date', insert_sql)
        self.assertIn('sa.subject_code IS NOT NULL', insert_sql)
        self.assertIn('WHEN p.report_date BETWEEN sra.effective_start_date AND sra.effective_end_date THEN 0', insert_sql)
        self.assertNotIn('AND sra.effective_end_date >= p.target_month', insert_sql)
        self.assertIn('sa.effective_end_date >= p.target_month', insert_sql)
        self.assertIn('oa.effective_end_date >= p.target_month', insert_sql)
        self.assertIn('SELECT store_id FROM joint_assessment_member_scope', insert_sql)
        self.assertIn('SELECT store_id FROM joint_assessment_anchor_scope', insert_sql)
        self.assertIn('FROM source_store_scope sss', insert_sql)
        self.assertIn('LEFT JOIN store_scope ss', insert_sql)
        self.assertIn(
            'COALESCE(MAX(sem.subject_month_target), SUM(COALESCE(tml.month_target, td.month_target, 0.00))) AS month_target',
            insert_sql,
        )
        self.assertIn(
            'COALESCE(st.subject_name, stml.subject_name, ass.subject_code)',
            insert_sql,
        )
        self.assertNotIn('INNER JOIN target_store_scope tss\n        ON sa.store_id = tss.store_id', insert_sql)

    def test_build_sql_statements_keeps_day_target_on_report_date_only(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 5, 14),
            data_version='v1',
            etl_time=datetime(2026, 5, 14, 15, 0, 0),
        )

        self.assertIn('WHERE st.target_date = p.report_date', insert_sql)
        self.assertIn('WHERE t.target_date = p.report_date', insert_sql)
        self.assertIn('WHERE st.target_date BETWEEN p.target_month AND p.report_date', insert_sql)
        self.assertIn('WHERE t.target_date BETWEEN p.target_month AND p.report_date', insert_sql)

    def test_build_sql_statements_normalizes_owner_entity_collation(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 5, 18),
            data_version='v2',
            etl_time=datetime(2026, 5, 18, 15, 30, 0),
        )

        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS report_entity_code', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS report_entity_type', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS report_entity_name', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS area_name', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS report_channel_type', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS store_grade', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS is_duty_free', insert_sql)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS store_code', insert_sql)
        self.assertIn('CONVERT(oac.entity_code USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_code', insert_sql)
        self.assertIn('CONVERT(MAX(sem.report_entity_name) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS store_name', insert_sql)
        self.assertIn('CONVERT(MAX(sem.area_name) USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS area_name', insert_sql)

    def test_fetch_config_stats_normalizes_owner_entity_collation(self):
        fetch_config_stats_source = inspect.getsource(_fetch_config_stats)

        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS entity_type', fetch_config_stats_source)
        self.assertIn('COLLATE utf8mb4_0900_ai_ci AS entity_code', fetch_config_stats_source)
        self.assertIn('CONVERT(oac.entity_type USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_type', fetch_config_stats_source)
        self.assertIn('CONVERT(oac.entity_code USING utf8mb4) COLLATE utf8mb4_0900_ai_ci AS entity_code', fetch_config_stats_source)
        self.assertIn('AND sra.effective_start_date <= p.report_date', fetch_config_stats_source)
        self.assertNotIn('AND sra.effective_end_date >= p.target_month', fetch_config_stats_source)
        self.assertNotIn('dim_report_product_rule', fetch_config_stats_source)


if __name__ == '__main__':
    unittest.main()