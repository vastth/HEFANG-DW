# -*- coding: utf-8 -*-

import unittest
from datetime import date, datetime

from etl_ads_daily_sales import _build_sql_statements as build_daily_sales_sql
from etl_ads_daily_sales import _fetch_scope_stats as fetch_daily_sales_scope_stats


class _RecordingCursor:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append({
            'sql': sql,
            'params': params,
        })

    def fetchone(self):
        return next(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _RecordingConnection:
    def __init__(self, rows):
        self.cursor_instance = _RecordingCursor(rows)

    def cursor(self):
        return self.cursor_instance


class TestAdsSalesScopeAlignment(unittest.TestCase):
    def test_ads_daily_sales_insert_sql_scopes_store_by_report_date_target(self):
        _, insert_sql = build_daily_sales_sql(
            report_date=date(2026, 5, 2),
            data_version='v2',
            etl_time=datetime(2026, 5, 8, 10, 0, 0),
        )

        self.assertIn('target_store_scope AS (', insert_sql)
        self.assertIn('joint_assessment_member_scope AS (', insert_sql)
        self.assertIn('joint_assessment_anchor_scope AS (', insert_sql)
        self.assertIn('store_attr_scope AS (', insert_sql)
        self.assertIn('source_store_scope AS (', insert_sql)
        self.assertIn('FROM cfg_store_target_daily t', insert_sql)
        self.assertIn('AND t.target_date BETWEEN p.battle_month AND p.report_date', insert_sql)
        self.assertIn('AND t.target_version = p.data_version', insert_sql)
        self.assertIn('sa.target_version = p.data_version', insert_sql)
        self.assertIn('SELECT store_id FROM joint_assessment_member_scope', insert_sql)
        self.assertIn('FROM source_store_scope sss', insert_sql)
        self.assertNotIn('INNER JOIN target_store_scope tss\n        ON sa.store_id = tss.store_id', insert_sql)

    def test_ads_daily_sales_insert_sql_uses_fixed_category_exclusion_scope(self):
        _, insert_sql = build_daily_sales_sql(
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

    def test_ads_daily_sales_scope_stats_use_same_target_filter(self):
        conn = _RecordingConnection([
            {
                'store_attr_row_count': 71,
                'distinct_store_count': 71,
                'store_attr_overlap_store_count': 0,
            },
            {
                'detail_group_count': 5,
            },
            {
                'missing_dim_store_count': 0,
            },
            {
                'target_row_count': 142,
                'distinct_target_day_count': 142,
                'distinct_target_store_count': 71,
            },
        ])

        fetch_daily_sales_scope_stats(conn, date(2026, 5, 2), 'v2')

        first_sql = conn.cursor_instance.executed[0]['sql']
        second_sql = conn.cursor_instance.executed[1]['sql']
        fourth_sql = conn.cursor_instance.executed[3]['sql']
        first_params = conn.cursor_instance.executed[0]['params']
        second_params = conn.cursor_instance.executed[1]['params']
        fourth_params = conn.cursor_instance.executed[3]['params']

        self.assertIn('FROM cfg_store_target_daily t', first_sql)
        self.assertIn('AND t.target_date BETWEEN %s AND %s', first_sql)
        self.assertIn('AND t.target_version = %s', first_sql)
        self.assertIn('joint_assessment_member_scope AS (', first_sql)
        self.assertIn('joint_assessment_anchor_scope AS (', first_sql)
        self.assertIn('store_attr_scope AS (', first_sql)
        self.assertEqual(
            (
                'v2',
                date(2026, 5, 1),
                date(2026, 5, 2),
                date(2026, 5, 1),
                'v2',
                date(2026, 5, 2),
                date(2026, 5, 1),
                date(2026, 5, 1),
                'v2',
                date(2026, 5, 2),
                date(2026, 5, 1),
                date(2026, 5, 2),
                date(2026, 5, 2),
                date(2026, 5, 1),
                date(2026, 5, 2),
            ),
            first_params,
        )

        self.assertIn('FROM cfg_store_target_daily t', second_sql)
        self.assertIn('AND t.target_date BETWEEN %s AND %s', second_sql)
        self.assertIn('AND t.target_version = %s', second_sql)
        self.assertIn('FROM source_store_scope sss', second_sql)
        self.assertEqual(
            (
                'v2',
                date(2026, 5, 1),
                date(2026, 5, 2),
                date(2026, 5, 1),
                'v2',
                date(2026, 5, 2),
                date(2026, 5, 1),
                date(2026, 5, 1),
                'v2',
                date(2026, 5, 2),
                date(2026, 5, 1),
                date(2026, 5, 2),
                date(2026, 5, 2),
                date(2026, 5, 1),
                date(2026, 5, 2),
                date(2026, 5, 1),
                'v2',
                date(2026, 5, 2),
                date(2026, 5, 1),
            ),
            second_params,
        )

        self.assertIn('FROM cfg_store_target_daily scoped_target', fourth_sql)
        self.assertIn('AND scoped_target.target_date BETWEEN %s AND %s', fourth_sql)
        self.assertIn('AND scoped_target.target_version = %s', fourth_sql)
        self.assertIn('COUNT(DISTINCT t.store_id) AS distinct_target_store_count', fourth_sql)
        self.assertEqual(
            ('v2', date(2026, 5, 1), date(2026, 5, 2), 'v2', date(2026, 5, 1), date(2026, 5, 2)),
            fourth_params,
        )


if __name__ == '__main__':
    unittest.main()