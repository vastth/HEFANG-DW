# -*- coding: utf-8 -*-

import unittest
from datetime import date, datetime

from etl_ads_store_daily_subject_report import _build_sql_statements


class TestAdsStoreDailySubjectReport(unittest.TestCase):
    def test_build_sql_statements_inherits_order_counts_from_store_report(self):
        _, insert_sql = _build_sql_statements(
            report_date=date(2026, 4, 28),
            data_version='v2',
            etl_time=datetime(2026, 4, 29, 15, 30, 0),
        )

        self.assertIn('sr.day_order_cnt,', insert_sql)
        self.assertIn('sr.mtd_order_cnt,', insert_sql)
        self.assertIn('rs.day_order_cnt,', insert_sql)
        self.assertIn('rs.mtd_order_cnt,', insert_sql)
        self.assertIn('ELSE ROUND(rs.day_sales_qty / rs.day_order_cnt, 4)', insert_sql)
        self.assertIn('ELSE ROUND(rs.mtd_sales_qty / rs.mtd_order_cnt, 4)', insert_sql)


if __name__ == '__main__':
    unittest.main()