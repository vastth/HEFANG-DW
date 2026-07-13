# -*- coding: utf-8 -*-

import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

from openpyxl import Workbook

from tools.import_duty_free_store_mtd_sales_from_nas import (
    _build_snapshot_diff_summary,
    _parse_workbook,
)


def _write_workbook(workbook_path: Path, rows: list[list[object]]) -> None:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = '免税月累计'
    worksheet.append(['目标月份', '数据版本', '门店ID', '门店名称', '渠道类型', '月累计'])
    for row in rows:
        worksheet.append(row)
    workbook.save(workbook_path)


class TestImportDutyFreeStoreMtdSalesFromNas(unittest.TestCase):
    def test_parse_workbook_reads_single_target_month_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = Path(temp_dir) / '免税门店月累计销售.xlsx'
            _write_workbook(
                workbook_path,
                [
                    ['2026-05', 'v1', 748, '海口美兰国际机场店免税店(T1)', '联营-免税', '125000.56'],
                    ['2026-05', 'v1', 749, '三亚凤凰国际机场店免税店', '联营-免税', '98000'],
                ],
            )

            parsed_rows, workbook_summary = _parse_workbook(workbook_path, '免税月累计')

        self.assertEqual(workbook_summary['target_month'], '2026-05')
        self.assertEqual(workbook_summary['target_month_start'], '2026-05-01')
        self.assertEqual(workbook_summary['data_version'], 'v1')
        self.assertEqual(workbook_summary['source_row_count'], 2)
        self.assertEqual(parsed_rows[0].store_key, '748')
        self.assertEqual(parsed_rows[0].external_mtd_sales_amt, Decimal('125000.56'))

    def test_parse_workbook_accepts_store_code_as_store_id_column(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = Path(temp_dir) / '免税门店月累计销售.xlsx'
            _write_workbook(
                workbook_path,
                [
                    ['2026-05', 'v1', 'RT113', '海口美兰国际机场店免税店(T1)', '联营-免税', '74390'],
                    ['2026-05', 'v1', 'RT110', '杭州萧山国际机场店', '联营-免税', None],
                ],
            )

            parsed_rows, workbook_summary = _parse_workbook(workbook_path, '免税月累计')

        self.assertEqual(workbook_summary['target_month'], '2026-05')
        self.assertEqual(parsed_rows[0].store_key, 'RT113')
        self.assertEqual(parsed_rows[1].store_key, 'RT110')
        self.assertEqual(parsed_rows[1].external_mtd_sales_amt, Decimal('0.00'))

    def test_parse_workbook_rejects_multiple_target_months(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workbook_path = Path(temp_dir) / '免税门店月累计销售.xlsx'
            _write_workbook(
                workbook_path,
                [
                    ['2026-04', 'v1', 748, '海口美兰国际机场店免税店(T1)', '联营-免税', '125000.56'],
                    ['2026-05', 'v1', 749, '三亚凤凰国际机场店免税店', '联营-免税', '98000'],
                ],
            )

            with self.assertRaisesRegex(ValueError, '只能包含一个 目标月份'):
                _parse_workbook(workbook_path, '免税月累计')

    def test_build_snapshot_diff_summary_counts_changed_new_and_exited(self):
        resolved_rows = [
            {
                'store_id': 748,
                'store_name': '海口美兰国际机场店免税店(T1)',
                'report_channel_type': '联营-免税',
                'external_mtd_sales_amt': Decimal('125000.00'),
            },
            {
                'store_id': 750,
                'store_name': '深圳机场店免税店',
                'report_channel_type': '联营-免税',
                'external_mtd_sales_amt': Decimal('88888.00'),
            },
        ]
        existing_snapshot_map = {
            748: {
                'store_name': '海口美兰国际机场店免税店(T1)',
                'report_channel_type': '联营-免税',
                'external_mtd_sales_amt': Decimal('120000.00'),
            },
            749: {
                'store_name': '三亚凤凰国际机场店免税店',
                'report_channel_type': '联营-免税',
                'external_mtd_sales_amt': Decimal('98000.00'),
            },
        }

        diff_summary = _build_snapshot_diff_summary(resolved_rows, existing_snapshot_map)

        self.assertEqual(diff_summary['changed_store_count'], 1)
        self.assertEqual(diff_summary['new_store_count'], 1)
        self.assertEqual(diff_summary['exited_store_count'], 1)
        self.assertTrue(diff_summary['has_changes'])


if __name__ == '__main__':
    unittest.main()