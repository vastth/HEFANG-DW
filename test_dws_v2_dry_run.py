# -*- coding: utf-8 -*-
"""DWS v2 dry-run 脚本的无数据库单元测试。"""

import unittest
from datetime import datetime

from etl_dws_inventory_v2 import (
    DwsInventoryV2DryRunConfig,
    WRITE_CONFIRMATION_TOKEN as INVENTORY_WRITE_CONFIRMATION_TOKEN,
    _assert_same_snapshot_cutoff_reproducible,
    build_delete_existing_slice_sql as build_inventory_delete_existing_slice_sql,
    build_late_loaded_scope_probe_sql as build_inventory_late_loaded_scope_probe_sql,
    build_old_dws_alignment_probe_sql as build_inventory_old_dws_alignment_probe_sql,
    build_old_dws_comparison_detail_sql as build_inventory_old_dws_comparison_detail_sql,
    build_reconciliation_detail_sql as build_inventory_reconciliation_detail_sql,
    build_insert_sql as build_inventory_insert_sql,
    build_params as build_inventory_params,
    build_source_summary_sql as build_inventory_source_summary_sql,
    build_target_summary_sql as build_inventory_target_summary_sql,
    execute_load as execute_inventory_load,
)
from etl_dws_sales_v2 import (
    DwsSalesV2DryRunConfig,
    WRITE_CONFIRMATION_TOKEN as SALES_WRITE_CONFIRMATION_TOKEN,
    build_reconciliation_detail_sql as build_sales_reconciliation_detail_sql,
    build_insert_sql as build_sales_insert_sql,
    build_params as build_sales_params,
    build_source_summary_sql as build_sales_source_summary_sql,
    build_target_summary_sql as build_sales_target_summary_sql,
    execute_load as execute_sales_load,
)


class DwsV2DryRunSqlTest(unittest.TestCase):
    def test_sales_dry_run_sql_targets_v2_and_dwd(self):
        config = DwsSalesV2DryRunConfig()

        sql = build_sales_insert_sql(config)
        summary_sql = build_sales_source_summary_sql(config)
        params = build_sales_params(config)

        self.assertIn('INSERT INTO dws_sales_daily_v2', sql)
        self.assertIn('FROM dwd_sales_retail_item', sql)
        self.assertIn('ON DUPLICATE KEY UPDATE', sql)
        self.assertIn("dws_sales_scope_flag = 'Y'", sql)
        self.assertIn('source_dwd_row_count', sql)
        self.assertIn('net_amount', sql)
        self.assertIn('COUNT(DISTINCT CASE WHEN is_positive_sale_flag', sql)
        self.assertNotIn('dws_sales_daily\n', sql)
        self.assertIn('COUNT(*) AS source_dwd_row_count', summary_sql)
        self.assertEqual(params['start_date'], 20260428)
        self.assertEqual(params['end_date'], 20260430)

        target_summary_sql = build_sales_target_summary_sql(config)
        reconciliation_sql = build_sales_reconciliation_detail_sql(config, limit=20)
        self.assertIn('FROM dws_sales_daily_v2', target_summary_sql)
        self.assertIn('FROM dwd_sales_retail_item', reconciliation_sql)
        self.assertIn('FROM dws_sales_daily_v2', reconciliation_sql)
        self.assertIn('LIMIT 20', reconciliation_sql)

    def test_inventory_dry_run_sql_targets_v2_and_dwd(self):
        config = DwsInventoryV2DryRunConfig()

        sql = build_inventory_insert_sql(config)
        summary_sql = build_inventory_source_summary_sql(config)
        params = build_inventory_params(config)

        self.assertIn('INSERT INTO dws_inventory_daily_v2', sql)
        self.assertIn('FROM dwd_inventory_storage_snapshot', sql)
        self.assertIn('ON DUPLICATE KEY UPDATE', sql)
        self.assertIn("dws_inventory_scope_flag = 'Y'", sql)
        self.assertIn('source_dwd_row_count', sql)
        self.assertIn('qty_oms_translate', sql)
        self.assertIn('qty_preout1', sql)
        self.assertNotIn('dws_inventory_daily\n', sql)
        self.assertIn('COUNT(*) AS source_dwd_row_count', summary_sql)
        self.assertEqual(params['snapshot_date'], 20260507)

        target_summary_sql = build_inventory_target_summary_sql(config)
        reconciliation_sql = build_inventory_reconciliation_detail_sql(config, limit=20)
        self.assertIn('FROM dws_inventory_daily_v2', target_summary_sql)
        self.assertIn('FROM dwd_inventory_storage_snapshot', reconciliation_sql)
        self.assertIn('FROM dws_inventory_daily_v2', reconciliation_sql)
        self.assertIn('LIMIT 20', reconciliation_sql)
        self.assertNotIn('source_loaded_at <= :source_loaded_at_cutoff', summary_sql)

    def test_inventory_cutoff_sql_and_old_dws_probe_are_generated(self):
        cutoff = datetime(2026, 5, 7, 4, 31, 36)
        config = DwsInventoryV2DryRunConfig(source_loaded_at_cutoff=cutoff)

        summary_sql = build_inventory_source_summary_sql(config)
        insert_sql = build_inventory_insert_sql(config)
        params = build_inventory_params(config)
        delete_sql = build_inventory_delete_existing_slice_sql(config)
        late_loaded_scope_probe_sql = build_inventory_late_loaded_scope_probe_sql(config)
        old_dws_probe_sql = build_inventory_old_dws_alignment_probe_sql(config)
        old_dws_comparison_sql = build_inventory_old_dws_comparison_detail_sql(config, limit=20)

        self.assertIn('source_loaded_at <= :source_loaded_at_cutoff', summary_sql)
        self.assertIn('source_loaded_at <= :source_loaded_at_cutoff', insert_sql)
        self.assertEqual(params['source_loaded_at_cutoff'], cutoff)
        self.assertIn('DELETE FROM dws_inventory_daily_v2', delete_sql)
        self.assertIn('WHERE date_id = :snapshot_date', delete_sql)
        self.assertIn("dws_inventory_scope_flag = 'Y'", late_loaded_scope_probe_sql)
        self.assertIn('source_loaded_at > :source_loaded_at_cutoff', late_loaded_scope_probe_sql)
        self.assertIn('FROM dws_inventory_daily', old_dws_probe_sql)
        self.assertIn('MAX(etl_time) AS old_dws_max_etl_time', old_dws_probe_sql)
        self.assertIn('FROM dws_inventory_daily_v2', old_dws_comparison_sql)
        self.assertIn('FROM dws_inventory_daily', old_dws_comparison_sql)
        self.assertIn('LIMIT 20', old_dws_comparison_sql)

    def test_inventory_same_snapshot_cutoff_probe_rejects_late_loaded_scope_rows(self):
        config = DwsInventoryV2DryRunConfig(
            snapshot_date=20260512,
            source_loaded_at_cutoff=datetime(2026, 5, 12, 9, 38, 18),
        )

        with self.assertRaisesRegex(RuntimeError, 'same-snapshot cutoff 无法复原历史快照'):
            _assert_same_snapshot_cutoff_reproducible(
                {
                    'late_scope_row_count': 1212,
                    'late_scope_qty': 55818,
                    'late_scope_qtypurchaserem': 0,
                    'min_source_loaded_at_after_cutoff': '2026-05-12 11:36:58',
                    'max_source_loaded_at_after_cutoff': '2026-05-12 11:36:58',
                },
                config,
            )

    def test_inventory_same_snapshot_cutoff_probe_accepts_empty_late_scope(self):
        config = DwsInventoryV2DryRunConfig(
            snapshot_date=20260512,
            source_loaded_at_cutoff=datetime(2026, 5, 12, 9, 38, 18),
        )

        self.assertIsNone(
            _assert_same_snapshot_cutoff_reproducible(
                {
                    'late_scope_row_count': 0,
                    'late_scope_qty': 0,
                    'late_scope_qtypurchaserem': 0,
                    'min_source_loaded_at_after_cutoff': None,
                    'max_source_loaded_at_after_cutoff': None,
                },
                config,
            )
        )

    def test_invalid_table_identifier_is_rejected(self):
        config = DwsSalesV2DryRunConfig(source_table='dwd_sales_retail_item; DROP TABLE x')

        with self.assertRaises(ValueError):
            build_sales_insert_sql(config)

        inventory_config = DwsInventoryV2DryRunConfig(old_dws_table='dws_inventory_daily; DROP TABLE x')

        with self.assertRaises(ValueError):
            build_inventory_old_dws_alignment_probe_sql(inventory_config)

    def test_execute_requires_explicit_confirmation_token(self):
        with self.assertRaisesRegex(RuntimeError, SALES_WRITE_CONFIRMATION_TOKEN):
            execute_sales_load(DwsSalesV2DryRunConfig(), confirm_write=None)

        with self.assertRaisesRegex(RuntimeError, INVENTORY_WRITE_CONFIRMATION_TOKEN):
            execute_inventory_load(DwsInventoryV2DryRunConfig(), confirm_write=None)


if __name__ == '__main__':
    unittest.main()