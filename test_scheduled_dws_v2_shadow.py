# -*- coding: utf-8 -*-

import unittest
from datetime import datetime
from unittest.mock import patch

from etl_ads_health import _build_shadow_inventory_health_projection_sql
from scheduled_dws_v2_shadow import (
    ADS_INVENTORY_HEALTH_SALES_DAYS_BACK,
    DWS_SALES_MAINLINE_DAYS_BACK,
    _build_execute_report,
    _build_inventory_ads_gate_validation,
    _count_inclusive_days,
    _build_chain_summary_payload,
    _build_inventory_old_alignment_count_sql,
    _build_inventory_old_alignment_detail_sql,
    _resolve_sales_timeout_profile,
    build_parser,
)


class TestScheduledDwsV2Shadow(unittest.TestCase):
    def test_ads_inventory_shadow_projection_aliases_color_and_size(self):
        sql = _build_shadow_inventory_health_projection_sql(
            datetime(2026, 5, 12, 17, 2, 0),
            'dws_inventory_daily_v2',
            'dws_sales_daily_v2',
            'SELECT CAST(NULL AS SIGNED) AS sku_id WHERE 1 = 0',
        )

        self.assertIn('sku.sku_color AS color', sql)
        self.assertIn('sku.sku_size AS size', sql)
        self.assertIn('ranked.color', sql)
        self.assertIn('ranked.size', sql)

    def test_default_sales_window_covers_ads_inventory_health_horizon(self):
        args = build_parser().parse_args([])

        self.assertEqual(args.sales_days_back, ADS_INVENTORY_HEALTH_SALES_DAYS_BACK)
        self.assertGreater(args.sales_days_back, DWS_SALES_MAINLINE_DAYS_BACK)

    def test_sales_timeout_profile_escalates_for_ads_horizon(self):
        self.assertEqual(_count_inclusive_days(20260412, 20260512), ADS_INVENTORY_HEALTH_SALES_DAYS_BACK)
        self.assertEqual(_resolve_sales_timeout_profile(20260412, 20260512), 'long_running')
        self.assertEqual(_resolve_sales_timeout_profile(20260506, 20260512), 'etl')

    def test_parser_accepts_inventory_same_snapshot_args(self):
        args = build_parser().parse_args(['--inventory-align-with-old-dws'])

        self.assertTrue(args.inventory_align_with_old_dws)
        self.assertIsNone(args.inventory_source_loaded_at_cutoff)

        args = build_parser().parse_args([
            '--inventory-source-loaded-at-cutoff',
            '2026-05-12 09:38:18',
        ])

        self.assertEqual(args.inventory_source_loaded_at_cutoff, '2026-05-12 09:38:18')
        self.assertFalse(args.inventory_align_with_old_dws)

    def test_execute_report_passes_inventory_alignment_config(self):
        inventory_execute_mock = unittest.mock.Mock(
            return_value={
                'status': 'SUCCESS',
                'reconciliation': {'mismatch_count': '0'},
                'align_with_old_dws': True,
                'source_loaded_at_cutoff': '2026-05-12 09:38:18',
            }
        )

        with (
            patch('scheduled_dws_v2_shadow.ods_retail_execute_load', return_value=1),
            patch('scheduled_dws_v2_shadow.ods_retailitem_execute_load', return_value=1),
            patch('scheduled_dws_v2_shadow.dwd_sales_execute_load', return_value={}),
            patch(
                'scheduled_dws_v2_shadow.dws_sales_v2_execute_load',
                return_value={'status': 'SUCCESS', 'reconciliation': {'mismatch_count': '0'}},
            ),
            patch(
                'scheduled_dws_v2_shadow._run_inventory_old_alignment_baseline',
                return_value={
                    'status': 'SUCCESS',
                    'mismatch_count': 0,
                    'compare_source': 'ods_fa_storage',
                    'old_dws_probe': {'old_dws_max_etl_time': '2026-05-12 09:38:18'},
                },
            ),
            patch('scheduled_dws_v2_shadow.ods_fa_storage_execute_load', return_value=1),
            patch('scheduled_dws_v2_shadow.dwd_inventory_execute_load', return_value={}),
            patch('scheduled_dws_v2_shadow.dws_inventory_v2_execute_load', inventory_execute_mock),
            patch(
                'scheduled_dws_v2_shadow.validate_inventory_health_shadow_against_persisted',
                return_value={
                    'status': 'SUCCESS',
                    'mismatch_count': 0,
                    'baseline_table': 'ads_inventory_health',
                    'shadow_inventory_table': 'dws_inventory_daily_v2',
                    'shadow_sales_table': 'dws_sales_daily_v2',
                },
            ),
        ):
            report = _build_execute_report(
                20260412,
                20260512,
                datetime(2026, 5, 11, 10, 52, 44),
                datetime(2026, 5, 12, 10, 52, 44),
                20260512,
                inventory_source_loaded_at_cutoff=None,
                inventory_align_with_old_dws=True,
                conn_test_only=False,
                retail_chunk_size=1000,
                retailitem_chunk_size=1000,
                inventory_chunk_size=1000,
                reconciliation_limit=20,
            )

        inventory_config = inventory_execute_mock.call_args.args[0]

        self.assertTrue(inventory_config.align_with_old_dws)
        self.assertIsNone(inventory_config.source_loaded_at_cutoff)
        self.assertTrue(report['inventory_alignment']['align_with_old_dws'])
        self.assertEqual(report['inventory_ads_gate_validation']['status'], 'READY')
        self.assertEqual(report['status'], 'SUCCESS')
        self.assertEqual(
            report['steps'][-1]['key'],
            'ads_inventory_health_shadow_validation',
        )
        self.assertEqual(report['steps'][-1]['summary']['mismatch_count'], 0)

    def test_execute_report_allows_missing_old_dws_snapshot_in_prerefresh(self):
        with (
            patch('scheduled_dws_v2_shadow.ods_retail_execute_load', return_value=1),
            patch('scheduled_dws_v2_shadow.ods_retailitem_execute_load', return_value=1),
            patch('scheduled_dws_v2_shadow.dwd_sales_execute_load', return_value={}),
            patch(
                'scheduled_dws_v2_shadow.dws_sales_v2_execute_load',
                return_value={'status': 'SUCCESS', 'reconciliation': {'mismatch_count': '0'}},
            ),
            patch(
                'scheduled_dws_v2_shadow._run_inventory_old_alignment_baseline',
                return_value={
                    'status': 'SKIPPED',
                    'reason': 'old_dws_snapshot_not_ready_in_pre_refresh',
                    'mismatch_count': 31967,
                    'compare_source': 'ods_fa_storage',
                    'old_dws_probe': {'old_dws_max_etl_time': None},
                },
            ),
            patch('scheduled_dws_v2_shadow.ods_fa_storage_execute_load', return_value=1),
            patch('scheduled_dws_v2_shadow.dwd_inventory_execute_load', return_value={}),
            patch(
                'scheduled_dws_v2_shadow.dws_inventory_v2_execute_load',
                return_value={'status': 'SUCCESS', 'reconciliation': {'mismatch_count': '0'}},
            ),
        ):
            report = _build_execute_report(
                20260413,
                20260513,
                datetime(2026, 5, 13, 0, 5, 32),
                datetime(2026, 5, 14, 0, 5, 32),
                20260514,
                inventory_source_loaded_at_cutoff=None,
                inventory_align_with_old_dws=False,
                conn_test_only=False,
                retail_chunk_size=1000,
                retailitem_chunk_size=1000,
                inventory_chunk_size=1000,
                reconciliation_limit=20,
                skip_ads_shadow_validation=True,
            )

        baseline_step = next(step for step in report['steps'] if step['key'] == 'inventory_old_dws_comparable_alignment')

        self.assertEqual(baseline_step['status'], 'SKIPPED')
        self.assertEqual(report['inventory_ads_gate_validation']['status'], 'READY')
        self.assertEqual(
            report['inventory_ads_gate_validation']['reason'],
            'pre_refresh_dwd_to_v2_passed_without_old_dws_snapshot',
        )
        self.assertEqual(report['status'], 'SUCCESS')

    def test_inventory_ads_gate_validation_uses_current_baseline(self):
        report = {
            'mode': 'execute',
            'inventory_alignment': {
                'align_with_old_dws': True,
                'source_loaded_at_cutoff': None,
            },
            'steps': [
                {
                    'key': 'inventory_old_dws_comparable_alignment',
                    'summary': {
                        'status': 'SUCCESS',
                        'compare_source': 'ods_fa_storage',
                        'mismatch_count': 0,
                    },
                },
                {
                    'key': 'dws_inventory_v2',
                    'summary': {
                        'status': 'FAILED',
                        'error': 'RuntimeError(\'inventory same-snapshot cutoff 无法复原历史快照\')',
                    },
                },
            ],
        }

        validation = _build_inventory_ads_gate_validation(report)

        self.assertEqual(validation['basis_key'], 'current_ods_and_dwd_baseline')
        self.assertEqual(validation['status'], 'BLOCKED')
        self.assertEqual(validation['reason'], 'same_snapshot_diagnostic_failed')
        self.assertTrue(validation['same_snapshot_requested'])

    def test_inventory_ads_gate_validation_allows_prerefresh_without_old_dws_snapshot(self):
        report = {
            'mode': 'execute',
            'skip_ads_shadow_validation': True,
            'inventory_alignment': {
                'align_with_old_dws': False,
                'source_loaded_at_cutoff': None,
            },
            'steps': [
                {
                    'key': 'inventory_old_dws_comparable_alignment',
                    'summary': {
                        'status': 'SKIPPED',
                        'reason': 'old_dws_snapshot_not_ready_in_pre_refresh',
                        'compare_source': 'ods_fa_storage',
                        'mismatch_count': 31967,
                    },
                },
                {
                    'key': 'dws_inventory_v2',
                    'summary': {
                        'status': 'SUCCESS',
                        'reconciliation': {'mismatch_count': '0'},
                    },
                },
            ],
        }

        validation = _build_inventory_ads_gate_validation(report)

        self.assertEqual(validation['basis_key'], 'current_ods_and_dwd_baseline')
        self.assertEqual(validation['status'], 'READY')
        self.assertEqual(
            validation['reason'],
            'pre_refresh_dwd_to_v2_passed_without_old_dws_snapshot',
        )
        self.assertFalse(validation['same_snapshot_requested'])

    def test_inventory_old_alignment_sql_compares_main_ods_and_old_dws(self):
        detail_sql = _build_inventory_old_alignment_detail_sql(20260508, limit=20)
        count_sql = _build_inventory_old_alignment_count_sql(20260508)

        self.assertIn('FROM ods_fa_storage fs', detail_sql)
        self.assertIn('FROM dws_inventory_daily', detail_sql)
        self.assertIn("fs.isactive = 'Y'", detail_sql)
        self.assertIn("fs.m_productalias_id IS NOT NULL", detail_sql)
        self.assertIn("s.store_code = '001' OR s.is_cloud_store = 'Y'", detail_sql)
        self.assertIn('LIMIT 20', detail_sql)
        self.assertIn('COUNT(*) AS mismatch_count', count_sql)
        self.assertIn('mismatch_scope', count_sql)

    def test_chain_summary_payload_includes_inventory_old_alignment_baseline(self):
        report = {
            'mode': 'execute',
            'status': 'SUCCESS',
            'started_at': datetime(2026, 5, 8, 0, 17, 50),
            'finished_at': datetime(2026, 5, 8, 0, 18, 57),
            'sales_window': {
                'start_date': 20260502,
                'end_date': 20260508,
            },
            'inventory_raw_window': {
                'start_time': '2026-05-07 00:17:53',
                'end_time': '2026-05-08 00:17:53',
            },
            'snapshot_date': 20260508,
            'steps': [
                {
                    'key': 'dws_sales_v2',
                    'status': 'SUCCESS',
                    'duration_seconds': 3.0,
                    'summary': {
                        'status': 'SUCCESS',
                        'insert_rowcount': 10613,
                        'reconciliation': {'mismatch_count': '0'},
                        'output_json': 'reports/context_cache/dws_sales_v2_shadow.json',
                    },
                },
                {
                    'key': 'inventory_old_dws_comparable_alignment',
                    'status': 'SUCCESS',
                    'duration_seconds': 1.2,
                    'summary': {
                        'status': 'SUCCESS',
                        'compare_source': 'ods_fa_storage',
                        'mismatch_count': 0,
                        'old_dws_probe': {
                            'old_dws_max_etl_time': '2026-05-08 00:06:31',
                        },
                    },
                },
                {
                    'key': 'dws_inventory_v2',
                    'status': 'SUCCESS',
                    'duration_seconds': 21.0,
                    'summary': {
                        'status': 'SUCCESS',
                        'delete_rowcount': 0,
                        'insert_rowcount': 73904,
                        'reconciliation': {'mismatch_count': '0'},
                        'output_json': 'reports/context_cache/dws_inventory_v2_shadow.json',
                    },
                },
            ],
        }

        payload = _build_chain_summary_payload(report, attempt=1, max_retries=3)

        self.assertIn(
            '库存 DWD→v2 对账：status=SUCCESS, mismatch_count=0',
            payload['summary_lines'],
        )
        self.assertIn(
            '库存当前 ODS 基线：status=SUCCESS, mismatch_count=0, compare_source=ods_fa_storage',
            payload['summary_lines'],
        )
        self.assertIn(
            '库存 ADS 门：status=READY, basis=current_ods_and_dwd_baseline',
            payload['summary_lines'],
        )
        self.assertIn(
            '库存当前 ODS 基线：compare_source=ods_fa_storage, mismatch_count=0, old_dws_max_etl_time=2026-05-08 00:06:31',
            payload['detail_lines'],
        )
        self.assertIn(
            '库存 ADS 门：basis=ods_fa_storage + dwd_inventory_storage_snapshot -> dws_inventory_daily_v2, compare_source=ods_fa_storage, current_ods_baseline_status=SUCCESS, dwd_to_v2_status=SUCCESS, dwd_to_v2_mismatch_count=0, reason=current_ods_baseline_and_dwd_to_v2_passed, same_snapshot_requested=False',
            payload['detail_lines'],
        )


if __name__ == '__main__':
    unittest.main()