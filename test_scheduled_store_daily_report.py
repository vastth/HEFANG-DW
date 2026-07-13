# -*- coding: utf-8 -*-

import unittest
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

from scheduled_store_daily_report import (
    AdsBackfillContext,
    DutyFreeImportInspection,
    DutyFreeImportRunResult,
    ImportInspection,
    OwnerImportInspection,
    OwnerImportRunResult,
    ScheduleRunResult,
    ScheduledAdsBackfillError,
    ScheduledImportError,
    ScheduledImportSkip,
    SCHEDULE_LOCK_NAME,
    _apply_natural_progress_fallback,
    _build_affected_date_summary,
    _build_duty_free_affected_date_summary,
    _build_schedule_result_chain_summary_payload,
    _build_owner_affected_date_summary,
    _build_skipped_ads_backfill_summary,
    _inspect_duty_free_file,
    _inspect_target_file,
    _merge_affected_date_summaries,
    _resolve_default_owner_snapshot_date,
    _run_ads_backfill_context,
    build_parser,
    run_schedule_once,
    run_with_retries,
)


def _make_runner(name, calls, fail_on_date=None):
    def _runner(report_date, data_version, max_retries, retry_sleep):
        report_date_text = report_date.isoformat()
        calls.append((name, report_date_text, data_version, max_retries, retry_sleep))
        if fail_on_date and report_date_text == fail_on_date:
            raise RuntimeError(f'{name} failed on {report_date_text}')
        return {
            'output_row_count': len(calls),
            'duration_seconds': len(calls),
        }

    return _runner


class TestScheduledStoreDailyReport(unittest.TestCase):
    @staticmethod
    def _make_target_inspection():
        return ImportInspection(
            file_path=Path('targets.xlsx'),
            file_md5='target-md5',
            target_month='2026-04',
            target_month_start=date(2026, 4, 1),
            target_version='v1',
            sheet_name='门店目标模板',
            source_row_count=10,
            available_target_months=['2026-04'],
            file_modified_at='2026-04-22 10:00:00',
        )

    @staticmethod
    def _make_owner_result(snapshot_date, changed=0, new=0, exited=0):
        return OwnerImportRunResult(
            outcome='IMPORTED',
            inspection=OwnerImportInspection(
                file_path=Path('owners.xlsx'),
                file_md5='owner-md5',
                snapshot_date=snapshot_date,
                sheet_name='门店负责人映射模板',
            ),
            summary={
                'snapshot_date': snapshot_date.isoformat(),
                'matched_entity_count': 73,
                'blank_owner_count': 2,
                'earliest_history_effective_start_date': snapshot_date.isoformat(),
                'history_diff_counts': {
                    'changed': changed,
                    'new': new,
                    'exited': exited,
                },
            },
            existing_log=None,
        )

    @staticmethod
    def _make_duty_free_result(target_month_start, changed=0, new=0, exited=0, has_changes=True):
        return DutyFreeImportRunResult(
            outcome='IMPORTED',
            inspection=DutyFreeImportInspection(
                file_path=Path('duty_free.xlsx'),
                file_md5='duty-free-md5',
                target_month=target_month_start.strftime('%Y-%m'),
                target_month_start=target_month_start,
                data_version='v1',
                sheet_name='免税月累计',
                source_row_count=3,
                file_modified_at='2026-04-27 09:00:00',
            ),
            summary={
                'target_month': target_month_start.strftime('%Y-%m'),
                'target_month_start': target_month_start.isoformat(),
                'data_version': 'v1',
                'matched_store_count': 3,
                'changed_store_count': changed,
                'new_store_count': new,
                'exited_store_count': exited,
                'has_changes': has_changes,
            },
            existing_log=None,
        )

    @patch('scheduled_store_daily_report._parse_workbook')
    @patch('scheduled_store_daily_report._compute_target_file_md5', return_value='target-md5')
    @patch('scheduled_store_daily_report._resolve_target_file')
    def test_inspect_target_file_accepts_previous_day_month_rollover(
        self,
        mock_resolve_target_file,
        mock_compute_target_file_md5,
        mock_parse_workbook,
    ):
        mock_resolve_target_file.return_value = Path(__file__)
        mock_parse_workbook.return_value = (
            None,
            {
                'target_month': '2026-05',
                'target_version': 'v1',
                'source_row_count': 10,
                'available_target_months': ['2026-05'],
            },
        )

        inspection = _inspect_target_file(
            None,
            None,
            '门店目标模板',
            True,
            report_date_mode='previous-day',
            run_date=date(2026, 6, 1),
        )

        self.assertEqual(inspection.target_month, '2026-05')
        mock_compute_target_file_md5.assert_called_once()

    @patch('scheduled_store_daily_report._parse_workbook')
    @patch('scheduled_store_daily_report._compute_target_file_md5', return_value='target-md5')
    @patch('scheduled_store_daily_report._resolve_target_file')
    def test_inspect_target_file_rejects_month_not_matching_current_day_mode(
        self,
        mock_resolve_target_file,
        mock_compute_target_file_md5,
        mock_parse_workbook,
    ):
        mock_resolve_target_file.return_value = Path(__file__)
        mock_parse_workbook.return_value = (
            None,
            {
                'target_month': '2026-05',
                'target_version': 'v1',
                'source_row_count': 10,
                'available_target_months': ['2026-05'],
            },
        )

        with self.assertRaises(ScheduledImportSkip) as cm:
            _inspect_target_file(
                None,
                None,
                '门店目标模板',
                True,
                report_date_mode='current-day',
                run_date=date(2026, 6, 1),
            )

        self.assertIn('2026-06-01', str(cm.exception))
        self.assertIn('2026-06', str(cm.exception))
        mock_compute_target_file_md5.assert_called_once()

    @patch('scheduled_store_daily_report._parse_duty_free_workbook')
    @patch('scheduled_store_daily_report._compute_duty_free_file_md5', return_value='duty-free-md5')
    @patch('scheduled_store_daily_report._resolve_duty_free_input_file')
    def test_inspect_duty_free_file_accepts_previous_day_month_rollover(
        self,
        mock_resolve_duty_free_input_file,
        mock_compute_duty_free_file_md5,
        mock_parse_duty_free_workbook,
    ):
        mock_resolve_duty_free_input_file.return_value = Path(__file__)
        mock_parse_duty_free_workbook.return_value = (
            None,
            {
                'target_month': '2026-05',
                'target_month_start': '2026-05-01',
                'data_version': 'v1',
                'source_row_count': 3,
            },
        )

        inspection = _inspect_duty_free_file(
            None,
            '免税月累计',
            report_date_mode='previous-day',
            run_date=date(2026, 6, 1),
        )

        self.assertEqual(inspection.target_month, '2026-05')
        mock_compute_duty_free_file_md5.assert_called_once()

    def test_resolve_default_owner_snapshot_date_uses_topic_upper_bound(self):
        self.assertEqual(
            _resolve_default_owner_snapshot_date(
                date(2026, 5, 1),
                date(2026, 6, 1),
                'previous-day',
            ),
            date(2026, 5, 31),
        )

    def test_run_ads_backfill_context_runs_three_ads_jobs_in_order(self):
        calls = []
        context = AdsBackfillContext(
            data_version='v1',
            report_dates=['2026-04-01', '2026-04-02'],
            source='affected_dates',
            requested_report_dates=['2026-04-01', '2026-04-02'],
            completed_report_dates=[],
        )

        summary = _run_ads_backfill_context(
            context,
            max_retries=3,
            retry_sleep=5,
            store_run_func=_make_runner('store', calls),
            subject_run_func=_make_runner('subject', calls),
            daily_sales_run_func=_make_runner('daily_sales', calls),
        )

        self.assertEqual(
            calls,
            [
                ('store', '2026-04-01', 'v1', 3, 5),
                ('subject', '2026-04-01', 'v1', 3, 5),
                ('daily_sales', '2026-04-01', 'v1', 3, 5),
                ('store', '2026-04-02', 'v1', 3, 5),
                ('subject', '2026-04-02', 'v1', 3, 5),
                ('daily_sales', '2026-04-02', 'v1', 3, 5),
            ],
        )
        self.assertEqual(summary['completed_report_dates'], ['2026-04-01', '2026-04-02'])
        self.assertEqual(summary['failed_date_count'], 0)
        self.assertIn('销售看板', summary['note'])

    @patch('scheduled_store_daily_report._fetch_stale_ads_dates_by_dws_freshness')
    @patch('scheduled_store_daily_report._fetch_ads_latest_report_dates')
    def test_apply_natural_progress_fallback_uses_dws_freshness_when_dates_already_covered(
        self,
        mock_fetch_latest,
        mock_fetch_stale,
    ):
        inspection = self._make_target_inspection()
        target_summary = _build_affected_date_summary(
            'SKIPPED',
            inspection,
            None,
            schedule_run_date=date(2026, 4, 27),
        )
        owner_summary = _build_owner_affected_date_summary(
            self._make_owner_result(date(2026, 4, 27), changed=0),
            inspection,
            schedule_run_date=date(2026, 4, 27),
        )
        merged = _merge_affected_date_summaries(target_summary, owner_summary)
        mock_fetch_latest.return_value = {
            'ads_store_daily_report': '2026-04-26',
            'ads_store_daily_subject_report': '2026-04-26',
            'ads_daily_sales': '2026-04-26',
        }
        mock_fetch_stale.return_value = {
            'enabled': True,
            'rule': 'dws_sales_daily_etl_time_newer_than_ads',
            'start_date': '2026-04-25',
            'end_date': '2026-04-26',
            'date_count': 2,
            'stale_dates': ['2026-04-25', '2026-04-26'],
            'stale_table_counts': {'ads_daily_sales': 2},
            'note': '检测到主链 dws_sales_daily 的 etl_time 晚于专题 ADS，按近 7 天 freshness 窗口触发重跑',
        }

        summary = _apply_natural_progress_fallback(inspection, merged)

        self.assertEqual(summary['affected_dates'], ['2026-04-25', '2026-04-26'])
        self.assertEqual(summary['affected_date_count'], 2)
        self.assertFalse(summary['natural_progress_branch']['enabled'])
        self.assertTrue(summary['source_freshness_branch']['enabled'])
        self.assertIn('DWS freshness', summary['note'])
        self.assertEqual(
            mock_fetch_stale.call_args.kwargs['sales_freshness_source_mode'],
            'legacy',
        )

    @patch('scheduled_store_daily_report._apply_natural_progress_fallback')
    @patch('scheduled_store_daily_report._merge_affected_date_summaries')
    @patch('scheduled_store_daily_report._build_owner_affected_date_summary')
    @patch('scheduled_store_daily_report._build_affected_date_summary')
    @patch('scheduled_store_daily_report._run_owner_schedule_once')
    @patch('scheduled_store_daily_report._fetch_existing_success_log')
    @patch('scheduled_store_daily_report._ensure_log_table_exists')
    @patch('scheduled_store_daily_report._inspect_target_file')
    def test_run_schedule_once_derives_v2_freshness_source_from_cutover_mode(
        self,
        mock_inspect_target_file,
        mock_ensure_log_table_exists,
        mock_fetch_existing_success_log,
        mock_run_owner_schedule_once,
        mock_build_affected_date_summary,
        mock_build_owner_affected_date_summary,
        mock_merge_affected_date_summaries,
        mock_apply_natural_progress_fallback,
    ):
        args = build_parser().parse_args(['--cutover-mode', 'v2', '--auto-report-date-mode', 'current-day', '--no-run-duty-free-import'])
        inspection = self._make_target_inspection()
        owner_result = self._make_owner_result(date(2026, 4, 27), changed=0)
        merged_summary = {
            'outcome': 'SKIPPED',
            'affected_date_count': 0,
            'affected_dates': [],
            'note': 'merged',
            'upper_bound': '2026-04-26',
            'target_branch': {'date_count': 0},
            'store_attr_branch': {'date_count': 0},
            'owner_branch': {'date_count': 0},
            'natural_progress_branch': {'date_count': 0},
            'source_freshness_branch': {'date_count': 0},
        }
        final_summary = {
            'outcome': 'SKIPPED',
            'affected_date_count': 0,
            'affected_dates': [],
            'note': 'final',
            'upper_bound': '2026-04-26',
            'target_branch': {'date_count': 0},
            'store_attr_branch': {'date_count': 0},
            'owner_branch': {'date_count': 0},
            'natural_progress_branch': {'date_count': 0},
            'source_freshness_branch': {'date_count': 0},
        }

        mock_inspect_target_file.return_value = inspection
        mock_fetch_existing_success_log.return_value = {
            'id': 4,
            'finished_at': '2026-04-27 08:00:00',
            'created_at': '2026-04-27 07:59:00',
            'records_inserted': 279,
        }
        mock_run_owner_schedule_once.return_value = owner_result
        mock_build_affected_date_summary.return_value = {'target_branch': {'date_count': 0}}
        mock_build_owner_affected_date_summary.return_value = {'owner_branch': {'date_count': 0}}
        mock_merge_affected_date_summaries.return_value = merged_summary
        mock_apply_natural_progress_fallback.return_value = final_summary

        result = run_schedule_once(args)

        self.assertEqual(result.outcome, 'SKIPPED')
        self.assertEqual(result.affected_date_summary, final_summary)
        self.assertEqual(result.ads_backfill_summary['mode'], 'SKIPPED')
        self.assertEqual(
            mock_build_affected_date_summary.call_args.kwargs['report_date_mode'],
            'current-day',
        )
        self.assertEqual(
            mock_build_owner_affected_date_summary.call_args.kwargs['report_date_mode'],
            'current-day',
        )
        self.assertEqual(
            mock_apply_natural_progress_fallback.call_args.kwargs['sales_freshness_source_mode'],
            'v2',
        )

    @patch('scheduled_store_daily_report.date')
    @patch('scheduled_store_daily_report._apply_natural_progress_fallback')
    @patch('scheduled_store_daily_report._merge_affected_date_summaries')
    @patch('scheduled_store_daily_report._build_owner_affected_date_summary')
    @patch('scheduled_store_daily_report._build_affected_date_summary')
    @patch('scheduled_store_daily_report._run_owner_schedule_once')
    @patch('scheduled_store_daily_report._fetch_existing_success_log')
    @patch('scheduled_store_daily_report._ensure_log_table_exists')
    @patch('scheduled_store_daily_report._inspect_target_file')
    def test_run_schedule_once_aligns_default_owner_snapshot_date_to_topic_upper_bound(
        self,
        mock_inspect_target_file,
        mock_ensure_log_table_exists,
        mock_fetch_existing_success_log,
        mock_run_owner_schedule_once,
        mock_build_affected_date_summary,
        mock_build_owner_affected_date_summary,
        mock_merge_affected_date_summaries,
        mock_apply_natural_progress_fallback,
        mock_date,
    ):
        args = build_parser().parse_args(['--cutover-mode', 'v2', '--no-run-duty-free-import'])
        inspection = ImportInspection(
            file_path=Path('targets.xlsx'),
            file_md5='target-md5',
            target_month='2026-05',
            target_month_start=date(2026, 5, 1),
            target_version='v1',
            sheet_name='门店目标模板',
            source_row_count=10,
            available_target_months=['2026-05'],
            file_modified_at='2026-06-01 09:00:00',
        )
        owner_result = self._make_owner_result(date(2026, 5, 31), changed=0)
        merged_summary = {
            'outcome': 'SKIPPED',
            'affected_date_count': 0,
            'affected_dates': [],
            'note': 'merged',
            'upper_bound': '2026-05-31',
            'target_branch': {'date_count': 0},
            'store_attr_branch': {'date_count': 0},
            'owner_branch': {'date_count': 0},
            'natural_progress_branch': {'date_count': 0},
            'source_freshness_branch': {'date_count': 0},
        }
        final_summary = dict(merged_summary)

        mock_date.today.return_value = date(2026, 6, 1)
        mock_inspect_target_file.return_value = inspection
        mock_fetch_existing_success_log.return_value = {
            'id': 21,
            'finished_at': '2026-05-19 11:35:36',
            'created_at': '2026-05-19 11:34:00',
            'records_inserted': 2249,
        }
        mock_run_owner_schedule_once.return_value = owner_result
        mock_build_affected_date_summary.return_value = {'target_branch': {'date_count': 0}}
        mock_build_owner_affected_date_summary.return_value = {'owner_branch': {'date_count': 0}}
        mock_merge_affected_date_summaries.return_value = merged_summary
        mock_apply_natural_progress_fallback.return_value = final_summary

        result = run_schedule_once(args)

        self.assertEqual(result.outcome, 'SKIPPED')
        self.assertEqual(
            mock_run_owner_schedule_once.call_args.kwargs['default_snapshot_date'],
            date(2026, 5, 31),
        )

    def test_build_duty_free_affected_date_summary_uses_schedule_upper_bound_when_snapshot_changed(self):
        inspection = self._make_target_inspection()
        duty_free_result = self._make_duty_free_result(date(2026, 4, 1), changed=1)

        summary = _build_duty_free_affected_date_summary(
            duty_free_result,
            schedule_run_date=date(2026, 4, 27),
        )

        self.assertEqual(summary['target_month'], '2026-04')
        self.assertEqual(summary['upper_bound'], '2026-04-26')
        self.assertEqual(summary['affected_dates'], ['2026-04-26'])
        self.assertEqual(summary['affected_date_count'], 1)
        self.assertTrue(summary['duty_free_branch']['enabled'])
        self.assertEqual(summary['duty_free_branch']['changed_store_count'], 1)

    def test_build_duty_free_affected_date_summary_extends_upper_bound_to_run_date_in_current_day_mode(self):
        duty_free_result = self._make_duty_free_result(date(2026, 4, 1), changed=1)

        summary = _build_duty_free_affected_date_summary(
            duty_free_result,
            schedule_run_date=date(2026, 4, 27),
            report_date_mode='current-day',
        )

        self.assertEqual(summary['upper_bound'], '2026-04-27')
        self.assertEqual(summary['affected_dates'], ['2026-04-27'])

    def test_build_affected_date_summary_extends_upper_bound_to_run_date_in_current_day_mode(self):
        inspection = self._make_target_inspection()

        summary = _build_affected_date_summary(
            'IMPORTED',
            inspection,
            {'sync_store_report_attr': False},
            schedule_run_date=date(2026, 4, 27),
            report_date_mode='current-day',
        )

        self.assertEqual(summary['upper_bound'], '2026-04-27')
        self.assertEqual(summary['target_branch']['end_date'], '2026-04-27')
        self.assertEqual(summary['affected_dates'][-1], '2026-04-27')
        self.assertEqual(summary['affected_date_count'], 27)

    def test_build_owner_affected_date_summary_clamps_snapshot_to_target_month(self):
        inspection = self._make_target_inspection()
        owner_result = self._make_owner_result(date(2026, 3, 28), changed=1)

        summary = _build_owner_affected_date_summary(
            owner_result,
            inspection,
            schedule_run_date=date(2026, 4, 6),
        )

        self.assertEqual(summary['owner_branch']['start_date'], '2026-04-01')
        self.assertEqual(summary['owner_branch']['end_date'], '2026-04-05')
        self.assertEqual(summary['affected_dates'][0], '2026-04-01')
        self.assertEqual(summary['affected_dates'][-1], '2026-04-05')

    def test_build_owner_affected_date_summary_uses_earliest_history_effective_start_date(self):
        inspection = self._make_target_inspection()
        owner_result = OwnerImportRunResult(
            outcome='IMPORTED',
            inspection=OwnerImportInspection(
                file_path=Path('owners.xlsx'),
                file_md5='owner-md5',
                snapshot_date=date(2026, 5, 12),
                sheet_name='门店负责人映射模板',
            ),
            summary={
                'snapshot_date': '2026-05-12',
                'matched_entity_count': 72,
                'blank_owner_count': 8,
                'earliest_history_effective_start_date': '2026-04-03',
                'history_diff_counts': {
                    'changed': 0,
                    'new': 1,
                    'exited': 0,
                },
            },
            existing_log=None,
        )

        summary = _build_owner_affected_date_summary(
            owner_result,
            inspection,
            schedule_run_date=date(2026, 4, 6),
        )

        self.assertEqual(summary['owner_branch']['effective_start_date'], '2026-04-03')
        self.assertEqual(summary['owner_branch']['start_date'], '2026-04-03')
        self.assertEqual(summary['affected_dates'], ['2026-04-03', '2026-04-04', '2026-04-05'])

    def test_build_owner_affected_date_summary_extends_upper_bound_to_run_date_in_current_day_mode(self):
        inspection = self._make_target_inspection()
        owner_result = self._make_owner_result(date(2026, 4, 27), changed=1)

        summary = _build_owner_affected_date_summary(
            owner_result,
            inspection,
            schedule_run_date=date(2026, 4, 27),
            report_date_mode='current-day',
        )

        self.assertEqual(summary['upper_bound'], '2026-04-27')
        self.assertEqual(summary['owner_branch']['end_date'], '2026-04-27')
        self.assertEqual(summary['affected_dates'], ['2026-04-27'])

    def test_merge_affected_date_summaries_unions_target_and_owner_dates(self):
        inspection = self._make_target_inspection()
        target_summary = _build_affected_date_summary(
            'IMPORTED',
            inspection,
            {'sync_store_report_attr': False},
            schedule_run_date=date(2026, 4, 5),
        )
        owner_summary = _build_owner_affected_date_summary(
            self._make_owner_result(date(2026, 4, 3), changed=1),
            inspection,
            schedule_run_date=date(2026, 4, 5),
        )

        merged = _merge_affected_date_summaries(target_summary, owner_summary)

        self.assertEqual(merged['affected_dates'], ['2026-04-01', '2026-04-02', '2026-04-03', '2026-04-04'])
        self.assertEqual(merged['target_branch']['date_count'], 4)
        self.assertEqual(merged['owner_branch']['date_count'], 2)
        self.assertIn('负责人链路', merged['note'])

    @patch('scheduled_store_daily_report._fetch_ads_latest_report_dates')
    def test_apply_natural_progress_fallback_backfills_full_month_when_any_ads_table_missing(
        self,
        mock_fetch_latest,
    ):
        inspection = self._make_target_inspection()
        target_summary = _build_affected_date_summary(
            'SKIPPED',
            inspection,
            None,
            schedule_run_date=date(2026, 4, 27),
        )
        owner_summary = _build_owner_affected_date_summary(
            self._make_owner_result(date(2026, 4, 26), changed=0),
            inspection,
            schedule_run_date=date(2026, 4, 27),
        )
        merged = _merge_affected_date_summaries(target_summary, owner_summary)
        mock_fetch_latest.return_value = {
            'ads_store_daily_report': '2026-04-24',
            'ads_store_daily_subject_report': '2026-04-24',
            'ads_daily_sales': None,
        }

        summary = _apply_natural_progress_fallback(inspection, merged)

        self.assertEqual(summary['affected_dates'][0], '2026-04-01')
        self.assertEqual(summary['affected_dates'][-1], '2026-04-26')
        self.assertEqual(summary['affected_date_count'], 26)
        self.assertTrue(summary['natural_progress_branch']['enabled'])
        self.assertEqual(
            summary['natural_progress_branch']['rule'],
            'full_month_if_any_ads_table_missing',
        )
        self.assertEqual(summary['natural_progress_branch']['missing_tables'], ['ads_daily_sales'])

    @patch('scheduled_store_daily_report._fetch_ads_latest_report_dates')
    def test_apply_natural_progress_fallback_backfills_from_min_latest_date_plus_one(
        self,
        mock_fetch_latest,
    ):
        inspection = self._make_target_inspection()
        target_summary = _build_affected_date_summary(
            'SKIPPED',
            inspection,
            None,
            schedule_run_date=date(2026, 4, 27),
        )
        owner_summary = _build_owner_affected_date_summary(
            self._make_owner_result(date(2026, 4, 26), changed=0),
            inspection,
            schedule_run_date=date(2026, 4, 27),
        )
        merged = _merge_affected_date_summaries(target_summary, owner_summary)
        mock_fetch_latest.return_value = {
            'ads_store_daily_report': '2026-04-24',
            'ads_store_daily_subject_report': '2026-04-25',
            'ads_daily_sales': '2026-04-26',
        }

        summary = _apply_natural_progress_fallback(inspection, merged)

        self.assertEqual(summary['affected_dates'], ['2026-04-25', '2026-04-26'])
        self.assertEqual(summary['affected_date_count'], 2)
        self.assertTrue(summary['natural_progress_branch']['enabled'])
        self.assertEqual(
            summary['natural_progress_branch']['rule'],
            'min_ads_latest_report_date_plus_one_to_upper_bound',
        )
        self.assertEqual(summary['natural_progress_branch']['start_date'], '2026-04-25')
        self.assertEqual(summary['natural_progress_branch']['end_date'], '2026-04-26')

    @patch('scheduled_store_daily_report._fetch_ads_latest_report_dates')
    def test_apply_natural_progress_fallback_still_runs_when_target_and_owner_both_skipped(
        self,
        mock_fetch_latest,
    ):
        inspection = self._make_target_inspection()
        target_summary = _build_affected_date_summary(
            'SKIPPED',
            inspection,
            None,
            schedule_run_date=date(2026, 4, 27),
        )
        owner_result = OwnerImportRunResult(
            outcome='SKIPPED',
            inspection=OwnerImportInspection(
                file_path=Path('owners.xlsx'),
                file_md5='owner-md5',
                snapshot_date=date(2026, 4, 27),
                sheet_name='门店负责人映射模板',
            ),
            summary=None,
            existing_log={'id': 5},
        )
        owner_summary = _build_owner_affected_date_summary(
            owner_result,
            inspection,
            schedule_run_date=date(2026, 4, 27),
        )
        merged = _merge_affected_date_summaries(target_summary, owner_summary)
        mock_fetch_latest.return_value = {
            'ads_store_daily_report': '2026-04-24',
            'ads_store_daily_subject_report': '2026-04-24',
            'ads_daily_sales': '2026-04-24',
        }

        summary = _apply_natural_progress_fallback(inspection, merged)

        self.assertEqual(summary['affected_dates'], ['2026-04-25', '2026-04-26'])
        self.assertEqual(summary['upper_bound'], '2026-04-26')
        self.assertTrue(summary['natural_progress_branch']['enabled'])

    def test_run_ads_backfill_context_keeps_remaining_dates_when_daily_sales_fails(self):
        calls = []
        context = AdsBackfillContext(
            data_version='v1',
            report_dates=['2026-04-01', '2026-04-02'],
            source='affected_dates',
            requested_report_dates=['2026-04-01', '2026-04-02'],
            completed_report_dates=[],
        )

        with self.assertRaises(ScheduledAdsBackfillError) as cm:
            _run_ads_backfill_context(
                context,
                max_retries=2,
                retry_sleep=1,
                store_run_func=_make_runner('store', calls),
                subject_run_func=_make_runner('subject', calls),
                daily_sales_run_func=_make_runner('daily_sales', calls, fail_on_date='2026-04-02'),
            )

        error = cm.exception
        self.assertEqual(error.context.completed_report_dates, ['2026-04-01'])
        self.assertEqual(error.context.report_dates, ['2026-04-02'])
        self.assertIn('销售看板', str(error))

    def test_build_skipped_ads_backfill_summary_mentions_sales_dashboard(self):
        summary = _build_skipped_ads_backfill_summary(
            reason='disabled_by_cli',
            data_version='v1',
            source='affected_dates',
            report_dates=['2026-04-01'],
        )

        self.assertIn('销售看板', summary['note'])
        self.assertEqual(summary['requested_report_dates'], ['2026-04-01'])

    def test_build_schedule_result_chain_summary_payload_marks_skipped_when_no_write_happened(self):
        inspection = self._make_target_inspection()
        target_summary = _build_affected_date_summary(
            'SKIPPED',
            inspection,
            None,
            schedule_run_date=date(2026, 4, 27),
        )
        owner_result = OwnerImportRunResult(
            outcome='SKIPPED',
            inspection=OwnerImportInspection(
                file_path=Path('owners.xlsx'),
                file_md5='owner-md5',
                snapshot_date=date(2026, 4, 27),
                sheet_name='门店负责人映射模板',
            ),
            summary=None,
            existing_log={'id': 5, 'finished_at': '2026-04-27 09:00:00', 'created_at': '2026-04-27 08:59:00'},
        )
        owner_summary = _build_owner_affected_date_summary(
            owner_result,
            inspection,
            schedule_run_date=date(2026, 4, 27),
        )
        result = ScheduleRunResult(
            outcome='SKIPPED',
            inspection=inspection,
            summary=None,
            existing_log={'id': 4, 'finished_at': '2026-04-27 08:00:00', 'created_at': '2026-04-27 07:59:00'},
            owner_result=owner_result,
            affected_date_summary=_merge_affected_date_summaries(target_summary, owner_summary),
            ads_backfill_summary=_build_skipped_ads_backfill_summary(
                reason='empty_affected_dates',
                data_version='v1',
                source='affected_dates',
            ),
        )

        payload = _build_schedule_result_chain_summary_payload(
            result,
            attempt=1,
            max_retries=3,
            started_at=datetime(2026, 4, 27, 9, 0, 0),
            ended_at=datetime(2026, 4, 27, 9, 0, 5),
        )

        self.assertEqual(payload['status'], 'SKIPPED')
        self.assertIn('ADS批量重跑：0/0', payload['summary_lines'][3])
        self.assertTrue(any('沿用既有成功记录' in line for line in payload['detail_lines']))

    @patch('scheduled_store_daily_report._release_singleton_lock')
    @patch('scheduled_store_daily_report.write_total_control_chain_summary')
    @patch('scheduled_store_daily_report._send_schedule_alert_if_enabled')
    @patch('scheduled_store_daily_report.run_schedule_once')
    @patch('scheduled_store_daily_report._acquire_singleton_lock')
    def test_run_with_retries_emits_warning_when_import_summary_contains_unmatched_stores(
        self,
        mock_acquire_lock,
        mock_run_schedule_once,
        mock_send_alert,
        mock_write_summary,
        mock_release_lock,
    ):
        args = build_parser().parse_args([])
        inspection = self._make_target_inspection()
        owner_result = self._make_owner_result(date(2026, 4, 27), changed=1)
        mock_acquire_lock.return_value = ('lock_conn', 321)
        mock_run_schedule_once.return_value = ScheduleRunResult(
            outcome='IMPORTED',
            inspection=inspection,
            summary={
                'matched_store_count': 9,
                'records_inserted': 279,
                'sync_store_report_attr': False,
                'sync_assessment': False,
                'warning_messages': [
                    "以下门店名称在 dim_store 中未命中，已跳过这些门店的目标/门店属性配置: ['长沙运达汇店']；候选建议: {'长沙运达汇店': ['长沙IFS店']}"
                ],
                'validation_status': 'WARNING',
            },
            existing_log=None,
            owner_result=owner_result,
            affected_date_summary={
                'affected_date_count': 1,
                'affected_dates': ['2026-04-26'],
                'note': '受影响日期来自目标导入',
                'owner_branch': {'date_count': 1},
            },
            ads_backfill_summary={
                'mode': 'EXECUTED',
                'source': 'affected_dates',
                'data_version': 'v1',
                'requested_report_dates': ['2026-04-26'],
                'requested_date_count': 1,
                'completed_report_dates': ['2026-04-26'],
                'completed_date_count': 1,
                'failed_report_dates': [],
                'failed_date_count': 0,
                'failed_details': [],
                'note': '已重跑 1 天专题 ADS',
            },
        )

        result = run_with_retries(args)

        self.assertEqual(result, 0)
        alert_content = mock_send_alert.call_args.args[0]
        self.assertIn('WARNING', alert_content)
        self.assertIn('长沙运达汇店', alert_content)
        payload = mock_write_summary.call_args.args[0]
        self.assertEqual(payload['status'], 'WARNING')
        self.assertTrue(any('门店映射告警' in line for line in payload['summary_lines']))
        self.assertTrue(any('长沙运达汇店' in line for line in payload['issue_lines']))
        mock_release_lock.assert_called_once_with('lock_conn', SCHEDULE_LOCK_NAME)

    @patch('scheduled_store_daily_report._release_singleton_lock')
    @patch('scheduled_store_daily_report._run_ads_backfill_context')
    @patch('scheduled_store_daily_report._build_explicit_ads_backfill_context')
    @patch('scheduled_store_daily_report._acquire_singleton_lock')
    def test_run_with_retries_uses_singleton_lock_for_explicit_rerun(
        self,
        mock_acquire_lock,
        mock_build_context,
        mock_run_backfill,
        mock_release_lock,
    ):
        args = build_parser().parse_args(['--rerun-report-date', '2026-04-01'])
        context = AdsBackfillContext(
            data_version='v1',
            report_dates=['2026-04-01'],
            source='explicit',
            requested_report_dates=['2026-04-01'],
            completed_report_dates=[],
        )
        mock_acquire_lock.return_value = ('lock_conn', 321)
        mock_build_context.return_value = context
        mock_run_backfill.return_value = {
            'mode': 'EXECUTED',
            'source': 'explicit',
            'data_version': 'v1',
            'requested_report_dates': ['2026-04-01'],
            'requested_date_count': 1,
            'completed_report_dates': ['2026-04-01'],
            'completed_date_count': 1,
            'failed_report_dates': [],
            'failed_date_count': 0,
            'failed_details': [],
            'note': 'ok',
        }

        result = run_with_retries(args)

        self.assertEqual(result, 0)
        mock_acquire_lock.assert_called_once_with(SCHEDULE_LOCK_NAME)
        mock_release_lock.assert_called_once_with('lock_conn', SCHEDULE_LOCK_NAME)

    @patch(
        'scheduled_store_daily_report._acquire_singleton_lock',
        side_effect=ScheduledImportError('已有其他专题调度实例在运行', retryable=False),
    )
    def test_run_with_retries_returns_error_when_singleton_lock_is_busy(self, mock_acquire_lock):
        args = build_parser().parse_args(['--rerun-report-date', '2026-04-01'])

        result = run_with_retries(args)

        self.assertEqual(result, 2)
        mock_acquire_lock.assert_called_once_with(SCHEDULE_LOCK_NAME)

    def test_build_parser_accepts_auto_report_date_mode(self):
        args = build_parser().parse_args(['--auto-report-date-mode', 'current-day'])

        self.assertEqual(args.auto_report_date_mode, 'current-day')


if __name__ == '__main__':
    unittest.main()